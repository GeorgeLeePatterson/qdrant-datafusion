//! Systematic `DataFusion` expression to `Qdrant` filter translation

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::not_impl_err;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::expr::{InList, Like, ScalarFunction};
use datafusion::logical_expr::{BinaryExpr, Operator};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use qdrant_client::qdrant::{Condition, Filter, Range};

use super::utils;

/// Schema-aware `DataFusion` expression to `Qdrant` filter translator
pub struct FilterBuilder {
    schema: SchemaRef,
}

impl FilterBuilder {
    /// Create a new `FilterBuilder` with the given schema
    pub fn new(schema: SchemaRef) -> Self { Self { schema } }

    /// Convert a `DataFusion` expression to a `Qdrant` filter using recursive descent
    pub fn expr_to_filter(&self, expr: &Expr) -> DataFusionResult<Filter> {
        match expr {
            // Boolean operators - recursive with eager merging
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => {
                let left_filter = self.expr_to_filter(left)?;
                let right_filter = self.expr_to_filter(right)?;
                Ok(merge_and_filters(left_filter, right_filter))
            }

            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Or, right }) => {
                let left_filter = self.expr_to_filter(left)?;
                let right_filter = self.expr_to_filter(right)?;
                Ok(merge_or_filters(left_filter, right_filter))
            }

            Expr::Not(inner) => {
                let inner_filter = self.expr_to_filter(inner)?;
                Ok(Filter::must_not([inner_filter.into()]))
            }

            // Leaf conditions
            Expr::BinaryExpr(binary) => self.build_comparison_condition(binary),
            Expr::InList(in_list) => self.build_in_list_condition(in_list),
            Expr::IsNull(expr) => self.build_is_null_condition(expr),
            Expr::Like(like) => self.build_like_condition(like),

            // Handle aliases by unwrapping
            Expr::Alias(alias) => self.expr_to_filter(&alias.expr),

            _ => not_impl_err!("Unsupported filter expression: {expr}"),
        }
    }

    /// Build comparison conditions (=, >, <, etc.)
    fn build_comparison_condition(&self, binary: &BinaryExpr) -> DataFusionResult<Filter> {
        let BinaryExpr { left, op, right } = binary;

        // Try pattern: <field> <op> <literal>
        if let (Some(field_info), Some(literal)) = (extract_field_info(left), right.as_literal()) {
            return self.build_field_condition(&field_info, *op, literal);
        }

        // Try reversed pattern: <literal> <op> <field>
        if let (Some(literal), Some(field_info)) = (left.as_literal(), extract_field_info(right)) {
            let reversed_op = utils::reverse_operator(*op);
            return self.build_field_condition(&field_info, reversed_op, literal);
        }

        not_impl_err!("Unsupported comparison: {binary}")
    }

    /// Build field-specific conditions based on field type
    fn build_field_condition(
        &self,
        field_info: &FieldInfo,
        op: Operator,
        literal: &ScalarValue,
    ) -> DataFusionResult<Filter> {
        let condition = match field_info.field_type {
            FieldType::Id => self.build_id_condition(op, literal)?,
            FieldType::Payload => self.build_payload_condition(&field_info.name, op, literal)?,
        };
        Ok(Filter::must([condition]))
    }

    /// Build ID-based conditions
    fn build_id_condition(
        &self,
        op: Operator,
        literal: &ScalarValue,
    ) -> DataFusionResult<Condition> {
        match op {
            Operator::Eq => Ok(Condition::has_id(utils::scalar_to_point_ids(literal)?)),
            _ => not_impl_err!("Unsupported ID operator: {op}"),
        }
    }

    /// Build payload field conditions
    fn build_payload_condition(
        &self,
        field_name: &str,
        op: Operator,
        literal: &ScalarValue,
    ) -> DataFusionResult<Condition> {
        match op {
            Operator::Eq => Ok(Condition::matches(field_name, utils::scalar_to_string(literal)?)),
            Operator::Gt => Ok(Condition::range(field_name, Range {
                gt: Some(utils::scalar_to_f64(literal)?),
                ..Default::default()
            })),
            Operator::GtEq => Ok(Condition::range(field_name, Range {
                gte: Some(utils::scalar_to_f64(literal)?),
                ..Default::default()
            })),
            Operator::Lt => Ok(Condition::range(field_name, Range {
                lt: Some(utils::scalar_to_f64(literal)?),
                ..Default::default()
            })),
            Operator::LtEq => Ok(Condition::range(field_name, Range {
                lte: Some(utils::scalar_to_f64(literal)?),
                ..Default::default()
            })),
            _ => not_impl_err!("Unsupported payload operator: {op}"),
        }
    }

    /// Build IN list conditions
    fn build_in_list_condition(&self, in_list: &InList) -> DataFusionResult<Filter> {
        let field_info = extract_field_info(&in_list.expr).ok_or_else(|| {
            datafusion::common::plan_datafusion_err!(
                "Cannot extract field info from InList expression"
            )
        })?;

        // TODO: Remove
        eprintln!("[build_in_list_condition] in_list: {in_list:?}, field_info: {field_info:?}");

        match field_info.field_type {
            FieldType::Id => {
                let mut point_ids = Vec::new();
                for expr in &in_list.list {
                    if let Some(literal) = expr.as_literal() {
                        point_ids.extend(utils::scalar_to_point_ids(literal)?);
                    } else {
                        return not_impl_err!("Non-literal values in ID IN list");
                    }
                }

                let condition = Condition::has_id(point_ids);
                let filter = if in_list.negated {
                    Filter::must_not([condition])
                } else {
                    Filter::must([condition])
                };
                Ok(filter)
            }
            FieldType::Payload => {
                // Build OR condition for payload IN list
                let mut conditions = Vec::new();
                for expr in &in_list.list {
                    if let Some(literal) = expr.as_literal() {
                        conditions.push(Condition::matches(
                            &field_info.name,
                            utils::scalar_to_string(literal)?,
                        ));
                    } else {
                        return not_impl_err!("Non-literal values in payload IN list");
                    }
                }

                let filter = if in_list.negated {
                    Filter::must_not([Filter::should(conditions).into()])
                } else {
                    Filter::should(conditions)
                };
                Ok(filter)
            }
        }
    }

    /// Build IS NULL conditions
    fn build_is_null_condition(&self, expr: &Expr) -> DataFusionResult<Filter> {
        let field_info = extract_field_info(expr).ok_or_else(|| {
            datafusion::common::plan_datafusion_err!(
                "Cannot extract field info from IS NULL expression"
            )
        })?;

        match field_info.field_type {
            FieldType::Id => not_impl_err!("IS NULL not supported on ID field"),
            FieldType::Payload => Ok(Filter::must([Condition::is_null(&field_info.name)])),
        }
    }

    /// Build LIKE conditions with text matching heuristics
    fn build_like_condition(&self, like: &Like) -> DataFusionResult<Filter> {
        let field_info = extract_field_info(&like.expr).ok_or_else(|| {
            datafusion::common::plan_datafusion_err!(
                "Cannot extract field info from LIKE expression"
            )
        })?;

        let pattern_literal = like.pattern.as_literal().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("LIKE pattern must be a literal")
        })?;

        let pattern = utils::scalar_to_string(pattern_literal)?;

        // Inline LIKE pattern heuristics - no tiny function needed
        let condition = if !pattern.contains('%') {
            Condition::matches_phrase(&field_info.name, pattern)
        } else if pattern.matches('%').count() >= 3
            || (pattern.matches('%').count() == 2
                && !pattern.starts_with('%')
                && !pattern.ends_with('%'))
        {
            Condition::matches_text(&field_info.name, pattern)
        } else {
            Condition::matches_phrase(&field_info.name, pattern)
        };

        let filter =
            if like.negated { Filter::must_not([condition]) } else { Filter::must([condition]) };
        Ok(filter)
    }

    /// Schema lookup helper - get data type for a field path
    fn field_data_type(&self, field_path: &str) -> Option<&DataType> {
        self.schema.field_with_name(field_path).ok().map(|field| field.data_type())
    }
}

/// Convert multiple `DataFusion` expressions to a single `Qdrant` filter (legacy API)
///
/// This function provides backward compatibility with the existing API.
/// For new code, use `FilterBuilder::expr_to_filter()` directly.
pub(crate) fn translate_payload_filters(
    filters: &[Expr],
    schema: &SchemaRef,
) -> DataFusionResult<Filter> {
    if filters.is_empty() {
        return Ok(Filter::default());
    }

    // Create a basic schema for legacy compatibility - in practice this should be passed in
    let builder = FilterBuilder::new(Arc::clone(schema));

    // Combine all expressions with AND logic
    let combined_expr = if filters.len() == 1 {
        filters[0].clone()
    } else {
        filters
            .iter()
            .cloned()
            .reduce(|acc, expr| {
                Expr::BinaryExpr(BinaryExpr::new(Box::new(acc), Operator::And, Box::new(expr)))
            })
            .unwrap()
    };

    builder.expr_to_filter(&combined_expr)
}

/// Merge two filters with AND logic, consolidating ranges on the same field
fn merge_and_filters(left: Filter, right: Filter) -> Filter {
    // TODO: Implement range consolidation for same-field ranges
    // For now, simple merge of must conditions
    let mut must_conditions = left.must;
    must_conditions.extend(right.must);

    let mut must_not_conditions = left.must_not;
    must_not_conditions.extend(right.must_not);

    Filter {
        must:       must_conditions,
        must_not:   must_not_conditions,
        should:     vec![], // AND filters don't have should conditions
        min_should: None,
    }
}

/// Merge two filters with OR logic
fn merge_or_filters(left: Filter, right: Filter) -> Filter {
    // Convert each filter to a condition and combine with should
    let mut should_conditions = Vec::new();

    // Convert left filter to condition
    if !left.must.is_empty() || !left.must_not.is_empty() || !left.should.is_empty() {
        should_conditions.push(left.into());
    }

    // Convert right filter to condition
    if !right.must.is_empty() || !right.must_not.is_empty() || !right.should.is_empty() {
        should_conditions.push(right.into());
    }

    Filter::should(should_conditions)
}

/// Legacy function for backward compatibility
pub(crate) fn analyze_filter_expr(expr: &Expr) -> DataFusionResult<FilterResult> {
    // Convert using FilterBuilder and wrap result
    let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
    let builder = FilterBuilder::new(schema);
    match builder.expr_to_filter(expr) {
        Ok(filter) => {
            // Convert Filter back to FilterResult format
            if filter.must.len() == 1 && filter.should.is_empty() && filter.must_not.is_empty() {
                Ok(FilterResult::Condition(Box::new(filter.must[0].clone())))
            } else {
                Ok(FilterResult::Condition(Box::new(filter.into())))
            }
        }
        Err(e) => Ok(FilterResult::Unsupported(e.to_string())),
    }
}

/// Result of analyzing a single filter expression (legacy)
#[derive(Debug)]
pub(crate) enum FilterResult {
    /// Successfully translated to a Qdrant condition
    Condition(Box<Condition>),
    /// Could not be translated, with reason
    Unsupported(String),
}

impl FilterResult {
    /// Create a new [`FilterResult::Condition`] from a [`Condition`].
    pub(crate) fn condition(condition: Condition) -> Self {
        FilterResult::Condition(Box::new(condition))
    }
}

// === Shared helper functions ===

/// Information about a field reference in an expression.
#[derive(Debug, Clone)]
struct FieldInfo {
    /// Type of field (payload field vs ID field)
    field_type: FieldType,
    /// Name of the field
    name:       String,
}

#[derive(Debug, Clone, PartialEq)]
enum FieldType {
    /// Payload field access (payload->field)
    Payload,
    /// ID field access (id = value)
    Id,
}

/// Extract field information from an expression if it represents a field access.
///
/// Returns Some(FieldInfo) for supported field patterns, None otherwise.
fn extract_field_info(expr: &Expr) -> Option<FieldInfo> {
    match expr {
        // Handle aliases that contain JSON functions (from payload->field SQL)
        Expr::Alias(alias) => extract_field_info(&alias.expr),

        // Handle simple column references like "id"
        Expr::Column(column) => {
            if column.name == "id" {
                Some(FieldInfo { field_type: FieldType::Id, name: "id".to_string() })
            } else {
                // Assume payload field access
                Some(FieldInfo { field_type: FieldType::Payload, name: column.name.clone() })
            }
        }

        // Handle JSON access functions (json_get, etc.) - may be wrapped in alias
        Expr::ScalarFunction(func) if is_json_function(func.func.name()) => {
            extract_json_field_access(func)
        }

        _ => None,
    }
}

/// Simple helper to detect json functions
fn is_json_function(func: &str) -> bool {
    // Check if this is a JSON access function
    matches!(
        func,
        "json_get"
            | "json_get_bool"
            | "json_get_float"
            | "json_get_int"
            | "json_get_json"
            | "json_get_str"
            | "json_get_array"
            | "json_as_text"
            | "json_contains"
            | "json_length"
    )
}

/// Extract field information from JSON access functions.
///
/// Handles patterns like `json_get(payload, 'field_name')` which come from `payload->field_name`
/// SQL.
fn extract_json_field_access(func: &ScalarFunction) -> Option<FieldInfo> {
    if !is_json_function(func.func.name()) {
        return None;
    }

    // JSON functions should have at least 2 arguments: (column, field_name, ...)
    if func.args.len() < 2 {
        return None;
    }

    // First argument should be a column reference to "payload"
    let column_refs = func.args[0].column_refs();
    if column_refs.is_empty() {
        return None;
    }
    let column_ref = column_refs.iter().next().unwrap();

    if column_ref.name != "payload" {
        return None; // Only support payload field access for now
    }

    // Second argument
    let field_name = match &func.args[1] {
        Expr::Literal(ScalarValue::Utf8(Some(name)) | ScalarValue::LargeUtf8(Some(name)), _) => {
            name.clone()
        }
        _ => return None,
    };

    Some(FieldInfo { field_type: FieldType::Payload, name: field_name })
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::{BinaryExpr, Operator};
    use datafusion::prelude::*;
    use qdrant_client::qdrant::condition::ConditionOneOf;
    use qdrant_client::qdrant::point_id::PointIdOptions;

    use super::*;

    #[test]
    fn test_id_equals_filter() {
        // Test: id = 'point-123'
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("id")),
            Operator::Eq,
            Box::new(lit("point-123")),
        ));

        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let result = translate_payload_filters(&[expr], &schema).unwrap();

        // Should create Filter::must with one condition
        assert_eq!(result.must.len(), 1);

        let condition = &result.must[0];
        if let Some(ConditionOneOf::HasId(has_id)) = &condition.condition_one_of {
            assert_eq!(has_id.has_id.len(), 1);

            // Check if it's a UUID PointId with correct value
            if let Some(PointIdOptions::Uuid(uuid)) = &has_id.has_id[0].point_id_options {
                assert_eq!(uuid, "point-123");
            } else {
                panic!("Expected UUID PointId, got {:?}", has_id.has_id[0]);
            }
        } else {
            panic!("Expected HasId condition, got {condition:?}");
        }
    }

    #[test]
    fn test_unsupported_expression() {
        // Test truly unsupported expression (Case expression)
        let expr = Expr::Case(datafusion::logical_expr::expr::Case {
            expr:           None,
            when_then_expr: vec![],
            else_expr:      None,
        });

        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let result = translate_payload_filters(&[expr], &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported"));
    }

    #[test]
    fn test_unsupported_operator_on_id() {
        // Test: id > 'point-123' (not supported)
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("id")),
            Operator::Gt,
            Box::new(lit("point-123")),
        ));

        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let result = translate_payload_filters(&[expr], &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported"));
    }

    // TODO: Add comprehensive payload filter tests once we have proper UDF integration
    // For now, the basic ID filtering tests verify the core filter translation logic works
}
