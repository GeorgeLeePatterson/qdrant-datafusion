//! `Expr` analysis for payload filters

use datafusion::common::{not_impl_err, plan_err};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{BinaryExpr, Operator};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use qdrant_client::qdrant::{Condition, Filter, Range};

use super::utils;

/// Result of analyzing a single filter expression.
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

/// Translate `DataFusion` payload filter expressions to Qdrant filter conditions.
///
/// This function analyzes each `DataFusion` expression individually to identify
/// patterns that can be translated into Qdrant query conditions.
///
/// # Supported Filter Patterns
/// - `payload->field = 'value'` → `Condition::matches`
/// - `payload->field > 10` → `Condition::range`
/// - `id = 'point_id'` → Point ID filtering
/// - Multiple filters are combined with `Filter::must` (AND logic)
///
/// # Arguments
/// * `filters` - Array of `DataFusion` expressions to translate
///
/// # Returns
/// A Qdrant `Filter` object containing the translated conditions.
///
/// # Errors
/// Returns an error if any filter expressions cannot be translated.
pub(crate) fn translate_payload_filters(filters: &[Expr]) -> DataFusionResult<Filter> {
    let mut conditions = Vec::with_capacity(filters.len());
    let mut unsupported = Vec::new();

    // Analyze each filter expression individually
    for filter in filters {
        match analyze_filter_expr(filter)? {
            FilterResult::Condition(condition) => conditions.push(*condition),
            FilterResult::Unsupported(reason) => unsupported.push(reason),
        }
    }

    if !unsupported.is_empty() {
        return not_impl_err!("Unsupported filter expressions: {}", unsupported.join(", "));
    }

    if conditions.is_empty() {
        return plan_err!("No valid payload filter conditions found");
    }

    Ok(Filter::must(conditions))
}

/// Analyze a single `DataFusion` expression and attempt to translate it to a Qdrant condition.
///
/// This function examines the structure of the expression to identify supported patterns
/// like `payload->field = 'value'` or `id = 'point_id'` and builds the corresponding
/// Qdrant condition.
///
/// # Errors
/// - Returns an error if the expression cannot be translated to a Qdrant condition.
pub(crate) fn analyze_filter_expr(expr: &Expr) -> DataFusionResult<FilterResult> {
    match expr {
        Expr::Alias(alias) => analyze_filter_expr(&alias.expr),

        // Handle binary comparison expressions
        Expr::BinaryExpr(binary) => analyze_binary_expr(binary),

        // TODO: Handle compound expressions with AND/OR logic
        // TODO: Handle other expression types:
        // - InList for IN conditions
        // - IsNull/IsNotNull for null checks
        // - Like for pattern matching
        // - Handle NOT

        // Everything else is unsupported for now
        _ => Ok(FilterResult::Unsupported(format!("{expr}"))),
    }
}

/// Analyze a binary expression to extract filter components.
///
/// Supported patterns:
/// - `payload->field = 'value'` → `Condition::matches`
/// - `payload->field > 10` → `Condition::range`
/// - `id = 'point_id'` → Point ID condition
fn analyze_binary_expr(binary: &BinaryExpr) -> DataFusionResult<FilterResult> {
    fn analyze_binary_impl(
        l: &Expr,
        o: Operator,
        r: &Expr,
    ) -> DataFusionResult<Option<FilterResult>> {
        match (extract_field_info(l), r.as_literal()) {
            (Some(field), Some(value)) => {
                match field.field_type {
                    // Build an ID-based condition.
                    FieldType::Id => build_id_condition(o, value).map(Some),
                    FieldType::Payload => build_payload_condition(field.name, o, value).map(Some),
                }
            }
            _ => Ok(None),
        }
    }

    let BinaryExpr { left, op, right } = binary;

    // Try to identify the pattern: <field_expr> <op> <literal>
    if let Some(result) = analyze_binary_impl(left, *op, right)? {
        return Ok(result);
    }

    // Try reversed pattern: <literal> <op> <field_expr>
    // Reverse the operator for reversed operands
    let reversed_op = utils::reverse_operator(*op);
    if let Some(result) = analyze_binary_impl(right, reversed_op, left)? {
        return Ok(result);
    }

    Ok(FilterResult::Unsupported(format!("{binary}")))
}

/// Build a id-based condition.
fn build_id_condition(op: Operator, literal: &ScalarValue) -> DataFusionResult<FilterResult> {
    match op {
        Operator::Eq => {
            Ok(FilterResult::condition(Condition::has_id(utils::scalar_to_point_ids(literal)?)))
        }
        _ => Ok(FilterResult::Unsupported(format!("Unsupported ID operator: {op}"))),
    }
}

/// Build a payload-based condition.
fn build_payload_condition(
    field: impl Into<String>,
    operator: Operator,
    value: &ScalarValue,
) -> DataFusionResult<FilterResult> {
    use utils::{scalar_to_f64, scalar_to_string};

    Ok(match operator {
        Operator::Eq => {
            FilterResult::condition(Condition::matches(field.into(), scalar_to_string(value)?))
        }
        Operator::Gt => FilterResult::condition(Condition::range(field.into(), Range {
            gt: Some(scalar_to_f64(value)?),
            ..Default::default()
        })),
        Operator::GtEq => FilterResult::condition(Condition::range(field.into(), Range {
            gte: Some(scalar_to_f64(value)?),
            ..Default::default()
        })),
        Operator::Lt => FilterResult::condition(Condition::range(field.into(), Range {
            lt: Some(scalar_to_f64(value)?),
            ..Default::default()
        })),
        Operator::LtEq => FilterResult::condition(Condition::range(field.into(), Range {
            lte: Some(scalar_to_f64(value)?),
            ..Default::default()
        })),
        _ => FilterResult::Unsupported(format!("Unsupported payload operator: {operator}")),
    })
}

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

        let result = translate_payload_filters(&[expr]).unwrap();

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
        // Test unsupported expression
        let expr = Expr::IsNull(Box::new(col("some_field")));

        let result = translate_payload_filters(&[expr]);
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

        let result = translate_payload_filters(&[expr]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported"));
    }

    // TODO: Add comprehensive payload filter tests once we have proper UDF integration
    // For now, the basic ID filtering tests verify the core filter translation logic works
}
