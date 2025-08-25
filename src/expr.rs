//! `DataFusion` `Expr` parsing and serialization

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{BinaryExpr, Operator};
use datafusion::prelude::Expr;
use datafusion::common::ScalarValue;
use qdrant_client::qdrant::{Condition, Filter, Range};

/// Translate `DataFusion` payload filter expressions to Qdrant filter conditions.
///
/// This function analyzes each DataFusion expression individually to identify
/// patterns that can be translated into Qdrant query conditions.
///
/// # Supported Filter Patterns
/// - `payload->field = 'value'` → `Condition::matches`
/// - `payload->field > 10` → `Condition::range`  
/// - `id = 'point_id'` → Point ID filtering
/// - Multiple filters are combined with `Filter::must` (AND logic)
///
/// # Arguments
/// * `filters` - Array of DataFusion expressions to translate
///
/// # Returns
/// A Qdrant `Filter` object containing the translated conditions.
///
/// # Errors
/// Returns an error if any filter expressions cannot be translated.
pub(crate) fn translate_payload_filters(filters: &[Expr]) -> DataFusionResult<Filter> {
    let mut conditions = Vec::new();
    let mut unsupported = Vec::new();
    
    // Analyze each filter expression individually
    for filter in filters {
        match analyze_filter_expr(filter)? {
            FilterResult::Condition(condition) => conditions.push(condition),
            FilterResult::Unsupported(reason) => unsupported.push(reason),
        }
    }
    
    if !unsupported.is_empty() {
        return Err(DataFusionError::NotImplemented(format!(
            "Unsupported filter expressions: {}",
            unsupported.join(", ")
        )));
    }
    
    if conditions.is_empty() {
        return Err(DataFusionError::Internal(
            "No valid payload filter conditions found".to_string(),
        ));
    }
    
    Ok(Filter::must(conditions))
}

/// Result of analyzing a single filter expression.
#[derive(Debug)]
pub enum FilterResult {
    /// Successfully translated to a Qdrant condition
    Condition(Condition),
    /// Could not be translated, with reason
    Unsupported(String),
}

/// Analyze a single DataFusion expression and attempt to translate it to a Qdrant condition.
///
/// This function examines the structure of the expression to identify supported patterns
/// like `payload->field = 'value'` or `id = 'point_id'` and builds the corresponding
/// Qdrant condition.
pub fn analyze_filter_expr(expr: &Expr) -> DataFusionResult<FilterResult> {
    match expr {
        // Handle binary comparison expressions
        Expr::BinaryExpr(binary) => analyze_binary_expr(binary),
        
        // TODO: Handle compound expressions with AND/OR logic
        // TODO: Handle other expression types:
        // - InList for IN conditions  
        // - IsNull/IsNotNull for null checks
        // - Like for pattern matching
        
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
    let BinaryExpr { left, op, right } = binary;
    
    // Try to identify the pattern: <field_expr> <op> <literal>
    match (extract_field_info(left), extract_literal_value(right)) {
        (Some(field_info), Some(literal_value)) => {
            build_condition_from_components(field_info, *op, literal_value)
        }
        // Try reversed pattern: <literal> <op> <field_expr>
        (None, None) => match (extract_literal_value(left), extract_field_info(right)) {
            (Some(literal_value), Some(field_info)) => {
                // Reverse the operator for reversed operands
                let reversed_op = reverse_operator(*op);
                build_condition_from_components(field_info, reversed_op, literal_value)
            }
            _ => Ok(FilterResult::Unsupported(format!("{binary}"))),
        }
        _ => Ok(FilterResult::Unsupported(format!("{binary}"))),
    }
}

/// Information about a field reference in an expression.
#[derive(Debug, Clone)]
struct FieldInfo {
    /// Type of field (payload field vs ID field)
    field_type: FieldType,
    /// Name of the field
    name: String,
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
        // Handle simple column references like "id"
        Expr::Column(column) => {
            if column.name == "id" {
                Some(FieldInfo {
                    field_type: FieldType::Id,
                    name: "id".to_string(),
                })
            } else {
                None // Other column references not supported yet
            }
        }
        
        // Handle JSON access functions (json_get, etc.) - may be wrapped in alias
        Expr::ScalarFunction(_func) => {
            extract_json_field_access(expr)
        }
        
        // Handle aliases that contain JSON functions (from payload->field SQL)
        Expr::Alias(alias) => {
            // The alias name will be something like "payload -> field"
            // but we need to look at the inner expression to extract field info
            extract_field_info(&alias.expr)
        }
        
        _ => None,
    }
}

/// Extract field information from JSON access functions.
///
/// Handles patterns like `json_get(payload, 'field_name')` which come from `payload->field_name` SQL.
fn extract_json_field_access(expr: &Expr) -> Option<FieldInfo> {
    let func = match expr {
        Expr::ScalarFunction(f) => f,
        _ => return None,
    };
    
    // Check if this is a JSON access function
    let func_name = func.func.name();
    if !matches!(func_name, "json_get" | "json_get_str" | "json_get_int" | "json_get_float" | "json_get_bool") {
        return None;
    }
    
    // JSON functions should have at least 2 arguments: (column, field_name, ...)
    if func.args.len() < 2 {
        return None;
    }
    
    // First argument should be a column reference to "payload"
    let column_ref = match &func.args[0] {
        Expr::Column(col) => col,
        _ => return None,
    };
    
    if column_ref.name != "payload" {
        return None; // Only support payload field access for now
    }
    
    // Second argument should be a string literal with the field name
    let field_name = match &func.args[1] {
        Expr::Literal(ScalarValue::Utf8(Some(name)), _) => name.clone(),
        Expr::Literal(ScalarValue::LargeUtf8(Some(name)), _) => name.clone(),
        _ => return None,
    };
    
    Some(FieldInfo {
        field_type: FieldType::Payload,
        name: field_name,
    })
}

/// Extract a literal value from an expression.
///
/// Returns Some(ScalarValue) if the expression is a literal, None otherwise.
fn extract_literal_value(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _metadata) => Some(value.clone()),
        _ => None,
    }
}

/// Build a Qdrant condition from the extracted components.
fn build_condition_from_components(
    field_info: FieldInfo,
    operator: Operator,
    literal: ScalarValue,
) -> DataFusionResult<FilterResult> {
    match field_info.field_type {
        FieldType::Id => build_id_condition(operator, literal),
        FieldType::Payload => build_payload_condition(&field_info.name, operator, literal),
    }
}

/// Build an ID-based condition.
fn build_id_condition(
    operator: Operator,
    literal: ScalarValue,
) -> DataFusionResult<FilterResult> {
    match operator {
        Operator::Eq => {
            let point_id = scalar_to_point_id(literal)?;
            Ok(FilterResult::Condition(
                Condition::has_id([point_id]),
            ))
        }
        _ => Ok(FilterResult::Unsupported(format!(
            "Unsupported ID operator: {operator}"
        ))),
    }
}

/// Build a payload-based condition.
fn build_payload_condition(
    field_name: &str,
    operator: Operator,
    literal: ScalarValue,
) -> DataFusionResult<FilterResult> {
    match operator {
        Operator::Eq => {
            let value = scalar_to_string(literal)?;
            Ok(FilterResult::Condition(
                Condition::matches(field_name.to_string(), value),
            ))
        }
        
        Operator::Gt => {
            let value = scalar_to_f64(literal)?;
            Ok(FilterResult::Condition(
                Condition::range(field_name.to_string(), Range {
                    gt: Some(value),
                    ..Default::default()
                }),
            ))
        }
        
        Operator::GtEq => {
            let value = scalar_to_f64(literal)?;
            Ok(FilterResult::Condition(
                Condition::range(field_name.to_string(), Range {
                    gte: Some(value),
                    ..Default::default()
                }),
            ))
        }
        
        Operator::Lt => {
            let value = scalar_to_f64(literal)?;
            Ok(FilterResult::Condition(
                Condition::range(field_name.to_string(), Range {
                    lt: Some(value),
                    ..Default::default()
                }),
            ))
        }
        
        Operator::LtEq => {
            let value = scalar_to_f64(literal)?;
            Ok(FilterResult::Condition(
                Condition::range(field_name.to_string(), Range {
                    lte: Some(value),
                    ..Default::default()
                }),
            ))
        }
        
        _ => Ok(FilterResult::Unsupported(format!(
            "Unsupported payload operator: {operator}"
        ))),
    }
}

/// Reverse a comparison operator for when operands are swapped.
fn reverse_operator(op: Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        // Commutative operators stay the same
        other => other,
    }
}

/// Convert a ScalarValue to a string for Qdrant conditions.
fn scalar_to_string(value: ScalarValue) -> DataFusionResult<String> {
    match value {
        ScalarValue::Utf8(Some(s)) => Ok(s),
        ScalarValue::LargeUtf8(Some(s)) => Ok(s),
        ScalarValue::Int32(Some(i)) => Ok(i.to_string()),
        ScalarValue::Int64(Some(i)) => Ok(i.to_string()),
        ScalarValue::Float32(Some(f)) => Ok(f.to_string()),
        ScalarValue::Float64(Some(f)) => Ok(f.to_string()),
        ScalarValue::Boolean(Some(b)) => Ok(b.to_string()),
        _ => Err(DataFusionError::Internal(format!(
            "Cannot convert ScalarValue to string: {value:?}"
        ))),
    }
}

/// Convert a ScalarValue to f64 for numeric range conditions.
fn scalar_to_f64(value: ScalarValue) -> DataFusionResult<f64> {
    match value {
        ScalarValue::Float64(Some(f)) => Ok(f),
        ScalarValue::Float32(Some(f)) => Ok(f as f64),
        ScalarValue::Int32(Some(i)) => Ok(i as f64),
        ScalarValue::Int64(Some(i)) => Ok(i as f64),
        ScalarValue::UInt32(Some(i)) => Ok(i as f64),
        ScalarValue::UInt64(Some(i)) => Ok(i as f64),
        _ => Err(DataFusionError::Internal(format!(
            "Cannot convert ScalarValue to f64: {value:?}"
        ))),
    }
}

/// Convert a ScalarValue to a PointId for Qdrant ID conditions.
fn scalar_to_point_id(value: ScalarValue) -> DataFusionResult<qdrant_client::qdrant::PointId> {
    use qdrant_client::qdrant::point_id::PointIdOptions;
    use qdrant_client::qdrant::PointId;
    
    match value {
        // Numeric IDs
        ScalarValue::Int32(Some(i)) => Ok(PointId {
            point_id_options: Some(PointIdOptions::Num(i as u64)),
        }),
        ScalarValue::Int64(Some(i)) => Ok(PointId {
            point_id_options: Some(PointIdOptions::Num(i as u64)),
        }),
        ScalarValue::UInt32(Some(i)) => Ok(PointId {
            point_id_options: Some(PointIdOptions::Num(i as u64)),
        }),
        ScalarValue::UInt64(Some(i)) => Ok(PointId {
            point_id_options: Some(PointIdOptions::Num(i)),
        }),
        
        // String IDs (treated as UUIDs)
        ScalarValue::Utf8(Some(s)) => Ok(PointId {
            point_id_options: Some(PointIdOptions::Uuid(s)),
        }),
        ScalarValue::LargeUtf8(Some(s)) => Ok(PointId {
            point_id_options: Some(PointIdOptions::Uuid(s)),
        }),
        
        _ => Err(DataFusionError::Internal(format!(
            "Cannot convert ScalarValue to PointId: {value:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{BinaryExpr, Operator};
    use datafusion::prelude::*;
    use qdrant_client::qdrant::condition::ConditionOneOf;

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
            use qdrant_client::qdrant::point_id::PointIdOptions;
            if let Some(PointIdOptions::Uuid(uuid)) = &has_id.has_id[0].point_id_options {
                assert_eq!(uuid, "point-123");
            } else {
                panic!("Expected UUID PointId, got {:?}", has_id.has_id[0]);
            }
        } else {
            panic!("Expected HasId condition, got {:?}", condition);
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