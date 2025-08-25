//! Utilities for working with `DataFusion` Exprs and Qdrant data.

use datafusion::common::plan_err;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Operator;
use datafusion::scalar::ScalarValue;
use qdrant_client::qdrant::PointId;

/// Reverse a comparison operator for when operands are swapped.
pub(crate) fn reverse_operator(op: Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        // Commutative operators stay the same
        other => other,
    }
}

/// Convert a `ScalarValue` to a string for Qdrant conditions.
pub(crate) fn scalar_to_string(value: ScalarValue) -> DataFusionResult<String> {
    match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(s),
        ScalarValue::Int32(Some(i)) => Ok(i.to_string()),
        ScalarValue::Int64(Some(i)) => Ok(i.to_string()),
        ScalarValue::Float32(Some(f)) => Ok(f.to_string()),
        ScalarValue::Float64(Some(f)) => Ok(f.to_string()),
        ScalarValue::Boolean(Some(b)) => Ok(b.to_string()),
        _ => plan_err!("Cannot convert ScalarValue to string: {value:?}"),
    }
}

/// Convert a `ScalarValue` to f64 for numeric range conditions.
#[expect(clippy::cast_precision_loss)]
pub(crate) fn scalar_to_f64(value: &ScalarValue) -> DataFusionResult<f64> {
    match value {
        ScalarValue::Float64(Some(f)) => Ok(*f),
        ScalarValue::Float32(Some(f)) => Ok(f64::from(*f)),
        ScalarValue::Int32(Some(i)) => Ok(f64::from(*i)),
        ScalarValue::Int64(Some(i)) => Ok(*i as f64),
        ScalarValue::UInt32(Some(i)) => Ok(f64::from(*i)),
        ScalarValue::UInt64(Some(i)) => Ok(*i as f64),
        _ => plan_err!("Cannot convert ScalarValue to f64: {value:?}"),
    }
}

/// Convert a `ScalarValue` to a `PointId` for Qdrant ID conditions.
#[expect(clippy::cast_sign_loss)]
pub(crate) fn scalar_to_point_id(value: ScalarValue) -> DataFusionResult<PointId> {
    let point_id = match value {
        // Numeric IDs - use qdrant-rust-client's From<u64> implementation
        ScalarValue::Int32(Some(i)) if i >= 0 => (i as u64).into(),
        ScalarValue::Int64(Some(i)) if i >= 0 => (i as u64).into(),
        ScalarValue::UInt32(Some(i)) => u64::from(i).into(),
        ScalarValue::UInt64(Some(i)) => i.into(),
        // String IDs - try parsing as number first, then fall back to UUID
        ScalarValue::Utf8(Some(s)) => {
            // First try parsing as a u64 (numeric ID)
            if let Ok(num_id) = s.parse::<u64>() { num_id.into() } else { s.into() }
        }
        ScalarValue::LargeUtf8(Some(s)) => {
            // Same logic for LargeUtf8
            if let Ok(num_id) = s.parse::<u64>() { num_id.into() } else { s.into() }
        }
        _ => return plan_err!("Cannot convert ScalarValue to PointId: {value:?}"),
    };

    Ok(point_id)
}
