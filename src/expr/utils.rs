//! Utilities for working with `DataFusion` Exprs and Qdrant data.

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
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
pub(crate) fn scalar_to_string(value: &ScalarValue) -> DataFusionResult<String> {
    match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(s.to_string()),
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

/// Convert possibly multiple `ScalarValue`s to a `PointId`s for Qdrant ID conditions.
pub(crate) fn scalar_to_point_ids(value: &ScalarValue) -> DataFusionResult<Vec<PointId>> {
    Ok(match value {
        ScalarValue::List(inner) => array_to_point_ids(inner.values())?,
        ScalarValue::LargeList(inner) => array_to_point_ids(inner.values())?,
        ScalarValue::FixedSizeList(inner) => array_to_point_ids(inner.values())?,
        _ => vec![scalar_to_point_id(value)?],
    })
}

/// Convert individual `ScalarValue` to a `PointId` for Qdrant ID conditions.
#[expect(clippy::cast_sign_loss)]
pub(crate) fn scalar_to_point_id(value: &ScalarValue) -> DataFusionResult<PointId> {
    Ok(match value {
        // Numeric IDs - use qdrant-rust-client's From<u64> implementation
        ScalarValue::Int8(Some(i)) if *i >= 0 => (*i as u64).into(),
        ScalarValue::Int16(Some(i)) if *i >= 0 => (*i as u64).into(),
        ScalarValue::Int32(Some(i)) if *i >= 0 => (*i as u64).into(),
        ScalarValue::Int64(Some(i)) if *i >= 0 => (*i as u64).into(),
        ScalarValue::UInt8(Some(i)) => u64::from(*i).into(),
        ScalarValue::UInt16(Some(i)) => u64::from(*i).into(),
        ScalarValue::UInt32(Some(i)) => u64::from(*i).into(),
        ScalarValue::UInt64(Some(i)) => (*i).into(),
        // String IDs - try parsing as number first, then fall back to UUID
        ScalarValue::Utf8(Some(s))
        | ScalarValue::Utf8View(Some(s))
        | ScalarValue::LargeUtf8(Some(s)) => {
            // First try parsing as a u64 (numeric ID)
            if let Ok(num_id) = s.parse::<u64>() { num_id.into() } else { s.as_str().into() }
        }
        _ => return plan_err!("Cannot convert ScalarValue to PointId: {value:?}"),
    })
}

/// Convert Arrow List Array values to a `PointId` for Qdrant ID conditions.
#[expect(clippy::cast_sign_loss)]
pub(crate) fn array_to_point_ids(array: &ArrayRef) -> DataFusionResult<Vec<PointId>> {
    use datafusion::arrow::compute::cast;

    macro_rules! convert_array {
        ($array:expr, $dt:ty => $e:expr) => {
            if let Some(a) = array.as_primitive_opt::<$dt>() {
                return Ok(a.iter().filter_map(|v| v.filter(|val| *val > 0)).map($e).collect());
            }
        };
    }

    convert_array!(array, Int8Type => |v| PointId::from(v as u64));
    convert_array!(array, Int16Type => |v| PointId::from(v as u64));
    convert_array!(array, Int32Type => |v| PointId::from(v as u64));
    convert_array!(array, Int64Type => |v| PointId::from(v as u64));
    convert_array!(array, UInt8Type => |v| PointId::from(u64::from(v)));
    convert_array!(array, UInt16Type => |v| PointId::from(u64::from(v)));
    convert_array!(array, UInt32Type => |v| PointId::from(u64::from(v)));
    convert_array!(array, UInt64Type => PointId::from);

    if let Ok(a) = cast(array, &DataType::Utf8)
        .or(cast(array, &DataType::Utf8View))
        .or(cast(array, &DataType::LargeUtf8))
        && let Some(string_array) = a.as_string_opt::<i32>()
    {
        return Ok(string_array
            .iter()
            .flatten()
            .map(|s| {
                if let Ok(num_id) = s.parse::<u64>() {
                    PointId::from(num_id)
                } else {
                    PointId::from(s)
                }
            })
            .collect());
    }

    plan_err!("Cannot convert ListArray to PointId")
}
