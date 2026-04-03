use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use datafusion::common::ScalarValue;
use prost_types::Timestamp;
use qdrant_client::qdrant::PointId;

pub(super) fn point_id_scalar(value: &ScalarValue) -> Option<PointId> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => value
            .parse::<u64>()
            .ok()
            .map(PointId::from)
            .or_else(|| Some(PointId::from(value.clone()))),
        ScalarValue::UInt64(Some(value)) => Some(PointId::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(PointId::from(u64::from(*value))),
        ScalarValue::UInt16(Some(value)) => Some(PointId::from(u64::from(*value))),
        ScalarValue::UInt8(Some(value)) => Some(PointId::from(u64::from(*value))),
        ScalarValue::Int64(Some(value)) => u64::try_from(*value).ok().map(PointId::from),
        ScalarValue::Int32(Some(value)) => u64::try_from(*value).ok().map(PointId::from),
        ScalarValue::Int16(Some(value)) => u64::try_from(*value).ok().map(PointId::from),
        ScalarValue::Int8(Some(value)) => u64::try_from(*value).ok().map(PointId::from),
        _ => None,
    }
}

pub(crate) fn string_scalar(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

pub(crate) fn integer_scalar(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(Some(value)) => Some(*value),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value).ok(),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => value.parse().ok(),
        _ => None,
    }
}

pub(crate) fn float_scalar(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float64(Some(value)) => Some(*value),
        ScalarValue::Float32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => value.parse().ok(),
        _ => integer_scalar(value).map(integer_to_f64),
    }
}

pub(crate) fn boolean_scalar(value: &ScalarValue) -> Option<bool> {
    match value {
        ScalarValue::Boolean(Some(value)) => Some(*value),
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => value.parse().ok(),
        _ => None,
    }
}

pub(crate) fn timestamp_scalar(value: &ScalarValue) -> Option<Timestamp> {
    match value {
        ScalarValue::TimestampSecond(Some(value), _) => Some(timestamp_from_scaled(*value, 1)),
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            Some(timestamp_from_scaled(*value, 1_000))
        }
        ScalarValue::TimestampMicrosecond(Some(value), _) => {
            Some(timestamp_from_scaled(*value, 1_000_000))
        }
        ScalarValue::TimestampNanosecond(Some(value), _) => {
            Some(timestamp_from_scaled(*value, 1_000_000_000))
        }
        ScalarValue::Date64(Some(value)) => Some(timestamp_from_scaled(*value, 1_000)),
        ScalarValue::Date32(Some(value)) => {
            Some(timestamp_from_scaled(i64::from(*value) * 86_400, 1))
        }
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            timestamp_from_string(value)
        }
        _ => None,
    }
}

fn timestamp_from_scaled(value: i64, scale: i64) -> Timestamp {
    let seconds = value.div_euclid(scale);
    let nanos = value.rem_euclid(scale) * (1_000_000_000 / scale);
    Timestamp { seconds, nanos: i32::try_from(nanos).expect("nanos fit in i32") }
}

fn nanos_i32(nanos: u32) -> i32 { i32::try_from(nanos).expect("nanos fit in i32") }

fn timestamp_from_string(value: &str) -> Option<Timestamp> {
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        let value = value.with_timezone(&Utc);
        return Some(Timestamp {
            seconds: value.timestamp(),
            nanos:   nanos_i32(value.timestamp_subsec_nanos()),
        });
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(Timestamp {
            seconds: value.and_utc().timestamp(),
            nanos:   nanos_i32(value.and_utc().timestamp_subsec_nanos()),
        });
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(Timestamp {
            seconds: value.and_utc().timestamp(),
            nanos:   nanos_i32(value.and_utc().timestamp_subsec_nanos()),
        });
    }
    let value = NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()?;
    let value = value.and_hms_opt(0, 0, 0)?;
    Some(Timestamp { seconds: value.and_utc().timestamp(), nanos: 0 })
}

#[expect(clippy::cast_precision_loss)]
fn integer_to_f64(value: i64) -> f64 { value as f64 }
