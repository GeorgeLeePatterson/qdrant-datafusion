use std::any::Any;
use std::sync::OnceLock;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use datafusion::arrow::array::{Array, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::expr::Cast;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::lit;

pub(crate) const PAYLOAD_TEXT_ACCESS_FUNCTION_NAME: &str = "__qdrant_payload_text_access";
pub(crate) const PAYLOAD_INT_ACCESS_FUNCTION_NAME: &str = "__qdrant_payload_int_access";
pub(crate) const PAYLOAD_FLOAT_ACCESS_FUNCTION_NAME: &str = "__qdrant_payload_float_access";
pub(crate) const PAYLOAD_BOOL_ACCESS_FUNCTION_NAME: &str = "__qdrant_payload_bool_access";
pub(crate) const PAYLOAD_DATETIME_ACCESS_FUNCTION_NAME: &str = "__qdrant_payload_datetime_access";

pub(crate) fn is_payload_access_function_name(name: &str) -> bool {
    matches!(
        name,
        PAYLOAD_TEXT_ACCESS_FUNCTION_NAME
            | PAYLOAD_INT_ACCESS_FUNCTION_NAME
            | PAYLOAD_FLOAT_ACCESS_FUNCTION_NAME
            | PAYLOAD_BOOL_ACCESS_FUNCTION_NAME
            | PAYLOAD_DATETIME_ACCESS_FUNCTION_NAME
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PayloadAccessKind {
    Text,
    Int64,
    Float64,
    Bool,
    Datetime,
}

impl PayloadAccessKind {
    fn return_type(self) -> DataType {
        match self {
            Self::Text => DataType::Utf8,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::Bool => DataType::Boolean,
            Self::Datetime => DataType::Timestamp(TimeUnit::Millisecond, None),
        }
    }

    fn function_name(self) -> &'static str {
        match self {
            Self::Text => PAYLOAD_TEXT_ACCESS_FUNCTION_NAME,
            Self::Int64 => PAYLOAD_INT_ACCESS_FUNCTION_NAME,
            Self::Float64 => PAYLOAD_FLOAT_ACCESS_FUNCTION_NAME,
            Self::Bool => PAYLOAD_BOOL_ACCESS_FUNCTION_NAME,
            Self::Datetime => PAYLOAD_DATETIME_ACCESS_FUNCTION_NAME,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PayloadAccessUdf {
    kind: PayloadAccessKind,
    signature: Signature,
}

impl PayloadAccessUdf {
    fn new(kind: PayloadAccessKind) -> Self {
        Self { kind, signature: Signature::variadic_any(Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for PayloadAccessUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.kind.function_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.kind.return_type())
    }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(std::sync::Arc::new(Field::new(self.name(), self.kind.return_type(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("{} requires a payload JSON value and a payload path", self.name());
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let payloads = &arrays[0];
        let paths = &arrays[1];
        let mut values = Vec::with_capacity(payloads.len());
        for index in 0..payloads.len() {
            values.push(payload_scalar(
                self.kind,
                array_string_value(payloads, index, "payload")?.as_deref(),
                array_string_value(paths, index, "payload path")?.as_deref(),
            )?);
        }
        Ok(ColumnarValue::Array(ScalarValue::iter_to_array(values)?))
    }
}

pub(crate) fn qdrant_payload_text_access(payload: Expr, path: impl Into<String>) -> Expr {
    qdrant_payload_text_access_udf().call(vec![payload, lit(path.into())])
}

pub(crate) fn qdrant_payload_int_access(payload: Expr, path: impl Into<String>) -> Expr {
    qdrant_payload_int_access_udf().call(vec![payload, lit(path.into())])
}

pub(crate) fn qdrant_payload_float_access(payload: Expr, path: impl Into<String>) -> Expr {
    qdrant_payload_float_access_udf().call(vec![payload, lit(path.into())])
}

pub(crate) fn qdrant_payload_bool_access(payload: Expr, path: impl Into<String>) -> Expr {
    qdrant_payload_bool_access_udf().call(vec![payload, lit(path.into())])
}

pub(crate) fn qdrant_payload_datetime_access(payload: Expr, path: impl Into<String>) -> Expr {
    qdrant_payload_datetime_access_udf().call(vec![payload, lit(path.into())])
}

pub(crate) fn qdrant_payload_text_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadAccessUdf::new(PayloadAccessKind::Text)))
        .clone()
}

pub(crate) fn qdrant_payload_int_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadAccessUdf::new(PayloadAccessKind::Int64)))
        .clone()
}

pub(crate) fn qdrant_payload_float_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadAccessUdf::new(PayloadAccessKind::Float64)))
        .clone()
}

pub(crate) fn qdrant_payload_bool_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadAccessUdf::new(PayloadAccessKind::Bool)))
        .clone()
}

pub(crate) fn qdrant_payload_datetime_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadAccessUdf::new(PayloadAccessKind::Datetime)))
        .clone()
}

pub(crate) fn payload_access_expr(
    payload: Expr,
    path: impl Into<String>,
    data_type: &DataType,
) -> Option<Expr> {
    let path = path.into();
    match data_type {
        DataType::Utf8 => Some(qdrant_payload_text_access(payload, path)),
        DataType::LargeUtf8 => Some(Expr::Cast(Cast::new(
            Box::new(qdrant_payload_text_access(payload, path)),
            DataType::LargeUtf8,
        ))),
        DataType::Boolean => Some(qdrant_payload_bool_access(payload, path)),
        DataType::Int64 => Some(qdrant_payload_int_access(payload, path)),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Some(Expr::Cast(Cast::new(
            Box::new(qdrant_payload_int_access(payload, path)),
            data_type.clone(),
        ))),
        DataType::Float64 => Some(qdrant_payload_float_access(payload, path)),
        DataType::Float32 => Some(Expr::Cast(Cast::new(
            Box::new(qdrant_payload_float_access(payload, path)),
            DataType::Float32,
        ))),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            Some(qdrant_payload_datetime_access(payload, path))
        }
        DataType::Timestamp(_, _) => Some(Expr::Cast(Cast::new(
            Box::new(qdrant_payload_datetime_access(payload, path)),
            data_type.clone(),
        ))),
        _ => None,
    }
}

fn array_string_value(
    array: &datafusion::arrow::array::ArrayRef,
    index: usize,
    argument: &str,
) -> Result<Option<String>> {
    match array.data_type() {
        DataType::Utf8 => Ok(array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 array")
            .is_valid(index)
            .then(|| {
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 array")
                    .value(index)
                    .to_owned()
            })),
        DataType::LargeUtf8 => Ok(array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("large utf8 array")
            .is_valid(index)
            .then(|| {
                array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("large utf8 array")
                    .value(index)
                    .to_owned()
            })),
        data_type => exec_err!(
            "{} accessor requires Utf8 or LargeUtf8 {}, found {}",
            PAYLOAD_TEXT_ACCESS_FUNCTION_NAME,
            argument,
            data_type
        ),
    }
}

fn payload_scalar(
    kind: PayloadAccessKind,
    payload_json: Option<&str>,
    path: Option<&str>,
) -> Result<ScalarValue> {
    let Some(payload_json) = payload_json else {
        return ScalarValue::try_new_null(&kind.return_type());
    };
    let Some(path) = path else {
        return ScalarValue::try_new_null(&kind.return_type());
    };
    let payload = serde_json::from_str::<serde_json::Value>(payload_json).map_err(|error| {
        datafusion::common::DataFusionError::Execution(format!(
            "{} could not parse payload JSON: {error}",
            kind.function_name()
        ))
    })?;
    let Some(value) = payload_value_at_path(&payload, path) else {
        return ScalarValue::try_new_null(&kind.return_type());
    };
    match kind {
        PayloadAccessKind::Text => Ok(ScalarValue::Utf8(Some(payload_text_value(value)))),
        PayloadAccessKind::Int64 => value.as_i64().map_or_else(
            || {
                exec_err!(
                    "{} expected integer payload value at '{}', found {}",
                    kind.function_name(),
                    path,
                    payload_kind(value)
                )
            },
            |value| Ok(ScalarValue::Int64(Some(value))),
        ),
        PayloadAccessKind::Float64 => value.as_f64().map_or_else(
            || {
                exec_err!(
                    "{} expected numeric payload value at '{}', found {}",
                    kind.function_name(),
                    path,
                    payload_kind(value)
                )
            },
            |value| Ok(ScalarValue::Float64(Some(value))),
        ),
        PayloadAccessKind::Bool => value.as_bool().map_or_else(
            || {
                exec_err!(
                    "{} expected bool payload value at '{}', found {}",
                    kind.function_name(),
                    path,
                    payload_kind(value)
                )
            },
            |value| Ok(ScalarValue::Boolean(Some(value))),
        ),
        PayloadAccessKind::Datetime => payload_datetime_value(value, path),
    }
}

fn payload_datetime_value(value: &serde_json::Value, path: &str) -> Result<ScalarValue> {
    let serde_json::Value::String(value) = value else {
        return exec_err!(
            "{} expected datetime payload value at '{}', found {}",
            PAYLOAD_DATETIME_ACCESS_FUNCTION_NAME,
            path,
            payload_kind(value)
        );
    };
    let Some(millis) = timestamp_millis_from_string(value) else {
        return exec_err!(
            "{} could not parse datetime payload value '{}' at '{}'",
            PAYLOAD_DATETIME_ACCESS_FUNCTION_NAME,
            value,
            path
        );
    };
    Ok(ScalarValue::TimestampMillisecond(Some(millis), None))
}

fn timestamp_millis_from_string(value: &str) -> Option<i64> {
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return Some(value.with_timezone(&Utc).timestamp_millis());
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(value.and_utc().timestamp_millis());
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(value.and_utc().timestamp_millis());
    }
    let value = NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()?;
    Some(value.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis())
}

fn payload_value_at_path<'a>(
    payload: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    let mut segments = path.split('.');
    let mut current = payload.get(segments.next()?)?;
    for segment in segments {
        current = current.get(segment)?;
    }
    Some(current)
}

fn payload_text_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(string) => string.clone(),
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::Bool(boolean) => boolean.to_string(),
        serde_json::Value::Null => "null".to_owned(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => value.to_string(),
    }
}

fn payload_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        serde_json::Value::Number(_) => "double",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_scalar_extracts_integer_values() {
        let scalar = payload_scalar(PayloadAccessKind::Int64, Some(r#"{"rank": 7}"#), Some("rank"))
            .expect("payload int");

        assert_eq!(scalar, ScalarValue::Int64(Some(7)));
    }

    #[test]
    fn payload_scalar_returns_null_for_missing_path() {
        let scalar =
            payload_scalar(PayloadAccessKind::Text, Some(r#"{"rank": 7}"#), Some("missing"))
                .expect("payload null");

        assert_eq!(scalar, ScalarValue::Utf8(None));
    }

    #[test]
    fn payload_scalar_extracts_datetime_values() {
        let scalar = payload_scalar(
            PayloadAccessKind::Datetime,
            Some(r#"{"created_at": "2024-01-02T03:04:05Z"}"#),
            Some("created_at"),
        )
        .expect("payload datetime");

        assert_eq!(scalar, ScalarValue::TimestampMillisecond(Some(1_704_164_645_000), None));
    }
}
