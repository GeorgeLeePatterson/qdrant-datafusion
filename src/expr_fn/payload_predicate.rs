use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::lit;

use super::payload_access::{array_string_value, payload_json_value};
use crate::qdrant::QdrantPayloadAccess;

pub const PAYLOAD_EXISTS_FUNCTION_NAME: &str = "payload_exists";
pub const PAYLOAD_IS_EMPTY_FUNCTION_NAME: &str = "payload_is_empty";
pub const PAYLOAD_VALUES_COUNT_FUNCTION_NAME: &str = "payload_values_count";
pub(crate) const PAYLOAD_EXISTS_ACCESS_FUNCTION_NAME: &str = "__qdrant_payload_exists_access";
pub(crate) const PAYLOAD_IS_EMPTY_ACCESS_FUNCTION_NAME: &str = "__qdrant_payload_is_empty_access";
pub(crate) const PAYLOAD_VALUES_COUNT_ACCESS_FUNCTION_NAME: &str =
    "__qdrant_payload_values_count_access";

const PAYLOAD_EXISTS_ALIASES: &[&str] = &["qdrant_payload_exists"];
const PAYLOAD_IS_EMPTY_ALIASES: &[&str] = &["qdrant_payload_is_empty"];
const PAYLOAD_VALUES_COUNT_ALIASES: &[&str] = &["qdrant_payload_values_count"];

pub(crate) fn is_payload_exists_function_name(name: &str) -> bool {
    name == PAYLOAD_EXISTS_FUNCTION_NAME || PAYLOAD_EXISTS_ALIASES.contains(&name)
}

pub(crate) fn is_payload_is_empty_function_name(name: &str) -> bool {
    name == PAYLOAD_IS_EMPTY_FUNCTION_NAME || PAYLOAD_IS_EMPTY_ALIASES.contains(&name)
}

pub(crate) fn is_payload_values_count_function_name(name: &str) -> bool {
    name == PAYLOAD_VALUES_COUNT_FUNCTION_NAME || PAYLOAD_VALUES_COUNT_ALIASES.contains(&name)
}

#[must_use]
pub fn qdrant_payload_exists(accessor: Expr) -> Expr {
    qdrant_payload_exists_udf().call(vec![accessor])
}

#[must_use]
pub fn qdrant_payload_is_empty(accessor: Expr) -> Expr {
    qdrant_payload_is_empty_udf().call(vec![accessor])
}

#[must_use]
pub fn qdrant_payload_values_count(accessor: Expr) -> Expr {
    qdrant_payload_values_count_udf().call(vec![accessor])
}

pub(crate) fn qdrant_payload_exists_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadPredicateUdf::exists())).clone()
}

pub(crate) fn qdrant_payload_is_empty_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadPredicateUdf::is_empty())).clone()
}

pub(crate) fn qdrant_payload_values_count_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadPredicateUdf::values_count())).clone()
}

pub(crate) fn qdrant_payload_exists_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(PayloadPredicateAccessUdf::new(PayloadPredicateKind::Exists))
    })
    .clone()
}

pub(crate) fn qdrant_payload_is_empty_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(PayloadPredicateAccessUdf::new(PayloadPredicateKind::IsEmpty))
    })
    .clone()
}

pub(crate) fn qdrant_payload_values_count_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(PayloadPredicateAccessUdf::new(PayloadPredicateKind::ValuesCount))
    })
    .clone()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PayloadPredicateKind {
    Exists,
    IsEmpty,
    ValuesCount,
}

impl PayloadPredicateKind {
    fn function_name(self) -> &'static str {
        match self {
            Self::Exists => PAYLOAD_EXISTS_FUNCTION_NAME,
            Self::IsEmpty => PAYLOAD_IS_EMPTY_FUNCTION_NAME,
            Self::ValuesCount => PAYLOAD_VALUES_COUNT_FUNCTION_NAME,
        }
    }

    fn aliases(self) -> &'static [&'static str] {
        match self {
            Self::Exists => PAYLOAD_EXISTS_ALIASES,
            Self::IsEmpty => PAYLOAD_IS_EMPTY_ALIASES,
            Self::ValuesCount => PAYLOAD_VALUES_COUNT_ALIASES,
        }
    }

    fn internal_function_name(self) -> &'static str {
        match self {
            Self::Exists => PAYLOAD_EXISTS_ACCESS_FUNCTION_NAME,
            Self::IsEmpty => PAYLOAD_IS_EMPTY_ACCESS_FUNCTION_NAME,
            Self::ValuesCount => PAYLOAD_VALUES_COUNT_ACCESS_FUNCTION_NAME,
        }
    }

    fn return_type(self) -> DataType {
        match self {
            Self::Exists | Self::IsEmpty => DataType::Boolean,
            Self::ValuesCount => DataType::Int64,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PayloadPredicateUdf {
    kind:      PayloadPredicateKind,
    aliases:   Vec<String>,
    signature: Signature,
}

impl PayloadPredicateUdf {
    fn new(kind: PayloadPredicateKind) -> Self {
        Self {
            kind,
            aliases: kind.aliases().iter().map(|alias| (*alias).to_owned()).collect(),
            signature: Signature::any(1, Volatility::Immutable)
                .with_parameter_names(vec!["accessor"])
                .expect("payload predicate signature should accept one named parameter"),
        }
    }

    fn exists() -> Self { Self::new(PayloadPredicateKind::Exists) }

    fn is_empty() -> Self { Self::new(PayloadPredicateKind::IsEmpty) }

    fn values_count() -> Self { Self::new(PayloadPredicateKind::ValuesCount) }
}

impl ScalarUDFImpl for PayloadPredicateUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { self.kind.function_name() }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.kind.return_type())
    }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), self.kind.return_type(), true)))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [accessor] = args.as_slice() else {
            return plan_err!("{} requires exactly one payload accessor argument", self.name());
        };
        let Some(access) = QdrantPayloadAccess::from_logical_expr(accessor) else {
            return plan_err!("{} accessor must be a qdrant payload path", self.name());
        };
        let (payload, path) = access.into_parts();
        let expr = match self.kind {
            PayloadPredicateKind::Exists => {
                qdrant_payload_exists_access_udf().call(vec![payload, lit(path)])
            }
            PayloadPredicateKind::IsEmpty => {
                qdrant_payload_is_empty_access_udf().call(vec![payload, lit(path)])
            }
            PayloadPredicateKind::ValuesCount => {
                qdrant_payload_values_count_access_udf().call(vec![payload, lit(path)])
            }
        };
        Ok(ExprSimplifyResult::Simplified(expr))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{} requires qdrant payload predicate simplification", self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PayloadPredicateAccessUdf {
    kind:      PayloadPredicateKind,
    signature: Signature,
}

impl PayloadPredicateAccessUdf {
    fn new(kind: PayloadPredicateKind) -> Self {
        Self { kind, signature: Signature::any(2, Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for PayloadPredicateAccessUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { self.kind.internal_function_name() }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.kind.return_type())
    }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), self.kind.return_type(), true)))
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
            values.push(payload_predicate_scalar(
                self.kind,
                array_string_value(payloads, index, "payload")?.as_deref(),
                array_string_value(paths, index, "payload path")?.as_deref(),
            )?);
        }
        Ok(ColumnarValue::Array(ScalarValue::iter_to_array(values)?))
    }
}

fn payload_predicate_scalar(
    kind: PayloadPredicateKind,
    payload_json: Option<&str>,
    path: Option<&str>,
) -> Result<ScalarValue> {
    let value = payload_json_value(payload_json, path, kind.internal_function_name())?;
    Ok(match kind {
        PayloadPredicateKind::Exists => ScalarValue::Boolean(Some(value.is_some())),
        PayloadPredicateKind::IsEmpty => ScalarValue::Boolean(Some(payload_is_empty_value(value))),
        PayloadPredicateKind::ValuesCount => ScalarValue::Int64(
            payload_values_count_value(value)
                .map(|value| i64::try_from(value).expect("payload values count should fit in i64")),
        ),
    })
}

fn payload_is_empty_value(value: Option<serde_json::Value>) -> bool {
    match value {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Array(values)) => values.is_empty(),
        Some(_) => false,
    }
}

fn payload_values_count_value(value: Option<serde_json::Value>) -> Option<u64> {
    match value {
        None => None,
        Some(serde_json::Value::Null) => Some(0),
        Some(serde_json::Value::Array(values)) => Some(values.len() as u64),
        Some(_) => Some(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_exists_matches_present_values_including_null_and_empty_arrays() {
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::Exists,
                Some(r#"{"list":[1]}"#),
                Some("list")
            )
            .expect("exists"),
            ScalarValue::Boolean(Some(true))
        );
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::Exists,
                Some(r#"{"list":null}"#),
                Some("list")
            )
            .expect("exists"),
            ScalarValue::Boolean(Some(true))
        );
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::Exists,
                Some(r#"{"list":[]}"#),
                Some("list")
            )
            .expect("exists"),
            ScalarValue::Boolean(Some(true))
        );
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::Exists,
                Some(r#"{"other":[1]}"#),
                Some("list")
            )
            .expect("missing"),
            ScalarValue::Boolean(Some(false))
        );
    }

    #[test]
    fn payload_is_empty_matches_missing_null_and_empty_arrays() {
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::IsEmpty,
                Some(r#"{"list": []}"#),
                Some("list")
            )
            .expect("empty array"),
            ScalarValue::Boolean(Some(true))
        );
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::IsEmpty,
                Some(r#"{"list": null}"#),
                Some("list")
            )
            .expect("null"),
            ScalarValue::Boolean(Some(true))
        );
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::IsEmpty,
                Some(r#"{"list": [1]}"#),
                Some("list")
            )
            .expect("non-empty array"),
            ScalarValue::Boolean(Some(false))
        );
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::IsEmpty,
                Some(r#"{"text": ""}"#),
                Some("text")
            )
            .expect("empty string"),
            ScalarValue::Boolean(Some(false))
        );
    }

    #[test]
    fn payload_values_count_matches_qdrant_contract() {
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::ValuesCount,
                Some(r#"{"list": []}"#),
                Some("list")
            )
            .expect("empty array"),
            ScalarValue::Int64(Some(0))
        );
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::ValuesCount,
                Some(r#"{"list": null}"#),
                Some("list")
            )
            .expect("null"),
            ScalarValue::Int64(Some(0))
        );
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::ValuesCount,
                Some(r#"{"obj": {}}"#),
                Some("obj")
            )
            .expect("object"),
            ScalarValue::Int64(Some(1))
        );
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::ValuesCount,
                Some(r#"{"list": [1, 2]}"#),
                Some("list")
            )
            .expect("array"),
            ScalarValue::Int64(Some(2))
        );
        assert_eq!(
            payload_predicate_scalar(
                PayloadPredicateKind::ValuesCount,
                Some(r#"{"other": 1}"#),
                Some("list")
            )
            .expect("missing"),
            ScalarValue::Int64(None)
        );
    }
}
