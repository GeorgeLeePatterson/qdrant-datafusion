use std::any::Any;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::lit;

use super::payload_access::payload_access_expr;
use crate::qdrant::QdrantPayloadAccess;

pub const PAYLOAD_FUNCTION_NAME: &str = "payload";
const PAYLOAD_ALIASES: &[&str] = &["qdrant_payload"];

pub(crate) fn is_payload_function_name(name: &str) -> bool {
    name == PAYLOAD_FUNCTION_NAME || PAYLOAD_ALIASES.contains(&name)
}

#[must_use]
pub fn qdrant_payload(accessor: Expr, data_type: impl Into<String>) -> Expr {
    qdrant_payload_udf().call(vec![accessor, lit(data_type.into())])
}

pub(crate) fn qdrant_payload_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadUdf::default())).clone()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PayloadUdf {
    aliases:   Vec<String>,
    signature: Signature,
}

impl Default for PayloadUdf {
    fn default() -> Self {
        Self {
            aliases:   PAYLOAD_ALIASES.iter().map(|alias| (*alias).to_owned()).collect(),
            signature: Signature::any(2, Volatility::Immutable)
                .with_parameter_names(vec!["accessor", "data_type"])
                .expect("payload signature should accept two named parameters"),
        }
    }
}

impl ScalarUDFImpl for PayloadUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { PAYLOAD_FUNCTION_NAME }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        plan_err!("{PAYLOAD_FUNCTION_NAME} determines its output type from its arguments")
    }

    fn return_field_from_args(
        &self,
        args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        if args.arg_fields.len() != 2 || args.scalar_arguments.len() != 2 {
            return plan_err!(
                "{PAYLOAD_FUNCTION_NAME} requires an accessor expression and a target data type"
            );
        }
        let Some(type_value) = args.scalar_arguments[1] else {
            return plan_err!(
                "{PAYLOAD_FUNCTION_NAME} requires a string literal data type argument"
            );
        };
        let data_type = parse_payload_data_type_scalar(type_value)?;
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [accessor, data_type] = args.as_slice() else {
            return plan_err!(
                "{PAYLOAD_FUNCTION_NAME} requires an accessor expression and a target data type"
            );
        };
        let data_type = parse_payload_data_type_expr(data_type)?;
        Ok(ExprSimplifyResult::Simplified(rewrite_payload_expr(accessor, &data_type)?))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{PAYLOAD_FUNCTION_NAME} requires qdrant payload access simplification")
    }
}

fn rewrite_payload_expr(accessor: &Expr, data_type: &DataType) -> Result<Expr> {
    let Some(access) = QdrantPayloadAccess::from_logical_expr(accessor) else {
        return plan_err!("{PAYLOAD_FUNCTION_NAME} accessor must be a qdrant payload path");
    };
    let (payload, path) = access.into_parts();
    payload_access_expr(payload, path, data_type).ok_or_else(|| {
        datafusion::common::DataFusionError::Plan(format!(
            "{PAYLOAD_FUNCTION_NAME} does not support target data type {data_type}"
        ))
    })
}

fn parse_payload_data_type_expr(expr: &Expr) -> Result<DataType> {
    match expr.clone().unalias_nested().data {
        Expr::Cast(cast) => parse_payload_data_type_expr(&cast.expr),
        Expr::TryCast(cast) => parse_payload_data_type_expr(&cast.expr),
        Expr::Literal(value, _) => parse_payload_data_type_scalar(&value),
        _ => plan_err!("{PAYLOAD_FUNCTION_NAME} requires a string literal data type argument"),
    }
}

fn parse_payload_data_type_scalar(value: &ScalarValue) -> Result<DataType> {
    let Some(raw) = (match value {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) => value.as_deref(),
        _ => None,
    }) else {
        return plan_err!("{PAYLOAD_FUNCTION_NAME} requires a string literal data type argument");
    };
    parse_payload_data_type(raw)
}

fn parse_payload_data_type(raw: &str) -> Result<DataType> {
    if let Ok(data_type) = DataType::from_str(raw) {
        return Ok(data_type);
    }
    match raw.trim().to_ascii_lowercase().as_str() {
        "string" | "text" | "keyword" | "uuid" => Ok(DataType::Utf8),
        "int" | "integer" => Ok(DataType::Int64),
        "float" | "double" | "number" | "numeric" => Ok(DataType::Float64),
        "bool" | "boolean" => Ok(DataType::Boolean),
        "datetime" | "timestamp" => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        _ => plan_err!(
            "{PAYLOAD_FUNCTION_NAME} data type '{raw}' is not a supported Arrow or qdrant payload \
             type"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_friendly_payload_type_aliases() {
        assert_eq!(parse_payload_data_type("Integer").expect("integer alias"), DataType::Int64);
        assert_eq!(parse_payload_data_type("String").expect("string alias"), DataType::Utf8);
        assert_eq!(
            parse_payload_data_type("datetime").expect("datetime alias"),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }
}
