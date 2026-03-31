use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::lit;

use super::common::{column_name, function_args};

pub const NEAREST_SCORE_FUNCTION_NAME: &str = "qdrant_nearest_score";
const ALIASES: &[&str] = &["nearest_score"];

#[derive(Debug, Clone)]
pub(crate) struct NearestCall {
    pub(crate) vector_field: String,
    pub(crate) vector:       Vec<f32>,
}

impl NearestCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, NEAREST_SCORE_FUNCTION_NAME, ALIASES) else {
            return Ok(None);
        };
        if args.len() < 2 {
            return plan_err!(
                "{NEAREST_SCORE_FUNCTION_NAME} requires a vector column and at least one query                  component"
            );
        }
        let vector_field = column_name(&args[0], NEAREST_SCORE_FUNCTION_NAME)?;
        let vector = args[1..].iter().map(query_component).collect::<Result<Vec<_>>>()?;
        Ok(Some(Self { vector_field, vector }))
    }
}

#[must_use]
pub fn qdrant_nearest_score(vector: Expr, query: impl IntoIterator<Item = f32>) -> Expr {
    let mut args = vec![vector];
    args.extend(query.into_iter().map(lit));
    qdrant_nearest_score_udf().call(args)
}

pub(crate) fn qdrant_nearest_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(NearestScoreUdf::default())).clone()
}

fn query_component(expr: &Expr) -> Result<f32> {
    let expr = expr.clone().unalias_nested().data;
    match expr {
        Expr::Negative(expr) => Ok(-query_component(&expr)?),
        Expr::Cast(cast) => query_component(&cast.expr),
        Expr::TryCast(cast) => query_component(&cast.expr),
        Expr::Literal(value, _) => scalar_to_f32(&value),
        _ => plan_err!("{NEAREST_SCORE_FUNCTION_NAME} query components must be numeric literals"),
    }
}

#[expect(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn scalar_to_f32(value: &ScalarValue) -> Result<f32> {
    match value {
        ScalarValue::Float32(Some(value)) => Ok(*value),
        ScalarValue::Float64(Some(value)) => Ok(*value as f32),
        ScalarValue::Int8(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::Int16(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::Int32(Some(value)) => Ok(*value as f32),
        ScalarValue::Int64(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt8(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::UInt16(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::UInt32(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt64(Some(value)) => Ok(*value as f32),
        _ => plan_err!("{NEAREST_SCORE_FUNCTION_NAME} query components must be numeric"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NearestScoreUdf {
    aliases:   Vec<String>,
    signature: Signature,
}

impl Default for NearestScoreUdf {
    fn default() -> Self {
        Self {
            aliases:   ALIASES.iter().map(|alias| (*alias).to_owned()).collect(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NearestScoreUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { NEAREST_SCORE_FUNCTION_NAME }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Float32) }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Float32, false)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{NEAREST_SCORE_FUNCTION_NAME} requires qdrant operator pushdown")
    }
}
