use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::lit;

pub const QDRANT_NEAREST_SCORE_FUNCTION_NAME: &str = "qdrant_nearest_score";

#[derive(Debug, Clone)]
pub(crate) struct QdrantNearestCall {
    pub(crate) vector_field: String,
    pub(crate) vector: Vec<f32>,
}

impl QdrantNearestCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Expr::ScalarFunction(function) = expr else {
            return Ok(None);
        };
        if function.name() != QDRANT_NEAREST_SCORE_FUNCTION_NAME {
            return Ok(None);
        }
        if function.args.len() < 2 {
            return plan_err!(
                "{QDRANT_NEAREST_SCORE_FUNCTION_NAME} requires a vector column and at least one \
                 query component"
            );
        }
        let vector_field = vector_field_name(&function.args[0])?;
        let vector = function.args[1..].iter().map(query_component).collect::<Result<Vec<_>>>()?;
        Ok(Some(Self { vector_field, vector }))
    }
}

#[must_use]
pub fn qdrant_nearest_score(vector: Expr, query: impl IntoIterator<Item = f32>) -> Expr {
    let mut args = vec![vector];
    args.extend(query.into_iter().map(lit));
    qdrant_nearest_score_udf().call(args)
}

pub(crate) fn register_qdrant_functions(ctx: &SessionContext) {
    ctx.register_udf(qdrant_nearest_score_udf());
}

pub(crate) fn qdrant_nearest_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(QdrantNearestScoreUdf::default())).clone()
}

fn vector_field_name(expr: &Expr) -> Result<String> {
    let expr = expr.clone().unalias_nested().data;
    let Expr::Column(column) = expr else {
        return plan_err!(
            "{QDRANT_NEAREST_SCORE_FUNCTION_NAME} requires a vector column reference"
        );
    };
    Ok(column.name)
}

fn query_component(expr: &Expr) -> Result<f32> {
    let expr = expr.clone().unalias_nested().data;
    match expr {
        Expr::Negative(expr) => Ok(-query_component(&expr)?),
        Expr::Cast(cast) => query_component(&cast.expr),
        Expr::TryCast(cast) => query_component(&cast.expr),
        Expr::Literal(value, _) => scalar_to_f32(&value),
        _ => plan_err!(
            "{QDRANT_NEAREST_SCORE_FUNCTION_NAME} query components must be numeric literals"
        ),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
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
        _ => plan_err!("{QDRANT_NEAREST_SCORE_FUNCTION_NAME} query components must be numeric"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QdrantNearestScoreUdf {
    signature: Signature,
}

impl Default for QdrantNearestScoreUdf {
    fn default() -> Self {
        Self { signature: Signature::variadic_any(Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for QdrantNearestScoreUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        QDRANT_NEAREST_SCORE_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Float32, false)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{QDRANT_NEAREST_SCORE_FUNCTION_NAME} requires qdrant operator pushdown")
    }
}
