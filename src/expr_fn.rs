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
pub const QDRANT_RECOMMEND_SCORE_FUNCTION_NAME: &str = "qdrant_recommend_score";
pub const QDRANT_DISCOVER_SCORE_FUNCTION_NAME: &str = "qdrant_discover_score";
pub const QDRANT_CONTEXT_SCORE_FUNCTION_NAME: &str = "qdrant_context_score";
pub const QDRANT_ORDER_BY_SCORE_FUNCTION_NAME: &str = "qdrant_order_by_score";
pub const QDRANT_FUSION_SCORE_FUNCTION_NAME: &str = "qdrant_fusion_score";
pub const QDRANT_SAMPLE_SCORE_FUNCTION_NAME: &str = "qdrant_sample_score";
pub const QDRANT_FORMULA_SCORE_FUNCTION_NAME: &str = "qdrant_formula_score";
pub const QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME: &str = "qdrant_nearest_with_mmr_score";
pub const QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME: &str = "qdrant_relevance_feedback_score";

#[derive(Debug, Clone)]
pub(crate) struct QdrantNearestCall {
    pub(crate) vector_field: String,
    pub(crate) vector:       Vec<f32>,
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

#[must_use]
pub fn qdrant_recommend_score(vector: Expr, positive: Expr, negative: Expr) -> Expr {
    qdrant_recommend_score_udf().call(vec![vector, positive, negative])
}

#[must_use]
pub fn qdrant_discover_score(vector: Expr, target: Expr, context: Expr) -> Expr {
    qdrant_discover_score_udf().call(vec![vector, target, context])
}

#[must_use]
pub fn qdrant_context_score(vector: Expr, context: Expr) -> Expr {
    qdrant_context_score_udf().call(vec![vector, context])
}

#[must_use]
pub fn qdrant_order_by_score(path: Expr, descending: bool) -> Expr {
    qdrant_order_by_score_udf().call(vec![path, lit(descending)])
}

#[must_use]
pub fn qdrant_fusion_score(method: impl Into<String>) -> Expr {
    qdrant_fusion_score_udf().call(vec![lit(method.into())])
}

#[must_use]
pub fn qdrant_sample_score(method: impl Into<String>) -> Expr {
    qdrant_sample_score_udf().call(vec![lit(method.into())])
}

#[must_use]
pub fn qdrant_formula_score(formula: Expr) -> Expr {
    qdrant_formula_score_udf().call(vec![formula])
}

#[must_use]
pub fn qdrant_nearest_with_mmr_score(
    vector: Expr,
    diversity: f32,
    candidates_limit: u32,
    query: impl IntoIterator<Item = f32>,
) -> Expr {
    let mut args = vec![vector, lit(diversity), lit(candidates_limit)];
    args.extend(query.into_iter().map(lit));
    qdrant_nearest_with_mmr_score_udf().call(args)
}

#[must_use]
pub fn qdrant_relevance_feedback_score(vector: Expr, target: Expr, feedback: Expr) -> Expr {
    qdrant_relevance_feedback_score_udf().call(vec![vector, target, feedback])
}

pub(crate) fn register_qdrant_functions(ctx: &SessionContext) {
    ctx.register_udf(qdrant_nearest_score_udf());
    ctx.register_udf(qdrant_recommend_score_udf());
    ctx.register_udf(qdrant_discover_score_udf());
    ctx.register_udf(qdrant_context_score_udf());
    ctx.register_udf(qdrant_order_by_score_udf());
    ctx.register_udf(qdrant_fusion_score_udf());
    ctx.register_udf(qdrant_sample_score_udf());
    ctx.register_udf(qdrant_formula_score_udf());
    ctx.register_udf(qdrant_nearest_with_mmr_score_udf());
    ctx.register_udf(qdrant_relevance_feedback_score_udf());
}

pub(crate) fn qdrant_nearest_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(QdrantNearestScoreUdf::default())).clone()
}

pub(crate) fn qdrant_recommend_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableQdrantScoreUdf::new(
            QDRANT_RECOMMEND_SCORE_FUNCTION_NAME,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_discover_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableQdrantScoreUdf::new(
            QDRANT_DISCOVER_SCORE_FUNCTION_NAME,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_context_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableQdrantScoreUdf::new(
            QDRANT_CONTEXT_SCORE_FUNCTION_NAME,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_fusion_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableQdrantScoreUdf::new(
            QDRANT_FUSION_SCORE_FUNCTION_NAME,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_sample_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableQdrantScoreUdf::new(
            QDRANT_SAMPLE_SCORE_FUNCTION_NAME,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_order_by_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableQdrantScoreUdf::new(
            QDRANT_ORDER_BY_SCORE_FUNCTION_NAME,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_formula_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableQdrantScoreUdf::new(
            QDRANT_FORMULA_SCORE_FUNCTION_NAME,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_nearest_with_mmr_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableQdrantScoreUdf::new(
            QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_relevance_feedback_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableQdrantScoreUdf::new(
            QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME,
        ))
    })
    .clone()
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
    fn default() -> Self { Self { signature: Signature::variadic_any(Volatility::Immutable) } }
}

impl ScalarUDFImpl for QdrantNearestScoreUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { QDRANT_NEAREST_SCORE_FUNCTION_NAME }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Float32) }

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NonExecutableQdrantScoreUdf {
    name:      &'static str,
    signature: Signature,
}

impl NonExecutableQdrantScoreUdf {
    fn new(name: &'static str) -> Self {
        Self { name, signature: Signature::variadic_any(Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for NonExecutableQdrantScoreUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { self.name }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Float32) }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Float32, false)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{} requires qdrant operator pushdown", self.name)
    }
}
