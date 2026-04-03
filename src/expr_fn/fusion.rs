use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::prelude::lit;

use super::common::{NonExecutableScoreUdf, function_args};

pub const FUSION_SCORE_FUNCTION_NAME: &str = "qdrant_fusion_score";
const ALIASES: &[&str] = &["fusion_score"];

#[derive(Debug, Clone)]
pub(crate) struct FusionCall {
    pub(crate) method:       Expr,
    pub(crate) rrf_k:        Option<Expr>,
    pub(crate) score_inputs: Vec<Expr>,
}

impl FusionCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, FUSION_SCORE_FUNCTION_NAME, ALIASES) else {
            return Ok(None);
        };
        if args.is_empty() {
            return plan_err!(
                "{FUSION_SCORE_FUNCTION_NAME} requires a fusion method and optional score inputs"
            );
        }
        let method = args[0].clone();
        let mut remainder = &args[1..];
        let mut rrf_k = None;
        if let Some(candidate) = remainder.first()
            && looks_like_integer_literal(candidate)
        {
            rrf_k = Some((*candidate).clone());
            remainder = &remainder[1..];
        }
        Ok(Some(Self { method, rrf_k, score_inputs: remainder.to_vec() }))
    }
}

fn looks_like_integer_literal(expr: &Expr) -> bool {
    match expr.clone().unalias_nested().data {
        Expr::Cast(cast) => looks_like_integer_literal(&cast.expr),
        Expr::TryCast(cast) => looks_like_integer_literal(&cast.expr),
        Expr::Literal(value, _) => match value {
            datafusion::common::ScalarValue::Int8(Some(value)) => value >= 0,
            datafusion::common::ScalarValue::Int16(Some(value)) => value >= 0,
            datafusion::common::ScalarValue::Int32(Some(value)) => value >= 0,
            datafusion::common::ScalarValue::Int64(Some(value)) => value >= 0,
            datafusion::common::ScalarValue::UInt8(Some(_))
            | datafusion::common::ScalarValue::UInt16(Some(_))
            | datafusion::common::ScalarValue::UInt32(Some(_))
            | datafusion::common::ScalarValue::UInt64(Some(_)) => true,
            _ => false,
        },
        _ => false,
    }
}

#[must_use]
pub fn qdrant_fusion_score(method: impl Into<String>) -> Expr {
    qdrant_fusion_score_udf().call(vec![lit(method.into())])
}

#[must_use]
pub fn qdrant_fusion_score_with_inputs(
    method: impl Into<String>,
    score_inputs: impl IntoIterator<Item = Expr>,
) -> Expr {
    let mut args = vec![lit(method.into())];
    args.extend(score_inputs);
    qdrant_fusion_score_udf().call(args)
}

pub(crate) fn qdrant_fusion_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(FUSION_SCORE_FUNCTION_NAME, ALIASES))
    })
    .clone()
}
