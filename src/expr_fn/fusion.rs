use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::prelude::lit;

use super::common::{NonExecutableScoreUdf, function_args};

pub const FUSION_SCORE_FUNCTION_NAME: &str = "qdrant_fusion_score";
const ALIASES: &[&str] = &["fusion_score"];

#[derive(Debug, Clone)]
pub(crate) struct FusionCall {
    pub(crate) method: Expr,
    pub(crate) rrf_k:  Option<Expr>,
}

impl FusionCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, FUSION_SCORE_FUNCTION_NAME, ALIASES) else {
            return Ok(None);
        };
        if !(1..=2).contains(&args.len()) {
            return plan_err!(
                "{FUSION_SCORE_FUNCTION_NAME} requires a fusion method and an optional RRF k"
            );
        }
        Ok(Some(Self { method: args[0].clone(), rrf_k: args.get(1).cloned() }))
    }
}

#[must_use]
pub fn qdrant_fusion_score(method: impl Into<String>) -> Expr {
    qdrant_fusion_score_udf().call(vec![lit(method.into())])
}

pub(crate) fn qdrant_fusion_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(FUSION_SCORE_FUNCTION_NAME, ALIASES))
    })
    .clone()
}
