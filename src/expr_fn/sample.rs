use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::prelude::lit;

use super::common::{NonExecutableScoreUdf, function_args};

pub const SAMPLE_SCORE_FUNCTION_NAME: &str = "qdrant_sample_score";
const ALIASES: &[&str] = &["sample_score"];

#[derive(Debug, Clone)]
pub(crate) struct SampleCall {
    pub(crate) method: Option<Expr>,
}

impl SampleCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, SAMPLE_SCORE_FUNCTION_NAME, ALIASES) else {
            return Ok(None);
        };
        if args.len() > 1 {
            return plan_err!(
                "{SAMPLE_SCORE_FUNCTION_NAME} admits at most one sampling method argument"
            );
        }
        Ok(Some(Self { method: args.first().cloned() }))
    }
}

#[must_use]
pub fn qdrant_sample_score(method: impl Into<String>) -> Expr {
    qdrant_sample_score_udf().call(vec![lit(method.into())])
}

pub(crate) fn qdrant_sample_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new_nullary_or_variadic(
            SAMPLE_SCORE_FUNCTION_NAME,
            ALIASES,
        ))
    })
    .clone()
}
