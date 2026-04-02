use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};

use super::common::{NonExecutableScoreUdf, column_name, function_args};

pub const DISCOVER_SCORE_FUNCTION_NAME: &str = "qdrant_discover_score";
const ALIASES: &[&str] = &["discover_score"];

#[derive(Debug, Clone)]
pub(crate) struct DiscoverCall {
    pub(crate) vector_field: String,
    pub(crate) target: Expr,
    pub(crate) context: Expr,
}

impl DiscoverCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, DISCOVER_SCORE_FUNCTION_NAME, ALIASES) else {
            return Ok(None);
        };
        if args.len() != 3 {
            return plan_err!(
                "{DISCOVER_SCORE_FUNCTION_NAME} requires a vector column, a target input, and                  context pairs"
            );
        }
        Ok(Some(Self {
            vector_field: column_name(&args[0], DISCOVER_SCORE_FUNCTION_NAME)?,
            target: args[1].clone(),
            context: args[2].clone(),
        }))
    }
}

#[must_use]
pub fn qdrant_discover_score(vector: Expr, target: Expr, context: Expr) -> Expr {
    qdrant_discover_score_udf().call(vec![vector, target, context])
}

pub(crate) fn qdrant_discover_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(DISCOVER_SCORE_FUNCTION_NAME, ALIASES))
    })
    .clone()
}
