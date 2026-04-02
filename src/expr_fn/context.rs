use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};

use super::common::{NonExecutableScoreUdf, column_name, function_args};

pub const CONTEXT_SCORE_FUNCTION_NAME: &str = "qdrant_context_score";
const ALIASES: &[&str] = &["context_score"];

#[derive(Debug, Clone)]
pub(crate) struct ContextCall {
    pub(crate) vector_field: String,
    pub(crate) context: Expr,
}

impl ContextCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, CONTEXT_SCORE_FUNCTION_NAME, ALIASES) else {
            return Ok(None);
        };
        if args.len() != 2 {
            return plan_err!(
                "{CONTEXT_SCORE_FUNCTION_NAME} requires a vector column and context pairs"
            );
        }
        Ok(Some(Self {
            vector_field: column_name(&args[0], CONTEXT_SCORE_FUNCTION_NAME)?,
            context: args[1].clone(),
        }))
    }
}

#[must_use]
pub fn qdrant_context_score(vector: Expr, context: Expr) -> Expr {
    qdrant_context_score_udf().call(vec![vector, context])
}

pub(crate) fn qdrant_context_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(CONTEXT_SCORE_FUNCTION_NAME, ALIASES))
    })
    .clone()
}
