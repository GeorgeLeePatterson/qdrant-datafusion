use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};

use super::common::{NonExecutableScoreUdf, column_name, function_args};

pub const RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME: &str = "qdrant_relevance_feedback_score";
const ALIASES: &[&str] = &["relevance_feedback_score", "feedback_score"];

#[derive(Debug, Clone)]
pub(crate) struct RelevanceFeedbackCall {
    pub(crate) vector_field:   String,
    pub(crate) target:         Expr,
    pub(crate) feedback:       Expr,
    pub(crate) naive_strategy: (Expr, Expr, Expr),
}

impl RelevanceFeedbackCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME, ALIASES)
        else {
            return Ok(None);
        };
        if args.len() != 6 {
            return plan_err!(
                "{RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME} requires a vector column, a target \
                 input, feedback items, and naive strategy coefficients"
            );
        }
        Ok(Some(Self {
            vector_field:   column_name(&args[0], RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME)?,
            target:         args[1].clone(),
            feedback:       args[2].clone(),
            naive_strategy: (args[3].clone(), args[4].clone(), args[5].clone()),
        }))
    }
}

#[must_use]
pub fn qdrant_relevance_feedback_score(
    vector: Expr,
    target: Expr,
    feedback: Expr,
    a: f32,
    b: f32,
    c: f32,
) -> Expr {
    qdrant_relevance_feedback_score_udf().call(vec![
        vector,
        target,
        feedback,
        Expr::Literal(datafusion::common::ScalarValue::Float32(Some(a)), None),
        Expr::Literal(datafusion::common::ScalarValue::Float32(Some(b)), None),
        Expr::Literal(datafusion::common::ScalarValue::Float32(Some(c)), None),
    ])
}

pub(crate) fn qdrant_relevance_feedback_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(
            RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME,
            ALIASES,
        ))
    })
    .clone()
}
