use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::prelude::lit;

use super::common::{NonExecutableScoreUdf, column_name, function_args};

pub const RECOMMEND_SCORE_FUNCTION_NAME: &str = "qdrant_recommend_score";
const ALIASES: &[&str] = &["recommend_score"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum QdrantRecommendStrategy {
    #[default]
    Default,
    AverageVector,
    BestScore,
    SumScores,
}

impl QdrantRecommendStrategy {
    #[must_use]
    pub fn as_str(self) -> Option<&'static str> {
        match self {
            Self::Default => None,
            Self::AverageVector => Some("average_vector"),
            Self::BestScore => Some("best_score"),
            Self::SumScores => Some("sum_scores"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RecommendCall {
    pub(crate) vector_field: String,
    pub(crate) strategy:     Option<Expr>,
    pub(crate) positive:     Expr,
    pub(crate) negative:     Expr,
}

impl RecommendCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, RECOMMEND_SCORE_FUNCTION_NAME, ALIASES) else {
            return Ok(None);
        };
        if !(3..=4).contains(&args.len()) {
            return plan_err!(
                "{RECOMMEND_SCORE_FUNCTION_NAME} requires a vector column, optional strategy,                  positive examples, and negative examples"
            );
        }
        let (strategy, positive, negative) = if args.len() == 4 {
            (Some(args[1].clone()), args[2].clone(), args[3].clone())
        } else {
            (None, args[1].clone(), args[2].clone())
        };
        Ok(Some(Self {
            vector_field: column_name(&args[0], RECOMMEND_SCORE_FUNCTION_NAME)?,
            strategy,
            positive,
            negative,
        }))
    }
}

#[must_use]
pub fn qdrant_recommend_score(
    vector: Expr,
    strategy: QdrantRecommendStrategy,
    positive: Expr,
    negative: Expr,
) -> Expr {
    let mut args = vec![vector];
    if let Some(strategy) = strategy.as_str() {
        args.push(lit(strategy));
    }
    args.push(positive);
    args.push(negative);
    qdrant_recommend_score_udf().call(args)
}

pub(crate) fn qdrant_recommend_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(RECOMMEND_SCORE_FUNCTION_NAME, ALIASES))
    })
    .clone()
}
