use datafusion::common::{Result, plan_err};
use qdrant_client::qdrant::{Query, RecommendInput, RecommendStrategy};

use super::super::source::Source;
use super::{QueryDescriptor, VectorQueryInput, scalar_string, vector_input_list};
use crate::expr_fn::{RECOMMEND_SCORE_FUNCTION_NAME, RecommendCall};

const AVERAGE_VECTOR_STRATEGY: &str = "average_vector";
const BEST_SCORE_STRATEGY: &str = "best_score";
const SUM_SCORES_STRATEGY: &str = "sum_scores";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecommendQueryStrategy {
    AverageVector,
    BestScore,
    SumScores,
}

impl RecommendQueryStrategy {
    fn into_proto(self) -> RecommendStrategy {
        match self {
            Self::AverageVector => RecommendStrategy::AverageVector,
            Self::BestScore => RecommendStrategy::BestScore,
            Self::SumScores => RecommendStrategy::SumScores,
        }
    }
}

impl TryFrom<&str> for RecommendQueryStrategy {
    type Error = datafusion::error::DataFusionError;

    fn try_from(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            AVERAGE_VECTOR_STRATEGY => Ok(Self::AverageVector),
            BEST_SCORE_STRATEGY => Ok(Self::BestScore),
            SUM_SCORES_STRATEGY => Ok(Self::SumScores),
            _ => {
                plan_err!(
                    "{RECOMMEND_SCORE_FUNCTION_NAME} strategy must be                      \
                     '{AVERAGE_VECTOR_STRATEGY}', '{BEST_SCORE_STRATEGY}', or                      \
                     '{SUM_SCORES_STRATEGY}'"
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RecommendQuery {
    using: Option<String>,
    strategy: Option<RecommendQueryStrategy>,
    positive: Vec<VectorQueryInput>,
    negative: Vec<VectorQueryInput>,
}

impl TryFrom<RecommendCall> for RecommendQuery {
    type Error = datafusion::error::DataFusionError;

    fn try_from(call: RecommendCall) -> Result<Self> {
        let strategy = match call.strategy.as_ref() {
            Some(strategy) => Some(parse_strategy(strategy)?),
            None => None,
        };
        Ok(Self {
            using: Some(call.vector_field),
            strategy,
            positive: vector_input_list(
                &call.positive,
                RECOMMEND_SCORE_FUNCTION_NAME,
                "positive examples",
            )?,
            negative: vector_input_list(
                &call.negative,
                RECOMMEND_SCORE_FUNCTION_NAME,
                "negative examples",
            )?,
        })
    }
}

impl RecommendQuery {
    pub(crate) fn same_semantics(&self, other: &Self) -> bool {
        self.using == other.using
            && self.strategy == other.strategy
            && same_inputs(&self.positive, &other.positive)
            && same_inputs(&self.negative, &other.negative)
    }

    pub(super) fn validate_on_source(&self, source: &Source) -> Result<()> {
        let Some(using) = self.using.as_deref() else {
            return Ok(());
        };
        for input in self.positive.iter().chain(&self.negative) {
            input.validate_on_source(
                source,
                using,
                RECOMMEND_SCORE_FUNCTION_NAME,
                "example input",
            )?;
        }
        Ok(())
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> QueryDescriptor {
        QueryDescriptor::new(
            Query::new_recommend(RecommendInput {
                positive: self.positive.iter().cloned().map(VectorQueryInput::into_proto).collect(),
                negative: self.negative.iter().cloned().map(VectorQueryInput::into_proto).collect(),
                strategy: self.strategy.map(|strategy| strategy.into_proto() as i32),
            }),
            self.using.clone(),
        )
    }
}

fn parse_strategy(expr: &datafusion::logical_expr::Expr) -> Result<RecommendQueryStrategy> {
    let strategy = scalar_string(
        &match expr.clone().unalias_nested().data {
            datafusion::logical_expr::Expr::Literal(value, _) => value,
            datafusion::logical_expr::Expr::Cast(cast) => return parse_strategy(&cast.expr),
            datafusion::logical_expr::Expr::TryCast(cast) => return parse_strategy(&cast.expr),
            _ => {
                return plan_err!(
                    "{RECOMMEND_SCORE_FUNCTION_NAME} requires strategy to be a string literal"
                );
            }
        },
        RECOMMEND_SCORE_FUNCTION_NAME,
        "strategy",
    )?;
    RecommendQueryStrategy::try_from(strategy.as_str())
}

fn same_inputs(lhs: &[VectorQueryInput], rhs: &[VectorQueryInput]) -> bool {
    lhs.len() == rhs.len() && lhs.iter().zip(rhs).all(|(lhs, rhs)| lhs.same_semantics(rhs))
}
