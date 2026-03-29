use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::{Query, RecommendInput, RecommendStrategy};

use super::super::source::Source;
use super::{
    QueryDescriptor, VectorQueryInput, column_name, function_args, scalar_string, vector_input_list,
};
use crate::expr_fn::QDRANT_RECOMMEND_SCORE_FUNCTION_NAME;

#[derive(Debug, Clone)]
pub(crate) struct RecommendQuery {
    using:    Option<String>,
    strategy: Option<RecommendStrategy>,
    positive: Vec<VectorQueryInput>,
    negative: Vec<VectorQueryInput>,
}

impl RecommendQuery {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, QDRANT_RECOMMEND_SCORE_FUNCTION_NAME) else {
            return Ok(None);
        };
        if !(3..=4).contains(&args.len()) {
            return plan_err!(
                "{QDRANT_RECOMMEND_SCORE_FUNCTION_NAME} requires a vector column, optional \
                 strategy, positive examples, and negative examples"
            );
        }
        let using = Some(column_name(&args[0], QDRANT_RECOMMEND_SCORE_FUNCTION_NAME)?);
        let (strategy, positive_arg, negative_arg) = if args.len() == 4 {
            (Some(parse_strategy(&args[1])?), &args[2], &args[3])
        } else {
            (None, &args[1], &args[2])
        };
        Ok(Some(Self {
            using,
            strategy,
            positive: vector_input_list(
                positive_arg,
                QDRANT_RECOMMEND_SCORE_FUNCTION_NAME,
                "positive examples",
            )?,
            negative: vector_input_list(
                negative_arg,
                QDRANT_RECOMMEND_SCORE_FUNCTION_NAME,
                "negative examples",
            )?,
        }))
    }

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
                QDRANT_RECOMMEND_SCORE_FUNCTION_NAME,
                "example input",
            )?;
        }
        Ok(())
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> Result<QueryDescriptor> {
        Ok(QueryDescriptor::new(
            Query::new_recommend(RecommendInput {
                positive: self.positive.iter().cloned().map(VectorQueryInput::into_proto).collect(),
                negative: self.negative.iter().cloned().map(VectorQueryInput::into_proto).collect(),
                strategy: self.strategy.map(|strategy| strategy as i32),
            }),
            self.using.clone(),
        ))
    }
}

fn parse_strategy(expr: &Expr) -> Result<RecommendStrategy> {
    let strategy = scalar_string(
        &match expr.clone().unalias_nested().data {
            Expr::Literal(value, _) => value,
            Expr::Cast(cast) => return parse_strategy(&cast.expr),
            Expr::TryCast(cast) => return parse_strategy(&cast.expr),
            _ => {
                return plan_err!(
                    "{QDRANT_RECOMMEND_SCORE_FUNCTION_NAME} requires strategy to be a string \
                     literal"
                );
            }
        },
        QDRANT_RECOMMEND_SCORE_FUNCTION_NAME,
        "strategy",
    )?;
    match strategy.to_ascii_uppercase().as_str() {
        "AVERAGE_VECTOR" => Ok(RecommendStrategy::AverageVector),
        "BEST_SCORE" => Ok(RecommendStrategy::BestScore),
        "SUM_SCORES" => Ok(RecommendStrategy::SumScores),
        _ => plan_err!(
            "{QDRANT_RECOMMEND_SCORE_FUNCTION_NAME} strategy must be 'AVERAGE_VECTOR', \
             'BEST_SCORE', or 'SUM_SCORES'"
        ),
    }
}

fn same_inputs(lhs: &[VectorQueryInput], rhs: &[VectorQueryInput]) -> bool {
    lhs.len() == rhs.len() && lhs.iter().zip(rhs).all(|(lhs, rhs)| lhs.same_semantics(rhs))
}
