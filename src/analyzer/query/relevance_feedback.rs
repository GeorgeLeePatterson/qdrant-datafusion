use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::{
    FeedbackItem, FeedbackStrategy, NaiveFeedbackStrategy, Query, RelevanceFeedbackInput,
    feedback_strategy,
};

use super::super::source::Source;
use super::{
    QueryDescriptor, column_name, f32_literal, feedback_input_list, function_args,
    vector_input_literal,
};
use crate::expr_fn::QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME;

#[derive(Debug, Clone)]
pub(crate) struct RelevanceFeedbackQuery {
    using:    Option<String>,
    target:   super::VectorQueryInput,
    feedback: Vec<(super::VectorQueryInput, f32)>,
    strategy: Option<FeedbackStrategy>,
}

impl RelevanceFeedbackQuery {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME) else {
            return Ok(None);
        };
        if args.len() != 3 && args.len() != 6 {
            return plan_err!(
                "{QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME} requires a vector column, a \
                 target input, feedback items, and optional naive strategy coefficients"
            );
        }
        let strategy = if args.len() == 6 {
            Some(FeedbackStrategy {
                variant: Some(feedback_strategy::Variant::Naive(NaiveFeedbackStrategy {
                    a: f32_literal(&args[3], QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME, "a")?,
                    b: f32_literal(&args[4], QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME, "b")?,
                    c: f32_literal(&args[5], QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME, "c")?,
                })),
            })
        } else {
            None
        };
        Ok(Some(Self {
            using: Some(column_name(&args[0], QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME)?),
            target: vector_input_literal(
                &args[1],
                QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME,
                "target input",
            )?,
            feedback: feedback_input_list(
                &args[2],
                QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME,
                "feedback items",
            )?,
            strategy,
        }))
    }

    pub(crate) fn same_semantics(&self, other: &Self) -> bool {
        self.using == other.using
            && self.target.same_semantics(&other.target)
            && self.feedback.len() == other.feedback.len()
            && self
                .feedback
                .iter()
                .zip(&other.feedback)
                .all(|((le, ls), (re, rs))| le.same_semantics(re) && ls.to_bits() == rs.to_bits())
            && self.strategy == other.strategy
    }

    pub(super) fn validate_on_source(&self, source: &Source) -> Result<()> {
        let Some(using) = self.using.as_deref() else {
            return Ok(());
        };
        self.target.validate_on_source(
            source,
            using,
            QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME,
            "target input",
        )?;
        for (example, _) in &self.feedback {
            example.validate_on_source(
                source,
                using,
                QDRANT_RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME,
                "feedback example",
            )?;
        }
        Ok(())
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> Result<QueryDescriptor> {
        Ok(QueryDescriptor::new(
            Query::new_relevance_feedback(RelevanceFeedbackInput {
                target:   Some(self.target.clone().into_proto()),
                feedback: self
                    .feedback
                    .iter()
                    .cloned()
                    .map(|(example, score)| FeedbackItem {
                        example: Some(example.into_proto()),
                        score,
                    })
                    .collect(),
                strategy: self.strategy,
            }),
            self.using.clone(),
        ))
    }
}
