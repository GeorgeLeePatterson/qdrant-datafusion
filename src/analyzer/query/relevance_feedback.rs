use datafusion::common::Result;
use qdrant_client::qdrant::{
    FeedbackItem, FeedbackStrategy, NaiveFeedbackStrategy, Query, RelevanceFeedbackInput,
    feedback_strategy,
};

use super::super::source::Source;
use super::{QueryDescriptor, f32_literal, feedback_input_list, vector_input_literal};
use crate::expr_fn::{RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME, RelevanceFeedbackCall};

#[derive(Debug, Clone)]
pub(crate) struct RelevanceFeedbackQuery {
    using:    Option<String>,
    target:   super::VectorQueryInput,
    feedback: Vec<(super::VectorQueryInput, f32)>,
    strategy: Option<FeedbackStrategy>,
}

impl TryFrom<RelevanceFeedbackCall> for RelevanceFeedbackQuery {
    type Error = datafusion::error::DataFusionError;

    fn try_from(call: RelevanceFeedbackCall) -> Result<Self> {
        let strategy = match call.naive_strategy.as_ref() {
            Some((a, b, c)) => Some(FeedbackStrategy {
                variant: Some(feedback_strategy::Variant::Naive(NaiveFeedbackStrategy {
                    a: f32_literal(a, RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME, "a")?,
                    b: f32_literal(b, RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME, "b")?,
                    c: f32_literal(c, RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME, "c")?,
                })),
            }),
            None => None,
        };
        Ok(Self {
            using: Some(call.vector_field),
            target: vector_input_literal(
                &call.target,
                RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME,
                "target input",
            )?,
            feedback: feedback_input_list(
                &call.feedback,
                RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME,
                "feedback items",
            )?,
            strategy,
        })
    }
}

impl RelevanceFeedbackQuery {
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
            RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME,
            "target input",
        )?;
        for (example, _) in &self.feedback {
            example.validate_on_source(
                source,
                using,
                RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME,
                "feedback example",
            )?;
        }
        Ok(())
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> QueryDescriptor {
        QueryDescriptor::new(
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
        )
    }
}
