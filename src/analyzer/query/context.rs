use datafusion::common::Result;
use qdrant_client::qdrant::{ContextInput, ContextInputPair, Query};

use super::super::source::Source;
use super::{QueryDescriptor, VectorQueryInput, vector_input_pair_list};
use crate::expr_fn::{CONTEXT_SCORE_FUNCTION_NAME, ContextCall};

#[derive(Debug, Clone)]
pub(crate) struct ContextQuery {
    using: Option<String>,
    pairs: Vec<(VectorQueryInput, VectorQueryInput)>,
}

impl TryFrom<ContextCall> for ContextQuery {
    type Error = datafusion::error::DataFusionError;

    fn try_from(call: ContextCall) -> Result<Self> {
        Ok(Self {
            using: Some(call.vector_field),
            pairs: vector_input_pair_list(
                &call.context,
                CONTEXT_SCORE_FUNCTION_NAME,
                "context pairs",
            )?,
        })
    }
}

impl ContextQuery {
    pub(crate) fn same_semantics(&self, other: &Self) -> bool {
        self.using == other.using
            && self.pairs.len() == other.pairs.len()
            && self
                .pairs
                .iter()
                .zip(&other.pairs)
                .all(|((lp, ln), (rp, rn))| lp.same_semantics(rp) && ln.same_semantics(rn))
    }

    pub(super) fn validate_on_source(&self, source: &Source) -> Result<()> {
        let Some(using) = self.using.as_deref() else {
            return Ok(());
        };
        for (positive, negative) in &self.pairs {
            positive.validate_on_source(
                source,
                using,
                CONTEXT_SCORE_FUNCTION_NAME,
                "positive context input",
            )?;
            negative.validate_on_source(
                source,
                using,
                CONTEXT_SCORE_FUNCTION_NAME,
                "negative context input",
            )?;
        }
        Ok(())
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> QueryDescriptor {
        QueryDescriptor::new(
            Query::new_context(ContextInput {
                pairs: self
                    .pairs
                    .iter()
                    .cloned()
                    .map(|(positive, negative)| ContextInputPair {
                        positive: Some(positive.into_proto()),
                        negative: Some(negative.into_proto()),
                    })
                    .collect(),
            }),
            self.using.clone(),
        )
    }
}
