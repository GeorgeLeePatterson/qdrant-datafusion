use datafusion::common::Result;
use qdrant_client::qdrant::{ContextInput, ContextInputPair, DiscoverInput, Query};

use super::super::source::Source;
use super::{QueryDescriptor, VectorQueryInput, vector_input_literal, vector_input_pair_list};
use crate::expr_fn::{DISCOVER_SCORE_FUNCTION_NAME, DiscoverCall};

#[derive(Debug, Clone)]
pub(crate) struct DiscoverQuery {
    using: Option<String>,
    target: VectorQueryInput,
    pairs: Vec<(VectorQueryInput, VectorQueryInput)>,
}

impl TryFrom<DiscoverCall> for DiscoverQuery {
    type Error = datafusion::error::DataFusionError;

    fn try_from(call: DiscoverCall) -> Result<Self> {
        Ok(Self {
            using: Some(call.vector_field),
            target: vector_input_literal(
                &call.target,
                DISCOVER_SCORE_FUNCTION_NAME,
                "target input",
            )?,
            pairs: vector_input_pair_list(
                &call.context,
                DISCOVER_SCORE_FUNCTION_NAME,
                "context pairs",
            )?,
        })
    }
}

impl DiscoverQuery {
    pub(crate) fn same_semantics(&self, other: &Self) -> bool {
        self.using == other.using
            && self.target.same_semantics(&other.target)
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
        self.target.validate_on_source(
            source,
            using,
            DISCOVER_SCORE_FUNCTION_NAME,
            "target input",
        )?;
        for (positive, negative) in &self.pairs {
            positive.validate_on_source(
                source,
                using,
                DISCOVER_SCORE_FUNCTION_NAME,
                "positive context input",
            )?;
            negative.validate_on_source(
                source,
                using,
                DISCOVER_SCORE_FUNCTION_NAME,
                "negative context input",
            )?;
        }
        Ok(())
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> QueryDescriptor {
        QueryDescriptor::new(
            Query::new_discover(DiscoverInput {
                target: Some(self.target.clone().into_proto()),
                context: Some(ContextInput {
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
            }),
            self.using.clone(),
        )
    }
}
