use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::{ContextInput, ContextInputPair, Query};

use super::super::source::Source;
use super::{
    QueryDescriptor, VectorQueryInput, column_name, function_args, vector_input_pair_list,
};
use crate::expr_fn::QDRANT_CONTEXT_SCORE_FUNCTION_NAME;

#[derive(Debug, Clone)]
pub(crate) struct ContextQuery {
    using: Option<String>,
    pairs: Vec<(VectorQueryInput, VectorQueryInput)>,
}

impl ContextQuery {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, QDRANT_CONTEXT_SCORE_FUNCTION_NAME) else {
            return Ok(None);
        };
        if args.len() != 2 {
            return plan_err!(
                "{QDRANT_CONTEXT_SCORE_FUNCTION_NAME} requires a vector column and context pairs"
            );
        }
        Ok(Some(Self {
            using: Some(column_name(&args[0], QDRANT_CONTEXT_SCORE_FUNCTION_NAME)?),
            pairs: vector_input_pair_list(
                &args[1],
                QDRANT_CONTEXT_SCORE_FUNCTION_NAME,
                "context pairs",
            )?,
        }))
    }

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
                QDRANT_CONTEXT_SCORE_FUNCTION_NAME,
                "positive context input",
            )?;
            negative.validate_on_source(
                source,
                using,
                QDRANT_CONTEXT_SCORE_FUNCTION_NAME,
                "negative context input",
            )?;
        }
        Ok(())
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> Result<QueryDescriptor> {
        Ok(QueryDescriptor::new(
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
        ))
    }
}
