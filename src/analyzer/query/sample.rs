use datafusion::common::{Result, plan_err};
use qdrant_client::qdrant::{Query, Sample, query};

use super::{QueryDescriptor, string_literal};
use crate::expr_fn::{SAMPLE_SCORE_FUNCTION_NAME, SampleCall};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SampleQuery {
    method: Sample,
}

impl TryFrom<SampleCall> for SampleQuery {
    type Error = datafusion::error::DataFusionError;

    fn try_from(call: SampleCall) -> Result<Self> {
        let method = if let Some(arg) = call.method.as_ref() {
            match string_literal(arg, SAMPLE_SCORE_FUNCTION_NAME, "method")?
                .to_ascii_uppercase()
                .as_str()
            {
                "RANDOM" => Sample::Random,
                _ => {
                    return plan_err!(
                        "{SAMPLE_SCORE_FUNCTION_NAME} sampling method must be 'random'"
                    );
                }
            }
        } else {
            Sample::Random
        };
        Ok(Self { method })
    }
}

impl SampleQuery {
    pub(crate) fn same_semantics(&self, other: &Self) -> bool {
        self == other
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> QueryDescriptor {
        QueryDescriptor::new(
            Query { variant: Some(query::Variant::Sample(self.method as i32)) },
            None,
        )
    }
}
