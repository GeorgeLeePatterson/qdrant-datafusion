use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::{Query, Sample, query};

use super::super::source::Source;
use super::{QueryDescriptor, function_args, string_literal};
use crate::expr_fn::QDRANT_SAMPLE_SCORE_FUNCTION_NAME;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SampleQuery {
    method: Sample,
}

impl SampleQuery {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, QDRANT_SAMPLE_SCORE_FUNCTION_NAME) else {
            return Ok(None);
        };
        if args.len() > 1 {
            return plan_err!(
                "{QDRANT_SAMPLE_SCORE_FUNCTION_NAME} admits at most one sampling method argument"
            );
        }
        let method = if let Some(arg) = args.first() {
            match string_literal(arg, QDRANT_SAMPLE_SCORE_FUNCTION_NAME, "method")?
                .to_ascii_uppercase()
                .as_str()
            {
                "RANDOM" => Sample::Random,
                _ => {
                    return plan_err!(
                        "{QDRANT_SAMPLE_SCORE_FUNCTION_NAME} sampling method must be 'random'"
                    );
                }
            }
        } else {
            Sample::Random
        };
        Ok(Some(Self { method }))
    }

    pub(crate) fn same_semantics(&self, other: &Self) -> bool { self == other }

    pub(super) fn validate_on_source(&self, _source: &Source) -> Result<()> { Ok(()) }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> Result<QueryDescriptor> {
        Ok(QueryDescriptor::new(
            Query { variant: Some(query::Variant::Sample(self.method as i32)) },
            None,
        ))
    }
}
