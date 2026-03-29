use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::{Fusion, Query, Rrf, query};

use super::super::source::Source;
use super::{QueryDescriptor, function_args, string_literal, u32_literal};
use crate::expr_fn::QDRANT_FUSION_SCORE_FUNCTION_NAME;

#[derive(Debug, Clone, PartialEq)]
enum FusionMethod {
    Default(Fusion),
    Rrf(Rrf),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FusionQuery {
    method: FusionMethod,
}

impl FusionQuery {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, QDRANT_FUSION_SCORE_FUNCTION_NAME) else {
            return Ok(None);
        };
        if !(1..=2).contains(&args.len()) {
            return plan_err!(
                "{QDRANT_FUSION_SCORE_FUNCTION_NAME} requires a fusion method and an optional RRF \
                 k"
            );
        }
        let method = string_literal(&args[0], QDRANT_FUSION_SCORE_FUNCTION_NAME, "method")?;
        let method = match method.to_ascii_uppercase().as_str() {
            "RRF" => {
                if args.len() == 2 {
                    FusionMethod::Rrf(Rrf {
                        k:       Some(u32_literal(
                            &args[1],
                            QDRANT_FUSION_SCORE_FUNCTION_NAME,
                            "rrf k",
                        )?),
                        weights: vec![],
                    })
                } else {
                    FusionMethod::Default(Fusion::Rrf)
                }
            }
            "DBSF" => {
                if args.len() != 1 {
                    return plan_err!(
                        "{QDRANT_FUSION_SCORE_FUNCTION_NAME} only admits an extra parameter for \
                         RRF"
                    );
                }
                FusionMethod::Default(Fusion::Dbsf)
            }
            _ => {
                return plan_err!(
                    "{QDRANT_FUSION_SCORE_FUNCTION_NAME} fusion method must be 'RRF' or 'DBSF'"
                );
            }
        };
        Ok(Some(Self { method }))
    }

    pub(crate) fn same_semantics(&self, other: &Self) -> bool { self == other }

    pub(super) fn validate_on_source(&self, _source: &Source) -> Result<()> { Ok(()) }

    pub(super) fn descriptor(&self, prefetch_count: usize) -> Result<QueryDescriptor> {
        if prefetch_count == 0 {
            return plan_err!(
                "{QDRANT_FUSION_SCORE_FUNCTION_NAME} requires one or more qdrant prefetch branches"
            );
        }
        let query = match &self.method {
            FusionMethod::Default(fusion) => {
                Query { variant: Some(query::Variant::Fusion(*fusion as i32)) }
            }
            FusionMethod::Rrf(rrf) => Query { variant: Some(query::Variant::Rrf(rrf.clone())) },
        };
        Ok(QueryDescriptor::new(query, None))
    }
}
