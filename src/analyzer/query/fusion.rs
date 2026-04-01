use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::{Fusion, Query, Rrf, query};

use super::{QueryDescriptor, string_literal, u32_literal};
use crate::expr_fn::{FUSION_SCORE_FUNCTION_NAME, FusionCall};

#[derive(Debug, Clone, PartialEq)]
enum FusionMethod {
    Default(Fusion),
    Rrf(Rrf),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FusionQuery {
    method:       FusionMethod,
    score_inputs: Vec<Expr>,
}

impl TryFrom<FusionCall> for FusionQuery {
    type Error = datafusion::error::DataFusionError;

    fn try_from(call: FusionCall) -> Result<Self> {
        let method = string_literal(&call.method, FUSION_SCORE_FUNCTION_NAME, "method")?;
        let method = match method.to_ascii_uppercase().as_str() {
            "RRF" => {
                if let Some(rrf_k) = &call.rrf_k {
                    FusionMethod::Rrf(Rrf {
                        k:       Some(u32_literal(rrf_k, FUSION_SCORE_FUNCTION_NAME, "rrf k")?),
                        weights: vec![],
                    })
                } else {
                    FusionMethod::Default(Fusion::Rrf)
                }
            }
            "DBSF" => {
                if call.rrf_k.is_some() {
                    return plan_err!(
                        "{FUSION_SCORE_FUNCTION_NAME} only admits an extra parameter for RRF"
                    );
                }
                FusionMethod::Default(Fusion::Dbsf)
            }
            _ => {
                return plan_err!(
                    "{FUSION_SCORE_FUNCTION_NAME} fusion method must be 'RRF' or 'DBSF'"
                );
            }
        };
        Ok(Self { method, score_inputs: call.score_inputs })
    }
}

impl FusionQuery {
    pub(crate) fn same_semantics(&self, other: &Self) -> bool { self == other }

    pub(crate) fn has_explicit_inputs(&self) -> bool { !self.score_inputs.is_empty() }

    pub(crate) fn score_inputs(&self) -> &[Expr] { &self.score_inputs }

    pub(super) fn descriptor(&self, prefetch_count: usize) -> Result<QueryDescriptor> {
        if prefetch_count == 0 {
            return plan_err!(
                "{FUSION_SCORE_FUNCTION_NAME} requires one or more qdrant prefetch branches"
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
