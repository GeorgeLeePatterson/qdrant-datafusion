use std::sync::Arc;

use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan};
use qdrant_client::Qdrant;

use super::op::{FacetOp, QueryOp};
use super::source::Source;
use crate::pushdown::filter::QdrantFilters;

#[derive(Debug, Clone)]
pub(crate) enum KernelSpec {
    Count(CountKernel),
    Query(QueryKernel),
    Facet(FacetKernel),
}

impl KernelSpec {
    pub(super) fn project(self, plan: &LogicalPlan) -> Result<Option<Self>> {
        match self {
            Self::Count(_) => Ok(None),
            Self::Query(mut kernel) => {
                let Some(query) = kernel.query.project(plan)? else {
                    return Ok(None);
                };
                kernel.query = query;
                Ok(Some(Self::Query(kernel)))
            }
            Self::Facet(mut kernel) => {
                let Some(op) = kernel.op.project(plan)? else {
                    return Ok(None);
                };
                kernel.op = op;
                Ok(Some(Self::Facet(kernel)))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CountKernel {
    pub(super) source:  Source,
    pub(super) filters: QdrantFilters,
}

#[derive(Debug, Clone)]
pub(crate) struct QueryKernel {
    pub(super) source:  Source,
    pub(super) filters: QdrantFilters,
    pub(super) query:   QueryOp,
    pub(super) limit:   u64,
}

#[derive(Debug, Clone)]
pub(crate) struct FacetKernel {
    pub(super) source:  Source,
    pub(super) filters: QdrantFilters,
    pub(super) op:      FacetOp,
    pub(super) limit:   u64,
}

impl CountKernel {
    pub(crate) fn client(&self) -> Arc<Qdrant> { Arc::clone(self.source.client()) }

    pub(crate) fn collection(&self) -> &str { self.source.collection() }

    pub(crate) fn filters(&self) -> &QdrantFilters { &self.filters }
}

impl QueryKernel {
    pub(crate) fn client(&self) -> Arc<Qdrant> { Arc::clone(self.source.client()) }

    pub(crate) fn collection(&self) -> &str { self.source.collection() }

    pub(crate) fn filters(&self) -> &QdrantFilters { &self.filters }

    pub(crate) fn query(&self) -> &QueryOp { &self.query }

    pub(crate) fn limit(&self) -> u64 { self.limit }
}

impl FacetKernel {
    pub(crate) fn client(&self) -> Arc<Qdrant> { Arc::clone(self.source.client()) }

    pub(crate) fn collection(&self) -> &str { self.source.collection() }

    pub(crate) fn filters(&self) -> &QdrantFilters { &self.filters }

    pub(crate) fn op(&self) -> &FacetOp { &self.op }

    pub(crate) fn limit(&self) -> u64 { self.limit }
}

pub(super) fn limit_rows(plan: &LogicalPlan) -> Result<u64> {
    fn integer_literal_u64(expr: &Expr) -> Result<u64> {
        match expr.clone().unalias_nested().data {
            Expr::Cast(cast) => integer_literal_u64(&cast.expr),
            Expr::TryCast(cast) => integer_literal_u64(&cast.expr),
            Expr::Literal(value, _) => match value {
                ScalarValue::Int8(Some(value)) if value >= 0 => Ok(value as u64),
                ScalarValue::Int16(Some(value)) if value >= 0 => Ok(value as u64),
                ScalarValue::Int32(Some(value)) if value >= 0 => Ok(value as u64),
                ScalarValue::Int64(Some(value)) if value >= 0 => Ok(value as u64),
                ScalarValue::UInt8(Some(value)) => Ok(u64::from(value)),
                ScalarValue::UInt16(Some(value)) => Ok(u64::from(value)),
                ScalarValue::UInt32(Some(value)) => Ok(u64::from(value)),
                ScalarValue::UInt64(Some(value)) => Ok(value),
                _ => plan_err!("qdrant limit requires a non-negative integer literal"),
            },
            _ => plan_err!("qdrant limit requires a non-negative integer literal"),
        }
    }

    let LogicalPlan::Limit(limit) = plan else {
        return plan_err!("prototype limit state mismatch");
    };
    match limit.skip.as_deref() {
        None => {}
        Some(expr) if integer_literal_u64(expr)? == 0 => {}
        _ => return plan_err!("qdrant limit does not support skip"),
    }
    match limit.fetch.as_deref() {
        Some(expr) => {
            let value = integer_literal_u64(expr)?;
            if value > 0 {
                Ok(value)
            } else {
                plan_err!("qdrant limit requires a positive literal fetch")
            }
        }
        None => plan_err!("qdrant limit requires a positive literal fetch"),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
pub(super) fn numeric_literal_f32(expr: &Expr) -> Result<f32> {
    match expr.clone().unalias_nested().data {
        Expr::Negative(expr) => Ok(-numeric_literal_f32(&expr)?),
        Expr::Cast(cast) => numeric_literal_f32(&cast.expr),
        Expr::TryCast(cast) => numeric_literal_f32(&cast.expr),
        Expr::Literal(value, _) => match value {
            ScalarValue::Float32(Some(value)) => Ok(value),
            ScalarValue::Float64(Some(value)) => Ok(value as f32),
            ScalarValue::Int8(Some(value)) => Ok(f32::from(value)),
            ScalarValue::Int16(Some(value)) => Ok(f32::from(value)),
            ScalarValue::Int32(Some(value)) => Ok(value as f32),
            ScalarValue::Int64(Some(value)) => Ok(value as f32),
            ScalarValue::UInt8(Some(value)) => Ok(f32::from(value)),
            ScalarValue::UInt16(Some(value)) => Ok(f32::from(value)),
            ScalarValue::UInt32(Some(value)) => Ok(value as f32),
            ScalarValue::UInt64(Some(value)) => Ok(value as f32),
            _ => plan_err!("score thresholds must be numeric"),
        },
        _ => plan_err!("score thresholds must be numeric"),
    }
}
