mod count;
mod facet;
mod query;

use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan};

pub(crate) use self::count::CountKernel;
pub(crate) use self::facet::FacetKernel;
pub(crate) use self::query::{QueryBatchKernel, QueryGroupsKernel, QueryKernel};

#[derive(Debug, Clone)]
pub(crate) enum KernelSpec {
    Count(CountKernel),
    Query(QueryKernel),
    QueryBatch(QueryBatchKernel),
    QueryGroups(QueryGroupsKernel),
    Facet(FacetKernel),
}

impl KernelSpec {
    pub(super) fn project(self, plan: &LogicalPlan) -> Result<Option<Self>> {
        match self {
            Self::Count(_) | Self::QueryBatch(_) | Self::QueryGroups(_) => Ok(None),
            Self::Query(kernel) => kernel.project(plan).map(|kernel| kernel.map(Self::Query)),
            Self::Facet(kernel) => kernel.project(plan).map(|kernel| kernel.map(Self::Facet)),
        }
    }
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
