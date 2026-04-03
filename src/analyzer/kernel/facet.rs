use std::sync::Arc;

use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;
use qdrant_client::Qdrant;

use super::super::op::FacetOp;
use super::super::source::Source;
use crate::qdrant::filter::QdrantFilters;

#[derive(Debug, Clone)]
pub(crate) struct FacetKernel {
    source:  Source,
    filters: QdrantFilters,
    op:      FacetOp,
    limit:   u64,
}

impl FacetKernel {
    pub(crate) fn new(source: Source, filters: QdrantFilters, op: FacetOp, limit: u64) -> Self {
        Self { source, filters, op, limit }
    }

    pub(super) fn project(mut self, plan: &LogicalPlan) -> Result<Option<Self>> {
        let Some(op) = self.op.project(plan)? else {
            return Ok(None);
        };
        self.op = op;
        Ok(Some(self))
    }

    pub(crate) fn client(&self) -> Arc<Qdrant> { Arc::clone(self.source.client()) }

    pub(crate) fn collection(&self) -> &str { self.source.collection() }

    pub(crate) fn filters(&self) -> &QdrantFilters { &self.filters }

    pub(crate) fn op(&self) -> &FacetOp { &self.op }

    pub(crate) fn limit(&self) -> u64 { self.limit }
}
