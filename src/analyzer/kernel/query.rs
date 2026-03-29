use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::LogicalPlan;
use qdrant_client::Qdrant;

use super::super::op::QueryOp;
use super::super::query::{
    QueryBatchRequestPlan, QueryBranchPlan, QueryGroupsRequestPlan, QueryPointsRequestPlan,
    QueryRequestPlan,
};
use super::super::source::Source;
use crate::pushdown::filter::QdrantFilters;

#[derive(Debug, Clone)]
pub(crate) struct QueryKernel {
    source:  Source,
    filters: QdrantFilters,
    query:   QueryOp,
    limit:   u64,
}

impl QueryKernel {
    pub(crate) fn new(source: Source, filters: QdrantFilters, query: QueryOp, limit: u64) -> Self {
        Self { source, filters, query, limit }
    }

    pub(super) fn project(mut self, plan: &LogicalPlan) -> Result<Option<Self>> {
        let Some(query) = self.query.project(plan)? else {
            return Ok(None);
        };
        self.query = query;
        Ok(Some(self))
    }

    pub(crate) fn branch_plan(&self) -> Result<QueryBranchPlan> {
        self.query.branch_plan(Some(self.filters.clone()), Some(self.limit))
    }

    pub(crate) fn client(&self) -> Arc<Qdrant> { Arc::clone(self.source.client()) }

    pub(crate) fn source(&self) -> &Source { &self.source }

    pub(crate) fn collection(&self) -> &str { self.source.collection() }

    pub(crate) fn filters(&self) -> &QdrantFilters { &self.filters }

    pub(crate) fn query(&self) -> &QueryOp { &self.query }

    pub(crate) fn request_plan(&self, output_schema: &SchemaRef) -> Result<QueryRequestPlan> {
        QueryRequestPlan::points(
            QueryPointsRequestPlan::new(
                self.source.collection().to_owned(),
                self.query.branch_plan(Some(self.filters.clone()), Some(self.limit))?,
                output_schema,
            ),
            self.query.score_output_names(),
        )
    }

    pub(crate) fn limit(&self) -> u64 { self.limit }
}

#[derive(Debug, Clone)]
pub(crate) struct QueryBatchKernel {
    queries: Vec<QueryKernel>,
}

impl QueryBatchKernel {
    pub(crate) fn try_new(queries: Vec<QueryKernel>) -> Result<Self> {
        let Some(first) = queries.first() else {
            return plan_err!("qdrant query batch requires at least one query kernel");
        };
        if queries.iter().skip(1).any(|query| !first.source.merge_compatible_with(&query.source)) {
            return plan_err!("qdrant query batch requires merge-compatible query kernels");
        }
        let score_outputs = first.query.score_output_names();
        if queries.iter().skip(1).any(|query| query.query.score_output_names() != score_outputs) {
            return plan_err!("qdrant query batch requires consistent score output names");
        }
        Ok(Self { queries })
    }

    pub(crate) fn client(&self) -> Arc<Qdrant> {
        self.queries.first().expect("validated query batch kernel").client()
    }

    pub(crate) fn collection(&self) -> &str {
        self.queries.first().expect("validated query batch kernel").collection()
    }

    pub(crate) fn request_plan(&self, output_schema: &SchemaRef) -> Result<QueryRequestPlan> {
        let queries = self
            .queries
            .iter()
            .map(|query| {
                Ok(QueryPointsRequestPlan::new(
                    query.source.collection().to_owned(),
                    query.query.branch_plan(Some(query.filters.clone()), Some(query.limit))?,
                    output_schema,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        QueryRequestPlan::batch(
            QueryBatchRequestPlan::new(self.collection().to_owned(), queries),
            self.queries.first().expect("validated query batch kernel").query.score_output_names(),
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueryGroupsKernel {
    source:           Source,
    filters:          QdrantFilters,
    query:            QueryOp,
    limit:            Option<u64>,
    group_by:         String,
    group_size:       u64,
    group_descending: bool,
}

impl QueryGroupsKernel {
    pub(crate) fn new(
        source: Source,
        filters: QdrantFilters,
        query: QueryOp,
        limit: Option<u64>,
        group_by: String,
        group_size: u64,
        group_descending: bool,
    ) -> Self {
        Self { source, filters, query, limit, group_by, group_size, group_descending }
    }

    pub(super) fn project(mut self, plan: &LogicalPlan) -> Result<Option<Self>> {
        let Some(query) = self.query.project(plan)? else {
            return Ok(None);
        };
        self.query = query;
        Ok(Some(self))
    }

    pub(crate) fn client(&self) -> Arc<Qdrant> { Arc::clone(self.source.client()) }

    pub(crate) fn collection(&self) -> &str { self.source.collection() }

    pub(crate) fn group_by(&self) -> &str { &self.group_by }

    pub(crate) fn group_size(&self) -> u64 { self.group_size }

    pub(crate) fn group_descending(&self) -> bool { self.group_descending }

    pub(crate) fn limit(&self) -> Option<u64> { self.limit }

    pub(crate) fn with_limit(mut self, limit: Option<u64>) -> Self {
        self.limit = limit;
        self
    }

    pub(crate) fn request_plan(&self, output_schema: &SchemaRef) -> Result<QueryRequestPlan> {
        QueryRequestPlan::groups(
            QueryGroupsRequestPlan::new(
                self.source.collection().to_owned(),
                self.query.branch_plan(Some(self.filters.clone()), self.limit)?,
                output_schema,
                self.group_by.clone(),
                self.group_size,
            ),
            self.query.score_output_names(),
        )
    }
}
