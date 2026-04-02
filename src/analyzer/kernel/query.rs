use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Column, DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::LogicalPlan;
use qdrant_client::Qdrant;

use super::super::op::QueryOp;
use super::super::query::{
    QueryBatchRequestPlan, QueryBranchPlan, QueryGroupsRequestPlan, QueryPointsRequestPlan,
    QueryPrefetchBranch, QueryRequestPlan,
};
use super::super::source::Source;
use crate::pushdown::filter::QdrantFilters;

#[derive(Debug, Clone)]
pub(crate) struct QueryKernel {
    source: Source,
    filters: QdrantFilters,
    query: QueryOp,
    limit: u64,
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
        self.query.branch_plan(&self.source, Some(self.filters.clone()), Some(self.limit))
    }

    pub(crate) fn prefetch_branch(
        &self,
        output_schema: &DFSchemaRef,
    ) -> Result<QueryPrefetchBranch> {
        Ok(QueryPrefetchBranch::new(self.branch_plan()?, self.score_output_columns(output_schema)?))
    }

    fn score_output_columns(&self, output_schema: &DFSchemaRef) -> Result<BTreeSet<Column>> {
        self.query
            .score_output_names()
            .into_iter()
            .map(|name| {
                let (qualifier, field) =
                    output_schema.qualified_field_with_unqualified_name(&name)?;
                Ok(Column::new(qualifier.cloned(), field.name().clone()))
            })
            .collect()
    }

    fn unqualified_score_output_columns(&self) -> BTreeSet<Column> {
        self.query.score_output_names().into_iter().map(Column::from_name).collect()
    }

    fn effective_score_output_names(&self, output_schema: &SchemaRef) -> BTreeSet<String> {
        let mut score_outputs = self.query.score_output_names();
        let payload_output_paths = self.query.payload_output_paths();
        for field in output_schema.fields() {
            if field.data_type() == &DataType::Float32
                && !payload_output_paths.contains_key(field.name())
            {
                let _ = score_outputs.insert(field.name().clone());
            }
        }
        score_outputs
    }

    pub(crate) fn prefetch_branch_unqualified(&self) -> Result<QueryPrefetchBranch> {
        Ok(QueryPrefetchBranch::new(self.branch_plan()?, self.unqualified_score_output_columns()))
    }

    pub(crate) fn client(&self) -> Arc<Qdrant> {
        Arc::clone(self.source.client())
    }

    pub(crate) fn source(&self) -> &Source {
        &self.source
    }

    pub(crate) fn collection(&self) -> &str {
        self.source.collection()
    }

    pub(crate) fn filters(&self) -> &QdrantFilters {
        &self.filters
    }

    pub(crate) fn query(&self) -> &QueryOp {
        &self.query
    }

    pub(crate) fn request_plan(&self, output_schema: &SchemaRef) -> Result<QueryRequestPlan> {
        let payload_output_paths = self.query.payload_output_paths();
        let score_output_names = self.effective_score_output_names(output_schema);
        QueryRequestPlan::points(
            QueryPointsRequestPlan::new(
                self.source.collection().to_owned(),
                self.query.branch_plan(
                    &self.source,
                    Some(self.filters.clone()),
                    Some(self.limit),
                )?,
                output_schema,
                !payload_output_paths.is_empty(),
            ),
            score_output_names,
            payload_output_paths,
        )
    }

    pub(crate) fn limit(&self) -> u64 {
        self.limit
    }
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
        let payload_outputs = first.query.payload_output_paths();
        if queries.iter().skip(1).any(|query| query.query.payload_output_paths() != payload_outputs)
        {
            return plan_err!("qdrant query batch requires consistent payload output paths");
        }
        Ok(Self { queries })
    }

    pub(crate) fn client(&self) -> Arc<Qdrant> {
        self.queries.first().expect("validated query batch kernel").client()
    }

    pub(crate) fn collection(&self) -> &str {
        self.queries.first().expect("validated query batch kernel").collection()
    }

    pub(crate) fn source(&self) -> &Source {
        self.queries.first().expect("validated query batch kernel").source()
    }

    pub(crate) fn prefetch_branches(&self) -> Result<Vec<QueryPrefetchBranch>> {
        self.queries.iter().map(QueryKernel::prefetch_branch_unqualified).collect()
    }

    pub(crate) fn request_plan(&self, output_schema: &SchemaRef) -> Result<QueryRequestPlan> {
        let score_output_names = self
            .queries
            .first()
            .expect("validated query batch kernel")
            .effective_score_output_names(output_schema);
        let payload_output_paths = self
            .queries
            .first()
            .expect("validated query batch kernel")
            .query
            .payload_output_paths();
        let queries = self
            .queries
            .iter()
            .map(|query| {
                Ok(QueryPointsRequestPlan::new(
                    query.source.collection().to_owned(),
                    query.query.branch_plan(
                        &query.source,
                        Some(query.filters.clone()),
                        Some(query.limit),
                    )?,
                    output_schema,
                    !payload_output_paths.is_empty(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        QueryRequestPlan::batch(
            QueryBatchRequestPlan::new(self.collection().to_owned(), queries),
            score_output_names,
            payload_output_paths,
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueryGroupsKernel {
    source: Source,
    filters: QdrantFilters,
    query: QueryOp,
    limit: Option<u64>,
    group_by: String,
    group_size: u64,
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

    pub(crate) fn client(&self) -> Arc<Qdrant> {
        Arc::clone(self.source.client())
    }

    pub(crate) fn collection(&self) -> &str {
        self.source.collection()
    }

    pub(crate) fn group_by(&self) -> &str {
        &self.group_by
    }

    pub(crate) fn group_size(&self) -> u64 {
        self.group_size
    }

    pub(crate) fn group_descending(&self) -> bool {
        self.group_descending
    }

    pub(crate) fn limit(&self) -> Option<u64> {
        self.limit
    }

    pub(crate) fn with_limit(mut self, limit: Option<u64>) -> Self {
        self.limit = limit;
        self
    }

    pub(crate) fn request_plan(&self, output_schema: &SchemaRef) -> Result<QueryRequestPlan> {
        let payload_output_paths = self.query.payload_output_paths();
        let mut score_output_names = self.query.score_output_names();
        for field in output_schema.fields() {
            if field.data_type() == &DataType::Float32
                && !payload_output_paths.contains_key(field.name())
            {
                let _ = score_output_names.insert(field.name().clone());
            }
        }
        QueryRequestPlan::groups(
            QueryGroupsRequestPlan::new(
                self.source.collection().to_owned(),
                self.query.branch_plan(&self.source, Some(self.filters.clone()), self.limit)?,
                output_schema,
                !payload_output_paths.is_empty(),
                self.group_by.clone(),
                self.group_size,
            ),
            score_output_names,
            payload_output_paths,
        )
    }
}
