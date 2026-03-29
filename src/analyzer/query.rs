mod context;
mod discover;
mod formula;
mod fusion;
mod nearest;
mod nearest_with_mmr;
mod order_by;
mod recommend;
mod relevance_feedback;
mod sample;

use std::collections::BTreeSet;

use datafusion::common::Result;
use qdrant_client::qdrant::{
    Filter, PrefetchQuery, Query, QueryBatchPoints, QueryPointGroups, QueryPoints,
    VectorsSelector, WithPayloadSelector, WithVectorsSelector, with_payload_selector,
    with_vectors_selector,
};

use super::source::Source;
use super::surface::QuerySurfaceCall;
use crate::arrow::schema::{
    PAYLOAD_FIELD_NAME, UNNAMED_VECTOR_FIELD_NAME, dense_vector_width, is_multi_vector_field,
    is_sparse_vector_field,
};

pub(crate) use self::context::ContextQuery;
pub(crate) use self::discover::DiscoverQuery;
pub(crate) use self::formula::FormulaQuery;
pub(crate) use self::fusion::FusionQuery;
pub(crate) use self::nearest::NearestQuery;
pub(crate) use self::nearest_with_mmr::NearestWithMmrQuery;
pub(crate) use self::order_by::OrderByQuery;
pub(crate) use self::recommend::RecommendQuery;
pub(crate) use self::relevance_feedback::RelevanceFeedbackQuery;
pub(crate) use self::sample::SampleQuery;

#[derive(Debug, Clone)]
pub(crate) struct QueryDescriptor {
    query: Query,
    using: Option<String>,
}

impl QueryDescriptor {
    pub(crate) fn new(query: Query, using: Option<String>) -> Self {
        Self { query, using }
    }

    fn into_parts(self) -> (Query, Option<String>) {
        (self.query, self.using)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueryVectorsSelector {
    None,
    All,
    Named(Vec<String>),
}

impl QueryVectorsSelector {
    pub(crate) fn from_schema(schema: &datafusion::arrow::datatypes::SchemaRef) -> Self {
        let vector_names = schema
            .fields()
            .iter()
            .filter(|field| {
                dense_vector_width(field).is_some()
                    || is_multi_vector_field(field)
                    || is_sparse_vector_field(field)
            })
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        if vector_names.is_empty() {
            Self::None
        } else if vector_names.len() == 1 && vector_names[0] == UNNAMED_VECTOR_FIELD_NAME {
            Self::All
        } else {
            Self::Named(vector_names)
        }
    }

    fn into_proto(self) -> WithVectorsSelector {
        let selector_options = match self {
            Self::None => with_vectors_selector::SelectorOptions::Enable(false),
            Self::All => with_vectors_selector::SelectorOptions::Enable(true),
            Self::Named(names) => {
                with_vectors_selector::SelectorOptions::Include(VectorsSelector { names })
            }
        };
        WithVectorsSelector { selector_options: Some(selector_options) }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueryBranchPlan {
    pub(crate) prefetch: Vec<QueryBranchPlan>,
    pub(crate) descriptor: Option<QueryDescriptor>,
    pub(crate) filter: Option<Filter>,
    pub(crate) score_threshold: Option<f32>,
    pub(crate) limit: Option<u64>,
}

impl QueryBranchPlan {
    pub(crate) fn descriptor(
        descriptor: QueryDescriptor,
        filter: Option<Filter>,
        score_threshold: Option<f32>,
        limit: Option<u64>,
    ) -> Self {
        Self {
            prefetch: vec![],
            descriptor: Some(descriptor),
            filter,
            score_threshold,
            limit,
        }
    }

    pub(crate) fn single(
        descriptor: QueryDescriptor,
        filter: Option<Filter>,
        score_threshold: Option<f32>,
        limit: u64,
    ) -> Self {
        Self::descriptor(descriptor, filter, score_threshold, Some(limit))
    }

    fn into_prefetch_proto(self) -> Result<PrefetchQuery> {
        let (query, using) = match self.descriptor {
            Some(descriptor) => {
                let (query, using) = descriptor.into_parts();
                (Some(query), using)
            }
            None => (None, None),
        };
        Ok(PrefetchQuery {
            prefetch: self
                .prefetch
                .into_iter()
                .map(Self::into_prefetch_proto)
                .collect::<Result<Vec<_>>>()?,
            query,
            using,
            filter: self.filter,
            params: None,
            score_threshold: self.score_threshold,
            limit: self.limit,
            lookup_from: None,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueryPointsRequestPlan {
    collection: String,
    branch: QueryBranchPlan,
    offset: Option<u64>,
    payload: bool,
    vectors: QueryVectorsSelector,
}

impl QueryPointsRequestPlan {
    pub(crate) fn new(
        collection: String,
        branch: QueryBranchPlan,
        output_schema: &datafusion::arrow::datatypes::SchemaRef,
    ) -> Self {
        let payload = output_schema.fields().iter().any(|field| field.name() == PAYLOAD_FIELD_NAME);
        let vectors = QueryVectorsSelector::from_schema(output_schema);
        Self { collection, branch, offset: None, payload, vectors }
    }

    fn into_proto(self) -> Result<QueryPoints> {
        let (query, using) = match self.branch.descriptor {
            Some(descriptor) => {
                let (query, using) = descriptor.into_parts();
                (Some(query), using)
            }
            None => (None, None),
        };
        Ok(QueryPoints {
            collection_name: self.collection,
            prefetch: self
                .branch
                .prefetch
                .into_iter()
                .map(QueryBranchPlan::into_prefetch_proto)
                .collect::<Result<Vec<_>>>()?,
            query,
            using,
            filter: self.branch.filter,
            params: None,
            score_threshold: self.branch.score_threshold,
            limit: self.branch.limit,
            offset: self.offset,
            with_vectors: Some(self.vectors.into_proto()),
            with_payload: Some(WithPayloadSelector {
                selector_options: Some(with_payload_selector::SelectorOptions::Enable(self.payload)),
            }),
            read_consistency: None,
            shard_key_selector: None,
            lookup_from: None,
            timeout: None,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueryBatchRequestPlan {
    collection: String,
    queries: Vec<QueryPointsRequestPlan>,
}

impl QueryBatchRequestPlan {
    pub(crate) fn new(collection: String, queries: Vec<QueryPointsRequestPlan>) -> Self {
        Self { collection, queries }
    }

    fn into_proto(self) -> Result<QueryBatchPoints> {
        Ok(QueryBatchPoints {
            collection_name: self.collection,
            query_points: self
                .queries
                .into_iter()
                .map(QueryPointsRequestPlan::into_proto)
                .collect::<Result<Vec<_>>>()?,
            read_consistency: None,
            timeout: None,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueryGroupsRequestPlan {
    collection: String,
    branch: QueryBranchPlan,
    payload: bool,
    vectors: QueryVectorsSelector,
    group_by: String,
    group_size: u64,
}

impl QueryGroupsRequestPlan {
    pub(crate) fn new(
        collection: String,
        branch: QueryBranchPlan,
        output_schema: &datafusion::arrow::datatypes::SchemaRef,
        group_by: String,
        group_size: u64,
    ) -> Self {
        let payload = output_schema.fields().iter().any(|field| field.name() == PAYLOAD_FIELD_NAME);
        let vectors = QueryVectorsSelector::from_schema(output_schema);
        Self { collection, branch, payload, vectors, group_by, group_size }
    }

    fn into_proto(self) -> Result<QueryPointGroups> {
        let (query, using) = match self.branch.descriptor {
            Some(descriptor) => {
                let (query, using) = descriptor.into_parts();
                (Some(query), using)
            }
            None => (None, None),
        };
        Ok(QueryPointGroups {
            collection_name: self.collection,
            prefetch: self
                .branch
                .prefetch
                .into_iter()
                .map(QueryBranchPlan::into_prefetch_proto)
                .collect::<Result<Vec<_>>>()?,
            query,
            using,
            filter: self.branch.filter,
            params: None,
            score_threshold: self.branch.score_threshold,
            with_payload: Some(WithPayloadSelector {
                selector_options: Some(with_payload_selector::SelectorOptions::Enable(self.payload)),
            }),
            with_vectors: Some(self.vectors.into_proto()),
            lookup_from: None,
            limit: self.branch.limit,
            group_size: Some(self.group_size),
            group_by: self.group_by,
            read_consistency: None,
            with_lookup: None,
            timeout: None,
            shard_key_selector: None,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) enum QueryRequest {
    Points(QueryPoints),
    Batch(QueryBatchPoints),
    Groups(QueryPointGroups),
}

#[derive(Debug, Clone)]
pub(crate) struct QueryRequestPlan {
    request: QueryRequest,
    score_output_names: BTreeSet<String>,
}

impl QueryRequestPlan {
    pub(crate) fn points(
        request: QueryPointsRequestPlan,
        score_output_names: BTreeSet<String>,
    ) -> Result<Self> {
        Ok(Self { request: QueryRequest::Points(request.into_proto()?), score_output_names })
    }

    pub(crate) fn batch(
        request: QueryBatchRequestPlan,
        score_output_names: BTreeSet<String>,
    ) -> Result<Self> {
        Ok(Self { request: QueryRequest::Batch(request.into_proto()?), score_output_names })
    }

    pub(crate) fn groups(
        request: QueryGroupsRequestPlan,
        score_output_names: BTreeSet<String>,
    ) -> Result<Self> {
        Ok(Self { request: QueryRequest::Groups(request.into_proto()?), score_output_names })
    }

    pub(crate) fn request(&self) -> &QueryRequest {
        &self.request
    }

    pub(crate) fn score_output_names(&self) -> &BTreeSet<String> {
        &self.score_output_names
    }
}

#[derive(Debug, Clone)]
pub(crate) enum QueryKind {
    Nearest(NearestQuery),
    Recommend(RecommendQuery),
    Discover(DiscoverQuery),
    Context(ContextQuery),
    OrderBy(OrderByQuery),
    Fusion(FusionQuery),
    Sample(SampleQuery),
    Formula(FormulaQuery),
    NearestWithMmr(NearestWithMmrQuery),
    RelevanceFeedback(RelevanceFeedbackQuery),
}

impl QueryKind {
    pub(super) fn from_surface(surface: QuerySurfaceCall) -> Self {
        match surface {
            QuerySurfaceCall::Nearest(query) => Self::Nearest(query),
            QuerySurfaceCall::Recommend(query) => Self::Recommend(query),
            QuerySurfaceCall::Discover(query) => Self::Discover(query),
            QuerySurfaceCall::Context(query) => Self::Context(query),
            QuerySurfaceCall::OrderBy(query) => Self::OrderBy(query),
            QuerySurfaceCall::Fusion(query) => Self::Fusion(query),
            QuerySurfaceCall::Sample(query) => Self::Sample(query),
            QuerySurfaceCall::Formula(query) => Self::Formula(query),
            QuerySurfaceCall::NearestWithMmr(query) => Self::NearestWithMmr(query),
            QuerySurfaceCall::RelevanceFeedback(query) => Self::RelevanceFeedback(query),
        }
    }

    pub(super) fn validate_on_source(&self, source: &Source) -> Result<()> {
        match self {
            Self::Nearest(query) => query.validate_on_source(source),
            Self::Recommend(query) => query.validate_on_source(source),
            Self::Discover(query) => query.validate_on_source(source),
            Self::Context(query) => query.validate_on_source(source),
            Self::OrderBy(query) => query.validate_on_source(source),
            Self::Fusion(query) => query.validate_on_source(source),
            Self::Sample(query) => query.validate_on_source(source),
            Self::Formula(query) => query.validate_on_source(source),
            Self::NearestWithMmr(query) => query.validate_on_source(source),
            Self::RelevanceFeedback(query) => query.validate_on_source(source),
        }
    }

    pub(super) fn matches_surface(&self, surface: &QuerySurfaceCall) -> bool {
        match (self, surface) {
            (Self::Nearest(lhs), QuerySurfaceCall::Nearest(rhs)) => lhs.same_semantics(rhs),
            (Self::Recommend(lhs), QuerySurfaceCall::Recommend(rhs)) => lhs.same_semantics(rhs),
            (Self::Discover(lhs), QuerySurfaceCall::Discover(rhs)) => lhs.same_semantics(rhs),
            (Self::Context(lhs), QuerySurfaceCall::Context(rhs)) => lhs.same_semantics(rhs),
            (Self::OrderBy(lhs), QuerySurfaceCall::OrderBy(rhs)) => lhs.same_semantics(rhs),
            (Self::Fusion(lhs), QuerySurfaceCall::Fusion(rhs)) => lhs.same_semantics(rhs),
            (Self::Sample(lhs), QuerySurfaceCall::Sample(rhs)) => lhs.same_semantics(rhs),
            (Self::Formula(lhs), QuerySurfaceCall::Formula(rhs)) => lhs.same_semantics(rhs),
            (Self::NearestWithMmr(lhs), QuerySurfaceCall::NearestWithMmr(rhs)) => {
                lhs.same_semantics(rhs)
            }
            (Self::RelevanceFeedback(lhs), QuerySurfaceCall::RelevanceFeedback(rhs)) => {
                lhs.same_semantics(rhs)
            }
            _ => false,
        }
    }

    pub(crate) fn descriptor(&self) -> Result<QueryDescriptor> {
        match self {
            Self::Nearest(query) => query.descriptor(),
            Self::Recommend(query) => query.descriptor(),
            Self::Discover(query) => query.descriptor(),
            Self::Context(query) => query.descriptor(),
            Self::OrderBy(query) => query.descriptor(),
            Self::Fusion(query) => query.descriptor(),
            Self::Sample(query) => query.descriptor(),
            Self::Formula(query) => query.descriptor(),
            Self::NearestWithMmr(query) => query.descriptor(),
            Self::RelevanceFeedback(query) => query.descriptor(),
        }
    }
}
