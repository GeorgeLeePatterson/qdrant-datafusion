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

use datafusion::arrow::array::Array;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::{
    Filter, PointId, PrefetchQuery, Query, QueryBatchPoints, QueryPointGroups, QueryPoints,
    VectorInput, VectorsSelector, WithPayloadSelector, WithVectorsSelector, vector_input,
    with_payload_selector, with_vectors_selector,
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
use super::source::Source;
use super::surface::QuerySurfaceCall;
use crate::arrow::schema::{PAYLOAD_FIELD_NAME, QdrantFieldBinding, UNNAMED_VECTOR_FIELD_NAME};

#[derive(Debug, Clone)]
pub(crate) struct QueryDescriptor {
    query: Query,
    using: Option<String>,
}

impl QueryDescriptor {
    pub(crate) fn new(query: Query, using: Option<String>) -> Self { Self { query, using } }

    fn into_parts(self) -> (Query, Option<String>) { (self.query, self.using) }
}

pub(super) fn string_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<String> {
    match expr.clone().unalias_nested().data {
        Expr::Literal(ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)), _) => {
            Ok(value)
        }
        Expr::Cast(cast) => string_literal(&cast.expr, function_name, argument),
        Expr::TryCast(cast) => string_literal(&cast.expr, function_name, argument),
        _ => plan_err!("{function_name} requires {argument} to be a string literal"),
    }
}

pub(super) fn bool_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<bool> {
    match expr.clone().unalias_nested().data {
        Expr::Literal(ScalarValue::Boolean(Some(value)), _) => Ok(value),
        Expr::Cast(cast) => bool_literal(&cast.expr, function_name, argument),
        Expr::TryCast(cast) => bool_literal(&cast.expr, function_name, argument),
        _ => plan_err!("{function_name} requires {argument} to be a boolean literal"),
    }
}

#[expect(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
pub(super) fn f32_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<f32> {
    match expr.clone().unalias_nested().data {
        Expr::Negative(expr) => Ok(-f32_literal(&expr, function_name, argument)?),
        Expr::Cast(cast) => f32_literal(&cast.expr, function_name, argument),
        Expr::TryCast(cast) => f32_literal(&cast.expr, function_name, argument),
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
            _ => plan_err!("{function_name} requires {argument} to be numeric"),
        },
        _ => plan_err!("{function_name} requires {argument} to be numeric"),
    }
}

pub(super) fn u32_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<u32> {
    match expr.clone().unalias_nested().data {
        Expr::Cast(cast) => u32_literal(&cast.expr, function_name, argument),
        Expr::TryCast(cast) => u32_literal(&cast.expr, function_name, argument),
        Expr::Literal(value, _) => match value {
            ScalarValue::Int8(Some(value)) if value >= 0 => Ok(u32::from(value.unsigned_abs())),
            ScalarValue::Int16(Some(value)) if value >= 0 => Ok(u32::from(value.unsigned_abs())),
            ScalarValue::Int32(Some(value)) if value >= 0 => Ok(value.unsigned_abs()),
            ScalarValue::Int64(Some(value)) if value >= 0 => u32::try_from(value).map_err(|_| {
                datafusion::error::DataFusionError::Plan(format!(
                    "{function_name} requires {argument} to fit in u32"
                ))
            }),
            ScalarValue::UInt8(Some(value)) => Ok(u32::from(value)),
            ScalarValue::UInt16(Some(value)) => Ok(u32::from(value)),
            ScalarValue::UInt32(Some(value)) => Ok(value),
            ScalarValue::UInt64(Some(value)) => u32::try_from(value).map_err(|_| {
                datafusion::error::DataFusionError::Plan(format!(
                    "{function_name} requires {argument} to fit in u32"
                ))
            }),
            _ => plan_err!("{function_name} requires {argument} to be a non-negative integer"),
        },
        _ => plan_err!("{function_name} requires {argument} to be a non-negative integer"),
    }
}

#[derive(Debug, Clone)]
pub(super) enum VectorQueryInput {
    Dense(Vec<f32>),
    Id(PointId),
}

impl VectorQueryInput {
    pub(super) fn same_semantics(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Dense(lhs), Self::Dense(rhs)) => {
                lhs.iter().map(|value| value.to_bits()).eq(rhs.iter().map(|value| value.to_bits()))
            }
            (Self::Id(lhs), Self::Id(rhs)) => {
                match (&lhs.point_id_options, &rhs.point_id_options) {
                    (Some(PointIdOptions::Num(lhs)), Some(PointIdOptions::Num(rhs))) => lhs == rhs,
                    (Some(PointIdOptions::Uuid(lhs)), Some(PointIdOptions::Uuid(rhs))) => {
                        lhs == rhs
                    }
                    (None, None) => true,
                    _ => false,
                }
            }
            _ => false,
        }
    }

    pub(super) fn validate_on_source(
        &self,
        source: &Source,
        using: &str,
        function_name: &str,
        argument: &str,
    ) -> Result<()> {
        match self {
            Self::Dense(vector) => match source.field_binding(using)? {
                QdrantFieldBinding::DenseFixed { width } => {
                    if width != vector.len() {
                        return plan_err!(
                            "{function_name} requires {argument} width to match source vector \
                             width"
                        );
                    }
                    Ok(())
                }
                QdrantFieldBinding::DenseVariable => Ok(()),
                QdrantFieldBinding::MultiDense { .. } => plan_err!(
                    "{function_name} requires {argument} to target a single dense vector field"
                ),
                QdrantFieldBinding::Sparse => {
                    plan_err!("{function_name} requires {argument} to target a dense vector field")
                }
                QdrantFieldBinding::Document => plan_err!(
                    "{function_name} requires {argument} to target a dense vector field, not a \
                     document field"
                ),
                QdrantFieldBinding::Image => plan_err!(
                    "{function_name} requires {argument} to target a dense vector field, not an \
                     image field"
                ),
                QdrantFieldBinding::Object => plan_err!(
                    "{function_name} requires {argument} to target a dense vector field, not an \
                     object field"
                ),
                QdrantFieldBinding::Unsupported(data_type) => plan_err!(
                    "{function_name} requires {argument} to target a supported vector field, \
                     found {:?}",
                    data_type
                ),
            },
            Self::Id(_) => Ok(()),
        }
    }

    pub(super) fn into_proto(self) -> VectorInput {
        match self {
            Self::Dense(vector) => VectorInput {
                variant: Some(vector_input::Variant::Dense(qdrant_client::qdrant::DenseVector {
                    data: vector,
                })),
            },
            Self::Id(point_id) => {
                VectorInput { variant: Some(vector_input::Variant::Id(point_id)) }
            }
        }
    }
}

pub(super) fn vector_input_literal(
    expr: &Expr,
    function_name: &str,
    argument: &str,
) -> Result<VectorQueryInput> {
    match expr.clone().unalias_nested().data {
        Expr::Cast(cast) => vector_input_literal(&cast.expr, function_name, argument),
        Expr::TryCast(cast) => vector_input_literal(&cast.expr, function_name, argument),
        Expr::Literal(value, _) => vector_input_from_scalar(&value, function_name, argument),
        _ => {
            plan_err!("{function_name} requires {argument} to be an id literal or an array literal")
        }
    }
}

pub(super) fn vector_input_list(
    expr: &Expr,
    function_name: &str,
    argument: &str,
) -> Result<Vec<VectorQueryInput>> {
    list_literal(expr, function_name, argument)?
        .iter()
        .map(|value| vector_input_from_scalar(value, function_name, argument))
        .collect()
}

pub(super) fn vector_input_pair_list(
    expr: &Expr,
    function_name: &str,
    argument: &str,
) -> Result<Vec<(VectorQueryInput, VectorQueryInput)>> {
    list_literal(expr, function_name, argument)?
        .iter()
        .map(|value| {
            let values = list_from_scalar(value, function_name, argument)?;
            if values.len() != 2 {
                return plan_err!(
                    "{function_name} requires each {argument} entry to contain exactly two vector \
                     inputs"
                );
            }
            Ok((
                vector_input_from_scalar(&values[0], function_name, argument)?,
                vector_input_from_scalar(&values[1], function_name, argument)?,
            ))
        })
        .collect()
}

pub(super) fn feedback_input_list(
    expr: &Expr,
    function_name: &str,
    argument: &str,
) -> Result<Vec<(VectorQueryInput, f32)>> {
    list_literal(expr, function_name, argument)?
        .iter()
        .map(|value| {
            let values = list_from_scalar(value, function_name, argument)?;
            if values.len() != 2 {
                return plan_err!(
                    "{function_name} requires each {argument} entry to contain an example and a \
                     score"
                );
            }
            Ok((
                vector_input_from_scalar(&values[0], function_name, argument)?,
                scalar_f32(&values[1], function_name, argument)?,
            ))
        })
        .collect()
}

fn vector_input_from_scalar(
    value: &ScalarValue,
    function_name: &str,
    argument: &str,
) -> Result<VectorQueryInput> {
    match value {
        ScalarValue::List(_) | ScalarValue::LargeList(_) => {
            let vector = list_from_scalar(value, function_name, argument)?
                .iter()
                .map(|value| scalar_f32(value, function_name, argument))
                .collect::<Result<Vec<_>>>()?;
            Ok(VectorQueryInput::Dense(vector))
        }
        _ => Ok(VectorQueryInput::Id(point_id_from_scalar(value, function_name, argument)?)),
    }
}

pub(super) fn list_literal(
    expr: &Expr,
    function_name: &str,
    argument: &str,
) -> Result<Vec<ScalarValue>> {
    match expr.clone().unalias_nested().data {
        Expr::Cast(cast) => list_literal(&cast.expr, function_name, argument),
        Expr::TryCast(cast) => list_literal(&cast.expr, function_name, argument),
        Expr::Literal(value, _) => list_from_scalar(&value, function_name, argument),
        _ => plan_err!("{function_name} requires {argument} to be an array literal"),
    }
}

pub(super) fn list_from_scalar(
    value: &ScalarValue,
    function_name: &str,
    argument: &str,
) -> Result<Vec<ScalarValue>> {
    match value {
        ScalarValue::List(array) => {
            if array.is_empty() {
                Ok(vec![])
            } else {
                list_values_from_array(array.value(0).as_ref())
            }
        }
        ScalarValue::LargeList(array) => {
            if array.is_empty() {
                Ok(vec![])
            } else {
                list_values_from_array(array.value(0).as_ref())
            }
        }
        _ => plan_err!("{function_name} requires {argument} to be an array literal"),
    }
}

fn list_values_from_array(array: &dyn Array) -> Result<Vec<ScalarValue>> {
    (0..array.len()).map(|index| ScalarValue::try_from_array(array, index)).collect()
}

fn point_id_from_scalar(
    value: &ScalarValue,
    function_name: &str,
    argument: &str,
) -> Result<PointId> {
    let point_id_options = match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            Some(PointIdOptions::Uuid(value.clone()))
        }
        ScalarValue::Int8(Some(value)) if *value >= 0 => {
            Some(PointIdOptions::Num(u64::from(value.unsigned_abs())))
        }
        ScalarValue::Int16(Some(value)) if *value >= 0 => {
            Some(PointIdOptions::Num(u64::from(value.unsigned_abs())))
        }
        ScalarValue::Int32(Some(value)) if *value >= 0 => {
            Some(PointIdOptions::Num(u64::from(value.unsigned_abs())))
        }
        ScalarValue::Int64(Some(value)) if *value >= 0 => {
            Some(PointIdOptions::Num(value.unsigned_abs()))
        }
        ScalarValue::UInt8(Some(value)) => Some(PointIdOptions::Num(u64::from(*value))),
        ScalarValue::UInt16(Some(value)) => Some(PointIdOptions::Num(u64::from(*value))),
        ScalarValue::UInt32(Some(value)) => Some(PointIdOptions::Num(u64::from(*value))),
        ScalarValue::UInt64(Some(value)) => Some(PointIdOptions::Num(*value)),
        _ => None,
    };
    if let Some(point_id_options) = point_id_options {
        Ok(PointId { point_id_options: Some(point_id_options) })
    } else {
        plan_err!(
            "{function_name} requires {argument} ids to be string or non-negative integer literals"
        )
    }
}

pub(super) fn scalar_string(
    value: &ScalarValue,
    function_name: &str,
    argument: &str,
) -> Result<String> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Ok(value.clone()),
        _ => plan_err!("{function_name} requires {argument} to be a string literal"),
    }
}

#[expect(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
pub(super) fn scalar_f32(value: &ScalarValue, function_name: &str, argument: &str) -> Result<f32> {
    match value {
        ScalarValue::Float32(Some(value)) => Ok(*value),
        ScalarValue::Float64(Some(value)) => Ok(*value as f32),
        ScalarValue::Int8(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::Int16(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::Int32(Some(value)) => Ok(*value as f32),
        ScalarValue::Int64(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt8(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::UInt16(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::UInt32(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt64(Some(value)) => Ok(*value as f32),
        _ => plan_err!("{function_name} requires {argument} to be numeric"),
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
            .filter(|field| QdrantFieldBinding::from_field(field).is_vector())
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
    pub(crate) prefetch:        Vec<QueryBranchPlan>,
    pub(crate) descriptor:      Option<QueryDescriptor>,
    pub(crate) filter:          Option<Filter>,
    pub(crate) score_threshold: Option<f32>,
    pub(crate) limit:           Option<u64>,
}

impl QueryBranchPlan {
    pub(crate) fn descriptor(
        descriptor: QueryDescriptor,
        filter: Option<Filter>,
        score_threshold: Option<f32>,
        limit: Option<u64>,
    ) -> Self {
        Self { prefetch: vec![], descriptor: Some(descriptor), filter, score_threshold, limit }
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
    branch:     QueryBranchPlan,
    offset:     Option<u64>,
    payload:    bool,
    vectors:    QueryVectorsSelector,
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
                selector_options: Some(with_payload_selector::SelectorOptions::Enable(
                    self.payload,
                )),
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
    queries:    Vec<QueryPointsRequestPlan>,
}

impl QueryBatchRequestPlan {
    pub(crate) fn new(collection: String, queries: Vec<QueryPointsRequestPlan>) -> Self {
        Self { collection, queries }
    }

    fn into_proto(self) -> Result<QueryBatchPoints> {
        Ok(QueryBatchPoints {
            collection_name:  self.collection,
            query_points:     self
                .queries
                .into_iter()
                .map(QueryPointsRequestPlan::into_proto)
                .collect::<Result<Vec<_>>>()?,
            read_consistency: None,
            timeout:          None,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueryGroupsRequestPlan {
    collection: String,
    branch:     QueryBranchPlan,
    payload:    bool,
    vectors:    QueryVectorsSelector,
    group_by:   String,
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
                selector_options: Some(with_payload_selector::SelectorOptions::Enable(
                    self.payload,
                )),
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
    request:            QueryRequest,
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

    pub(crate) fn request(&self) -> &QueryRequest { &self.request }

    pub(crate) fn score_output_names(&self) -> &BTreeSet<String> { &self.score_output_names }
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
            Self::Fusion(_) | Self::Sample(_) => Ok(()),
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

    pub(crate) fn descriptor(&self, prefetch_count: usize) -> Result<QueryDescriptor> {
        match self {
            Self::Nearest(query) => Ok(query.descriptor(prefetch_count)),
            Self::Recommend(query) => Ok(query.descriptor(prefetch_count)),
            Self::Discover(query) => Ok(query.descriptor(prefetch_count)),
            Self::Context(query) => Ok(query.descriptor(prefetch_count)),
            Self::OrderBy(query) => Ok(query.descriptor(prefetch_count)),
            Self::Fusion(query) => query.descriptor(prefetch_count),
            Self::Sample(query) => Ok(query.descriptor(prefetch_count)),
            Self::Formula(query) => Ok(query.descriptor(prefetch_count)),
            Self::NearestWithMmr(query) => Ok(query.descriptor(prefetch_count)),
            Self::RelevanceFeedback(query) => Ok(query.descriptor(prefetch_count)),
        }
    }
}
