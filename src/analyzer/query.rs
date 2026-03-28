use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::PointId;
use qdrant_client::qdrant::point_id::PointIdOptions;

use super::source::Source;
use super::surface::QuerySurfaceCall;
use crate::expr_fn::QdrantNearestCall;

#[derive(Debug, Clone)]
pub(crate) enum QueryExecution {
    NearestDense {
        using:  Option<String>,
        vector: Vec<f32>,
    },
    NearestSparse {
        using:   Option<String>,
        indices: Vec<u32>,
        values:  Vec<f32>,
    },
    NearestMultiDense {
        using:   Option<String>,
        vectors: Vec<Vec<f32>>,
    },
    NearestById {
        using:    Option<String>,
        point_id: PointId,
    },
    NearestDocument {
        using: Option<String>,
        text:  String,
        model: Option<String>,
    },
    NearestImage {
        using: Option<String>,
        image: Vec<u8>,
        model: Option<String>,
    },
    NearestObject {
        using:  Option<String>,
        object: Vec<(String, ScalarValue)>,
        model:  Option<String>,
    },
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

    pub(crate) fn execution(&self) -> QueryExecution {
        match self {
            Self::Nearest(query) => match &query.input {
                NearestInput::Dense(input) => QueryExecution::NearestDense {
                    using:  query.using.clone(),
                    vector: input.vector.clone(),
                },
                NearestInput::Sparse(input) => QueryExecution::NearestSparse {
                    using:   query.using.clone(),
                    indices: input.indices.clone(),
                    values:  input.values.clone(),
                },
                NearestInput::MultiDense(input) => QueryExecution::NearestMultiDense {
                    using:   query.using.clone(),
                    vectors: input.vectors.clone(),
                },
                NearestInput::Id(input) => QueryExecution::NearestById {
                    using:    query.using.clone(),
                    point_id: input.point_id.clone(),
                },
                NearestInput::Document(input) => QueryExecution::NearestDocument {
                    using: query.using.clone(),
                    text:  input.text.clone(),
                    model: input.model.clone(),
                },
                NearestInput::Image(input) => QueryExecution::NearestImage {
                    using: query.using.clone(),
                    image: input.image.clone(),
                    model: input.model.clone(),
                },
                NearestInput::Object(input) => QueryExecution::NearestObject {
                    using:  query.using.clone(),
                    object: input.object.clone(),
                    model:  input.model.clone(),
                },
            },
            Self::Recommend(query) => QueryExecution::Recommend(query.clone()),
            Self::Discover(query) => QueryExecution::Discover(query.clone()),
            Self::Context(query) => QueryExecution::Context(query.clone()),
            Self::OrderBy(query) => QueryExecution::OrderBy(query.clone()),
            Self::Fusion(query) => QueryExecution::Fusion(query.clone()),
            Self::Sample(query) => QueryExecution::Sample(query.clone()),
            Self::Formula(query) => QueryExecution::Formula(query.clone()),
            Self::NearestWithMmr(query) => QueryExecution::NearestWithMmr(query.clone()),
            Self::RelevanceFeedback(query) => QueryExecution::RelevanceFeedback(query.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NearestQuery {
    using: Option<String>,
    input: NearestInput,
}

impl NearestQuery {
    pub(super) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        QdrantNearestCall::from_expr(expr).map(|call| call.map(Into::into))
    }

    pub(super) fn same_semantics(&self, other: &Self) -> bool {
        self.using == other.using && self.input.same_semantics(&other.input)
    }

    fn validate_on_source(&self, source: &Source) -> Result<()> {
        let Some(using) = self.using.as_deref() else {
            return Ok(());
        };
        self.input.validate_on_source(source, using)
    }
}

impl From<QdrantNearestCall> for NearestQuery {
    fn from(call: QdrantNearestCall) -> Self {
        Self {
            using: Some(call.vector_field),
            input: NearestInput::Dense(DenseNearestInput { vector: call.vector }),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum NearestInput {
    Dense(DenseNearestInput),
    Sparse(SparseNearestInput),
    MultiDense(MultiDenseNearestInput),
    Id(IdNearestInput),
    Document(DocumentNearestInput),
    Image(ImageNearestInput),
    Object(ObjectNearestInput),
}

impl NearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Dense(lhs), Self::Dense(rhs)) => lhs.same_semantics(rhs),
            (Self::Sparse(lhs), Self::Sparse(rhs)) => lhs.same_semantics(rhs),
            (Self::MultiDense(lhs), Self::MultiDense(rhs)) => lhs.same_semantics(rhs),
            (Self::Id(lhs), Self::Id(rhs)) => lhs.same_semantics(rhs),
            (Self::Document(lhs), Self::Document(rhs)) => lhs.same_semantics(rhs),
            (Self::Image(lhs), Self::Image(rhs)) => lhs.same_semantics(rhs),
            (Self::Object(lhs), Self::Object(rhs)) => lhs.same_semantics(rhs),
            _ => false,
        }
    }

    fn validate_on_source(&self, source: &Source, using: &str) -> Result<()> {
        match self {
            Self::Dense(input) => input.validate_on_source(source, using),
            Self::Sparse(input) => input.validate_on_source(source, using),
            Self::MultiDense(input) => input.validate_on_source(source, using),
            Self::Id(input) => input.validate_on_source(source, using),
            Self::Document(input) => input.validate_on_source(source, using),
            Self::Image(input) => input.validate_on_source(source, using),
            Self::Object(input) => input.validate_on_source(source, using),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DenseNearestInput {
    vector: Vec<f32>,
}

impl DenseNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.vector
            .iter()
            .map(|value| value.to_bits())
            .eq(other.vector.iter().map(|value| value.to_bits()))
    }

    fn validate_on_source(&self, source: &Source, using: &str) -> Result<()> {
        match QueryVectorBinding::from_source(source, using)? {
            QueryVectorBinding::DenseFixed { width } => {
                if width != self.vector.len() {
                    return plan_err!("query vector width does not match source vector width");
                }
                Ok(())
            }
            QueryVectorBinding::DenseVariable => Ok(()),
            QueryVectorBinding::Sparse => {
                plan_err!("dense query input requires a dense vector binding")
            }
            QueryVectorBinding::MultiDense => {
                plan_err!("dense query input requires a single dense vector binding")
            }
            QueryVectorBinding::Document => {
                plan_err!("dense query input does not bind to a document inference field")
            }
            QueryVectorBinding::Image => {
                plan_err!("dense query input does not bind to an image inference field")
            }
            QueryVectorBinding::Object => {
                plan_err!("dense query input does not bind to an object inference field")
            }
            QueryVectorBinding::Unsupported(data_type) => plan_err!(
                "dense query input does not bind to source field '{}' of type {:?}",
                using,
                data_type
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SparseNearestInput {
    indices: Vec<u32>,
    values:  Vec<f32>,
}

impl SparseNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.indices == other.indices
            && self
                .values
                .iter()
                .map(|value| value.to_bits())
                .eq(other.values.iter().map(|value| value.to_bits()))
    }

    fn validate_on_source(&self, source: &Source, using: &str) -> Result<()> {
        if self.indices.len() != self.values.len() {
            return plan_err!("sparse query input requires matching index and value lengths");
        }
        match QueryVectorBinding::from_source(source, using)? {
            QueryVectorBinding::Sparse => Ok(()),
            QueryVectorBinding::DenseFixed { .. } | QueryVectorBinding::DenseVariable => {
                plan_err!("sparse query input requires a sparse vector binding")
            }
            QueryVectorBinding::MultiDense => {
                plan_err!("sparse query input does not bind to a multivector field")
            }
            QueryVectorBinding::Document => {
                plan_err!("sparse query input does not bind to a document inference field")
            }
            QueryVectorBinding::Image => {
                plan_err!("sparse query input does not bind to an image inference field")
            }
            QueryVectorBinding::Object => {
                plan_err!("sparse query input does not bind to an object inference field")
            }
            QueryVectorBinding::Unsupported(data_type) => plan_err!(
                "sparse query input does not bind to source field '{}' of type {:?}",
                using,
                data_type
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MultiDenseNearestInput {
    vectors: Vec<Vec<f32>>,
}

impl MultiDenseNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.vectors.len() == other.vectors.len()
            && self.vectors.iter().zip(&other.vectors).all(|(lhs, rhs)| {
                lhs.iter().map(|value| value.to_bits()).eq(rhs.iter().map(|value| value.to_bits()))
            })
    }

    fn validate_on_source(&self, source: &Source, using: &str) -> Result<()> {
        match QueryVectorBinding::from_source(source, using)? {
            QueryVectorBinding::MultiDense => Ok(()),
            QueryVectorBinding::DenseFixed { .. }
            | QueryVectorBinding::DenseVariable
            | QueryVectorBinding::Sparse => {
                plan_err!("multivector query input requires a multivector binding")
            }
            QueryVectorBinding::Document => {
                plan_err!("multivector query input does not bind to a document inference field")
            }
            QueryVectorBinding::Image => {
                plan_err!("multivector query input does not bind to an image inference field")
            }
            QueryVectorBinding::Object => {
                plan_err!("multivector query input does not bind to an object inference field")
            }
            QueryVectorBinding::Unsupported(data_type) => plan_err!(
                "multivector query input does not bind to source field '{}' of type {:?}",
                using,
                data_type
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IdNearestInput {
    point_id: PointId,
}

impl IdNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        match (&self.point_id.point_id_options, &other.point_id.point_id_options) {
            (Some(PointIdOptions::Num(lhs)), Some(PointIdOptions::Num(rhs))) => lhs == rhs,
            (Some(PointIdOptions::Uuid(lhs)), Some(PointIdOptions::Uuid(rhs))) => lhs == rhs,
            (None, None) => true,
            _ => false,
        }
    }

    fn validate_on_source(&self, _source: &Source, _using: &str) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone)]
pub(crate) struct DocumentNearestInput {
    text:  String,
    model: Option<String>,
}

impl DocumentNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.text == other.text && self.model == other.model
    }

    fn validate_on_source(&self, _source: &Source, _using: &str) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone)]
pub(crate) struct ImageNearestInput {
    image: Vec<u8>,
    model: Option<String>,
}

impl ImageNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.image == other.image && self.model == other.model
    }

    fn validate_on_source(&self, _source: &Source, _using: &str) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone)]
pub(crate) struct ObjectNearestInput {
    object: Vec<(String, ScalarValue)>,
    model:  Option<String>,
}

impl ObjectNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.object == other.object && self.model == other.model
    }

    fn validate_on_source(&self, _source: &Source, _using: &str) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone)]
pub(crate) enum QueryVectorBinding {
    DenseFixed { width: usize },
    DenseVariable,
    Sparse,
    MultiDense,
    Document,
    Image,
    Object,
    Unsupported(DataType),
}

impl QueryVectorBinding {
    fn from_source(source: &Source, using: &str) -> Result<Self> {
        let field: &datafusion::arrow::datatypes::Field = match source.schema.field_with_name(using)
        {
            Ok(field) => field,
            Err(_) => return plan_err!("query vector field '{}' not found", using),
        };
        Ok(Self::from_data_type(field.data_type()))
    }

    fn from_data_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::FixedSizeList(field, width)
                if matches!(
                    field.data_type(),
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                ) =>
            {
                Self::DenseFixed { width: usize::try_from(*width).unwrap_or_default() }
            }
            DataType::List(field) | DataType::LargeList(field)
                if matches!(
                    field.data_type(),
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                ) =>
            {
                Self::DenseVariable
            }
            DataType::List(field) | DataType::LargeList(field)
                if matches!(
                    field.data_type(),
                    DataType::FixedSizeList(_, _) | DataType::List(_) | DataType::LargeList(_)
                ) =>
            {
                Self::MultiDense
            }
            DataType::List(field) | DataType::LargeList(field)
                if matches!(field.data_type(), DataType::Struct(_)) =>
            {
                Self::Sparse
            }
            DataType::Struct(fields)
                if fields.iter().any(|field| field.name() == "indices")
                    && fields.iter().any(|field| field.name() == "values") =>
            {
                Self::Sparse
            }
            DataType::Utf8 | DataType::LargeUtf8 => Self::Document,
            DataType::Binary | DataType::LargeBinary => Self::Image,
            DataType::Struct(_) => Self::Object,
            other => Self::Unsupported(other.clone()),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RecommendQuery;

impl RecommendQuery {
    pub(super) fn same_semantics(&self, _other: &Self) -> bool { true }

    fn validate_on_source(&self, _source: &Source) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DiscoverQuery;

impl DiscoverQuery {
    pub(super) fn same_semantics(&self, _other: &Self) -> bool { true }

    fn validate_on_source(&self, _source: &Source) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ContextQuery;

impl ContextQuery {
    pub(super) fn same_semantics(&self, _other: &Self) -> bool { true }

    fn validate_on_source(&self, _source: &Source) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct OrderByQuery;

impl OrderByQuery {
    pub(super) fn same_semantics(&self, _other: &Self) -> bool { true }

    fn validate_on_source(&self, _source: &Source) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FusionQuery;

impl FusionQuery {
    pub(super) fn same_semantics(&self, _other: &Self) -> bool { true }

    fn validate_on_source(&self, _source: &Source) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SampleQuery;

impl SampleQuery {
    pub(super) fn same_semantics(&self, _other: &Self) -> bool { true }

    fn validate_on_source(&self, _source: &Source) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FormulaQuery;

impl FormulaQuery {
    pub(super) fn same_semantics(&self, _other: &Self) -> bool { true }

    fn validate_on_source(&self, _source: &Source) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NearestWithMmrQuery;

impl NearestWithMmrQuery {
    pub(super) fn same_semantics(&self, _other: &Self) -> bool { true }

    fn validate_on_source(&self, _source: &Source) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RelevanceFeedbackQuery;

impl RelevanceFeedbackQuery {
    pub(super) fn same_semantics(&self, _other: &Self) -> bool { true }

    fn validate_on_source(&self, _source: &Source) -> Result<()> { Ok(()) }
}
