use datafusion::common::{Result, plan_err};
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::{
    DenseVector, Document, Image, InferenceObject, MultiDenseVector, PointId, Query, Value,
    VectorInput, vector_input,
};

use super::super::source::Source;
use super::QueryDescriptor;
use crate::arrow::schema::QdrantFieldBinding;
use crate::expr_fn::NearestCall;

#[derive(Debug, Clone)]
pub(crate) struct NearestQuery {
    using: Option<String>,
    input: NearestInput,
}

impl NearestQuery {
    pub(crate) fn same_semantics(&self, other: &Self) -> bool {
        self.using == other.using && self.input.same_semantics(&other.input)
    }

    pub(super) fn validate_on_source(&self, source: &Source) -> Result<()> {
        let Some(using) = self.using.as_deref() else {
            return Ok(());
        };
        self.input.validate_on_source(source, using)
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> QueryDescriptor {
        self.input.descriptor(self.using.clone())
    }
}

impl TryFrom<NearestCall> for NearestQuery {
    type Error = datafusion::error::DataFusionError;

    fn try_from(call: NearestCall) -> Result<Self> {
        let (using, input) = match call {
            NearestCall::Dense { vector_field, vector } => {
                (Some(vector_field), NearestInput::Dense(DenseNearestInput { vector }))
            }
            NearestCall::Sparse { vector_field, indices, values } => {
                (Some(vector_field), NearestInput::Sparse(SparseNearestInput { indices, values }))
            }
            NearestCall::MultiDense { vector_field, vectors } => {
                (Some(vector_field), NearestInput::MultiDense(MultiDenseNearestInput { vectors }))
            }
            NearestCall::Id { vector_field, point_id } => {
                (Some(vector_field), NearestInput::Id(IdNearestInput { point_id }))
            }
            NearestCall::Document { vector_field, text, model } => {
                (Some(vector_field), NearestInput::Document(DocumentNearestInput { text, model }))
            }
            NearestCall::Image { vector_field, image, model } => {
                (Some(vector_field), NearestInput::Image(ImageNearestInput { image, model }))
            }
            NearestCall::Object { vector_field, object, model } => {
                (Some(vector_field), NearestInput::Object(ObjectNearestInput { object, model }))
            }
        };
        Ok(Self { using, input })
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
    fn descriptor(&self, using: Option<String>) -> QueryDescriptor {
        let query = match self {
            Self::Dense(input) => Query {
                variant: Some(qdrant_client::qdrant::query::Variant::Nearest(VectorInput {
                    variant: Some(vector_input::Variant::Dense(DenseVector {
                        data: input.vector.clone(),
                    })),
                })),
            },
            Self::Sparse(input) => Query {
                variant: Some(qdrant_client::qdrant::query::Variant::Nearest(VectorInput {
                    variant: Some(vector_input::Variant::Sparse(
                        qdrant_client::qdrant::SparseVector {
                            values: input.values.clone(),
                            indices: input.indices.clone(),
                        },
                    )),
                })),
            },
            Self::MultiDense(input) => Query {
                variant: Some(qdrant_client::qdrant::query::Variant::Nearest(VectorInput {
                    variant: Some(vector_input::Variant::MultiDense(MultiDenseVector {
                        vectors: input
                            .vectors
                            .iter()
                            .cloned()
                            .map(|vector| DenseVector { data: vector })
                            .collect(),
                    })),
                })),
            },
            Self::Id(input) => Query {
                variant: Some(qdrant_client::qdrant::query::Variant::Nearest(VectorInput {
                    variant: Some(vector_input::Variant::Id(input.point_id.clone())),
                })),
            },
            Self::Document(input) => Query {
                variant: Some(qdrant_client::qdrant::query::Variant::Nearest(VectorInput {
                    variant: Some(vector_input::Variant::Document(Document {
                        text: input.text.clone(),
                        model: input.model.clone().unwrap_or_default(),
                        options: std::collections::HashMap::new(),
                    })),
                })),
            },
            Self::Image(input) => Query {
                variant: Some(qdrant_client::qdrant::query::Variant::Nearest(VectorInput {
                    variant: Some(vector_input::Variant::Image(Image {
                        image: Some(input.image.clone()),
                        model: input.model.clone().unwrap_or_default(),
                        options: std::collections::HashMap::new(),
                    })),
                })),
            },
            Self::Object(input) => Query {
                variant: Some(qdrant_client::qdrant::query::Variant::Nearest(VectorInput {
                    variant: Some(vector_input::Variant::Object(InferenceObject {
                        object: Some(input.object.clone()),
                        model: input.model.clone().unwrap_or_default(),
                        options: std::collections::HashMap::new(),
                    })),
                })),
            },
        };
        QueryDescriptor::new(query, using)
    }

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
            Self::Id(_) => IdNearestInput::validate_on_source(source, using),
            Self::Document(_) => DocumentNearestInput::validate_on_source(source, using),
            Self::Image(_) => ImageNearestInput::validate_on_source(source, using),
            Self::Object(_) => ObjectNearestInput::validate_on_source(source, using),
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
        match source.field_binding(using)? {
            QdrantFieldBinding::DenseFixed { width } => {
                if width != self.vector.len() {
                    return plan_err!("query vector width does not match source vector width");
                }
                Ok(())
            }
            QdrantFieldBinding::DenseVariable => Ok(()),
            QdrantFieldBinding::Sparse => {
                plan_err!("dense query input requires a dense vector binding")
            }
            QdrantFieldBinding::MultiDense { .. } => {
                plan_err!("dense query input requires a single dense vector binding")
            }
            QdrantFieldBinding::Document => {
                plan_err!("dense query input does not bind to a document inference field")
            }
            QdrantFieldBinding::Image => {
                plan_err!("dense query input does not bind to an image inference field")
            }
            QdrantFieldBinding::Object => {
                plan_err!("dense query input does not bind to an object inference field")
            }
            QdrantFieldBinding::Unsupported(data_type) => plan_err!(
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
    values: Vec<f32>,
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
        match source.field_binding(using)? {
            QdrantFieldBinding::Sparse => Ok(()),
            QdrantFieldBinding::DenseFixed { .. } | QdrantFieldBinding::DenseVariable => {
                plan_err!("sparse query input requires a sparse vector binding")
            }
            QdrantFieldBinding::MultiDense { .. } => {
                plan_err!("sparse query input does not bind to a multivector field")
            }
            QdrantFieldBinding::Document => {
                plan_err!("sparse query input does not bind to a document inference field")
            }
            QdrantFieldBinding::Image => {
                plan_err!("sparse query input does not bind to an image inference field")
            }
            QdrantFieldBinding::Object => {
                plan_err!("sparse query input does not bind to an object inference field")
            }
            QdrantFieldBinding::Unsupported(data_type) => plan_err!(
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
        if self.vectors.is_empty() {
            return plan_err!("multivector query input requires at least one dense vector");
        }
        match source.field_binding(using)? {
            QdrantFieldBinding::MultiDense { width } => {
                if let Some(width) = width
                    && self.vectors.iter().any(|vector| vector.len() != width)
                {
                    return plan_err!(
                        "multivector query input width does not match source vector width"
                    );
                }
                Ok(())
            }
            QdrantFieldBinding::DenseFixed { .. }
            | QdrantFieldBinding::DenseVariable
            | QdrantFieldBinding::Sparse => {
                plan_err!("multivector query input requires a multivector binding")
            }
            QdrantFieldBinding::Document => {
                plan_err!("multivector query input does not bind to a document inference field")
            }
            QdrantFieldBinding::Image => {
                plan_err!("multivector query input does not bind to an image inference field")
            }
            QdrantFieldBinding::Object => {
                plan_err!("multivector query input does not bind to an object inference field")
            }
            QdrantFieldBinding::Unsupported(data_type) => plan_err!(
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

    fn validate_on_source(source: &Source, using: &str) -> Result<()> {
        match source.field_binding(using)? {
            QdrantFieldBinding::Unsupported(data_type) => plan_err!(
                "id nearest input does not bind to source field '{}' of type {:?}",
                using,
                data_type
            ),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DocumentNearestInput {
    text: String,
    model: Option<String>,
}

impl DocumentNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.text == other.text && self.model == other.model
    }

    fn validate_on_source(source: &Source, using: &str) -> Result<()> {
        match source.field_binding(using)? {
            QdrantFieldBinding::Document => Ok(()),
            QdrantFieldBinding::DenseFixed { .. } | QdrantFieldBinding::DenseVariable => {
                plan_err!("document nearest input requires a document inference field")
            }
            QdrantFieldBinding::MultiDense { .. } => {
                plan_err!("document nearest input does not bind to a multivector field")
            }
            QdrantFieldBinding::Sparse => {
                plan_err!("document nearest input does not bind to a sparse vector field")
            }
            QdrantFieldBinding::Image => {
                plan_err!("document nearest input does not bind to an image inference field")
            }
            QdrantFieldBinding::Object => {
                plan_err!("document nearest input does not bind to an object inference field")
            }
            QdrantFieldBinding::Unsupported(data_type) => plan_err!(
                "document nearest input does not bind to source field '{}' of type {:?}",
                using,
                data_type
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ImageNearestInput {
    image: Value,
    model: Option<String>,
}

impl ImageNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.image == other.image && self.model == other.model
    }

    fn validate_on_source(source: &Source, using: &str) -> Result<()> {
        match source.field_binding(using)? {
            QdrantFieldBinding::Image => Ok(()),
            QdrantFieldBinding::DenseFixed { .. } | QdrantFieldBinding::DenseVariable => {
                plan_err!("image nearest input requires an image inference field")
            }
            QdrantFieldBinding::MultiDense { .. } => {
                plan_err!("image nearest input does not bind to a multivector field")
            }
            QdrantFieldBinding::Sparse => {
                plan_err!("image nearest input does not bind to a sparse vector field")
            }
            QdrantFieldBinding::Document => {
                plan_err!("image nearest input does not bind to a document inference field")
            }
            QdrantFieldBinding::Object => {
                plan_err!("image nearest input does not bind to an object inference field")
            }
            QdrantFieldBinding::Unsupported(data_type) => plan_err!(
                "image nearest input does not bind to source field '{}' of type {:?}",
                using,
                data_type
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ObjectNearestInput {
    object: Value,
    model: Option<String>,
}

impl ObjectNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.object == other.object && self.model == other.model
    }

    fn validate_on_source(source: &Source, using: &str) -> Result<()> {
        match source.field_binding(using)? {
            QdrantFieldBinding::Object => Ok(()),
            QdrantFieldBinding::DenseFixed { .. } | QdrantFieldBinding::DenseVariable => {
                plan_err!("object nearest input requires an object inference field")
            }
            QdrantFieldBinding::MultiDense { .. } => {
                plan_err!("object nearest input does not bind to a multivector field")
            }
            QdrantFieldBinding::Sparse => {
                plan_err!("object nearest input does not bind to a sparse vector field")
            }
            QdrantFieldBinding::Document => {
                plan_err!("object nearest input does not bind to a document inference field")
            }
            QdrantFieldBinding::Image => {
                plan_err!("object nearest input does not bind to an image inference field")
            }
            QdrantFieldBinding::Unsupported(data_type) => plan_err!(
                "object nearest input does not bind to source field '{}' of type {:?}",
                using,
                data_type
            ),
        }
    }
}
