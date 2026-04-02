//! Schema utilities for `Qdrant` `DataFusion` integration.
use std::collections::HashSet;

use arrow_schema::extension::{
    EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType, VariableShapeTensor,
};
use datafusion::arrow::datatypes::*;
use ndarrow::CsrMatrixBatchExtension;
use qdrant_client::qdrant::{CollectionConfig, Datatype, vectors_config};

use crate::error::{Error, Result};

pub const ID_FIELD_NAME: &str = "id";
pub const PAYLOAD_FIELD_NAME: &str = "payload";
pub const UNNAMED_VECTOR_FIELD_NAME: &str = "vector";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QdrantFieldBinding {
    DenseFixed { width: usize },
    DenseVariable,
    MultiDense { width: Option<usize> },
    Sparse,
    Document,
    Image,
    Object,
    Unsupported(DataType),
}

impl QdrantFieldBinding {
    pub fn from_field(field: &Field) -> Self {
        if let Ok(extension) = field.try_extension_type::<VariableShapeTensor>()
            && extension.dimensions() == 2
        {
            let width = extension
                .uniform_shapes()
                .and_then(|shape| (shape.len() == 2).then_some(shape))
                .and_then(|shape| shape[1])
                .and_then(|width| usize::try_from(width).ok());
            return Self::MultiDense { width };
        }

        if field.try_extension_type::<CsrMatrixBatchExtension>().is_ok() {
            return Self::Sparse;
        }

        match field.data_type() {
            DataType::FixedSizeList(_, len) => usize::try_from(*len).map_or_else(
                |_| Self::Unsupported(field.data_type().clone()),
                |width| Self::DenseFixed { width },
            ),
            DataType::List(inner) | DataType::LargeList(inner)
                if matches!(
                    inner.data_type(),
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                ) =>
            {
                Self::DenseVariable
            }
            DataType::Utf8 | DataType::LargeUtf8 => Self::Document,
            DataType::Binary | DataType::LargeBinary => Self::Image,
            DataType::Struct(_) => Self::Object,
            _ => Self::Unsupported(field.data_type().clone()),
        }
    }

    pub fn is_vector(&self) -> bool {
        matches!(
            self,
            Self::DenseFixed { .. } | Self::DenseVariable | Self::MultiDense { .. } | Self::Sparse
        )
    }

    pub fn dense_vector_width(&self) -> Option<usize> {
        match self {
            Self::DenseFixed { width } => Some(*width),
            _ => None,
        }
    }

    pub fn multivector_width(&self) -> Option<usize> {
        match self {
            Self::MultiDense { width } => *width,
            _ => None,
        }
    }
}

/// Convert a Qdrant datatype to the Arrow value type used by the canonical carriers.
pub fn datatype_to_arrow(_datatype: Datatype) -> DataType {
    // Qdrant currently returns f32 payloads for vector outputs. Keep the contract explicit here
    // until broader datatype support is admitted deliberately.
    DataType::Float32
}

fn fixed_size_vector_field(name: &str, vector_datatype: Datatype, len: u64) -> Result<Field> {
    let len = i32::try_from(len).map_err(|_| {
        Error::InvalidCollectionSchema(format!(
            "vector field '{name}' width exceeds Arrow i32 limits"
        ))
    })?;
    Ok(Field::new(
        name,
        DataType::new_fixed_size_list(datatype_to_arrow(vector_datatype), len, false),
        true,
    ))
}

fn variable_shape_tensor_field(name: &str, vector_datatype: Datatype, len: u64) -> Result<Field> {
    let len = i32::try_from(len).map_err(|_| {
        Error::InvalidCollectionSchema(format!(
            "multivector field '{name}' width exceeds Arrow i32 limits"
        ))
    })?;
    let value_type = datatype_to_arrow(vector_datatype);
    let tensor_storage_type = DataType::Struct(
        vec![
            Field::new("data", DataType::new_list(value_type.clone(), false), false),
            Field::new("shape", DataType::new_fixed_size_list(DataType::Int32, 2, false), false),
        ]
        .into(),
    );
    let extension =
        VariableShapeTensor::try_new(value_type, 2, None, None, Some(vec![None, Some(len)]))
            .map_err(|error| {
                Error::InvalidCollectionSchema(format!(
                    "failed to create multivector field '{name}': {error}"
                ))
            })?;
    extension.supports_data_type(&tensor_storage_type).map_err(|error| {
        Error::InvalidCollectionSchema(format!(
            "multivector field '{name}' has incompatible storage type: {error}"
        ))
    })?;

    let metadata_json = serde_json::json!({
        "uniform_shape": [serde_json::Value::Null, len],
    })
    .to_string();

    let mut field = Field::new(name, tensor_storage_type, true);
    drop(
        field
            .metadata_mut()
            .insert(EXTENSION_TYPE_NAME_KEY.to_owned(), VariableShapeTensor::NAME.to_owned()),
    );
    drop(field.metadata_mut().insert(EXTENSION_TYPE_METADATA_KEY.to_owned(), metadata_json));
    Ok(field)
}

fn sparse_vector_field(name: &str) -> Result<Field> {
    let data_type = DataType::Struct(
        vec![
            Field::new("shape", DataType::new_fixed_size_list(DataType::Int32, 2, false), false),
            Field::new("row_ptrs", DataType::new_list(DataType::Int32, false), false),
            Field::new("col_indices", DataType::new_list(DataType::UInt32, false), false),
            Field::new("values", DataType::new_list(DataType::Float32, false), false),
        ]
        .into(),
    );
    let extension = CsrMatrixBatchExtension::try_new(&data_type, ()).map_err(|error| {
        Error::InvalidCollectionSchema(format!(
            "failed to create sparse vector field '{name}': {error}"
        ))
    })?;

    let mut field = Field::new(name, data_type, true);
    field.try_with_extension_type(extension).map_err(|error| {
        Error::InvalidCollectionSchema(format!(
            "failed to attach sparse vector extension for field '{name}': {error}"
        ))
    })?;
    Ok(field)
}

fn push_unique_field(
    fields: &mut Vec<Field>,
    seen: &mut HashSet<String>,
    field: Field,
) -> Result<()> {
    let name = field.name().clone();
    if !seen.insert(name.clone()) {
        return Err(Error::InvalidCollectionSchema(format!(
            "collection schema yields duplicate field name '{name}'"
        )));
    }
    fields.push(field);
    Ok(())
}

/// Convert a collection's configuration info into an Arrow schema.
///
/// # Errors
/// - Returns an error if the collection info or the vector params is missing.
pub fn collection_to_arrow_schema(collection: &str, config: &CollectionConfig) -> Result<Schema> {
    let mut fields = vec![
        Field::new(ID_FIELD_NAME, DataType::Utf8, false),
        Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
    ];
    let mut seen = fields.iter().map(|field| field.name().clone()).collect::<HashSet<_>>();

    let params =
        config.params.as_ref().ok_or(Error::MissingCollectionInfoParams(collection.into()))?;

    if let Some(config) = params.vectors_config.as_ref().and_then(|config| config.config.as_ref()) {
        match config {
            vectors_config::Config::Params(vector_params) => {
                let field = if vector_params.multivector_config.is_some() {
                    variable_shape_tensor_field(
                        UNNAMED_VECTOR_FIELD_NAME,
                        vector_params.datatype(),
                        vector_params.size,
                    )?
                } else {
                    fixed_size_vector_field(
                        UNNAMED_VECTOR_FIELD_NAME,
                        vector_params.datatype(),
                        vector_params.size,
                    )?
                };
                push_unique_field(&mut fields, &mut seen, field)?;
            }
            vectors_config::Config::ParamsMap(params_map) => {
                for (name, params) in &params_map.map {
                    let field = if params.multivector_config.is_some() {
                        variable_shape_tensor_field(name, params.datatype(), params.size)?
                    } else {
                        fixed_size_vector_field(name, params.datatype(), params.size)?
                    };
                    push_unique_field(&mut fields, &mut seen, field)?;
                }
            }
        }
    }

    if let Some(sparse_config) = &params.sparse_vectors_config {
        for name in sparse_config.map.keys() {
            push_unique_field(&mut fields, &mut seen, sparse_vector_field(name)?)?;
        }
    }

    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use qdrant_client::qdrant::{
        Distance, MultiVectorComparator, MultiVectorConfig, VectorParamsBuilder,
    };

    use super::*;

    #[test]
    fn dense_vector_fields_use_fixed_size_lists() {
        let params = VectorParamsBuilder::new(3, Distance::Cosine).build();
        let field = fixed_size_vector_field("embedding", params.datatype(), params.size)
            .expect("dense field");

        let DataType::FixedSizeList(item, len) = field.data_type() else {
            panic!("expected FixedSizeList carrier");
        };
        assert_eq!(*len, 3);
        assert_eq!(item.data_type(), &DataType::Float32);
        assert!(!item.is_nullable());
        assert!(field.is_nullable());
        assert_eq!(
            QdrantFieldBinding::from_field(&field),
            QdrantFieldBinding::DenseFixed { width: 3 }
        );
    }

    #[test]
    fn multivector_fields_use_variable_shape_tensor_extension() {
        let params = VectorParamsBuilder::new(3, Distance::Dot)
            .multivector_config(MultiVectorConfig {
                comparator: MultiVectorComparator::MaxSim.into(),
            })
            .build();
        let field = variable_shape_tensor_field("multi", params.datatype(), params.size)
            .expect("multivector field");
        let extension =
            field.try_extension_type::<VariableShapeTensor>().expect("variable tensor extension");

        assert_eq!(extension.dimensions(), 2);
        assert_eq!(extension.uniform_shapes(), Some(&[None, Some(3)][..]));
        assert_eq!(field.extension_type_name(), Some(VariableShapeTensor::NAME));
        assert!(field.is_nullable());
        assert_eq!(
            QdrantFieldBinding::from_field(&field),
            QdrantFieldBinding::MultiDense { width: Some(3) }
        );
    }

    #[test]
    fn sparse_vector_fields_use_csr_extension() {
        let field = sparse_vector_field("keywords").expect("sparse field");
        assert!(field.try_extension_type::<CsrMatrixBatchExtension>().is_ok());
        assert_eq!(field.name(), "keywords");
        assert!(field.is_nullable());
        assert_eq!(QdrantFieldBinding::from_field(&field), QdrantFieldBinding::Sparse);
    }
}
