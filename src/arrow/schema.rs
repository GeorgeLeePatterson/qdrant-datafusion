use std::sync::Arc;

use datafusion::arrow::datatypes::*;
use qdrant_client::qdrant::{CollectionConfig, Datatype, VectorParams, vectors_config};

use crate::error::{Error, Result};

/// Simple helper function to determine if a field is a multi-vector
pub fn is_multi_vector_field(field: &Field) -> bool {
    matches!(
        field.data_type(),
        DataType::List(inner) if matches!(inner.data_type(), DataType::List(_))
    )
}

/// Simple helper function to convert a Qdrant datatype to an Arrow datatype.
pub fn datatype_to_arrow(dt: Datatype) -> DataType {
    match dt {
        Datatype::Default | Datatype::Float32 => DataType::Float32,
        Datatype::Float16 => DataType::Float16,
        Datatype::Uint8 => DataType::UInt8,
    }
}

/// Simple helper function to create a vector field
pub fn create_vector_field(name: &str, dt: Datatype, nullable: bool) -> FieldRef {
    Field::new(name, datatype_to_arrow(dt), nullable).into()
}

/// Simple helper function to create a list field for vector parameters
pub fn create_vector_param_field(name: &str, vector_params: &VectorParams) -> Field {
    if vector_params.multivector_config.is_some() {
        Field::new(
            name,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(create_vector_field("item", vector_params.datatype(), true)),
                true,
            ))),
            true, // Allow nulls - points may not have all vectors
        )
    } else {
        Field::new(
            name,
            DataType::List(create_vector_field("item", vector_params.datatype(), true)),
            true, // Allow nulls - points may not have all vectors
        )
    }
}

/// Convert a collection's configuration info into an Arrow schema.
///
/// # Errors
/// - Returns an error if the collection info or the vector params is missing.
pub fn collection_to_arrow_schema(collection: &str, config: &CollectionConfig) -> Result<Schema> {
    let mut fields = vec![
        // The point ID (can be numeric or UUID string)
        Field::new("id", DataType::Utf8, false),
        // Payload as JSON string
        Field::new("payload", DataType::Utf8, true),
    ];

    // Get the params from config
    let params =
        config.params.as_ref().ok_or(Error::MissingCollectionInfoParams(collection.into()))?;

    // Parse vectors_config if present
    if let Some(config) = params.vectors_config.as_ref().and_then(|c| c.config.as_ref()) {
        match config {
            vectors_config::Config::Params(vector_params) => {
                // Single unnamed vector
                fields.push(create_vector_param_field("vector", vector_params));
            }
            vectors_config::Config::ParamsMap(params_map) => {
                // Multiple named vectors
                fields.extend(
                    params_map
                        .map
                        .iter()
                        .map(|(name, params)| create_vector_param_field(name, params)),
                );
            }
        }
    }

    // Parse sparse_vectors_config if present
    if let Some(sparse_config) = &params.sparse_vectors_config {
        // SparseVectorConfig has a map field
        for name in sparse_config.map.keys() {
            // Sparse indices are always u32, regardless of index config datatype
            fields.push(Field::new(
                format!("{name}_indices"),
                DataType::List(Field::new("item", DataType::UInt32, true).into()),
                true, // Allow nulls - points may not have all sparse vectors
            ));
            // Sparse values are always f32
            fields.push(Field::new(
                format!("{name}_values"),
                DataType::List(create_vector_field("item", Datatype::Float32, true)),
                true, // Allow nulls - points may not have all sparse vectors
            ));
        }
    }

    Ok(Schema::new(fields))
}
