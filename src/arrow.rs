//! Conversion utilities for `Qdrant` data types to `Arrow` data types.

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use qdrant_client::qdrant::{Datatype, ScoredPoint, point_id, vector_output, vectors_output};

pub fn datatype_to_arrow(dt: Datatype) -> DataType {
    match dt {
        Datatype::Default | Datatype::Float32 => DataType::Float32,
        Datatype::Float16 => DataType::Float16,
        Datatype::Uint8 => DataType::UInt8,
    }
}

pub fn create_vector_field(name: &str, dt: Datatype, nullable: bool) -> FieldRef {
    Field::new(name, datatype_to_arrow(dt), nullable).into()
}

/// Helper function that implements same logic as `Qdrant`'s rust client's `try_into_multi`
///
/// # Errors
/// - Returns an error if the data length is not divisible by the vectors count.
pub fn convert_to_multi_vector(
    data: &[f32],
    vectors_count: u32,
) -> DataFusionResult<Vec<Vec<f32>>> {
    if data.len() % vectors_count as usize != 0 {
        return Err(DataFusionError::External(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Malformed multi vector: data length {} is not divisible by vectors count {}",
                data.len(),
                vectors_count
            ),
        ))));
    }

    let chunk_size = data.len() / vectors_count as usize;
    Ok(data.chunks(chunk_size).map(<[f32]>::to_vec).collect())
}

pub fn extract_ids_from_scored(points: &[ScoredPoint]) -> ArrayRef {
    let ids: Vec<Option<String>> = points
        .iter()
        .map(|p| {
            p.id.as_ref().map(|id| match &id.point_id_options {
                Some(point_id::PointIdOptions::Num(n)) => n.to_string(),
                Some(point_id::PointIdOptions::Uuid(s)) => s.clone(),
                None => String::new(),
            })
        })
        .collect();

    Arc::new(StringArray::from(ids))
}

pub fn extract_payload_from_scored(points: &[ScoredPoint]) -> ArrayRef {
    let payloads: Vec<Option<String>> = points
        .iter()
        .map(|p| if p.payload.is_empty() { None } else { serde_json::to_string(&p.payload).ok() })
        .collect();

    Arc::new(StringArray::from(payloads))
}

pub fn extract_dense_vector_from_scored(points: &[ScoredPoint], name: &str) -> ArrayRef {
    let is_multi_vector = points
        .first()
        .and_then(|point| {
            let vectors = point.vectors.as_ref()?;
            match vectors.vectors_options.as_ref()? {
                vectors_output::VectorsOptions::Vector(vector_output) => {
                    if name == "vector" {
                        // Check newer format first
                        if let Some(vector_output::Vector::MultiDense(_)) = &vector_output.vector {
                            Some(true)
                        } else {
                            // Check deprecated format: vectors_count indicates multi-vector
                            Some(vector_output.vectors_count.is_some())
                        }
                    } else {
                        None
                    }
                }
                vectors_output::VectorsOptions::Vectors(named_vectors) => {
                    named_vectors.vectors.get(name).map(|vector_output| {
                        // Check newer format first
                        if let Some(vector_output::Vector::MultiDense(_)) = &vector_output.vector {
                            true
                        } else {
                            // Check deprecated format: vectors_count indicates multi-vector
                            vector_output.vectors_count.is_some()
                        }
                    })
                }
            }
        })
        .unwrap_or(false);

    if is_multi_vector {
        extract_multi_dense_vector_from_scored(points, name)
    } else {
        extract_single_dense_vector_from_scored(points, name)
    }
}

pub fn extract_single_dense_vector_from_scored(points: &[ScoredPoint], name: &str) -> ArrayRef {
    let mut list_builder = ListBuilder::new(Float32Builder::new());

    for point in points {
        let vector_data = if let Some(vectors) = &point.vectors {
            match vectors.vectors_options.as_ref() {
                Some(vectors_output::VectorsOptions::Vector(vector_output)) if name == "vector" => {
                    // Check newer format first
                    if let Some(vector_output::Vector::Dense(dense_vector)) = &vector_output.vector
                    {
                        Some(&dense_vector.data)
                    } else {
                        // Fall back to deprecated format
                        Some(&vector_output.data)
                    }
                }
                Some(vectors_output::VectorsOptions::Vectors(named_vectors)) => {
                    named_vectors.vectors.get(name).map(|vector_output| {
                        // Check newer format first
                        if let Some(vector_output::Vector::Dense(dense_vector)) =
                            &vector_output.vector
                        {
                            &dense_vector.data
                        } else {
                            // Fall back to deprecated format
                            &vector_output.data
                        }
                    })
                }
                _ => None,
            }
        } else {
            None
        };

        if let Some(data) = vector_data {
            list_builder.values().append_slice(data);
            list_builder.append(true);
        } else {
            list_builder.append(false);
        }
    }

    Arc::new(list_builder.finish())
}

pub fn extract_multi_dense_vector_from_scored(points: &[ScoredPoint], name: &str) -> ArrayRef {
    let mut outer_list_builder = ListBuilder::new(ListBuilder::new(Float32Builder::new()));

    for point in points {
        let multi_vector_result = if let Some(vectors) = &point.vectors {
            match vectors.vectors_options.as_ref() {
                Some(vectors_output::VectorsOptions::Vector(vector_output)) if name == "vector" => {
                    // Check newer format first
                    if let Some(vector_output::Vector::MultiDense(multi_dense)) =
                        &vector_output.vector
                    {
                        Ok(multi_dense.vectors.iter().map(|v| v.data.clone()).collect())
                    } else if let Some(vectors_count) = vector_output.vectors_count {
                        // Use deprecated format with helper function
                        convert_to_multi_vector(&vector_output.data, vectors_count)
                    } else {
                        Err(DataFusionError::Internal("Not a multi-vector".to_string()))
                    }
                }
                Some(vectors_output::VectorsOptions::Vectors(named_vectors)) => {
                    if let Some(vector_output) = named_vectors.vectors.get(name) {
                        // Check newer format first
                        if let Some(vector_output::Vector::MultiDense(multi_dense)) =
                            &vector_output.vector
                        {
                            Ok(multi_dense.vectors.iter().map(|v| v.data.clone()).collect())
                        } else if let Some(vectors_count) = vector_output.vectors_count {
                            // Use deprecated format with helper function
                            convert_to_multi_vector(&vector_output.data, vectors_count)
                        } else {
                            Err(DataFusionError::Internal("Not a multi-vector".to_string()))
                        }
                    } else {
                        Err(DataFusionError::Internal("Vector not found".to_string()))
                    }
                }
                _ => Err(DataFusionError::Internal("Invalid vector format".to_string())),
            }
        } else {
            Err(DataFusionError::Internal("No vectors".to_string()))
        };

        match multi_vector_result {
            Ok(dense_vectors) => {
                for dense_vector in dense_vectors {
                    outer_list_builder.values().values().append_slice(&dense_vector);
                    outer_list_builder.values().append(true);
                }
                outer_list_builder.append(true);
            }
            Err(_) => {
                outer_list_builder.append(false);
            }
        }
    }

    Arc::new(outer_list_builder.finish())
}

pub fn extract_sparse_indices_from_scored(points: &[ScoredPoint], sparse_name: &str) -> ArrayRef {
    let mut list_builder = ListBuilder::new(UInt32Builder::new());

    for point in points {
        if let Some(indices) = get_sparse_indices_from_point(point, sparse_name) {
            list_builder.values().append_slice(&indices);
            list_builder.append(true);
        } else {
            list_builder.append(false);
        }
    }

    Arc::new(list_builder.finish())
}

pub fn extract_sparse_values_from_scored(points: &[ScoredPoint], sparse_name: &str) -> ArrayRef {
    let mut list_builder = ListBuilder::new(Float32Builder::new());

    for point in points {
        if let Some(values) = get_sparse_values_from_point(point, sparse_name) {
            list_builder.values().append_slice(&values);
            list_builder.append(true);
        } else {
            list_builder.append(false);
        }
    }

    Arc::new(list_builder.finish())
}

pub fn get_sparse_indices_from_point(point: &ScoredPoint, sparse_name: &str) -> Option<Vec<u32>> {
    let vectors = point.vectors.as_ref()?;

    match vectors.vectors_options.as_ref()? {
        vectors_output::VectorsOptions::Vector(vector_output) => {
            // For unnamed sparse vectors, check the newer format first
            if let Some(vector_output::Vector::Sparse(sparse_vector)) = &vector_output.vector {
                return Some(sparse_vector.indices.clone());
            }
            // Fall back to deprecated format for backward compatibility
            vector_output.indices.as_ref().map(|indices| indices.data.clone())
        }
        vectors_output::VectorsOptions::Vectors(named_vectors) => {
            // For named sparse vectors
            if let Some(vector_output) = named_vectors.vectors.get(sparse_name) {
                // Check newer format first
                if let Some(vector_output::Vector::Sparse(sparse_vector)) = &vector_output.vector {
                    return Some(sparse_vector.indices.clone());
                }
                // Fall back to deprecated format
                return vector_output.indices.as_ref().map(|indices| indices.data.clone());
            }
            None
        }
    }
}

pub fn get_sparse_values_from_point(point: &ScoredPoint, sparse_name: &str) -> Option<Vec<f32>> {
    let vectors = point.vectors.as_ref()?;

    match vectors.vectors_options.as_ref()? {
        vectors_output::VectorsOptions::Vector(vector_output) => {
            // For unnamed sparse vectors, check the newer format first
            if let Some(vector_output::Vector::Sparse(sparse_vector)) = &vector_output.vector {
                return Some(sparse_vector.values.clone());
            }
            // Fall back to deprecated format for backward compatibility
            if sparse_name == "vector" {
                // For deprecated format, values are in the main `data` field
                return Some(vector_output.data.clone());
            }
            None
        }
        vectors_output::VectorsOptions::Vectors(named_vectors) => {
            // For named sparse vectors
            if let Some(vector_output) = named_vectors.vectors.get(sparse_name) {
                // Check newer format first
                if let Some(vector_output::Vector::Sparse(sparse_vector)) = &vector_output.vector {
                    return Some(sparse_vector.values.clone());
                }
                // Fall back to deprecated format
                return Some(vector_output.data.clone());
            }
            None
        }
    }
}
