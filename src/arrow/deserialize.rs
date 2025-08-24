//! Schema-driven [`RecordBatch`] builder for `Qdrant` data
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use qdrant_client::qdrant::{
    ScoredPoint, SparseVector, VectorOutput, VectorsOutput, point_id, vector_output, vectors_output,
};

use super::schema::is_multi_vector_field;

/// Convert flat vector data into multi-vector format with proper validation.
///
/// This function implements the same conversion logic as `Qdrant`'s Rust client `try_into_multi()`
/// method. It takes a flat array of floats and splits it into multiple sub-vectors based on the
/// vectors count. This handles Qdrant's deprecated protobuf format for multi-vectors.
///
/// # Arguments
/// * `data` - Flat array of float values representing concatenated vectors
/// * `vectors_count` - Number of vectors to split the data into
///
/// # Returns
/// A vector of vectors, where each sub-vector represents one embedding.
///
/// # Errors
/// Returns a `DataFusionError` if the data length is not evenly divisible by the vectors count,
/// which indicates malformed multi-vector data.
///
/// # Examples
/// ```rust,ignore
/// use qdrant_datafusion::arrow::deserialize::convert_to_multi_vector;
///
/// // Convert [1.0, 2.0, 3.0, 4.0] into 2 vectors of length 2
/// let data = vec![1.0, 2.0, 3.0, 4.0];
/// let result = convert_to_multi_vector(&data, 2).unwrap();
/// assert_eq!(result, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
/// ```
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

/// Internal vector content representation for efficient processing.
///
/// This enum provides a clean abstraction over `Qdrant`'s various vector formats,
/// normalizing them for consistent processing in the record batch builder.
/// It handles both newer and deprecated protobuf formats from Qdrant.
#[derive(Debug)]
pub enum Vector {
    Dense(Vec<f32>),
    Sparse(SparseVector),
    MultiDense(Vec<Vec<f32>>),
}

impl Vector {
    /// Extract vector content from `VectorOutput`
    fn from_vector_output(vector_output: VectorOutput) -> Option<Self> {
        // Check newer format first
        if let Some(vector) = vector_output.vector {
            return match vector {
                vector_output::Vector::Dense(dense) => Some(Self::Dense(dense.data)),
                vector_output::Vector::Sparse(sparse) => Some(Self::Sparse(sparse)),
                vector_output::Vector::MultiDense(multi) => {
                    Some(Self::MultiDense(multi.vectors.into_iter().map(|v| v.data).collect()))
                }
            };
        }

        // Fall back to deprecated format
        if let Some(vectors_count) = vector_output.vectors_count
            && let Ok(multi_vectors) = convert_to_multi_vector(&vector_output.data, vectors_count)
        {
            // Multi-vector in deprecated format
            return Some(Self::MultiDense(multi_vectors));
        }

        // Check for sparse in deprecated format
        if let Some(indices) = vector_output.indices {
            return Some(Self::Sparse(SparseVector {
                indices: indices.data,
                values:  vector_output.data,
            }));
        }

        // No vectors found
        if vector_output.data.is_empty() {
            return None;
        }

        // Regular dense vector in deprecated format
        Some(Self::Dense(vector_output.data))
    }
}

/// Schema-driven field extractor - one per schema field
enum FieldExtractor {
    Id(StringBuilder),
    Payload(StringBuilder),
    DenseVector { name: String, builder: ListBuilder<Float32Builder> },
    MultiVector { name: String, builder: ListBuilder<ListBuilder<Float32Builder>> },
    SparseIndices { name: String, builder: ListBuilder<UInt32Builder> },
    SparseValues { name: String, builder: ListBuilder<Float32Builder> },
}

impl FieldExtractor {
    /// Create field extractor from schema field
    fn from_schema_field(field: &datafusion::arrow::datatypes::Field, capacity: usize) -> Self {
        match field.name().as_str() {
            "id" => Self::Id(StringBuilder::with_capacity(capacity, capacity * 16)),
            "payload" => Self::Payload(StringBuilder::with_capacity(capacity, capacity * 64)),
            name if name.ends_with("_indices") => Self::SparseIndices {
                name:    name.to_string(),
                builder: ListBuilder::with_capacity(UInt32Builder::new(), capacity),
            },
            name if name.ends_with("_values") => Self::SparseValues {
                name:    name.to_string(),
                builder: ListBuilder::with_capacity(Float32Builder::new(), capacity),
            },
            name if is_multi_vector_field(field) => Self::MultiVector {
                name:    name.to_string(),
                builder: ListBuilder::with_capacity(
                    ListBuilder::new(Float32Builder::new()),
                    capacity,
                ),
            },
            name => Self::DenseVector {
                name:    name.to_string(),
                builder: ListBuilder::with_capacity(Float32Builder::new(), capacity),
            },
        }
    }
}

/// Schema-driven [`RecordBatch`] builder for `Qdrant` data.
///
/// This is the core component that converts `Qdrant` `ScoredPoint` data into Arrow `RecordBatch`es
/// for `DataFusion` consumption. It uses a clean, schema-driven architecture that eliminates the
/// complex nested matching logic of previous implementations.
///
/// # Architecture
/// The builder is initialized with an Arrow schema and creates one `FieldExtractor` per schema
/// field in the exact same order. During processing, each point is processed once with all fields
/// updated in a single pass, achieving O(F) performance where F is the number of fields.
///
/// # Performance Features
/// - **Single-Pass Processing**: Each point is destructured once and all fields updated
/// - **Owned Iteration**: No unnecessary borrowing or reference management
/// - **Pre-Allocated Builders**: Capacity is allocated upfront for optimal memory usage
/// - **Inline Logic**: All processing logic is inline with no hidden function calls
///
/// # Examples
/// ```rust,ignore
/// use qdrant_datafusion::arrow::deserialize::QdrantRecordBatchBuilder;
/// use datafusion::arrow::datatypes::{Schema, Field, DataType};
/// use std::sync::Arc;
///
/// // Create schema and builder
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Utf8, false),
///     Field::new("vector", DataType::List(
///         Arc::new(Field::new("item", DataType::Float32, true))
///     ), true),
/// ]));
///
/// let mut builder = QdrantRecordBatchBuilder::new(schema, 1000);
///
/// // Process points (would normally come from Qdrant query)
/// // for point in qdrant_points {
/// //     builder.append_point(point);
/// // }
///
/// // Create final record batch
/// // let batch = builder.finish()?;
/// ```
pub struct QdrantRecordBatchBuilder {
    schema:           SchemaRef,
    field_extractors: Vec<FieldExtractor>, // 1:1 with schema fields, in schema order
}

impl QdrantRecordBatchBuilder {
    /// Create builder from schema with proper capacity allocation
    pub fn new(schema: SchemaRef, point_count: usize) -> Self {
        // Schema-driven initialization - one extractor per field, in schema order
        let field_extractors = schema
            .fields()
            .iter()
            .map(|field| FieldExtractor::from_schema_field(field, point_count))
            .collect();

        Self { schema, field_extractors }
    }

    /// Append a point using owned destructuring - defines its own invariants
    pub fn append_point(&mut self, point: ScoredPoint) {
        // Single destructuring
        let ScoredPoint { id, payload, vectors, .. } = point;

        // Build lookup once per point
        let vector_lookup = build_vector_lookup(vectors);

        // Schema-driven extraction - inline logic, no hidden functions
        for extractor in &mut self.field_extractors {
            match extractor {
                FieldExtractor::Id(builder) => {
                    if let Some(id) = &id {
                        match &id.point_id_options {
                            Some(point_id::PointIdOptions::Num(n)) => {
                                builder.append_value(n.to_string());
                            }
                            Some(point_id::PointIdOptions::Uuid(s)) => builder.append_value(s),
                            None => builder.append_value(""),
                        }
                    } else {
                        builder.append_null();
                    }
                }

                FieldExtractor::Payload(builder) => {
                    if !payload.is_empty()
                        && let Ok(json) = serde_json::to_string(&payload)
                    {
                        builder.append_value(json);
                    } else {
                        builder.append_null();
                    }
                }

                FieldExtractor::DenseVector { name, builder } => {
                    if let Some(Vector::Dense(data)) = vector_lookup.get(name) {
                        builder.values().append_slice(data);
                        builder.append(true);
                    } else {
                        builder.append(false);
                    }
                }

                FieldExtractor::MultiVector { name, builder } => {
                    if let Some(Vector::MultiDense(vectors)) = vector_lookup.get(name) {
                        for vector in vectors {
                            builder.values().values().append_slice(vector);
                            builder.values().append(true);
                        }
                        builder.append(true);
                    } else {
                        builder.append(false);
                    }
                }

                FieldExtractor::SparseIndices { name, builder } => {
                    let sparse_name = name.trim_end_matches("_indices");
                    if let Some(Vector::Sparse(sparse)) = vector_lookup.get(sparse_name) {
                        builder.values().append_slice(&sparse.indices);
                        builder.append(true);
                    } else {
                        builder.append(false);
                    }
                }

                FieldExtractor::SparseValues { name, builder } => {
                    let sparse_name = name.trim_end_matches("_values");
                    if let Some(Vector::Sparse(sparse)) = vector_lookup.get(sparse_name) {
                        builder.values().append_slice(&sparse.values);
                        builder.append(true);
                    } else {
                        builder.append(false);
                    }
                }
            }
        }
    }

    /// Finish building and create the final `RecordBatch`
    ///
    /// # Errors
    /// - Returns an error if `RecordBatch` creation fails.
    pub fn finish(self) -> DataFusionResult<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());

        // Extract arrays from field extractors in schema order
        for extractor in self.field_extractors {
            let array: ArrayRef = match extractor {
                FieldExtractor::Id(mut builder) | FieldExtractor::Payload(mut builder) => {
                    Arc::new(builder.finish())
                }
                FieldExtractor::DenseVector { mut builder, .. }
                | FieldExtractor::SparseValues { mut builder, .. } => Arc::new(builder.finish()),
                FieldExtractor::MultiVector { mut builder, .. } => Arc::new(builder.finish()),
                FieldExtractor::SparseIndices { mut builder, .. } => Arc::new(builder.finish()),
            };
            arrays.push(array);
        }

        RecordBatch::try_new(self.schema, arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// Simple helper - builds flat lookup map once per point
fn build_vector_lookup(vectors: Option<VectorsOutput>) -> HashMap<String, Vector> {
    let mut lookup = HashMap::new();

    if let Some(vectors) = vectors {
        match vectors.vectors_options {
            Some(vectors_output::VectorsOptions::Vector(vector_output)) => {
                // Unnamed case - use "vector" as key
                if let Some(content) = Vector::from_vector_output(vector_output) {
                    drop(lookup.insert("vector".to_string(), content));
                }
            }
            Some(vectors_output::VectorsOptions::Vectors(named_vectors)) => {
                // Named case - use actual names
                for (name, vector_output) in named_vectors.vectors {
                    if let Some(content) = Vector::from_vector_output(vector_output) {
                        drop(lookup.insert(name, content));
                    }
                }
            }
            None => {}
        }
    }

    lookup
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_to_multi_vector_error() {
        // Test the error path when data length is not divisible by vectors count
        let data = vec![1.0, 2.0, 3.0]; // length = 3
        let vectors_count = 2; // 3 % 2 != 0

        let result = convert_to_multi_vector(&data, vectors_count);

        assert!(result.is_err());
        if let Err(DataFusionError::External(boxed_error)) = result {
            let error_msg = boxed_error.to_string();
            assert!(error_msg.contains("Malformed multi vector"));
            assert!(error_msg.contains("data length 3 is not divisible by vectors count 2"));
        } else {
            panic!("Expected DataFusionError::External");
        }
    }

    #[test]
    fn test_vector_from_new_format() {
        use qdrant_client::qdrant::{DenseVector, MultiDenseVector, SparseVector, vector_output};

        // Test newer format dense vector (lines 55-59)
        let dense_vector_output = VectorOutput {
            vector:        Some(vector_output::Vector::Dense(DenseVector {
                data: vec![1.0, 2.0, 3.0],
            })),
            data:          vec![], // Should be ignored when vector.is_some()
            indices:       None,
            vectors_count: None,
        };

        let result = Vector::from_vector_output(dense_vector_output);
        if let Some(Vector::Dense(data)) = result {
            assert_eq!(data, vec![1.0, 2.0, 3.0]);
        } else {
            panic!("Expected Dense vector");
        }

        // Test newer format sparse vector
        let sparse_vector_output = VectorOutput {
            vector:        Some(vector_output::Vector::Sparse(SparseVector {
                indices: vec![0, 2, 5],
                values:  vec![0.1, 0.2, 0.3],
            })),
            data:          vec![], // Should be ignored
            indices:       None,
            vectors_count: None,
        };

        let result = Vector::from_vector_output(sparse_vector_output);
        if let Some(Vector::Sparse(sparse)) = result {
            assert_eq!(sparse.indices, vec![0, 2, 5]);
            assert_eq!(sparse.values, vec![0.1, 0.2, 0.3]);
        } else {
            panic!("Expected Sparse vector");
        }

        // Test newer format multi-dense vector
        let multi_vector_output = VectorOutput {
            vector:        Some(vector_output::Vector::MultiDense(MultiDenseVector {
                vectors: vec![DenseVector { data: vec![1.0, 2.0] }, DenseVector {
                    data: vec![3.0, 4.0],
                }],
            })),
            data:          vec![], // Should be ignored
            indices:       None,
            vectors_count: None,
        };

        let result = Vector::from_vector_output(multi_vector_output);
        if let Some(Vector::MultiDense(multi)) = result {
            assert_eq!(multi, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
        } else {
            panic!("Expected MultiDense vector");
        }
    }
}
