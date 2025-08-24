//! Efficient record batch builder over `ScoredPoint` vectors
//!
//! TODO: Remove - IMPORTANT! Currently only supports f32 as Datatype. This will error with other
//! vector data types

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
use crate::arrow::convert_to_multi_vector;

/// Owned vector data extracted from a point
#[derive(Debug)]
pub enum VectorData {
    /// Single unnamed vector
    Unnamed(Vector),
    /// Multiple named vectors
    Named(HashMap<String, Vector>),
}

/// The vector values
#[derive(Debug)]
pub enum Vector {
    Dense(Vec<f32>),
    Sparse(SparseVector),
    MultiDense(Vec<Vec<f32>>),
}

impl VectorData {
    /// Extract vector data from `ScoredPoint` vectors field
    pub fn from_vectors(vectors: VectorsOutput) -> Option<Self> {
        match vectors.vectors_options? {
            vectors_output::VectorsOptions::Vector(vector) => {
                let content = Vector::from_vector_output(vector)?;
                Some(Self::Unnamed(content))
            }
            vectors_output::VectorsOptions::Vectors(named_vectors) => {
                let mut named_content = HashMap::new();
                for (name, vector_output) in named_vectors.vectors {
                    if let Some(content) = Vector::from_vector_output(vector_output) {
                        drop(named_content.insert(name, content));
                    }
                }
                if named_content.is_empty() { None } else { Some(Self::Named(named_content)) }
            }
        }
    }
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

// pub trait VectorArrayBuilder {
//     fn append(&mut self, vector: VectorData);

//     fn finish(&mut self) -> ArrayRef;
// }

// pub enum QdrantVectorBuilder {
//     Dense(ListBuilder<Float32Builder>),
//     Multi(ListBuilder<ListBuilder<Float32Builder>>),
//     Sparse((ListBuilder<UInt32Builder>, ListBuilder<Float32Builder>)),
// }

// impl VectorArrayBuilder for QdrantVectorBuilder {
//     fn append(&mut self, vector: VectorData) {
//         match self {
//             Self::Dense(builder) => builder.append(vector.data),
//             Self::Multi(builder) => builder.append(vector.data),
//             Self::Sparse((keys, values)) => {
//                 keys.append(vector.keys);
//                 values.append(vector.data);
//             }
//         }
//     }

//     fn finish(&mut self) -> ArrayRef {
//         match self {
//             Self::Dense(builder) => builder.finish(),
//             Self::Multi(builder) => builder.finish(),
//             Self::Sparse((keys, values)) => {
//                 keys.finish();
//                 values.finish();
//             }
//         }
//     }
// }

/// Efficient record batch builder that destructures points once
pub struct QdrantRecordBatchBuilder {
    schema: SchemaRef,

    // Simple builders for non-vector fields
    id_builder:      Option<StringBuilder>,
    payload_builder: Option<StringBuilder>,

    // Vector builders organized by field name
    dense_builders:  HashMap<String, ListBuilder<Float32Builder>>,
    multi_builders:  HashMap<String, ListBuilder<ListBuilder<Float32Builder>>>,
    sparse_builders: HashMap<String, (ListBuilder<UInt32Builder>, ListBuilder<Float32Builder>)>,
}

impl QdrantRecordBatchBuilder {
    /// Create builder from schema with proper capacity allocation
    pub fn new(schema: SchemaRef, point_count: usize) -> Self {
        let mut id_builder = None;
        let mut payload_builder = None;
        let mut dense_builders = HashMap::new();
        let mut multi_builders = HashMap::new();
        let mut sparse_builders = HashMap::new();

        for field in schema.fields() {
            match field.name().as_str() {
                "id" => {
                    id_builder = Some(StringBuilder::with_capacity(point_count, point_count * 16));
                }
                "payload" => {
                    payload_builder =
                        Some(StringBuilder::with_capacity(point_count, point_count * 64));
                }
                // Sparse vector (List<u32>, List<f32>)
                name if name.ends_with("_indices") => {
                    drop(sparse_builders.insert(
                        name.trim_end_matches("_indices").to_string(),
                        (
                            ListBuilder::with_capacity(UInt32Builder::new(), point_count),
                            ListBuilder::with_capacity(Float32Builder::new(), point_count),
                        ),
                    ));
                }
                // Handled via indices above
                name if name.ends_with("_values") => {}
                // Multi vector (List<List<T>>)
                name if is_multi_vector_field(field) => drop(multi_builders.insert(
                    name.to_string(),
                    ListBuilder::with_capacity(
                        ListBuilder::new(Float32Builder::new()),
                        point_count,
                    ),
                )),
                // Dense vector (List<T>)
                name => drop(dense_builders.insert(
                    name.to_string(),
                    ListBuilder::with_capacity(Float32Builder::new(), point_count),
                )),
            }
        }

        Self {
            schema,
            id_builder,
            payload_builder,
            dense_builders,
            multi_builders,
            sparse_builders,
        }
    }

    /// Append a point using owned destructuring
    pub fn append_point(&mut self, point: ScoredPoint) {
        let ScoredPoint { id, payload, vectors, .. } = point;

        // Handle ID
        if let Some(ref mut builder) = self.id_builder {
            if let Some(id) = id {
                match id.point_id_options {
                    Some(point_id::PointIdOptions::Num(n)) => builder.append_value(n.to_string()),
                    Some(point_id::PointIdOptions::Uuid(s)) => builder.append_value(s),
                    None => builder.append_value(""),
                }
            } else {
                builder.append_null();
            }
        }

        // Handle payload
        if let Some(ref mut builder) = self.payload_builder {
            if !payload.is_empty()
                && let Ok(json) = serde_json::to_string(&payload)
            {
                builder.append_value(json);
            } else {
                builder.append_null();
            }
        }

        // Handle vectors
        if let Some(owned_vectors) = vectors.and_then(VectorData::from_vectors) {
            self.append_vector_data(owned_vectors);
        } else {
            // No vectors - append nulls to all vector builders
            for builder in self.dense_builders.values_mut() {
                builder.append(false);
            }
            for builder in self.multi_builders.values_mut() {
                builder.append(false);
            }
            for (idxs, vals) in self.sparse_builders.values_mut() {
                idxs.append(false);
                vals.append(false);
            }
        }
    }

    /// Append owned vector data to appropriate builders
    #[expect(clippy::too_many_lines)]
    fn append_vector_data(&mut self, vector_data: VectorData) {
        macro_rules! append_vector {
            ($builder:expr, $data:expr) => {
                if let Some(builder) = $builder {
                    builder.values().append_slice($data);
                    builder.append(true);
                }
            };
            (multi => $builder:expr, $vectors:expr) => {
                if let Some(builder) = $builder {
                    for vector in $vectors {
                        builder.values().values().append_slice(vector);
                        builder.values().append(true);
                    }
                    builder.append(true);
                }
            };
            (sparse => $builders:expr, $vectors:expr) => {
                if let Some((idxs, vals)) = $builders {
                    let SparseVector { indices, values } = $vectors;
                    idxs.values().append_slice(indices);
                    vals.values().append_slice(values);
                    idxs.append(true);
                    vals.append(true);
                }
            };
        }

        match vector_data {
            VectorData::Unnamed(content) => {
                // Handle unnamed vector (field name "vector")
                match &content {
                    Vector::Dense(data) => {
                        append_vector!(self.dense_builders.get_mut("vector"), data);
                    }
                    Vector::MultiDense(vectors) => {
                        append_vector!(multi => self.multi_builders.get_mut("vector"), vectors);
                    }
                    Vector::Sparse(sparse) => {
                        append_vector!(sparse => self.sparse_builders.get_mut("vector"), sparse);
                    }
                }
                // Append nulls to other vector types that don't match
                if !matches!(content, Vector::Dense(_)) {
                    for builder in self.dense_builders.values_mut() {
                        builder.append(false);
                    }
                }
                if !matches!(content, Vector::MultiDense(_)) {
                    for builder in self.multi_builders.values_mut() {
                        builder.append(false);
                    }
                }
                if !matches!(content, Vector::Sparse { .. }) {
                    let sparse_builders = self
                        .sparse_builders
                        .iter_mut()
                        .filter(|(n, _)| *n != "vector")
                        .map(|(_, b)| b);
                    for (idx, val) in sparse_builders {
                        idx.append(false);
                        val.append(false);
                    }
                }
            }
            VectorData::Named(named_vectors) => {
                // Handle each named vector
                for (name, content) in &named_vectors {
                    match content {
                        Vector::Dense(data) => {
                            append_vector!(self.dense_builders.get_mut(name), data);
                        }
                        Vector::MultiDense(vectors) => {
                            append_vector!(multi => self.multi_builders.get_mut(name), vectors);
                        }
                        Vector::Sparse(sparse) => {
                            append_vector!(sparse => self.sparse_builders.get_mut(name), sparse);
                        }
                    }
                }
                // Append nulls to builders that didn't get data
                let dense_builders = self
                    .dense_builders
                    .iter_mut()
                    .filter(|(n, _)| !named_vectors.contains_key(*n))
                    .map(|(_, b)| b);
                for builder in dense_builders {
                    builder.append(false);
                }
                let multi_builders = self
                    .multi_builders
                    .iter_mut()
                    .filter(|(n, _)| !named_vectors.contains_key(*n))
                    .map(|(_, b)| b);
                for builder in multi_builders {
                    builder.append(false);
                }
                for (idx, val) in &mut self
                    .sparse_builders
                    .iter_mut()
                    .filter(|(n, _)| !named_vectors.contains_key(*n))
                    .map(|(_, b)| b)
                {
                    idx.append(false);
                    val.append(false);
                }
            }
        }
    }

    /// Finish building and create the final `RecordBatch`
    ///
    /// # Errors
    pub fn finish(mut self) -> DataFusionResult<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());

        // Build arrays in schema order
        for field in self.schema.fields() {
            match field.name().as_str() {
                "id" => {
                    let Some(mut builder) = self.id_builder.take() else {
                        return Err(DataFusionError::Internal("Missing ID builder".to_string()));
                    };
                    arrays.push(Arc::new(builder.finish()));
                }
                "payload" => {
                    let Some(mut builder) = self.payload_builder.take() else {
                        return Err(DataFusionError::Internal(
                            "Missing payload builder".to_string(),
                        ));
                    };
                    arrays.push(Arc::new(builder.finish()));
                }
                name if name.ends_with("_indices") => {
                    let base_name = name.trim_end_matches("_indices");
                    if let Some((idxs, _)) = self.sparse_builders.get_mut(base_name) {
                        arrays.push(Arc::new(std::mem::take(idxs).finish()));
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "Missing sparse indices builder for {base_name}"
                        )));
                    }
                }
                // Handled above
                name if name.ends_with("_values") => {
                    let base_name = name.trim_end_matches("_values");
                    if let Some((_, vals)) = self.sparse_builders.get_mut(base_name) {
                        arrays.push(Arc::new(std::mem::take(vals).finish()));
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "Missing sparse values builder for {base_name}"
                        )));
                    }
                }
                name => {
                    // Try multi-vector first, then dense
                    if let Some(mut builder) = self.multi_builders.remove(name) {
                        arrays.push(Arc::new(builder.finish()));
                    } else if let Some(mut builder) = self.dense_builders.remove(name) {
                        arrays.push(Arc::new(builder.finish()));
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "Missing vector builder for {name}"
                        )));
                    }
                }
            }
        }
        RecordBatch::try_new(self.schema, arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}
