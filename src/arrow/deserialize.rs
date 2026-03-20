//! Schema-driven [`RecordBatch`] builder for `Qdrant` data.
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, FixedSizeListArray, Float32Array, Int32Array, ListArray, StringBuilder, StructArray,
    UInt32Array,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use qdrant_client::qdrant::{
    ScoredPoint, SparseVector, VectorOutput, VectorsOutput, point_id, vector_output, vectors_output,
};

use super::schema::{
    dense_vector_width, is_multi_vector_field, is_sparse_vector_field, multivector_width,
};

#[derive(Debug)]
enum Vector {
    Dense(Vec<f32>),
    Sparse(SparseVector),
    MultiDense(Vec<Vec<f32>>),
}

impl Vector {
    fn kind(&self) -> &'static str {
        match self {
            Self::Dense(_) => "dense",
            Self::Sparse(_) => "sparse",
            Self::MultiDense(_) => "multidense",
        }
    }

    fn from_vector_output(vector_output: VectorOutput) -> DataFusionResult<Self> {
        let vector = vector_output.vector.ok_or_else(|| {
            DataFusionError::Execution(
                "Qdrant returned a vector output without a typed vector body".to_owned(),
            )
        })?;

        match vector {
            vector_output::Vector::Dense(dense) => Ok(Self::Dense(dense.data)),
            vector_output::Vector::Sparse(sparse) => Ok(Self::Sparse(sparse)),
            vector_output::Vector::MultiDense(multi) => {
                Ok(Self::MultiDense(multi.vectors.into_iter().map(|vector| vector.data).collect()))
            }
        }
    }
}

enum FieldExtractor {
    Id(StringBuilder),
    Payload(StringBuilder),
    DenseVector { name: String, width: usize, rows: Vec<Vec<f32>> },
    MultiVector { name: String, width: usize, rows: Vec<Vec<Vec<f32>>> },
    SparseVector { name: String, rows: Vec<SparseVector> },
}

impl FieldExtractor {
    fn from_schema_field(field: &Field, capacity: usize) -> DataFusionResult<Self> {
        match field.name().as_str() {
            "id" => Ok(Self::Id(StringBuilder::with_capacity(capacity, capacity * 16))),
            "payload" => Ok(Self::Payload(StringBuilder::with_capacity(capacity, capacity * 64))),
            name => {
                if let Some(width) = dense_vector_width(field) {
                    Ok(Self::DenseVector {
                        name: name.to_string(),
                        width,
                        rows: Vec::with_capacity(capacity),
                    })
                } else if is_multi_vector_field(field) {
                    let width = multivector_width(field).ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "field '{}' is missing multivector width metadata",
                            field.name()
                        ))
                    })?;
                    Ok(Self::MultiVector {
                        name: name.to_string(),
                        width,
                        rows: Vec::with_capacity(capacity),
                    })
                } else if is_sparse_vector_field(field) {
                    Ok(Self::SparseVector {
                        name: name.to_string(),
                        rows: Vec::with_capacity(capacity),
                    })
                } else {
                    Err(DataFusionError::Execution(format!(
                        "unsupported scan field contract for '{}' with data type {}",
                        name,
                        field.data_type()
                    )))
                }
            }
        }
    }

    fn finish(self) -> DataFusionResult<ArrayRef> {
        match self {
            Self::Id(mut builder) | Self::Payload(mut builder) => Ok(Arc::new(builder.finish())),
            Self::DenseVector { name, width, rows } => build_dense_vector_array(&name, width, rows),
            Self::MultiVector { name, width, rows } => build_multivector_array(&name, width, rows),
            Self::SparseVector { name, rows } => build_sparse_vector_array(&name, rows),
        }
    }
}

pub struct QdrantRecordBatchBuilder {
    schema:           SchemaRef,
    field_extractors: Vec<FieldExtractor>,
}

impl QdrantRecordBatchBuilder {
    /// Create a schema-driven record-batch builder for Qdrant scan output.
    ///
    /// # Errors
    /// Returns an error if the projected schema contains unsupported field contracts.
    pub fn new(schema: SchemaRef, point_count: usize) -> DataFusionResult<Self> {
        let field_extractors = schema
            .fields()
            .iter()
            .map(|field| FieldExtractor::from_schema_field(field, point_count))
            .collect::<DataFusionResult<Vec<_>>>()?;

        Ok(Self { schema, field_extractors })
    }

    /// Append a single Qdrant point to the in-progress batch.
    ///
    /// # Errors
    /// Returns an error if the point does not match the admitted scan schema contract.
    pub fn append_point(&mut self, point: ScoredPoint) -> DataFusionResult<()> {
        let ScoredPoint { id, payload, vectors, .. } = point;
        let mut vector_lookup = build_vector_lookup(vectors)?;

        for extractor in &mut self.field_extractors {
            match extractor {
                FieldExtractor::Id(builder) => {
                    match id.as_ref().and_then(|id| id.point_id_options.as_ref()) {
                        Some(point_id::PointIdOptions::Num(number)) => {
                            builder.append_value(number.to_string());
                        }
                        Some(point_id::PointIdOptions::Uuid(uuid)) => builder.append_value(uuid),
                        None => {
                            return Err(DataFusionError::Execution(
                                "Qdrant returned a point without an id".to_owned(),
                            ));
                        }
                    }
                }
                FieldExtractor::Payload(builder) => {
                    let json = serde_json::to_string(&payload)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                    builder.append_value(json);
                }
                FieldExtractor::DenseVector { name, width, rows } => {
                    let vector = vector_lookup.remove(name.as_str()).ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Qdrant response is missing requested dense vector field '{name}'"
                        ))
                    })?;
                    match vector {
                        Vector::Dense(data) => {
                            validate_dense_vector(name, *width, &data)?;
                            rows.push(data);
                        }
                        other => {
                            return Err(DataFusionError::Execution(format!(
                                "field '{}' expected dense vector output, found {}",
                                name,
                                other.kind()
                            )));
                        }
                    }
                }
                FieldExtractor::MultiVector { name, width, rows } => {
                    let vector = vector_lookup.remove(name.as_str()).ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Qdrant response is missing requested multivector field '{name}'"
                        ))
                    })?;
                    match vector {
                        Vector::MultiDense(vectors) => {
                            validate_multivector(name, *width, &vectors)?;
                            rows.push(vectors);
                        }
                        other => {
                            return Err(DataFusionError::Execution(format!(
                                "field '{}' expected multivector output, found {}",
                                name,
                                other.kind()
                            )));
                        }
                    }
                }
                FieldExtractor::SparseVector { name, rows } => {
                    let vector = vector_lookup.remove(name.as_str()).ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Qdrant response is missing requested sparse vector field '{name}'"
                        ))
                    })?;
                    match vector {
                        Vector::Sparse(sparse) => {
                            validate_sparse_vector(name, &sparse)?;
                            rows.push(sparse);
                        }
                        other => {
                            return Err(DataFusionError::Execution(format!(
                                "field '{}' expected sparse vector output, found {}",
                                name,
                                other.kind()
                            )));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Finish the batch and materialize the projected Arrow arrays.
    ///
    /// # Errors
    /// Returns an error if the accumulated rows cannot be materialized into a valid `RecordBatch`.
    pub fn finish(self) -> DataFusionResult<RecordBatch> {
        let arrays = self
            .field_extractors
            .into_iter()
            .map(FieldExtractor::finish)
            .collect::<DataFusionResult<Vec<_>>>()?;

        RecordBatch::try_new(self.schema, arrays)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
    }
}

fn validate_dense_vector(name: &str, width: usize, data: &[f32]) -> DataFusionResult<()> {
    if data.len() != width {
        return Err(DataFusionError::Execution(format!(
            "field '{}' expected dense vector width {}, found {}",
            name,
            width,
            data.len()
        )));
    }
    Ok(())
}

fn validate_multivector(name: &str, width: usize, vectors: &[Vec<f32>]) -> DataFusionResult<()> {
    for (index, vector) in vectors.iter().enumerate() {
        if vector.len() != width {
            return Err(DataFusionError::Execution(format!(
                "field '{}' expected multivector inner width {}, row {} found {}",
                name,
                width,
                index,
                vector.len()
            )));
        }
    }
    Ok(())
}

fn validate_sparse_vector(name: &str, sparse: &SparseVector) -> DataFusionResult<()> {
    if sparse.indices.len() != sparse.values.len() {
        return Err(DataFusionError::Execution(format!(
            "field '{}' has malformed sparse vector: indices length {} does not match values \
             length {}",
            name,
            sparse.indices.len(),
            sparse.values.len()
        )));
    }
    Ok(())
}

fn int32_from_usize(name: &str, context: &str, value: usize) -> DataFusionResult<i32> {
    i32::try_from(value).map_err(|_| {
        DataFusionError::Execution(format!(
            "field '{name}' {context} exceeds Arrow i32 limits: {value}"
        ))
    })
}

fn checked_add_i32(name: &str, context: &str, left: i32, right: i32) -> DataFusionResult<i32> {
    left.checked_add(right).ok_or_else(|| {
        DataFusionError::Execution(format!("field '{name}' {context} exceeds Arrow i32 limits"))
    })
}

fn build_dense_vector_array(
    name: &str,
    width: usize,
    rows: Vec<Vec<f32>>,
) -> DataFusionResult<ArrayRef> {
    let width_i32 = int32_from_usize(name, "dense vector width", width)?;
    let mut flat_values = Vec::with_capacity(rows.len().saturating_mul(width));

    for row in rows {
        validate_dense_vector(name, width, &row)?;
        flat_values.extend(row);
    }

    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let values: ArrayRef = Arc::new(Float32Array::from(flat_values));
    Ok(Arc::new(FixedSizeListArray::new(item_field, width_i32, values, None)))
}

fn build_multivector_array(
    name: &str,
    width: usize,
    rows: Vec<Vec<Vec<f32>>>,
) -> DataFusionResult<ArrayRef> {
    let width_i32 = int32_from_usize(name, "multivector width", width)?;
    let mut data_offsets = Vec::with_capacity(rows.len() + 1);
    data_offsets.push(0_i32);
    let mut running_offset = 0_i32;
    let mut packed_values = Vec::new();
    let mut packed_shapes = Vec::with_capacity(rows.len() * 2);

    for vectors in rows {
        validate_multivector(name, width, &vectors)?;
        let mut row_elements = 0_usize;
        for vector in &vectors {
            row_elements += vector.len();
            packed_values.extend_from_slice(vector);
        }
        let row_elements_i32 = int32_from_usize(name, "multivector packed length", row_elements)?;
        running_offset =
            checked_add_i32(name, "multivector packed length", running_offset, row_elements_i32)?;
        data_offsets.push(running_offset);
        packed_shapes.push(int32_from_usize(name, "multivector row count", vectors.len())?);
        packed_shapes.push(width_i32);
    }

    let data_item_field = Arc::new(Field::new_list_field(DataType::Float32, false));
    let data_values: ArrayRef = Arc::new(Float32Array::from(packed_values));
    let data_list: ArrayRef = Arc::new(ListArray::new(
        data_item_field,
        OffsetBuffer::new(ScalarBuffer::from(data_offsets)),
        data_values,
        None,
    ));

    let shape_item_field = Arc::new(Field::new("item", DataType::Int32, false));
    let shape_values: ArrayRef = Arc::new(Int32Array::from(packed_shapes));
    let shape_array: ArrayRef =
        Arc::new(FixedSizeListArray::new(shape_item_field, 2, shape_values, None));

    let struct_fields = vec![
        Field::new("data", data_list.data_type().clone(), false),
        Field::new("shape", shape_array.data_type().clone(), false),
    ];
    Ok(Arc::new(StructArray::new(struct_fields.into(), vec![data_list, shape_array], None)))
}

fn build_sparse_vector_array(name: &str, rows: Vec<SparseVector>) -> DataFusionResult<ArrayRef> {
    let mut packed_shapes = Vec::with_capacity(rows.len() * 2);
    let mut row_ptr_offsets = vec![0_i32];
    let mut row_ptr_values = Vec::new();
    let mut col_offsets = vec![0_i32];
    let mut col_values = Vec::new();
    let mut value_offsets = vec![0_i32];
    let mut value_values = Vec::new();

    for sparse in rows {
        validate_sparse_vector(name, &sparse)?;
        let nnz = sparse.indices.len();
        let nnz_i32 = int32_from_usize(name, "sparse non-zero length", nnz)?;
        let cols = sparse
            .indices
            .iter()
            .copied()
            .max()
            .map(|index| {
                let cols = index.checked_add(1).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "field '{name}' sparse dimension exceeds u32 limits"
                    ))
                })?;
                i32::try_from(cols).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "field '{name}' sparse dimension exceeds Arrow i32 limits: {cols}"
                    ))
                })
            })
            .transpose()?
            .unwrap_or(0);

        packed_shapes.push(1);
        packed_shapes.push(cols);

        row_ptr_values.push(0);
        row_ptr_values.push(nnz_i32);
        let next_row_ptr_offset = checked_add_i32(
            name,
            "sparse row pointer offsets",
            *row_ptr_offsets.last().expect("offset seed"),
            2,
        )?;
        row_ptr_offsets.push(next_row_ptr_offset);

        col_values.extend(sparse.indices);
        let next_col_offset = checked_add_i32(
            name,
            "sparse column offsets",
            *col_offsets.last().expect("offset seed"),
            nnz_i32,
        )?;
        col_offsets.push(next_col_offset);

        value_values.extend(sparse.values);
        let next_value_offset = checked_add_i32(
            name,
            "sparse value offsets",
            *value_offsets.last().expect("offset seed"),
            nnz_i32,
        )?;
        value_offsets.push(next_value_offset);
    }

    let shape_item_field = Arc::new(Field::new("item", DataType::Int32, false));
    let shape_values: ArrayRef = Arc::new(Int32Array::from(packed_shapes));
    let shape_array: ArrayRef =
        Arc::new(FixedSizeListArray::new(shape_item_field, 2, shape_values, None));

    let row_ptr_item_field = Arc::new(Field::new_list_field(DataType::Int32, false));
    let row_ptr_values_array: ArrayRef = Arc::new(Int32Array::from(row_ptr_values));
    let row_ptr_array: ArrayRef = Arc::new(ListArray::new(
        row_ptr_item_field,
        OffsetBuffer::new(ScalarBuffer::from(row_ptr_offsets)),
        row_ptr_values_array,
        None,
    ));

    let col_item_field = Arc::new(Field::new_list_field(DataType::UInt32, false));
    let col_values_array: ArrayRef = Arc::new(UInt32Array::from(col_values));
    let col_array: ArrayRef = Arc::new(ListArray::new(
        col_item_field,
        OffsetBuffer::new(ScalarBuffer::from(col_offsets)),
        col_values_array,
        None,
    ));

    let value_item_field = Arc::new(Field::new_list_field(DataType::Float32, false));
    let value_values_array: ArrayRef = Arc::new(Float32Array::from(value_values));
    let value_array: ArrayRef = Arc::new(ListArray::new(
        value_item_field,
        OffsetBuffer::new(ScalarBuffer::from(value_offsets)),
        value_values_array,
        None,
    ));

    let struct_fields = vec![
        Field::new("shape", shape_array.data_type().clone(), false),
        Field::new("row_ptrs", row_ptr_array.data_type().clone(), false),
        Field::new("col_indices", col_array.data_type().clone(), false),
        Field::new("values", value_array.data_type().clone(), false),
    ];
    Ok(Arc::new(StructArray::new(
        struct_fields.into(),
        vec![shape_array, row_ptr_array, col_array, value_array],
        None,
    )))
}

fn build_vector_lookup(
    vectors: Option<VectorsOutput>,
) -> DataFusionResult<HashMap<String, Vector>> {
    let mut lookup = HashMap::new();

    if let Some(vectors) = vectors {
        match vectors.vectors_options {
            Some(vectors_output::VectorsOptions::Vector(vector_output)) => {
                drop(
                    lookup.insert("vector".to_string(), Vector::from_vector_output(vector_output)?),
                );
            }
            Some(vectors_output::VectorsOptions::Vectors(named_vectors)) => {
                for (name, vector_output) in named_vectors.vectors {
                    drop(lookup.insert(name, Vector::from_vector_output(vector_output)?));
                }
            }
            None => {}
        }
    }

    Ok(lookup)
}

#[cfg(test)]
mod tests {
    #![allow(deprecated)]

    use std::collections::HashMap;

    use arrow_schema::extension::{
        EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType, VariableShapeTensor,
    };
    use datafusion::arrow::array::Array;
    use datafusion::arrow::array::types::Float32Type;
    use ndarrow::{
        CsrMatrixBatchExtension, csr_matrix_batch_iter, fixed_size_list_as_array2,
        variable_shape_tensor_iter,
    };
    use qdrant_client::qdrant::{DenseVector, MultiDenseVector, SparseVector, vector_output};

    use super::*;

    fn assert_f32_eq(left: f32, right: f32) {
        assert!((left - right).abs() < 1.0e-6, "left={left}, right={right}");
    }

    fn multivector_test_field(array: &StructArray, width: i32) -> Field {
        let extension = VariableShapeTensor::try_new(
            DataType::Float32,
            2,
            None,
            None,
            Some(vec![None, Some(width)]),
        )
        .expect("variable tensor extension");
        extension.supports_data_type(array.data_type()).expect("compatible multivector storage");

        Field::new("multi", array.data_type().clone(), false).with_metadata(HashMap::from([
            (EXTENSION_TYPE_NAME_KEY.to_owned(), VariableShapeTensor::NAME.to_owned()),
            (
                EXTENSION_TYPE_METADATA_KEY.to_owned(),
                serde_json::json!({ "uniform_shape": [serde_json::Value::Null, width] })
                    .to_string(),
            ),
        ]))
    }

    #[test]
    fn vector_from_typed_output_uses_current_qdrant_shape() {
        let dense_vector_output = VectorOutput {
            vector:        Some(vector_output::Vector::Dense(DenseVector {
                data: vec![1.0, 2.0, 3.0],
            })),
            data:          vec![],
            indices:       None,
            vectors_count: None,
        };
        match Vector::from_vector_output(dense_vector_output) {
            Ok(Vector::Dense(data)) => assert_eq!(data, vec![1.0, 2.0, 3.0]),
            other => panic!("expected dense vector, found {other:?}"),
        }

        let sparse_vector_output = VectorOutput {
            vector:        Some(vector_output::Vector::Sparse(SparseVector {
                indices: vec![0, 2, 5],
                values:  vec![0.1, 0.2, 0.3],
            })),
            data:          vec![],
            indices:       None,
            vectors_count: None,
        };
        match Vector::from_vector_output(sparse_vector_output) {
            Ok(Vector::Sparse(sparse)) => {
                assert_eq!(sparse.indices, vec![0, 2, 5]);
                assert_eq!(sparse.values, vec![0.1, 0.2, 0.3]);
            }
            other => panic!("expected sparse vector, found {other:?}"),
        }

        let multi_vector_output = VectorOutput {
            vector:        Some(vector_output::Vector::MultiDense(MultiDenseVector {
                vectors: vec![DenseVector { data: vec![1.0, 2.0] }, DenseVector {
                    data: vec![3.0, 4.0],
                }],
            })),
            data:          vec![],
            indices:       None,
            vectors_count: None,
        };
        match Vector::from_vector_output(multi_vector_output) {
            Ok(Vector::MultiDense(multi)) => {
                assert_eq!(multi, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
            }
            other => panic!("expected multivector, found {other:?}"),
        }
    }

    #[test]
    fn vector_from_output_rejects_missing_typed_body() {
        let vector_output = VectorOutput {
            vector:        None,
            data:          vec![],
            indices:       None,
            vectors_count: None,
        };
        assert!(Vector::from_vector_output(vector_output).is_err());
    }

    #[test]
    fn dense_vector_arrays_round_trip_into_ndarrow_views() {
        let array = build_dense_vector_array("embedding", 3, vec![vec![1.0, 2.0, 3.0], vec![
            4.0, 5.0, 6.0,
        ]])
        .expect("dense array");
        let array = array.as_any().downcast_ref::<FixedSizeListArray>().expect("fixed-size list");

        let view = fixed_size_list_as_array2::<Float32Type>(array).expect("ndarray view");
        assert_eq!(view.shape(), &[2, 3]);
        assert_f32_eq(view[[0, 0]], 1.0);
        assert_f32_eq(view[[1, 2]], 6.0);
    }

    #[test]
    fn multivector_arrays_round_trip_into_ndarrow_views() {
        let array = build_multivector_array("multi", 2, vec![
            vec![vec![1.0, 2.0], vec![3.0, 4.0]],
            vec![vec![5.0, 6.0]],
        ])
        .expect("multivector array");
        let array = array.as_any().downcast_ref::<StructArray>().expect("struct");
        let field = multivector_test_field(array, 2);

        let rows = variable_shape_tensor_iter::<Float32Type>(&field, array)
            .expect("variable tensor iterator")
            .collect::<Result<Vec<_>, _>>()
            .expect("valid multivector rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 0);
        assert_eq!(rows[0].1.shape(), &[2, 2]);
        assert_f32_eq(rows[0].1[[0, 0]], 1.0);
        assert_f32_eq(rows[0].1[[1, 1]], 4.0);
        assert_eq!(rows[1].1.shape(), &[1, 2]);
        assert_f32_eq(rows[1].1[[0, 1]], 6.0);
    }

    #[test]
    fn sparse_vector_arrays_round_trip_into_ndarrow_views() {
        let array = build_sparse_vector_array("keywords", vec![
            SparseVector { indices: vec![0, 5], values: vec![0.1, 0.9] },
            SparseVector { indices: vec![1, 3, 4], values: vec![0.2, 0.3, 0.4] },
        ])
        .expect("sparse array");
        let array = array.as_any().downcast_ref::<StructArray>().expect("struct");
        let mut field = Field::new("keywords", array.data_type().clone(), false);
        field
            .try_with_extension_type(
                CsrMatrixBatchExtension::try_new(array.data_type(), ()).expect("csr extension"),
            )
            .expect("compatible csr storage");

        let rows = csr_matrix_batch_iter::<Float32Type>(&field, array)
            .expect("csr iterator")
            .collect::<Result<Vec<_>, _>>()
            .expect("valid sparse rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 0);
        assert_eq!(rows[0].1.nrows, 1);
        assert_eq!(rows[0].1.ncols, 6);
        assert_eq!(rows[0].1.col_indices, &[0, 5]);
        assert_eq!(rows[0].1.values, &[0.1, 0.9]);
        assert_eq!(rows[1].1.nrows, 1);
        assert_eq!(rows[1].1.ncols, 5);
        assert_eq!(rows[1].1.col_indices, &[1, 3, 4]);
        assert_eq!(rows[1].1.values, &[0.2, 0.3, 0.4]);
    }
}
