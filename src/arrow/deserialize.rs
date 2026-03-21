//! Schema-driven [`RecordBatch`] builder for `Qdrant` data.
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, FixedSizeListArray, Float32Array, Int32Array, ListArray, NullBufferBuilder,
    StringBuilder, StructArray, UInt32Array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::exec_err;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use qdrant_client::qdrant::{
    PointId, RetrievedPoint, ScoredPoint, Value, VectorOutput, VectorsOutput, point_id,
    vector_output, vectors_output,
};

use super::schema::{
    ID_FIELD_NAME, PAYLOAD_FIELD_NAME, UNNAMED_VECTOR_FIELD_NAME, dense_vector_width,
    is_multi_vector_field, is_sparse_vector_field, multivector_width,
};

fn vector_kind(vector: &vector_output::Vector) -> &'static str {
    match vector {
        vector_output::Vector::Dense(_) => "dense",
        vector_output::Vector::Sparse(_) => "sparse",
        vector_output::Vector::MultiDense(_) => "multidense",
    }
}

struct DenseVectorRows {
    name:    String,
    unnamed: bool,
    width:   usize,
    values:  Vec<f32>,
    nulls:   NullBufferBuilder,
}

impl DenseVectorRows {
    fn new(name: String, unnamed: bool, width: usize, capacity: usize) -> Self {
        Self {
            name,
            unnamed,
            width,
            values: Vec::with_capacity(capacity.saturating_mul(width)),
            nulls: NullBufferBuilder::new(capacity),
        }
    }

    fn push(&mut self, vector: Option<vector_output::Vector>) -> DataFusionResult<()> {
        match vector {
            Some(vector_output::Vector::Dense(dense)) => {
                if dense.data.len() != self.width {
                    return exec_err!(
                        "'{}' dense width expected={}, found={}",
                        self.name,
                        self.width,
                        dense.data.len()
                    );
                }
                self.values.extend(dense.data);
                self.nulls.append_non_null();
            }
            Some(other) => {
                return exec_err!(
                    "'{}' expected dense vector, found {}",
                    self.name,
                    vector_kind(&other)
                );
            }
            None => {
                self.values.resize(self.values.len() + self.width, 0.0);
                self.nulls.append_null();
            }
        }
        Ok(())
    }

    fn finish(self) -> DataFusionResult<ArrayRef> {
        let width_i32 = int32_from_usize(&self.name, "dense vector width", self.width)?;
        if !self.values.len().is_multiple_of(self.width) {
            return exec_err!(
                "'{}' packed dense length {} is not divisible by width {}",
                self.name,
                self.values.len(),
                self.width
            );
        }
        let row_count = self.values.len() / self.width;
        let nulls = self.nulls.build();
        validate_null_count(&self.name, row_count, nulls.as_ref())?;

        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let values: ArrayRef = Arc::new(Float32Array::from(self.values));
        Ok(Arc::new(FixedSizeListArray::new(item_field, width_i32, values, nulls)))
    }
}

struct MultiVectorRows {
    name:         String,
    unnamed:      bool,
    width:        usize,
    width_i32:    i32,
    data_offsets: Vec<i32>,
    running_size: i32,
    values:       Vec<f32>,
    shapes:       Vec<i32>,
    nulls:        NullBufferBuilder,
}

impl MultiVectorRows {
    fn new(name: String, unnamed: bool, width: usize, capacity: usize) -> DataFusionResult<Self> {
        Ok(Self {
            width_i32: int32_from_usize(&name, "multivector width", width)?,
            name,
            unnamed,
            width,
            data_offsets: vec![0],
            running_size: 0,
            values: Vec::new(),
            shapes: Vec::with_capacity(capacity.saturating_mul(2)),
            nulls: NullBufferBuilder::new(capacity),
        })
    }

    fn push(&mut self, vector: Option<vector_output::Vector>) -> DataFusionResult<()> {
        match vector {
            Some(vector_output::Vector::MultiDense(multi)) => {
                let row_count = multi.vectors.len();
                let mut row_size = 0_usize;
                for vector in multi.vectors {
                    if vector.data.len() != self.width {
                        return exec_err!(
                            "'{}' multivector row expected={}, found={}",
                            self.name,
                            self.width,
                            vector.data.len()
                        );
                    }
                    row_size += vector.data.len();
                    self.values.extend(vector.data);
                }
                let row_size = int32_from_usize(&self.name, "multivector packed length", row_size)?;
                self.running_size = checked_add_i32(
                    &self.name,
                    "multivector packed length",
                    self.running_size,
                    row_size,
                )?;
                self.data_offsets.push(self.running_size);
                self.shapes.push(int32_from_usize(&self.name, "multivector row count", row_count)?);
                self.shapes.push(self.width_i32);
                self.nulls.append_non_null();
            }
            Some(other) => {
                return exec_err!(
                    "'{}' expected multivector, found {}",
                    self.name,
                    vector_kind(&other)
                );
            }
            None => {
                self.data_offsets.push(self.running_size);
                self.shapes.push(0);
                self.shapes.push(self.width_i32);
                self.nulls.append_null();
            }
        }
        Ok(())
    }

    fn finish(self) -> DataFusionResult<ArrayRef> {
        let row_count = self.data_offsets.len().saturating_sub(1);
        if self.shapes.len() != row_count.saturating_mul(2) {
            return exec_err!(
                "'{}' packed multivector shapes length {} does not match row count {row_count}",
                self.name,
                self.shapes.len()
            );
        }
        let nulls = self.nulls.build();
        validate_null_count(&self.name, row_count, nulls.as_ref())?;

        let data_item_field = Arc::new(Field::new_list_field(DataType::Float32, false));
        let data_values: ArrayRef = Arc::new(Float32Array::from(self.values));
        let data_list: ArrayRef = Arc::new(ListArray::new(
            data_item_field,
            OffsetBuffer::new(ScalarBuffer::from(self.data_offsets)),
            data_values,
            None,
        ));

        let shape_item_field = Arc::new(Field::new("item", DataType::Int32, false));
        let shape_values: ArrayRef = Arc::new(Int32Array::from(self.shapes));
        let shape_array: ArrayRef =
            Arc::new(FixedSizeListArray::new(shape_item_field, 2, shape_values, None));

        let struct_fields = vec![
            Field::new("data", data_list.data_type().clone(), false),
            Field::new("shape", shape_array.data_type().clone(), false),
        ];
        Ok(Arc::new(StructArray::new(struct_fields.into(), vec![data_list, shape_array], nulls)))
    }
}

struct SparseVectorRows {
    name:          String,
    unnamed:       bool,
    shapes:        Vec<i32>,
    row_ptrs:      Vec<i32>,
    row_ptrs_offs: Vec<i32>,
    col_indices:   Vec<u32>,
    col_offs:      Vec<i32>,
    values:        Vec<f32>,
    value_offs:    Vec<i32>,
    nulls:         NullBufferBuilder,
}

impl SparseVectorRows {
    fn new(name: String, unnamed: bool, capacity: usize) -> Self {
        Self {
            name,
            unnamed,
            shapes: Vec::with_capacity(capacity.saturating_mul(2)),
            row_ptrs: Vec::new(),
            row_ptrs_offs: vec![0],
            col_indices: Vec::new(),
            col_offs: vec![0],
            values: Vec::new(),
            value_offs: vec![0],
            nulls: NullBufferBuilder::new(capacity),
        }
    }

    fn push(&mut self, vector: Option<vector_output::Vector>) -> DataFusionResult<()> {
        match vector {
            Some(vector_output::Vector::Sparse(sparse)) => {
                if sparse.indices.len() != sparse.values.len() {
                    return exec_err!(
                        "'{}' sparse indices length {} does not match values length {}",
                        self.name,
                        sparse.indices.len(),
                        sparse.values.len()
                    );
                }
                let nnz = sparse.indices.len();
                let nnz_i32 = int32_from_usize(&self.name, "sparse non-zero length", nnz)?;
                let cols = sparse.indices.iter().copied().max().map_or(Ok(0), |index| {
                    let Some(cols) = index.checked_add(1) else {
                        return exec_err!("'{}' sparse dimension exceeds u32 limits", self.name);
                    };
                    int32_from_usize(&self.name, "sparse dimension", cols as usize)
                })?;

                self.shapes.push(1);
                self.shapes.push(cols);
                self.row_ptrs.extend([0, nnz_i32]);
                self.row_ptrs_offs.push(checked_add_i32(
                    &self.name,
                    "sparse row pointer offsets",
                    *self.row_ptrs_offs.last().expect("offset seed"),
                    2,
                )?);
                self.col_indices.extend(sparse.indices);
                self.col_offs.push(checked_add_i32(
                    &self.name,
                    "sparse column offsets",
                    *self.col_offs.last().expect("offset seed"),
                    nnz_i32,
                )?);
                self.values.extend(sparse.values);
                self.value_offs.push(checked_add_i32(
                    &self.name,
                    "sparse value offsets",
                    *self.value_offs.last().expect("offset seed"),
                    nnz_i32,
                )?);
                self.nulls.append_non_null();
            }
            Some(other) => {
                return exec_err!(
                    "'{}' expected sparse vector, found {}",
                    self.name,
                    vector_kind(&other)
                );
            }
            None => {
                self.shapes.extend([1, 0]);
                self.row_ptrs.extend([0, 0]);
                self.row_ptrs_offs.push(checked_add_i32(
                    &self.name,
                    "sparse row pointer offsets",
                    *self.row_ptrs_offs.last().expect("offset seed"),
                    2,
                )?);
                self.col_offs.push(*self.col_offs.last().expect("offset seed"));
                self.value_offs.push(*self.value_offs.last().expect("offset seed"));
                self.nulls.append_null();
            }
        }
        Ok(())
    }

    fn finish(self) -> DataFusionResult<ArrayRef> {
        let row_count = self.row_ptrs_offs.len().saturating_sub(1);
        if self.shapes.len() != row_count.saturating_mul(2) {
            return exec_err!(
                "'{}' packed sparse shapes length {} does not match row count {row_count}",
                self.name,
                self.shapes.len()
            );
        }
        if self.col_offs.len() != row_count + 1 || self.value_offs.len() != row_count + 1 {
            return exec_err!("'{}' sparse offsets do not match row count {row_count}", self.name);
        }
        let nulls = self.nulls.build();
        validate_null_count(&self.name, row_count, nulls.as_ref())?;

        let shape_item_field = Arc::new(Field::new("item", DataType::Int32, false));
        let shape_values: ArrayRef = Arc::new(Int32Array::from(self.shapes));
        let shape_array: ArrayRef =
            Arc::new(FixedSizeListArray::new(shape_item_field, 2, shape_values, None));

        let row_ptr_item_field = Arc::new(Field::new_list_field(DataType::Int32, false));
        let row_ptr_values: ArrayRef = Arc::new(Int32Array::from(self.row_ptrs));
        let row_ptr_array: ArrayRef = Arc::new(ListArray::new(
            row_ptr_item_field,
            OffsetBuffer::new(ScalarBuffer::from(self.row_ptrs_offs)),
            row_ptr_values,
            None,
        ));

        let col_item_field = Arc::new(Field::new_list_field(DataType::UInt32, false));
        let col_values: ArrayRef = Arc::new(UInt32Array::from(self.col_indices));
        let col_array: ArrayRef = Arc::new(ListArray::new(
            col_item_field,
            OffsetBuffer::new(ScalarBuffer::from(self.col_offs)),
            col_values,
            None,
        ));

        let value_item_field = Arc::new(Field::new_list_field(DataType::Float32, false));
        let value_values: ArrayRef = Arc::new(Float32Array::from(self.values));
        let value_array: ArrayRef = Arc::new(ListArray::new(
            value_item_field,
            OffsetBuffer::new(ScalarBuffer::from(self.value_offs)),
            value_values,
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
            nulls,
        )))
    }
}

enum FieldAppender {
    Id(StringBuilder),
    Payload(StringBuilder),
    DenseVector(DenseVectorRows),
    MultiVector(MultiVectorRows),
    SparseVector(SparseVectorRows),
}

pub struct QdrantRecordBatchBuilder {
    schema:          SchemaRef,
    field_appenders: Vec<FieldAppender>,
}

impl QdrantRecordBatchBuilder {
    /// Create a schema-driven record-batch builder for Qdrant scan output.
    ///
    /// # Errors
    /// Returns an error if the projected schema contains unsupported field contracts.
    pub fn new(schema: SchemaRef, point_count: usize) -> DataFusionResult<Self> {
        let field_appenders = schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() == ID_FIELD_NAME {
                    Ok(FieldAppender::Id(StringBuilder::with_capacity(
                        point_count,
                        point_count * 16,
                    )))
                } else if field.name() == PAYLOAD_FIELD_NAME {
                    Ok(FieldAppender::Payload(StringBuilder::with_capacity(
                        point_count,
                        point_count * 64,
                    )))
                } else if let Some(width) = dense_vector_width(field) {
                    Ok(FieldAppender::DenseVector(DenseVectorRows::new(
                        field.name().clone(),
                        field.name() == UNNAMED_VECTOR_FIELD_NAME,
                        width,
                        point_count,
                    )))
                } else if is_multi_vector_field(field) {
                    let Some(width) = multivector_width(field) else {
                        return exec_err!(
                            "field '{}' is missing multivector width metadata",
                            field.name()
                        );
                    };
                    Ok(FieldAppender::MultiVector(MultiVectorRows::new(
                        field.name().clone(),
                        field.name() == UNNAMED_VECTOR_FIELD_NAME,
                        width,
                        point_count,
                    )?))
                } else if is_sparse_vector_field(field) {
                    Ok(FieldAppender::SparseVector(SparseVectorRows::new(
                        field.name().clone(),
                        field.name() == UNNAMED_VECTOR_FIELD_NAME,
                        point_count,
                    )))
                } else {
                    exec_err!(
                        "unsupported scan field contract for '{}' with data type {}",
                        field.name(),
                        field.data_type()
                    )
                }
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

        Ok(Self { schema, field_appenders })
    }

    /// Append a single Qdrant point to the in-progress batch.
    ///
    /// # Errors
    /// Returns an error if the point does not match the admitted scan schema contract.
    pub fn append_point(&mut self, point: ScoredPoint) -> DataFusionResult<()> {
        let ScoredPoint { id, payload, vectors, .. } = point;
        self.append_parts(id, &payload, vectors)
    }

    /// Append a single retrieved Qdrant point to the in-progress batch.
    ///
    /// # Errors
    /// Returns an error if the point does not match the admitted scan schema contract.
    pub fn append_retrieved_point(&mut self, point: RetrievedPoint) -> DataFusionResult<()> {
        let RetrievedPoint { id, payload, vectors, .. } = point;
        self.append_parts(id, &payload, vectors)
    }

    fn append_parts(
        &mut self,
        id: Option<PointId>,
        payload: &HashMap<String, Value>,
        vectors: Option<VectorsOutput>,
    ) -> DataFusionResult<()> {
        let point_id = id.and_then(|id| id.point_id_options);
        let (mut unnamed_vector, mut named_vectors) =
            match vectors.and_then(|vectors| vectors.vectors_options) {
                Some(vectors_output::VectorsOptions::Vector(vector)) => (Some(vector), None),
                Some(vectors_output::VectorsOptions::Vectors(named_vectors)) => {
                    (None, Some(named_vectors.vectors))
                }
                None => (None, None),
            };

        for appender in &mut self.field_appenders {
            match appender {
                FieldAppender::Id(builder) => match point_id.as_ref() {
                    Some(point_id::PointIdOptions::Num(number)) => {
                        builder.append_value(number.to_string());
                    }
                    Some(point_id::PointIdOptions::Uuid(uuid)) => builder.append_value(uuid),
                    None => return exec_err!("Qdrant returned a point without an id"),
                },
                FieldAppender::Payload(builder) => builder.append_value(
                    serde_json::to_string(payload)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?,
                ),
                FieldAppender::DenseVector(rows) => {
                    rows.push(take_vector(
                        rows.unnamed,
                        &rows.name,
                        &mut unnamed_vector,
                        &mut named_vectors,
                    )?)?;
                }
                FieldAppender::MultiVector(rows) => {
                    rows.push(take_vector(
                        rows.unnamed,
                        &rows.name,
                        &mut unnamed_vector,
                        &mut named_vectors,
                    )?)?;
                }
                FieldAppender::SparseVector(rows) => {
                    rows.push(take_vector(
                        rows.unnamed,
                        &rows.name,
                        &mut unnamed_vector,
                        &mut named_vectors,
                    )?)?;
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
            .field_appenders
            .into_iter()
            .map(|appender| match appender {
                FieldAppender::Id(mut builder) | FieldAppender::Payload(mut builder) => {
                    Ok(Arc::new(builder.finish()) as ArrayRef)
                }
                FieldAppender::DenseVector(rows) => rows.finish(),
                FieldAppender::MultiVector(rows) => rows.finish(),
                FieldAppender::SparseVector(rows) => rows.finish(),
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

        RecordBatch::try_new(self.schema, arrays)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
    }
}

fn int32_from_usize(name: &str, context: &str, value: usize) -> DataFusionResult<i32> {
    let Ok(value) = i32::try_from(value) else {
        return exec_err!("'{name}' {context} exceeds Arrow i32 limits: {value}");
    };
    Ok(value)
}

fn checked_add_i32(name: &str, context: &str, left: i32, right: i32) -> DataFusionResult<i32> {
    let Some(sum) = left.checked_add(right) else {
        return exec_err!("'{name}' {context} exceeds Arrow i32 limits");
    };
    Ok(sum)
}

fn take_vector(
    unnamed: bool,
    name: &str,
    unnamed_vector: &mut Option<VectorOutput>,
    named_vectors: &mut Option<HashMap<String, VectorOutput>>,
) -> DataFusionResult<Option<vector_output::Vector>> {
    let vector_output = if unnamed {
        unnamed_vector.take()
    } else {
        named_vectors.as_mut().and_then(|vectors| vectors.remove(name))
    };

    match vector_output {
        Some(vector_output) => {
            let Some(vector) = vector_output.vector else {
                return exec_err!("Qdrant returned a vector output without a typed vector body");
            };
            Ok(Some(vector))
        }
        None => Ok(None),
    }
}

fn validate_null_count(
    name: &str,
    row_count: usize,
    nulls: Option<&NullBuffer>,
) -> DataFusionResult<()> {
    if let Some(nulls) = nulls
        && nulls.len() != row_count
    {
        return exec_err!(
            "'{name}' validity length {} does not match row count {row_count}",
            nulls.len()
        );
    }
    Ok(())
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
    use datafusion::arrow::datatypes::Schema;
    use ndarrow::{
        CsrMatrixBatchExtension, csr_matrix_batch_iter, fixed_size_list_as_array2,
        fixed_size_list_as_array2_masked, variable_shape_tensor_iter,
    };
    use qdrant_client::qdrant::{DenseVector, MultiDenseVector, SparseVector, vector_output};

    use super::*;

    fn assert_f32_eq(left: f32, right: f32) {
        assert!((left - right).abs() < 1.0e-6, "left={left}, right={right}");
    }

    fn multivector_test_field(array: &StructArray, width: i32, nullable: bool) -> Field {
        let extension = VariableShapeTensor::try_new(
            DataType::Float32,
            2,
            None,
            None,
            Some(vec![None, Some(width)]),
        )
        .expect("variable tensor extension");
        extension.supports_data_type(array.data_type()).expect("compatible multivector storage");

        Field::new("multi", array.data_type().clone(), nullable).with_metadata(HashMap::from([
            (EXTENSION_TYPE_NAME_KEY.to_owned(), VariableShapeTensor::NAME.to_owned()),
            (
                EXTENSION_TYPE_METADATA_KEY.to_owned(),
                serde_json::json!({ "uniform_shape": [serde_json::Value::Null, width] })
                    .to_string(),
            ),
        ]))
    }

    #[test]
    fn append_retrieved_point_uses_current_qdrant_vector_shape() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, false),
            Field::new(
                UNNAMED_VECTOR_FIELD_NAME,
                DataType::new_fixed_size_list(DataType::Float32, 3, false),
                true,
            ),
        ]));
        let mut builder = QdrantRecordBatchBuilder::new(Arc::clone(&schema), 1).expect("builder");

        builder
            .append_retrieved_point(RetrievedPoint {
                id:          Some(1_u64.into()),
                payload:     HashMap::new(),
                vectors:     Some(VectorsOutput {
                    vectors_options: Some(vectors_output::VectorsOptions::Vector(VectorOutput {
                        vector:        Some(vector_output::Vector::Dense(DenseVector {
                            data: vec![1.0, 2.0, 3.0],
                        })),
                        data:          vec![],
                        indices:       None,
                        vectors_count: None,
                    })),
                }),
                shard_key:   None,
                order_value: None,
            })
            .expect("append point");

        let batch = builder.finish().expect("batch");
        let array =
            batch.column(2).as_any().downcast_ref::<FixedSizeListArray>().expect("fixed-size list");
        let view = fixed_size_list_as_array2::<Float32Type>(array).expect("ndarray view");
        assert_eq!(view.shape(), &[1, 3]);
        assert_f32_eq(view[[0, 2]], 3.0);
    }

    #[test]
    fn append_retrieved_point_rejects_missing_typed_body() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, false),
            Field::new(
                UNNAMED_VECTOR_FIELD_NAME,
                DataType::new_fixed_size_list(DataType::Float32, 3, false),
                true,
            ),
        ]));
        let mut builder = QdrantRecordBatchBuilder::new(schema, 1).expect("builder");

        let result = builder.append_retrieved_point(RetrievedPoint {
            id:          Some(1_u64.into()),
            payload:     HashMap::new(),
            vectors:     Some(VectorsOutput {
                vectors_options: Some(vectors_output::VectorsOptions::Vector(VectorOutput {
                    vector:        None,
                    data:          vec![],
                    indices:       None,
                    vectors_count: None,
                })),
            }),
            shard_key:   None,
            order_value: None,
        });

        assert!(result.is_err());
    }

    #[test]
    fn dense_vector_arrays_round_trip_into_ndarrow_views() {
        let mut rows = DenseVectorRows::new("embedding".to_string(), false, 3, 2);
        rows.push(Some(vector_output::Vector::Dense(DenseVector { data: vec![1.0, 2.0, 3.0] })))
            .expect("row 0");
        rows.push(Some(vector_output::Vector::Dense(DenseVector { data: vec![4.0, 5.0, 6.0] })))
            .expect("row 1");
        let array = rows.finish().expect("dense array");
        let array = array.as_any().downcast_ref::<FixedSizeListArray>().expect("fixed-size list");

        let view = fixed_size_list_as_array2::<Float32Type>(array).expect("ndarray view");
        assert_eq!(view.shape(), &[2, 3]);
        assert_f32_eq(view[[0, 0]], 1.0);
        assert_f32_eq(view[[1, 2]], 6.0);
    }

    #[test]
    fn dense_vector_arrays_preserve_outer_nulls() {
        let mut rows = DenseVectorRows::new("embedding".to_string(), false, 3, 3);
        rows.push(Some(vector_output::Vector::Dense(DenseVector { data: vec![1.0, 2.0, 3.0] })))
            .expect("row 0");
        rows.push(None).expect("row 1");
        rows.push(Some(vector_output::Vector::Dense(DenseVector { data: vec![4.0, 5.0, 6.0] })))
            .expect("row 2");
        let array = rows.finish().expect("dense array");
        let array = array.as_any().downcast_ref::<FixedSizeListArray>().expect("fixed-size list");

        let (view, mask) =
            fixed_size_list_as_array2_masked::<Float32Type>(array).expect("masked view");
        let mask = mask.expect("outer null mask");
        assert_eq!(array.null_count(), 1);
        assert_eq!(view.shape(), &[3, 3]);
        assert!(mask.is_valid(0));
        assert!(!mask.is_valid(1));
        assert!(mask.is_valid(2));
        assert_f32_eq(view[[0, 0]], 1.0);
        assert_f32_eq(view[[2, 2]], 6.0);
    }

    #[test]
    fn multivector_arrays_round_trip_into_ndarrow_views() {
        let mut rows =
            MultiVectorRows::new("multi".to_string(), false, 2, 2).expect("multivector rows");
        rows.push(Some(vector_output::Vector::MultiDense(MultiDenseVector {
            vectors: vec![DenseVector { data: vec![1.0, 2.0] }, DenseVector {
                data: vec![3.0, 4.0],
            }],
        })))
        .expect("row 0");
        rows.push(Some(vector_output::Vector::MultiDense(MultiDenseVector {
            vectors: vec![DenseVector { data: vec![5.0, 6.0] }],
        })))
        .expect("row 1");
        let array = rows.finish().expect("multivector array");
        let array = array.as_any().downcast_ref::<StructArray>().expect("struct");
        let field = multivector_test_field(array, 2, true);

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
    fn multivector_arrays_preserve_outer_nulls() {
        let mut rows =
            MultiVectorRows::new("multi".to_string(), false, 2, 3).expect("multivector rows");
        rows.push(Some(vector_output::Vector::MultiDense(MultiDenseVector {
            vectors: vec![DenseVector { data: vec![1.0, 2.0] }, DenseVector {
                data: vec![3.0, 4.0],
            }],
        })))
        .expect("row 0");
        rows.push(None).expect("row 1");
        rows.push(Some(vector_output::Vector::MultiDense(MultiDenseVector {
            vectors: vec![DenseVector { data: vec![5.0, 6.0] }],
        })))
        .expect("row 2");
        let array = rows.finish().expect("multivector array");
        let array = array.as_any().downcast_ref::<StructArray>().expect("struct");

        assert_eq!(array.null_count(), 1);
        assert!(array.is_valid(0));
        assert!(array.is_null(1));
        assert!(array.is_valid(2));
    }

    #[test]
    fn sparse_vector_arrays_round_trip_into_ndarrow_views() {
        let mut rows = SparseVectorRows::new("keywords".to_string(), false, 2);
        rows.push(Some(vector_output::Vector::Sparse(SparseVector {
            indices: vec![0, 5],
            values:  vec![0.1, 0.9],
        })))
        .expect("row 0");
        rows.push(Some(vector_output::Vector::Sparse(SparseVector {
            indices: vec![1, 3, 4],
            values:  vec![0.2, 0.3, 0.4],
        })))
        .expect("row 1");
        let array = rows.finish().expect("sparse array");
        let array = array.as_any().downcast_ref::<StructArray>().expect("struct");
        let mut field = Field::new("keywords", array.data_type().clone(), true);
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

    #[test]
    fn sparse_vector_arrays_preserve_outer_nulls() {
        let mut rows = SparseVectorRows::new("keywords".to_string(), false, 3);
        rows.push(Some(vector_output::Vector::Sparse(SparseVector {
            indices: vec![0, 5],
            values:  vec![0.1, 0.9],
        })))
        .expect("row 0");
        rows.push(None).expect("row 1");
        rows.push(Some(vector_output::Vector::Sparse(SparseVector {
            indices: vec![1, 3, 4],
            values:  vec![0.2, 0.3, 0.4],
        })))
        .expect("row 2");
        let array = rows.finish().expect("sparse array");
        let array = array.as_any().downcast_ref::<StructArray>().expect("struct");

        assert_eq!(array.null_count(), 1);
        assert!(array.is_valid(0));
        assert!(array.is_null(1));
        assert!(array.is_valid(2));
    }
}
