use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, Int32Array, LargeListArray,
    LargeStringArray, ListArray, StringArray, StructArray, UInt32Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::exec_err;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use qdrant_client::Payload;
use qdrant_client::qdrant::vectors::VectorsOptions;
use qdrant_client::qdrant::{NamedVectors, PointId, PointStruct, Vector, Vectors};

use super::schema::{
    ID_FIELD_NAME, PAYLOAD_FIELD_NAME, QdrantFieldBinding, UNNAMED_VECTOR_FIELD_NAME,
    schema_uses_unnamed_vector_contract,
};

pub(crate) fn record_batch_to_points(
    batch: &RecordBatch,
    target_schema: &datafusion::arrow::datatypes::SchemaRef,
) -> DataFusionResult<Vec<PointStruct>> {
    let schema = batch.schema();
    let id_index = schema.index_of(ID_FIELD_NAME).map_err(|_| {
        DataFusionError::Execution(format!(
            "write batch is missing required '{ID_FIELD_NAME}' column"
        ))
    })?;
    let payload_index = schema.index_of(PAYLOAD_FIELD_NAME).ok();
    let vector_fields = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name() != ID_FIELD_NAME && field.name() != PAYLOAD_FIELD_NAME)
        .map(|(index, field)| {
            let binding = QdrantFieldBinding::from_field(field);
            Ok((index, field.name().clone(), binding))
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    if vector_fields.is_empty() {
        return exec_err!("write batch schema does not contain any qdrant vector columns");
    }

    let unnamed_only = vector_fields.len() == 1
        && vector_fields[0].1 == UNNAMED_VECTOR_FIELD_NAME
        && schema_uses_unnamed_vector_contract(target_schema.as_ref());
    let mut points = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let id = point_id_at(batch.column(id_index), row)?;
        let payload = payload_index
            .map(|index| payload_at(batch.column(index), row))
            .transpose()?
            .unwrap_or_else(Payload::new);

        let vectors = if unnamed_only {
            let (index, name, binding) = &vector_fields[0];
            vector_at(batch.column(*index), row, name, binding)?
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "row {row} is missing required unnamed vector column '{name}'"
                    ))
                })?
                .into()
        } else {
            let mut named_vectors = NamedVectors::default();
            let mut present = false;
            for (index, name, binding) in &vector_fields {
                if let Some(vector) = vector_at(batch.column(*index), row, name, binding)? {
                    named_vectors = named_vectors.add_vector(name.clone(), vector);
                    present = true;
                }
            }
            if !present {
                return exec_err!("row {row} does not contain any named vector values");
            }
            Vectors { vectors_options: Some(VectorsOptions::Vectors(named_vectors)) }
        };

        points.push(PointStruct::new(id, vectors, payload));
    }
    Ok(points)
}

fn point_id_at(array: &ArrayRef, row: usize) -> DataFusionResult<PointId> {
    let value = string_at(array, row, ID_FIELD_NAME)?.ok_or_else(|| {
        DataFusionError::Execution(format!("row {row} is missing required '{ID_FIELD_NAME}' value"))
    })?;
    Ok(value.parse::<u64>().map_or_else(|_| PointId::from(value), PointId::from))
}

fn payload_at(array: &ArrayRef, row: usize) -> DataFusionResult<Payload> {
    let Some(value) = string_at(array, row, PAYLOAD_FIELD_NAME)? else {
        return Ok(Payload::new());
    };
    let json = serde_json::from_str::<serde_json::Value>(&value).map_err(|error| {
        DataFusionError::Execution(format!(
            "row {row} payload column contains invalid JSON object: {error}"
        ))
    })?;
    Payload::try_from(json).map_err(|error| DataFusionError::External(Box::new(error)))
}

fn string_at(array: &ArrayRef, row: usize, name: &str) -> DataFusionResult<Option<String>> {
    match array.data_type() {
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("column '{name}' expected Utf8 array"))
            })?;
            Ok((!array.is_null(row)).then(|| array.value(row).to_owned()))
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("column '{name}' expected LargeUtf8 array"))
            })?;
            Ok((!array.is_null(row)).then(|| array.value(row).to_owned()))
        }
        other => exec_err!("column '{name}' expected string array, found {other}"),
    }
}

fn vector_at(
    array: &ArrayRef,
    row: usize,
    name: &str,
    binding: &QdrantFieldBinding,
) -> DataFusionResult<Option<Vector>> {
    match binding {
        QdrantFieldBinding::DenseFixed { width } => dense_fixed_vector_at(array, row, name, *width),
        QdrantFieldBinding::DenseVariable => dense_variable_vector_at(array, row, name),
        QdrantFieldBinding::MultiDense { width } => multi_dense_vector_at(array, row, name, *width),
        QdrantFieldBinding::Sparse => sparse_vector_at(array, row, name),
        QdrantFieldBinding::Document
        | QdrantFieldBinding::Image
        | QdrantFieldBinding::Object
        | QdrantFieldBinding::Unsupported(_) => {
            exec_err!("write column '{name}' has unsupported qdrant write binding {binding:?}")
        }
    }
}

fn dense_fixed_vector_at(
    array: &ArrayRef,
    row: usize,
    name: &str,
    width: usize,
) -> DataFusionResult<Option<Vector>> {
    let array = array.as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' expected FixedSizeList array"))
    })?;
    if array.is_null(row) {
        return Ok(None);
    }
    let values = vector_values_to_f32(&array.value(row), name)?;
    if values.len() != width {
        return exec_err!(
            "column '{name}' expected dense vector width {width}, found {}",
            values.len()
        );
    }
    Ok(Some(Vector::new_dense(values)))
}

fn dense_variable_vector_at(
    array: &ArrayRef,
    row: usize,
    name: &str,
) -> DataFusionResult<Option<Vector>> {
    let values = match array.data_type() {
        DataType::List(_) => {
            let array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("column '{name}' expected List array"))
            })?;
            if array.is_null(row) {
                return Ok(None);
            }
            vector_values_to_f32(&array.value(row), name)?
        }
        DataType::LargeList(_) => {
            let array = array.as_any().downcast_ref::<LargeListArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("column '{name}' expected LargeList array"))
            })?;
            if array.is_null(row) {
                return Ok(None);
            }
            vector_values_to_f32(&array.value(row), name)?
        }
        other => return exec_err!("column '{name}' expected list array, found {other}"),
    };
    Ok(Some(Vector::new_dense(values)))
}

fn multi_dense_vector_at(
    array: &ArrayRef,
    row: usize,
    name: &str,
    width: Option<usize>,
) -> DataFusionResult<Option<Vector>> {
    let array = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' expected Struct array"))
    })?;
    if array.is_null(row) {
        return Ok(None);
    }

    let data = array.column_by_name("data").ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' is missing multivector data field"))
    })?;
    let shapes = array.column_by_name("shape").ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' is missing multivector shape field"))
    })?;

    let packed = match data.data_type() {
        DataType::List(_) => {
            let data = data.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("column '{name}' multivector data must be List"))
            })?;
            vector_values_to_f32(&data.value(row), name)?
        }
        DataType::LargeList(_) => {
            let data = data.as_any().downcast_ref::<LargeListArray>().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "column '{name}' multivector data must be LargeList"
                ))
            })?;
            vector_values_to_f32(&data.value(row), name)?
        }
        other => return exec_err!("column '{name}' multivector data has unsupported type {other}"),
    };

    let shapes = shapes.as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "column '{name}' multivector shape must be FixedSizeList"
        ))
    })?;
    let shape_values = shapes.value(row);
    let shape_values = shape_values.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' multivector shape must contain Int32"))
    })?;
    if shape_values.len() != 2 {
        return exec_err!("column '{name}' multivector shape must contain exactly 2 entries");
    }
    let row_count = usize::try_from(shape_values.value(0)).map_err(|_| {
        DataFusionError::Execution(format!(
            "column '{name}' multivector row count must be non-negative"
        ))
    })?;
    let row_width = usize::try_from(shape_values.value(1)).map_err(|_| {
        DataFusionError::Execution(format!(
            "column '{name}' multivector width must be non-negative"
        ))
    })?;
    if let Some(expected_width) = width
        && row_width != expected_width
    {
        return exec_err!(
            "column '{name}' expected multivector width {expected_width}, found {row_width}"
        );
    }
    if row_width == 0 {
        return Ok(Some(Vector::new_multi(Vec::<Vec<f32>>::new())));
    }
    if packed.len() != row_count.saturating_mul(row_width) {
        return exec_err!(
            "column '{name}' packed multivector length {} does not match shape [{row_count}, \
             {row_width}]",
            packed.len()
        );
    }
    Ok(Some(Vector::new_multi(packed.chunks(row_width).map(<[f32]>::to_vec).collect::<Vec<_>>())))
}

fn sparse_vector_at(array: &ArrayRef, row: usize, name: &str) -> DataFusionResult<Option<Vector>> {
    let array = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' expected Struct array"))
    })?;
    if array.is_null(row) {
        return Ok(None);
    }

    let shape = array.column_by_name("shape").ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' is missing sparse shape field"))
    })?;
    let row_ptrs = array.column_by_name("row_ptrs").ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' is missing sparse row_ptrs field"))
    })?;
    let col_indices = array.column_by_name("col_indices").ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' is missing sparse col_indices field"))
    })?;
    let values = array.column_by_name("values").ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' is missing sparse values field"))
    })?;

    let shape = shape.as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' sparse shape must be FixedSizeList"))
    })?;
    let shape_values = shape.value(row);
    let shape_values = shape_values.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' sparse shape must contain Int32"))
    })?;
    if shape_values.len() != 2 {
        return exec_err!("column '{name}' sparse shape must contain exactly 2 entries");
    }
    let row_count = usize::try_from(shape_values.value(0)).map_err(|_| {
        DataFusionError::Execution(format!("column '{name}' sparse row count must be non-negative"))
    })?;
    if row_count != 1 {
        return exec_err!(
            "column '{name}' sparse carrier must contain exactly one row per point, found \
             {row_count}"
        );
    }

    let row_ptrs = row_ptrs.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' sparse row_ptrs must be List"))
    })?;
    let row_ptrs = row_ptrs.value(row);
    let row_ptrs = row_ptrs.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' sparse row_ptrs must contain Int32"))
    })?;
    if row_ptrs.len() != 2 || row_ptrs.value(0) != 0 {
        return exec_err!("column '{name}' sparse row_ptrs must be [0, nnz]");
    }
    let nnz = usize::try_from(row_ptrs.value(1)).map_err(|_| {
        DataFusionError::Execution(format!("column '{name}' sparse nnz must be non-negative"))
    })?;

    let col_indices = col_indices.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' sparse col_indices must be List"))
    })?;
    let col_indices = col_indices.value(row);
    let col_indices = col_indices.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "column '{name}' sparse col_indices must contain UInt32"
        ))
    })?;
    let indices = (0..col_indices.len()).map(|index| col_indices.value(index)).collect::<Vec<_>>();

    let values = values.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Execution(format!("column '{name}' sparse values must be List"))
    })?;
    let values = values.value(row);
    let values = vector_values_to_f32(&values, name)?;

    if indices.len() != nnz || values.len() != nnz {
        return exec_err!(
            "column '{name}' sparse nnz {nnz} does not match indices {} / values {}",
            indices.len(),
            values.len()
        );
    }

    Ok(Some(Vector::new_sparse(indices, values)))
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "qdrant vectors are f32 and provider write serialization targets the canonical \
              carrier"
)]
fn vector_values_to_f32(array: &ArrayRef, name: &str) -> DataFusionResult<Vec<f32>> {
    match array.data_type() {
        DataType::Float32 => {
            let array = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Execution(format!("column '{name}' expected Float32 array"))
            })?;
            if array.null_count() != 0 {
                return exec_err!("column '{name}' vector values must not contain nulls");
            }
            Ok((0..array.len()).map(|index| array.value(index)).collect())
        }
        DataType::Float64 => {
            let array = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Execution(format!("column '{name}' expected Float64 array"))
            })?;
            if array.null_count() != 0 {
                return exec_err!("column '{name}' vector values must not contain nulls");
            }
            Ok((0..array.len()).map(|index| array.value(index) as f32).collect())
        }
        other => exec_err!("column '{name}' vector values must be Float32/Float64, found {other}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::extension::{
        EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType, VariableShapeTensor,
    };
    use datafusion::arrow::array::{
        FixedSizeListArray, Float32Array, Int32Array, ListArray, RecordBatch, StringArray,
        StructArray, UInt32Array,
    };
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use ndarrow::CsrMatrixBatchExtension;
    use qdrant_client::qdrant::Value;
    use qdrant_client::qdrant::point_id::PointIdOptions;

    use super::*;

    fn id_array(values: &[&str]) -> ArrayRef { Arc::new(StringArray::from(values.to_vec())) }

    fn payload_array(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn fixed_dense_array(vectors: &[Option<Vec<f32>>], width: i32) -> ArrayRef {
        let values = vectors
            .iter()
            .flat_map(|vector| {
                vector.as_ref().map(|vector| vector.iter().copied()).into_iter().flatten()
            })
            .collect::<Vec<_>>();
        let nulls = vectors.iter().map(Option::is_some).collect::<Vec<_>>();
        Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            width,
            Arc::new(Float32Array::from(values)),
            Some(nulls.into()),
        ))
    }

    fn fixed_size_list_field(name: &str, width: i32) -> Field {
        Field::new(name, DataType::new_fixed_size_list(DataType::Float32, width, false), true)
    }

    fn multivector_field(name: &str, width: i32) -> Field {
        let value_type = DataType::Float32;
        let storage = DataType::Struct(
            vec![
                Field::new("data", DataType::new_list(value_type.clone(), false), false),
                Field::new(
                    "shape",
                    DataType::new_fixed_size_list(DataType::Int32, 2, false),
                    false,
                ),
            ]
            .into(),
        );
        drop(
            VariableShapeTensor::try_new(value_type, 2, None, None, Some(vec![None, Some(width)]))
                .expect("tensor extension"),
        );
        let mut field = Field::new(name, storage, true);
        drop(
            field
                .metadata_mut()
                .insert(EXTENSION_TYPE_NAME_KEY.to_owned(), VariableShapeTensor::NAME.to_owned()),
        );
        drop(field.metadata_mut().insert(
            EXTENSION_TYPE_METADATA_KEY.to_owned(),
            serde_json::json!({"uniform_shape": [serde_json::Value::Null, width]}).to_string(),
        ));
        drop(
            field
                .try_extension_type::<VariableShapeTensor>()
                .expect("multivector extension attached"),
        );
        field
    }

    fn multivector_array(vectors: &[Option<Vec<Vec<f32>>>], width: i32) -> ArrayRef {
        let mut data_offsets = vec![0_i32];
        let mut packed = Vec::new();
        let mut shapes = Vec::new();
        let mut running = 0_i32;
        for vector in vectors {
            let rows = vector.as_ref().map_or(0, Vec::len);
            let flat = vector
                .as_ref()
                .map(|rows| rows.iter().flat_map(|row| row.iter().copied()).collect::<Vec<_>>())
                .unwrap_or_default();
            running += i32::try_from(flat.len()).expect("flat len fits i32");
            data_offsets.push(running);
            shapes.push(i32::try_from(rows).expect("rows fit i32"));
            shapes.push(width);
            packed.extend(flat);
        }
        let data = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float32, false)),
            OffsetBuffer::new(ScalarBuffer::from(data_offsets)),
            Arc::new(Float32Array::from(packed)),
            None,
        );
        let shape = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            2,
            Arc::new(Int32Array::from(shapes)),
            None,
        );
        Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("data", data.data_type().clone(), false),
                Field::new("shape", shape.data_type().clone(), false),
            ]),
            vec![Arc::new(data), Arc::new(shape)],
            Some(vectors.iter().map(Option::is_some).collect()),
        ))
    }

    fn sparse_field(name: &str) -> Field {
        let data_type = DataType::Struct(
            vec![
                Field::new(
                    "shape",
                    DataType::new_fixed_size_list(DataType::Int32, 2, false),
                    false,
                ),
                Field::new("row_ptrs", DataType::new_list(DataType::Int32, false), false),
                Field::new("col_indices", DataType::new_list(DataType::UInt32, false), false),
                Field::new("values", DataType::new_list(DataType::Float32, false), false),
            ]
            .into(),
        );
        let extension = CsrMatrixBatchExtension::try_new(&data_type, ()).expect("csr extension");
        let mut field = Field::new(name, data_type, true);
        field.try_with_extension_type(extension).expect("sparse extension attached");
        field
    }

    fn sparse_array(vectors: &[Option<(Vec<u32>, Vec<f32>)>]) -> ArrayRef {
        let shape = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            2,
            Arc::new(Int32Array::from(
                vectors
                    .iter()
                    .flat_map(|vector| {
                        vector.as_ref().map_or_else(
                            || vec![0_i32, 0_i32],
                            |(indices, _)| {
                                vec![
                                    1_i32,
                                    i32::try_from(indices.iter().copied().max().unwrap_or(0) + 1)
                                        .expect("dim fits"),
                                ]
                            },
                        )
                    })
                    .collect::<Vec<_>>(),
            )),
            None,
        );
        let row_ptrs_offsets = OffsetBuffer::new(ScalarBuffer::from(
            std::iter::once(0_i32)
                .chain(vectors.iter().scan(0_i32, |acc, vector| {
                    *acc += if vector.is_some() { 2 } else { 0 };
                    Some(*acc)
                }))
                .collect::<Vec<_>>(),
        ));
        let row_ptrs_values = Int32Array::from(
            vectors
                .iter()
                .flat_map(|vector| {
                    vector.as_ref().map_or_else(Vec::new, |(indices, _)| {
                        vec![0_i32, i32::try_from(indices.len()).expect("nnz fits")]
                    })
                })
                .collect::<Vec<_>>(),
        );
        let row_ptrs = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, false)),
            row_ptrs_offsets,
            Arc::new(row_ptrs_values),
            None,
        );
        let index_offsets = OffsetBuffer::new(ScalarBuffer::from(
            std::iter::once(0_i32)
                .chain(vectors.iter().scan(0_i32, |acc, vector| {
                    *acc += i32::try_from(vector.as_ref().map_or(0, |(indices, _)| indices.len()))
                        .expect("nnz fits");
                    Some(*acc)
                }))
                .collect::<Vec<_>>(),
        ));
        let indices = ListArray::new(
            Arc::new(Field::new_list_field(DataType::UInt32, false)),
            index_offsets.clone(),
            Arc::new(UInt32Array::from(
                vectors
                    .iter()
                    .flat_map(|vector| {
                        vector.as_ref().map_or_else(Vec::new, |(indices, _)| indices.clone())
                    })
                    .collect::<Vec<_>>(),
            )),
            None,
        );
        let values = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float32, false)),
            index_offsets,
            Arc::new(Float32Array::from(
                vectors
                    .iter()
                    .flat_map(|vector| {
                        vector.as_ref().map_or_else(Vec::new, |(_, values)| values.clone())
                    })
                    .collect::<Vec<_>>(),
            )),
            None,
        );
        Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("shape", shape.data_type().clone(), false),
                Field::new("row_ptrs", row_ptrs.data_type().clone(), false),
                Field::new("col_indices", indices.data_type().clone(), false),
                Field::new("values", values.data_type().clone(), false),
            ]),
            vec![Arc::new(shape), Arc::new(row_ptrs), Arc::new(indices), Arc::new(values)],
            Some(vectors.iter().map(Option::is_some).collect()),
        ))
    }

    #[test]
    fn serializes_dense_points_with_payload() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            fixed_size_list_field(UNNAMED_VECTOR_FIELD_NAME, 2),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
            id_array(&["1", "abc"]),
            payload_array(&[Some(r#"{"rank":10}"#), None]),
            fixed_dense_array(&[Some(vec![1.0, 0.0]), Some(vec![0.0, 1.0])], 2),
        ])
        .expect("batch");

        let points = record_batch_to_points(&batch, &schema).expect("points");
        assert_eq!(points.len(), 2);
        assert_eq!(
            points[0].id.as_ref().and_then(|id| id.point_id_options.as_ref()),
            Some(&PointIdOptions::Num(1))
        );
        assert_eq!(
            points[1].id.as_ref().and_then(|id| id.point_id_options.as_ref()),
            Some(&PointIdOptions::Uuid("abc".to_owned()))
        );
        assert_eq!(points[0].payload.get("rank").and_then(Value::as_integer), Some(10));
        assert!(points[1].payload.is_empty());
    }

    #[test]
    fn serializes_named_multivector_and_sparse_points() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            multivector_field("multi", 2),
            sparse_field("keywords"),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
            id_array(&["1", "2"]),
            payload_array(&[None, None]),
            multivector_array(&[Some(vec![vec![1.0, 2.0], vec![3.0, 4.0]]), None], 2),
            sparse_array(&[Some((vec![0, 2], vec![0.5, 1.5])), Some((vec![1], vec![0.25]))]),
        ])
        .expect("batch");

        let points = record_batch_to_points(&batch, &schema).expect("points");
        assert_eq!(points.len(), 2);
        let first_vectors = points[0].vectors.as_ref().expect("first vectors");
        let second_vectors = points[1].vectors.as_ref().expect("second vectors");
        match first_vectors.vectors_options.as_ref().expect("vector body") {
            VectorsOptions::Vectors(named) => {
                assert!(named.vectors.contains_key("multi"));
                assert!(named.vectors.contains_key("keywords"));
            }
            other @ VectorsOptions::Vector(_) => panic!("expected named vectors, found {other:?}"),
        }
        match second_vectors.vectors_options.as_ref().expect("vector body") {
            VectorsOptions::Vectors(named) => {
                assert!(!named.vectors.contains_key("multi"));
                assert!(named.vectors.contains_key("keywords"));
            }
            other @ VectorsOptions::Vector(_) => panic!("expected named vectors, found {other:?}"),
        }
    }
}
