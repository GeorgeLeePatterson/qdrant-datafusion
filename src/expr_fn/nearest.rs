use std::sync::OnceLock;

use datafusion::arrow::array::Array;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::prelude::lit;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::{PointId, Value};

use super::common::{NonExecutableScoreUdf, column_name, function_args};

pub const NEAREST_SCORE_FUNCTION_NAME: &str = "qdrant_nearest_score";
pub const NEAREST_SPARSE_SCORE_FUNCTION_NAME: &str = "qdrant_nearest_sparse_score";
pub const NEAREST_MULTI_SCORE_FUNCTION_NAME: &str = "qdrant_nearest_multi_score";
pub const NEAREST_ID_SCORE_FUNCTION_NAME: &str = "qdrant_nearest_id_score";
pub const NEAREST_DOCUMENT_SCORE_FUNCTION_NAME: &str = "qdrant_nearest_document_score";
pub const NEAREST_IMAGE_SCORE_FUNCTION_NAME: &str = "qdrant_nearest_image_score";
pub const NEAREST_OBJECT_SCORE_FUNCTION_NAME: &str = "qdrant_nearest_object_score";

const NEAREST_SCORE_ALIASES: &[&str] = &["nearest_score"];
const NEAREST_SPARSE_SCORE_ALIASES: &[&str] = &["nearest_sparse_score"];
const NEAREST_MULTI_SCORE_ALIASES: &[&str] = &["nearest_multi_score", "nearest_multidense_score"];
const NEAREST_ID_SCORE_ALIASES: &[&str] = &["nearest_id_score"];
const NEAREST_DOCUMENT_SCORE_ALIASES: &[&str] = &["nearest_document_score"];
const NEAREST_IMAGE_SCORE_ALIASES: &[&str] = &["nearest_image_score"];
const NEAREST_OBJECT_SCORE_ALIASES: &[&str] = &["nearest_object_score"];

#[derive(Debug, Clone)]
pub(crate) enum NearestCall {
    Dense { vector_field: String, vector: Vec<f32> },
    Sparse { vector_field: String, indices: Vec<u32>, values: Vec<f32> },
    MultiDense { vector_field: String, vectors: Vec<Vec<f32>> },
    Id { vector_field: String, point_id: PointId },
    Document { vector_field: String, text: String, model: Option<String> },
    Image { vector_field: String, image: Value, model: Option<String> },
    Object { vector_field: String, object: Value, model: Option<String> },
}

impl NearestCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        if let Some(call) = dense_call(expr)? {
            return Ok(Some(call));
        }
        if let Some(call) = sparse_call(expr)? {
            return Ok(Some(call));
        }
        if let Some(call) = multi_call(expr)? {
            return Ok(Some(call));
        }
        if let Some(call) = id_call(expr)? {
            return Ok(Some(call));
        }
        if let Some(call) = document_call(expr)? {
            return Ok(Some(call));
        }
        if let Some(call) = image_call(expr)? {
            return Ok(Some(call));
        }
        if let Some(call) = object_call(expr)? {
            return Ok(Some(call));
        }
        Ok(None)
    }
}

#[must_use]
pub fn qdrant_nearest_score(vector: Expr, query: impl IntoIterator<Item = f32>) -> Expr {
    let mut args = vec![vector];
    args.extend(query.into_iter().map(lit));
    qdrant_nearest_score_udf().call(args)
}

#[must_use]
pub fn qdrant_nearest_sparse_score(vector: Expr, indices: Expr, values: Expr) -> Expr {
    qdrant_nearest_sparse_score_udf().call(vec![vector, indices, values])
}

#[must_use]
pub fn qdrant_nearest_multi_score(vector: Expr, query: Expr) -> Expr {
    qdrant_nearest_multi_score_udf().call(vec![vector, query])
}

#[must_use]
pub fn qdrant_nearest_id_score(vector: Expr, point_id: Expr) -> Expr {
    qdrant_nearest_id_score_udf().call(vec![vector, point_id])
}

#[must_use]
pub fn qdrant_nearest_document_score(vector: Expr, document: Expr) -> Expr {
    qdrant_nearest_document_score_udf().call(vec![vector, document])
}

#[must_use]
pub fn qdrant_nearest_image_score(vector: Expr, image: Expr) -> Expr {
    qdrant_nearest_image_score_udf().call(vec![vector, image])
}

#[must_use]
pub fn qdrant_nearest_object_score(vector: Expr, object: Expr) -> Expr {
    qdrant_nearest_object_score_udf().call(vec![vector, object])
}

pub(crate) fn qdrant_nearest_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(
            NEAREST_SCORE_FUNCTION_NAME,
            NEAREST_SCORE_ALIASES,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_nearest_sparse_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(
            NEAREST_SPARSE_SCORE_FUNCTION_NAME,
            NEAREST_SPARSE_SCORE_ALIASES,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_nearest_multi_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(
            NEAREST_MULTI_SCORE_FUNCTION_NAME,
            NEAREST_MULTI_SCORE_ALIASES,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_nearest_id_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(
            NEAREST_ID_SCORE_FUNCTION_NAME,
            NEAREST_ID_SCORE_ALIASES,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_nearest_document_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(
            NEAREST_DOCUMENT_SCORE_FUNCTION_NAME,
            NEAREST_DOCUMENT_SCORE_ALIASES,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_nearest_image_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(
            NEAREST_IMAGE_SCORE_FUNCTION_NAME,
            NEAREST_IMAGE_SCORE_ALIASES,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_nearest_object_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(
            NEAREST_OBJECT_SCORE_FUNCTION_NAME,
            NEAREST_OBJECT_SCORE_ALIASES,
        ))
    })
    .clone()
}

fn dense_call(expr: &Expr) -> Result<Option<NearestCall>> {
    let Some(args) = function_args(expr, NEAREST_SCORE_FUNCTION_NAME, NEAREST_SCORE_ALIASES) else {
        return Ok(None);
    };
    if args.len() < 2 {
        return plan_err!(
            "{NEAREST_SCORE_FUNCTION_NAME} requires a vector column and at least one query \
             component"
        );
    }
    Ok(Some(NearestCall::Dense {
        vector_field: column_name(&args[0], NEAREST_SCORE_FUNCTION_NAME)?,
        vector: args[1..].iter().map(query_component).collect::<Result<Vec<_>>>()?,
    }))
}

fn sparse_call(expr: &Expr) -> Result<Option<NearestCall>> {
    let Some(args) =
        function_args(expr, NEAREST_SPARSE_SCORE_FUNCTION_NAME, NEAREST_SPARSE_SCORE_ALIASES)
    else {
        return Ok(None);
    };
    if args.len() != 3 {
        return plan_err!(
            "{NEAREST_SPARSE_SCORE_FUNCTION_NAME} requires a vector column, sparse indices, and \
             sparse values"
        );
    }
    Ok(Some(NearestCall::Sparse {
        vector_field: column_name(&args[0], NEAREST_SPARSE_SCORE_FUNCTION_NAME)?,
        indices: u32_list_literal(&args[1], NEAREST_SPARSE_SCORE_FUNCTION_NAME, "sparse indices")?,
        values: f32_list_literal(&args[2], NEAREST_SPARSE_SCORE_FUNCTION_NAME, "sparse values")?,
    }))
}

fn multi_call(expr: &Expr) -> Result<Option<NearestCall>> {
    let Some(args) =
        function_args(expr, NEAREST_MULTI_SCORE_FUNCTION_NAME, NEAREST_MULTI_SCORE_ALIASES)
    else {
        return Ok(None);
    };
    if args.len() != 2 {
        return plan_err!(
            "{NEAREST_MULTI_SCORE_FUNCTION_NAME} requires a vector column and a multivector query"
        );
    }
    Ok(Some(NearestCall::MultiDense {
        vector_field: column_name(&args[0], NEAREST_MULTI_SCORE_FUNCTION_NAME)?,
        vectors: nested_f32_list_literal(
            &args[1],
            NEAREST_MULTI_SCORE_FUNCTION_NAME,
            "multivector query",
        )?,
    }))
}

fn id_call(expr: &Expr) -> Result<Option<NearestCall>> {
    let Some(args) = function_args(expr, NEAREST_ID_SCORE_FUNCTION_NAME, NEAREST_ID_SCORE_ALIASES)
    else {
        return Ok(None);
    };
    if args.len() != 2 {
        return plan_err!(
            "{NEAREST_ID_SCORE_FUNCTION_NAME} requires a vector column and a point id"
        );
    }
    Ok(Some(NearestCall::Id {
        vector_field: column_name(&args[0], NEAREST_ID_SCORE_FUNCTION_NAME)?,
        point_id: point_id_literal(&args[1], NEAREST_ID_SCORE_FUNCTION_NAME, "point id")?,
    }))
}

fn document_call(expr: &Expr) -> Result<Option<NearestCall>> {
    let Some(args) =
        function_args(expr, NEAREST_DOCUMENT_SCORE_FUNCTION_NAME, NEAREST_DOCUMENT_SCORE_ALIASES)
    else {
        return Ok(None);
    };
    if args.len() != 2 && args.len() != 3 {
        return plan_err!(
            "{NEAREST_DOCUMENT_SCORE_FUNCTION_NAME} requires a vector column, document text, and \
             optional model name"
        );
    }
    Ok(Some(NearestCall::Document {
        vector_field: column_name(&args[0], NEAREST_DOCUMENT_SCORE_FUNCTION_NAME)?,
        text: string_literal(&args[1], NEAREST_DOCUMENT_SCORE_FUNCTION_NAME, "document text")?,
        model: optional_model(&args[2..], NEAREST_DOCUMENT_SCORE_FUNCTION_NAME)?,
    }))
}

fn image_call(expr: &Expr) -> Result<Option<NearestCall>> {
    let Some(args) =
        function_args(expr, NEAREST_IMAGE_SCORE_FUNCTION_NAME, NEAREST_IMAGE_SCORE_ALIASES)
    else {
        return Ok(None);
    };
    if args.len() != 2 && args.len() != 3 {
        return plan_err!(
            "{NEAREST_IMAGE_SCORE_FUNCTION_NAME} requires a vector column, image input, and \
             optional model name"
        );
    }
    Ok(Some(NearestCall::Image {
        vector_field: column_name(&args[0], NEAREST_IMAGE_SCORE_FUNCTION_NAME)?,
        image: image_literal(&args[1], NEAREST_IMAGE_SCORE_FUNCTION_NAME, "image input")?,
        model: optional_model(&args[2..], NEAREST_IMAGE_SCORE_FUNCTION_NAME)?,
    }))
}

fn object_call(expr: &Expr) -> Result<Option<NearestCall>> {
    let Some(args) =
        function_args(expr, NEAREST_OBJECT_SCORE_FUNCTION_NAME, NEAREST_OBJECT_SCORE_ALIASES)
    else {
        return Ok(None);
    };
    if args.len() != 2 && args.len() != 3 {
        return plan_err!(
            "{NEAREST_OBJECT_SCORE_FUNCTION_NAME} requires a vector column, object input, and \
             optional model name"
        );
    }
    Ok(Some(NearestCall::Object {
        vector_field: column_name(&args[0], NEAREST_OBJECT_SCORE_FUNCTION_NAME)?,
        object: object_literal(&args[1], NEAREST_OBJECT_SCORE_FUNCTION_NAME, "object input")?,
        model: optional_model(&args[2..], NEAREST_OBJECT_SCORE_FUNCTION_NAME)?,
    }))
}

fn optional_model(args: &[Expr], function_name: &str) -> Result<Option<String>> {
    args.first().map(|expr| string_literal(expr, function_name, "model name")).transpose()
}

fn string_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<String> {
    match expr.clone().unalias_nested().data {
        Expr::Literal(ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)), _) => {
            Ok(value)
        }
        Expr::Cast(cast) => string_literal(&cast.expr, function_name, argument),
        Expr::TryCast(cast) => string_literal(&cast.expr, function_name, argument),
        _ => plan_err!("{function_name} requires {argument} to be a string literal"),
    }
}

fn point_id_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<PointId> {
    match expr.clone().unalias_nested().data {
        Expr::Literal(value, _) => point_id_from_scalar(&value, function_name, argument),
        Expr::Cast(cast) => point_id_literal(&cast.expr, function_name, argument),
        Expr::TryCast(cast) => point_id_literal(&cast.expr, function_name, argument),
        _ => plan_err!(
            "{function_name} requires {argument} to be a string or non-negative integer literal"
        ),
    }
}

fn image_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<Value> {
    Ok(Value::from(serde_json::Value::String(string_literal(expr, function_name, argument)?)))
}

fn object_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<Value> {
    let raw = string_literal(expr, function_name, argument)?;
    let value = serde_json::from_str::<serde_json::Value>(&raw).map_err(|error| {
        datafusion::error::DataFusionError::Plan(format!(
            "{function_name} requires {argument} to be valid JSON: {error}"
        ))
    })?;
    if !value.is_object() {
        return plan_err!("{function_name} requires {argument} to decode to a JSON object");
    }
    Ok(Value::from(value))
}

fn u32_list_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<Vec<u32>> {
    list_literal(expr, function_name, argument)?
        .iter()
        .map(|value| scalar_to_u32(value, function_name, argument))
        .collect()
}

fn f32_list_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<Vec<f32>> {
    list_literal(expr, function_name, argument)?
        .iter()
        .map(|value| scalar_to_f32(value, function_name, argument))
        .collect()
}

fn nested_f32_list_literal(
    expr: &Expr,
    function_name: &str,
    argument: &str,
) -> Result<Vec<Vec<f32>>> {
    list_literal(expr, function_name, argument)?
        .iter()
        .map(|value| {
            list_from_scalar(value, function_name, argument)?
                .iter()
                .map(|value| scalar_to_f32(value, function_name, argument))
                .collect()
        })
        .collect()
}

fn list_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<Vec<ScalarValue>> {
    match expr.clone().unalias_nested().data {
        Expr::Literal(value, _) => list_from_scalar(&value, function_name, argument),
        Expr::Cast(cast) => list_literal(&cast.expr, function_name, argument),
        Expr::TryCast(cast) => list_literal(&cast.expr, function_name, argument),
        _ => plan_err!("{function_name} requires {argument} to be an array literal"),
    }
}

fn list_from_scalar(
    value: &ScalarValue,
    function_name: &str,
    argument: &str,
) -> Result<Vec<ScalarValue>> {
    match value {
        ScalarValue::List(array) => {
            if array.is_empty() {
                Ok(vec![])
            } else {
                list_values_from_array(array.value(0).as_ref())
            }
        }
        ScalarValue::LargeList(array) => {
            if array.is_empty() {
                Ok(vec![])
            } else {
                list_values_from_array(array.value(0).as_ref())
            }
        }
        _ => plan_err!("{function_name} requires {argument} to be an array literal"),
    }
}

fn list_values_from_array(array: &dyn Array) -> Result<Vec<ScalarValue>> {
    (0..array.len()).map(|index| ScalarValue::try_from_array(array, index)).collect()
}

fn point_id_from_scalar(
    value: &ScalarValue,
    function_name: &str,
    argument: &str,
) -> Result<PointId> {
    let point_id_options = match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            Some(PointIdOptions::Uuid(value.clone()))
        }
        ScalarValue::Int8(Some(value)) if *value >= 0 => {
            Some(PointIdOptions::Num(u64::from(value.unsigned_abs())))
        }
        ScalarValue::Int16(Some(value)) if *value >= 0 => {
            Some(PointIdOptions::Num(u64::from(value.unsigned_abs())))
        }
        ScalarValue::Int32(Some(value)) if *value >= 0 => {
            Some(PointIdOptions::Num(u64::from(value.unsigned_abs())))
        }
        ScalarValue::Int64(Some(value)) if *value >= 0 => {
            Some(PointIdOptions::Num(value.unsigned_abs()))
        }
        ScalarValue::UInt8(Some(value)) => Some(PointIdOptions::Num(u64::from(*value))),
        ScalarValue::UInt16(Some(value)) => Some(PointIdOptions::Num(u64::from(*value))),
        ScalarValue::UInt32(Some(value)) => Some(PointIdOptions::Num(u64::from(*value))),
        ScalarValue::UInt64(Some(value)) => Some(PointIdOptions::Num(*value)),
        _ => None,
    };
    if let Some(point_id_options) = point_id_options {
        Ok(PointId { point_id_options: Some(point_id_options) })
    } else {
        plan_err!(
            "{function_name} requires {argument} to be a string or non-negative integer literal"
        )
    }
}

#[expect(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn scalar_to_f32(value: &ScalarValue, function_name: &str, argument: &str) -> Result<f32> {
    match value {
        ScalarValue::Float32(Some(value)) => Ok(*value),
        ScalarValue::Float64(Some(value)) => Ok(*value as f32),
        ScalarValue::Int8(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::Int16(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::Int32(Some(value)) => Ok(*value as f32),
        ScalarValue::Int64(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt8(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::UInt16(Some(value)) => Ok(f32::from(*value)),
        ScalarValue::UInt32(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt64(Some(value)) => Ok(*value as f32),
        _ => plan_err!("{function_name} requires {argument} to be numeric"),
    }
}

fn scalar_to_u32(value: &ScalarValue, function_name: &str, argument: &str) -> Result<u32> {
    match value {
        ScalarValue::Int8(Some(value)) if *value >= 0 => Ok(u32::from(value.unsigned_abs())),
        ScalarValue::Int16(Some(value)) if *value >= 0 => Ok(u32::from(value.unsigned_abs())),
        ScalarValue::Int32(Some(value)) if *value >= 0 => Ok(value.unsigned_abs()),
        ScalarValue::Int64(Some(value)) if *value >= 0 => u32::try_from(*value).map_err(|_| {
            datafusion::error::DataFusionError::Plan(format!(
                "{function_name} requires {argument} entries to fit in u32"
            ))
        }),
        ScalarValue::UInt8(Some(value)) => Ok(u32::from(*value)),
        ScalarValue::UInt16(Some(value)) => Ok(u32::from(*value)),
        ScalarValue::UInt32(Some(value)) => Ok(*value),
        ScalarValue::UInt64(Some(value)) => u32::try_from(*value).map_err(|_| {
            datafusion::error::DataFusionError::Plan(format!(
                "{function_name} requires {argument} entries to fit in u32"
            ))
        }),
        _ => plan_err!("{function_name} requires {argument} entries to be non-negative integers"),
    }
}

fn query_component(expr: &Expr) -> Result<f32> {
    let expr = expr.clone().unalias_nested().data;
    match expr {
        Expr::Negative(expr) => Ok(-query_component(&expr)?),
        Expr::Cast(cast) => query_component(&cast.expr),
        Expr::TryCast(cast) => query_component(&cast.expr),
        Expr::Literal(value, _) => {
            scalar_to_f32(&value, NEAREST_SCORE_FUNCTION_NAME, "query component")
        }
        _ => plan_err!("{NEAREST_SCORE_FUNCTION_NAME} query components must be numeric literals"),
    }
}
