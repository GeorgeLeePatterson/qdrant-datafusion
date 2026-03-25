pub(crate) mod filter;

use std::collections::HashMap;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::BinaryExpr;
use datafusion::logical_expr::{Expr, Operator};
use qdrant_client::qdrant::{PayloadSchemaInfo, PayloadSchemaType, payload_index_params};

use self::filter::QdrantFilterValue;
use self::filter::value::{
    boolean_scalar, float_scalar, integer_scalar, string_scalar, timestamp_scalar,
};
use crate::arrow::schema::PAYLOAD_FIELD_NAME;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct QdrantPayloadSchema {
    fields: HashMap<String, QdrantPayloadField>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QdrantPayloadField {
    Keyword,
    Integer { range: bool },
    Float,
    Bool,
    Datetime,
    Uuid,
}

impl QdrantPayloadField {
    pub(crate) fn into_filter_value(self, literal: &ScalarValue) -> Option<QdrantFilterValue> {
        match self {
            QdrantPayloadField::Keyword | QdrantPayloadField::Uuid => {
                Some(QdrantFilterValue::String(string_scalar(literal)?))
            }
            QdrantPayloadField::Integer { .. } => {
                Some(QdrantFilterValue::Integer(integer_scalar(literal)?))
            }
            QdrantPayloadField::Float => Some(QdrantFilterValue::Float(float_scalar(literal)?)),
            QdrantPayloadField::Bool => Some(QdrantFilterValue::Bool(boolean_scalar(literal)?)),
            QdrantPayloadField::Datetime => {
                Some(QdrantFilterValue::Datetime(timestamp_scalar(literal)?))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct QdrantPayloadPath {
    path: String,
}

impl QdrantPayloadPath {
    pub(crate) fn new(path: String) -> Option<Self> { (!path.is_empty()).then_some(Self { path }) }

    pub(crate) fn key(&self) -> &str { &self.path }

    pub(crate) fn from_logical_expr(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Colon, right }) => {
                let Expr::Column(column) = left.as_ref() else {
                    return None;
                };
                if column.name != PAYLOAD_FIELD_NAME {
                    return None;
                }
                match right.as_ref() {
                    Expr::Literal(
                        ScalarValue::Utf8(Some(path)) | ScalarValue::LargeUtf8(Some(path)),
                        _,
                    ) => Self::new(path.clone()),
                    _ => None,
                }
            }
            Expr::Alias(alias) => Self::from_logical_expr(&alias.expr),
            _ => None,
        }
    }
}

impl QdrantPayloadSchema {
    pub(crate) fn field(&self, field: &str) -> Option<QdrantPayloadField> {
        self.fields.get(field).copied()
    }
}

impl From<HashMap<String, PayloadSchemaInfo>> for QdrantPayloadSchema {
    fn from(payload_schema: HashMap<String, PayloadSchemaInfo>) -> Self {
        let fields = payload_schema
            .into_iter()
            .filter_map(|(field_name, info)| {
                let data_type = PayloadSchemaType::try_from(info.data_type).ok()?;
                let params = info.params?.index_params?;
                let field = match (data_type, params) {
                    (
                        PayloadSchemaType::Keyword,
                        payload_index_params::IndexParams::KeywordIndexParams(_),
                    ) => QdrantPayloadField::Keyword,
                    (
                        PayloadSchemaType::Integer,
                        payload_index_params::IndexParams::IntegerIndexParams(params),
                    ) => QdrantPayloadField::Integer { range: params.range.unwrap_or(true) },
                    (
                        PayloadSchemaType::Float,
                        payload_index_params::IndexParams::FloatIndexParams(_),
                    ) => QdrantPayloadField::Float,
                    (
                        PayloadSchemaType::Bool,
                        payload_index_params::IndexParams::BoolIndexParams(_),
                    ) => QdrantPayloadField::Bool,
                    (
                        PayloadSchemaType::Datetime,
                        payload_index_params::IndexParams::DatetimeIndexParams(_),
                    ) => QdrantPayloadField::Datetime,
                    (
                        PayloadSchemaType::Uuid,
                        payload_index_params::IndexParams::UuidIndexParams(_),
                    ) => QdrantPayloadField::Uuid,
                    _ => return None,
                };
                Some((field_name, field))
            })
            .collect();
        Self { fields }
    }
}

#[cfg(test)]
mod tests {
    use qdrant_client::qdrant::{
        BoolIndexParams, FloatIndexParams, IntegerIndexParams, KeywordIndexParams, UuidIndexParams,
    };

    use super::*;
    use crate::table::pushdown::QdrantPayloadOrdering;

    #[test]
    fn payload_schema_keeps_filterable_and_orderable_scalar_indexes() {
        let schema = QdrantPayloadSchema::from(HashMap::from([
            ("rank".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Integer as i32,
                params:    Some(qdrant_client::qdrant::PayloadIndexParams {
                    index_params: Some(payload_index_params::IndexParams::IntegerIndexParams(
                        IntegerIndexParams { range: Some(true), ..Default::default() },
                    )),
                }),
                points:    None,
            }),
            ("lookup_only".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Integer as i32,
                params:    Some(qdrant_client::qdrant::PayloadIndexParams {
                    index_params: Some(payload_index_params::IndexParams::IntegerIndexParams(
                        IntegerIndexParams { range: Some(false), ..Default::default() },
                    )),
                }),
                points:    None,
            }),
            ("score".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Float as i32,
                params:    Some(qdrant_client::qdrant::PayloadIndexParams {
                    index_params: Some(payload_index_params::IndexParams::FloatIndexParams(
                        FloatIndexParams::default(),
                    )),
                }),
                points:    None,
            }),
            ("active".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Bool as i32,
                params:    Some(qdrant_client::qdrant::PayloadIndexParams {
                    index_params: Some(payload_index_params::IndexParams::BoolIndexParams(
                        BoolIndexParams::default(),
                    )),
                }),
                points:    None,
            }),
            ("tag".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Keyword as i32,
                params:    Some(qdrant_client::qdrant::PayloadIndexParams {
                    index_params: Some(payload_index_params::IndexParams::KeywordIndexParams(
                        KeywordIndexParams::default(),
                    )),
                }),
                points:    None,
            }),
            ("doc_id".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Uuid as i32,
                params:    Some(qdrant_client::qdrant::PayloadIndexParams {
                    index_params: Some(payload_index_params::IndexParams::UuidIndexParams(
                        UuidIndexParams::default(),
                    )),
                }),
                points:    None,
            }),
        ]));

        assert_eq!(
            schema.ordering_for("rank", false),
            Some(QdrantPayloadOrdering { field: "rank".to_owned(), descending: false }),
        );
        assert_eq!(
            schema.ordering_for("score", true),
            Some(QdrantPayloadOrdering { field: "score".to_owned(), descending: true }),
        );
        assert_eq!(schema.ordering_for("lookup_only", false), None);
        assert_eq!(schema.field("tag"), Some(QdrantPayloadField::Keyword));
        assert_eq!(schema.field("active"), Some(QdrantPayloadField::Bool));
        assert_eq!(schema.field("doc_id"), Some(QdrantPayloadField::Uuid));
    }
}
