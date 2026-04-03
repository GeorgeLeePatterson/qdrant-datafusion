pub(crate) mod filter;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr::BinaryExpr;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr as PhysicalBinaryExpr, Column as PhysicalColumn, Literal as PhysicalLiteral,
};
use qdrant_client::qdrant::{PayloadSchemaInfo, PayloadSchemaType, payload_index_params};

use self::filter::QdrantFilterValue;
use self::filter::value::{
    boolean_scalar, float_scalar, integer_scalar, string_scalar, timestamp_scalar,
};
use crate::arrow::schema::PAYLOAD_FIELD_NAME;
use crate::expr_fn::{is_payload_access_function_name, is_payload_function_name};
use crate::table::scan_spec::QdrantPayloadOrdering;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct QdrantPayloadSchema {
    fields: HashMap<String, QdrantPayloadField>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QdrantPayloadField {
    Keyword,
    Integer { lookup: bool, range: bool },
    Float,
    Geo,
    Bool,
    Datetime,
    Uuid,
}

impl QdrantPayloadField {
    pub(crate) fn projection_data_type(self) -> Option<DataType> {
        match self {
            QdrantPayloadField::Keyword | QdrantPayloadField::Uuid => Some(DataType::Utf8),
            QdrantPayloadField::Integer { .. } => Some(DataType::Int64),
            QdrantPayloadField::Float => Some(DataType::Float64),
            QdrantPayloadField::Bool => Some(DataType::Boolean),
            QdrantPayloadField::Datetime => Some(DataType::Timestamp(TimeUnit::Millisecond, None)),
            QdrantPayloadField::Geo => None,
        }
    }

    pub(crate) fn supports_equality(self) -> bool {
        matches!(
            self,
            QdrantPayloadField::Keyword
                | QdrantPayloadField::Float
                | QdrantPayloadField::Bool
                | QdrantPayloadField::Datetime
                | QdrantPayloadField::Uuid
                | QdrantPayloadField::Integer { lookup: true, .. }
        )
    }

    pub(crate) fn supports_facet(self) -> bool {
        matches!(
            self,
            QdrantPayloadField::Keyword
                | QdrantPayloadField::Bool
                | QdrantPayloadField::Integer { lookup: true, .. }
        )
    }

    pub(crate) fn into_filter_value(self, literal: &ScalarValue) -> Option<QdrantFilterValue> {
        match self {
            QdrantPayloadField::Keyword | QdrantPayloadField::Uuid => {
                Some(QdrantFilterValue::String(string_scalar(literal)?))
            }
            QdrantPayloadField::Integer { .. } => {
                Some(QdrantFilterValue::Integer(integer_scalar(literal)?))
            }
            QdrantPayloadField::Float => Some(QdrantFilterValue::Float(float_scalar(literal)?)),
            QdrantPayloadField::Geo => None,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QdrantPayloadAccess {
    payload: Column,
    path: QdrantPayloadPath,
}

impl QdrantPayloadAccess {
    pub(crate) fn from_logical_expr(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::Alias(alias) => Self::from_logical_expr(&alias.expr),
            Expr::ScalarFunction(function) if is_payload_function_name(function.name()) => {
                function.args.first().and_then(Self::from_logical_expr)
            }
            Expr::ScalarFunction(function) if is_payload_access_function_name(function.name()) => {
                let [payload, path] = function.args.as_slice() else {
                    return None;
                };
                let Expr::Column(column) = payload.clone().unalias_nested().data else {
                    return None;
                };
                if column.name != PAYLOAD_FIELD_NAME {
                    return None;
                }
                Some(Self { payload: column, path: logical_path_literal(path)? })
            }
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Colon, right }) => {
                let Expr::Column(column) = left.as_ref() else {
                    return None;
                };
                if column.name != PAYLOAD_FIELD_NAME {
                    return None;
                }
                Some(Self { payload: column.clone(), path: logical_path_literal(right)? })
            }
            _ => None,
        }
    }

    pub(crate) fn from_raw_logical_expr(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::Alias(alias) => Self::from_raw_logical_expr(&alias.expr),
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Colon, right }) => {
                let Expr::Column(column) = left.as_ref() else {
                    return None;
                };
                if column.name != PAYLOAD_FIELD_NAME {
                    return None;
                }
                Some(Self { payload: column.clone(), path: logical_path_literal(right)? })
            }
            _ => None,
        }
    }

    pub(crate) fn payload_expr(&self) -> Expr {
        Expr::Column(self.payload.clone())
    }

    pub(crate) fn path(&self) -> &QdrantPayloadPath {
        &self.path
    }

    pub(crate) fn into_parts(self) -> (Expr, String) {
        (Expr::Column(self.payload), self.path.path)
    }
}

impl QdrantPayloadPath {
    pub(crate) fn new(path: String) -> Option<Self> {
        (!path.is_empty()).then_some(Self { path })
    }

    pub(crate) fn key(&self) -> &str {
        &self.path
    }

    pub(crate) fn from_logical_expr(expr: &Expr) -> Option<Self> {
        QdrantPayloadAccess::from_logical_expr(expr).map(|access| access.path)
    }

    pub(crate) fn from_physical_expr(expr: &Arc<dyn PhysicalExpr>) -> Option<Self> {
        if let Some(binary) = expr.as_any().downcast_ref::<PhysicalBinaryExpr>()
            && *binary.op() == Operator::Colon
        {
            let column = binary.left().as_any().downcast_ref::<PhysicalColumn>()?;
            if column.name() != PAYLOAD_FIELD_NAME {
                return None;
            }
            return physical_path_literal(binary.right());
        }
        let function = expr.as_any().downcast_ref::<ScalarFunctionExpr>()?;
        if !is_payload_access_function_name(function.name()) {
            return None;
        }
        let [payload, path] = function.args() else {
            return None;
        };
        let column = payload.as_any().downcast_ref::<PhysicalColumn>()?;
        if column.name() != PAYLOAD_FIELD_NAME {
            return None;
        }
        physical_path_literal(path)
    }
}

impl QdrantPayloadSchema {
    pub(crate) fn field(&self, field: &str) -> Option<QdrantPayloadField> {
        self.fields.get(field).copied()
    }

    pub(crate) fn field_for_path(&self, path: &str) -> Option<QdrantPayloadField> {
        self.field(path).or_else(|| path.split('.').next().and_then(|prefix| self.field(prefix)))
    }

    pub(crate) fn ordering_for(
        &self,
        field: &str,
        descending: bool,
    ) -> Option<QdrantPayloadOrdering> {
        match self.field_for_path(field) {
            Some(
                QdrantPayloadField::Integer { range: true, .. }
                | QdrantPayloadField::Float
                | QdrantPayloadField::Datetime,
            ) => Some(QdrantPayloadOrdering { field: field.to_owned(), descending }),
            _ => None,
        }
    }
}

impl From<HashMap<String, PayloadSchemaInfo>> for QdrantPayloadSchema {
    fn from(payload_schema: HashMap<String, PayloadSchemaInfo>) -> Self {
        let fields = payload_schema
            .into_iter()
            .filter_map(|(field_name, info)| {
                let data_type = PayloadSchemaType::try_from(info.data_type).ok()?;
                let params = info.params.and_then(|params| params.index_params);
                let field = match data_type {
                    PayloadSchemaType::Keyword => match params {
                        None | Some(payload_index_params::IndexParams::KeywordIndexParams(_)) => {
                            QdrantPayloadField::Keyword
                        }
                        _ => return None,
                    },
                    PayloadSchemaType::Integer => match params {
                        None => QdrantPayloadField::Integer { lookup: true, range: true },
                        Some(payload_index_params::IndexParams::IntegerIndexParams(params)) => {
                            QdrantPayloadField::Integer {
                                lookup: params.lookup.unwrap_or(true),
                                range: params.range.unwrap_or(true),
                            }
                        }
                        _ => return None,
                    },
                    PayloadSchemaType::Float => match params {
                        None | Some(payload_index_params::IndexParams::FloatIndexParams(_)) => {
                            QdrantPayloadField::Float
                        }
                        _ => return None,
                    },
                    PayloadSchemaType::Geo => match params {
                        None | Some(payload_index_params::IndexParams::GeoIndexParams(_)) => {
                            QdrantPayloadField::Geo
                        }
                        _ => return None,
                    },
                    PayloadSchemaType::Bool => match params {
                        None | Some(payload_index_params::IndexParams::BoolIndexParams(_)) => {
                            QdrantPayloadField::Bool
                        }
                        _ => return None,
                    },
                    PayloadSchemaType::Datetime => match params {
                        None | Some(payload_index_params::IndexParams::DatetimeIndexParams(_)) => {
                            QdrantPayloadField::Datetime
                        }
                        _ => return None,
                    },
                    PayloadSchemaType::Uuid => match params {
                        None | Some(payload_index_params::IndexParams::UuidIndexParams(_)) => {
                            QdrantPayloadField::Uuid
                        }
                        _ => return None,
                    },
                    _ => return None,
                };
                Some((field_name, field))
            })
            .collect();
        Self { fields }
    }
}

fn logical_path_literal(expr: &Expr) -> Option<QdrantPayloadPath> {
    match expr.clone().unalias_nested().data {
        Expr::Literal(ScalarValue::Utf8(Some(path)) | ScalarValue::LargeUtf8(Some(path)), _) => {
            QdrantPayloadPath::new(path)
        }
        _ => None,
    }
}

fn physical_path_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<QdrantPayloadPath> {
    let literal = expr.as_any().downcast_ref::<PhysicalLiteral>()?;
    QdrantPayloadPath::new(string_scalar(literal.value())?)
}

#[cfg(test)]
mod tests {
    use qdrant_client::qdrant::{
        BoolIndexParams, FloatIndexParams, IntegerIndexParams, KeywordIndexParams, UuidIndexParams,
    };

    use super::*;

    #[test]
    fn payload_access_recognizes_raw_public_and_internal_forms() {
        use datafusion::common::Column;
        use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
        use datafusion::prelude::lit;

        let payload = Expr::Column(Column::new_unqualified(PAYLOAD_FIELD_NAME));
        let raw = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(payload.clone()),
            Operator::Colon,
            Box::new(lit("rank")),
        ));
        let public = crate::expr_fn::qdrant_payload(raw.clone(), "Integer");
        let internal =
            crate::expr_fn::payload_access_expr(payload.clone(), "rank", &DataType::Int64)
                .expect("internal payload access");

        assert_eq!(QdrantPayloadPath::from_logical_expr(&raw).expect("raw path").key(), "rank");
        assert_eq!(
            QdrantPayloadPath::from_logical_expr(&public).expect("public path").key(),
            "rank"
        );
        assert_eq!(
            QdrantPayloadPath::from_logical_expr(&internal).expect("internal path").key(),
            "rank"
        );
        assert_eq!(
            QdrantPayloadAccess::from_logical_expr(&public).expect("public access").payload_expr(),
            payload
        );
    }

    #[test]
    #[expect(clippy::too_many_lines)]
    fn payload_schema_keeps_filterable_and_orderable_scalar_indexes() {
        let schema = QdrantPayloadSchema::from(HashMap::from([
            (
                "rank".to_owned(),
                PayloadSchemaInfo {
                    data_type: PayloadSchemaType::Integer as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(payload_index_params::IndexParams::IntegerIndexParams(
                            IntegerIndexParams { range: Some(true), ..Default::default() },
                        )),
                    }),
                    points: None,
                },
            ),
            (
                "match_only".to_owned(),
                PayloadSchemaInfo {
                    data_type: PayloadSchemaType::Integer as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(payload_index_params::IndexParams::IntegerIndexParams(
                            IntegerIndexParams {
                                lookup: Some(true),
                                range: Some(false),
                                ..Default::default()
                            },
                        )),
                    }),
                    points: None,
                },
            ),
            (
                "range_only".to_owned(),
                PayloadSchemaInfo {
                    data_type: PayloadSchemaType::Integer as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(payload_index_params::IndexParams::IntegerIndexParams(
                            IntegerIndexParams {
                                lookup: Some(false),
                                range: Some(true),
                                ..Default::default()
                            },
                        )),
                    }),
                    points: None,
                },
            ),
            (
                "regular_int".to_owned(),
                PayloadSchemaInfo {
                    data_type: PayloadSchemaType::Integer as i32,
                    params: None,
                    points: None,
                },
            ),
            (
                "score".to_owned(),
                PayloadSchemaInfo {
                    data_type: PayloadSchemaType::Float as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(payload_index_params::IndexParams::FloatIndexParams(
                            FloatIndexParams::default(),
                        )),
                    }),
                    points: None,
                },
            ),
            (
                "active".to_owned(),
                PayloadSchemaInfo {
                    data_type: PayloadSchemaType::Bool as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(payload_index_params::IndexParams::BoolIndexParams(
                            BoolIndexParams::default(),
                        )),
                    }),
                    points: None,
                },
            ),
            (
                "tag".to_owned(),
                PayloadSchemaInfo {
                    data_type: PayloadSchemaType::Keyword as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(payload_index_params::IndexParams::KeywordIndexParams(
                            KeywordIndexParams::default(),
                        )),
                    }),
                    points: None,
                },
            ),
            (
                "doc_id".to_owned(),
                PayloadSchemaInfo {
                    data_type: PayloadSchemaType::Uuid as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(payload_index_params::IndexParams::UuidIndexParams(
                            UuidIndexParams::default(),
                        )),
                    }),
                    points: None,
                },
            ),
        ]));

        assert_eq!(
            schema.ordering_for("rank", false),
            Some(QdrantPayloadOrdering { field: "rank".to_owned(), descending: false }),
        );
        assert_eq!(
            schema.ordering_for("score", true),
            Some(QdrantPayloadOrdering { field: "score".to_owned(), descending: true }),
        );
        assert_eq!(schema.ordering_for("match_only", false), None);
        assert_eq!(
            schema.ordering_for("regular_int", false),
            Some(QdrantPayloadOrdering { field: "regular_int".to_owned(), descending: false }),
        );
        assert_eq!(
            schema.field("match_only"),
            Some(QdrantPayloadField::Integer { lookup: true, range: false }),
        );
        assert_eq!(
            schema.field("range_only"),
            Some(QdrantPayloadField::Integer { lookup: false, range: true }),
        );
        assert_eq!(
            schema.field("regular_int"),
            Some(QdrantPayloadField::Integer { lookup: true, range: true }),
        );
        assert!(schema.field("match_only").is_some_and(QdrantPayloadField::supports_facet));
        assert!(schema.field("regular_int").is_some_and(QdrantPayloadField::supports_facet));
        assert!(!schema.field("range_only").is_some_and(QdrantPayloadField::supports_facet));
        assert_eq!(schema.field("tag"), Some(QdrantPayloadField::Keyword));
        assert_eq!(schema.field("active"), Some(QdrantPayloadField::Bool));
        assert_eq!(schema.field("doc_id"), Some(QdrantPayloadField::Uuid));
        assert_eq!(schema.field_for_path("rank.value"), schema.field("rank"));
        assert_eq!(
            QdrantPayloadField::Datetime.projection_data_type(),
            Some(DataType::Timestamp(TimeUnit::Millisecond, None))
        );
    }
}
