mod lower;
mod normalize;
pub(super) mod value;

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalExpr;
use normalize::{exact_expr, exact_physical_expr};
use prost_types::Timestamp;
use qdrant_client::qdrant::{Condition, Filter, PointId, ValuesCount};

use super::{QdrantPayloadPath, QdrantPayloadSchema};

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct QdrantFilters {
    exprs: Vec<QdrantFilterExpr>,
}

impl QdrantFilters {
    pub(crate) fn try_new(
        base_schema: &SchemaRef,
        payload_schema: &QdrantPayloadSchema,
        filters: &[Expr],
    ) -> DataFusionResult<Self> {
        let exprs = filters
            .iter()
            .map(|filter| {
                let filter = filter.clone().unalias_nested().data;
                if let Some(expr) = exact_expr(base_schema, payload_schema, &filter) {
                    Ok(expr)
                } else {
                    exec_err!("unsupported pushed filter: {filter}")
                }
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        Ok(Self::from_exprs(exprs))
    }

    pub(crate) fn supports_exact(
        base_schema: &SchemaRef,
        payload_schema: &QdrantPayloadSchema,
        filter: &Expr,
    ) -> bool {
        let filter = filter.clone().unalias_nested().data;
        exact_expr(base_schema, payload_schema, &filter).is_some()
    }

    pub(crate) fn partition_physical(
        self,
        base_schema: &SchemaRef,
        payload_schema: &QdrantPayloadSchema,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> (Self, Vec<bool>) {
        let (support, exact_exprs): (Vec<_>, Vec<_>) = filters
            .iter()
            .map(|filter| {
                let expr = exact_physical_expr(base_schema, payload_schema, filter);
                (expr.is_some(), expr)
            })
            .unzip();
        let exprs =
            self.exprs.into_iter().chain(exact_exprs.into_iter().flatten()).collect::<Vec<_>>();
        (Self::from_exprs(exprs), support)
    }

    pub(crate) fn len(&self) -> usize {
        self.exprs.iter().map(QdrantFilterExpr::leaf_count).sum()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.exprs.is_empty()
    }

    pub(crate) fn to_filter(&self) -> Option<Filter> {
        match self.exprs.as_slice() {
            [] => None,
            [expr] => Some(expr.to_filter()),
            exprs => Some(Filter::must(exprs.iter().map(QdrantFilterExpr::to_condition))),
        }
    }

    pub(crate) fn possible_point_ids(&self) -> Option<Vec<PointId>> {
        Self::fold_possible_point_ids(&self.exprs)
    }

    fn from_exprs(exprs: impl IntoIterator<Item = QdrantFilterExpr>) -> Self {
        exprs.into_iter().fold(Self::default(), |mut filters, expr| {
            filters.push(expr);
            filters
        })
    }

    fn push(&mut self, expr: QdrantFilterExpr) {
        for expr in expr.into_and_parts() {
            if !self.exprs.contains(&expr) {
                self.exprs.push(expr);
            }
        }
    }

    fn fold_possible_point_ids(exprs: &[QdrantFilterExpr]) -> Option<Vec<PointId>> {
        exprs.iter().filter_map(QdrantFilterExpr::possible_point_ids).reduce(|mut ids, next_ids| {
            ids.retain(|id| next_ids.contains(id));
            ids
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QdrantFilterValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Datetime(Timestamp),
}

#[derive(Debug, Clone, PartialEq)]
enum QdrantPredicate {
    IdIn(Vec<PointId>),
    HasVector(String),
    PayloadIsNull(QdrantPayloadPath),
    PayloadIsEmpty(QdrantPayloadPath),
    PayloadExists(QdrantPayloadPath),
    PayloadEq {
        field: QdrantPayloadPath,
        value: QdrantFilterValue,
    },
    PayloadIn {
        field: QdrantPayloadPath,
        values: Vec<QdrantFilterValue>,
    },
    PayloadRange {
        field: QdrantPayloadPath,
        lower: Option<(QdrantFilterValue, bool)>,
        upper: Option<(QdrantFilterValue, bool)>,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum QdrantFilterExpr {
    Predicate(QdrantPredicate),
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
}

impl QdrantFilterExpr {
    fn possible_point_ids(&self) -> Option<Vec<PointId>> {
        match self {
            QdrantFilterExpr::Predicate(QdrantPredicate::IdIn(ids)) => Some(ids.clone()),
            QdrantFilterExpr::Predicate(_) | QdrantFilterExpr::Not(_) => None,
            QdrantFilterExpr::And(exprs) => QdrantFilters::fold_possible_point_ids(exprs),
            QdrantFilterExpr::Or(exprs) => {
                exprs.iter().map(Self::possible_point_ids).collect::<Option<Vec<_>>>().map(
                    |id_sets| {
                        id_sets.into_iter().flatten().fold(Vec::new(), |mut ids, id| {
                            if !ids.contains(&id) {
                                ids.push(id);
                            }
                            ids
                        })
                    },
                )
            }
        }
    }
}

impl QdrantPayloadPath {
    fn sql_null_filter_expr(self) -> QdrantFilterExpr {
        let missing = QdrantFilterExpr::and([
            QdrantFilterExpr::Predicate(QdrantPredicate::PayloadIsEmpty(self.clone())),
            QdrantFilterExpr::not(QdrantFilterExpr::Predicate(QdrantPredicate::PayloadExists(
                self.clone(),
            ))),
        ]);
        QdrantFilterExpr::or([
            QdrantFilterExpr::Predicate(QdrantPredicate::PayloadIsNull(self)),
            missing,
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QdrantFieldRef {
    Id,
    Payload(QdrantPayloadPath),
    Vector(String),
}

impl QdrantPredicate {
    fn to_condition(&self) -> Condition {
        match self {
            Self::IdIn(ids) => Condition::has_id(ids.clone()),
            Self::HasVector(name) => Condition::has_vector(name.clone()),
            Self::PayloadIsNull(field) => Condition::is_null(field.key()),
            Self::PayloadIsEmpty(field) => Condition::is_empty(field.key()),
            Self::PayloadExists(field) => Condition::values_count(
                field.key(),
                ValuesCount { gte: Some(0), ..Default::default() },
            ),
            Self::PayloadEq { field, value } => field.eq_condition(value),
            Self::PayloadIn { field, values } => field.in_condition(values),
            Self::PayloadRange { field, lower, upper } => {
                field.range_condition(lower.as_ref(), upper.as_ref())
            }
        }
    }
}

impl QdrantFilterExpr {
    fn and(exprs: impl IntoIterator<Item = Self>) -> Self {
        let mut flat = exprs.into_iter().flat_map(Self::into_and_parts).collect::<Vec<_>>();
        match flat.len() {
            1 => flat.pop().expect("single and child"),
            _ => Self::And(flat),
        }
    }

    fn or(exprs: impl IntoIterator<Item = Self>) -> Self {
        let mut flat = exprs.into_iter().flat_map(Self::into_or_parts).collect::<Vec<_>>();
        if let Some(predicate) = QdrantPredicate::from_disjunction(&flat) {
            return Self::Predicate(predicate);
        }
        match flat.len() {
            1 => flat.pop().expect("single or child"),
            _ => Self::Or(flat),
        }
    }

    fn not(expr: Self) -> Self {
        match expr {
            Self::Not(expr) => *expr,
            expr => Self::Not(Box::new(expr)),
        }
    }

    fn leaf_count(&self) -> usize {
        match self {
            Self::Predicate(_) => 1,
            Self::And(exprs) | Self::Or(exprs) => exprs.iter().map(Self::leaf_count).sum(),
            Self::Not(expr) => expr.leaf_count(),
        }
    }

    fn into_and_parts(self) -> Vec<Self> {
        match self {
            Self::And(exprs) => exprs,
            expr => vec![expr],
        }
    }

    fn into_or_parts(self) -> Vec<Self> {
        match self {
            Self::Or(exprs) => exprs,
            expr => vec![expr],
        }
    }

    fn to_filter(&self) -> Filter {
        match self {
            Self::Predicate(predicate) => Filter::must([predicate.to_condition()]),
            Self::And(exprs) => Filter::must(exprs.iter().map(Self::to_condition)),
            Self::Or(exprs) => Filter::should(exprs.iter().map(Self::to_condition)),
            Self::Not(expr) => Filter { must_not: vec![expr.to_condition()], ..Default::default() },
        }
    }

    fn to_condition(&self) -> Condition {
        match self {
            Self::Predicate(predicate) => predicate.to_condition(),
            _ => self.to_filter().into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{
        BinaryExpr as PhysicalBinaryExpr, Column as PhysicalColumn, IsNullExpr,
        Literal as PhysicalLiteral, NotExpr,
    };
    use qdrant_client::qdrant::{
        IntegerIndexParams, KeywordIndexParams, PayloadSchemaInfo, PayloadSchemaType,
        payload_index_params,
    };

    use super::*;
    use crate::arrow::schema::{ID_FIELD_NAME, PAYLOAD_FIELD_NAME, UNNAMED_VECTOR_FIELD_NAME};
    use crate::qdrant::QdrantPayloadSchema;

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    fn payload_schema() -> QdrantPayloadSchema {
        QdrantPayloadSchema::from(HashMap::from([
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
        ]))
    }

    fn payload_path(path: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name(PAYLOAD_FIELD_NAME))),
            Operator::Colon,
            Box::new(Expr::Literal(ScalarValue::Utf8(Some(path.to_owned())), None)),
        ))
    }

    fn physical_payload_path(path: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(PhysicalBinaryExpr::new(
            Arc::new(PhysicalColumn::new(PAYLOAD_FIELD_NAME, 1)),
            Operator::Colon,
            Arc::new(PhysicalLiteral::new(ScalarValue::Utf8(Some(path.to_owned())))),
        ))
    }

    #[test]
    fn supports_exact_payload_scalar_filters() {
        let schema = schema(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
        ]);
        let payload_schema = payload_schema();

        assert!(QdrantFilters::supports_exact(
            &schema,
            &payload_schema,
            &Expr::BinaryExpr(BinaryExpr::new(
                Box::new(payload_path("rank")),
                Operator::GtEq,
                Box::new(Expr::Literal(ScalarValue::Int64(Some(10)), None)),
            )),
        ));
        assert!(!QdrantFilters::supports_exact(
            &schema,
            &payload_schema,
            &Expr::BinaryExpr(BinaryExpr::new(
                Box::new(payload_path("range_only")),
                Operator::Eq,
                Box::new(Expr::Literal(ScalarValue::Int64(Some(10)), None)),
            )),
        ));
        assert!(QdrantFilters::supports_exact(
            &schema,
            &payload_schema,
            &Expr::BinaryExpr(BinaryExpr::new(
                Box::new(payload_path("range_only")),
                Operator::GtEq,
                Box::new(Expr::Literal(ScalarValue::Int64(Some(10)), None)),
            )),
        ));
        assert!(QdrantFilters::supports_exact(
            &schema,
            &payload_schema,
            &Expr::InList(InList::new(
                Box::new(payload_path("tag")),
                vec![
                    Expr::Literal(ScalarValue::Utf8(Some("a".to_owned())), None),
                    Expr::Literal(ScalarValue::Utf8(Some("b".to_owned())), None),
                ],
                false,
            )),
        ));
        assert!(QdrantFilters::supports_exact(
            &schema,
            &payload_schema,
            &Expr::BinaryExpr(BinaryExpr::new(
                Box::new(payload_path("tag")),
                Operator::Eq,
                Box::new(Expr::Literal(ScalarValue::Utf8(Some(String::new())), None)),
            )),
        ));
        assert!(QdrantFilters::supports_exact(
            &schema,
            &payload_schema,
            &Expr::Not(Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(payload_path("tag")),
                Operator::Eq,
                Box::new(Expr::Literal(ScalarValue::Utf8(Some("a".to_owned())), None)),
            )))),
        ));
        assert!(QdrantFilters::supports_exact(
            &schema,
            &payload_schema,
            &Expr::BinaryExpr(BinaryExpr::new(
                Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(payload_path("tag")),
                    Operator::Eq,
                    Box::new(Expr::Literal(ScalarValue::Utf8(Some("a".to_owned())), None)),
                ))),
                Operator::Or,
                Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(payload_path("rank")),
                    Operator::Eq,
                    Box::new(Expr::Literal(ScalarValue::Int64(Some(10)), None)),
                ))),
            )),
        ));
        assert!(QdrantFilters::supports_exact(
            &schema,
            &payload_schema,
            &Expr::IsNull(Box::new(payload_path("remark"))),
        ));
        assert!(QdrantFilters::supports_exact(
            &schema,
            &payload_schema,
            &Expr::IsNotNull(Box::new(payload_path("remark"))),
        ));
        assert!(!QdrantFilters::supports_exact(
            &schema,
            &payload_schema,
            &Expr::BinaryExpr(BinaryExpr::new(
                Box::new(payload_path("rank")),
                Operator::LikeMatch,
                Box::new(Expr::Literal(ScalarValue::Utf8(Some("%1".to_owned())), None)),
            )),
        ));
    }

    #[test]
    fn supports_exact_id_and_vector_filters() {
        let vector_schema = schema(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new("image", DataType::new_fixed_size_list(DataType::Float32, 3, false), true),
        ]);

        assert!(QdrantFilters::supports_exact(
            &vector_schema,
            &QdrantPayloadSchema::default(),
            &Expr::InList(InList::new(
                Box::new(Expr::Column(Column::from_name(ID_FIELD_NAME))),
                vec![
                    Expr::Literal(ScalarValue::UInt64(Some(1)), None),
                    Expr::Literal(ScalarValue::Utf8(Some("2".to_owned())), None),
                ],
                false,
            )),
        ));
        assert!(QdrantFilters::supports_exact(
            &vector_schema,
            &QdrantPayloadSchema::default(),
            &Expr::IsNull(Box::new(Expr::Column(Column::from_name("image")))),
        ));
        assert!(QdrantFilters::supports_exact(
            &vector_schema,
            &QdrantPayloadSchema::default(),
            &Expr::Not(Box::new(Expr::IsNotNull(Box::new(Expr::Column(Column::from_name(
                "image",
            )))))),
        ));
        assert!(!QdrantFilters::supports_exact(
            &vector_schema,
            &QdrantPayloadSchema::default(),
            &Expr::IsNull(Box::new(Expr::Column(Column::from_name(UNNAMED_VECTOR_FIELD_NAME)))),
        ));
    }

    #[test]
    fn pushdown_physical_supports_boolean_filters() {
        let schema = schema(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
        ]);
        let payload_schema = payload_schema();
        let left = Arc::new(PhysicalBinaryExpr::new(
            physical_payload_path("rank"),
            Operator::GtEq,
            Arc::new(PhysicalLiteral::new(ScalarValue::Utf8(Some("10".to_owned())))),
        ));
        let right = Arc::new(PhysicalBinaryExpr::new(
            physical_payload_path("tag"),
            Operator::Eq,
            Arc::new(PhysicalLiteral::new(ScalarValue::Utf8(Some("blue".to_owned())))),
        ));
        let filter =
            Arc::new(NotExpr::new(Arc::new(PhysicalBinaryExpr::new(left, Operator::Or, right))));

        let (filters, support) =
            QdrantFilters::default().partition_physical(&schema, &payload_schema, &[filter]);

        assert_eq!(support, vec![true]);
        assert_eq!(filters.len(), 2);
        let filter = filters.to_filter().expect("qdrant filter");
        assert_eq!(filter.must_not.len(), 1);

        let payload_null = Arc::new(IsNullExpr::new(physical_payload_path("remark")));
        let (filters, support) =
            QdrantFilters::default().partition_physical(&schema, &payload_schema, &[payload_null]);
        assert_eq!(support, vec![true]);
        assert!(filters.to_filter().is_some());
    }

    #[test]
    fn pushdown_physical_rejects_unsupported_boolean_leaf() {
        let schema = schema(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
        ]);
        let left = Arc::new(PhysicalBinaryExpr::new(
            physical_payload_path("tag"),
            Operator::LikeMatch,
            Arc::new(PhysicalLiteral::new(ScalarValue::Utf8(Some("%red".to_owned())))),
        ));
        let right = Arc::new(PhysicalBinaryExpr::new(
            Arc::new(PhysicalColumn::new(ID_FIELD_NAME, 0)),
            Operator::Eq,
            Arc::new(PhysicalLiteral::new(ScalarValue::Utf8(Some("1".to_owned())))),
        ));
        let filter = Arc::new(PhysicalBinaryExpr::new(left, Operator::Or, right));

        let (_, support) =
            QdrantFilters::default().partition_physical(&schema, &payload_schema(), &[filter]);

        assert_eq!(support, vec![false]);
    }

    #[test]
    fn possible_point_ids_tracks_finite_id_upper_bounds() {
        let schema = schema(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
        ]);
        let filters = QdrantFilters::try_new(
            &schema,
            &payload_schema(),
            &[Expr::BinaryExpr(BinaryExpr::new(
                Box::new(Expr::InList(InList::new(
                    Box::new(Expr::Column(Column::from_name(ID_FIELD_NAME))),
                    vec![
                        Expr::Literal(ScalarValue::Utf8(Some("1".to_owned())), None),
                        Expr::Literal(ScalarValue::Utf8(Some("2".to_owned())), None),
                    ],
                    false,
                ))),
                Operator::And,
                Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(payload_path("tag")),
                    Operator::Eq,
                    Box::new(Expr::Literal(ScalarValue::Utf8(Some("blue".to_owned())), None)),
                ))),
            ))],
        )
        .expect("filters");
        let ids = filters.possible_point_ids().expect("point ids");
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn possible_point_ids_rejects_unbounded_not_branch() {
        let schema = schema(vec![Field::new(ID_FIELD_NAME, DataType::Utf8, false)]);
        let filters = QdrantFilters::try_new(
            &schema,
            &QdrantPayloadSchema::default(),
            &[Expr::Not(Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(Expr::Column(Column::from_name(ID_FIELD_NAME))),
                Operator::Eq,
                Box::new(Expr::Literal(ScalarValue::Utf8(Some("1".to_owned())), None)),
            ))))],
        )
        .expect("filters");
        assert!(filters.possible_point_ids().is_none());
    }
}
