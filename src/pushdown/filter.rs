use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{ScalarValue, exec_err};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::expr::{Between, InList};
use datafusion::logical_expr::utils::{split_binary, split_conjunction};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr as PhysicalBinaryExpr, Column as PhysicalColumn, InListExpr, IsNotNullExpr,
    IsNullExpr, Literal as PhysicalLiteral, NotExpr,
};
use datafusion::physical_expr::utils::{
    split_conjunction as split_physical_conjunction, split_disjunction,
};
use prost_types::Timestamp;
use qdrant_client::qdrant::{Condition, DatetimeRange, Filter, PointId, Range};

use super::{QdrantPayloadField, QdrantPayloadPath, QdrantPayloadSchema, logical_payload_path};
use crate::arrow::schema::{
    ID_FIELD_NAME, PAYLOAD_FIELD_NAME, UNNAMED_VECTOR_FIELD_NAME, dense_vector_width,
    is_multi_vector_field, is_sparse_vector_field,
};

#[derive(Debug, Clone, PartialEq)]
enum QdrantFilterValue {
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
    PayloadEq {
        field: QdrantPayloadPath,
        value: QdrantFilterValue,
    },
    PayloadIn {
        field:  QdrantPayloadPath,
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

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct QdrantFilters {
    exprs: Vec<QdrantFilterExpr>,
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
            Self::PayloadEq { field, value } => eq_condition(field, value),
            Self::PayloadIn { field, values } => in_condition(field, values),
            Self::PayloadRange { field, lower, upper } => {
                range_condition(field, lower.as_ref(), upper.as_ref())
            }
        }
    }
}

impl QdrantFilterExpr {
    fn and(exprs: impl IntoIterator<Item = Self>) -> Self {
        let mut flat = vec![];
        for expr in exprs {
            match expr {
                Self::And(exprs) => flat.extend(exprs),
                expr => flat.push(expr),
            }
        }
        match flat.len() {
            1 => flat.pop().expect("single and child"),
            _ => Self::And(flat),
        }
    }

    fn or(exprs: impl IntoIterator<Item = Self>) -> Self {
        let mut flat = vec![];
        for expr in exprs {
            match expr {
                Self::Or(exprs) => flat.extend(exprs),
                expr => flat.push(expr),
            }
        }
        if let Some(predicate) = disjunction_predicate(&flat) {
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

impl QdrantFilters {
    pub(crate) fn try_new(
        base_schema: &SchemaRef,
        payload_schema: &QdrantPayloadSchema,
        filters: &[Expr],
    ) -> DataFusionResult<Self> {
        let mut pushdown = Self::default();
        for filter in filters {
            let filter = filter.clone().unalias_nested().data;
            let Some(expr) = exact_expr(base_schema, payload_schema, &filter) else {
                return exec_err!("unsupported pushed filter: {filter}");
            };
            pushdown.push(expr);
        }
        Ok(pushdown)
    }

    pub(crate) fn supports_exact(
        base_schema: &SchemaRef,
        payload_schema: &QdrantPayloadSchema,
        filter: &Expr,
    ) -> bool {
        let filter = filter.clone().unalias_nested().data;
        exact_expr(base_schema, payload_schema, &filter).is_some()
    }

    pub(crate) fn pushdown_physical(
        &self,
        base_schema: &SchemaRef,
        payload_schema: &QdrantPayloadSchema,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> (Self, Vec<bool>) {
        let mut pushed = self.clone();
        let support = filters
            .iter()
            .map(|filter| {
                let Some(expr) = exact_physical_expr(base_schema, payload_schema, filter) else {
                    return false;
                };
                pushed.push(expr);
                true
            })
            .collect();
        (pushed, support)
    }

    pub(crate) fn len(&self) -> usize { self.exprs.iter().map(QdrantFilterExpr::leaf_count).sum() }

    pub(crate) fn is_empty(&self) -> bool { self.exprs.is_empty() }

    pub(crate) fn to_filter(&self) -> Option<Filter> {
        match self.exprs.as_slice() {
            [] => None,
            [expr] => Some(expr.to_filter()),
            exprs => Some(Filter::must(exprs.iter().map(QdrantFilterExpr::to_condition))),
        }
    }

    fn push(&mut self, expr: QdrantFilterExpr) {
        for expr in expr.into_and_parts() {
            if !self.exprs.contains(&expr) {
                self.exprs.push(expr);
            }
        }
    }
}

fn exact_expr(
    base_schema: &SchemaRef,
    payload_schema: &QdrantPayloadSchema,
    expr: &Expr,
) -> Option<QdrantFilterExpr> {
    match expr {
        Expr::Alias(alias) => exact_expr(base_schema, payload_schema, &alias.expr),
        Expr::Not(expr) => {
            Some(QdrantFilterExpr::not(exact_expr(base_schema, payload_schema, expr)?))
        }
        Expr::BinaryExpr(BinaryExpr { op: Operator::And, .. }) => Some(QdrantFilterExpr::and(
            split_conjunction(expr)
                .into_iter()
                .map(|expr| exact_expr(base_schema, payload_schema, expr))
                .collect::<Option<Vec<_>>>()?,
        )),
        Expr::BinaryExpr(BinaryExpr { op: Operator::Or, .. }) => Some(QdrantFilterExpr::or(
            split_binary(expr, Operator::Or)
                .into_iter()
                .map(|expr| exact_expr(base_schema, payload_schema, expr))
                .collect::<Option<Vec<_>>>()?,
        )),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => filter_expr_from_refs(
            payload_schema,
            field_ref(base_schema, left),
            field_ref(base_schema, right),
            *op,
            scalar_literal(left),
            scalar_literal(right),
        ),
        Expr::InList(InList { expr, list, negated }) => in_list_expr_from_refs(
            payload_schema,
            field_ref(base_schema, expr),
            list.iter().map(scalar_literal).collect::<Option<Vec<_>>>(),
            *negated,
        ),
        Expr::IsNull(expr) => match field_ref(base_schema, expr)? {
            QdrantFieldRef::Vector(name) => Some(QdrantFilterExpr::not(
                QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)),
            )),
            _ => None,
        },
        Expr::IsNotNull(expr) => match field_ref(base_schema, expr)? {
            QdrantFieldRef::Vector(name) => {
                Some(QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)))
            }
            _ => None,
        },
        Expr::Between(Between { expr, negated, low, high }) => {
            let QdrantFieldRef::Payload(field) = field_ref(base_schema, expr)? else {
                return None;
            };
            let field_type = payload_schema.field(field.key())?;
            let low = payload_scalar(field_type, scalar_literal(low)?)?;
            let high = payload_scalar(field_type, scalar_literal(high)?)?;
            let range = range_predicate(field, field_type, Some((low, true)), Some((high, true)))?;
            if *negated {
                Some(QdrantFilterExpr::not(QdrantFilterExpr::Predicate(range)))
            } else {
                Some(QdrantFilterExpr::Predicate(range))
            }
        }
        _ => None,
    }
}

fn exact_physical_expr(
    base_schema: &SchemaRef,
    payload_schema: &QdrantPayloadSchema,
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<QdrantFilterExpr> {
    if let Some(expr) = expr.as_any().downcast_ref::<NotExpr>() {
        return Some(QdrantFilterExpr::not(exact_physical_expr(
            base_schema,
            payload_schema,
            expr.arg(),
        )?));
    }
    if let Some(binary) = expr.as_any().downcast_ref::<PhysicalBinaryExpr>() {
        if *binary.op() == Operator::And {
            return Some(QdrantFilterExpr::and(
                split_physical_conjunction(expr)
                    .into_iter()
                    .map(|expr| exact_physical_expr(base_schema, payload_schema, expr))
                    .collect::<Option<Vec<_>>>()?,
            ));
        }
        if *binary.op() == Operator::Or {
            return Some(QdrantFilterExpr::or(
                split_disjunction(expr)
                    .into_iter()
                    .map(|expr| exact_physical_expr(base_schema, payload_schema, expr))
                    .collect::<Option<Vec<_>>>()?,
            ));
        }
        return filter_expr_from_refs(
            payload_schema,
            physical_field_ref(base_schema, binary.left()),
            physical_field_ref(base_schema, binary.right()),
            *binary.op(),
            physical_scalar_literal(binary.left()),
            physical_scalar_literal(binary.right()),
        );
    }
    if let Some(in_list) = expr.as_any().downcast_ref::<InListExpr>() {
        return in_list_expr_from_refs(
            payload_schema,
            physical_field_ref(base_schema, in_list.expr()),
            in_list.list().iter().map(physical_scalar_literal).collect::<Option<Vec<_>>>(),
            in_list.negated(),
        );
    }
    if let Some(expr) = expr.as_any().downcast_ref::<IsNullExpr>() {
        return match physical_field_ref(base_schema, expr.arg())? {
            QdrantFieldRef::Vector(name) => Some(QdrantFilterExpr::not(
                QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)),
            )),
            _ => None,
        };
    }
    if let Some(expr) = expr.as_any().downcast_ref::<IsNotNullExpr>() {
        return match physical_field_ref(base_schema, expr.arg())? {
            QdrantFieldRef::Vector(name) => {
                Some(QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)))
            }
            _ => None,
        };
    }
    None
}

fn filter_expr_from_refs(
    payload_schema: &QdrantPayloadSchema,
    left_field: Option<QdrantFieldRef>,
    right_field: Option<QdrantFieldRef>,
    op: Operator,
    left_literal: Option<&ScalarValue>,
    right_literal: Option<&ScalarValue>,
) -> Option<QdrantFilterExpr> {
    match (left_field, right_field) {
        (Some(field), None) => filter_expr(payload_schema, field, op, right_literal?),
        (None, Some(field)) => {
            filter_expr(payload_schema, field, reverse_operator(op)?, left_literal?)
        }
        _ => None,
    }
}

fn in_list_expr_from_refs(
    payload_schema: &QdrantPayloadSchema,
    field: Option<QdrantFieldRef>,
    values: Option<Vec<&ScalarValue>>,
    negated: bool,
) -> Option<QdrantFilterExpr> {
    let values = values?;
    if values.is_empty() {
        return None;
    }
    let predicate = match field? {
        QdrantFieldRef::Id => QdrantPredicate::IdIn(
            values.into_iter().map(point_id_scalar).collect::<Option<Vec<_>>>()?,
        ),
        QdrantFieldRef::Payload(field) => {
            let field_type = payload_schema.field(field.key())?;
            let values = values
                .into_iter()
                .map(|value| payload_scalar(field_type, value))
                .collect::<Option<Vec<_>>>()?;
            QdrantPredicate::PayloadIn { field, values }
        }
        QdrantFieldRef::Vector(_) => return None,
    };
    let expr = QdrantFilterExpr::Predicate(predicate);
    Some(if negated { QdrantFilterExpr::not(expr) } else { expr })
}

fn filter_expr(
    payload_schema: &QdrantPayloadSchema,
    field: QdrantFieldRef,
    op: Operator,
    literal: &ScalarValue,
) -> Option<QdrantFilterExpr> {
    match field {
        QdrantFieldRef::Id => {
            let expr =
                QdrantFilterExpr::Predicate(QdrantPredicate::IdIn(vec![point_id_scalar(literal)?]));
            match op {
                Operator::Eq => Some(expr),
                Operator::NotEq => Some(QdrantFilterExpr::not(expr)),
                _ => None,
            }
        }
        QdrantFieldRef::Payload(field) => {
            let field_type = payload_schema.field(field.key())?;
            let value = payload_scalar(field_type, literal)?;
            match op {
                Operator::Eq => {
                    Some(QdrantFilterExpr::Predicate(QdrantPredicate::PayloadEq { field, value }))
                }
                Operator::NotEq => Some(QdrantFilterExpr::not(QdrantFilterExpr::Predicate(
                    QdrantPredicate::PayloadEq { field, value },
                ))),
                Operator::Lt => Some(QdrantFilterExpr::Predicate(range_predicate(
                    field,
                    field_type,
                    None,
                    Some((value, false)),
                )?)),
                Operator::LtEq => Some(QdrantFilterExpr::Predicate(range_predicate(
                    field,
                    field_type,
                    None,
                    Some((value, true)),
                )?)),
                Operator::Gt => Some(QdrantFilterExpr::Predicate(range_predicate(
                    field,
                    field_type,
                    Some((value, false)),
                    None,
                )?)),
                Operator::GtEq => Some(QdrantFilterExpr::Predicate(range_predicate(
                    field,
                    field_type,
                    Some((value, true)),
                    None,
                )?)),
                _ => None,
            }
        }
        QdrantFieldRef::Vector(_) => None,
    }
}

fn range_predicate(
    field: QdrantPayloadPath,
    field_type: QdrantPayloadField,
    lower: Option<(QdrantFilterValue, bool)>,
    upper: Option<(QdrantFilterValue, bool)>,
) -> Option<QdrantPredicate> {
    match field_type {
        QdrantPayloadField::Integer { range: true }
        | QdrantPayloadField::Float
        | QdrantPayloadField::Datetime => {
            Some(QdrantPredicate::PayloadRange { field, lower, upper })
        }
        _ => None,
    }
}

fn disjunction_predicate(exprs: &[QdrantFilterExpr]) -> Option<QdrantPredicate> {
    match exprs {
        [QdrantFilterExpr::Predicate(QdrantPredicate::IdIn(_)), ..] => {
            let mut ids = vec![];
            for expr in exprs {
                let QdrantFilterExpr::Predicate(QdrantPredicate::IdIn(next_ids)) = expr else {
                    return None;
                };
                if next_ids.len() != 1 {
                    return None;
                }
                ids.push(next_ids[0].clone());
            }
            Some(QdrantPredicate::IdIn(ids))
        }
        [QdrantFilterExpr::Predicate(QdrantPredicate::PayloadEq { field, .. }), ..] => {
            let field = field.clone();
            let mut values = vec![];
            for expr in exprs {
                let QdrantFilterExpr::Predicate(QdrantPredicate::PayloadEq {
                    field: next_field,
                    value,
                }) = expr
                else {
                    return None;
                };
                if *next_field != field {
                    return None;
                }
                values.push(value.clone());
            }
            Some(QdrantPredicate::PayloadIn { field, values })
        }
        _ => None,
    }
}

fn field_ref(base_schema: &SchemaRef, expr: &Expr) -> Option<QdrantFieldRef> {
    match expr {
        Expr::Column(column) => column_field_ref(base_schema, &column.name),
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Colon, right: _ }) => {
            let Expr::Column(column) = left.as_ref() else {
                return None;
            };
            if column.name != PAYLOAD_FIELD_NAME {
                return None;
            }
            Some(QdrantFieldRef::Payload(logical_payload_path(expr)?))
        }
        Expr::Alias(alias) => field_ref(base_schema, &alias.expr),
        _ => None,
    }
}

fn physical_field_ref(
    base_schema: &SchemaRef,
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<QdrantFieldRef> {
    if let Some(column) = expr.as_any().downcast_ref::<PhysicalColumn>() {
        return column_field_ref(base_schema, column.name());
    }
    let binary = expr.as_any().downcast_ref::<PhysicalBinaryExpr>()?;
    if *binary.op() != Operator::Colon {
        return None;
    }
    let column = binary.left().as_any().downcast_ref::<PhysicalColumn>()?;
    if column.name() != PAYLOAD_FIELD_NAME {
        return None;
    }
    Some(QdrantFieldRef::Payload(QdrantPayloadPath::new(string_scalar(physical_scalar_literal(
        binary.right(),
    )?)?)?))
}

fn column_field_ref(base_schema: &SchemaRef, name: &str) -> Option<QdrantFieldRef> {
    if name == ID_FIELD_NAME {
        return Some(QdrantFieldRef::Id);
    }
    let field = base_schema.field_with_name(name).ok()?;
    if name == UNNAMED_VECTOR_FIELD_NAME {
        return None;
    }
    if dense_vector_width(field).is_some()
        || is_multi_vector_field(field)
        || is_sparse_vector_field(field)
    {
        return Some(QdrantFieldRef::Vector(name.to_owned()));
    }
    None
}

fn reverse_operator(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::NotEq => Some(Operator::NotEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        _ => None,
    }
}

fn scalar_literal(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value),
        Expr::Alias(alias) => scalar_literal(&alias.expr),
        _ => None,
    }
}

fn physical_scalar_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<&ScalarValue> {
    expr.as_any().downcast_ref::<PhysicalLiteral>().map(PhysicalLiteral::value)
}

fn point_id_scalar(value: &ScalarValue) -> Option<PointId> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => value
            .parse::<u64>()
            .ok()
            .map(PointId::from)
            .or_else(|| Some(PointId::from(value.clone()))),
        ScalarValue::UInt64(Some(value)) => Some(PointId::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(PointId::from(u64::from(*value))),
        ScalarValue::UInt16(Some(value)) => Some(PointId::from(u64::from(*value))),
        ScalarValue::UInt8(Some(value)) => Some(PointId::from(u64::from(*value))),
        ScalarValue::Int64(Some(value)) => u64::try_from(*value).ok().map(PointId::from),
        ScalarValue::Int32(Some(value)) => u64::try_from(*value).ok().map(PointId::from),
        ScalarValue::Int16(Some(value)) => u64::try_from(*value).ok().map(PointId::from),
        ScalarValue::Int8(Some(value)) => u64::try_from(*value).ok().map(PointId::from),
        _ => None,
    }
}

fn payload_scalar(
    field_type: QdrantPayloadField,
    literal: &ScalarValue,
) -> Option<QdrantFilterValue> {
    match field_type {
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

fn string_scalar(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

fn integer_scalar(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(Some(value)) => Some(*value),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value).ok(),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => value.parse().ok(),
        _ => None,
    }
}

fn float_scalar(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float64(Some(value)) => Some(*value),
        ScalarValue::Float32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => value.parse().ok(),
        _ => integer_scalar(value).map(integer_to_f64),
    }
}

fn boolean_scalar(value: &ScalarValue) -> Option<bool> {
    match value {
        ScalarValue::Boolean(Some(value)) => Some(*value),
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => value.parse().ok(),
        _ => None,
    }
}

fn timestamp_scalar(value: &ScalarValue) -> Option<Timestamp> {
    match value {
        ScalarValue::TimestampSecond(Some(value), _) => Some(timestamp_from_scaled(*value, 1)),
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            Some(timestamp_from_scaled(*value, 1_000))
        }
        ScalarValue::TimestampMicrosecond(Some(value), _) => {
            Some(timestamp_from_scaled(*value, 1_000_000))
        }
        ScalarValue::TimestampNanosecond(Some(value), _) => {
            Some(timestamp_from_scaled(*value, 1_000_000_000))
        }
        ScalarValue::Date64(Some(value)) => Some(timestamp_from_scaled(*value, 1_000)),
        ScalarValue::Date32(Some(value)) => {
            Some(timestamp_from_scaled(i64::from(*value) * 86_400, 1))
        }
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            timestamp_from_string(value)
        }
        _ => None,
    }
}

fn timestamp_from_scaled(value: i64, scale: i64) -> Timestamp {
    let seconds = value.div_euclid(scale);
    let nanos = value.rem_euclid(scale) * (1_000_000_000 / scale);
    Timestamp { seconds, nanos: i32::try_from(nanos).expect("nanos fit in i32") }
}

fn nanos_i32(nanos: u32) -> i32 { i32::try_from(nanos).expect("nanos fit in i32") }

fn timestamp_from_string(value: &str) -> Option<Timestamp> {
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        let value = value.with_timezone(&Utc);
        return Some(Timestamp {
            seconds: value.timestamp(),
            nanos:   nanos_i32(value.timestamp_subsec_nanos()),
        });
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(Timestamp {
            seconds: value.and_utc().timestamp(),
            nanos:   nanos_i32(value.and_utc().timestamp_subsec_nanos()),
        });
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(Timestamp {
            seconds: value.and_utc().timestamp(),
            nanos:   nanos_i32(value.and_utc().timestamp_subsec_nanos()),
        });
    }
    let value = NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()?;
    let value = value.and_hms_opt(0, 0, 0)?;
    Some(Timestamp { seconds: value.and_utc().timestamp(), nanos: 0 })
}

fn eq_condition(field: &QdrantPayloadPath, value: &QdrantFilterValue) -> Condition {
    match value {
        QdrantFilterValue::String(value) => Condition::matches(field.key(), value.clone()),
        QdrantFilterValue::Integer(value) => Condition::matches(field.key(), *value),
        QdrantFilterValue::Bool(value) => Condition::matches(field.key(), *value),
        QdrantFilterValue::Float(value) => Condition::range(field.key(), Range {
            gte: Some(*value),
            lte: Some(*value),
            ..Default::default()
        }),
        QdrantFilterValue::Datetime(value) => {
            Condition::datetime_range(field.key(), DatetimeRange {
                gte: Some(*value),
                lte: Some(*value),
                ..Default::default()
            })
        }
    }
}

fn in_condition(field: &QdrantPayloadPath, values: &[QdrantFilterValue]) -> Condition {
    match values {
        [] => unreachable!("empty IN list is not admitted"),
        [QdrantFilterValue::String(_), ..] => Condition::matches(
            field.key(),
            values
                .iter()
                .map(|value| match value {
                    QdrantFilterValue::String(value) => value.clone(),
                    _ => unreachable!("validated homogeneous IN list"),
                })
                .collect::<Vec<_>>(),
        ),
        [QdrantFilterValue::Integer(_), ..] => Condition::matches(
            field.key(),
            values
                .iter()
                .map(|value| match value {
                    QdrantFilterValue::Integer(value) => *value,
                    _ => unreachable!("validated homogeneous IN list"),
                })
                .collect::<Vec<_>>(),
        ),
        _ => Filter::should(values.iter().map(|value| eq_condition(field, value))).into(),
    }
}

fn range_condition(
    field: &QdrantPayloadPath,
    lower: Option<&(QdrantFilterValue, bool)>,
    upper: Option<&(QdrantFilterValue, bool)>,
) -> Condition {
    if let Some(QdrantFilterValue::Datetime(_)) = lower.or(upper).map(|(value, _)| value) {
        let mut range = DatetimeRange::default();
        if let Some((QdrantFilterValue::Datetime(value), inclusive)) = lower {
            if *inclusive {
                range.gte = Some(*value);
            } else {
                range.gt = Some(*value);
            }
        }
        if let Some((QdrantFilterValue::Datetime(value), inclusive)) = upper {
            if *inclusive {
                range.lte = Some(*value);
            } else {
                range.lt = Some(*value);
            }
        }
        return Condition::datetime_range(field.key(), range);
    }
    let mut range = Range::default();
    if let Some((value, inclusive)) = lower {
        let Some(value) = range_bound(value) else {
            unreachable!("validated range lower bound");
        };
        if *inclusive {
            range.gte = Some(value);
        } else {
            range.gt = Some(value);
        }
    }
    if let Some((value, inclusive)) = upper {
        let Some(value) = range_bound(value) else {
            unreachable!("validated range upper bound");
        };
        if *inclusive {
            range.lte = Some(value);
        } else {
            range.lt = Some(value);
        }
    }
    Condition::range(field.key(), range)
}

#[allow(clippy::cast_precision_loss)]
fn integer_to_f64(value: i64) -> f64 { value as f64 }

fn range_bound(value: &QdrantFilterValue) -> Option<f64> {
    match value {
        QdrantFilterValue::Integer(value) => Some(integer_to_f64(*value)),
        QdrantFilterValue::Float(value) => Some(*value),
        _ => None,
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
        BinaryExpr as PhysicalBinaryExpr, Column as PhysicalColumn, Literal as PhysicalLiteral,
        NotExpr,
    };
    use qdrant_client::qdrant::{
        IntegerIndexParams, KeywordIndexParams, PayloadSchemaInfo, PayloadSchemaType,
        payload_index_params,
    };

    use super::*;

    fn schema(fields: Vec<Field>) -> SchemaRef { Arc::new(Schema::new(fields)) }

    fn payload_schema() -> QdrantPayloadSchema {
        QdrantPayloadSchema::from(HashMap::from([
            ("rank".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Integer as i32,
                params:    Some(qdrant_client::qdrant::PayloadIndexParams {
                    index_params: Some(payload_index_params::IndexParams::IntegerIndexParams(
                        IntegerIndexParams { range: Some(true), ..Default::default() },
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
        let schema = schema(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new("image", DataType::new_fixed_size_list(DataType::Float32, 3, false), true),
        ]);

        assert!(QdrantFilters::supports_exact(
            &schema,
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
            &schema,
            &QdrantPayloadSchema::default(),
            &Expr::IsNull(Box::new(Expr::Column(Column::from_name("image")))),
        ));
        assert!(QdrantFilters::supports_exact(
            &schema,
            &QdrantPayloadSchema::default(),
            &Expr::Not(Box::new(Expr::IsNotNull(Box::new(Expr::Column(Column::from_name(
                "image",
            )))))),
        ));
        assert!(!QdrantFilters::supports_exact(
            &schema,
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
            QdrantFilters::default().pushdown_physical(&schema, &payload_schema, &[filter]);

        assert_eq!(support, vec![true]);
        assert_eq!(filters.len(), 2);
        let filter = filters.to_filter().expect("qdrant filter");
        assert_eq!(filter.must_not.len(), 1);
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
            QdrantFilters::default().pushdown_physical(&schema, &payload_schema(), &[filter]);

        assert_eq!(support, vec![false]);
    }
}
