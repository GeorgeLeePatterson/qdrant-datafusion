use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
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

use super::value::{point_id_scalar, string_scalar};
use super::{
    QdrantFieldRef, QdrantFilterExpr, QdrantPayloadPath, QdrantPayloadSchema, QdrantPredicate,
};
use crate::arrow::schema::{
    ID_FIELD_NAME, PAYLOAD_FIELD_NAME, QdrantFieldBinding, UNNAMED_VECTOR_FIELD_NAME,
};

pub(super) fn exact_expr(
    base_schema: &SchemaRef,
    payload_schema: &QdrantPayloadSchema,
    expr: &Expr,
) -> Option<QdrantFilterExpr> {
    QdrantExprNormalizer::new(base_schema, payload_schema).exact_expr(expr)
}

pub(super) fn exact_physical_expr(
    base_schema: &SchemaRef,
    payload_schema: &QdrantPayloadSchema,
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<QdrantFilterExpr> {
    QdrantExprNormalizer::new(base_schema, payload_schema).exact_physical_expr(expr)
}

struct QdrantExprNormalizer<'a> {
    base_schema:    &'a SchemaRef,
    payload_schema: &'a QdrantPayloadSchema,
}

impl<'a> QdrantExprNormalizer<'a> {
    fn new(base_schema: &'a SchemaRef, payload_schema: &'a QdrantPayloadSchema) -> Self {
        Self { base_schema, payload_schema }
    }

    fn exact_expr(&self, expr: &Expr) -> Option<QdrantFilterExpr> {
        match expr {
            Expr::Alias(alias) => self.exact_expr(&alias.expr),
            Expr::Not(expr) => Some(QdrantFilterExpr::not(self.exact_expr(expr)?)),
            Expr::BinaryExpr(BinaryExpr { op: Operator::And, .. }) => Some(QdrantFilterExpr::and(
                split_conjunction(expr)
                    .into_iter()
                    .map(|expr| self.exact_expr(expr))
                    .collect::<Option<Vec<_>>>()?,
            )),
            Expr::BinaryExpr(BinaryExpr { op: Operator::Or, .. }) => Some(QdrantFilterExpr::or(
                split_binary(expr, Operator::Or)
                    .into_iter()
                    .map(|expr| self.exact_expr(expr))
                    .collect::<Option<Vec<_>>>()?,
            )),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => self.filter_expr_from_refs(
                QdrantFieldRef::from_logical_expr(self.base_schema, left),
                QdrantFieldRef::from_logical_expr(self.base_schema, right),
                *op,
                Self::logical_scalar_literal(left),
                Self::logical_scalar_literal(right),
            ),
            Expr::InList(InList { expr, list, negated }) => self.in_list_expr_from_refs(
                QdrantFieldRef::from_logical_expr(self.base_schema, expr),
                list.iter().map(Self::logical_scalar_literal).collect::<Option<Vec<_>>>(),
                *negated,
            ),
            Expr::IsNull(expr) => {
                match QdrantFieldRef::from_logical_expr(self.base_schema, expr)? {
                    QdrantFieldRef::Vector(name) => Some(QdrantFilterExpr::not(
                        QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)),
                    )),
                    QdrantFieldRef::Payload(field) => Some(field.sql_null_filter_expr()),
                    QdrantFieldRef::Id => None,
                }
            }
            Expr::IsNotNull(expr) => {
                match QdrantFieldRef::from_logical_expr(self.base_schema, expr)? {
                    QdrantFieldRef::Vector(name) => {
                        Some(QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)))
                    }
                    QdrantFieldRef::Payload(field) => {
                        Some(QdrantFilterExpr::not(field.sql_null_filter_expr()))
                    }
                    QdrantFieldRef::Id => None,
                }
            }
            Expr::Between(Between { expr, negated, low, high }) => {
                let QdrantFieldRef::Payload(field) =
                    QdrantFieldRef::from_logical_expr(self.base_schema, expr)?
                else {
                    return None;
                };
                let field_type = self.payload_schema.field(field.key())?;
                let low = field_type.into_filter_value(Self::logical_scalar_literal(low)?)?;
                let high = field_type.into_filter_value(Self::logical_scalar_literal(high)?)?;
                let range = field_type.into_range_predicate(
                    field,
                    Some((low, true)),
                    Some((high, true)),
                )?;
                if *negated {
                    Some(QdrantFilterExpr::not(QdrantFilterExpr::Predicate(range)))
                } else {
                    Some(QdrantFilterExpr::Predicate(range))
                }
            }
            _ => None,
        }
    }

    fn exact_physical_expr(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<QdrantFilterExpr> {
        if let Some(expr) = expr.as_any().downcast_ref::<NotExpr>() {
            return Some(QdrantFilterExpr::not(self.exact_physical_expr(expr.arg())?));
        }
        if let Some(binary) = expr.as_any().downcast_ref::<PhysicalBinaryExpr>() {
            if *binary.op() == Operator::And {
                return Some(QdrantFilterExpr::and(
                    split_physical_conjunction(expr)
                        .into_iter()
                        .map(|expr| self.exact_physical_expr(expr))
                        .collect::<Option<Vec<_>>>()?,
                ));
            }
            if *binary.op() == Operator::Or {
                return Some(QdrantFilterExpr::or(
                    split_disjunction(expr)
                        .into_iter()
                        .map(|expr| self.exact_physical_expr(expr))
                        .collect::<Option<Vec<_>>>()?,
                ));
            }
            return self.filter_expr_from_refs(
                QdrantFieldRef::from_physical_expr(self.base_schema, binary.left()),
                QdrantFieldRef::from_physical_expr(self.base_schema, binary.right()),
                *binary.op(),
                Self::physical_scalar_literal(binary.left()),
                Self::physical_scalar_literal(binary.right()),
            );
        }
        if let Some(in_list) = expr.as_any().downcast_ref::<InListExpr>() {
            return self.in_list_expr_from_refs(
                QdrantFieldRef::from_physical_expr(self.base_schema, in_list.expr()),
                in_list
                    .list()
                    .iter()
                    .map(Self::physical_scalar_literal)
                    .collect::<Option<Vec<_>>>(),
                in_list.negated(),
            );
        }
        if let Some(expr) = expr.as_any().downcast_ref::<IsNullExpr>() {
            return match QdrantFieldRef::from_physical_expr(self.base_schema, expr.arg())? {
                QdrantFieldRef::Vector(name) => Some(QdrantFilterExpr::not(
                    QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)),
                )),
                QdrantFieldRef::Payload(field) => Some(field.sql_null_filter_expr()),
                QdrantFieldRef::Id => None,
            };
        }
        if let Some(expr) = expr.as_any().downcast_ref::<IsNotNullExpr>() {
            return match QdrantFieldRef::from_physical_expr(self.base_schema, expr.arg())? {
                QdrantFieldRef::Vector(name) => {
                    Some(QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)))
                }
                QdrantFieldRef::Payload(field) => {
                    Some(QdrantFilterExpr::not(field.sql_null_filter_expr()))
                }
                QdrantFieldRef::Id => None,
            };
        }
        None
    }

    fn filter_expr_from_refs(
        &self,
        left_field: Option<QdrantFieldRef>,
        right_field: Option<QdrantFieldRef>,
        op: Operator,
        left_literal: Option<&ScalarValue>,
        right_literal: Option<&ScalarValue>,
    ) -> Option<QdrantFilterExpr> {
        match (left_field, right_field) {
            (Some(field), None) => self.filter_expr(field, op, right_literal?),
            (None, Some(field)) => self.filter_expr(field, reverse_operator(op)?, left_literal?),
            _ => None,
        }
    }

    fn in_list_expr_from_refs(
        &self,
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
                let field_type = self.payload_schema.field(field.key())?;
                if !field_type.supports_equality() {
                    return None;
                }
                let values = values
                    .into_iter()
                    .map(|value| field_type.into_filter_value(value))
                    .collect::<Option<Vec<_>>>()?;
                QdrantPredicate::PayloadIn { field, values }
            }
            QdrantFieldRef::Vector(_) => return None,
        };
        let expr = QdrantFilterExpr::Predicate(predicate);
        Some(if negated { QdrantFilterExpr::not(expr) } else { expr })
    }

    fn filter_expr(
        &self,
        field: QdrantFieldRef,
        op: Operator,
        literal: &ScalarValue,
    ) -> Option<QdrantFilterExpr> {
        match field {
            QdrantFieldRef::Id => {
                let expr =
                    QdrantFilterExpr::Predicate(QdrantPredicate::IdIn(vec![point_id_scalar(
                        literal,
                    )?]));
                match op {
                    Operator::Eq => Some(expr),
                    Operator::NotEq => Some(QdrantFilterExpr::not(expr)),
                    _ => None,
                }
            }
            QdrantFieldRef::Payload(field) => {
                let field_type = self.payload_schema.field(field.key())?;
                let value = field_type.into_filter_value(literal)?;
                match op {
                    Operator::Eq if field_type.supports_equality() => {
                        Some(QdrantFilterExpr::Predicate(QdrantPredicate::PayloadEq {
                            field,
                            value,
                        }))
                    }
                    Operator::NotEq if field_type.supports_equality() => {
                        Some(QdrantFilterExpr::not(QdrantFilterExpr::Predicate(
                            QdrantPredicate::PayloadEq { field, value },
                        )))
                    }
                    Operator::Lt => Some(QdrantFilterExpr::Predicate(
                        field_type.into_range_predicate(field, None, Some((value, false)))?,
                    )),
                    Operator::LtEq => Some(QdrantFilterExpr::Predicate(
                        field_type.into_range_predicate(field, None, Some((value, true)))?,
                    )),
                    Operator::Gt => Some(QdrantFilterExpr::Predicate(
                        field_type.into_range_predicate(field, Some((value, false)), None)?,
                    )),
                    Operator::GtEq => Some(QdrantFilterExpr::Predicate(
                        field_type.into_range_predicate(field, Some((value, true)), None)?,
                    )),
                    _ => None,
                }
            }
            QdrantFieldRef::Vector(_) => None,
        }
    }

    fn logical_scalar_literal(expr: &Expr) -> Option<&ScalarValue> {
        match expr {
            Expr::Literal(value, _) => Some(value),
            Expr::Alias(alias) => Self::logical_scalar_literal(&alias.expr),
            _ => None,
        }
    }

    fn physical_scalar_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<&ScalarValue> {
        expr.as_any().downcast_ref::<PhysicalLiteral>().map(PhysicalLiteral::value)
    }
}

impl QdrantFieldRef {
    fn from_logical_expr(base_schema: &SchemaRef, expr: &Expr) -> Option<Self> {
        match expr {
            Expr::Column(column) => Self::from_column_name(base_schema, &column.name),
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Colon, .. }) => {
                let Expr::Column(column) = left.as_ref() else {
                    return None;
                };
                if column.name != PAYLOAD_FIELD_NAME {
                    return None;
                }
                Some(Self::Payload(QdrantPayloadPath::from_logical_expr(expr)?))
            }
            Expr::Alias(alias) => Self::from_logical_expr(base_schema, &alias.expr),
            _ => None,
        }
    }

    fn from_physical_expr(base_schema: &SchemaRef, expr: &Arc<dyn PhysicalExpr>) -> Option<Self> {
        if let Some(column) = expr.as_any().downcast_ref::<PhysicalColumn>() {
            return Self::from_column_name(base_schema, column.name());
        }
        let binary = expr.as_any().downcast_ref::<PhysicalBinaryExpr>()?;
        if *binary.op() != Operator::Colon {
            return None;
        }
        let column = binary.left().as_any().downcast_ref::<PhysicalColumn>()?;
        if column.name() != PAYLOAD_FIELD_NAME {
            return None;
        }
        Some(Self::Payload(QdrantPayloadPath::new(string_scalar(
            QdrantExprNormalizer::physical_scalar_literal(binary.right())?,
        )?)?))
    }

    fn from_column_name(base_schema: &SchemaRef, name: &str) -> Option<Self> {
        if name == ID_FIELD_NAME {
            return Some(Self::Id);
        }
        let field = base_schema.field_with_name(name).ok()?;
        if name == UNNAMED_VECTOR_FIELD_NAME {
            return None;
        }
        if QdrantFieldBinding::from_field(field).is_vector() {
            return Some(Self::Vector(name.to_owned()));
        }
        None
    }
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
