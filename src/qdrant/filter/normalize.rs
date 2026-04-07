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

use super::value::{float_scalar, point_id_scalar, string_scalar};
use super::{QdrantFieldRef, QdrantFilterExpr, QdrantPayloadSchema, QdrantPredicate};
use crate::arrow::schema::{ID_FIELD_NAME, QdrantFieldBinding, field_uses_unnamed_vector_contract};
use crate::expr_fn::{
    PAYLOAD_GEO_DISTANCE_ACCESS_FUNCTION_NAME, PAYLOAD_GEO_WITHIN_BBOX_ACCESS_FUNCTION_NAME,
    PAYLOAD_GEO_WITHIN_POLYGON_ACCESS_FUNCTION_NAME, PAYLOAD_IS_EMPTY_ACCESS_FUNCTION_NAME,
    PAYLOAD_NESTED_MATCH_FUNCTION_NAME, PAYLOAD_PHRASE_MATCH_ACCESS_FUNCTION_NAME,
    PAYLOAD_TEXT_ANY_ACCESS_FUNCTION_NAME, PAYLOAD_TEXT_MATCH_ACCESS_FUNCTION_NAME,
    PAYLOAD_VALUES_COUNT_ACCESS_FUNCTION_NAME, canonical_geo_polygon,
    is_payload_geo_distance_function_name, is_payload_geo_within_bbox_function_name,
    is_payload_geo_within_polygon_function_name, is_payload_is_empty_function_name,
    is_payload_nested_match_function_name, is_payload_phrase_match_function_name,
    is_payload_text_any_function_name, is_payload_text_match_function_name,
    is_payload_values_count_function_name, payload_text_any_query_string,
};
use crate::qdrant::{QdrantPayloadAccess, QdrantPayloadPath};

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

    fn field_for_path(
        &self,
        field: &QdrantPayloadPath,
        nested_base: Option<&QdrantPayloadPath>,
    ) -> Option<crate::qdrant::QdrantPayloadField> {
        scoped_field_type(self.payload_schema, nested_base, field)
    }

    fn exact_expr(&self, expr: &Expr) -> Option<QdrantFilterExpr> {
        self.exact_logical_expr(expr, None)
    }

    #[expect(clippy::too_many_lines)]
    fn exact_logical_expr(
        &self,
        expr: &Expr,
        nested_base: Option<&QdrantPayloadPath>,
    ) -> Option<QdrantFilterExpr> {
        if let Some(field) = unary_payload_logical_path(
            self.payload_schema,
            nested_base,
            expr,
            is_payload_is_empty_function_name,
            PAYLOAD_IS_EMPTY_ACCESS_FUNCTION_NAME,
        ) {
            return Some(QdrantFilterExpr::Predicate(QdrantPredicate::PayloadIsEmpty(field)));
        }
        if let Some(predicate) =
            payload_geo_predicate_logical(self.payload_schema, nested_base, expr)
        {
            return Some(QdrantFilterExpr::Predicate(predicate));
        }
        if let Some(predicate) =
            payload_text_predicate_logical(self.payload_schema, nested_base, expr)
        {
            return Some(QdrantFilterExpr::Predicate(predicate));
        }
        if nested_base.is_none()
            && let Some(predicate) = self.payload_nested_predicate_logical(expr)
        {
            return Some(QdrantFilterExpr::Predicate(predicate));
        }
        match expr {
            Expr::Alias(alias) => self.exact_logical_expr(&alias.expr, nested_base),
            Expr::Not(expr) => {
                Some(QdrantFilterExpr::not(self.exact_logical_expr(expr, nested_base)?))
            }
            Expr::BinaryExpr(BinaryExpr { op: Operator::And, .. }) => Some(QdrantFilterExpr::and(
                split_conjunction(expr)
                    .into_iter()
                    .map(|expr| self.exact_logical_expr(expr, nested_base))
                    .collect::<Option<Vec<_>>>()?,
            )),
            Expr::BinaryExpr(BinaryExpr { op: Operator::Or, .. }) => Some(QdrantFilterExpr::or(
                split_binary(expr, Operator::Or)
                    .into_iter()
                    .map(|expr| self.exact_logical_expr(expr, nested_base))
                    .collect::<Option<Vec<_>>>()?,
            )),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => self.filter_expr_from_refs(
                QdrantFieldRef::from_logical_expr(
                    self.base_schema,
                    self.payload_schema,
                    nested_base,
                    left,
                ),
                QdrantFieldRef::from_logical_expr(
                    self.base_schema,
                    self.payload_schema,
                    nested_base,
                    right,
                ),
                nested_base,
                *op,
                Self::logical_scalar_literal(left),
                Self::logical_scalar_literal(right),
            ),
            Expr::InList(InList { expr, list, negated }) => self.in_list_expr_from_refs(
                QdrantFieldRef::from_logical_expr(
                    self.base_schema,
                    self.payload_schema,
                    nested_base,
                    expr,
                ),
                nested_base,
                list.iter().map(Self::logical_scalar_literal).collect::<Option<Vec<_>>>(),
                *negated,
            ),
            Expr::IsNull(expr) => {
                match QdrantFieldRef::from_logical_expr(
                    self.base_schema,
                    self.payload_schema,
                    nested_base,
                    expr,
                )? {
                    QdrantFieldRef::Vector(name) => Some(QdrantFilterExpr::not(
                        QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)),
                    )),
                    QdrantFieldRef::Payload(field) => Some(field.sql_null_filter_expr()),
                    QdrantFieldRef::PayloadValuesCount(_)
                    | QdrantFieldRef::PayloadGeoDistance { .. }
                    | QdrantFieldRef::Id => None,
                }
            }
            Expr::IsNotNull(expr) => {
                match QdrantFieldRef::from_logical_expr(
                    self.base_schema,
                    self.payload_schema,
                    nested_base,
                    expr,
                )? {
                    QdrantFieldRef::Vector(name) => {
                        Some(QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)))
                    }
                    QdrantFieldRef::Payload(field) => {
                        Some(QdrantFilterExpr::not(field.sql_null_filter_expr()))
                    }
                    QdrantFieldRef::PayloadValuesCount(_)
                    | QdrantFieldRef::PayloadGeoDistance { .. }
                    | QdrantFieldRef::Id => None,
                }
            }
            Expr::Between(Between { expr, negated, low, high }) => {
                match QdrantFieldRef::from_logical_expr(
                    self.base_schema,
                    self.payload_schema,
                    nested_base,
                    expr,
                )? {
                    QdrantFieldRef::Payload(field) => {
                        let field_type = self.field_for_path(&field, nested_base)?;
                        let low =
                            field_type.into_filter_value(Self::logical_scalar_literal(low)?)?;
                        let high =
                            field_type.into_filter_value(Self::logical_scalar_literal(high)?)?;
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
                    QdrantFieldRef::PayloadValuesCount(field) => {
                        let low = values_count_scalar(Self::logical_scalar_literal(low)?)?;
                        let high = values_count_scalar(Self::logical_scalar_literal(high)?)?;
                        let range = QdrantPredicate::PayloadValuesCount {
                            field,
                            lower: Some((low, true)),
                            upper: Some((high, true)),
                        };
                        if *negated {
                            Some(QdrantFilterExpr::not(QdrantFilterExpr::Predicate(range)))
                        } else {
                            Some(QdrantFilterExpr::Predicate(range))
                        }
                    }
                    QdrantFieldRef::PayloadGeoDistance { field, lon, lat } => {
                        if self.field_for_path(&field, nested_base)?
                            != crate::qdrant::QdrantPayloadField::Geo
                        {
                            return None;
                        }
                        let low = float_scalar(Self::logical_scalar_literal(low)?)?;
                        if *negated || !low.is_finite() || low != 0.0 {
                            return None;
                        }
                        let radius = geo_radius_scalar(Self::logical_scalar_literal(high)?)?;
                        Some(QdrantFilterExpr::Predicate(QdrantPredicate::PayloadGeoRadius {
                            field,
                            lon,
                            lat,
                            radius,
                        }))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn exact_physical_expr(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<QdrantFilterExpr> {
        self.exact_physical_expr_in_scope(expr, None)
    }

    #[expect(clippy::too_many_lines)]
    fn exact_physical_expr_in_scope(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        nested_base: Option<&QdrantPayloadPath>,
    ) -> Option<QdrantFilterExpr> {
        if let Some(field) = unary_payload_physical_path(
            self.payload_schema,
            nested_base,
            expr,
            PAYLOAD_IS_EMPTY_ACCESS_FUNCTION_NAME,
        ) {
            return Some(QdrantFilterExpr::Predicate(QdrantPredicate::PayloadIsEmpty(field)));
        }
        if let Some(predicate) =
            payload_geo_predicate_physical(self.payload_schema, nested_base, expr)
        {
            return Some(QdrantFilterExpr::Predicate(predicate));
        }
        if let Some(predicate) =
            payload_text_predicate_physical(self.payload_schema, nested_base, expr)
        {
            return Some(QdrantFilterExpr::Predicate(predicate));
        }
        if nested_base.is_none()
            && let Some(predicate) = self.payload_nested_predicate_physical(expr)
        {
            return Some(QdrantFilterExpr::Predicate(predicate));
        }
        if let Some(expr) = expr.as_any().downcast_ref::<NotExpr>() {
            return Some(QdrantFilterExpr::not(
                self.exact_physical_expr_in_scope(expr.arg(), nested_base)?,
            ));
        }
        if let Some(binary) = expr.as_any().downcast_ref::<PhysicalBinaryExpr>() {
            if *binary.op() == Operator::And {
                return Some(QdrantFilterExpr::and(
                    split_physical_conjunction(expr)
                        .into_iter()
                        .map(|expr| self.exact_physical_expr_in_scope(expr, nested_base))
                        .collect::<Option<Vec<_>>>()?,
                ));
            }
            if *binary.op() == Operator::Or {
                return Some(QdrantFilterExpr::or(
                    split_disjunction(expr)
                        .into_iter()
                        .map(|expr| self.exact_physical_expr_in_scope(expr, nested_base))
                        .collect::<Option<Vec<_>>>()?,
                ));
            }
            return self.filter_expr_from_refs(
                QdrantFieldRef::from_physical_expr(
                    self.base_schema,
                    self.payload_schema,
                    nested_base,
                    binary.left(),
                ),
                QdrantFieldRef::from_physical_expr(
                    self.base_schema,
                    self.payload_schema,
                    nested_base,
                    binary.right(),
                ),
                nested_base,
                *binary.op(),
                Self::physical_scalar_literal(binary.left()),
                Self::physical_scalar_literal(binary.right()),
            );
        }
        if let Some(in_list) = expr.as_any().downcast_ref::<InListExpr>() {
            return self.in_list_expr_from_refs(
                QdrantFieldRef::from_physical_expr(
                    self.base_schema,
                    self.payload_schema,
                    nested_base,
                    in_list.expr(),
                ),
                nested_base,
                in_list
                    .list()
                    .iter()
                    .map(Self::physical_scalar_literal)
                    .collect::<Option<Vec<_>>>(),
                in_list.negated(),
            );
        }
        if let Some(expr) = expr.as_any().downcast_ref::<IsNullExpr>() {
            return match QdrantFieldRef::from_physical_expr(
                self.base_schema,
                self.payload_schema,
                nested_base,
                expr.arg(),
            )? {
                QdrantFieldRef::Vector(name) => Some(QdrantFilterExpr::not(
                    QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)),
                )),
                QdrantFieldRef::Payload(field) => Some(field.sql_null_filter_expr()),
                QdrantFieldRef::PayloadValuesCount(_)
                | QdrantFieldRef::PayloadGeoDistance { .. }
                | QdrantFieldRef::Id => None,
            };
        }
        if let Some(expr) = expr.as_any().downcast_ref::<IsNotNullExpr>() {
            return match QdrantFieldRef::from_physical_expr(
                self.base_schema,
                self.payload_schema,
                nested_base,
                expr.arg(),
            )? {
                QdrantFieldRef::Vector(name) => {
                    Some(QdrantFilterExpr::Predicate(QdrantPredicate::HasVector(name)))
                }
                QdrantFieldRef::Payload(field) => {
                    Some(QdrantFilterExpr::not(field.sql_null_filter_expr()))
                }
                QdrantFieldRef::PayloadValuesCount(_)
                | QdrantFieldRef::PayloadGeoDistance { .. }
                | QdrantFieldRef::Id => None,
            };
        }
        None
    }

    fn filter_expr_from_refs(
        &self,
        left_field: Option<QdrantFieldRef>,
        right_field: Option<QdrantFieldRef>,
        nested_base: Option<&QdrantPayloadPath>,
        op: Operator,
        left_literal: Option<&ScalarValue>,
        right_literal: Option<&ScalarValue>,
    ) -> Option<QdrantFilterExpr> {
        match (left_field, right_field) {
            (Some(field), None) => self.filter_expr(field, nested_base, op, right_literal?),
            (None, Some(field)) => {
                self.filter_expr(field, nested_base, reverse_operator(op)?, left_literal?)
            }
            _ => None,
        }
    }

    fn in_list_expr_from_refs(
        &self,
        field: Option<QdrantFieldRef>,
        nested_base: Option<&QdrantPayloadPath>,
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
                let field_type = self.field_for_path(&field, nested_base)?;
                if !field_type.supports_equality() {
                    return None;
                }
                let values = values
                    .into_iter()
                    .map(|value| field_type.into_filter_value(value))
                    .collect::<Option<Vec<_>>>()?;
                QdrantPredicate::PayloadIn { field, values }
            }
            QdrantFieldRef::PayloadValuesCount(_)
            | QdrantFieldRef::PayloadGeoDistance { .. }
            | QdrantFieldRef::Vector(_) => return None,
        };
        let expr = QdrantFilterExpr::Predicate(predicate);
        Some(if negated { QdrantFilterExpr::not(expr) } else { expr })
    }

    #[expect(clippy::too_many_lines)]
    fn filter_expr(
        &self,
        field: QdrantFieldRef,
        nested_base: Option<&QdrantPayloadPath>,
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
                let field_type = self.field_for_path(&field, nested_base)?;
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
            QdrantFieldRef::PayloadValuesCount(field) => {
                let value = values_count_scalar(literal)?;
                let predicate = match op {
                    Operator::Eq => QdrantPredicate::PayloadValuesCount {
                        field,
                        lower: Some((value, true)),
                        upper: Some((value, true)),
                    },
                    Operator::NotEq => {
                        return Some(QdrantFilterExpr::not(QdrantFilterExpr::Predicate(
                            QdrantPredicate::PayloadValuesCount {
                                field,
                                lower: Some((value, true)),
                                upper: Some((value, true)),
                            },
                        )));
                    }
                    Operator::Lt => QdrantPredicate::PayloadValuesCount {
                        field,
                        lower: None,
                        upper: Some((value, false)),
                    },
                    Operator::LtEq => QdrantPredicate::PayloadValuesCount {
                        field,
                        lower: None,
                        upper: Some((value, true)),
                    },
                    Operator::Gt => QdrantPredicate::PayloadValuesCount {
                        field,
                        lower: Some((value, false)),
                        upper: None,
                    },
                    Operator::GtEq => QdrantPredicate::PayloadValuesCount {
                        field,
                        lower: Some((value, true)),
                        upper: None,
                    },
                    _ => return None,
                };
                Some(QdrantFilterExpr::Predicate(predicate))
            }
            QdrantFieldRef::PayloadGeoDistance { field, lon, lat } => {
                if self.field_for_path(&field, nested_base)?
                    != crate::qdrant::QdrantPayloadField::Geo
                {
                    return None;
                }
                let radius = geo_radius_scalar(literal)?;
                match op {
                    Operator::LtEq => {
                        Some(QdrantFilterExpr::Predicate(QdrantPredicate::PayloadGeoRadius {
                            field,
                            lon,
                            lat,
                            radius,
                        }))
                    }
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

    fn payload_nested_predicate_logical(&self, expr: &Expr) -> Option<QdrantPredicate> {
        match expr {
            Expr::Alias(alias) => self.payload_nested_predicate_logical(&alias.expr),
            Expr::ScalarFunction(function)
                if is_payload_nested_match_function_name(function.name()) =>
            {
                let [accessor, predicate] = function.args.as_slice() else {
                    return None;
                };
                let field = QdrantPayloadAccess::from_logical_expr(accessor)?.path().clone();
                let filter = self.exact_logical_expr(predicate, Some(&field))?;
                filter
                    .supports_nested()
                    .then_some(QdrantPredicate::PayloadNested { field, filter: Box::new(filter) })
            }
            _ => None,
        }
    }

    fn payload_nested_predicate_physical(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<QdrantPredicate> {
        let function =
            expr.as_any().downcast_ref::<datafusion::physical_expr::ScalarFunctionExpr>()?;
        if function.name() != PAYLOAD_NESTED_MATCH_FUNCTION_NAME {
            return None;
        }
        let [accessor, predicate] = function.args() else {
            return None;
        };
        let field = scoped_payload_physical_path(self.payload_schema, None, accessor)?;
        let filter = self.exact_physical_expr_in_scope(predicate, Some(&field))?;
        filter
            .supports_nested()
            .then_some(QdrantPredicate::PayloadNested { field, filter: Box::new(filter) })
    }
}

impl QdrantFieldRef {
    fn from_logical_expr(
        base_schema: &SchemaRef,
        payload_schema: &QdrantPayloadSchema,
        nested_base: Option<&QdrantPayloadPath>,
        expr: &Expr,
    ) -> Option<Self> {
        if let Some(path) = unary_payload_logical_path(
            payload_schema,
            nested_base,
            expr,
            is_payload_values_count_function_name,
            PAYLOAD_VALUES_COUNT_ACCESS_FUNCTION_NAME,
        ) {
            return Some(Self::PayloadValuesCount(path));
        }
        if let Some((field, lon, lat)) =
            payload_geo_distance_logical(payload_schema, nested_base, expr)
        {
            return Some(Self::PayloadGeoDistance { field, lon, lat });
        }
        if let Some(path) = scoped_payload_logical_path(payload_schema, nested_base, expr) {
            return Some(Self::Payload(path));
        }
        match expr {
            Expr::Column(column) => Self::from_column_name(base_schema, &column.name),
            Expr::Alias(alias) => {
                Self::from_logical_expr(base_schema, payload_schema, nested_base, &alias.expr)
            }
            _ => None,
        }
    }

    fn from_physical_expr(
        base_schema: &SchemaRef,
        payload_schema: &QdrantPayloadSchema,
        nested_base: Option<&QdrantPayloadPath>,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<Self> {
        if let Some(path) = unary_payload_physical_path(
            payload_schema,
            nested_base,
            expr,
            PAYLOAD_VALUES_COUNT_ACCESS_FUNCTION_NAME,
        ) {
            return Some(Self::PayloadValuesCount(path));
        }
        if let Some((field, lon, lat)) =
            payload_geo_distance_physical(payload_schema, nested_base, expr)
        {
            return Some(Self::PayloadGeoDistance { field, lon, lat });
        }
        if let Some(path) = scoped_payload_physical_path(payload_schema, nested_base, expr) {
            return Some(Self::Payload(path));
        }
        let column = expr.as_any().downcast_ref::<PhysicalColumn>()?;
        Self::from_column_name(base_schema, column.name())
    }

    fn from_column_name(base_schema: &SchemaRef, name: &str) -> Option<Self> {
        if name == ID_FIELD_NAME {
            return Some(Self::Id);
        }
        let field = base_schema.field_with_name(name).ok()?;
        if field_uses_unnamed_vector_contract(base_schema.as_ref(), field) {
            return None;
        }
        if QdrantFieldBinding::from_field(field).is_vector() {
            return Some(Self::Vector(name.to_owned()));
        }
        None
    }
}

fn unary_payload_logical_path(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    expr: &Expr,
    public_name: impl Fn(&str) -> bool,
    internal_name: &str,
) -> Option<QdrantPayloadPath> {
    match expr {
        Expr::Alias(alias) => unary_payload_logical_path(
            payload_schema,
            nested_base,
            &alias.expr,
            public_name,
            internal_name,
        ),
        Expr::ScalarFunction(function) if public_name(function.name()) => {
            let [accessor] = function.args.as_slice() else {
                return None;
            };
            scoped_payload_logical_path(payload_schema, nested_base, accessor)
        }
        Expr::ScalarFunction(function) if function.name() == internal_name => {
            let [payload, path] = function.args.as_slice() else {
                return None;
            };
            scope_payload_path(
                payload_schema,
                nested_base,
                QdrantPayloadAccess::from_logical_parts(payload, path)?.path(),
            )
        }
        _ => None,
    }
}

fn unary_payload_physical_path(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    expr: &Arc<dyn PhysicalExpr>,
    internal_name: &str,
) -> Option<QdrantPayloadPath> {
    let function = expr.as_any().downcast_ref::<datafusion::physical_expr::ScalarFunctionExpr>()?;
    if function.name() != internal_name {
        return None;
    }
    let [payload, path] = function.args() else {
        return None;
    };
    scope_payload_path(
        payload_schema,
        nested_base,
        QdrantPayloadAccess::from_physical_parts(payload, path)?.path(),
    )
}

fn payload_geo_distance_logical(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    expr: &Expr,
) -> Option<(QdrantPayloadPath, f64, f64)> {
    match expr {
        Expr::Alias(alias) => {
            payload_geo_distance_logical(payload_schema, nested_base, &alias.expr)
        }
        Expr::ScalarFunction(function)
            if is_payload_geo_distance_function_name(function.name()) =>
        {
            let [accessor, lon, lat] = function.args.as_slice() else {
                return None;
            };
            Some((
                scoped_payload_logical_path(payload_schema, nested_base, accessor)?,
                float_scalar(logical_scalar_literal_value(lon)?)?,
                float_scalar(logical_scalar_literal_value(lat)?)?,
            ))
        }
        Expr::ScalarFunction(function)
            if function.name() == PAYLOAD_GEO_DISTANCE_ACCESS_FUNCTION_NAME =>
        {
            let [payload, path, lon, lat] = function.args.as_slice() else {
                return None;
            };
            Some((
                scope_payload_path(
                    payload_schema,
                    nested_base,
                    QdrantPayloadAccess::from_logical_parts(payload, path)?.path(),
                )?,
                float_scalar(logical_scalar_literal_value(lon)?)?,
                float_scalar(logical_scalar_literal_value(lat)?)?,
            ))
        }
        _ => None,
    }
}

fn payload_geo_distance_physical(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<(QdrantPayloadPath, f64, f64)> {
    let function = expr.as_any().downcast_ref::<datafusion::physical_expr::ScalarFunctionExpr>()?;
    if function.name() != PAYLOAD_GEO_DISTANCE_ACCESS_FUNCTION_NAME {
        return None;
    }
    let [payload, path, lon, lat] = function.args() else {
        return None;
    };
    Some((
        scope_payload_path(
            payload_schema,
            nested_base,
            QdrantPayloadAccess::from_physical_parts(payload, path)?.path(),
        )?,
        float_scalar(physical_scalar_literal_value(lon)?)?,
        float_scalar(physical_scalar_literal_value(lat)?)?,
    ))
}

fn payload_geo_predicate_logical(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    expr: &Expr,
) -> Option<QdrantPredicate> {
    match expr {
        Expr::Alias(alias) => {
            payload_geo_predicate_logical(payload_schema, nested_base, &alias.expr)
        }
        Expr::ScalarFunction(function)
            if is_payload_geo_within_bbox_function_name(function.name()) =>
        {
            let [accessor, lon1, lat1, lon2, lat2] = function.args.as_slice() else {
                return None;
            };
            let field = scoped_payload_logical_path(payload_schema, nested_base, accessor)?;
            if scoped_field_type(payload_schema, nested_base, &field)?
                != crate::qdrant::QdrantPayloadField::Geo
            {
                return None;
            }
            let (west, south, east, north) = logical_bbox_bounds(lon1, lat1, lon2, lat2)?;
            Some(QdrantPredicate::PayloadGeoBoundingBox { field, west, south, east, north })
        }
        Expr::ScalarFunction(function)
            if function.name() == PAYLOAD_GEO_WITHIN_BBOX_ACCESS_FUNCTION_NAME =>
        {
            let [payload, path, lon1, lat1, lon2, lat2] = function.args.as_slice() else {
                return None;
            };
            let field = scope_payload_path(
                payload_schema,
                nested_base,
                QdrantPayloadAccess::from_logical_parts(payload, path)?.path(),
            )?;
            if scoped_field_type(payload_schema, nested_base, &field)?
                != crate::qdrant::QdrantPayloadField::Geo
            {
                return None;
            }
            let (west, south, east, north) = logical_bbox_bounds(lon1, lat1, lon2, lat2)?;
            Some(QdrantPredicate::PayloadGeoBoundingBox { field, west, south, east, north })
        }
        Expr::ScalarFunction(function)
            if is_payload_geo_within_polygon_function_name(function.name()) =>
        {
            let [accessor, points] = function.args.as_slice() else {
                return None;
            };
            let field = scoped_payload_logical_path(payload_schema, nested_base, accessor)?;
            if scoped_field_type(payload_schema, nested_base, &field)?
                != crate::qdrant::QdrantPayloadField::Geo
            {
                return None;
            }
            Some(QdrantPredicate::PayloadGeoPolygon {
                field,
                points: canonical_geo_polygon(
                    logical_scalar_literal_value(points)?,
                    function.name(),
                    "points",
                )
                .ok()?,
            })
        }
        Expr::ScalarFunction(function)
            if function.name() == PAYLOAD_GEO_WITHIN_POLYGON_ACCESS_FUNCTION_NAME =>
        {
            let [payload, path, points] = function.args.as_slice() else {
                return None;
            };
            let field = scope_payload_path(
                payload_schema,
                nested_base,
                QdrantPayloadAccess::from_logical_parts(payload, path)?.path(),
            )?;
            if scoped_field_type(payload_schema, nested_base, &field)?
                != crate::qdrant::QdrantPayloadField::Geo
            {
                return None;
            }
            Some(QdrantPredicate::PayloadGeoPolygon {
                field,
                points: canonical_geo_polygon(
                    logical_scalar_literal_value(points)?,
                    PAYLOAD_GEO_WITHIN_POLYGON_ACCESS_FUNCTION_NAME,
                    "points",
                )
                .ok()?,
            })
        }
        _ => None,
    }
}

fn payload_geo_predicate_physical(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<QdrantPredicate> {
    let function = expr.as_any().downcast_ref::<datafusion::physical_expr::ScalarFunctionExpr>()?;
    match function.name() {
        PAYLOAD_GEO_WITHIN_BBOX_ACCESS_FUNCTION_NAME => {
            let [payload, path, lon1, lat1, lon2, lat2] = function.args() else {
                return None;
            };
            let field = scope_payload_path(
                payload_schema,
                nested_base,
                QdrantPayloadAccess::from_physical_parts(payload, path)?.path(),
            )?;
            if scoped_field_type(payload_schema, nested_base, &field)?
                != crate::qdrant::QdrantPayloadField::Geo
            {
                return None;
            }
            let (west, south, east, north) = physical_bbox_bounds(lon1, lat1, lon2, lat2)?;
            Some(QdrantPredicate::PayloadGeoBoundingBox { field, west, south, east, north })
        }
        PAYLOAD_GEO_WITHIN_POLYGON_ACCESS_FUNCTION_NAME => {
            let [payload, path, points] = function.args() else {
                return None;
            };
            let field = scope_payload_path(
                payload_schema,
                nested_base,
                QdrantPayloadAccess::from_physical_parts(payload, path)?.path(),
            )?;
            if scoped_field_type(payload_schema, nested_base, &field)?
                != crate::qdrant::QdrantPayloadField::Geo
            {
                return None;
            }
            Some(QdrantPredicate::PayloadGeoPolygon {
                field,
                points: canonical_geo_polygon(
                    physical_scalar_literal_value(points)?,
                    PAYLOAD_GEO_WITHIN_POLYGON_ACCESS_FUNCTION_NAME,
                    "points",
                )
                .ok()?,
            })
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PayloadTextPredicateKind {
    Match,
    Any,
    Phrase,
}

impl PayloadTextPredicateKind {
    fn supported(self, field_type: crate::qdrant::QdrantPayloadField) -> bool {
        match self {
            Self::Match | Self::Any => field_type.supports_text_match(),
            Self::Phrase => field_type.supports_phrase_match(),
        }
    }

    fn build(self, field: QdrantPayloadPath, query: String) -> QdrantPredicate {
        match self {
            Self::Match => QdrantPredicate::PayloadTextMatch { field, query },
            Self::Any => QdrantPredicate::PayloadTextAny { field, query },
            Self::Phrase => QdrantPredicate::PayloadPhraseMatch { field, phrase: query },
        }
    }
}

fn text_predicate_from_query(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    kind: PayloadTextPredicateKind,
    field: QdrantPayloadPath,
    query: String,
) -> Option<QdrantPredicate> {
    let field_type = scoped_field_type(payload_schema, nested_base, &field)?;
    kind.supported(field_type).then(|| kind.build(field, query))
}

#[expect(clippy::too_many_lines)]
fn payload_text_predicate_logical(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    expr: &Expr,
) -> Option<QdrantPredicate> {
    match expr {
        Expr::Alias(alias) => {
            payload_text_predicate_logical(payload_schema, nested_base, &alias.expr)
        }
        Expr::ScalarFunction(function) if is_payload_text_match_function_name(function.name()) => {
            let [accessor, query] = function.args.as_slice() else {
                return None;
            };
            text_predicate_from_query(
                payload_schema,
                nested_base,
                PayloadTextPredicateKind::Match,
                scoped_payload_logical_path(payload_schema, nested_base, accessor)?,
                string_scalar(logical_scalar_literal_value(query)?)?,
            )
        }
        Expr::ScalarFunction(function) if is_payload_text_any_function_name(function.name()) => {
            let [accessor, terms] = function.args.as_slice() else {
                return None;
            };
            text_predicate_from_query(
                payload_schema,
                nested_base,
                PayloadTextPredicateKind::Any,
                scoped_payload_logical_path(payload_schema, nested_base, accessor)?,
                payload_text_any_query_string(
                    logical_scalar_literal_value(terms)?,
                    function.name(),
                    "terms",
                )
                .ok()?,
            )
        }
        Expr::ScalarFunction(function)
            if is_payload_phrase_match_function_name(function.name()) =>
        {
            let [accessor, phrase] = function.args.as_slice() else {
                return None;
            };
            text_predicate_from_query(
                payload_schema,
                nested_base,
                PayloadTextPredicateKind::Phrase,
                scoped_payload_logical_path(payload_schema, nested_base, accessor)?,
                string_scalar(logical_scalar_literal_value(phrase)?)?,
            )
        }
        Expr::ScalarFunction(function)
            if function.name() == PAYLOAD_TEXT_MATCH_ACCESS_FUNCTION_NAME =>
        {
            let [payload, path, query] = function.args.as_slice() else {
                return None;
            };
            text_predicate_from_query(
                payload_schema,
                nested_base,
                PayloadTextPredicateKind::Match,
                scope_payload_path(
                    payload_schema,
                    nested_base,
                    QdrantPayloadAccess::from_logical_parts(payload, path)?.path(),
                )?,
                string_scalar(logical_scalar_literal_value(query)?)?,
            )
        }
        Expr::ScalarFunction(function)
            if function.name() == PAYLOAD_TEXT_ANY_ACCESS_FUNCTION_NAME =>
        {
            let [payload, path, terms] = function.args.as_slice() else {
                return None;
            };
            text_predicate_from_query(
                payload_schema,
                nested_base,
                PayloadTextPredicateKind::Any,
                scope_payload_path(
                    payload_schema,
                    nested_base,
                    QdrantPayloadAccess::from_logical_parts(payload, path)?.path(),
                )?,
                payload_text_any_query_string(
                    logical_scalar_literal_value(terms)?,
                    PAYLOAD_TEXT_ANY_ACCESS_FUNCTION_NAME,
                    "terms",
                )
                .ok()?,
            )
        }
        Expr::ScalarFunction(function)
            if function.name() == PAYLOAD_PHRASE_MATCH_ACCESS_FUNCTION_NAME =>
        {
            let [payload, path, phrase] = function.args.as_slice() else {
                return None;
            };
            text_predicate_from_query(
                payload_schema,
                nested_base,
                PayloadTextPredicateKind::Phrase,
                scope_payload_path(
                    payload_schema,
                    nested_base,
                    QdrantPayloadAccess::from_logical_parts(payload, path)?.path(),
                )?,
                string_scalar(logical_scalar_literal_value(phrase)?)?,
            )
        }
        _ => None,
    }
}

fn payload_text_predicate_physical(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<QdrantPredicate> {
    let function = expr.as_any().downcast_ref::<datafusion::physical_expr::ScalarFunctionExpr>()?;
    match function.name() {
        PAYLOAD_TEXT_MATCH_ACCESS_FUNCTION_NAME => {
            let [payload, path, query] = function.args() else {
                return None;
            };
            text_predicate_from_query(
                payload_schema,
                nested_base,
                PayloadTextPredicateKind::Match,
                scope_payload_path(
                    payload_schema,
                    nested_base,
                    QdrantPayloadAccess::from_physical_parts(payload, path)?.path(),
                )?,
                string_scalar(physical_scalar_literal_value(query)?)?,
            )
        }
        PAYLOAD_TEXT_ANY_ACCESS_FUNCTION_NAME => {
            let [payload, path, terms] = function.args() else {
                return None;
            };
            text_predicate_from_query(
                payload_schema,
                nested_base,
                PayloadTextPredicateKind::Any,
                scope_payload_path(
                    payload_schema,
                    nested_base,
                    QdrantPayloadAccess::from_physical_parts(payload, path)?.path(),
                )?,
                payload_text_any_query_string(
                    physical_scalar_literal_value(terms)?,
                    PAYLOAD_TEXT_ANY_ACCESS_FUNCTION_NAME,
                    "terms",
                )
                .ok()?,
            )
        }
        PAYLOAD_PHRASE_MATCH_ACCESS_FUNCTION_NAME => {
            let [payload, path, phrase] = function.args() else {
                return None;
            };
            text_predicate_from_query(
                payload_schema,
                nested_base,
                PayloadTextPredicateKind::Phrase,
                scope_payload_path(
                    payload_schema,
                    nested_base,
                    QdrantPayloadAccess::from_physical_parts(payload, path)?.path(),
                )?,
                string_scalar(physical_scalar_literal_value(phrase)?)?,
            )
        }
        _ => None,
    }
}

fn scoped_payload_logical_path(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    expr: &Expr,
) -> Option<QdrantPayloadPath> {
    let path = if nested_base.is_some() {
        QdrantPayloadPath::from_logical_expr(expr)
    } else {
        payload_schema.path_for_logical_expr(expr)
    }?;
    scope_payload_path(payload_schema, nested_base, &path)
}

fn scoped_payload_physical_path(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<QdrantPayloadPath> {
    let path = if nested_base.is_some() {
        QdrantPayloadPath::from_physical_expr(expr)
    } else {
        payload_schema.path_for_physical_expr(expr)
    }?;
    scope_payload_path(payload_schema, nested_base, &path)
}

fn scope_payload_path(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    path: &QdrantPayloadPath,
) -> Option<QdrantPayloadPath> {
    let Some(base) = nested_base else {
        return Some(path.clone());
    };
    if let Some(relative) = path.strip_prefix(base.key()) {
        return Some(relative);
    }
    let relative = path.clone();
    payload_schema
        .field_for_path(&format!("{}.{}", base.key(), relative.key()))
        .or_else(|| payload_schema.field_for_path(relative.key()))
        .map(|_| relative)
}

fn scoped_field_type(
    payload_schema: &QdrantPayloadSchema,
    nested_base: Option<&QdrantPayloadPath>,
    field: &QdrantPayloadPath,
) -> Option<crate::qdrant::QdrantPayloadField> {
    if let Some(base) = nested_base {
        return payload_schema
            .field_for_path(&format!("{}.{}", base.key(), field.key()))
            .or_else(|| payload_schema.field_for_path(field.key()));
    }
    payload_schema.field_for_path(field.key())
}

fn logical_scalar_literal_value(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value),
        Expr::Alias(alias) => logical_scalar_literal_value(&alias.expr),
        _ => None,
    }
}

fn physical_scalar_literal_value(expr: &Arc<dyn PhysicalExpr>) -> Option<&ScalarValue> {
    expr.as_any().downcast_ref::<PhysicalLiteral>().map(PhysicalLiteral::value)
}

fn geo_radius_scalar(value: &ScalarValue) -> Option<f64> {
    let value = float_scalar(value)?;
    (value.is_finite() && value >= 0.0).then_some(value)
}

fn logical_bbox_bounds(
    lon1: &Expr,
    lat1: &Expr,
    lon2: &Expr,
    lat2: &Expr,
) -> Option<(f64, f64, f64, f64)> {
    normalized_bbox_bounds(
        float_scalar(logical_scalar_literal_value(lon1)?)?,
        float_scalar(logical_scalar_literal_value(lat1)?)?,
        float_scalar(logical_scalar_literal_value(lon2)?)?,
        float_scalar(logical_scalar_literal_value(lat2)?)?,
    )
}

fn physical_bbox_bounds(
    lon1: &Arc<dyn PhysicalExpr>,
    lat1: &Arc<dyn PhysicalExpr>,
    lon2: &Arc<dyn PhysicalExpr>,
    lat2: &Arc<dyn PhysicalExpr>,
) -> Option<(f64, f64, f64, f64)> {
    normalized_bbox_bounds(
        float_scalar(physical_scalar_literal_value(lon1)?)?,
        float_scalar(physical_scalar_literal_value(lat1)?)?,
        float_scalar(physical_scalar_literal_value(lon2)?)?,
        float_scalar(physical_scalar_literal_value(lat2)?)?,
    )
}

fn normalized_bbox_bounds(
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
) -> Option<(f64, f64, f64, f64)> {
    (lon1.is_finite() && lat1.is_finite() && lon2.is_finite() && lat2.is_finite()).then_some((
        lon1.min(lon2),
        lat1.min(lat2),
        lon1.max(lon2),
        lat1.max(lat2),
    ))
}

fn values_count_scalar(value: &ScalarValue) -> Option<u64> {
    let value = super::value::integer_scalar(value)?;
    u64::try_from(value).ok()
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
