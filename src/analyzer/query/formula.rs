use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Column, Result, ScalarValue, plan_err};
use datafusion::logical_expr::expr::BinaryExpr;
use datafusion::logical_expr::{Expr, Operator};
use qdrant_client::qdrant::{
    Condition, DecayParamsExpression, DivExpression, Expression, Formula, GeoDistance, GeoPoint,
    MultExpression, PowExpression, Query, SumExpression, Value, expression,
};

use super::super::source::Source;
use super::{QueryDescriptor, QueryPrefetchBranch, f32_literal, scalar_f32, string_literal};
use crate::arrow::schema::PAYLOAD_FIELD_NAME;
use crate::expr_fn::{
    CONDITION_FUNCTION_NAME, ConditionCall, DATETIME_VALUE_FUNCTION_NAME, DatetimeValueCall,
    DecayCall, DecayKind, FORMULA_SCORE_FUNCTION_NAME, FormulaCall, GEO_DISTANCE_FUNCTION_NAME,
    GeoDistanceCall, PAYLOAD_DATETIME_FUNCTION_NAME, PAYLOAD_NUM_FUNCTION_NAME,
    PayloadDatetimeCall, PayloadNumCall,
};
use crate::qdrant::filter::QdrantFilters;
use crate::qdrant::{QdrantPayloadField, QdrantPayloadPath};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FormulaFunction {
    Abs,
    Sqrt,
    Pow,
    Exp,
    Log10,
    Ln,
}

impl TryFrom<&str> for FormulaFunction {
    type Error = datafusion::error::DataFusionError;

    fn try_from(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "abs" => Ok(Self::Abs),
            "sqrt" => Ok(Self::Sqrt),
            "pow" | "power" => Ok(Self::Pow),
            "exp" => Ok(Self::Exp),
            "log10" => Ok(Self::Log10),
            "ln" => Ok(Self::Ln),
            _ => plan_err!(
                "{FORMULA_SCORE_FUNCTION_NAME} does not support scalar function '{}'",
                value
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DecayExpr {
    x:        Box<FormulaExpr>,
    target:   Option<Box<FormulaExpr>>,
    scale:    f32,
    midpoint: Option<f32>,
}

impl DecayExpr {
    fn validate_on_source(&self, source: &Source, function_name: &str) -> Result<()> {
        self.x.validate_on_source(source)?;
        if let Some(target) = &self.target {
            target.validate_on_source(source)?;
        }
        if !(self.scale.is_finite() && self.scale > 0.0) {
            return plan_err!("{function_name} requires scale to be a positive finite number");
        }
        if let Some(midpoint) = self.midpoint
            && (!(midpoint.is_finite()) || midpoint <= 0.0 || midpoint >= 1.0)
        {
            return plan_err!(
                "{function_name} requires midpoint to be a finite number strictly between 0 and 1"
            );
        }
        Ok(())
    }

    fn into_proto(
        self,
        source: &Source,
        prefetch: &[QueryPrefetchBranch],
    ) -> Result<DecayParamsExpression> {
        Ok(DecayParamsExpression {
            x:        self.x.into_proto(source, prefetch).map(Box::new).map(Some)?,
            target:   self
                .target
                .map(|target| target.into_proto(source, prefetch).map(Box::new))
                .transpose()?,
            scale:    Some(self.scale),
            midpoint: self.midpoint,
        })
    }

    fn collect_defaults(&self, defaults: &mut HashMap<String, Value>) -> Result<()> {
        self.x.collect_defaults(defaults)?;
        if let Some(target) = &self.target {
            target.collect_defaults(defaults)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
enum FormulaExpr {
    Constant(f32),
    Column(Column),
    PayloadNum { path: String, default: Option<Value> },
    PayloadDatetime { path: String, default: Option<Value> },
    Datetime(String),
    Condition(Expr),
    GeoDistance { origin: GeoPoint, to: String },
    Sum(Vec<FormulaExpr>),
    Mult(Vec<FormulaExpr>),
    Div(Box<FormulaExpr>, Box<FormulaExpr>),
    Neg(Box<FormulaExpr>),
    Abs(Box<FormulaExpr>),
    Sqrt(Box<FormulaExpr>),
    Pow(Box<FormulaExpr>, Box<FormulaExpr>),
    Exp(Box<FormulaExpr>),
    Log10(Box<FormulaExpr>),
    Ln(Box<FormulaExpr>),
    ExpDecay(DecayExpr),
    GaussDecay(DecayExpr),
    LinDecay(DecayExpr),
}

impl FormulaExpr {
    fn from_expr(expr: &Expr) -> Result<Self> {
        if let Some(call) = PayloadNumCall::from_expr(expr)? {
            return Self::from_payload_num_call(&call);
        }
        if let Some(call) = PayloadDatetimeCall::from_expr(expr)? {
            return Self::from_payload_datetime_call(&call);
        }
        if let Some(call) = DatetimeValueCall::from_expr(expr)? {
            return Ok(Self::Datetime(datetime_literal(&call.value)?));
        }
        if let Some(call) = ConditionCall::from_expr(expr)? {
            return Ok(Self::Condition(rewrite_condition_expr(call.predicate)?));
        }
        if let Some(call) = GeoDistanceCall::from_expr(expr)? {
            return Ok(Self::GeoDistance {
                origin: GeoPoint {
                    lon: f64_literal(&call.lon, GEO_DISTANCE_FUNCTION_NAME, "longitude")?,
                    lat: f64_literal(&call.lat, GEO_DISTANCE_FUNCTION_NAME, "latitude")?,
                },
                to:     payload_path_arg(&call.path, GEO_DISTANCE_FUNCTION_NAME, "payload path")?,
            });
        }
        if let Some(call) = DecayCall::from_expr(expr)? {
            return Self::from_decay_call(&call);
        }
        if let Some(path) = QdrantPayloadPath::from_logical_expr(expr) {
            return Ok(Self::PayloadNum { path: path.key().to_owned(), default: None });
        }
        match expr.clone().unalias_nested().data {
            Expr::Alias(alias) => Self::from_expr(&alias.expr),
            Expr::Cast(cast) => Self::from_expr(&cast.expr),
            Expr::TryCast(cast) => Self::from_expr(&cast.expr),
            Expr::Column(column) => Ok(Self::Column(column)),
            Expr::Literal(value, _) => Self::from_scalar(&value),
            Expr::Negative(expr) => Ok(Self::Neg(Box::new(Self::from_expr(&expr)?))),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                Self::from_binary_expr(left.as_ref(), op, right.as_ref())
            }
            Expr::ScalarFunction(function) => {
                Self::from_scalar_function(function.name(), &function.args)
            }
            _ => {
                plan_err!(
                    "{FORMULA_SCORE_FUNCTION_NAME} only admits numeric literals, resolved \
                     columns,                  qdrant formula leaves, arithmetic operators, and \
                     supported scalar functions"
                )
            }
        }
    }

    fn from_payload_num_call(call: &PayloadNumCall) -> Result<Self> {
        Ok(Self::PayloadNum {
            path:    payload_path_arg(&call.path, PAYLOAD_NUM_FUNCTION_NAME, "payload path")?,
            default: call
                .default
                .as_ref()
                .map(|expr| numeric_default_value(expr, PAYLOAD_NUM_FUNCTION_NAME))
                .transpose()?,
        })
    }

    fn from_payload_datetime_call(call: &PayloadDatetimeCall) -> Result<Self> {
        Ok(Self::PayloadDatetime {
            path:    payload_path_arg(&call.path, PAYLOAD_DATETIME_FUNCTION_NAME, "payload path")?,
            default: call
                .default
                .as_ref()
                .map(|expr| datetime_default_value(expr, PAYLOAD_DATETIME_FUNCTION_NAME))
                .transpose()?,
        })
    }

    fn from_decay_call(call: &DecayCall) -> Result<Self> {
        let decay = DecayExpr {
            x:        Box::new(Self::from_expr(&call.x)?),
            target:   call.target.as_ref().map(Self::from_expr).transpose()?.map(Box::new),
            scale:    f32_literal(&call.scale, call.kind.function_name(), "scale")?,
            midpoint: call
                .midpoint
                .as_ref()
                .map(|expr| f32_literal(expr, call.kind.function_name(), "midpoint"))
                .transpose()?,
        };
        Ok(match call.kind {
            DecayKind::Exp => Self::ExpDecay(decay),
            DecayKind::Gauss => Self::GaussDecay(decay),
            DecayKind::Lin => Self::LinDecay(decay),
        })
    }

    fn from_scalar(value: &ScalarValue) -> Result<Self> {
        scalar_f32(value, FORMULA_SCORE_FUNCTION_NAME, "formula literal").map(Self::Constant)
    }

    fn from_binary_expr(left: &Expr, op: Operator, right: &Expr) -> Result<Self> {
        let left = Self::from_expr(left)?;
        let right = Self::from_expr(right)?;
        match op {
            Operator::Plus => Ok(Self::sum(vec![left, right])),
            Operator::Minus => Ok(Self::sum(vec![left, Self::Neg(Box::new(right))])),
            Operator::Multiply => Ok(Self::mult(vec![left, right])),
            Operator::Divide => Ok(Self::Div(Box::new(left), Box::new(right))),
            _ => plan_err!(
                "{FORMULA_SCORE_FUNCTION_NAME} does not support formula operator {:?}",
                op
            ),
        }
    }

    fn from_scalar_function(name: &str, args: &[Expr]) -> Result<Self> {
        match FormulaFunction::try_from(name)? {
            FormulaFunction::Abs => unary(args).map(|expr| Self::Abs(Box::new(expr))),
            FormulaFunction::Sqrt => unary(args).map(|expr| Self::Sqrt(Box::new(expr))),
            FormulaFunction::Pow => {
                binary(args).map(|(lhs, rhs)| Self::Pow(Box::new(lhs), Box::new(rhs)))
            }
            FormulaFunction::Exp => unary(args).map(|expr| Self::Exp(Box::new(expr))),
            FormulaFunction::Log10 => unary(args).map(|expr| Self::Log10(Box::new(expr))),
            FormulaFunction::Ln => unary(args).map(|expr| Self::Ln(Box::new(expr))),
        }
    }

    fn sum(values: Vec<Self>) -> Self {
        let mut sum = vec![];
        for value in values {
            match value {
                Self::Sum(values) => sum.extend(values),
                value => sum.push(value),
            }
        }
        Self::Sum(sum)
    }

    fn mult(values: Vec<Self>) -> Self {
        let mut mult = vec![];
        for value in values {
            match value {
                Self::Mult(values) => mult.extend(values),
                value => mult.push(value),
            }
        }
        Self::Mult(mult)
    }

    fn validate_on_source(&self, source: &Source) -> Result<()> {
        match self {
            Self::Constant(_) | Self::Column(_) | Self::Datetime(_) => Ok(()),
            Self::PayloadNum { path, .. } => validate_payload_num(path, source),
            Self::PayloadDatetime { path, .. } => validate_payload_datetime(path, source),
            Self::Condition(predicate) => {
                drop(exact_condition_expr(predicate, source, CONDITION_FUNCTION_NAME)?);
                Ok(())
            }
            Self::GeoDistance { to, .. } => validate_geo_distance(to, source),
            Self::Sum(values) | Self::Mult(values) => {
                for value in values {
                    value.validate_on_source(source)?;
                }
                Ok(())
            }
            Self::Div(lhs, rhs) | Self::Pow(lhs, rhs) => {
                lhs.validate_on_source(source)?;
                rhs.validate_on_source(source)
            }
            Self::Neg(expr)
            | Self::Abs(expr)
            | Self::Sqrt(expr)
            | Self::Exp(expr)
            | Self::Log10(expr)
            | Self::Ln(expr) => expr.validate_on_source(source),
            Self::ExpDecay(decay) => {
                decay.validate_on_source(source, crate::expr_fn::EXP_DECAY_FUNCTION_NAME)
            }
            Self::GaussDecay(decay) => {
                decay.validate_on_source(source, crate::expr_fn::GAUSS_DECAY_FUNCTION_NAME)
            }
            Self::LinDecay(decay) => {
                decay.validate_on_source(source, crate::expr_fn::LIN_DECAY_FUNCTION_NAME)
            }
        }
    }

    fn into_proto(self, source: &Source, prefetch: &[QueryPrefetchBranch]) -> Result<Expression> {
        let variant = match self {
            Self::Constant(value) => expression::Variant::Constant(value),
            Self::Column(column) => {
                expression::Variant::Variable(resolve_column(&column, source, prefetch)?)
            }
            Self::PayloadNum { path, .. } | Self::PayloadDatetime { path, .. } => {
                expression::Variant::Variable(path)
            }
            Self::Datetime(value) => expression::Variant::Datetime(value),
            Self::Condition(predicate) => expression::Variant::Condition(exact_condition_expr(
                &predicate,
                source,
                CONDITION_FUNCTION_NAME,
            )?),
            Self::GeoDistance { origin, to } => {
                expression::Variant::GeoDistance(GeoDistance { origin: Some(origin), to })
            }
            Self::Sum(sum) => expression::Variant::Sum(SumExpression {
                sum: sum
                    .into_iter()
                    .map(|expr| expr.into_proto(source, prefetch))
                    .collect::<Result<Vec<_>>>()?,
            }),
            Self::Mult(mult) => expression::Variant::Mult(MultExpression {
                mult: mult
                    .into_iter()
                    .map(|expr| expr.into_proto(source, prefetch))
                    .collect::<Result<Vec<_>>>()?,
            }),
            Self::Div(lhs, rhs) => expression::Variant::Div(Box::new(DivExpression {
                left:            Some(Box::new(lhs.into_proto(source, prefetch)?)),
                right:           Some(Box::new(rhs.into_proto(source, prefetch)?)),
                by_zero_default: None,
            })),
            Self::Neg(expr) => {
                expression::Variant::Neg(Box::new(expr.into_proto(source, prefetch)?))
            }
            Self::Abs(expr) => {
                expression::Variant::Abs(Box::new(expr.into_proto(source, prefetch)?))
            }
            Self::Sqrt(expr) => {
                expression::Variant::Sqrt(Box::new(expr.into_proto(source, prefetch)?))
            }
            Self::Pow(base, exponent) => expression::Variant::Pow(Box::new(PowExpression {
                base:     Some(Box::new(base.into_proto(source, prefetch)?)),
                exponent: Some(Box::new(exponent.into_proto(source, prefetch)?)),
            })),
            Self::Exp(expr) => {
                expression::Variant::Exp(Box::new(expr.into_proto(source, prefetch)?))
            }
            Self::Log10(expr) => {
                expression::Variant::Log10(Box::new(expr.into_proto(source, prefetch)?))
            }
            Self::Ln(expr) => expression::Variant::Ln(Box::new(expr.into_proto(source, prefetch)?)),
            Self::ExpDecay(decay) => {
                expression::Variant::ExpDecay(Box::new(decay.into_proto(source, prefetch)?))
            }
            Self::GaussDecay(decay) => {
                expression::Variant::GaussDecay(Box::new(decay.into_proto(source, prefetch)?))
            }
            Self::LinDecay(decay) => {
                expression::Variant::LinDecay(Box::new(decay.into_proto(source, prefetch)?))
            }
        };
        Ok(Expression { variant: Some(variant) })
    }

    fn collect_defaults(&self, defaults: &mut HashMap<String, Value>) -> Result<()> {
        match self {
            Self::PayloadNum { path, default } | Self::PayloadDatetime { path, default } => {
                if let Some(default) = default {
                    insert_default(defaults, path, default.clone())?;
                }
                Ok(())
            }
            Self::Sum(values) | Self::Mult(values) => {
                for value in values {
                    value.collect_defaults(defaults)?;
                }
                Ok(())
            }
            Self::Div(lhs, rhs) | Self::Pow(lhs, rhs) => {
                lhs.collect_defaults(defaults)?;
                rhs.collect_defaults(defaults)
            }
            Self::Neg(expr)
            | Self::Abs(expr)
            | Self::Sqrt(expr)
            | Self::Exp(expr)
            | Self::Log10(expr)
            | Self::Ln(expr) => expr.collect_defaults(defaults),
            Self::ExpDecay(decay) | Self::GaussDecay(decay) | Self::LinDecay(decay) => {
                decay.collect_defaults(defaults)
            }
            Self::Constant(_)
            | Self::Column(_)
            | Self::Datetime(_)
            | Self::Condition(_)
            | Self::GeoDistance { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FormulaQuery {
    expression: FormulaExpr,
}

impl TryFrom<FormulaCall> for FormulaQuery {
    type Error = datafusion::error::DataFusionError;

    fn try_from(call: FormulaCall) -> Result<Self> {
        Ok(Self { expression: FormulaExpr::from_expr(&call.formula)? })
    }
}

impl FormulaQuery {
    pub(crate) fn same_semantics(&self, other: &Self) -> bool { self == other }

    pub(super) fn validate_on_source(&self, source: &Source) -> Result<()> {
        self.expression.validate_on_source(source)
    }

    pub(super) fn descriptor(
        &self,
        source: &Source,
        prefetch: &[QueryPrefetchBranch],
    ) -> Result<QueryDescriptor> {
        if prefetch.is_empty() {
            return plan_err!(
                "{FORMULA_SCORE_FUNCTION_NAME} requires qdrant prefetch input to rescore"
            );
        }
        let mut defaults = HashMap::new();
        self.expression.collect_defaults(&mut defaults)?;
        Ok(QueryDescriptor::new(
            Query::new_formula(Formula {
                expression: Some(self.expression.clone().into_proto(source, prefetch)?),
                defaults,
            }),
            None,
        ))
    }
}

fn payload_path_arg(expr: &Expr, function_name: &str, argument: &str) -> Result<String> {
    Ok(normalize_payload_path(&string_literal(expr, function_name, argument)?))
}

fn normalize_payload_path(path: &str) -> String {
    path.strip_prefix("payload:")
        .or_else(|| path.strip_prefix("payload."))
        .unwrap_or(path)
        .to_owned()
}

fn validate_payload_num(path: &str, source: &Source) -> Result<()> {
    match source.payload_field(path) {
        Some(QdrantPayloadField::Float | QdrantPayloadField::Integer { .. }) => Ok(()),
        Some(field) => plan_err!(
            "{PAYLOAD_NUM_FUNCTION_NAME} requires '{}' to be a numeric payload field, found {:?}",
            path,
            field
        ),
        None => {
            plan_err!("{PAYLOAD_NUM_FUNCTION_NAME} references unknown payload field '{}'", path)
        }
    }
}

fn validate_payload_datetime(path: &str, source: &Source) -> Result<()> {
    match source.payload_field(path) {
        Some(QdrantPayloadField::Datetime) => Ok(()),
        Some(field) => plan_err!(
            "{PAYLOAD_DATETIME_FUNCTION_NAME} requires '{}' to be a datetime payload field, found \
             {:?}",
            path,
            field
        ),
        None => plan_err!(
            "{PAYLOAD_DATETIME_FUNCTION_NAME} references unknown payload field '{}'",
            path
        ),
    }
}

fn validate_geo_distance(path: &str, source: &Source) -> Result<()> {
    match source.payload_field(path) {
        Some(QdrantPayloadField::Geo) => Ok(()),
        Some(field) => plan_err!(
            "{GEO_DISTANCE_FUNCTION_NAME} requires '{}' to be a geo payload field, found {:?}",
            path,
            field
        ),
        None => {
            plan_err!("{GEO_DISTANCE_FUNCTION_NAME} references unknown payload field '{}'", path)
        }
    }
}

fn score_output_matches(
    score_output_columns: &std::collections::BTreeSet<Column>,
    column: &Column,
) -> bool {
    if score_output_columns.contains(column) {
        return true;
    }
    if column.relation.is_some() {
        return false;
    }
    let mut matches = score_output_columns.iter().filter(|candidate| candidate.name == column.name);
    matches.next().is_some() && matches.next().is_none()
}

fn resolve_column(
    column: &Column,
    source: &Source,
    prefetch: &[QueryPrefetchBranch],
) -> Result<String> {
    let matching_prefetch = prefetch
        .iter()
        .enumerate()
        .filter_map(|(index, branch)| {
            score_output_matches(&branch.score_output_columns, column).then_some(index)
        })
        .collect::<Vec<_>>();
    match matching_prefetch.as_slice() {
        [index] => {
            if prefetch.len() == 1 && *index == 0 {
                return Ok("$score".to_owned());
            }
            return Ok(format!("$score[{index}]"));
        }
        [] => {}
        _ => {
            return plan_err!(
                "{FORMULA_SCORE_FUNCTION_NAME} column '{}' is ambiguous across qdrant prefetch                  branches",
                column.flat_name()
            );
        }
    }

    match source.payload_field(&column.name) {
        Some(QdrantPayloadField::Float | QdrantPayloadField::Integer { .. }) => {
            Ok(column.name.clone())
        }
        Some(field) => plan_err!(
            "{FORMULA_SCORE_FUNCTION_NAME} column '{}' resolves to non-numeric payload field {:?}",
            column.flat_name(),
            field
        ),
        None => {
            plan_err!(
                "{FORMULA_SCORE_FUNCTION_NAME} column '{}' does not resolve to a qdrant score \
                 column              or numeric payload field",
                column.flat_name()
            )
        }
    }
}

fn rewrite_condition_expr(expr: Expr) -> Result<Expr> {
    expr.transform_up(|nested_expr| {
        if let Some(call) = PayloadNumCall::from_expr(&nested_expr)? {
            if call.default.is_some() {
                return plan_err!(
                    "{CONDITION_FUNCTION_NAME} does not admit payload defaults inside condition \
                     predicates"
                );
            }
            return Ok(Transformed::yes(payload_path_expr(&payload_path_arg(
                &call.path,
                PAYLOAD_NUM_FUNCTION_NAME,
                "payload path",
            )?)));
        }
        if let Some(call) = PayloadDatetimeCall::from_expr(&nested_expr)? {
            if call.default.is_some() {
                return plan_err!(
                    "{CONDITION_FUNCTION_NAME} does not admit payload defaults inside condition \
                     predicates"
                );
            }
            return Ok(Transformed::yes(payload_path_expr(&payload_path_arg(
                &call.path,
                PAYLOAD_DATETIME_FUNCTION_NAME,
                "payload path",
            )?)));
        }
        if let Some(call) = DatetimeValueCall::from_expr(&nested_expr)? {
            return Ok(Transformed::yes(datetime_literal_expr(&call.value)?));
        }
        Ok(Transformed::no(nested_expr))
    })
    .data()
}

fn payload_path_expr(path: &str) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left:  Box::new(Expr::Column(Column::from_name(PAYLOAD_FIELD_NAME))),
        op:    Operator::Colon,
        right: Box::new(Expr::Literal(ScalarValue::Utf8(Some(path.to_owned())), None)),
    })
}

fn exact_condition_expr(expr: &Expr, source: &Source, function_name: &str) -> Result<Condition> {
    let filters =
        QdrantFilters::try_new(&source.schema, &source.payload_schema, std::slice::from_ref(expr))
            .map_err(|error| {
                datafusion::error::DataFusionError::Plan(format!(
                    "{function_name} requires an exact qdrant predicate: {error}"
                ))
            })?;
    let Some(filter) = filters.to_filter() else {
        return plan_err!("{function_name} requires an exact qdrant predicate");
    };
    Ok(Condition::from(filter))
}

fn insert_default(defaults: &mut HashMap<String, Value>, key: &str, value: Value) -> Result<()> {
    if let Some(existing) = defaults.get(key) {
        if existing != &value {
            return plan_err!(
                "{FORMULA_SCORE_FUNCTION_NAME} found conflicting defaults for payload variable \
                 '{}'",
                key
            );
        }
        return Ok(());
    }
    drop(defaults.insert(key.to_owned(), value));
    Ok(())
}

fn numeric_default_value(expr: &Expr, function_name: &str) -> Result<Value> {
    match expr.clone().unalias_nested().data {
        Expr::Negative(expr) => {
            if let Expr::Literal(value, _) = expr.as_ref().clone().unalias_nested().data {
                numeric_scalar_value(&value, function_name, true)
            } else {
                plan_err!("{function_name} requires default values to be numeric literals")
            }
        }
        Expr::Cast(cast) => numeric_default_value(&cast.expr, function_name),
        Expr::TryCast(cast) => numeric_default_value(&cast.expr, function_name),
        Expr::Literal(value, _) => numeric_scalar_value(&value, function_name, false),
        _ => plan_err!("{function_name} requires default values to be numeric literals"),
    }
}

fn numeric_scalar_value(value: &ScalarValue, function_name: &str, negative: bool) -> Result<Value> {
    macro_rules! signed {
        ($value:expr) => {{
            let value = if negative { -$value } else { $value };
            Ok(Value::from(value))
        }};
    }
    macro_rules! floaty {
        ($value:expr) => {{
            let value = if negative { -$value } else { $value };
            Ok(Value::from(value))
        }};
    }
    match value {
        ScalarValue::Int8(Some(value)) => signed!(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => signed!(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => signed!(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => signed!(*value),
        ScalarValue::UInt8(Some(value)) if !negative => Ok(Value::from(i64::from(*value))),
        ScalarValue::UInt16(Some(value)) if !negative => Ok(Value::from(i64::from(*value))),
        ScalarValue::UInt32(Some(value)) if !negative => Ok(Value::from(i64::from(*value))),
        ScalarValue::UInt64(Some(value)) if !negative => {
            i64::try_from(*value).map(Value::from).map_err(|_| {
                datafusion::error::DataFusionError::Plan(format!(
                    "{function_name} requires integer defaults to fit in i64"
                ))
            })
        }
        ScalarValue::Float32(Some(value)) => floaty!(f64::from(*value)),
        ScalarValue::Float64(Some(value)) => floaty!(*value),
        _ => plan_err!("{function_name} requires default values to be numeric literals"),
    }
}

fn datetime_default_value(expr: &Expr, function_name: &str) -> Result<Value> {
    Ok(Value::from(datetime_literal_with_name(expr, function_name, "datetime default")?))
}

fn datetime_literal(expr: &Expr) -> Result<String> {
    datetime_literal_with_name(expr, DATETIME_VALUE_FUNCTION_NAME, "datetime literal")
}

fn datetime_literal_with_name(expr: &Expr, function_name: &str, argument: &str) -> Result<String> {
    match expr.clone().unalias_nested().data {
        Expr::Cast(cast) => datetime_literal_with_name(&cast.expr, function_name, argument),
        Expr::TryCast(cast) => datetime_literal_with_name(&cast.expr, function_name, argument),
        Expr::Literal(value, _) => datetime_scalar_string(&value, function_name, argument),
        _ => plan_err!("{function_name} requires {argument} to be a datetime literal"),
    }
}

fn datetime_literal_expr(expr: &Expr) -> Result<Expr> {
    Ok(Expr::Literal(ScalarValue::Utf8(Some(datetime_literal(expr)?)), None))
}

fn datetime_scalar_string(
    value: &ScalarValue,
    function_name: &str,
    argument: &str,
) -> Result<String> {
    match value {
        ScalarValue::TimestampSecond(Some(value), _) => datetime_from_scaled(*value, 1),
        ScalarValue::TimestampMillisecond(Some(value), _) | ScalarValue::Date64(Some(value)) => {
            datetime_from_scaled(*value, 1_000)
        }
        ScalarValue::TimestampMicrosecond(Some(value), _) => {
            datetime_from_scaled(*value, 1_000_000)
        }
        ScalarValue::TimestampNanosecond(Some(value), _) => {
            datetime_from_scaled(*value, 1_000_000_000)
        }
        ScalarValue::Date32(Some(value)) => datetime_from_scaled(i64::from(*value) * 86_400, 1),
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            normalize_datetime_literal(value)
        }
        _ => plan_err!("{function_name} requires {argument} to be a datetime literal"),
    }
}

fn datetime_from_scaled(value: i64, scale: i64) -> Result<String> {
    let seconds = value.div_euclid(scale);
    let nanos = value.rem_euclid(scale) * (1_000_000_000 / scale);
    let Some(datetime) =
        DateTime::<Utc>::from_timestamp(seconds, u32::try_from(nanos).expect("nanos fit in u32"))
    else {
        return plan_err!("{DATETIME_VALUE_FUNCTION_NAME} datetime literal is out of range");
    };
    Ok(datetime.to_rfc3339())
}

fn normalize_datetime_literal(value: &str) -> Result<String> {
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return Ok(value.with_timezone(&Utc).to_rfc3339());
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(value.and_utc().to_rfc3339());
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(value.and_utc().to_rfc3339());
    }
    let Ok(value) = NaiveDate::parse_from_str(value, "%Y-%m-%d") else {
        return plan_err!(
            "{DATETIME_VALUE_FUNCTION_NAME} requires an RFC3339 or SQL datetime literal"
        );
    };
    Ok(value.and_hms_opt(0, 0, 0).expect("midnight is valid").and_utc().to_rfc3339())
}

#[expect(
    clippy::cast_precision_loss,
    reason = "formula geo/datetime numeric literals intentionally coerce into qdrant f64 \
              coordinates"
)]
fn f64_literal(expr: &Expr, function_name: &str, argument: &str) -> Result<f64> {
    match expr.clone().unalias_nested().data {
        Expr::Negative(expr) => Ok(-f64_literal(&expr, function_name, argument)?),
        Expr::Cast(cast) => f64_literal(&cast.expr, function_name, argument),
        Expr::TryCast(cast) => f64_literal(&cast.expr, function_name, argument),
        Expr::Literal(value, _) => match value {
            ScalarValue::Float32(Some(value)) => Ok(f64::from(value)),
            ScalarValue::Float64(Some(value)) => Ok(value),
            ScalarValue::Int8(Some(value)) => Ok(f64::from(value)),
            ScalarValue::Int16(Some(value)) => Ok(f64::from(value)),
            ScalarValue::Int32(Some(value)) => Ok(f64::from(value)),
            ScalarValue::Int64(Some(value)) => Ok(value as f64),
            ScalarValue::UInt8(Some(value)) => Ok(f64::from(value)),
            ScalarValue::UInt16(Some(value)) => Ok(f64::from(value)),
            ScalarValue::UInt32(Some(value)) => Ok(f64::from(value)),
            ScalarValue::UInt64(Some(value)) => Ok(value as f64),
            _ => plan_err!("{function_name} requires {argument} to be numeric"),
        },
        _ => plan_err!("{function_name} requires {argument} to be numeric"),
    }
}

fn unary(args: &[Expr]) -> Result<FormulaExpr> {
    if let [expr] = args {
        FormulaExpr::from_expr(expr)
    } else {
        plan_err!(
            "{FORMULA_SCORE_FUNCTION_NAME} unary scalar functions require exactly one operand"
        )
    }
}

fn binary(args: &[Expr]) -> Result<(FormulaExpr, FormulaExpr)> {
    if let [lhs, rhs] = args {
        Ok((FormulaExpr::from_expr(lhs)?, FormulaExpr::from_expr(rhs)?))
    } else {
        plan_err!(
            "{FORMULA_SCORE_FUNCTION_NAME} binary scalar functions require exactly two operands"
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::logical_expr::expr::BinaryExpr;
    use datafusion::logical_expr::{Expr, Operator};
    use datafusion::sql::TableReference;
    use qdrant_client::Qdrant;
    use qdrant_client::qdrant::{PayloadSchemaInfo, PayloadSchemaType};

    use super::*;
    use crate::analyzer::query::QueryBranchPlan;
    use crate::expr_fn::{
        qdrant_condition, qdrant_datetime_value, qdrant_geo_distance, qdrant_payload_datetime,
        qdrant_payload_num,
    };
    use crate::qdrant::QdrantPayloadSchema;

    fn test_source(payload_schema: QdrantPayloadSchema) -> Source {
        Source {
            client:                  Arc::new(
                Qdrant::from_url("http://localhost:6334").build().expect("client"),
            ),
            collection:              "vectors".to_owned(),
            schema:                  Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Utf8,
                true,
            )])),
            payload_schema:          Arc::new(payload_schema),
            ordered_scroll_contract: crate::table::QdrantOrderedScrollContract::ExactSinglePeer,
        }
    }

    fn test_prefetch(column: Column) -> QueryPrefetchBranch {
        QueryPrefetchBranch::new(
            QueryBranchPlan::descriptor(
                QueryDescriptor::new(Query { variant: None }, None),
                None,
                None,
                Some(10),
            ),
            BTreeSet::from([column]),
        )
    }

    #[test]
    fn formula_query_lowers_expr_tree_with_sql_columns() {
        let formula = Expr::BinaryExpr(BinaryExpr {
            left:  Box::new(Expr::Column(Column::from_name("score"))),
            op:    Operator::Plus,
            right: Box::new(Expr::Column(Column::from_name("rank"))),
        });
        let query = FormulaQuery::try_from(FormulaCall { formula }).expect("formula query");
        let source = test_source(QdrantPayloadSchema::from(HashMap::from([(
            "rank".to_owned(),
            PayloadSchemaInfo {
                data_type: PayloadSchemaType::Integer as i32,
                params:    None,
                points:    None,
            },
        )])));

        query.validate_on_source(&source).expect("validate on source");
        drop(
            query
                .descriptor(&source, &[test_prefetch(Column::from_name("score"))])
                .expect("descriptor"),
        );
    }

    #[test]
    fn formula_query_rejects_old_string_carrier() {
        let error = FormulaQuery::try_from(FormulaCall {
            formula: Expr::Literal(ScalarValue::Utf8(Some("score".to_owned())), None),
        })
        .expect_err("string carrier is rejected");

        assert!(error.to_string().contains("requires formula literal to be numeric"), "{error}");
    }

    #[test]
    fn formula_query_resolves_qualified_score_columns() {
        let query = FormulaQuery::try_from(FormulaCall {
            formula: Expr::Column(Column::new(Some(TableReference::bare("rhs")), "score")),
        })
        .expect("formula query");
        let source = test_source(QdrantPayloadSchema::default());
        drop(
            query
                .descriptor(&source, &[
                    test_prefetch(Column::new(Some(TableReference::bare("lhs")), "score")),
                    test_prefetch(Column::new(Some(TableReference::bare("rhs")), "score")),
                ])
                .expect("qualified score column resolves"),
        );
    }

    #[test]
    fn formula_query_rejects_ambiguous_score_columns() {
        let query = FormulaQuery::try_from(FormulaCall {
            formula: Expr::Column(Column::from_name("score")),
        })
        .expect("formula query");
        let source = test_source(QdrantPayloadSchema::default());
        let error = query
            .descriptor(&source, &[
                test_prefetch(Column::new(Some(TableReference::bare("lhs")), "score")),
                test_prefetch(Column::new(Some(TableReference::bare("rhs")), "score")),
            ])
            .expect_err("ambiguous score columns are rejected");
        assert!(error.to_string().contains("ambiguous"), "{error}");
    }

    #[test]
    fn formula_query_requires_prefetch_context() {
        let query = FormulaQuery::try_from(FormulaCall {
            formula: Expr::Column(Column::from_name("score")),
        })
        .expect("formula query");
        let source = test_source(QdrantPayloadSchema::default());
        let error = query.descriptor(&source, &[]).expect_err("prefetch is required");
        assert!(error.to_string().contains("requires qdrant prefetch input"), "{error}");
    }

    #[test]
    fn formula_query_collects_payload_defaults() {
        let query = FormulaQuery::try_from(FormulaCall {
            formula: Expr::BinaryExpr(BinaryExpr {
                left:  Box::new(qdrant_payload_num("rank")),
                op:    Operator::Plus,
                right: Box::new(qdrant_payload_datetime("created_at")),
            }),
        })
        .expect("formula query");
        let mut defaults = HashMap::new();
        query.expression.collect_defaults(&mut defaults).expect("defaults");
        assert!(defaults.is_empty(), "{defaults:?}");

        let query = FormulaQuery::try_from(FormulaCall {
            formula: Expr::BinaryExpr(BinaryExpr {
                left:  Box::new(crate::expr_fn::qdrant_payload_num_udf().call(vec![
                    Expr::Literal(ScalarValue::Utf8(Some("rank".to_owned())), None),
                    Expr::Literal(ScalarValue::Int64(Some(7)), None),
                ])),
                op:    Operator::Plus,
                right: Box::new(crate::expr_fn::qdrant_payload_datetime_udf().call(vec![
                    Expr::Literal(ScalarValue::Utf8(Some("created_at".to_owned())), None),
                    Expr::Literal(ScalarValue::Utf8(Some("2024-01-01".to_owned())), None),
                ])),
            }),
        })
        .expect("formula query with defaults");
        let mut defaults = HashMap::new();
        query.expression.collect_defaults(&mut defaults).expect("defaults");
        assert_eq!(defaults.get("rank"), Some(&Value::from(7_i64)));
        assert_eq!(defaults.get("created_at"), Some(&Value::from("2024-01-01T00:00:00+00:00")),);
    }

    #[test]
    fn formula_query_lowers_condition_leaf() {
        let predicate = Expr::BinaryExpr(BinaryExpr {
            left:  Box::new(qdrant_payload_num("rank")),
            op:    Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(3)), None)),
        });
        let query = FormulaQuery::try_from(FormulaCall { formula: qdrant_condition(predicate) })
            .expect("formula query");
        let source = test_source(QdrantPayloadSchema::from(HashMap::from([(
            "rank".to_owned(),
            PayloadSchemaInfo {
                data_type: PayloadSchemaType::Integer as i32,
                params:    None,
                points:    None,
            },
        )])));
        let Expression { variant } = query
            .expression
            .clone()
            .into_proto(&source, &[test_prefetch(Column::from_name("score"))])
            .expect("formula proto");
        assert!(matches!(variant, Some(expression::Variant::Condition(_))));
    }

    #[test]
    fn formula_query_lowers_decay_and_datetime_leaves() {
        let query = FormulaQuery::try_from(FormulaCall {
            formula: crate::expr_fn::qdrant_exp_decay_udf().call(vec![
                qdrant_payload_datetime("created_at"),
                qdrant_datetime_value("2024-01-01"),
                Expr::Literal(ScalarValue::Float64(Some(86_400.0)), None),
                Expr::Literal(ScalarValue::Float64(Some(0.25)), None),
            ]),
        })
        .expect("formula query");
        let source = test_source(QdrantPayloadSchema::from(HashMap::from([(
            "created_at".to_owned(),
            PayloadSchemaInfo {
                data_type: PayloadSchemaType::Datetime as i32,
                params:    None,
                points:    None,
            },
        )])));
        let Expression { variant } = query
            .expression
            .clone()
            .into_proto(&source, &[test_prefetch(Column::from_name("score"))])
            .expect("formula proto");
        assert!(matches!(variant, Some(expression::Variant::ExpDecay(_))));
    }

    #[test]
    fn formula_query_lowers_geo_distance_leaf() {
        let query = FormulaQuery::try_from(FormulaCall {
            formula: qdrant_geo_distance("location", -73.9, 40.7),
        })
        .expect("formula query");
        let source = test_source(QdrantPayloadSchema::from(HashMap::from([(
            "location".to_owned(),
            PayloadSchemaInfo {
                data_type: PayloadSchemaType::Geo as i32,
                params:    None,
                points:    None,
            },
        )])));
        let Expression { variant } = query
            .expression
            .clone()
            .into_proto(&source, &[test_prefetch(Column::from_name("score"))])
            .expect("formula proto");
        assert!(matches!(variant, Some(expression::Variant::GeoDistance(_))));
    }

    #[test]
    fn formula_query_rejects_conflicting_defaults() {
        let query = FormulaQuery::try_from(FormulaCall {
            formula: Expr::BinaryExpr(BinaryExpr {
                left:  Box::new(crate::expr_fn::qdrant_payload_num_udf().call(vec![
                    Expr::Literal(ScalarValue::Utf8(Some("rank".to_owned())), None),
                    Expr::Literal(ScalarValue::Int64(Some(1)), None),
                ])),
                op:    Operator::Plus,
                right: Box::new(crate::expr_fn::qdrant_payload_num_udf().call(vec![
                    Expr::Literal(ScalarValue::Utf8(Some("rank".to_owned())), None),
                    Expr::Literal(ScalarValue::Int64(Some(2)), None),
                ])),
            }),
        })
        .expect("formula query");
        let mut defaults = HashMap::new();
        let error = query.expression.collect_defaults(&mut defaults).expect_err("conflict");
        assert!(error.to_string().contains("conflicting defaults"), "{error}");
    }
}
