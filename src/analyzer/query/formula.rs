use std::collections::HashMap;

use datafusion::common::{Column, Result, ScalarValue, plan_err};
use datafusion::logical_expr::expr::BinaryExpr;
use datafusion::logical_expr::{Expr, Operator};
use qdrant_client::qdrant::{
    DivExpression, Expression, Formula, MultExpression, PowExpression, Query, SumExpression,
    expression,
};

use super::super::source::Source;
use super::{QueryDescriptor, QueryPrefetchBranch, scalar_f32, string_literal};
use crate::expr_fn::{
    FORMULA_SCORE_FUNCTION_NAME, FormulaCall, PAYLOAD_NUM_FUNCTION_NAME, PayloadNumCall,
};
use crate::pushdown::{QdrantPayloadField, QdrantPayloadPath};

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
enum FormulaExpr {
    Constant(f32),
    Column(Column),
    PayloadNum(String),
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
}

impl FormulaExpr {
    fn from_expr(expr: &Expr) -> Result<Self> {
        if let Some(call) = PayloadNumCall::from_expr(expr)? {
            return Ok(Self::PayloadNum(payload_path_arg(&call.path)?));
        }
        if let Some(path) = QdrantPayloadPath::from_logical_expr(expr) {
            return Ok(Self::PayloadNum(path.key().to_owned()));
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
            _ => plan_err!(
                "{FORMULA_SCORE_FUNCTION_NAME} only admits numeric literals, resolved columns, \
                 payload numeric leaves, arithmetic operators, and supported scalar functions"
            ),
        }
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
            Self::Constant(_) | Self::Column(_) => Ok(()),
            Self::PayloadNum(path) => validate_payload_num(path, source),
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
        }
    }

    fn into_proto(self, source: &Source, prefetch: &[QueryPrefetchBranch]) -> Result<Expression> {
        let variant = match self {
            Self::Constant(value) => expression::Variant::Constant(value),
            Self::Column(column) => {
                expression::Variant::Variable(resolve_column(&column, source, prefetch)?)
            }
            Self::PayloadNum(path) => expression::Variant::Variable(path),
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
        };
        Ok(Expression { variant: Some(variant) })
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
        Ok(QueryDescriptor::new(
            Query::new_formula(Formula {
                expression: Some(self.expression.clone().into_proto(source, prefetch)?),
                defaults:   HashMap::new(),
            }),
            None,
        ))
    }
}

fn payload_path_arg(expr: &Expr) -> Result<String> {
    Ok(normalize_payload_path(&string_literal(expr, PAYLOAD_NUM_FUNCTION_NAME, "payload path")?))
}

fn normalize_payload_path(path: &str) -> String {
    path.strip_prefix("payload:")
        .or_else(|| path.strip_prefix("payload."))
        .unwrap_or(path)
        .to_owned()
}

fn validate_payload_num(path: &str, source: &Source) -> Result<()> {
    let field = source
        .payload_schema
        .field(path)
        .or_else(|| path.split('.').next().and_then(|prefix| source.payload_schema.field(prefix)));
    match field {
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
                "{FORMULA_SCORE_FUNCTION_NAME} column '{}' is ambiguous across qdrant prefetch \
                 branches",
                column.flat_name()
            );
        }
    }

    match source.payload_schema.field(&column.name) {
        Some(QdrantPayloadField::Float | QdrantPayloadField::Integer { .. }) => {
            Ok(column.name.clone())
        }
        Some(field) => plan_err!(
            "{FORMULA_SCORE_FUNCTION_NAME} column '{}' resolves to non-numeric payload field {:?}",
            column.flat_name(),
            field
        ),
        None => plan_err!(
            "{FORMULA_SCORE_FUNCTION_NAME} column '{}' does not resolve to a qdrant score column \
             or numeric payload field",
            column.flat_name()
        ),
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
    use crate::pushdown::QdrantPayloadSchema;

    fn test_source(payload_schema: QdrantPayloadSchema) -> Source {
        Source {
            client:         Arc::new(
                Qdrant::from_url("http://localhost:6334").build().expect("client"),
            ),
            collection:     "vectors".to_owned(),
            schema:         Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Utf8,
                true,
            )])),
            payload_schema: Arc::new(payload_schema),
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
}
