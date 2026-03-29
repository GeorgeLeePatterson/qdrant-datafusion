use std::collections::HashMap;

use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::{
    DivExpression, Expression, Formula, MultExpression, PowExpression, Query, SumExpression,
    expression,
};

use super::super::source::Source;
use super::{QueryDescriptor, function_args, list_from_scalar, scalar_f32, scalar_string};
use crate::expr_fn::QDRANT_FORMULA_SCORE_FUNCTION_NAME;

#[derive(Debug, Clone, PartialEq)]
enum FormulaExpr {
    Constant(f32),
    Variable(String),
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
        match expr.clone().unalias_nested().data {
            Expr::Cast(cast) => Self::from_expr(&cast.expr),
            Expr::TryCast(cast) => Self::from_expr(&cast.expr),
            Expr::Literal(value, _) => Self::from_scalar(&value),
            _ => plan_err!(
                "{QDRANT_FORMULA_SCORE_FUNCTION_NAME} requires the formula to be encoded as \
                 literals and array literals"
            ),
        }
    }

    fn from_scalar(value: &ScalarValue) -> Result<Self> {
        if let Ok(value) = scalar_f32(value, QDRANT_FORMULA_SCORE_FUNCTION_NAME, "formula leaf") {
            return Ok(Self::Constant(value));
        }
        if let Ok(value) = scalar_string(value, QDRANT_FORMULA_SCORE_FUNCTION_NAME, "formula leaf")
        {
            return Ok(Self::Variable(parse_variable(&value)));
        }
        let values = list_from_scalar(value, QDRANT_FORMULA_SCORE_FUNCTION_NAME, "formula tree")?;
        if values.is_empty() {
            return plan_err!(
                "{QDRANT_FORMULA_SCORE_FUNCTION_NAME} formula arrays may not be empty"
            );
        }
        let operator =
            scalar_string(&values[0], QDRANT_FORMULA_SCORE_FUNCTION_NAME, "formula operator")?;
        let args = values[1..].iter().map(Self::from_scalar).collect::<Result<Vec<_>>>()?;
        match operator.to_ascii_lowercase().as_str() {
            "sum" => Ok(Self::Sum(args)),
            "mult" => Ok(Self::Mult(args)),
            "div" => binary(operator.as_str(), args)
                .map(|(lhs, rhs)| Self::Div(Box::new(lhs), Box::new(rhs))),
            "pow" => binary(operator.as_str(), args)
                .map(|(lhs, rhs)| Self::Pow(Box::new(lhs), Box::new(rhs))),
            "neg" => unary(operator.as_str(), args).map(|expr| Self::Neg(Box::new(expr))),
            "abs" => unary(operator.as_str(), args).map(|expr| Self::Abs(Box::new(expr))),
            "sqrt" => unary(operator.as_str(), args).map(|expr| Self::Sqrt(Box::new(expr))),
            "exp" => unary(operator.as_str(), args).map(|expr| Self::Exp(Box::new(expr))),
            "log10" => unary(operator.as_str(), args).map(|expr| Self::Log10(Box::new(expr))),
            "ln" => unary(operator.as_str(), args).map(|expr| Self::Ln(Box::new(expr))),
            _ => plan_err!(
                "{QDRANT_FORMULA_SCORE_FUNCTION_NAME} does not support formula operator '{}'",
                operator
            ),
        }
    }

    fn validate_on_source(&self, source: &Source) -> Result<()> {
        match self {
            Self::Constant(_) => Ok(()),
            Self::Variable(name) => {
                if name == "score" {
                    return Ok(());
                }
                let field = name.split('.').next().unwrap_or(name);
                if source.payload_schema.field(field).is_some() {
                    Ok(())
                } else {
                    plan_err!(
                        "{QDRANT_FORMULA_SCORE_FUNCTION_NAME} references unknown payload field \
                         '{}'",
                        name
                    )
                }
            }
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

    fn into_proto(self) -> Expression {
        let variant = match self {
            Self::Constant(value) => expression::Variant::Constant(value),
            Self::Variable(name) => expression::Variant::Variable(name),
            Self::Sum(sum) => expression::Variant::Sum(SumExpression {
                sum: sum.into_iter().map(Self::into_proto).collect(),
            }),
            Self::Mult(mult) => expression::Variant::Mult(MultExpression {
                mult: mult.into_iter().map(Self::into_proto).collect(),
            }),
            Self::Div(lhs, rhs) => expression::Variant::Div(Box::new(DivExpression {
                left:            Some(Box::new(lhs.into_proto())),
                right:           Some(Box::new(rhs.into_proto())),
                by_zero_default: None,
            })),
            Self::Neg(expr) => expression::Variant::Neg(Box::new(expr.into_proto())),
            Self::Abs(expr) => expression::Variant::Abs(Box::new(expr.into_proto())),
            Self::Sqrt(expr) => expression::Variant::Sqrt(Box::new(expr.into_proto())),
            Self::Pow(base, exponent) => expression::Variant::Pow(Box::new(PowExpression {
                base:     Some(Box::new(base.into_proto())),
                exponent: Some(Box::new(exponent.into_proto())),
            })),
            Self::Exp(expr) => expression::Variant::Exp(Box::new(expr.into_proto())),
            Self::Log10(expr) => expression::Variant::Log10(Box::new(expr.into_proto())),
            Self::Ln(expr) => expression::Variant::Ln(Box::new(expr.into_proto())),
        };
        Expression { variant: Some(variant) }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FormulaQuery {
    expression: FormulaExpr,
}

impl FormulaQuery {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, QDRANT_FORMULA_SCORE_FUNCTION_NAME) else {
            return Ok(None);
        };
        if args.len() != 1 {
            return plan_err!(
                "{QDRANT_FORMULA_SCORE_FUNCTION_NAME} requires a single formula expression"
            );
        }
        Ok(Some(Self { expression: FormulaExpr::from_expr(&args[0])? }))
    }

    pub(crate) fn same_semantics(&self, other: &Self) -> bool { self == other }

    pub(super) fn validate_on_source(&self, source: &Source) -> Result<()> {
        self.expression.validate_on_source(source)
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> Result<QueryDescriptor> {
        Ok(QueryDescriptor::new(
            Query::new_formula(Formula {
                expression: Some(self.expression.clone().into_proto()),
                defaults:   HashMap::new(),
            }),
            None,
        ))
    }
}

fn parse_variable(value: &str) -> String {
    if value.eq_ignore_ascii_case("score") {
        return "score".to_owned();
    }
    value
        .strip_prefix("payload:")
        .or_else(|| value.strip_prefix("payload."))
        .unwrap_or(value)
        .to_owned()
}

fn unary(operator: &str, args: Vec<FormulaExpr>) -> Result<FormulaExpr> {
    match args.as_slice() {
        [expr] => Ok(expr.clone()),
        _ => plan_err!(
            "{QDRANT_FORMULA_SCORE_FUNCTION_NAME} operator '{}' requires exactly one operand",
            operator
        ),
    }
}

fn binary(operator: &str, args: Vec<FormulaExpr>) -> Result<(FormulaExpr, FormulaExpr)> {
    match args.as_slice() {
        [lhs, rhs] => Ok((lhs.clone(), rhs.clone())),
        _ => plan_err!(
            "{QDRANT_FORMULA_SCORE_FUNCTION_NAME} operator '{}' requires exactly two operands",
            operator
        ),
    }
}
