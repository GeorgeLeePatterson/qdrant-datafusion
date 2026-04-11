use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::{AggregateFunction, Alias};
use datafusion::logical_expr::utils::COUNT_STAR_EXPANSION;

pub(crate) fn count_like_arg(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::Alias(Alias { expr, .. }) => count_like_arg(expr),
        Expr::AggregateFunction(AggregateFunction { func, params }) => (func.name() == "count"
            && !params.distinct
            && params.filter.is_none()
            && params.order_by.is_empty()
            && params.null_treatment.is_none())
        .then_some(params.args.as_slice())
        .and_then(|args| match args {
            [arg] => Some(arg),
            _ => None,
        }),
        _ => None,
    }
}

pub(crate) fn count_star_like(expr: &Expr) -> bool {
    matches!(count_like_arg(expr), Some(Expr::Literal(value, _)) if count_like_literal(value))
}

pub(crate) fn count_like_literal(value: &ScalarValue) -> bool {
    !value.is_null()
        && (value == &COUNT_STAR_EXPANSION
            || matches!(
                value,
                ScalarValue::Int8(_)
                    | ScalarValue::Int16(_)
                    | ScalarValue::Int32(_)
                    | ScalarValue::Int64(_)
                    | ScalarValue::UInt8(_)
                    | ScalarValue::UInt16(_)
                    | ScalarValue::UInt32(_)
                    | ScalarValue::UInt64(_)
                    | ScalarValue::Utf8(_)
                    | ScalarValue::Utf8View(_)
                    | ScalarValue::LargeUtf8(_)
                    | ScalarValue::Boolean(_)
            ))
}
