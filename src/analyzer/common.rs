use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::{AggregateFunction, Alias};
use datafusion::logical_expr::utils::COUNT_STAR_EXPANSION;

pub(crate) fn count_star_like(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(Alias { expr, .. }) => count_star_like(expr),
        Expr::AggregateFunction(AggregateFunction { func, params }) => {
            func.name() == "count"
                && !params.distinct
                && params.filter.is_none()
                && params.order_by.is_empty()
                && params.null_treatment.is_none()
                && matches!(params.args.as_slice(), [Expr::Literal(value, _)] if count_like_literal(value))
        }
        _ => false,
    }
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
