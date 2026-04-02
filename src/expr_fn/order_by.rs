use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::prelude::lit;

use super::common::{NonExecutableScoreUdf, function_args};

pub const ORDER_BY_SCORE_FUNCTION_NAME: &str = "qdrant_order_by_score";
const ALIASES: &[&str] = &["order_by_score"];

#[derive(Debug, Clone)]
pub(crate) struct OrderByCall {
    pub(crate) path: Expr,
    pub(crate) direction: Option<Expr>,
}

impl OrderByCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, ORDER_BY_SCORE_FUNCTION_NAME, ALIASES) else {
            return Ok(None);
        };
        if !(1..=2).contains(&args.len()) {
            return plan_err!(
                "{ORDER_BY_SCORE_FUNCTION_NAME} requires a payload path and an optional direction"
            );
        }
        Ok(Some(Self { path: args[0].clone(), direction: args.get(1).cloned() }))
    }
}

#[must_use]
pub fn qdrant_order_by_score(path: Expr, descending: bool) -> Expr {
    qdrant_order_by_score_udf().call(vec![path, lit(descending)])
}

pub(crate) fn qdrant_order_by_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(ORDER_BY_SCORE_FUNCTION_NAME, ALIASES))
    })
    .clone()
}
