use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::prelude::lit;

use super::common::{NonExecutableScoreUdf, function_args};

pub const FORMULA_SCORE_FUNCTION_NAME: &str = "qdrant_formula_score";
pub const PAYLOAD_NUM_FUNCTION_NAME: &str = "qdrant_payload_num";

const FORMULA_ALIASES: &[&str] = &["formula_score"];
const PAYLOAD_NUM_ALIASES: &[&str] = &["payload_num"];

#[derive(Debug, Clone)]
pub(crate) struct FormulaCall {
    pub(crate) formula: Expr,
}

impl FormulaCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, FORMULA_SCORE_FUNCTION_NAME, FORMULA_ALIASES) else {
            return Ok(None);
        };
        if args.len() != 1 {
            return plan_err!("{FORMULA_SCORE_FUNCTION_NAME} requires a single formula expression");
        }
        Ok(Some(Self { formula: args[0].clone() }))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PayloadNumCall {
    pub(crate) path: Expr,
}

impl PayloadNumCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, PAYLOAD_NUM_FUNCTION_NAME, PAYLOAD_NUM_ALIASES) else {
            return Ok(None);
        };
        if args.len() != 1 {
            return plan_err!("{PAYLOAD_NUM_FUNCTION_NAME} requires exactly one payload path");
        }
        Ok(Some(Self { path: args[0].clone() }))
    }
}

#[must_use]
pub fn qdrant_formula_score(formula: Expr) -> Expr {
    qdrant_formula_score_udf().call(vec![formula])
}

#[must_use]
pub fn qdrant_payload_num(path: impl Into<String>) -> Expr {
    qdrant_payload_num_udf().call(vec![lit(path.into())])
}

pub(crate) fn qdrant_formula_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(
            FORMULA_SCORE_FUNCTION_NAME,
            FORMULA_ALIASES,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_payload_num_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(
            PAYLOAD_NUM_FUNCTION_NAME,
            PAYLOAD_NUM_ALIASES,
        ))
    })
    .clone()
}
