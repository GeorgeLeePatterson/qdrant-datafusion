use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};

use super::common::{NonExecutableScoreUdf, function_args};

pub const FORMULA_SCORE_FUNCTION_NAME: &str = "qdrant_formula_score";
const ALIASES: &[&str] = &["formula_score"];

#[derive(Debug, Clone)]
pub(crate) struct FormulaCall {
    pub(crate) formula: Expr,
}

impl FormulaCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, FORMULA_SCORE_FUNCTION_NAME, ALIASES) else {
            return Ok(None);
        };
        if args.len() != 1 {
            return plan_err!("{FORMULA_SCORE_FUNCTION_NAME} requires a single formula expression");
        }
        Ok(Some(Self { formula: args[0].clone() }))
    }
}

#[must_use]
pub fn qdrant_formula_score(formula: Expr) -> Expr {
    qdrant_formula_score_udf().call(vec![formula])
}

pub(crate) fn qdrant_formula_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(FORMULA_SCORE_FUNCTION_NAME, ALIASES))
    })
    .clone()
}
