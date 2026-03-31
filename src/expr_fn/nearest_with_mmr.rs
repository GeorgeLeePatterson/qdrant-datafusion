use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::prelude::lit;

use super::common::{NonExecutableScoreUdf, column_name, function_args};

pub const NEAREST_WITH_MMR_SCORE_FUNCTION_NAME: &str = "qdrant_nearest_with_mmr_score";
const ALIASES: &[&str] = &["nearest_with_mmr_score", "mmr_score"];

#[derive(Debug, Clone)]
pub(crate) struct NearestWithMmrCall {
    pub(crate) vector_field:     String,
    pub(crate) diversity:        Expr,
    pub(crate) candidates_limit: Expr,
    pub(crate) query_components: Vec<Expr>,
}

impl NearestWithMmrCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, NEAREST_WITH_MMR_SCORE_FUNCTION_NAME, ALIASES) else {
            return Ok(None);
        };
        if args.len() < 4 {
            return plan_err!(
                "{NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} requires a vector column, diversity,                  candidates_limit, and query components"
            );
        }
        Ok(Some(Self {
            vector_field:     column_name(&args[0], NEAREST_WITH_MMR_SCORE_FUNCTION_NAME)?,
            diversity:        args[1].clone(),
            candidates_limit: args[2].clone(),
            query_components: args[3..].to_vec(),
        }))
    }
}

#[must_use]
pub fn qdrant_nearest_with_mmr_score(
    vector: Expr,
    diversity: f32,
    candidates_limit: u32,
    query: impl IntoIterator<Item = f32>,
) -> Expr {
    let mut args = vec![vector, lit(diversity), lit(candidates_limit)];
    args.extend(query.into_iter().map(lit));
    qdrant_nearest_with_mmr_score_udf().call(args)
}

pub(crate) fn qdrant_nearest_with_mmr_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(
            NEAREST_WITH_MMR_SCORE_FUNCTION_NAME,
            ALIASES,
        ))
    })
    .clone()
}
