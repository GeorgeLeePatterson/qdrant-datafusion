use std::sync::OnceLock;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::prelude::lit;

use super::common::{NonExecutableScoreUdf, function_args};

pub const FUSION_SCORE_FUNCTION_NAME: &str = "qdrant_fusion_score";
const ALIASES: &[&str] = &["fusion_score"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QdrantFusionMethod {
    Rrf { k: Option<u32> },
    Dbsf,
}

impl Default for QdrantFusionMethod {
    fn default() -> Self { Self::Rrf { k: None } }
}

impl QdrantFusionMethod {
    fn into_args(self) -> Vec<Expr> {
        match self {
            Self::Rrf { k } => {
                let mut args = vec![lit("RRF")];
                if let Some(k) = k {
                    args.push(lit(k));
                }
                args
            }
            Self::Dbsf => vec![lit("DBSF")],
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FusionCall {
    pub(crate) method:       Expr,
    pub(crate) rrf_k:        Option<Expr>,
    pub(crate) score_inputs: Vec<Expr>,
}

impl FusionCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, FUSION_SCORE_FUNCTION_NAME, ALIASES) else {
            return Ok(None);
        };
        if args.is_empty() {
            return plan_err!(
                "{FUSION_SCORE_FUNCTION_NAME} requires a fusion method and optional score inputs"
            );
        }
        let method = args[0].clone();
        let mut remainder = &args[1..];
        let mut rrf_k = None;
        if let Some(candidate) = remainder.first()
            && looks_like_integer_literal(candidate)
        {
            rrf_k = Some((*candidate).clone());
            remainder = &remainder[1..];
        }
        Ok(Some(Self { method, rrf_k, score_inputs: remainder.to_vec() }))
    }
}

fn looks_like_integer_literal(expr: &Expr) -> bool {
    match expr.clone().unalias_nested().data {
        Expr::Cast(cast) => looks_like_integer_literal(&cast.expr),
        Expr::TryCast(cast) => looks_like_integer_literal(&cast.expr),
        Expr::Literal(value, _) => match value {
            datafusion::common::ScalarValue::Int8(Some(value)) => value >= 0,
            datafusion::common::ScalarValue::Int16(Some(value)) => value >= 0,
            datafusion::common::ScalarValue::Int32(Some(value)) => value >= 0,
            datafusion::common::ScalarValue::Int64(Some(value)) => value >= 0,
            datafusion::common::ScalarValue::UInt8(Some(_))
            | datafusion::common::ScalarValue::UInt16(Some(_))
            | datafusion::common::ScalarValue::UInt32(Some(_))
            | datafusion::common::ScalarValue::UInt64(Some(_)) => true,
            _ => false,
        },
        _ => false,
    }
}

#[must_use]
pub fn qdrant_fusion_score(
    method: QdrantFusionMethod,
    score_inputs: impl IntoIterator<Item = Expr>,
) -> Expr {
    let mut args = method.into_args();
    args.extend(score_inputs);
    qdrant_fusion_score_udf().call(args)
}

pub(crate) fn qdrant_fusion_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableScoreUdf::new(FUSION_SCORE_FUNCTION_NAME, ALIASES))
    })
    .clone()
}

#[cfg(test)]
mod tests {
    use datafusion::common::ScalarValue;
    use datafusion::prelude::col;

    use super::*;

    #[test]
    fn fusion_helper_builds_typed_method_and_optional_rrf_k() {
        let expr = qdrant_fusion_score(QdrantFusionMethod::Rrf { k: Some(8) }, [col("lhs_score")]);
        let call = FusionCall::from_expr(&expr).expect("fusion call").expect("parsed fusion call");

        assert_eq!(call.score_inputs.len(), 1);
        assert_eq!(call.method, Expr::Literal(ScalarValue::Utf8(Some("RRF".to_owned())), None));
        assert_eq!(call.rrf_k, Some(Expr::Literal(ScalarValue::UInt32(Some(8)), None)));
    }
}
