use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::{
    DenseVector, Mmr, NearestInputWithMmr, Query, VectorInput, query, vector_input,
};

use super::super::source::Source;
use super::nearest::QueryVectorBinding;
use super::{QueryDescriptor, column_name, f32_literal, function_args, u32_literal};
use crate::expr_fn::QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME;

#[derive(Debug, Clone)]
pub(crate) struct NearestWithMmrQuery {
    using:            String,
    diversity:        f32,
    candidates_limit: u32,
    vector:           Vec<f32>,
}

impl NearestWithMmrQuery {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME) else {
            return Ok(None);
        };
        if args.len() < 4 {
            return plan_err!(
                "{QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} requires a vector column, \
                 diversity, candidates_limit, and query components"
            );
        }
        let using = column_name(&args[0], QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME)?;
        let diversity =
            f32_literal(&args[1], QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME, "diversity")?;
        if !(0.0..=1.0).contains(&diversity) {
            return plan_err!(
                "{QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} diversity must be in [0, 1]"
            );
        }
        let candidates_limit =
            u32_literal(&args[2], QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME, "candidates_limit")?;
        let vector = args[3..]
            .iter()
            .map(|expr| {
                f32_literal(expr, QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME, "query component")
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(Self { using, diversity, candidates_limit, vector }))
    }

    pub(crate) fn same_semantics(&self, other: &Self) -> bool {
        self.using == other.using
            && self.diversity.to_bits() == other.diversity.to_bits()
            && self.candidates_limit == other.candidates_limit
            && self
                .vector
                .iter()
                .map(|value| value.to_bits())
                .eq(other.vector.iter().map(|value| value.to_bits()))
    }

    pub(super) fn validate_on_source(&self, source: &Source) -> Result<()> {
        match QueryVectorBinding::from_source(source, &self.using)? {
            QueryVectorBinding::DenseFixed { width } => {
                if width != self.vector.len() {
                    return plan_err!("query vector width does not match source vector width");
                }
                Ok(())
            }
            QueryVectorBinding::DenseVariable => Ok(()),
            QueryVectorBinding::Sparse => plan_err!(
                "{QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} requires a dense vector binding"
            ),
            QueryVectorBinding::MultiDense => plan_err!(
                "{QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} requires a single dense vector \
                 binding"
            ),
            QueryVectorBinding::Document => plan_err!(
                "{QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} does not bind to a document \
                 inference field"
            ),
            QueryVectorBinding::Image => plan_err!(
                "{QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} does not bind to an image \
                 inference field"
            ),
            QueryVectorBinding::Object => plan_err!(
                "{QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} does not bind to an object \
                 inference field"
            ),
            QueryVectorBinding::Unsupported(data_type) => plan_err!(
                "{QDRANT_NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} does not bind to source field '{}' \
                 of type {:?}",
                self.using,
                data_type
            ),
        }
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> Result<QueryDescriptor> {
        Ok(QueryDescriptor::new(
            Query {
                variant: Some(query::Variant::NearestWithMmr(NearestInputWithMmr {
                    nearest: Some(VectorInput {
                        variant: Some(vector_input::Variant::Dense(DenseVector {
                            data: self.vector.clone(),
                        })),
                    }),
                    mmr:     Some(Mmr {
                        diversity:        Some(self.diversity),
                        candidates_limit: Some(self.candidates_limit),
                    }),
                })),
            },
            Some(self.using.clone()),
        ))
    }
}
