use datafusion::common::{Result, plan_err};
use qdrant_client::qdrant::{
    DenseVector, Mmr, NearestInputWithMmr, Query, VectorInput, query, vector_input,
};

use super::super::source::Source;
use super::{QueryDescriptor, f32_literal, u32_literal};
use crate::arrow::schema::QdrantFieldBinding;
use crate::expr_fn::{NEAREST_WITH_MMR_SCORE_FUNCTION_NAME, NearestWithMmrCall};

#[derive(Debug, Clone)]
pub(crate) struct NearestWithMmrQuery {
    using:            String,
    diversity:        f32,
    candidates_limit: u32,
    vector:           Vec<f32>,
}

impl TryFrom<NearestWithMmrCall> for NearestWithMmrQuery {
    type Error = datafusion::error::DataFusionError;

    fn try_from(call: NearestWithMmrCall) -> Result<Self> {
        let diversity =
            f32_literal(&call.diversity, NEAREST_WITH_MMR_SCORE_FUNCTION_NAME, "diversity")?;
        if !(0.0..=1.0).contains(&diversity) {
            return plan_err!("{NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} diversity must be in [0, 1]");
        }
        let candidates_limit = u32_literal(
            &call.candidates_limit,
            NEAREST_WITH_MMR_SCORE_FUNCTION_NAME,
            "candidates_limit",
        )?;
        let vector = call
            .query_components
            .iter()
            .map(|expr| f32_literal(expr, NEAREST_WITH_MMR_SCORE_FUNCTION_NAME, "query component"))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { using: call.vector_field, diversity, candidates_limit, vector })
    }
}

impl NearestWithMmrQuery {
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
        match source.field_binding(&self.using)? {
            QdrantFieldBinding::DenseFixed { width } => {
                if width != self.vector.len() {
                    return plan_err!("query vector width does not match source vector width");
                }
                Ok(())
            }
            QdrantFieldBinding::DenseVariable => Ok(()),
            QdrantFieldBinding::Sparse => {
                plan_err!("{NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} requires a dense vector binding")
            }
            QdrantFieldBinding::MultiDense { .. } => plan_err!(
                "{NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} requires a single dense vector binding"
            ),
            QdrantFieldBinding::Document => plan_err!(
                "{NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} does not bind to a document inference                  field"
            ),
            QdrantFieldBinding::Image => plan_err!(
                "{NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} does not bind to an image inference field"
            ),
            QdrantFieldBinding::Object => plan_err!(
                "{NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} does not bind to an object inference field"
            ),
            QdrantFieldBinding::Unsupported(data_type) => plan_err!(
                "{NEAREST_WITH_MMR_SCORE_FUNCTION_NAME} does not bind to source field '{}' of                  type {:?}",
                self.using,
                data_type
            ),
        }
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> QueryDescriptor {
        QueryDescriptor::new(
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
        )
    }
}
