mod common;
mod context;
mod discover;
mod formula;
mod fusion;
mod nearest;
mod nearest_with_mmr;
mod order_by;
mod payload;
mod payload_access;
mod payload_geo;
mod payload_predicate;
mod payload_text;
mod recommend;
mod relevance_feedback;
mod sample;

use datafusion::execution::context::SessionContext;

pub(crate) use self::common::literal_scalar;
pub use self::context::{CONTEXT_SCORE_FUNCTION_NAME, qdrant_context_score};
pub(crate) use self::context::{ContextCall, qdrant_context_score_udf};
pub use self::discover::{DISCOVER_SCORE_FUNCTION_NAME, qdrant_discover_score};
pub(crate) use self::discover::{DiscoverCall, qdrant_discover_score_udf};
pub use self::formula::{
    CONDITION_FUNCTION_NAME, DATETIME_VALUE_FUNCTION_NAME, EXP_DECAY_FUNCTION_NAME,
    FORMULA_SCORE_FUNCTION_NAME, GAUSS_DECAY_FUNCTION_NAME, GEO_DISTANCE_FUNCTION_NAME,
    LIN_DECAY_FUNCTION_NAME, PAYLOAD_DATETIME_FUNCTION_NAME, PAYLOAD_NUM_FUNCTION_NAME,
    qdrant_condition, qdrant_datetime_value, qdrant_exp_decay, qdrant_formula_score,
    qdrant_gauss_decay, qdrant_geo_distance, qdrant_lin_decay, qdrant_payload_datetime,
    qdrant_payload_num,
};
pub(crate) use self::formula::{
    ConditionCall, DatetimeValueCall, DecayCall, DecayKind, FormulaCall, GeoDistanceCall,
    PayloadDatetimeCall, PayloadNumCall, qdrant_condition_udf, qdrant_datetime_value_udf,
    qdrant_exp_decay_udf, qdrant_formula_score_udf, qdrant_gauss_decay_udf,
    qdrant_geo_distance_udf, qdrant_lin_decay_udf, qdrant_payload_datetime_udf,
    qdrant_payload_num_udf,
};
pub use self::fusion::{
    FUSION_SCORE_FUNCTION_NAME, qdrant_fusion_score, qdrant_fusion_score_with_inputs,
};
pub(crate) use self::fusion::{FusionCall, qdrant_fusion_score_udf};
pub use self::nearest::{
    NEAREST_DOCUMENT_SCORE_FUNCTION_NAME, NEAREST_ID_SCORE_FUNCTION_NAME,
    NEAREST_IMAGE_SCORE_FUNCTION_NAME, NEAREST_MULTI_SCORE_FUNCTION_NAME,
    NEAREST_OBJECT_SCORE_FUNCTION_NAME, NEAREST_SCORE_FUNCTION_NAME,
    NEAREST_SPARSE_SCORE_FUNCTION_NAME, qdrant_nearest_document_score, qdrant_nearest_id_score,
    qdrant_nearest_image_score, qdrant_nearest_multi_score, qdrant_nearest_object_score,
    qdrant_nearest_score, qdrant_nearest_sparse_score,
};
pub(crate) use self::nearest::{
    NearestCall, qdrant_nearest_document_score_udf, qdrant_nearest_id_score_udf,
    qdrant_nearest_image_score_udf, qdrant_nearest_multi_score_udf,
    qdrant_nearest_object_score_udf, qdrant_nearest_score_udf, qdrant_nearest_sparse_score_udf,
};
pub use self::nearest_with_mmr::{
    NEAREST_WITH_MMR_SCORE_FUNCTION_NAME, qdrant_nearest_with_mmr_score,
};
pub(crate) use self::nearest_with_mmr::{NearestWithMmrCall, qdrant_nearest_with_mmr_score_udf};
pub use self::order_by::{ORDER_BY_SCORE_FUNCTION_NAME, qdrant_order_by_score};
pub(crate) use self::order_by::{OrderByCall, qdrant_order_by_score_udf};
pub use self::payload::{PAYLOAD_FUNCTION_NAME, qdrant_payload};
pub(crate) use self::payload::{is_payload_function_name, qdrant_payload_udf};
pub(crate) use self::payload_access::{
    is_payload_access_function_name, payload_access_expr, qdrant_payload_bool_access_udf,
    qdrant_payload_datetime_access_udf, qdrant_payload_float_access_udf,
    qdrant_payload_int_access_udf, qdrant_payload_text_access_udf,
};
pub(crate) use self::payload_geo::{
    PAYLOAD_GEO_DISTANCE_ACCESS_FUNCTION_NAME, is_payload_geo_distance_function_name,
    qdrant_payload_geo_distance_access_udf, qdrant_payload_geo_distance_udf,
};
pub use self::payload_geo::{PAYLOAD_GEO_DISTANCE_FUNCTION_NAME, qdrant_payload_geo_distance};
pub(crate) use self::payload_predicate::{
    PAYLOAD_IS_EMPTY_ACCESS_FUNCTION_NAME, PAYLOAD_VALUES_COUNT_ACCESS_FUNCTION_NAME,
    is_payload_is_empty_function_name, is_payload_values_count_function_name,
    qdrant_payload_is_empty_access_udf, qdrant_payload_is_empty_udf,
    qdrant_payload_values_count_access_udf, qdrant_payload_values_count_udf,
};
pub use self::payload_predicate::{
    PAYLOAD_IS_EMPTY_FUNCTION_NAME, PAYLOAD_VALUES_COUNT_FUNCTION_NAME, qdrant_payload_is_empty,
    qdrant_payload_values_count,
};
pub(crate) use self::payload_text::{
    PAYLOAD_PHRASE_MATCH_ACCESS_FUNCTION_NAME, PAYLOAD_TEXT_MATCH_ACCESS_FUNCTION_NAME,
    is_payload_phrase_match_function_name, is_payload_text_match_function_name,
    qdrant_payload_phrase_match_access_udf, qdrant_payload_phrase_match_udf,
    qdrant_payload_text_match_access_udf, qdrant_payload_text_match_udf,
};
pub use self::payload_text::{
    PAYLOAD_PHRASE_MATCH_FUNCTION_NAME, PAYLOAD_TEXT_MATCH_FUNCTION_NAME,
    qdrant_payload_phrase_match, qdrant_payload_text_match,
};
pub use self::recommend::{
    RECOMMEND_SCORE_FUNCTION_NAME, qdrant_recommend_score, qdrant_recommend_score_with_strategy,
};
pub(crate) use self::recommend::{RecommendCall, qdrant_recommend_score_udf};
pub use self::relevance_feedback::{
    RELEVANCE_FEEDBACK_SCORE_FUNCTION_NAME, qdrant_relevance_feedback_score,
};
pub(crate) use self::relevance_feedback::{
    RelevanceFeedbackCall, qdrant_relevance_feedback_score_udf,
};
pub use self::sample::{SAMPLE_SCORE_FUNCTION_NAME, qdrant_sample_score};
pub(crate) use self::sample::{SampleCall, qdrant_sample_score_udf};

pub(crate) fn register_functions(ctx: &SessionContext) {
    ctx.register_udf(qdrant_nearest_score_udf());
    ctx.register_udf(qdrant_nearest_sparse_score_udf());
    ctx.register_udf(qdrant_nearest_multi_score_udf());
    ctx.register_udf(qdrant_nearest_id_score_udf());
    ctx.register_udf(qdrant_nearest_document_score_udf());
    ctx.register_udf(qdrant_nearest_image_score_udf());
    ctx.register_udf(qdrant_nearest_object_score_udf());
    ctx.register_udf(qdrant_recommend_score_udf());
    ctx.register_udf(qdrant_discover_score_udf());
    ctx.register_udf(qdrant_context_score_udf());
    ctx.register_udf(qdrant_order_by_score_udf());
    ctx.register_udf(qdrant_fusion_score_udf());
    ctx.register_udf(qdrant_sample_score_udf());
    ctx.register_udf(qdrant_formula_score_udf());
    ctx.register_udf(qdrant_payload_num_udf());
    ctx.register_udf(qdrant_payload_datetime_udf());
    ctx.register_udf(qdrant_payload_udf());
    ctx.register_udf(qdrant_payload_geo_distance_udf());
    ctx.register_udf(qdrant_payload_text_access_udf());
    ctx.register_udf(qdrant_payload_int_access_udf());
    ctx.register_udf(qdrant_payload_float_access_udf());
    ctx.register_udf(qdrant_payload_bool_access_udf());
    ctx.register_udf(qdrant_payload_datetime_access_udf());
    ctx.register_udf(qdrant_payload_geo_distance_access_udf());
    ctx.register_udf(qdrant_payload_is_empty_udf());
    ctx.register_udf(qdrant_payload_values_count_udf());
    ctx.register_udf(qdrant_payload_is_empty_access_udf());
    ctx.register_udf(qdrant_payload_values_count_access_udf());
    ctx.register_udf(qdrant_payload_text_match_udf());
    ctx.register_udf(qdrant_payload_phrase_match_udf());
    ctx.register_udf(qdrant_payload_text_match_access_udf());
    ctx.register_udf(qdrant_payload_phrase_match_access_udf());
    ctx.register_udf(qdrant_datetime_value_udf());
    ctx.register_udf(qdrant_condition_udf());
    ctx.register_udf(qdrant_geo_distance_udf());
    ctx.register_udf(qdrant_exp_decay_udf());
    ctx.register_udf(qdrant_gauss_decay_udf());
    ctx.register_udf(qdrant_lin_decay_udf());
    ctx.register_udf(qdrant_nearest_with_mmr_score_udf());
    ctx.register_udf(qdrant_relevance_feedback_score_udf());
}
