//! ## Convenience exports for working with the library.
//!
//! To simplify compatibility, [`qdrant_client`] is re-exported

/// Re-exports
mod reexports {
    pub use qdrant_client;
}

pub use reexports::*;

pub use crate::context::{QdrantSessionContext, prepare_session_context};
pub use crate::error::Result;
pub use crate::expr_fn::{
    qdrant_condition, qdrant_context_score, qdrant_datetime_value, qdrant_discover_score,
    qdrant_exp_decay, qdrant_formula_score, qdrant_fusion_score, qdrant_fusion_score_with_inputs,
    qdrant_gauss_decay, qdrant_geo_distance, qdrant_lin_decay, qdrant_nearest_document_score,
    qdrant_nearest_id_score, qdrant_nearest_image_score, qdrant_nearest_multi_score,
    qdrant_nearest_object_score, qdrant_nearest_score, qdrant_nearest_sparse_score,
    qdrant_nearest_with_mmr_score, qdrant_order_by_score, qdrant_payload, qdrant_payload_datetime,
    qdrant_payload_geo_distance, qdrant_payload_geo_within_bbox, qdrant_payload_geo_within_polygon,
    qdrant_payload_is_empty, qdrant_payload_num, qdrant_payload_phrase_match,
    qdrant_payload_text_any, qdrant_payload_text_match, qdrant_payload_values_count,
    qdrant_recommend_score, qdrant_recommend_score_with_strategy, qdrant_relevance_feedback_score,
    qdrant_sample_score,
};
pub use crate::table::{QdrantScanExec, QdrantTableProvider};
