//! ## Convenience exports for working with the library.
//!
//! To simplify compatibility, [`qdrant_client`] is re-exported

/// Re-exports
mod reexports {
    pub use qdrant_client;
}

pub use reexports::*;

pub use crate::context::{
    QDRANT_SCORE_FIELD_NAME, QdrantNearestQuery, QdrantSessionContext, prepare_session_context,
};
pub use crate::error::Result;
pub use crate::table::{QdrantScanExec, QdrantTableProvider};
