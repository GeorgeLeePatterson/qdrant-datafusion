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
pub use crate::expr_fn::qdrant_nearest_score;
pub use crate::table::{QdrantScanExec, QdrantTableProvider};
