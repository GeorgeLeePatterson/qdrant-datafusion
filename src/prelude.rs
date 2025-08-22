//! ## Convenience exports for working with the library.
//!
//! To simplify compatibility, [`qdrant_client`] is re-exported

/// Re-exports
mod reexports {
    pub use qdrant_client;
}

pub use reexports::*;

pub use crate::error::Result;
