//! UDF registration hooks for `qdrant-datafusion`.
//!
//! The crate is intentionally starting from a minimal baseline on the same `DataFusion`
//! revision as `ndatafusion`. Additional UDFs can be introduced later once they are
//! justified and implemented on that aligned dependency line.

use datafusion::execution::FunctionRegistry;

use crate::error::Result;

/// Register `qdrant-datafusion` UDFs with the given function registry.
///
/// No crate-specific UDFs are registered yet.
///
/// # Errors
/// Returns an error if future UDF registration fails. The current baseline always succeeds.
pub fn register_json_udfs(_ctx: &mut dyn FunctionRegistry) -> Result<()> { Ok(()) }
