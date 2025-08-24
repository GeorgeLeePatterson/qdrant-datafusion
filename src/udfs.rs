//! `datafusion-functions-json` and other functions relevant for Qdrant.

use datafusion::execution::FunctionRegistry;
// Re-export
pub use datafusion_functions_json;

use crate::error::Result;

/// Register JSON-related UDFs with the given function registry.
///
/// # Errors
/// - Returns an error if any of the JSON-related UDFs fail to register.
pub fn register_json_udfs(ctx: &mut dyn FunctionRegistry) -> Result<()> {
    datafusion_functions_json::register_all(ctx)?;
    Ok(())
}
