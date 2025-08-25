//! Various utility functions for working with schema and data.

use std::collections::HashSet;

use datafusion::arrow::datatypes::Schema;

use crate::builder::VectorSelection;
use crate::constants::RESERVED_FIELD_NAMES;

/// Build a vector selector based on the projected schema.
///
/// This function analyzes the `DataFusion` schema (after projection) to determine
/// which vector fields are actually needed, enabling efficient queries that only
/// fetch required data from `Qdrant`.
///
/// # Arguments
/// * `schema` - The Arrow schema (potentially projected) defining which fields are needed
/// * `reserved` - List of field names that should not be considered vectors
///
/// # Returns
/// A `VectorSelector` that tells `Qdrant` exactly which vectors to include in the response.
///
/// # Examples
/// ```rust,ignore
/// use datafusion::arrow::datatypes::{Schema, Field, DataType};
/// use qdrant_datafusion::utils::{build_vector_selector, VectorSelectorSpec};
/// use std::sync::Arc;
///
/// // Schema with only metadata - no vectors needed
/// let metadata_schema = Schema::new(vec![
///     Field::new("id", DataType::Utf8, false),
///     Field::new("payload", DataType::Utf8, true),
/// ]);
/// assert!(matches!(build_vector_selector(&metadata_schema), VectorSelectorSpec::None));
///
/// // Schema with unnamed vector - fetch all
/// let unnamed_schema = Schema::new(vec![
///     Field::new("id", DataType::Utf8, false),
///     Field::new("vector", DataType::List(Arc::new(Field::new("item", DataType::Float32, true))), true),
/// ]);
/// assert!(matches!(build_vector_selector(&unnamed_schema), VectorSelectorSpec::All));
/// ```
pub fn build_vector_selector(schema: &Schema, reserved: Option<&[&str]>) -> VectorSelection {
    let mut vector_names: HashSet<_> = schema
        .fields()
        .iter()
        .filter(|f| !RESERVED_FIELD_NAMES.contains(&f.name().as_str()))
        .filter(|f| !reserved.is_some_and(|r| r.contains(&f.name().as_str())))
        .map(|f| f.name())
        .map(|name| {
            if name.ends_with("_indices") || name.ends_with("_values") {
                // Extract base name for sparse vectors
                name.trim_end_matches("_indices").trim_end_matches("_values")
            } else {
                name
            }
        })
        .collect();

    if vector_names.is_empty() {
        return VectorSelection::None;
    }

    let has_unnamed_vector = vector_names.remove("vector");
    if has_unnamed_vector || vector_names.is_empty() {
        // Unnamed vector collection - use simple boolean selector
        VectorSelection::All
    } else {
        // Specific named vectors
        VectorSelection::Named(
            vector_names.into_iter().map(ToString::to_string).collect::<Vec<_>>(),
        )
    }
}

/// Determine if payload data should be included in `Qdrant` queries.
///
/// This function checks if the schema includes a "payload" field, which indicates
/// that payload data is needed and should be fetched from `Qdrant`.
///
/// # Arguments
/// * `schema` - The Arrow schema to analyze
///
/// # Returns
/// `true` if payload field is present in schema, `false` otherwise
///
/// # Examples
/// ```rust,ignore
/// use datafusion::arrow::datatypes::{Schema, Field, DataType};
/// use qdrant_datafusion::utils::build_payload_selector;
///
/// // Schema with payload field
/// let with_payload = Schema::new(vec![
///     Field::new("id", DataType::Utf8, false),
///     Field::new("payload", DataType::Utf8, true),
/// ]);
/// assert_eq!(build_payload_selector(&with_payload), true);
///
/// // Schema without payload
/// let no_payload = Schema::new(vec![
///     Field::new("id", DataType::Utf8, false),
/// ]);
/// assert_eq!(build_payload_selector(&no_payload), false);
/// ```
pub fn build_payload_selector(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| f.name() == "payload")
}
