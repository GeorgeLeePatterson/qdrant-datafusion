//! Various utility functions for working with schema and data.

use std::collections::HashSet;

use datafusion::arrow::datatypes::Schema;

/// Specification for selecting which vectors to retrieve from `Qdrant`.
#[derive(Debug, Clone)]
pub enum VectorSelectorSpec {
    /// No vectors needed - only metadata fields (id, payload) requested.
    None,
    /// All vectors needed - either unnamed collection or all named vectors requested.
    All,
    /// Specific named vectors needed - only fetch these vector fields.
    Named(Vec<String>),
}

/// Build an optimal vector selector based on the projected schema.
pub fn build_vector_selector(schema: &Schema) -> VectorSelectorSpec {
    let mut vector_names: HashSet<_> = schema
        .fields()
        .iter()
        .filter(|field| !["id", "payload"].contains(&field.name().as_str()))
        .map(|field| field.name().clone())
        .collect();

    if vector_names.is_empty() {
        return VectorSelectorSpec::None;
    }

    let has_unnamed_vector = vector_names.remove("vector");
    if has_unnamed_vector || vector_names.is_empty() {
        VectorSelectorSpec::All
    } else {
        VectorSelectorSpec::Named(vector_names.into_iter().collect())
    }
}

/// Determine if payload data should be included in `Qdrant` queries.
pub fn build_payload_selector(schema: &Schema) -> bool {
    schema.fields().iter().any(|field| field.name() == "payload")
}
