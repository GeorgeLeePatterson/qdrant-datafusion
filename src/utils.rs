//! Various utility functions for working with schema and data.

use std::collections::HashSet;

use datafusion::arrow::datatypes::Schema;

#[derive(Debug, Clone)]
pub enum VectorSelectorSpec {
    None,
    All,
    Named(Vec<String>),
}

pub fn build_vector_selector(schema: &Schema) -> VectorSelectorSpec {
    let mut vector_names: HashSet<_> = schema
        .fields()
        .iter()
        .filter(|f| !["id", "payload"].contains(&f.name().as_str()))
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
        return VectorSelectorSpec::None;
    }

    let has_unnamed_vector = vector_names.remove("vector");
    if has_unnamed_vector || vector_names.is_empty() {
        // Unnamed vector collection - use simple boolean selector
        VectorSelectorSpec::All
    } else {
        // Specific named vectors
        VectorSelectorSpec::Named(
            vector_names.into_iter().map(ToString::to_string).collect::<Vec<_>>(),
        )
    }
}

pub fn build_payload_selector(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| f.name() == "payload")
}
