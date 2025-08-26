//! Query builder for translating `DataFusion` concepts to Qdrant queries.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::Expr;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{QueryPointsBuilder, QueryResponse, VectorsSelector};

use crate::expr::filters::translate_payload_filters;

/// Specification for selecting which vectors to retrieve from `Qdrant`.
///
/// This enum determines what vector data will be requested from `Qdrant` during query execution.
#[derive(Debug, Clone)]
pub enum VectorSelection {
    /// No vectors in response
    None,
    /// All vectors from collection
    All,
    /// Specific named vectors
    Named(Vec<String>),
}

/// High-level builder for constructing Qdrant queries from `DataFusion` concepts.
///
/// This builder abstracts the translation from `DataFusion` `Expr` filters, projections,
/// and other query concepts into the appropriate Qdrant query builders. It serves as
/// the "builder of builders" that will be extended to support different query types
/// (filtering, vector search, recommendations, etc.) in the future.
///
/// # Examples
/// ```rust,ignore
/// use qdrant_datafusion::builder::{QdrantQueryBuilder, VectorSelection};
///
/// let response = QdrantQueryBuilder::new(client, "collection", schema)
///     .with_vectors(VectorSelection::Named(vec!["text_embedding".to_string()]))?
///     .with_payload_filters(&filters)
///     .with_payload(true)
///     .with_limit(Some(10))
///     .execute().await?;
/// ```
pub struct QdrantQueryBuilder {
    client:           Arc<Qdrant>,
    collection:       String,
    schema:           SchemaRef,
    vector_selection: VectorSelection,
    include_payload:  bool,
    payload_filters:  Vec<Expr>,
    limit:            Option<usize>,
}

impl QdrantQueryBuilder {
    /// Create a new query builder with schema for validation context.
    pub fn new(client: Arc<Qdrant>, collection: String, schema: SchemaRef) -> Self {
        Self {
            client,
            collection,
            schema,
            vector_selection: VectorSelection::All,
            include_payload: true,
            payload_filters: Vec::new(),
            limit: None,
        }
    }

    /// Set which vectors to include in results with schema validation.
    ///
    /// # Errors
    /// Returns an error if named vectors are not found in the collection schema.
    pub fn with_vectors(mut self, selection: VectorSelection) -> DataFusionResult<Self> {
        // Validate that requested vector names exist in schema
        if let VectorSelection::Named(ref names) = selection {
            for name in names {
                if !self.schema_has_vector_field(name) {
                    return Err(DataFusionError::Plan(format!(
                        "Vector field '{name}' not found in collection schema"
                    )));
                }
            }
        }
        self.vector_selection = selection;
        Ok(self)
    }

    /// Add `DataFusion` expressions for payload filtering.
    ///
    /// These expressions will be analyzed and translated to Qdrant filter conditions
    /// that operate on the point payload data.
    #[must_use]
    pub fn with_payload_filters(mut self, filters: &[Expr]) -> Self {
        self.payload_filters.extend_from_slice(filters);
        self
    }

    /// Set whether to include payload data in results.
    #[must_use]
    pub fn with_payload(mut self, include: bool) -> Self {
        self.include_payload = include;
        self
    }

    /// Set the query limit.
    #[must_use]
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Execute the query and return the Qdrant response.
    ///
    /// This method translates all the `DataFusion` concepts into a Qdrant `QueryPointsBuilder`,
    /// executes the query, and returns the raw `QueryResponse` for further processing.
    ///
    /// # Errors
    /// Returns an error if the query translation fails or the Qdrant query execution fails.
    pub async fn execute(self) -> DataFusionResult<QueryResponse> {
        // Build QueryPointsBuilder with current parameters
        let mut query_builder = QueryPointsBuilder::new(&self.collection);

        // Apply vector selection
        match &self.vector_selection {
            VectorSelection::None => {
                query_builder = query_builder.with_vectors(false);
            }
            VectorSelection::All => {
                query_builder = query_builder.with_vectors(true);
            }
            VectorSelection::Named(names) => {
                query_builder =
                    query_builder.with_vectors(VectorsSelector { names: names.clone() });
            }
        }

        // Apply payload inclusion
        query_builder = query_builder.with_payload(self.include_payload);

        // Apply limit
        if let Some(limit_val) = self.limit {
            query_builder = query_builder.limit(limit_val as u64);
        }

        // Apply payload filters
        if !self.payload_filters.is_empty() {
            let qdrant_filter = translate_payload_filters(&self.payload_filters, &self.schema)?;
            query_builder = query_builder.filter(qdrant_filter);
        }

        // Execute query
        self.client.query(query_builder).await.map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// Check if the schema contains a vector field with the given name.
    fn schema_has_vector_field(&self, field_name: &str) -> bool {
        // Check direct match first (dense/multi vectors)
        if self.schema.field_with_name(field_name).is_ok() {
            return true;
        }

        // Check for sparse vector pattern (allocation-free)
        self.schema.fields().iter().any(|f| {
            f.name().starts_with(field_name)
                && (f.name().ends_with("_indices") || f.name().ends_with("_values"))
        })
    }
}
