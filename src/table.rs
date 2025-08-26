//! `DataFusion` `TableProvider` implementation for `Qdrant` vector database collections.
use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use qdrant_client::Qdrant;
use tracing::debug;

use crate::arrow::deserialize::QdrantRecordBatchBuilder;
use crate::arrow::schema::collection_to_arrow_schema;
use crate::builder::QdrantQueryBuilder;
use crate::error::{Error, Result};
use crate::stream::QdrantQueryStream;
use crate::utils;

/// `DataFusion` `TableProvider` implementation for `Qdrant` vector database collections.
///
/// This is the main interface for integrating `Qdrant` collections with `DataFusion` SQL queries.
/// It provides a complete SQL interface over vector data with support for all `Qdrant` vector
/// types, schema projection optimization, and heterogeneous collection handling.
///
/// # Features
/// - **Complete Vector Support**: Dense, multi-dense, and sparse vectors
/// - **Schema Projection**: Only fetches vector fields that are actually requested
/// - **Heterogeneous Collections**: Handles points with different vector field subsets
/// - **High Performance**: Single-pass processing with minimal allocations
///
/// # Examples
///
/// ## Basic Usage
/// ```rust,ignore
/// use qdrant_datafusion::prelude::*;
/// use qdrant_client::Qdrant;
/// use datafusion::prelude::*;
/// use std::sync::Arc;
///
/// # async fn example() -> Result<()> {
/// // Connect to Qdrant
/// let client = Qdrant::from_url("http://localhost:6334").build()?;
///
/// // Create table provider for a collection
/// let table_provider = QdrantTableProvider::try_new(client, "my_vectors").await?;
///
/// // Register with DataFusion
/// let ctx = SessionContext::new();
/// ctx.register_table("vectors", Arc::new(table_provider))?;
///
/// // Query with SQL
/// let df = ctx.sql("SELECT id, embedding FROM vectors LIMIT 10").await?;
/// let results = df.collect().await?;
/// # Ok(())
/// # }
/// ```
///
/// ## Advanced Projections
/// ```rust,no_run
/// # use qdrant_datafusion::prelude::*;
/// # use datafusion::prelude::*;
/// # async fn example(ctx: SessionContext) -> Result<()> {
/// // Only fetch specific vector fields (optimized query to Qdrant)
/// let df = ctx.sql("
///     SELECT
///         text_embedding,
///         keywords_indices,
///         keywords_values
///     FROM mixed_vectors
///     WHERE id = 'doc123'
/// ").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct QdrantTableProvider {
    table:  TableReference,
    client: Arc<Qdrant>,
    schema: Arc<Schema>,
}

impl std::fmt::Debug for QdrantTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantTableProvider")
            .field("table", &self.table)
            .field("client", &"Qdrant")
            .field("schema", &self.schema)
            .finish()
    }
}

impl QdrantTableProvider {
    /// Create a new `QdrantTableProvider` for the specified `Qdrant` collection.
    ///
    /// This constructor connects to the `Qdrant` collection, analyzes its schema, and creates
    /// a DataFusion-compatible table provider. The schema is built by examining the collection's
    /// vector configuration and creating appropriate Arrow fields for all vector types.
    ///
    /// # Arguments
    /// * `client` - Connected `Qdrant` client instance
    /// * `collection` - Name of the `Qdrant` collection to provide access to
    ///
    /// # Returns
    /// A configured `QdrantTableProvider` ready for SQL queries.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The collection does not exist or is inaccessible
    /// - The collection configuration cannot be retrieved
    /// - The collection has an unsupported schema configuration
    ///
    /// # Examples
    /// ```rust,ignore
    /// use qdrant_datafusion::prelude::*;
    /// use qdrant_client::Qdrant;
    ///
    /// # async fn example() -> Result<()> {
    /// let client = Qdrant::from_url("http://localhost:6334")
    ///     .api_key("optional-api-key")
    ///     .build()?;
    ///
    /// let table_provider = QdrantTableProvider::try_new(client, "embeddings").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn try_new(client: Qdrant, collection: &str) -> Result<Self> {
        let info = client.collection_info(collection).await?;
        // Get the config
        let config = info
            .result
            .ok_or(Error::MissingCollectionInfo(collection.into()))?
            .config
            .ok_or(Error::MissingCollectionInfo(collection.into()))?;
        let schema = collection_to_arrow_schema(collection, &config)?;
        Ok(Self {
            table:  TableReference::bare(collection),
            client: Arc::new(client),
            schema: Arc::new(schema),
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for QdrantTableProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }

    fn table_type(&self) -> TableType { TableType::Base }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|&filter| match crate::expr::filters::analyze_filter_expr(filter) {
                Ok(crate::expr::filters::FilterResult::Condition(_)) => {
                    Ok(TableProviderFilterPushDown::Exact)
                }
                Ok(crate::expr::filters::FilterResult::Unsupported(_)) => {
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
                Err(_) => Ok(TableProviderFilterPushDown::Unsupported),
            })
            .collect()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Apply projection to schema ONCE, here
        let projected_schema = match projection {
            Some(indices) if !indices.is_empty() => Arc::new(self.schema.project(indices)?),
            _ => Arc::clone(&self.schema),
        };

        // Filters are handled by QdrantQueryBuilder.with_payload_filters()
        Ok(Arc::new(QdrantScanExec::new(
            Arc::clone(&self.client),
            self.table.table().to_string(),
            projected_schema,
            filters,
            limit,
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}

/// `DataFusion` `ExecutionPlan` implementation for scanning Qdrant collections.
///
/// This is the physical execution plan node that actually performs queries against `Qdrant`.
/// It's created by the `QdrantTableProvider` during query planning and handles the execution
/// of `Qdrant` queries with optimizations like vector field selection and payload filtering.
///
/// # Features
/// - **Optimized Vector Selection**: Only fetches vector fields that are needed
/// - **Schema Projection**: Respects `DataFusion` column pruning
/// - **Async Streaming**: Non-blocking execution with proper backpressure
/// - **Limit Pushdown**: Limit constraints are pushed to Qdrant for efficiency
///
/// This struct is typically not used directly - it's created automatically by the
/// `QdrantTableProvider` during SQL query execution.
#[derive(Clone)]
pub struct QdrantScanExec {
    client:     Arc<Qdrant>,
    collection: String,
    schema:     SchemaRef, // Already projected
    filter:     Arc<[Expr]>,
    limit:      Option<usize>,
    properties: PlanProperties,
}

impl std::fmt::Debug for QdrantScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantScanExec")
            .field("client", &"Qdrant")
            .field("collection", &self.collection)
            .field("schema", &self.schema)
            .field("filter", &self.filter)
            .field("limit", &self.limit)
            .field("properties", &self.properties)
            .finish_non_exhaustive()
    }
}

impl QdrantScanExec {
    pub fn new(
        client: Arc<Qdrant>,
        collection: String,
        schema: SchemaRef,
        filter: &[Expr],
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            Boundedness::Bounded,
        );

        Self { client, collection, schema, filter: Arc::from(filter), limit, properties }
    }
}

/// Execute a `Qdrant` query and return a `RecordBatch`.
///
/// # Errors
/// - Returns an error if the query fails.
pub(crate) async fn execute_qdrant_table_scan(
    client: Arc<Qdrant>,
    collection: String,
    schema: SchemaRef,
    filters: &[Expr],
    limit: Option<usize>,
) -> DataFusionResult<RecordBatch> {
    debug!("Executing Qdrant table scan for collection {collection}");

    // Build selectors based on what fields are in the projected schema
    let vector_selection = utils::build_vector_selector_from_schema(&schema, None);
    let payload_selection = utils::build_payload_selector_from_schema(&schema);

    // Build and execute query using our QdrantQueryBuilder
    let response = QdrantQueryBuilder::new(client, collection, Arc::clone(&schema))
        .with_vectors(vector_selection)?
        .with_payload(payload_selection)
        .with_payload_filters(filters)
        .with_limit(limit)
        .execute()
        .await?;

    // Convert points to RecordBatch using incremental builder
    let points = response.result;

    if points.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Create incremental builder with pre-allocated capacity
    let mut builder = QdrantRecordBatchBuilder::new(schema, points.len());

    // Single pass through points with true owned iteration
    for point in points {
        builder.append_point(point); // Pass owned point, not borrowed
    }

    builder.finish()
}

impl ExecutionPlan for QdrantScanExec {
    fn name(&self) -> &'static str { "QdrantScanExec" }

    fn as_any(&self) -> &dyn Any { self }

    fn properties(&self) -> &PlanProperties { &self.properties }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let client = Arc::clone(&self.client);
        let collection = self.collection.clone();
        let schema = Arc::clone(&self.schema);
        let filter = Arc::clone(&self.filter);
        let limit = self.limit;
        let inner = Box::pin(futures_util::stream::once(async move {
            execute_qdrant_table_scan(client, collection, schema, &filter, limit).await
        }));
        let stream = QdrantQueryStream::new(Arc::clone(&self.schema), inner);
        Ok(Box::pin(stream))
    }
}

impl DisplayAs for QdrantScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "QdrantScanExec: collection={}", self.collection)?;
                if let Some(limit) = self.limit {
                    write!(f, ", limit={limit}")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                write!(f, "QdrantScanExec: collection={}", self.collection)
            }
        }
    }
}
