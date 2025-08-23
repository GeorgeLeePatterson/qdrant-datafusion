use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    CollectionConfig, Datatype, QueryPointsBuilder, VectorsSelector, vectors_config,
};

use super::arrow::create_vector_field;
use crate::arrow::{
    extract_dense_vector_from_scored, extract_ids_from_scored, extract_payload_from_scored,
    extract_sparse_indices_from_scored, extract_sparse_values_from_scored,
};
use crate::error::{Error, Result};
use crate::stream::QdrantQueryStream;
use crate::utils;

/// `TableProvider` for Qdrant collections.
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
    /// # Errors
    /// - Returns an error if the collection info or the vector params is missing.
    pub async fn try_new(client: Qdrant, collection: &str) -> Result<Self> {
        let info = client.collection_info(collection).await?;

        // Get the config
        let config = info
            .result
            .ok_or(Error::MissingCollectionInfo(collection.into()))?
            .config
            .ok_or(Error::MissingCollectionInfo(collection.into()))?;
        let schema = Self::build_schema(collection, &config)?;

        Ok(Self {
            table:  TableReference::bare(collection),
            client: Arc::new(client),
            schema: Arc::new(schema),
        })
    }

    fn build_schema(collection: &str, config: &CollectionConfig) -> Result<Schema> {
        let mut fields = vec![
            // Always present - the point ID (can be numeric or UUID string)
            Field::new("id", DataType::Utf8, false),
        ];

        // Get the params from config
        let params =
            config.params.as_ref().ok_or(Error::MissingCollectionInfoParams(collection.into()))?;

        // Parse vectors_config if present
        if let Some(vectors_config) = &params.vectors_config
            && let Some(ref config) = vectors_config.config
        {
            match config {
                vectors_config::Config::Params(vector_params) => {
                    // Single unnamed vector
                    fields.push(Field::new(
                        "vector",
                        DataType::List(create_vector_field("item", vector_params.datatype(), true)),
                        false,
                    ));
                }
                vectors_config::Config::ParamsMap(params_map) => {
                    // Multiple named vectors
                    for (name, vector_params) in &params_map.map {
                        // Check if it's a multi-vector based on multivector_config
                        if vector_params.multivector_config.is_some() {
                            // Multi-vector (list of lists)
                            fields.push(Field::new(
                                name,
                                DataType::List(Arc::new(Field::new(
                                    "item",
                                    DataType::List(create_vector_field(
                                        "item",
                                        vector_params.datatype(),
                                        true,
                                    )),
                                    true,
                                ))),
                                false,
                            ));
                        } else {
                            // Regular dense vector
                            fields.push(Field::new(
                                name,
                                DataType::List(create_vector_field(
                                    "item",
                                    vector_params.datatype(),
                                    true,
                                )),
                                false,
                            ));
                        }
                    }
                }
            }
        }

        // Parse sparse_vectors_config if present
        if let Some(sparse_config) = &params.sparse_vectors_config {
            // SparseVectorConfig has a map field
            for name in sparse_config.map.keys() {
                // Sparse indices are always u32, regardless of index config datatype
                fields.push(Field::new(
                    format!("{name}_indices"),
                    DataType::List(Field::new("item", DataType::UInt32, true).into()),
                    true,
                ));
                // Sparse values are always f32
                fields.push(Field::new(
                    format!("{name}_values"),
                    DataType::List(create_vector_field("item", Datatype::Float32, true)),
                    true,
                ));
            }
        }

        // Payload as JSON string
        fields.push(Field::new("payload", DataType::Utf8, true));

        Ok(Schema::new(fields))
    }
}

#[async_trait::async_trait]
impl TableProvider for QdrantTableProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }

    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Apply projection to schema ONCE, here
        let projected_schema = match projection {
            Some(indices) if !indices.is_empty() => Arc::new(self.schema.project(indices)?),
            _ => Arc::clone(&self.schema),
        };

        // Build selectors based on what fields are in the projected schema
        let vector_selector = utils::build_vector_selector(&projected_schema);
        let payload_selector = utils::build_payload_selector(&projected_schema);

        // For now, ignore filters - we'll handle them with UDFs later
        Ok(Arc::new(QdrantScanExec::new(
            Arc::clone(&self.client),
            self.table.table().to_string(),
            projected_schema,
            vector_selector,
            payload_selector,
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

pub struct QdrantScanExec {
    client:           Arc<Qdrant>,
    collection:       String,
    schema:           SchemaRef, // Already projected
    vector_selector:  utils::VectorSelectorSpec,
    payload_selector: bool,
    limit:            Option<usize>,
    properties:       PlanProperties,
}

impl std::fmt::Debug for QdrantScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantScanExec")
            .field("client", &"Qdrant")
            .field("collection", &self.collection)
            .field("schema", &self.schema)
            .field("vector_selector", &self.vector_selector)
            .field("payload_selector", &self.payload_selector)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl QdrantScanExec {
    pub fn new(
        client: Arc<Qdrant>,
        collection: String,
        schema: SchemaRef,
        vector_selector: utils::VectorSelectorSpec,
        payload_selector: bool,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            Boundedness::Bounded,
        );

        Self { client, collection, schema, vector_selector, payload_selector, limit, properties }
    }
}

/// Execute a `Qdrant` query and return a `RecordBatch`.
///
/// # Errors
/// - Returns an error if the query fails.
pub async fn execute_qdrant_query(
    client: Arc<Qdrant>,
    collection: String,
    schema: SchemaRef,
    vector_selector: utils::VectorSelectorSpec,
    payload_selector: bool,
    limit: Option<usize>,
) -> DataFusionResult<RecordBatch> {
    // Build query using QueryPointsBuilder
    let mut query_builder = QueryPointsBuilder::new(&collection);

    // Use the builder's API which accepts Into<SelectorOptions>
    match vector_selector {
        utils::VectorSelectorSpec::None => {
            query_builder = query_builder.with_vectors(false);
        }
        utils::VectorSelectorSpec::All => {
            query_builder = query_builder.with_vectors(true);
        }
        utils::VectorSelectorSpec::Named(names) => {
            query_builder = query_builder.with_vectors(VectorsSelector { names });
        }
    }

    query_builder = query_builder.with_payload(payload_selector);

    if let Some(limit_val) = limit {
        query_builder = query_builder.limit(limit_val as u64);
    }

    // Execute query
    let response =
        client.query(query_builder).await.map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Convert points to RecordBatch using existing logic
    let points = response.result;

    if points.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let array = match field.name().as_str() {
            "id" => extract_ids_from_scored(&points),
            "payload" => extract_payload_from_scored(&points),
            name if name.ends_with("_indices") => {
                let base_name = name.trim_end_matches("_indices");
                extract_sparse_indices_from_scored(&points, base_name)
            }
            name if name.ends_with("_values") => {
                let base_name = name.trim_end_matches("_values");
                extract_sparse_values_from_scored(&points, base_name)
            }
            name => extract_dense_vector_from_scored(&points, name),
        };
        arrays.push(array);
    }

    RecordBatch::try_new(schema, arrays).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
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
        let vector_selector = self.vector_selector.clone();
        let payload_selector = self.payload_selector;
        let limit = self.limit;
        let inner = Box::pin(futures_util::stream::once(async move {
            execute_qdrant_query(
                client,
                collection,
                schema,
                vector_selector,
                payload_selector,
                limit,
            )
            .await
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
