//! `DataFusion` `TableProvider` implementation for `Qdrant` vector database collections.
use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::*;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::exec_err;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::config::ConfigOptions;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SortOrderPushdownResult,
};
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    Condition, Direction, Filter, OrderByBuilder, RetrievedPoint, ScrollPointsBuilder,
    VectorsSelector, order_value, start_from,
};

use crate::arrow::deserialize::QdrantRecordBatchBuilder;
use crate::arrow::schema::{ID_FIELD_NAME, PAYLOAD_FIELD_NAME, collection_to_arrow_schema};
use crate::error::{Error, Result};
use crate::pushdown::{
    QdrantContinuation, QdrantFilters, QdrantOrderValue, QdrantOrderedContinuation, QdrantOrdering,
    QdrantPayloadSchema, QdrantPayloadSelector, QdrantScanSpec, QdrantVectorSelector,
};
use crate::stream::QdrantQueryStream;

/// `DataFusion` `TableProvider` implementation for `Qdrant` vector database collections.
///
/// This is the main scan interface for integrating `Qdrant` collections with `DataFusion` SQL
/// queries. The current admitted scope is narrow: collection-schema introspection, paginated
/// collection scans, projection-aware vector selection, and canonical Arrow materialization for
/// the supported `Qdrant` vector types.
///
/// # Features
/// - **Canonical Vector Carriers**: Dense, multi-dense, and sparse vectors
/// - **Schema Projection**: Only fetches vector fields that are actually requested
/// - **Heterogeneous Collections**: Handles points with different vector field subsets
/// - **Thin Scan Path**: Paginated `scroll` execution with compact batch materialization
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
///         keywords
///     FROM mixed_vectors
///     WHERE id = 'doc123'
/// ").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct QdrantTableProvider {
    table:          TableReference,
    client:         Arc<Qdrant>,
    schema:         Arc<Schema>,
    payload_schema: Arc<QdrantPayloadSchema>,
}

impl std::fmt::Debug for QdrantTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantTableProvider")
            .field("table", &self.table)
            .field("client", &"Qdrant")
            .field("schema", &self.schema)
            .field("payload_schema", &self.payload_schema)
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
        let info = info.result.ok_or(Error::MissingCollectionInfo(collection.into()))?;
        let payload_schema = Arc::new(QdrantPayloadSchema::from(info.payload_schema));
        let config = info.config.ok_or(Error::MissingCollectionInfo(collection.into()))?;
        let schema = collection_to_arrow_schema(collection, &config)?;
        Ok(Self {
            table: TableReference::bare(collection),
            client: Arc::new(client),
            schema: Arc::new(schema),
            payload_schema,
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
        Ok(filters
            .iter()
            .map(|filter| {
                if QdrantFilters::supports_exact(&self.schema, &self.payload_schema, filter) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let pushdown = Arc::new(QdrantScanSpec::try_new(
            &self.schema,
            &self.payload_schema,
            projection,
            filters,
            limit,
        )?);
        Ok(Arc::new(QdrantScanExec::new(
            Arc::clone(&self.client),
            self.table.table().to_string(),
            pushdown,
            Arc::clone(&self.payload_schema),
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        exec_err!("INSERT INTO is not supported for Qdrant tables")
    }
}

/// `DataFusion` `ExecutionPlan` implementation for scanning Qdrant collections.
///
/// This is the physical execution plan node that actually performs queries against `Qdrant`.
/// It's created by the `QdrantTableProvider` during query planning and handles the execution
/// of collection scans with optimizations like vector field selection, limit pushdown, and
/// exact `ORDER BY id ASC` pushdown.
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
    client:         Arc<Qdrant>,
    collection:     String,
    pushdown:       Arc<QdrantScanSpec>,
    payload_schema: Arc<QdrantPayloadSchema>,
    properties:     Arc<PlanProperties>,
}

impl std::fmt::Debug for QdrantScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantScanExec")
            .field("client", &"Qdrant")
            .field("collection", &self.collection)
            .field("pushdown", &self.pushdown)
            .field("payload_schema", &self.payload_schema)
            .finish_non_exhaustive()
    }
}

impl QdrantScanExec {
    fn new(
        client: Arc<Qdrant>,
        collection: String,
        pushdown: Arc<QdrantScanSpec>,
        payload_schema: Arc<QdrantPayloadSchema>,
    ) -> Self {
        let mut eq_properties =
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&pushdown.schema));
        if matches!(pushdown.ordering, QdrantOrdering::ById)
            && let Ok(index) = pushdown.schema.index_of(ID_FIELD_NAME)
        {
            eq_properties.add_orderings([vec![PhysicalSortExpr::new_default(Arc::new(
                Column::new(ID_FIELD_NAME, index),
            ))]]);
        }
        let properties = PlanProperties::new(
            eq_properties,
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            Boundedness::Bounded,
        );

        Self { client, collection, pushdown, payload_schema, properties: Arc::new(properties) }
    }
}

const SCAN_PAGE_SIZE: usize = 1024;

#[derive(Clone)]
struct QdrantScrollState {
    client:       Arc<Qdrant>,
    collection:   String,
    pushdown:     Arc<QdrantScanSpec>,
    remaining:    Option<usize>,
    continuation: QdrantContinuation,
}

impl QdrantOrderValue {
    fn start_from(self) -> start_from::Value {
        match self {
            Self::Integer(value) => start_from::Value::Integer(value),
            Self::Float(value) => start_from::Value::Float(value),
        }
    }
}

impl QdrantOrderedContinuation {
    fn next(mut self, points: &[RetrievedPoint]) -> DataFusionResult<Self> {
        let Some(last_point) = points.last() else {
            return Ok(self);
        };
        let last_value =
            match last_point.order_value.as_ref().and_then(|value| value.variant.as_ref()) {
                Some(order_value::Variant::Int(value)) => QdrantOrderValue::Integer(*value),
                Some(order_value::Variant::Float(value)) => QdrantOrderValue::Float(*value),
                None => return exec_err!("ordered row missing order value"),
            };
        let mut page_boundary_ids = vec![];
        for point in points.iter().rev() {
            let point_value =
                match point.order_value.as_ref().and_then(|value| value.variant.as_ref()) {
                    Some(order_value::Variant::Int(value)) => QdrantOrderValue::Integer(*value),
                    Some(order_value::Variant::Float(value)) => QdrantOrderValue::Float(*value),
                    None => return exec_err!("ordered row missing order value"),
                };
            if point_value != last_value {
                break;
            }
            page_boundary_ids.push(
                point
                    .id
                    .clone()
                    .ok_or_else(|| DataFusionError::Execution("ordered row missing id".into()))?,
            );
        }
        page_boundary_ids.reverse();
        if self.start_from.as_ref() == Some(&last_value) {
            self.boundary_ids.extend(page_boundary_ids);
        } else {
            self.boundary_ids = page_boundary_ids;
        }
        self.start_from = Some(last_value);
        Ok(self)
    }
}

impl QdrantScrollState {
    async fn execute_page(self) -> DataFusionResult<Option<(RecordBatch, Option<Self>)>> {
        let Self { client, collection, pushdown, remaining, continuation } = self;

        if remaining == Some(0) {
            return Ok(None);
        }

        let page_limit =
            remaining.map_or(SCAN_PAGE_SIZE, |remaining| remaining.min(SCAN_PAGE_SIZE));
        let page_limit = u32::try_from(page_limit).expect("scan page size fits in u32");
        let mut request = ScrollPointsBuilder::new(&collection)
            .limit(page_limit)
            .with_payload(matches!(pushdown.payload, QdrantPayloadSelector::Full));
        let mut filter = pushdown.filters.to_filter();
        let mut ordered = None;
        match &pushdown.vectors {
            QdrantVectorSelector::None => request = request.with_vectors(false),
            QdrantVectorSelector::All => request = request.with_vectors(true),
            QdrantVectorSelector::Named(names) => {
                request = request.with_vectors(VectorsSelector { names: names.clone() });
            }
        }
        match continuation {
            QdrantContinuation::Offset(Some(offset)) => request = request.offset(offset),
            QdrantContinuation::Offset(None) => {}
            QdrantContinuation::Ordered(next) => {
                let mut order_by = OrderByBuilder::new(&next.ordering.field).direction(
                    if next.ordering.descending {
                        Direction::Desc as i32
                    } else {
                        Direction::Asc as i32
                    },
                );
                if let Some(start_from) = next.start_from {
                    order_by = order_by.start_from(start_from.start_from());
                }
                request = request.order_by(order_by);
                if !next.boundary_ids.is_empty() {
                    filter
                        .get_or_insert_with(Filter::default)
                        .must_not
                        .push(Condition::has_id(next.boundary_ids.clone()));
                }
                ordered = Some(next);
            }
        }
        if let Some(filter) = filter {
            request = request.filter(filter);
        }

        let response = client
            .scroll(request)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let qdrant_client::qdrant::ScrollResponse { result, next_page_offset, .. } = response;

        if result.is_empty() {
            return Ok(None);
        }

        if ordered.is_some() && next_page_offset.is_some() {
            return exec_err!("ordered scroll returned id offset");
        }
        let ordered = ordered.map(|ordered| ordered.next(&result)).transpose()?;
        let point_count = result.len();
        let mut builder = QdrantRecordBatchBuilder::new(Arc::clone(&pushdown.schema), point_count)?;
        for point in result {
            builder.append_retrieved_point(point)?;
        }
        let batch = builder.finish()?;
        let remaining = remaining.map(|remaining| remaining.saturating_sub(point_count));

        let next_state = match (remaining, ordered, next_page_offset) {
            (Some(0), _, _) | (_, None, None) => None,
            (remaining, Some(ordered), _) => Some(Self {
                client,
                collection,
                pushdown,
                remaining,
                continuation: QdrantContinuation::Ordered(ordered),
            }),
            (remaining, None, Some(offset)) => Some(Self {
                client,
                collection,
                pushdown,
                remaining,
                continuation: QdrantContinuation::Offset(Some(offset)),
            }),
        };

        Ok(Some((batch, next_state)))
    }
}

impl ExecutionPlan for QdrantScanExec {
    fn name(&self) -> &'static str { "QdrantScanExec" }

    fn as_any(&self) -> &dyn Any { self }

    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> DataFusionResult<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        let [sort] = order else {
            return Ok(SortOrderPushdownResult::Unsupported);
        };
        if let Some(column) = sort.expr.as_any().downcast_ref::<Column>() {
            if column.name() != ID_FIELD_NAME || sort.options.descending {
                return Ok(SortOrderPushdownResult::Unsupported);
            }
            return Ok(SortOrderPushdownResult::Exact { inner: Arc::new(self.clone()) });
        }
        let Some(expr) = sort.expr.as_any().downcast_ref::<BinaryExpr>() else {
            return Ok(SortOrderPushdownResult::Unsupported);
        };
        if *expr.op() != datafusion::logical_expr::Operator::Colon {
            return Ok(SortOrderPushdownResult::Unsupported);
        }
        let Some(column) = expr.left().as_any().downcast_ref::<Column>() else {
            return Ok(SortOrderPushdownResult::Unsupported);
        };
        if column.name() != PAYLOAD_FIELD_NAME {
            return Ok(SortOrderPushdownResult::Unsupported);
        }
        let Some(path) = expr.right().as_any().downcast_ref::<Literal>().and_then(|literal| {
            match literal.value() {
                datafusion::common::ScalarValue::Utf8(Some(path)) => Some(path),
                _ => None,
            }
        }) else {
            return Ok(SortOrderPushdownResult::Unsupported);
        };
        let Some(ordering) = self.payload_schema.ordering_for(path, sort.options.descending) else {
            return Ok(SortOrderPushdownResult::Unsupported);
        };
        let mut pushdown = (*self.pushdown).clone();
        pushdown.ordering = QdrantOrdering::ByPayload(ordering);
        Ok(SortOrderPushdownResult::Exact {
            inner: Arc::new(Self::new(
                Arc::clone(&self.client),
                self.collection.clone(),
                Arc::new(pushdown),
                Arc::clone(&self.payload_schema),
            )),
        })
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DataFusionResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if phase != FilterPushdownPhase::Pre {
            return Ok(FilterPushdownPropagation::all_unsupported(child_pushdown_result));
        }
        let parent_filters = child_pushdown_result
            .parent_filters
            .iter()
            .map(|filter| Arc::clone(&filter.filter))
            .collect::<Vec<_>>();
        let (filters, support) = self.pushdown.filters.pushdown_physical(
            &self.pushdown.schema,
            &self.payload_schema,
            &parent_filters,
        );
        let support = support
            .into_iter()
            .map(|supported| if supported { PushedDown::Yes } else { PushedDown::No })
            .collect::<Vec<_>>();
        let propagation = FilterPushdownPropagation::with_parent_pushdown_result(support);
        if filters == self.pushdown.filters {
            return Ok(propagation);
        }
        let mut pushdown = (*self.pushdown).clone();
        pushdown.filters = filters;
        Ok(propagation.with_updated_node(Arc::new(Self::new(
            Arc::clone(&self.client),
            self.collection.clone(),
            Arc::new(pushdown),
            Arc::clone(&self.payload_schema),
        ))))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let state = Some(QdrantScrollState {
            client:       Arc::clone(&self.client),
            collection:   self.collection.clone(),
            pushdown:     Arc::clone(&self.pushdown),
            remaining:    self.pushdown.limit,
            continuation: self.pushdown.initial_continuation(),
        });
        let inner = Box::pin(futures_util::stream::try_unfold(state, |state| async move {
            let Some(state) = state else {
                return Ok(None);
            };
            state.execute_page().await
        }));
        let stream = QdrantQueryStream::new(Arc::clone(&self.pushdown.schema), inner);
        Ok(Box::pin(stream))
    }
}

impl DisplayAs for QdrantScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "QdrantScanExec: collection={}", self.collection)?;
                match &self.pushdown.ordering {
                    QdrantOrdering::ById => {}
                    QdrantOrdering::ByPayload(ordering) => {
                        write!(f, ", order_by={}", ordering.field)?;
                        if ordering.descending {
                            write!(f, " DESC")?;
                        }
                    }
                }
                if let Some(projection) = &self.pushdown.projection {
                    write!(f, ", projected_columns={}", projection.len())?;
                }
                if !self.pushdown.filters.is_empty() {
                    write!(f, ", pushed_filters={}", self.pushdown.filters.len())?;
                }
                if let Some(limit) = self.pushdown.limit {
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::compute::SortOptions;
    use datafusion::common::{Column as ExprColumn, ScalarValue};
    use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};
    use datafusion::physical_expr::expressions::{
        BinaryExpr as PhysicalBinaryExpr, Column, Literal,
    };
    use datafusion::physical_plan::coop::CooperativeExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{SortOrderPushdownResult, displayable};
    use datafusion::prelude::SessionContext;
    use futures_util::FutureExt;
    use qdrant_client::qdrant::{
        IntegerIndexParams, OrderValue, PayloadSchemaInfo, PointId, point_id,
    };

    use super::*;
    use crate::arrow::schema::{ID_FIELD_NAME, PAYLOAD_FIELD_NAME};

    fn test_provider(schema: Schema) -> QdrantTableProvider {
        QdrantTableProvider {
            table:          TableReference::bare("vectors"),
            client:         Arc::new(
                Qdrant::from_url("http://localhost:6334").build().expect("client"),
            ),
            schema:         Arc::new(schema),
            payload_schema: Arc::new(QdrantPayloadSchema::default()),
        }
    }

    fn payload_schema(
        entries: impl IntoIterator<Item = (&'static str, PayloadSchemaInfo)>,
    ) -> Arc<QdrantPayloadSchema> {
        Arc::new(QdrantPayloadSchema::from(
            entries
                .into_iter()
                .map(|(field, info)| (field.to_owned(), info))
                .collect::<std::collections::HashMap<_, _>>(),
        ))
    }

    fn scan_exec(provider: &QdrantTableProvider) -> Arc<QdrantScanExec> {
        let context = SessionContext::new();
        let state = context.state();
        provider
            .scan(&state, None, &[], None)
            .now_or_never()
            .expect("scan future is ready")
            .expect("scan plan")
            .as_any()
            .downcast_ref::<QdrantScanExec>()
            .expect("qdrant scan exec")
            .clone()
            .into()
    }

    fn logical_plan(provider: QdrantTableProvider, sql: &str) -> LogicalPlan {
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(provider)).expect("register table"));
        ctx.sql(sql)
            .now_or_never()
            .expect("sql future is ready")
            .expect("dataframe")
            .into_unoptimized_plan()
    }

    fn sort_expr(plan: &LogicalPlan) -> &Expr {
        match plan {
            LogicalPlan::Sort(sort) => &sort.expr[0].expr,
            LogicalPlan::Projection(projection) => match projection.input.as_ref() {
                LogicalPlan::Sort(sort) => &sort.expr[0].expr,
                input => panic!("expected sort under projection, got {input:?}"),
            },
            other => panic!("expected sort plan, got {other:?}"),
        }
    }

    fn assert_payload_string_access(expr: &Expr, expected_op: Operator, path: &str) {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
            panic!("expected binary expr, got {expr:?}");
        };
        assert_eq!(op, &expected_op);
        let Expr::Column(ExprColumn { name, .. }) = left.as_ref() else {
            panic!("expected payload column, got {left:?}");
        };
        assert_eq!(name, PAYLOAD_FIELD_NAME);
        assert_eq!(right.as_ref(), &Expr::Literal(ScalarValue::Utf8(Some(path.to_owned())), None),);
    }

    fn qdrant_scan(plan: &Arc<dyn ExecutionPlan>) -> &QdrantScanExec {
        if let Some(scan) = plan.as_any().downcast_ref::<QdrantScanExec>() {
            return scan;
        }
        if let Some(cooperative) = plan.as_any().downcast_ref::<CooperativeExec>() {
            return qdrant_scan(cooperative.input());
        }
        if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            return qdrant_scan(projection.input());
        }
        if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
            return qdrant_scan(filter.input());
        }
        if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
            return qdrant_scan(repartition.input());
        }
        panic!("expected qdrant scan exec in plan:\n{}", displayable(plan.as_ref()).indent(true));
    }

    #[test]
    fn scan_uses_all_for_unnamed_vector_contract() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new("vector", DataType::new_fixed_size_list(DataType::Float32, 3, false), true),
        ]));

        assert_eq!(scan_exec(&provider).pushdown.vectors, QdrantVectorSelector::All);
    }

    #[test]
    fn scan_ignores_non_vector_columns() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 3, false),
                true,
            ),
        ]));

        assert_eq!(
            scan_exec(&provider).pushdown.vectors,
            QdrantVectorSelector::Named(vec!["embedding".to_owned()]),
        );
    }

    #[test]
    fn sort_pushdown_is_exact_for_id_ascending() {
        let scan = scan_exec(&test_provider(Schema::new(vec![Field::new(
            ID_FIELD_NAME,
            DataType::Utf8,
            false,
        )])));
        let order = [PhysicalSortExpr::new(
            Arc::new(Column::new(ID_FIELD_NAME, 0)),
            SortOptions::default(),
        )];

        assert!(matches!(
            scan.try_pushdown_sort(&order).expect("sort pushdown"),
            SortOrderPushdownResult::Exact { .. }
        ));
    }

    #[test]
    fn sort_pushdown_rejects_descending_id() {
        let scan = scan_exec(&test_provider(Schema::new(vec![Field::new(
            ID_FIELD_NAME,
            DataType::Utf8,
            false,
        )])));
        let order = [PhysicalSortExpr::new(Arc::new(Column::new(ID_FIELD_NAME, 0)), SortOptions {
            descending:  true,
            nulls_first: false,
        })];

        assert!(matches!(
            scan.try_pushdown_sort(&order).expect("sort pushdown"),
            SortOrderPushdownResult::Unsupported
        ));
    }

    #[test]
    fn sort_pushdown_is_exact_for_payload_path() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
        ]));
        let scan = Arc::new(QdrantScanExec::new(
            Arc::clone(&provider.client),
            "vectors".to_owned(),
            Arc::new(
                QdrantScanSpec::try_new(
                    &provider.schema,
                    &provider.payload_schema,
                    None,
                    &[],
                    None,
                )
                .expect("scan spec"),
            ),
            payload_schema([(
                "rank",
                PayloadSchemaInfo {
                    data_type: qdrant_client::qdrant::PayloadSchemaType::Integer as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(
                            qdrant_client::qdrant::payload_index_params::IndexParams::IntegerIndexParams(
                                IntegerIndexParams {
                                    range: Some(true),
                                    ..Default::default()
                                },
                            ),
                        ),
                    }),
                    points: None,
                },
            )]),
        ));
        let order = [PhysicalSortExpr::new(
            Arc::new(PhysicalBinaryExpr::new(
                Arc::new(Column::new(PAYLOAD_FIELD_NAME, 1)),
                Operator::Colon,
                Arc::new(Literal::new(ScalarValue::Utf8(Some("rank".to_owned())))),
            )),
            SortOptions::default(),
        )];

        let SortOrderPushdownResult::Exact { inner } =
            scan.try_pushdown_sort(&order).expect("sort pushdown")
        else {
            panic!("expected exact payload sort pushdown");
        };
        let pushed = inner.as_any().downcast_ref::<QdrantScanExec>().expect("qdrant scan exec");

        assert_eq!(
            pushed.pushdown.ordering,
            QdrantOrdering::ByPayload(crate::pushdown::QdrantPayloadOrdering {
                field:      "rank".to_owned(),
                descending: false,
            }),
        );
    }

    #[test]
    fn sort_pushdown_rejects_unindexed_payload_path() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
        ]));
        let scan = Arc::new(QdrantScanExec::new(
            Arc::clone(&provider.client),
            "vectors".to_owned(),
            Arc::new(
                QdrantScanSpec::try_new(
                    &provider.schema,
                    &provider.payload_schema,
                    None,
                    &[],
                    None,
                )
                .expect("scan spec"),
            ),
            payload_schema([(
                "rank",
                PayloadSchemaInfo {
                    data_type: qdrant_client::qdrant::PayloadSchemaType::Integer as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(
                            qdrant_client::qdrant::payload_index_params::IndexParams::IntegerIndexParams(
                                IntegerIndexParams {
                                    range: Some(false),
                                    ..Default::default()
                                },
                            ),
                        ),
                    }),
                    points: None,
                },
            )]),
        ));
        let order = [PhysicalSortExpr::new(
            Arc::new(PhysicalBinaryExpr::new(
                Arc::new(Column::new(PAYLOAD_FIELD_NAME, 1)),
                Operator::Colon,
                Arc::new(Literal::new(ScalarValue::Utf8(Some("rank".to_owned())))),
            )),
            SortOptions::default(),
        )];

        assert!(matches!(
            scan.try_pushdown_sort(&order).expect("sort pushdown"),
            SortOrderPushdownResult::Unsupported
        ));
    }

    #[test]
    fn physical_plan_drops_sort_exec_for_order_by_id() {
        let provider =
            test_provider(Schema::new(vec![Field::new(ID_FIELD_NAME, DataType::Utf8, false)]));
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(provider)).expect("register table"));
        let dataframe = ctx
            .sql("SELECT id FROM vectors ORDER BY id")
            .now_or_never()
            .expect("sql future is ready")
            .expect("dataframe");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();

        assert!(display.contains("QdrantScanExec"), "{display}");
        assert!(!display.contains("SortExec"), "{display}");
    }

    #[test]
    fn physical_plan_drops_sort_exec_for_order_by_payload_path() {
        let provider = QdrantTableProvider {
            payload_schema: payload_schema([
                (
                    "rank",
                    PayloadSchemaInfo {
                        data_type: qdrant_client::qdrant::PayloadSchemaType::Integer as i32,
                        params: Some(qdrant_client::qdrant::PayloadIndexParams {
                            index_params: Some(
                                qdrant_client::qdrant::payload_index_params::IndexParams::IntegerIndexParams(
                                    IntegerIndexParams {
                                        range: Some(true),
                                        ..Default::default()
                                    },
                                ),
                            ),
                        }),
                        points: None,
                    },
                ),
            ]),
            ..test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            ]))
        };
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(provider.clone())).expect("register table"));
        let dataframe = ctx
            .sql("SELECT id FROM vectors ORDER BY payload:rank")
            .now_or_never()
            .expect("sql future is ready")
            .expect("dataframe");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let projection = plan.as_any().downcast_ref::<ProjectionExec>().expect("projection exec");
        let cooperative = projection
            .input()
            .as_any()
            .downcast_ref::<CooperativeExec>()
            .expect("cooperative exec");
        let scan = cooperative
            .input()
            .as_any()
            .downcast_ref::<QdrantScanExec>()
            .expect("qdrant scan exec");

        assert!(!display.contains("SortExec"), "{display}");
        assert_eq!(
            scan.pushdown.ordering,
            QdrantOrdering::ByPayload(crate::pushdown::QdrantPayloadOrdering {
                field:      "rank".to_owned(),
                descending: false,
            }),
        );
    }

    #[test]
    fn physical_plan_drops_filter_exec_for_id_in() {
        let provider =
            test_provider(Schema::new(vec![Field::new(ID_FIELD_NAME, DataType::Utf8, false)]));
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(provider)).expect("register table"));
        let dataframe = ctx
            .sql("SELECT id FROM vectors WHERE id IN ('1', '2')")
            .now_or_never()
            .expect("sql future is ready")
            .expect("dataframe");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let scan = qdrant_scan(&plan);

        assert_eq!(scan.pushdown.filters.len(), 1);
        assert!(!display.contains("FilterExec"), "{display}");
    }

    #[test]
    fn physical_plan_drops_filter_exec_for_payload_path() {
        let provider = QdrantTableProvider {
            payload_schema: payload_schema([
                (
                    "rank",
                    PayloadSchemaInfo {
                        data_type: qdrant_client::qdrant::PayloadSchemaType::Integer as i32,
                        params: Some(qdrant_client::qdrant::PayloadIndexParams {
                            index_params: Some(
                                qdrant_client::qdrant::payload_index_params::IndexParams::IntegerIndexParams(
                                    IntegerIndexParams {
                                        range: Some(true),
                                        ..Default::default()
                                    },
                                ),
                            ),
                        }),
                        points: None,
                    },
                ),
            ]),
            ..test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            ]))
        };
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(provider.clone())).expect("register table"));
        let dataframe = ctx
            .sql("SELECT id FROM vectors WHERE payload:rank >= 10")
            .now_or_never()
            .expect("sql future is ready")
            .expect("dataframe");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let scan = qdrant_scan(&plan);

        assert_eq!(scan.pushdown.filters.len(), 1);
        assert!(!display.contains("FilterExec"), "{display}");
    }

    #[test]
    fn physical_plan_drops_filter_exec_for_payload_or_chain() {
        let provider = QdrantTableProvider {
            payload_schema: payload_schema([
                (
                    "tag",
                    PayloadSchemaInfo {
                        data_type: qdrant_client::qdrant::PayloadSchemaType::Keyword as i32,
                        params: Some(qdrant_client::qdrant::PayloadIndexParams {
                            index_params: Some(
                                qdrant_client::qdrant::payload_index_params::IndexParams::KeywordIndexParams(
                                    qdrant_client::qdrant::KeywordIndexParams::default(),
                                ),
                            ),
                        }),
                        points: None,
                    },
                ),
            ]),
            ..test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            ]))
        };
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(provider.clone())).expect("register table"));
        let dataframe = ctx
            .sql("SELECT id FROM vectors WHERE payload:tag = 'red' OR payload:tag = 'blue'")
            .now_or_never()
            .expect("sql future is ready")
            .expect("dataframe");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let scan = qdrant_scan(&plan);

        assert_eq!(scan.pushdown.filters.len(), 1);
        assert!(!display.contains("FilterExec"), "{display}");
    }

    #[test]
    fn physical_plan_drops_filter_exec_for_general_boolean_filter() {
        let provider = QdrantTableProvider {
            payload_schema: payload_schema([
                (
                    "rank",
                    PayloadSchemaInfo {
                        data_type: qdrant_client::qdrant::PayloadSchemaType::Integer as i32,
                        params: Some(qdrant_client::qdrant::PayloadIndexParams {
                            index_params: Some(
                                qdrant_client::qdrant::payload_index_params::IndexParams::IntegerIndexParams(
                                    IntegerIndexParams {
                                        range: Some(true),
                                        ..Default::default()
                                    },
                                ),
                            ),
                        }),
                        points: None,
                    },
                ),
                (
                    "tag",
                    PayloadSchemaInfo {
                        data_type: qdrant_client::qdrant::PayloadSchemaType::Keyword as i32,
                        params: Some(qdrant_client::qdrant::PayloadIndexParams {
                            index_params: Some(
                                qdrant_client::qdrant::payload_index_params::IndexParams::KeywordIndexParams(
                                    qdrant_client::qdrant::KeywordIndexParams::default(),
                                ),
                            ),
                        }),
                        points: None,
                    },
                ),
            ]),
            ..test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            ]))
        };
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(provider)).expect("register table"));
        let dataframe = ctx
            .sql(
                "SELECT id FROM vectors WHERE (payload:tag = 'red' OR id = '2') AND NOT \
                 payload:rank > 20",
            )
            .now_or_never()
            .expect("sql future is ready")
            .expect("dataframe");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let scan = qdrant_scan(&plan);

        assert_eq!(scan.pushdown.filters.len(), 3);
        assert!(!display.contains("FilterExec"), "{display}");
    }

    #[test]
    fn physical_plan_drops_filter_exec_for_vector_presence() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new("image", DataType::new_fixed_size_list(DataType::Float32, 3, false), true),
        ]));
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(provider)).expect("register table"));
        let dataframe = ctx
            .sql("SELECT id FROM vectors WHERE image IS NULL")
            .now_or_never()
            .expect("sql future is ready")
            .expect("dataframe");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let scan = qdrant_scan(&plan);

        assert_eq!(scan.pushdown.filters.len(), 1);
        assert!(!display.contains("FilterExec"), "{display}");
    }

    #[test]
    fn logical_sort_expr_uses_payload_json_access_for_direct_path() {
        let plan = logical_plan(
            test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            ])),
            "SELECT id FROM vectors ORDER BY payload:rank",
        );

        assert_payload_string_access(sort_expr(&plan), Operator::Colon, "rank");
    }

    #[test]
    fn logical_sort_expr_preserves_nested_payload_json_path() {
        let plan = logical_plan(
            test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            ])),
            "SELECT id FROM vectors ORDER BY payload:metadata.rank",
        );

        assert_payload_string_access(sort_expr(&plan), Operator::Colon, "metadata.rank");
    }

    fn ordered_point(id: u64, value: QdrantOrderValue) -> RetrievedPoint {
        RetrievedPoint {
            id: Some(PointId { point_id_options: Some(point_id::PointIdOptions::Num(id)) }),
            order_value: Some(OrderValue {
                variant: Some(match value {
                    QdrantOrderValue::Integer(value) => order_value::Variant::Int(value),
                    QdrantOrderValue::Float(value) => order_value::Variant::Float(value),
                }),
            }),
            ..Default::default()
        }
    }

    fn numeric_id(point: &PointId) -> u64 {
        match point.point_id_options.as_ref() {
            Some(point_id::PointIdOptions::Num(id)) => *id,
            _ => panic!("expected numeric point id"),
        }
    }

    #[test]
    fn ordered_continuation_accumulates_duplicate_boundary_ids() {
        let ordered = QdrantOrderedContinuation {
            ordering:     crate::pushdown::QdrantPayloadOrdering {
                field:      "rank".to_owned(),
                descending: false,
            },
            start_from:   Some(QdrantOrderValue::Integer(10)),
            boundary_ids: vec![PointId {
                point_id_options: Some(point_id::PointIdOptions::Num(1)),
            }],
        };
        let next = ordered
            .next(&[
                ordered_point(2, QdrantOrderValue::Integer(10)),
                ordered_point(3, QdrantOrderValue::Integer(10)),
            ])
            .expect("ordered continuation");

        assert_eq!(next.start_from, Some(QdrantOrderValue::Integer(10)));
        assert_eq!(next.boundary_ids.iter().map(numeric_id).collect::<Vec<_>>(), vec![1, 2, 3],);
    }

    #[test]
    fn ordered_continuation_resets_boundary_ids_for_new_boundary() {
        let ordered = QdrantOrderedContinuation {
            ordering:     crate::pushdown::QdrantPayloadOrdering {
                field:      "rank".to_owned(),
                descending: false,
            },
            start_from:   Some(QdrantOrderValue::Integer(10)),
            boundary_ids: vec![PointId {
                point_id_options: Some(point_id::PointIdOptions::Num(1)),
            }],
        };
        let next = ordered
            .next(&[
                ordered_point(2, QdrantOrderValue::Integer(10)),
                ordered_point(4, QdrantOrderValue::Integer(20)),
                ordered_point(5, QdrantOrderValue::Integer(20)),
            ])
            .expect("ordered continuation");

        assert_eq!(next.start_from, Some(QdrantOrderValue::Integer(20)));
        assert_eq!(next.boundary_ids.iter().map(numeric_id).collect::<Vec<_>>(), vec![4, 5],);
    }
}
