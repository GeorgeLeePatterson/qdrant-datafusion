//! `DataFusion` `TableProvider` implementation for `Qdrant` vector database collections.
mod exec;
mod provider;
pub(crate) mod pushdown;
mod scroll;

use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::sql::TableReference;
use qdrant_client::Qdrant;

pub(crate) use self::pushdown::{
    QdrantContinuation, QdrantOrderValue, QdrantOrderedContinuation, QdrantOrdering,
    QdrantPayloadSelector, QdrantScanSpec, QdrantVectorSelector,
};
use crate::arrow::schema::{ID_FIELD_NAME, collection_to_arrow_schema};
use crate::error::{Error, Result};
use crate::pushdown::QdrantPayloadSchema;

const SCAN_PAGE_SIZE: usize = 1024;

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

impl QdrantTableProvider {
    /// Creates a table provider by introspecting a Qdrant collection.
    ///
    /// # Errors
    /// Returns an error if the collection metadata cannot be fetched or cannot
    /// be translated into the admitted Arrow schema contract.
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

    pub(crate) fn client(&self) -> &Arc<Qdrant> { &self.client }

    pub(crate) fn collection(&self) -> &str { self.table.table() }

    pub(crate) fn payload_schema(&self) -> &Arc<QdrantPayloadSchema> { &self.payload_schema }

    pub(crate) fn new_for_planner(
        collection: String,
        client: Arc<Qdrant>,
        schema: SchemaRef,
        payload_schema: Arc<QdrantPayloadSchema>,
    ) -> Self {
        Self { table: TableReference::bare(collection), client, schema, payload_schema }
    }

    #[cfg(test)]
    #[expect(dead_code, reason = "test-only constructor kept for future analyzer/planner tests")]
    pub(crate) fn new_test(
        table: &str,
        schema: Schema,
        payload_schema: QdrantPayloadSchema,
    ) -> Self {
        Self::new_for_planner(
            table.to_owned(),
            Arc::new(Qdrant::from_url("http://localhost:6334").build().expect("client")),
            Arc::new(schema),
            Arc::new(payload_schema),
        )
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

impl QdrantScanExec {
    pub(super) fn new(
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Column as ExprColumn, ScalarValue};
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};
    use datafusion::physical_expr::expressions::{
        BinaryExpr as PhysicalBinaryExpr, Column, Literal,
    };
    use datafusion::physical_plan::coop::CooperativeExec;
    use datafusion::physical_plan::expressions::PhysicalSortExpr;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, SortOrderPushdownResult, displayable};
    use datafusion::prelude::{SessionContext, col};
    use futures_util::FutureExt;
    use qdrant_client::Qdrant;
    use qdrant_client::qdrant::{
        IntegerIndexParams, OrderValue, PayloadSchemaInfo, PointId, RetrievedPoint, order_value,
        point_id,
    };

    use super::*;
    use crate::arrow::schema::{ID_FIELD_NAME, PAYLOAD_FIELD_NAME};
    use crate::context::QdrantSessionContext;
    use crate::context::exec::{
        QdrantCountExec, QdrantFacetExec, QdrantQueryBatchExec, QdrantQueryExec,
        QdrantQueryGroupsExec,
    };
    use crate::expr_fn::{
        qdrant_context_score, qdrant_discover_score, qdrant_recommend_score,
        qdrant_relevance_feedback_score,
    };
    use crate::pushdown::QdrantPayloadSchema;
    use crate::table::pushdown::QdrantPayloadOrdering;

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

    fn scalar_expr(value: ScalarValue) -> Expr { Expr::Literal(value, None) }

    fn list_data_type(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new_list_field(item, true)))
    }

    fn float_vector_scalar(values: &[f32]) -> ScalarValue {
        let scalars = values
            .iter()
            .copied()
            .map(|value| ScalarValue::Float32(Some(value)))
            .collect::<Vec<_>>();
        ScalarValue::List(ScalarValue::new_list_nullable(&scalars, &DataType::Float32))
    }

    fn float_vector_expr(values: &[f32]) -> Expr { scalar_expr(float_vector_scalar(values)) }

    fn float_vector_list_expr(vectors: &[&[f32]]) -> Expr {
        let vector_type = list_data_type(DataType::Float32);
        let scalars = vectors.iter().map(|values| float_vector_scalar(values)).collect::<Vec<_>>();
        scalar_expr(ScalarValue::List(ScalarValue::new_list_nullable(&scalars, &vector_type)))
    }

    fn float_vector_pair_list_expr(pairs: &[(&[f32], &[f32])]) -> Expr {
        let vector_type = list_data_type(DataType::Float32);
        let pair_type = list_data_type(vector_type.clone());
        let scalars = pairs
            .iter()
            .map(|(positive, negative)| {
                ScalarValue::List(ScalarValue::new_list_nullable(
                    &[float_vector_scalar(positive), float_vector_scalar(negative)],
                    &vector_type,
                ))
            })
            .collect::<Vec<_>>();
        scalar_expr(ScalarValue::List(ScalarValue::new_list_nullable(&scalars, &pair_type)))
    }

    fn empty_list_expr(item: &DataType) -> Expr {
        scalar_expr(ScalarValue::List(ScalarValue::new_list_nullable(&[], item)))
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

    fn qdrant_count(plan: &Arc<dyn ExecutionPlan>) -> &QdrantCountExec {
        if let Some(count) = plan.as_any().downcast_ref::<QdrantCountExec>() {
            return count;
        }
        if let Some(cooperative) = plan.as_any().downcast_ref::<CooperativeExec>() {
            return qdrant_count(cooperative.input());
        }
        if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            return qdrant_count(projection.input());
        }
        panic!("expected qdrant count exec in plan:\n{}", displayable(plan.as_ref()).indent(true));
    }

    fn qdrant_facet(plan: &Arc<dyn ExecutionPlan>) -> &QdrantFacetExec {
        if let Some(facet) = plan.as_any().downcast_ref::<QdrantFacetExec>() {
            return facet;
        }
        if let Some(cooperative) = plan.as_any().downcast_ref::<CooperativeExec>() {
            return qdrant_facet(cooperative.input());
        }
        if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            return qdrant_facet(projection.input());
        }
        panic!("expected qdrant facet exec in plan:\n{}", displayable(plan.as_ref()).indent(true));
    }

    fn qdrant_query(plan: &Arc<dyn ExecutionPlan>) -> &QdrantQueryExec {
        if let Some(query) = plan.as_any().downcast_ref::<QdrantQueryExec>() {
            return query;
        }
        if let Some(cooperative) = plan.as_any().downcast_ref::<CooperativeExec>() {
            return qdrant_query(cooperative.input());
        }
        panic!("expected qdrant query exec in plan:\n{}", displayable(plan.as_ref()).indent(true));
    }

    fn qdrant_query_batch(plan: &Arc<dyn ExecutionPlan>) -> &QdrantQueryBatchExec {
        if let Some(query) = plan.as_any().downcast_ref::<QdrantQueryBatchExec>() {
            return query;
        }
        if let Some(cooperative) = plan.as_any().downcast_ref::<CooperativeExec>() {
            return qdrant_query_batch(cooperative.input());
        }
        panic!(
            "expected qdrant query batch exec in plan:
{}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    fn qdrant_query_groups(plan: &Arc<dyn ExecutionPlan>) -> &QdrantQueryGroupsExec {
        if let Some(query) = plan.as_any().downcast_ref::<QdrantQueryGroupsExec>() {
            return query;
        }
        if let Some(cooperative) = plan.as_any().downcast_ref::<CooperativeExec>() {
            return qdrant_query_groups(cooperative.input());
        }
        panic!(
            "expected qdrant query groups exec in plan:
{}",
            displayable(plan.as_ref()).indent(true)
        );
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
    fn scan_tracks_projection_payload_limit_and_continuation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 3, false),
                true,
            ),
        ]));

        let projection = vec![1, 2];
        let spec = QdrantScanSpec::try_new(
            &schema,
            &QdrantPayloadSchema::default(),
            Some(&projection),
            &[],
            Some(7),
        )
        .expect("scan spec");

        assert_eq!(spec.projection, Some(projection));
        assert_eq!(spec.payload, QdrantPayloadSelector::Full);
        assert_eq!(spec.limit, Some(7));
        assert_eq!(spec.filters.len(), 0);
        assert_eq!(spec.initial_continuation(), QdrantContinuation::Offset(None));
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
            QdrantOrdering::ByPayload(QdrantPayloadOrdering {
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
            QdrantOrdering::ByPayload(QdrantPayloadOrdering {
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
    fn physical_plan_uses_qdrant_count_exec_for_count_star() {
        let provider = QdrantTableProvider {
            payload_schema: payload_schema([(
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
            ..test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            ]))
        };
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql("SELECT COUNT(*) FROM vectors WHERE payload:rank >= 10")
            .now_or_never()
            .expect("sql future is ready")
            .expect("dataframe");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let _count = qdrant_count(&plan);

        assert!(display.contains("QdrantCountExec"), "{display}");
        assert!(!display.contains("AggregateExec"), "{display}");
        assert!(!display.contains("FilterExec"), "{display}");
    }

    #[test]
    fn physical_plan_keeps_aggregate_exec_for_count_column() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql("SELECT COUNT(payload) FROM vectors")
            .now_or_never()
            .expect("sql future is ready")
            .expect("dataframe");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();

        assert!(display.contains("AggregateExec"), "{display}");
        assert!(!display.contains("QdrantCountExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_facet_exec_for_top_keyword_facets() {
        let provider = QdrantTableProvider {
            payload_schema: payload_schema([(
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
            )]),
            ..test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            ]))
        };
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                "SELECT payload:tag AS tag, COUNT(*) AS total FROM vectors GROUP BY payload:tag \
                 ORDER BY total DESC LIMIT 2",
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
        let _facet = qdrant_facet(&plan);

        assert!(display.contains("QdrantFacetExec"), "{display}");
        assert!(!display.contains("AggregateExec"), "{display}");
        assert!(!display.contains("SortExec"), "{display}");
        assert!(!display.contains("GlobalLimitExec"), "{display}");
        assert!(!display.contains("LocalLimitExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_facet_exec_for_top_integer_facets() {
        let provider = QdrantTableProvider {
            payload_schema: payload_schema([(
                "rank",
                PayloadSchemaInfo {
                    data_type: qdrant_client::qdrant::PayloadSchemaType::Integer as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(
                            qdrant_client::qdrant::payload_index_params::IndexParams::IntegerIndexParams(
                                IntegerIndexParams {
                                    lookup: Some(true),
                                    range: Some(false),
                                    ..Default::default()
                                },
                            ),
                        ),
                    }),
                    points: None,
                },
            )]),
            ..test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            ]))
        };
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                "SELECT payload:rank AS rank, COUNT(*) AS total FROM vectors GROUP BY \
                 payload:rank ORDER BY total DESC LIMIT 2",
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
        let _facet = qdrant_facet(&plan);

        assert!(display.contains("QdrantFacetExec"), "{display}");
        assert!(!display.contains("AggregateExec"), "{display}");
        assert!(!display.contains("SortExec"), "{display}");
        assert!(!display.contains("GlobalLimitExec"), "{display}");
        assert!(!display.contains("LocalLimitExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_nearest_score_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 2, false),
                true,
            ),
            Field::new("aux", DataType::new_fixed_size_list(DataType::Float32, 2, false), true),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                "SELECT id, payload, embedding, aux, qdrant_nearest_score(embedding, 1.0, 0.0) AS \
                 score FROM vectors WHERE id <> '3' ORDER BY score DESC LIMIT 2",
            )
            .now_or_never()
            .expect("sql future is ready")
            .expect("dataframe");
        assert_eq!(
            dataframe
                .logical_plan()
                .schema()
                .field_with_unqualified_name("score")
                .expect("score field")
                .data_type(),
            &DataType::Float32,
        );
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_preserves_qdrant_pushdown_inside_recursive_cte_terms() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 2, false),
                true,
            ),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe =
            ctx
                .sql(
                    "WITH RECURSIVE ranked AS ((SELECT id, payload, embedding,                  \
                     qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors ORDER BY \
                     score                  DESC LIMIT 1) UNION ALL SELECT id, payload, \
                     embedding, score FROM ranked WHERE                  false) SELECT * FROM \
                     ranked",
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

        assert!(display.contains("RecursiveQueryExec"), "{display}");
        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_groups_exec_for_distinct_on_nearest_queries() {
        let provider = QdrantTableProvider {
            payload_schema: payload_schema([(
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
            )]),
            ..test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
                Field::new(
                    "embedding",
                    DataType::new_fixed_size_list(DataType::Float32, 2, false),
                    true,
                ),
            ]))
        };
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                "SELECT DISTINCT ON (payload:tag) id, payload, embedding, \
                 qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors ORDER BY \
                 payload:tag, qdrant_nearest_score(embedding, 1.0, 0.0) DESC",
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
        let _query = qdrant_query_groups(&plan);

        assert!(display.contains("QdrantQueryGroupsExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_batch_exec_for_union_all_of_nearest_queries() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 2, false),
                true,
            ),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                "SELECT * FROM (SELECT id, payload, embedding, qdrant_nearest_score(embedding, \
                 1.0, 0.0) AS score FROM vectors ORDER BY score DESC LIMIT 2) a UNION ALL SELECT \
                 * FROM (SELECT id, payload, embedding, qdrant_nearest_score(embedding, 0.0, 1.0) \
                 AS score FROM vectors ORDER BY score DESC LIMIT 2) b",
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
        let _query = qdrant_query_batch(&plan);

        assert!(display.contains("QdrantQueryBatchExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_sample_score_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                "SELECT id, payload, qdrant_sample_score('random') AS score FROM vectors ORDER BY \
                 score DESC LIMIT 2",
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
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_order_by_score_sql() {
        let provider = QdrantTableProvider {
            payload_schema: payload_schema([(
                "rank",
                PayloadSchemaInfo {
                    data_type: qdrant_client::qdrant::PayloadSchemaType::Integer as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(
                            qdrant_client::qdrant::payload_index_params::IndexParams::IntegerIndexParams(
                                IntegerIndexParams::default(),
                            ),
                        ),
                    }),
                    points: None,
                },
            )]),
            ..test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            ]))
        };
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                "SELECT id, payload, qdrant_order_by_score(payload:rank, true) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2",
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
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_nearest_with_mmr_score_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 2, false),
                true,
            ),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                "SELECT id, payload, embedding, qdrant_nearest_with_mmr_score(embedding, 0.5, 8, \
                 1.0, 0.0) AS score FROM vectors ORDER BY score DESC LIMIT 2",
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
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_recommend_score_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 2, false),
                true,
            ),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .session_context()
            .table("vectors")
            .now_or_never()
            .expect("table future is ready")
            .expect("table")
            .select(vec![
                col("id"),
                col("payload"),
                qdrant_recommend_score(
                    col("embedding"),
                    float_vector_list_expr(&[&[1.0, 0.0]]),
                    float_vector_list_expr(&[&[0.0, 1.0]]),
                )
                .alias("score"),
            ])
            .expect("select")
            .sort(vec![col("score").sort(false, false)])
            .expect("sort")
            .limit(0, Some(2))
            .expect("limit");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_discover_score_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 2, false),
                true,
            ),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .session_context()
            .table("vectors")
            .now_or_never()
            .expect("table future is ready")
            .expect("table")
            .select(vec![
                col("id"),
                col("payload"),
                qdrant_discover_score(
                    col("embedding"),
                    float_vector_expr(&[1.0, 0.0]),
                    float_vector_pair_list_expr(&[(&[1.0, 0.0], &[0.0, 1.0])]),
                )
                .alias("score"),
            ])
            .expect("select")
            .sort(vec![col("score").sort(false, false)])
            .expect("sort")
            .limit(0, Some(2))
            .expect("limit");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_context_score_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 2, false),
                true,
            ),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .session_context()
            .table("vectors")
            .now_or_never()
            .expect("table future is ready")
            .expect("table")
            .select(vec![
                col("id"),
                col("payload"),
                qdrant_context_score(
                    col("embedding"),
                    float_vector_pair_list_expr(&[(&[1.0, 0.0], &[0.0, 1.0])]),
                )
                .alias("score"),
            ])
            .expect("select")
            .sort(vec![col("score").sort(false, false)])
            .expect("sort")
            .limit(0, Some(2))
            .expect("limit");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_formula_score_sql() {
        let provider = QdrantTableProvider {
            table: TableReference::bare("vectors"),
            client: Arc::new(Qdrant::from_url("http://localhost:6334").build().expect("client")),
            schema: Arc::new(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
                Field::new(
                    "embedding",
                    DataType::new_fixed_size_list(DataType::Float32, 2, false),
                    true,
                ),
            ])),
            payload_schema: payload_schema([(
                "rank",
                PayloadSchemaInfo {
                    data_type: qdrant_client::qdrant::PayloadSchemaType::Integer as i32,
                    params: Some(qdrant_client::qdrant::PayloadIndexParams {
                        index_params: Some(qdrant_client::qdrant::payload_index_params::IndexParams::IntegerIndexParams(
                            IntegerIndexParams { ..Default::default() },
                        )),
                    }),
                    points: None,
                },
            )]),
        };
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe =
            ctx
                .sql(
                    "SELECT id, payload, formula_score(ranked.base_score + payload_num('rank')) AS score FROM                      (SELECT id, payload, embedding, qdrant_nearest_score(embedding, 1.0, 0.0) AS                      base_score FROM vectors ORDER BY base_score DESC LIMIT 5) ranked ORDER BY                      score DESC LIMIT 2",
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
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_relevance_feedback_score_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 2, false),
                true,
            ),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .session_context()
            .table("vectors")
            .now_or_never()
            .expect("table future is ready")
            .expect("table")
            .select(vec![
                col("id"),
                col("payload"),
                qdrant_relevance_feedback_score(
                    col("embedding"),
                    float_vector_expr(&[1.0, 0.0]),
                    empty_list_expr(&list_data_type(DataType::Float32)),
                )
                .alias("score"),
            ])
            .expect("select")
            .sort(vec![col("score").sort(false, false)])
            .expect("sort")
            .limit(0, Some(2))
            .expect("limit");
        let plan = dataframe
            .create_physical_plan()
            .now_or_never()
            .expect("plan future is ready")
            .expect("physical plan");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_nearest_id_score_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 2, false),
                true,
            ),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe =
            ctx
                .sql(
                    "SELECT id, payload, qdrant_nearest_id_score(embedding, 42) AS score FROM \
                     vectors                  ORDER BY score DESC LIMIT 2",
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
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_nearest_document_score_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new("text_embedding", DataType::Utf8, true),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe =
            ctx
                .sql(
                    "SELECT id, payload, qdrant_nearest_document_score(text_embedding, 'hello \
                     world')                  AS score FROM vectors ORDER BY score DESC LIMIT 2",
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
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_nearest_image_score_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new("image_embedding", DataType::Binary, true),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                "SELECT id, payload, qdrant_nearest_image_score(image_embedding,                  'https://example.com/cat.png') AS score FROM vectors ORDER BY score DESC LIMIT                  2",
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
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_nearest_object_score_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "object_embedding",
                DataType::Struct(vec![Field::new("kind", DataType::Utf8, true)].into()),
                true,
            ),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                r#"SELECT id, payload, qdrant_nearest_object_score(object_embedding, '{"kind":"cat"}') AS score FROM vectors ORDER BY score DESC LIMIT 2"#,
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
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
    }

    #[test]
    fn physical_plan_composes_closed_qdrant_children_locally_after_join() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 2, false),
                true,
            ),
            Field::new("aux", DataType::new_fixed_size_list(DataType::Float32, 2, false), true),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe =
            ctx
                .sql(
                    "SELECT lhs.id, rhs.id FROM (SELECT id, qdrant_nearest_score(embedding, 1.0, \
                     0.0)                  AS score FROM vectors ORDER BY score DESC LIMIT 1) lhs \
                     JOIN (SELECT id,                  qdrant_nearest_score(aux, 0.0, 1.0) AS \
                     score FROM vectors ORDER BY score DESC                  LIMIT 1) rhs ON \
                     lhs.id = rhs.id",
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

        assert_eq!(display.matches("QdrantQueryExec").count(), 2, "{display}");
        assert!(display.contains("JoinExec") || display.contains("HashJoinExec"), "{display}");
    }

    #[test]
    fn physical_plan_uses_qdrant_query_exec_for_fusion_prefetch_sql() {
        let provider = test_provider(Schema::new(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 2, false),
                true,
            ),
        ]));
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                "SELECT id, payload, embedding, qdrant_fusion_score('RRF') AS score FROM ((SELECT \
                 id, payload, embedding, qdrant_nearest_score(embedding, 1.0, 0.0) AS \
                 source_score FROM vectors ORDER BY source_score DESC LIMIT 2) UNION ALL (SELECT \
                 id, payload, embedding, qdrant_nearest_score(embedding, 0.0, 1.0) AS \
                 source_score FROM vectors ORDER BY source_score DESC LIMIT 2)) prefetched ORDER \
                 BY score DESC LIMIT 3",
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
        let _query = qdrant_query(&plan);

        assert!(display.contains("QdrantQueryExec"), "{display}");
        assert!(display.contains("prefetch=2"), "{display}");
        assert!(!display.contains("QdrantQueryBatchExec"), "{display}");
    }

    #[test]
    fn physical_plan_pushes_limit_into_qdrant_query_groups_exec() {
        let provider = QdrantTableProvider {
            payload_schema: payload_schema([(
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
            )]),
            ..test_provider(Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
                Field::new(
                    "embedding",
                    DataType::new_fixed_size_list(DataType::Float32, 2, false),
                    true,
                ),
            ]))
        };
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(
            ctx.session_context()
                .register_table("vectors", Arc::new(provider))
                .expect("register table"),
        );
        let dataframe = ctx
            .sql(
                "SELECT * FROM (SELECT DISTINCT ON (payload:tag) id, payload, embedding, \
                 qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors ORDER BY \
                 payload:tag, qdrant_nearest_score(embedding, 1.0, 0.0) DESC) grouped LIMIT 3",
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
        let _query = qdrant_query_groups(&plan);

        assert!(display.contains("QdrantQueryGroupsExec"), "{display}");
        assert!(display.contains("limit=3"), "{display}");
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
            ordering:     QdrantPayloadOrdering {
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
            ordering:     QdrantPayloadOrdering {
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
