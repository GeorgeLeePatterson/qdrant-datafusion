pub mod plan_node;
mod planner;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::TableProvider;
use datafusion::common::{DFSchema, plan_err};
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::{DataFrame, Expr, SQLOptions, SessionContext};
use datafusion::sql::TableReference;

use crate::analyzer::QdrantRelationPushdown;
use crate::context::plan_node::{
    QdrantKernelNode, QdrantKernelSpec, QdrantQueryKernel, QdrantQueryVariant,
};
use crate::context::planner::QdrantExtensionPlanner;
use crate::pushdown::filter::QdrantFilters;
use crate::table::QdrantTableProvider;

pub const QDRANT_SCORE_FIELD_NAME: &str = "__qdrant_score";

pub fn prepare_session_context(ctx: SessionContext) -> SessionContext {
    let state = ctx.state();
    let mut analyzer_rules = state.analyzer().rules.clone();
    let type_coercion = TypeCoercion::default();
    let pos =
        analyzer_rules.iter().position(|rule| rule.name() == type_coercion.name()).unwrap_or(0);
    let rule: Arc<dyn AnalyzerRule + Send + Sync> = Arc::new(QdrantRelationPushdown);
    if !analyzer_rules.iter().any(|existing| existing.name() == rule.name()) {
        analyzer_rules.insert(pos, rule);
    }
    SessionContext::new_with_state(
        ctx.into_state_builder()
            .with_analyzer_rules(analyzer_rules)
            .with_query_planner(Arc::new(QdrantQueryPlanner::default()))
            .build(),
    )
}

#[derive(Clone)]
pub struct QdrantQueryPlanner {
    planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl std::fmt::Debug for QdrantQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantQueryPlanner").finish()
    }
}

impl Default for QdrantQueryPlanner {
    fn default() -> Self { Self { planners: vec![Arc::new(QdrantExtensionPlanner)] } }
}

#[async_trait]
impl QueryPlanner for QdrantQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(self.planners.clone())
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

#[derive(Clone)]
pub struct QdrantSessionContext {
    inner: SessionContext,
}

impl QdrantSessionContext {
    pub fn new(ctx: SessionContext) -> Self { Self { inner: prepare_session_context(ctx) } }

    pub fn session_context(&self) -> &SessionContext { &self.inner }

    pub fn into_session_context(self) -> SessionContext { self.inner }

    /// Returns a SQL dataframe using the prepared `Qdrant` analyzer and planner hooks.
    ///
    /// # Errors
    ///
    /// Returns any `DataFusion` planning error produced while parsing or building the query.
    pub async fn sql(&self, sql: &str) -> datafusion::error::Result<DataFrame> {
        self.inner.sql_with_options(sql, SQLOptions::new()).await
    }

    /// Returns a nearest-neighbor retrieval relation over a registered `Qdrant` table.
    ///
    /// The current admitted surface is intentionally narrow: dense nearest-neighbor query,
    /// optional exact filters from the current predicate subset, optional named-vector selection,
    /// `LIMIT`, and optional score threshold. The result relation always returns the full base row
    /// plus [`QDRANT_SCORE_FIELD_NAME`].
    ///
    /// # Errors
    ///
    /// Returns an error if the table is not a registered `Qdrant` table, the named vector
    /// selection is invalid for the current schema, the nearest filters are outside the admitted
    /// exact subset, or the retrieval relation cannot be built.
    pub async fn nearest(
        &self,
        table_ref: impl Into<TableReference>,
        query: QdrantNearestQuery,
    ) -> datafusion::error::Result<DataFrame> {
        let table_ref = table_ref.into();
        let provider = self.inner.table_provider(table_ref.clone()).await?;
        let Some(provider) = provider.as_any().downcast_ref::<QdrantTableProvider>() else {
            return plan_err!("nearest requires a qdrant table");
        };
        let schema = provider.schema();

        let using = query.using.clone();
        if let Some(name) = using.as_deref() {
            let field = schema.field_with_name(name).map_err(|_| {
                datafusion::common::DataFusionError::Plan(format!(
                    "nearest vector '{name}' not found"
                ))
            })?;
            if !matches!(field.data_type(), DataType::FixedSizeList(_, _)) {
                return plan_err!("nearest requires a dense vector field");
            }
            if let DataType::FixedSizeList(_, len) = field.data_type()
                && usize::try_from(*len).ok() != Some(query.vector.len())
            {
                return plan_err!("nearest query width does not match vector field");
            }
        } else {
            let dense_fields = schema
                .fields()
                .iter()
                .filter(|field| matches!(field.data_type(), DataType::FixedSizeList(_, _)))
                .collect::<Vec<_>>();
            if dense_fields
                .iter()
                .map(|field| field.name().as_str())
                .any(|name| name != crate::arrow::schema::UNNAMED_VECTOR_FIELD_NAME)
            {
                return plan_err!("nearest requires `using` for named vectors");
            }
            if let Some(field) = dense_fields
                .iter()
                .find(|field| field.name() == crate::arrow::schema::UNNAMED_VECTOR_FIELD_NAME)
                && let DataType::FixedSizeList(_, len) = field.data_type()
                && usize::try_from(*len).ok() != Some(query.vector.len())
            {
                return plan_err!("nearest query width does not match vector field");
            }
        }

        if let Some(filter) = query.filters.iter().find(|filter| {
            !QdrantFilters::supports_exact(&schema, provider.payload_schema(), filter)
        }) {
            return plan_err!("unsupported nearest filter: {filter}");
        }
        let filters = QdrantFilters::try_new(&schema, provider.payload_schema(), &query.filters)?;

        if schema.fields().iter().any(|field| field.name() == QDRANT_SCORE_FIELD_NAME) {
            return plan_err!("nearest score field conflicts with schema");
        }
        let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(QDRANT_SCORE_FIELD_NAME, DataType::Float32, false)));
        let output_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let schema = Arc::new(DFSchema::try_from(Arc::clone(&output_schema))?);
        let limit = query.limit;
        let score_threshold = query.score_threshold;
        let vector = query.vector;

        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(QdrantKernelNode::with_spec(
                schema,
                Arc::clone(provider.client()),
                QdrantKernelSpec::Query(QdrantQueryKernel {
                    collection: provider.collection().to_owned(),
                    filters,
                    query: QdrantQueryVariant::Nearest { vector },
                    using,
                    limit,
                    score_threshold,
                }),
            )),
        });
        Ok(DataFrame::new(self.inner.state(), plan))
    }
}

impl From<SessionContext> for QdrantSessionContext {
    fn from(ctx: SessionContext) -> Self { Self::new(ctx) }
}

#[derive(Debug, Clone)]
pub struct QdrantNearestQuery {
    vector:          Vec<f32>,
    using:           Option<String>,
    filters:         Vec<Expr>,
    limit:           u64,
    score_threshold: Option<f32>,
}

impl QdrantNearestQuery {
    #[must_use]
    pub fn new(vector: Vec<f32>) -> Self {
        Self { vector, using: None, filters: vec![], limit: 10, score_threshold: None }
    }

    #[must_use]
    pub fn using(mut self, name: impl Into<String>) -> Self {
        self.using = Some(name.into());
        self
    }

    #[must_use]
    pub fn filter(mut self, filter: Expr) -> Self {
        self.filters.push(filter);
        self
    }

    #[must_use]
    pub fn filters(mut self, filters: impl IntoIterator<Item = Expr>) -> Self {
        self.filters.extend(filters);
        self
    }

    #[must_use]
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = limit;
        self
    }

    #[must_use]
    pub fn score_threshold(mut self, threshold: f32) -> Self {
        self.score_threshold = Some(threshold);
        self
    }
}
