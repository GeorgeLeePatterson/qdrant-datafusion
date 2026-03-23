pub mod plan_node;
mod planner;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::{DataFrame, SQLOptions, SessionContext};

use crate::analyzer::{QdrantCountPushdown, QdrantFacetPushdown};
use crate::context::planner::QdrantExtensionPlanner;

pub fn prepare_session_context(ctx: SessionContext) -> SessionContext {
    let state = ctx.state();
    let mut analyzer_rules = state.analyzer().rules.clone();
    let type_coercion = TypeCoercion::default();
    let mut pos =
        analyzer_rules.iter().position(|rule| rule.name() == type_coercion.name()).unwrap_or(0);
    for rule in [
        Arc::new(QdrantFacetPushdown) as Arc<dyn AnalyzerRule + Send + Sync>,
        Arc::new(QdrantCountPushdown) as Arc<dyn AnalyzerRule + Send + Sync>,
    ] {
        if analyzer_rules.iter().any(|existing| existing.name() == rule.name()) {
            continue;
        }
        analyzer_rules.insert(pos, rule);
        pos += 1;
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
}

impl From<SessionContext> for QdrantSessionContext {
    fn from(ctx: SessionContext) -> Self { Self::new(ctx) }
}
