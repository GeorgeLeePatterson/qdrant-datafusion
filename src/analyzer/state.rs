mod composite;
mod fatal;
mod kernel;
mod local;
mod processing;
mod source;

use datafusion::common::Result;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, LogicalPlan};

pub(crate) use self::composite::CompositeState;
use self::fatal::FatalState;
pub(crate) use self::kernel::KernelState;
use self::local::LocalState;
use self::processing::ProcessingState;
pub(crate) use self::source::SourceState;
use super::source::Source;
use crate::pushdown::filter::QdrantFilters;

#[derive(Debug, Clone)]
pub(crate) struct SemanticError {
    pub(crate) message: String,
}

impl SemanticError {
    fn new(message: impl Into<String>) -> Self { Self { message: message.into() } }
}

#[derive(Debug, Clone)]
pub(crate) enum State {
    Local(LocalState),
    Source(SourceState),
    Processing(ProcessingState),
    Composite(CompositeState),
    Kernel(KernelState),
    Fatal(FatalState),
}

impl State {
    pub(super) fn local() -> Self { Self::Local(LocalState) }

    pub(super) fn fatal(message: impl Into<String>) -> Self {
        Self::Fatal(FatalState { error: SemanticError::new(message) })
    }

    pub(super) fn projection(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::Analysis> {
        match self {
            Self::Local(state) => state.projection(plan, transformed),
            Self::Source(state) => state.projection(plan, transformed),
            Self::Processing(state) => state.projection(plan, transformed),
            Self::Composite(state) => state.projection(plan, transformed),
            Self::Kernel(state) => state.projection(plan, transformed),
            Self::Fatal(state) => state.projection(plan, transformed),
        }
    }

    pub(super) fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<super::Analysis> {
        match self {
            Self::Local(state) => state.filter(plan, transformed),
            Self::Source(state) => state.filter(plan, transformed),
            Self::Processing(state) => state.filter(plan, transformed),
            Self::Composite(state) => state.filter(plan, transformed),
            Self::Kernel(state) => state.filter(plan, transformed),
            Self::Fatal(state) => state.filter(plan, transformed),
        }
    }

    pub(super) fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<super::Analysis> {
        match self {
            Self::Local(state) => state.sort(plan, transformed),
            Self::Source(state) => state.sort(plan, transformed),
            Self::Processing(state) => state.sort(plan, transformed),
            Self::Composite(state) => state.sort(plan, transformed),
            Self::Kernel(state) => state.sort(plan, transformed),
            Self::Fatal(state) => state.sort(plan, transformed),
        }
    }

    pub(super) fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<super::Analysis> {
        match self {
            Self::Local(state) => state.limit(plan, transformed),
            Self::Source(state) => state.limit(plan, transformed),
            Self::Processing(state) => state.limit(plan, transformed),
            Self::Composite(state) => state.limit(plan, transformed),
            Self::Kernel(state) => state.limit(plan, transformed),
            Self::Fatal(state) => state.limit(plan, transformed),
        }
    }

    pub(super) fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<super::Analysis> {
        match self {
            Self::Local(state) => state.aggregate(plan, transformed),
            Self::Source(state) => state.aggregate(plan, transformed),
            Self::Processing(state) => state.aggregate(plan, transformed),
            Self::Composite(state) => state.aggregate(plan, transformed),
            Self::Kernel(state) => state.aggregate(plan, transformed),
            Self::Fatal(state) => state.aggregate(plan, transformed),
        }
    }

    pub(super) fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<super::Analysis> {
        match self {
            Self::Local(state) => state.unary(plan, transformed),
            Self::Source(state) => state.unary(plan, transformed),
            Self::Processing(state) => state.unary(plan, transformed),
            Self::Composite(state) => state.unary(plan, transformed),
            Self::Kernel(state) => state.unary(plan, transformed),
            Self::Fatal(state) => state.unary(plan, transformed),
        }
    }

    pub(super) fn requires_composite_coordination(&self) -> bool {
        matches!(self, Self::Processing(_) | Self::Composite(_))
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FiltersState {
    pub(super) exprs: Vec<Expr>,
}

impl FiltersState {
    pub(super) fn push(mut self, expr: Expr) -> Self {
        self.exprs.push(expr);
        self
    }

    pub(super) fn exact(&self, source: &Source) -> Result<QdrantFilters> {
        QdrantFilters::try_new(&source.schema, &source.payload_schema, &self.exprs)
    }

    pub(super) fn combined_expr(&self) -> Option<Expr> { conjunction(self.exprs.clone()) }
}
