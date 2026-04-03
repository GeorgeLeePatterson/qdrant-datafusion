use std::any::Any;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SortOrderPushdownResult,
};

use super::scroll::QdrantScrollState;
use super::{QdrantOrdering, QdrantScanExec};
use crate::arrow::schema::ID_FIELD_NAME;
use crate::stream::QdrantQueryStream;

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
        let Some(path) = self.payload_schema.path_for_physical_ordering_expr(&sort.expr) else {
            return Ok(SortOrderPushdownResult::Unsupported);
        };
        let path = path.key().to_owned();
        let Some(ordering) = self.payload_schema.ordering_for(&path, sort.options.descending)
        else {
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
        let (filters, support) = self.pushdown.filters.clone().partition_physical(
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
