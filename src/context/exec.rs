use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::stream;
use qdrant_client::qdrant::{CountPointsBuilder, FacetCountsBuilder, QueryPointsBuilder, facet_value};

use crate::analyzer::{CountKernel, FacetKernel, QueryExecution, QueryKernel, STATE_NODE_NAME, State, StateNode};
use crate::arrow::deserialize::QdrantRecordBatchBuilder;

pub(crate) fn execution_plan_for_state_node(node: &StateNode) -> Result<Arc<dyn ExecutionPlan>> {
    match node.state() {
        State::Kernel(kernel) => execution_plan_for_kernel(kernel.spec(), Arc::clone(node.output_schema().inner())),
        State::Processing(_) => plan_err!("{STATE_NODE_NAME} reached physical planning with unfinished processing state"),
        State::Local(_) | State::Source(_) | State::Composite(_) | State::Fatal(_) => {
            plan_err!("{STATE_NODE_NAME} reached physical planning with non-executable state")
        }
    }
}

fn execution_plan_for_kernel(
    kernel: &crate::analyzer::KernelSpec,
    schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    match kernel {
        crate::analyzer::KernelSpec::Count(spec) => {
            Ok(Arc::new(QdrantCountExec::new(spec.clone(), schema)))
        }
        crate::analyzer::KernelSpec::Facet(spec) => {
            Ok(Arc::new(QdrantFacetExec::new(spec.clone(), schema)))
        }
        crate::analyzer::KernelSpec::Query(spec) => {
            Ok(Arc::new(QdrantQueryExec::new(spec.clone(), schema)))
        }
    }
}

#[derive(Clone)]
pub(crate) struct QdrantCountExec {
    spec: CountKernel,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

#[derive(Clone)]
pub(crate) struct QdrantFacetExec {
    spec: FacetKernel,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

#[derive(Clone)]
pub(crate) struct QdrantQueryExec {
    spec: QueryKernel,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl QdrantCountExec {
    fn new(spec: CountKernel, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { spec, schema, properties: Arc::new(properties) }
    }
}

impl QdrantFacetExec {
    fn new(spec: FacetKernel, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { spec, schema, properties: Arc::new(properties) }
    }
}

impl QdrantQueryExec {
    fn new(spec: QueryKernel, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { spec, schema, properties: Arc::new(properties) }
    }
}

impl std::fmt::Debug for QdrantCountExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantCountExec")
            .field("collection", &self.spec.collection())
            .field("filters", &self.spec.filters())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for QdrantFacetExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantFacetExec")
            .field("collection", &self.spec.collection())
            .field("field", &self.spec.op().field())
            .field("limit", &self.spec.limit())
            .field("filters", &self.spec.filters())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for QdrantQueryExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantQueryExec")
            .field("collection", &self.spec.collection())
            .field("limit", &self.spec.limit())
            .field("score_threshold", &self.spec.query().score_threshold())
            .field("score_output_names", &self.spec.query().score_output_names())
            .field("filters", &self.spec.filters())
            .finish_non_exhaustive()
    }
}

impl ExecutionPlan for QdrantCountExec {
    fn name(&self) -> &'static str { "QdrantCountExec" }

    fn as_any(&self) -> &dyn Any { self }

    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return exec_err!("QdrantCountExec expects no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("QdrantCountExec invalid partition {partition}");
        }

        let client = self.spec.client();
        let collection = self.spec.collection().to_owned();
        let filters = self.spec.filters().clone();
        let schema = Arc::clone(&self.schema);
        let fut = async move {
            let mut request = CountPointsBuilder::new(collection).exact(true);
            if let Some(filter) = filters.to_filter() {
                request = request.filter(filter);
            }
            let response = client
                .count(request)
                .await
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
            let count = response
                .result
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Qdrant count response missing result".to_owned(),
                    )
                })?
                .count;
            let count = i64::try_from(count).map_err(|_| {
                datafusion::error::DataFusionError::Execution("Qdrant count exceeds i64".to_owned())
            })?;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![count])) as ArrayRef],
            )?;
            Ok(batch)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), stream::once(fut))))
    }
}

impl ExecutionPlan for QdrantFacetExec {
    fn name(&self) -> &'static str { "QdrantFacetExec" }

    fn as_any(&self) -> &dyn Any { self }

    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return exec_err!("QdrantFacetExec expects no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("QdrantFacetExec invalid partition {partition}");
        }

        let client = self.spec.client();
        let collection = self.spec.collection().to_owned();
        let filters = self.spec.filters().clone();
        let field = self.spec.op().field().clone();
        let limit = self.spec.limit();
        let schema = Arc::clone(&self.schema);
        let op = self.spec.op().clone();
        let fut = async move {
            let mut request = FacetCountsBuilder::new(collection, field.key()).exact(true);
            if let Some(filter) = filters.to_filter() {
                request = request.filter(filter);
            }
            request = request.limit(limit);
            let response = client
                .facet(request)
                .await
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
            let mut keys = Vec::with_capacity(response.hits.len());
            let mut counts = Vec::with_capacity(response.hits.len());
            for hit in response.hits {
                let value = hit.value.and_then(|value| value.variant).ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Qdrant facet hit missing value".to_owned(),
                    )
                })?;
                let facet_value = match value {
                    facet_value::Variant::StringValue(value) => value,
                    facet_value::Variant::IntegerValue(value) => value.to_string(),
                    facet_value::Variant::BoolValue(value) => value.to_string(),
                };
                keys.push(facet_value);
                counts.push(i64::try_from(hit.count).map_err(|_| {
                    datafusion::error::DataFusionError::Execution(
                        "Qdrant facet count exceeds i64".to_owned(),
                    )
                })?);
            }
            let key_array = Arc::new(StringArray::from(keys)) as ArrayRef;
            let count_array = Arc::new(Int64Array::from(counts)) as ArrayRef;
            let columns = schema
                .fields()
                .iter()
                .map(|field| {
                    if op.is_key_output_name(field.name()) {
                        Ok(Arc::clone(&key_array))
                    } else if op.is_count_output_name(field.name()) {
                        Ok(Arc::clone(&count_array))
                    } else {
                        exec_err!("unsupported facet output field '{}' in kernel schema", field.name())
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), stream::once(fut))))
    }
}

impl ExecutionPlan for QdrantQueryExec {
    fn name(&self) -> &'static str { "QdrantQueryExec" }

    fn as_any(&self) -> &dyn Any { self }

    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return exec_err!("QdrantQueryExec expects no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("QdrantQueryExec invalid partition {partition}");
        }

        let client = self.spec.client();
        let collection = self.spec.collection().to_owned();
        let filters = self.spec.filters().clone();
        let execution = self.spec.query().execution();
        let score_threshold = self.spec.query().score_threshold();
        let score_output_names = self.spec.query().score_output_names();
        let limit = self.spec.limit();
        let schema = Arc::clone(&self.schema);
        let fut = async move {
            if limit == 0 {
                return Ok(RecordBatch::new_empty(schema));
            }
            let mut request = match execution {
                QueryExecution::NearestDense { using, vector } => {
                    let mut request = QueryPointsBuilder::new(collection).query(vector).limit(limit).with_payload(true).with_vectors(true);
                    if let Some(using) = using {
                        request = request.using(using);
                    }
                    request
                }
                QueryExecution::NearestSparse { .. } => {
                    return exec_err!("sparse nearest execution is not yet implemented");
                }
                QueryExecution::NearestMultiDense { .. } => {
                    return exec_err!("multidense nearest execution is not yet implemented");
                }
                QueryExecution::NearestById { .. } => {
                    return exec_err!("point-id nearest execution is not yet implemented");
                }
                QueryExecution::NearestDocument { .. } => {
                    return exec_err!("document nearest execution is not yet implemented");
                }
                QueryExecution::NearestImage { .. } => {
                    return exec_err!("image nearest execution is not yet implemented");
                }
                QueryExecution::NearestObject { .. } => {
                    return exec_err!("object nearest execution is not yet implemented");
                }
                QueryExecution::Recommend(_) => return exec_err!("recommend execution is not yet implemented"),
                QueryExecution::Discover(_) => return exec_err!("discover execution is not yet implemented"),
                QueryExecution::Context(_) => return exec_err!("context execution is not yet implemented"),
                QueryExecution::OrderBy(_) => return exec_err!("order-by execution is not yet implemented"),
                QueryExecution::Fusion(_) => return exec_err!("fusion execution is not yet implemented"),
                QueryExecution::Sample(_) => return exec_err!("sample execution is not yet implemented"),
                QueryExecution::Formula(_) => return exec_err!("formula execution is not yet implemented"),
                QueryExecution::NearestWithMmr(_) => return exec_err!("nearest-with-mmr execution is not yet implemented"),
                QueryExecution::RelevanceFeedback(_) => return exec_err!("relevance-feedback execution is not yet implemented"),
            };
            if let Some(filter) = filters.to_filter() {
                request = request.filter(filter);
            }
            if let Some(threshold) = score_threshold {
                request = request.score_threshold(threshold);
            }
            let response = client
                .query(request)
                .await
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
            let mut builder = QdrantRecordBatchBuilder::new(
                Arc::clone(&schema),
                response.result.len(),
                Some(&score_output_names),
            )?;
            for point in response.result {
                builder.append_point(point)?;
            }
            builder.finish()
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), stream::once(fut))))
    }
}

impl DisplayAs for QdrantCountExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "QdrantCountExec: collection={}", self.spec.collection())
            }
            DisplayFormatType::TreeRender => write!(f, "QdrantCountExec"),
        }
    }
}

impl DisplayAs for QdrantFacetExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "QdrantFacetExec: collection={}, field={}, limit={}",
                self.spec.collection(),
                self.spec.op().field().key(),
                self.spec.limit(),
            ),
            DisplayFormatType::TreeRender => write!(f, "QdrantFacetExec"),
        }
    }
}

impl DisplayAs for QdrantQueryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "QdrantQueryExec: collection={}, limit={}", self.spec.collection(), self.spec.limit())?;
                if let Some(threshold) = self.spec.query().score_threshold() {
                    write!(f, ", score_threshold={threshold}")?;
                }
                write!(f, ", score_outputs={:?}", self.spec.query().score_output_names())
            }
            DisplayFormatType::TreeRender => write!(f, "QdrantQueryExec"),
        }
    }
}
