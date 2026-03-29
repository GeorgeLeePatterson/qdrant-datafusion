use std::any::Any;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, exec_err};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::stream;
use qdrant_client::qdrant::{
    CountPointsBuilder, FacetCountsBuilder, GroupId, PointGroup, ScoredPoint, facet_value,
    group_id,
};

use crate::analyzer::{
    CountKernel, FacetKernel, QueryBatchKernel, QueryGroupsKernel, QueryKernel, QueryRequest,
};
use crate::arrow::deserialize::QdrantRecordBatchBuilder;

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

#[derive(Clone)]
pub(crate) struct QdrantQueryBatchExec {
    spec: QueryBatchKernel,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

#[derive(Clone)]
pub(crate) struct QdrantQueryGroupsExec {
    spec: QueryGroupsKernel,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

fn expect_no_children(
    name: &'static str,
    children: &[Arc<dyn ExecutionPlan>],
) -> Result<()> {
    if children.is_empty() {
        Ok(())
    } else {
        exec_err!("{name} expects no children")
    }
}

fn expect_partition_zero(name: &'static str, partition: usize) -> Result<()> {
    if partition == 0 {
        Ok(())
    } else {
        exec_err!("{name} invalid partition {partition}")
    }
}

macro_rules! impl_leaf_execution_plan {
    ($ty:ty, $name:literal) => {
        fn name(&self) -> &'static str {
            $name
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            expect_no_children($name, &children)?;
            Ok(self)
        }
    };
}

fn leaf_properties(schema: &SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(schema)),
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
        datafusion::physical_plan::execution_plan::EmissionType::Final,
        Boundedness::Bounded,
    ))
}

impl QdrantCountExec {
    pub(crate) fn new(spec: CountKernel, schema: SchemaRef) -> Self {
        Self { spec, schema: Arc::clone(&schema), properties: leaf_properties(&schema) }
    }
}

impl QdrantFacetExec {
    pub(crate) fn new(spec: FacetKernel, schema: SchemaRef) -> Self {
        Self { spec, schema: Arc::clone(&schema), properties: leaf_properties(&schema) }
    }
}

impl QdrantQueryExec {
    pub(crate) fn new(spec: QueryKernel, schema: SchemaRef) -> Self {
        Self { spec, schema: Arc::clone(&schema), properties: leaf_properties(&schema) }
    }
}

impl QdrantQueryBatchExec {
    pub(crate) fn new(spec: QueryBatchKernel, schema: SchemaRef) -> Self {
        Self { spec, schema: Arc::clone(&schema), properties: leaf_properties(&schema) }
    }
}

impl QdrantQueryGroupsExec {
    pub(crate) fn new(spec: QueryGroupsKernel, schema: SchemaRef) -> Self {
        Self { spec, schema: Arc::clone(&schema), properties: leaf_properties(&schema) }
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

impl std::fmt::Debug for QdrantQueryBatchExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantQueryBatchExec")
            .field("collection", &self.spec.collection())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for QdrantQueryGroupsExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantQueryGroupsExec")
            .field("collection", &self.spec.collection())
            .field("group_by", &self.spec.group_by())
            .field("group_size", &self.spec.group_size())
            .finish_non_exhaustive()
    }
}

impl ExecutionPlan for QdrantCountExec {
    impl_leaf_execution_plan!(QdrantCountExec, "QdrantCountExec");

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        expect_partition_zero("QdrantCountExec", partition)?;

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
    impl_leaf_execution_plan!(QdrantFacetExec, "QdrantFacetExec");

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        expect_partition_zero("QdrantFacetExec", partition)?;

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
                        exec_err!(
                            "unsupported facet output field '{}' in kernel schema",
                            field.name()
                        )
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), stream::once(fut))))
    }
}

fn append_scored_points_to_batch(
    schema: &SchemaRef,
    point_count: usize,
    score_output_names: &BTreeSet<String>,
    points: impl IntoIterator<Item = ScoredPoint>,
) -> Result<RecordBatch> {
    let mut builder =
        QdrantRecordBatchBuilder::new(Arc::clone(schema), point_count, Some(score_output_names))?;
    for point in points {
        builder.append_point(point)?;
    }
    builder.finish()
}

fn group_id_cmp(lhs: Option<&GroupId>, rhs: Option<&GroupId>) -> Ordering {
    match (
        lhs.and_then(|id| id.kind.as_ref()),
        rhs.and_then(|id| id.kind.as_ref()),
    ) {
        (Some(group_id::Kind::UnsignedValue(lhs)), Some(group_id::Kind::UnsignedValue(rhs))) => lhs.cmp(rhs),
        (Some(group_id::Kind::IntegerValue(lhs)), Some(group_id::Kind::IntegerValue(rhs))) => lhs.cmp(rhs),
        (Some(group_id::Kind::StringValue(lhs)), Some(group_id::Kind::StringValue(rhs))) => lhs.cmp(rhs),
        (Some(group_id::Kind::UnsignedValue(_)), Some(group_id::Kind::IntegerValue(_))) => Ordering::Less,
        (Some(group_id::Kind::UnsignedValue(_)), Some(group_id::Kind::StringValue(_))) => Ordering::Less,
        (Some(group_id::Kind::IntegerValue(_)), Some(group_id::Kind::UnsignedValue(_))) => Ordering::Greater,
        (Some(group_id::Kind::IntegerValue(_)), Some(group_id::Kind::StringValue(_))) => Ordering::Less,
        (Some(group_id::Kind::StringValue(_)), Some(group_id::Kind::UnsignedValue(_))) => Ordering::Greater,
        (Some(group_id::Kind::StringValue(_)), Some(group_id::Kind::IntegerValue(_))) => Ordering::Greater,
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
    }
}

fn sort_point_groups(groups: &mut [PointGroup], descending: bool) {
    groups.sort_by(|lhs, rhs| {
        let order = group_id_cmp(lhs.id.as_ref(), rhs.id.as_ref());
        if descending { order.reverse() } else { order }
    });
}

impl ExecutionPlan for QdrantQueryExec {
    impl_leaf_execution_plan!(QdrantQueryExec, "QdrantQueryExec");

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        expect_partition_zero("QdrantQueryExec", partition)?;

        let client = self.spec.client();
        let request_plan = self.spec.request_plan(&self.schema)?;
        let score_output_names = request_plan.score_output_names().clone();
        let limit = self.spec.limit();
        let schema = Arc::clone(&self.schema);
        let fut = async move {
            if limit == 0 {
                return Ok(RecordBatch::new_empty(schema));
            }
            let QueryRequest::Points(request) = request_plan.request() else {
                return exec_err!("qdrant query exec received a non-point request plan");
            };
            let response = client
                .query((*request).clone())
                .await
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
            append_scored_points_to_batch(&schema, response.result.len(), &score_output_names, response.result)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), stream::once(fut))))
    }
}

impl ExecutionPlan for QdrantQueryBatchExec {
    impl_leaf_execution_plan!(QdrantQueryBatchExec, "QdrantQueryBatchExec");

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        expect_partition_zero("QdrantQueryBatchExec", partition)?;

        let client = self.spec.client();
        let request_plan = self.spec.request_plan(&self.schema)?;
        let score_output_names = request_plan.score_output_names().clone();
        let schema = Arc::clone(&self.schema);
        let fut = async move {
            let QueryRequest::Batch(request) = request_plan.request() else {
                return exec_err!("qdrant query batch exec received a non-batch request plan");
            };
            let response = client
                .query_batch((*request).clone())
                .await
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
            let point_count = response.result.iter().map(|batch| batch.result.len()).sum();
            let points = response
                .result
                .into_iter()
                .flat_map(|batch| batch.result)
                .collect::<Vec<_>>();
            append_scored_points_to_batch(&schema, point_count, &score_output_names, points)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), stream::once(fut))))
    }
}

impl ExecutionPlan for QdrantQueryGroupsExec {
    impl_leaf_execution_plan!(QdrantQueryGroupsExec, "QdrantQueryGroupsExec");

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        expect_partition_zero("QdrantQueryGroupsExec", partition)?;

        let client = self.spec.client();
        let request_plan = self.spec.request_plan(&self.schema)?;
        let score_output_names = request_plan.score_output_names().clone();
        let group_descending = self.spec.group_descending();
        let schema = Arc::clone(&self.schema);
        let fut = async move {
            let QueryRequest::Groups(request) = request_plan.request() else {
                return exec_err!("qdrant query groups exec received a non-group request plan");
            };
            let response = client
                .query_groups((*request).clone())
                .await
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
            let mut groups = response.result.ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Qdrant query groups response missing result".to_owned(),
                )
            })?.groups;
            sort_point_groups(&mut groups, group_descending);
            let point_count = groups.iter().map(|group| group.hits.len()).sum();
            let points = groups.into_iter().flat_map(|group| group.hits).collect::<Vec<_>>();
            append_scored_points_to_batch(&schema, point_count, &score_output_names, points)
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
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "QdrantFacetExec: collection={}, field={}", self.spec.collection(), self.spec.op().field().key())?;
                write!(f, ", limit={}", self.spec.limit())
            }
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

impl DisplayAs for QdrantQueryBatchExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "QdrantQueryBatchExec: collection={}", self.spec.collection())
            }
            DisplayFormatType::TreeRender => write!(f, "QdrantQueryBatchExec"),
        }
    }
}

impl DisplayAs for QdrantQueryGroupsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "QdrantQueryGroupsExec: collection={}, group_by={}, group_size={}", self.spec.collection(), self.spec.group_by(), self.spec.group_size())
            }
            DisplayFormatType::TreeRender => write!(f, "QdrantQueryGroupsExec"),
        }
    }
}
