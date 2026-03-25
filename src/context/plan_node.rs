use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchemaRef, exec_err, plan_err};
use datafusion::logical_expr::{InvariantLevel, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::Expr;
use futures_util::stream;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{CountPointsBuilder, FacetCountsBuilder, facet_value};

use crate::pushdown::QdrantPayloadPath;
use crate::pushdown::filter::QdrantFilters;

pub(crate) const QDRANT_COUNT_NODE_NAME: &str = "QdrantCountNode";
pub(crate) const QDRANT_FACET_NODE_NAME: &str = "QdrantFacetNode";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum QdrantFacetOutput {
    Key,
    Count,
}

#[derive(Clone)]
pub(crate) struct QdrantCountNode {
    schema:     DFSchemaRef,
    client:     Arc<Qdrant>,
    collection: String,
    filters:    QdrantFilters,
}

impl QdrantCountNode {
    pub(crate) fn new(
        schema: DFSchemaRef,
        client: Arc<Qdrant>,
        collection: String,
        filters: QdrantFilters,
    ) -> Self {
        Self { schema, client, collection, filters }
    }

    pub(crate) fn execute(&self) -> Arc<dyn ExecutionPlan> {
        Arc::new(QdrantCountExec::new(
            Arc::clone(&self.client),
            self.collection.clone(),
            self.filters.clone(),
            Arc::clone(self.schema.inner()),
        ))
    }
}

impl std::fmt::Debug for QdrantCountNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantCountNode")
            .field("collection", &self.collection)
            .field("filters", &self.filters)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct QdrantFacetNode {
    schema:     DFSchemaRef,
    client:     Arc<Qdrant>,
    collection: String,
    filters:    QdrantFilters,
    field:      QdrantPayloadPath,
    limit:      u64,
    outputs:    Vec<QdrantFacetOutput>,
}

impl QdrantFacetNode {
    pub(crate) fn new(
        schema: DFSchemaRef,
        client: Arc<Qdrant>,
        collection: String,
        filters: QdrantFilters,
        field: QdrantPayloadPath,
        limit: u64,
        outputs: Vec<QdrantFacetOutput>,
    ) -> Self {
        Self { schema, client, collection, filters, field, limit, outputs }
    }

    pub(crate) fn execute(&self) -> Arc<dyn ExecutionPlan> {
        Arc::new(QdrantFacetExec::new(
            Arc::clone(&self.client),
            self.collection.clone(),
            self.filters.clone(),
            self.field.clone(),
            self.limit,
            self.outputs.clone(),
            Arc::clone(self.schema.inner()),
        ))
    }
}

impl std::fmt::Debug for QdrantFacetNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantFacetNode")
            .field("collection", &self.collection)
            .field("field", &self.field)
            .field("limit", &self.limit)
            .field("filters", &self.filters)
            .finish_non_exhaustive()
    }
}

impl UserDefinedLogicalNodeCore for QdrantCountNode {
    fn name(&self) -> &str { QDRANT_COUNT_NODE_NAME }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> { vec![] }

    fn schema(&self) -> &DFSchemaRef { &self.schema }

    fn expressions(&self) -> Vec<Expr> { vec![] }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{QDRANT_COUNT_NODE_NAME}: collection={}", self.collection)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<datafusion::logical_expr::LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("{QDRANT_COUNT_NODE_NAME} expects no expressions");
        }
        if !inputs.is_empty() {
            return plan_err!("{QDRANT_COUNT_NODE_NAME} expects no inputs");
        }
        Ok(self.clone())
    }

    fn check_invariants(&self, _check: InvariantLevel) -> datafusion::error::Result<()> { Ok(()) }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}

impl UserDefinedLogicalNodeCore for QdrantFacetNode {
    fn name(&self) -> &str { QDRANT_FACET_NODE_NAME }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> { vec![] }

    fn schema(&self) -> &DFSchemaRef { &self.schema }

    fn expressions(&self) -> Vec<Expr> { vec![] }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{QDRANT_FACET_NODE_NAME}: collection={}, field={}, limit={}",
            self.collection,
            self.field.key(),
            self.limit,
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<datafusion::logical_expr::LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("{QDRANT_FACET_NODE_NAME} expects no expressions");
        }
        if !inputs.is_empty() {
            return plan_err!("{QDRANT_FACET_NODE_NAME} expects no inputs");
        }
        Ok(self.clone())
    }

    fn check_invariants(&self, _check: InvariantLevel) -> datafusion::error::Result<()> { Ok(()) }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}

impl PartialEq for QdrantCountNode {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_eq(Arc::as_ptr(&self.client), Arc::as_ptr(&other.client))
            && self.collection == other.collection
            && self.filters == other.filters
            && self.schema == other.schema
    }
}

impl Eq for QdrantCountNode {}

impl PartialOrd for QdrantCountNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.collection.as_str(), format!("{:?}", self.filters), format!("{:?}", self.schema))
            .partial_cmp(&(
                other.collection.as_str(),
                format!("{:?}", other.filters),
                format!("{:?}", other.schema),
            ))
    }
}

impl std::hash::Hash for QdrantCountNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        QDRANT_COUNT_NODE_NAME.hash(state);
        Arc::as_ptr(&self.client).hash(state);
        self.collection.hash(state);
        format!("{:?}", self.filters).hash(state);
        format!("{:?}", self.schema).hash(state);
    }
}

impl PartialEq for QdrantFacetNode {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_eq(Arc::as_ptr(&self.client), Arc::as_ptr(&other.client))
            && self.collection == other.collection
            && self.filters == other.filters
            && self.field == other.field
            && self.limit == other.limit
            && self.outputs == other.outputs
            && self.schema == other.schema
    }
}

impl Eq for QdrantFacetNode {}

impl PartialOrd for QdrantFacetNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (
            self.collection.as_str(),
            self.field.key(),
            self.limit,
            &self.outputs,
            format!("{:?}", self.filters),
            format!("{:?}", self.schema),
        )
            .partial_cmp(&(
                other.collection.as_str(),
                other.field.key(),
                other.limit,
                &other.outputs,
                format!("{:?}", other.filters),
                format!("{:?}", other.schema),
            ))
    }
}

impl std::hash::Hash for QdrantFacetNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        QDRANT_FACET_NODE_NAME.hash(state);
        Arc::as_ptr(&self.client).hash(state);
        self.collection.hash(state);
        self.field.hash(state);
        self.limit.hash(state);
        self.outputs.hash(state);
        format!("{:?}", self.filters).hash(state);
        format!("{:?}", self.schema).hash(state);
    }
}

#[derive(Clone)]
pub(crate) struct QdrantCountExec {
    client:     Arc<Qdrant>,
    collection: String,
    filters:    QdrantFilters,
    schema:     SchemaRef,
    properties: Arc<PlanProperties>,
}

#[derive(Clone)]
pub(crate) struct QdrantFacetExec {
    client:     Arc<Qdrant>,
    collection: String,
    filters:    QdrantFilters,
    field:      QdrantPayloadPath,
    limit:      u64,
    outputs:    Vec<QdrantFacetOutput>,
    schema:     SchemaRef,
    properties: Arc<PlanProperties>,
}

impl QdrantCountExec {
    fn new(
        client: Arc<Qdrant>,
        collection: String,
        filters: QdrantFilters,
        schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { client, collection, filters, schema, properties: Arc::new(properties) }
    }
}

impl QdrantFacetExec {
    fn new(
        client: Arc<Qdrant>,
        collection: String,
        filters: QdrantFilters,
        field: QdrantPayloadPath,
        limit: u64,
        outputs: Vec<QdrantFacetOutput>,
        schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            client,
            collection,
            filters,
            field,
            limit,
            outputs,
            schema,
            properties: Arc::new(properties),
        }
    }
}

impl std::fmt::Debug for QdrantCountExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantCountExec")
            .field("collection", &self.collection)
            .field("filters", &self.filters)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for QdrantFacetExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantFacetExec")
            .field("collection", &self.collection)
            .field("field", &self.field)
            .field("limit", &self.limit)
            .field("filters", &self.filters)
            .finish_non_exhaustive()
    }
}

impl ExecutionPlan for QdrantCountExec {
    fn name(&self) -> &'static str { "QdrantCountExec" }

    fn as_any(&self) -> &dyn Any { self }

    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> datafusion::error::Result<TreeNodeRecursion>,
    ) -> datafusion::error::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return exec_err!("QdrantCountExec expects no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("QdrantCountExec invalid partition {partition}");
        }

        let client = Arc::clone(&self.client);
        let collection = self.collection.clone();
        let filters = self.filters.clone();
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
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
                Arc::new(Int64Array::from(vec![count])) as ArrayRef,
            ])?;
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
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> datafusion::error::Result<TreeNodeRecursion>,
    ) -> datafusion::error::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return exec_err!("QdrantFacetExec expects no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("QdrantFacetExec invalid partition {partition}");
        }

        let client = Arc::clone(&self.client);
        let collection = self.collection.clone();
        let filters = self.filters.clone();
        let field = self.field.clone();
        let limit = self.limit;
        let outputs = self.outputs.clone();
        let schema = Arc::clone(&self.schema);
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
                let facet_value::Variant::StringValue(facet_value) = value else {
                    return exec_err!(
                        "Qdrant facet field {} returned non-string value",
                        field.key()
                    );
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
            let columns = outputs
                .iter()
                .map(|output| match output {
                    QdrantFacetOutput::Key => Arc::clone(&key_array),
                    QdrantFacetOutput::Count => Arc::clone(&count_array),
                })
                .collect::<Vec<_>>();
            Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), stream::once(fut))))
    }
}

impl DisplayAs for QdrantCountExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "QdrantCountExec: collection={}", self.collection)
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
                self.collection,
                self.field.key(),
                self.limit,
            ),
            DisplayFormatType::TreeRender => write!(f, "QdrantFacetExec"),
        }
    }
}
