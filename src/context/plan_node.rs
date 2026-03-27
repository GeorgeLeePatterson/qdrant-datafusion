use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, DFSchemaRef, Result, exec_err, plan_err};
use datafusion::logical_expr::{InvariantLevel, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::Expr;
use futures_util::stream;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    CountPointsBuilder, FacetCountsBuilder, QueryPointsBuilder, facet_value,
};

use crate::arrow::deserialize::QdrantRecordBatchBuilder;
use crate::expr_fn::QdrantNearestCall;
use crate::pushdown::QdrantPayloadPath;
use crate::pushdown::filter::QdrantFilters;

pub(crate) const QDRANT_OP_NODE_NAME: &str = "QdrantOpNode";
pub(crate) const QDRANT_KERNEL_NODE_NAME: &str = "QdrantKernelNode";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum QdrantFacetOutput {
    Key,
    Count,
}

#[derive(Clone)]
pub(crate) struct QdrantOpNode {
    input: LogicalPlan,
    schema: DFSchemaRef,
    op: QdrantOp,
}

#[derive(Debug, Clone)]
pub(crate) enum QdrantOp {
    Query(QdrantQueryOp),
}

#[derive(Debug, Clone)]
pub(crate) struct QdrantQueryOp {
    pub(crate) query: QdrantQueryVariant,
    pub(crate) vector_field: String,
    pub(crate) score_field_name: String,
}

#[derive(Clone)]
pub(crate) struct QdrantKernelNode {
    schema: DFSchemaRef,
    client: Arc<Qdrant>,
    spec: QdrantKernelSpec,
}

#[derive(Debug, Clone)]
pub(crate) enum QdrantKernelSpec {
    Count(QdrantCountKernel),
    Facet(QdrantFacetKernel),
    Query(QdrantQueryKernel),
}

#[derive(Debug, Clone)]
pub(crate) struct QdrantCountKernel {
    pub(crate) collection: String,
    pub(crate) filters: QdrantFilters,
}

#[derive(Debug, Clone)]
pub(crate) struct QdrantFacetKernel {
    pub(crate) collection: String,
    pub(crate) filters: QdrantFilters,
    pub(crate) field: QdrantPayloadPath,
    pub(crate) limit: u64,
    pub(crate) outputs: Vec<QdrantFacetOutput>,
}

#[derive(Debug, Clone)]
pub(crate) struct QdrantQueryKernel {
    pub(crate) collection: String,
    pub(crate) filters: QdrantFilters,
    pub(crate) query: QdrantQueryVariant,
    pub(crate) using: Option<String>,
    pub(crate) limit: u64,
    pub(crate) score_threshold: Option<f32>,
    pub(crate) score_field_name: String,
}

#[derive(Debug, Clone)]
pub(crate) enum QdrantQueryVariant {
    Nearest { vector: Vec<f32> },
}

impl QdrantKernelNode {
    pub(crate) fn with_spec(
        schema: DFSchemaRef,
        client: Arc<Qdrant>,
        spec: QdrantKernelSpec,
    ) -> Self {
        Self { schema, client, spec }
    }

    pub(crate) fn count(
        schema: DFSchemaRef,
        client: Arc<Qdrant>,
        collection: String,
        filters: QdrantFilters,
    ) -> Self {
        Self::with_spec(
            schema,
            client,
            QdrantKernelSpec::Count(QdrantCountKernel { collection, filters }),
        )
    }

    pub(crate) fn facet(
        schema: DFSchemaRef,
        client: Arc<Qdrant>,
        collection: String,
        filters: QdrantFilters,
        field: QdrantPayloadPath,
        limit: u64,
        outputs: Vec<QdrantFacetOutput>,
    ) -> Self {
        Self::with_spec(
            schema,
            client,
            QdrantKernelSpec::Facet(QdrantFacetKernel {
                collection,
                filters,
                field,
                limit,
                outputs,
            }),
        )
    }

    pub(crate) fn execute(&self) -> Arc<dyn ExecutionPlan> {
        match &self.spec {
            QdrantKernelSpec::Count(spec) => Arc::new(QdrantCountExec::new(
                Arc::clone(&self.client),
                spec.collection.clone(),
                spec.filters.clone(),
                Arc::clone(self.schema.inner()),
            )),
            QdrantKernelSpec::Facet(spec) => Arc::new(QdrantFacetExec::new(
                Arc::clone(&self.client),
                spec.collection.clone(),
                spec.filters.clone(),
                spec.field.clone(),
                spec.limit,
                spec.outputs.clone(),
                Arc::clone(self.schema.inner()),
            )),
            QdrantKernelSpec::Query(spec) => Arc::new(QdrantQueryExec::new(
                Arc::clone(&self.client),
                spec.clone(),
                Arc::clone(self.schema.inner()),
            )),
        }
    }
}

impl QdrantOpNode {
    pub(crate) fn query(input: LogicalPlan, op: QdrantQueryOp) -> Result<Self> {
        let Ok(vector_field) = input.schema().field_with_unqualified_name(&op.vector_field) else {
            return plan_err!("nearest vector '{}' not found", op.vector_field);
        };
        let DataType::FixedSizeList(_, width) = vector_field.data_type() else {
            return plan_err!("nearest requires a dense vector field");
        };
        let QdrantQueryVariant::Nearest { vector } = &op.query;
        if usize::try_from(*width).ok() != Some(vector.len()) {
            return plan_err!("nearest query width does not match vector field");
        }
        if input.schema().fields().iter().any(|field| field.name() == &op.score_field_name) {
            return plan_err!("nearest score field conflicts with schema");
        }

        let mut fields = input
            .schema()
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect::<Vec<_>>();
        fields.push((None, Arc::new(Field::new(&op.score_field_name, DataType::Float32, false))));
        let schema =
            DFSchema::new_with_metadata(fields, input.schema().inner().metadata().clone())?
                .with_functional_dependencies(input.schema().functional_dependencies().clone())?;

        Ok(Self { input, schema: Arc::new(schema), op: QdrantOp::Query(op) })
    }

    pub(crate) fn from_plan(plan: &LogicalPlan) -> Option<&Self> {
        let LogicalPlan::Extension(extension) = plan else {
            return None;
        };
        extension.node.as_any().downcast_ref::<Self>()
    }

    pub(crate) fn input(&self) -> &LogicalPlan {
        &self.input
    }

    pub(crate) fn op(&self) -> &QdrantOp {
        &self.op
    }

    pub(crate) fn output_schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    pub(crate) fn score_field_name(&self) -> &str {
        match &self.op {
            QdrantOp::Query(op) => &op.score_field_name,
        }
    }

    pub(crate) fn matches_nearest_call(&self, call: &QdrantNearestCall) -> bool {
        match &self.op {
            QdrantOp::Query(op) => {
                op.vector_field == call.vector_field
                    && op.score_field_name == call.score_field_name
                    && match (&op.query, call.vector.as_slice()) {
                        (QdrantQueryVariant::Nearest { vector }, rhs) => vector
                            .iter()
                            .map(|value| value.to_bits())
                            .eq(rhs.iter().map(|value| value.to_bits())),
                    }
            }
        }
    }
}

impl QdrantKernelSpec {
    fn collection(&self) -> &str {
        match self {
            Self::Count(spec) => &spec.collection,
            Self::Facet(spec) => &spec.collection,
            Self::Query(spec) => &spec.collection,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Count(_) => "count",
            Self::Facet(_) => "facet",
            Self::Query(_) => "query",
        }
    }

    fn kind_rank(&self) -> u8 {
        match self {
            Self::Count(_) => 0,
            Self::Facet(_) => 1,
            Self::Query(_) => 2,
        }
    }
}

impl std::fmt::Debug for QdrantOpNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("QdrantOpNode");
        match &self.op {
            QdrantOp::Query(op) => {
                let _ = debug
                    .field("kind", &"query")
                    .field("vector_field", &op.vector_field)
                    .field("score_field_name", &op.score_field_name);
            }
        }
        debug.finish_non_exhaustive()
    }
}

impl UserDefinedLogicalNodeCore for QdrantOpNode {
    fn name(&self) -> &str {
        QDRANT_OP_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::from([self.score_field_name().to_owned()])
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.op {
            QdrantOp::Query(op) => write!(
                f,
                "{QDRANT_OP_NODE_NAME}: kind=query, vector_field={}, score_field={}",
                op.vector_field, op.score_field_name
            ),
        }
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("{QDRANT_OP_NODE_NAME} expects no expressions");
        }
        if inputs.len() != 1 {
            return plan_err!("{QDRANT_OP_NODE_NAME} expects one input");
        }
        let input = inputs.into_iter().next().expect("checked input length");
        match &self.op {
            QdrantOp::Query(op) => Self::query(input, op.clone()),
        }
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<()> {
        Ok(())
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let QdrantOp::Query(op) = &self.op;
        let vector_index = self.input.schema().index_of_column_by_name(None, &op.vector_field)?;
        let score_index =
            self.schema.index_of_column_by_name(None, &op.score_field_name).unwrap_or(usize::MAX);
        let mut input_columns = output_columns
            .iter()
            .filter(|index| **index != score_index)
            .copied()
            .collect::<Vec<_>>();
        if !input_columns.contains(&vector_index) {
            input_columns.push(vector_index);
        }
        input_columns.sort_unstable();
        input_columns.dedup();
        Some(vec![input_columns])
    }
}

impl PartialEq for QdrantOpNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.schema == other.schema
            && match (&self.op, &other.op) {
                (QdrantOp::Query(lhs), QdrantOp::Query(rhs)) => {
                    lhs.vector_field == rhs.vector_field
                        && lhs.score_field_name == rhs.score_field_name
                        && match (&lhs.query, &rhs.query) {
                            (
                                QdrantQueryVariant::Nearest { vector: lhs },
                                QdrantQueryVariant::Nearest { vector: rhs },
                            ) => lhs
                                .iter()
                                .map(|value| value.to_bits())
                                .eq(rhs.iter().map(|value| value.to_bits())),
                        }
                }
            }
    }
}

impl Eq for QdrantOpNode {}

impl PartialOrd for QdrantOpNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (&self.op, &other.op) {
            (QdrantOp::Query(lhs), QdrantOp::Query(rhs)) => (
                lhs.vector_field.as_str(),
                lhs.score_field_name.as_str(),
                match &lhs.query {
                    QdrantQueryVariant::Nearest { vector } => {
                        vector.iter().map(|value| value.to_bits()).collect::<Vec<_>>()
                    }
                },
                format!("{:?}", self.input),
                format!("{:?}", self.schema),
            )
                .partial_cmp(&(
                    rhs.vector_field.as_str(),
                    rhs.score_field_name.as_str(),
                    match &rhs.query {
                        QdrantQueryVariant::Nearest { vector } => {
                            vector.iter().map(|value| value.to_bits()).collect::<Vec<_>>()
                        }
                    },
                    format!("{:?}", other.input),
                    format!("{:?}", other.schema),
                )),
        }
    }
}

impl std::hash::Hash for QdrantOpNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        QDRANT_OP_NODE_NAME.hash(state);
        self.input.hash(state);
        match &self.op {
            QdrantOp::Query(op) => {
                op.vector_field.hash(state);
                op.score_field_name.hash(state);
                match &op.query {
                    QdrantQueryVariant::Nearest { vector } => {
                        for value in vector {
                            value.to_bits().hash(state);
                        }
                    }
                }
            }
        }
        format!("{:?}", self.schema).hash(state);
    }
}

impl std::fmt::Debug for QdrantKernelNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("QdrantKernelNode");
        let _ = debug.field("kind", &self.spec.kind()).field("collection", &self.spec.collection());
        match &self.spec {
            QdrantKernelSpec::Count(spec) => {
                let _ = debug.field("filters", &spec.filters);
            }
            QdrantKernelSpec::Facet(spec) => {
                let _ = debug
                    .field("field", &spec.field)
                    .field("limit", &spec.limit)
                    .field("filters", &spec.filters);
            }
            QdrantKernelSpec::Query(spec) => {
                let _ = debug
                    .field("using", &spec.using)
                    .field("limit", &spec.limit)
                    .field("score_threshold", &spec.score_threshold)
                    .field("score_field_name", &spec.score_field_name)
                    .field("filters", &spec.filters);
            }
        }
        debug.finish_non_exhaustive()
    }
}

impl UserDefinedLogicalNodeCore for QdrantKernelNode {
    fn name(&self) -> &str {
        QDRANT_KERNEL_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.spec {
            QdrantKernelSpec::Count(spec) => {
                write!(f, "{QDRANT_KERNEL_NODE_NAME}: kind=count, collection={}", spec.collection)
            }
            QdrantKernelSpec::Facet(spec) => write!(
                f,
                "{QDRANT_KERNEL_NODE_NAME}: kind=facet, collection={}, field={}, limit={}",
                spec.collection,
                spec.field.key(),
                spec.limit,
            ),
            QdrantKernelSpec::Query(spec) => {
                write!(
                    f,
                    "{QDRANT_KERNEL_NODE_NAME}: kind=query, collection={}, limit={}",
                    spec.collection, spec.limit
                )?;
                if let Some(using) = &spec.using {
                    write!(f, ", using={using}")?;
                }
                if let Some(threshold) = spec.score_threshold {
                    write!(f, ", score_threshold={threshold}")?;
                }
                write!(f, ", score_field={}", spec.score_field_name)?;
                Ok(())
            }
        }
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("{QDRANT_KERNEL_NODE_NAME} expects no expressions");
        }
        if !inputs.is_empty() {
            return plan_err!("{QDRANT_KERNEL_NODE_NAME} expects no inputs");
        }
        Ok(self.clone())
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<()> {
        Ok(())
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}

impl PartialEq for QdrantKernelNode {
    fn eq(&self, other: &Self) -> bool {
        if !std::ptr::addr_eq(Arc::as_ptr(&self.client), Arc::as_ptr(&other.client))
            || self.schema != other.schema
        {
            return false;
        }
        match (&self.spec, &other.spec) {
            (QdrantKernelSpec::Count(lhs), QdrantKernelSpec::Count(rhs)) => {
                lhs.collection == rhs.collection && lhs.filters == rhs.filters
            }
            (QdrantKernelSpec::Facet(lhs), QdrantKernelSpec::Facet(rhs)) => {
                lhs.collection == rhs.collection
                    && lhs.filters == rhs.filters
                    && lhs.field == rhs.field
                    && lhs.limit == rhs.limit
                    && lhs.outputs == rhs.outputs
            }
            (QdrantKernelSpec::Query(lhs), QdrantKernelSpec::Query(rhs)) => {
                lhs.collection == rhs.collection
                    && lhs.filters == rhs.filters
                    && lhs.using == rhs.using
                    && lhs.limit == rhs.limit
                    && lhs.score_field_name == rhs.score_field_name
                    && lhs.score_threshold.map(f32::to_bits)
                        == rhs.score_threshold.map(f32::to_bits)
                    && match (&lhs.query, &rhs.query) {
                        (
                            QdrantQueryVariant::Nearest { vector: lhs },
                            QdrantQueryVariant::Nearest { vector: rhs },
                        ) => lhs
                            .iter()
                            .map(|value| value.to_bits())
                            .eq(rhs.iter().map(|value| value.to_bits())),
                    }
            }
            _ => false,
        }
    }
}

impl Eq for QdrantKernelNode {}

impl PartialOrd for QdrantKernelNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let kind_cmp = self.spec.kind_rank().cmp(&other.spec.kind_rank());
        if kind_cmp != std::cmp::Ordering::Equal {
            return Some(kind_cmp);
        }
        match (&self.spec, &other.spec) {
            (QdrantKernelSpec::Count(lhs), QdrantKernelSpec::Count(rhs)) => (
                lhs.collection.as_str(),
                format!("{:?}", lhs.filters),
                format!("{:?}", self.schema),
            )
                .partial_cmp(&(
                    rhs.collection.as_str(),
                    format!("{:?}", rhs.filters),
                    format!("{:?}", other.schema),
                )),
            (QdrantKernelSpec::Facet(lhs), QdrantKernelSpec::Facet(rhs)) => (
                lhs.collection.as_str(),
                lhs.field.key(),
                lhs.limit,
                &lhs.outputs,
                format!("{:?}", lhs.filters),
                format!("{:?}", self.schema),
            )
                .partial_cmp(&(
                    rhs.collection.as_str(),
                    rhs.field.key(),
                    rhs.limit,
                    &rhs.outputs,
                    format!("{:?}", rhs.filters),
                    format!("{:?}", other.schema),
                )),
            (QdrantKernelSpec::Query(lhs), QdrantKernelSpec::Query(rhs)) => (
                lhs.collection.as_str(),
                format!("{:?}", lhs.filters),
                match &lhs.query {
                    QdrantQueryVariant::Nearest { vector } => {
                        vector.iter().map(|value| value.to_bits()).collect::<Vec<_>>()
                    }
                },
                &lhs.using,
                lhs.limit,
                lhs.score_field_name.as_str(),
                lhs.score_threshold.map(f32::to_bits),
                format!("{:?}", self.schema),
            )
                .partial_cmp(&(
                    rhs.collection.as_str(),
                    format!("{:?}", rhs.filters),
                    match &rhs.query {
                        QdrantQueryVariant::Nearest { vector } => {
                            vector.iter().map(|value| value.to_bits()).collect::<Vec<_>>()
                        }
                    },
                    &rhs.using,
                    rhs.limit,
                    rhs.score_field_name.as_str(),
                    rhs.score_threshold.map(f32::to_bits),
                    format!("{:?}", other.schema),
                )),
            _ => Some(std::cmp::Ordering::Equal),
        }
    }
}

impl std::hash::Hash for QdrantKernelNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        QDRANT_KERNEL_NODE_NAME.hash(state);
        Arc::as_ptr(&self.client).hash(state);
        self.spec.kind_rank().hash(state);
        match &self.spec {
            QdrantKernelSpec::Count(spec) => {
                spec.collection.hash(state);
                format!("{:?}", spec.filters).hash(state);
            }
            QdrantKernelSpec::Facet(spec) => {
                spec.collection.hash(state);
                spec.field.hash(state);
                spec.limit.hash(state);
                spec.outputs.hash(state);
                format!("{:?}", spec.filters).hash(state);
            }
            QdrantKernelSpec::Query(spec) => {
                spec.collection.hash(state);
                format!("{:?}", spec.filters).hash(state);
                spec.using.hash(state);
                spec.limit.hash(state);
                spec.score_field_name.hash(state);
                spec.score_threshold.map(f32::to_bits).hash(state);
                match &spec.query {
                    QdrantQueryVariant::Nearest { vector } => {
                        for value in vector {
                            value.to_bits().hash(state);
                        }
                    }
                }
            }
        }
        format!("{:?}", self.schema).hash(state);
    }
}

#[derive(Clone)]
pub(crate) struct QdrantCountExec {
    client: Arc<Qdrant>,
    collection: String,
    filters: QdrantFilters,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

#[derive(Clone)]
pub(crate) struct QdrantFacetExec {
    client: Arc<Qdrant>,
    collection: String,
    filters: QdrantFilters,
    field: QdrantPayloadPath,
    limit: u64,
    outputs: Vec<QdrantFacetOutput>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

#[derive(Clone)]
pub(crate) struct QdrantQueryExec {
    client: Arc<Qdrant>,
    spec: QdrantQueryKernel,
    schema: SchemaRef,
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

impl QdrantQueryExec {
    fn new(client: Arc<Qdrant>, spec: QdrantQueryKernel, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { client, spec, schema, properties: Arc::new(properties) }
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

impl std::fmt::Debug for QdrantQueryExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantQueryExec")
            .field("collection", &self.spec.collection)
            .field("using", &self.spec.using)
            .field("limit", &self.spec.limit)
            .field("score_threshold", &self.spec.score_threshold)
            .field("score_field_name", &self.spec.score_field_name)
            .field("filters", &self.spec.filters)
            .finish_non_exhaustive()
    }
}

impl ExecutionPlan for QdrantCountExec {
    fn name(&self) -> &'static str {
        "QdrantCountExec"
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
    fn name(&self) -> &'static str {
        "QdrantFacetExec"
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

impl ExecutionPlan for QdrantQueryExec {
    fn name(&self) -> &'static str {
        "QdrantQueryExec"
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

        let client = Arc::clone(&self.client);
        let spec = self.spec.clone();
        let schema = Arc::clone(&self.schema);
        let fut = async move {
            let QdrantQueryKernel {
                collection,
                filters,
                query,
                using,
                limit,
                score_threshold,
                score_field_name,
            } = spec;

            if limit == 0 {
                return Ok(RecordBatch::new_empty(schema));
            }

            let mut request = QueryPointsBuilder::new(collection)
                .query(match query {
                    QdrantQueryVariant::Nearest { vector } => vector,
                })
                .limit(limit)
                .with_payload(true)
                .with_vectors(true);
            if let Some(using) = using {
                request = request.using(using);
            }
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
                Some(&score_field_name),
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

impl DisplayAs for QdrantQueryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "QdrantQueryExec: collection={}, limit={}",
                    self.spec.collection, self.spec.limit
                )?;
                if let Some(using) = &self.spec.using {
                    write!(f, ", using={using}")?;
                }
                if let Some(threshold) = self.spec.score_threshold {
                    write!(f, ", score_threshold={threshold}")?;
                }
                write!(f, ", score_field={}", self.spec.score_field_name)
            }
            DisplayFormatType::TreeRender => write!(f, "QdrantQueryExec"),
        }
    }
}
