use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{NullEquality, Result, ScalarValue, plan_err};
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion::logical_expr::utils::{conjunction, disjunction};
use datafusion::logical_expr::{Extension, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::AnalyzerRule;
use qdrant_client::qdrant::PointId;
use qdrant_client::qdrant::point_id::PointIdOptions;

use super::common::qdrant_source;
use super::count_pushdown::count_node;
use super::facet_pushdown::facet_node;
use crate::context::plan_node::{
    QDRANT_COUNT_NODE_NAME, QDRANT_FACET_NODE_NAME, QdrantCountNode, QdrantFacetNode,
};
use crate::pushdown::{QdrantFilters, logical_payload_path};
use crate::table::QdrantTableProvider;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantSourceClass {
    None,
    SingleQdrant,
    MultiQdrant,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantTopologyClass {
    Leaf,
    UnaryChain,
    UnaryRelationChange,
    MultiBranch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantCompositionClass {
    Atomic,
    Mergeable,
    Batchable,
    Coordinated,
    LocalCompose,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantKernelClass {
    None,
    ExactSelf,
    ExactChild,
    ExactChildren,
}

#[derive(Debug, Clone)]
enum QdrantRelationNode {
    Count(QdrantCountNode),
    Facet(QdrantFacetNode),
}

#[derive(Debug, Clone)]
struct QdrantRelationCandidate {
    source:      QdrantSourceClass,
    topology:    QdrantTopologyClass,
    composition: QdrantCompositionClass,
    node:        QdrantRelationNode,
}

impl QdrantRelationCandidate {
    fn into_plan(self) -> LogicalPlan {
        let _ = (self.source, self.topology, self.composition);
        let node: Arc<dyn datafusion::logical_expr::UserDefinedLogicalNode> = match self.node {
            QdrantRelationNode::Count(node) => Arc::new(node),
            QdrantRelationNode::Facet(node) => Arc::new(node),
        };
        LogicalPlan::Extension(Extension { node })
    }
}

#[derive(Debug, Clone)]
struct QdrantSubtreeStatus {
    class:     QdrantSubtreeClass,
    candidate: Option<QdrantRelationCandidate>,
}

struct RawQdrantUnion {
    collection:     String,
    client:         Arc<qdrant_client::Qdrant>,
    schema:         datafusion::arrow::datatypes::SchemaRef,
    payload_schema: Arc<crate::pushdown::QdrantPayloadSchema>,
    branches:       Vec<Option<datafusion::logical_expr::Expr>>,
    branch_ids:     Vec<Option<Vec<PointId>>>,
}

struct RawQdrantSetJoin {
    collection:     String,
    client:         Arc<qdrant_client::Qdrant>,
    schema:         datafusion::arrow::datatypes::SchemaRef,
    payload_schema: Arc<crate::pushdown::QdrantPayloadSchema>,
    left_filter:    Option<datafusion::logical_expr::Expr>,
    right_filter:   Option<datafusion::logical_expr::Expr>,
    join_type:      JoinType,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct QdrantRelationPushdown;

impl AnalyzerRule for QdrantRelationPushdown {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            let status = subtree_status(&plan)?;
            if let Some(candidate) = status.candidate {
                return Ok(Transformed::yes(candidate.into_plan()));
            }
            if let Some(distinct_input) = redundant_raw_qdrant_distinct_plan(&plan) {
                return Ok(Transformed::yes(distinct_input));
            }
            if status.class.composition == QdrantCompositionClass::Mergeable
                && let Some(merged) = mergeable_set_join_plan(&plan)?
            {
                return Ok(Transformed::yes(merged));
            }
            if status.class.composition == QdrantCompositionClass::Mergeable
                && let Some(merged) = mergeable_union_plan(&plan)?
            {
                return Ok(Transformed::yes(merged));
            }
            if status.class.composition == QdrantCompositionClass::Invalid {
                return plan_err!("unsupported qdrant payload access outside admitted kernel");
            }
            Ok(Transformed::no(plan))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str { "qdrant_relation_pushdown" }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QdrantSubtreeClass {
    source:      QdrantSourceClass,
    topology:    QdrantTopologyClass,
    composition: QdrantCompositionClass,
    kernel:      QdrantKernelClass,
}

fn subtree_status(plan: &LogicalPlan) -> Result<QdrantSubtreeStatus> {
    let candidate = exact_self_candidate(plan)?;
    let source = source_class(plan);
    let topology = topology_class(plan);
    let kernel = kernel_class(plan, candidate.is_some());
    let composition = composition_class(plan, source, topology, kernel)?;
    let class = QdrantSubtreeClass { source, topology, composition, kernel };
    let candidate = if class.source == QdrantSourceClass::SingleQdrant
        && class.composition == QdrantCompositionClass::Atomic
    {
        candidate
    } else {
        None
    };
    Ok(QdrantSubtreeStatus { class, candidate })
}

fn exact_self_candidate(plan: &LogicalPlan) -> Result<Option<QdrantRelationCandidate>> {
    if let Some(node) = facet_node(plan)? {
        return Ok(Some(QdrantRelationCandidate {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    topology_class(plan),
            composition: QdrantCompositionClass::Atomic,
            node:        QdrantRelationNode::Facet(node),
        }));
    }
    if let Some(node) = count_node(plan)? {
        return Ok(Some(QdrantRelationCandidate {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    topology_class(plan),
            composition: QdrantCompositionClass::Atomic,
            node:        QdrantRelationNode::Count(node),
        }));
    }
    Ok(None)
}

#[cfg(test)]
fn subtree_class(plan: &LogicalPlan) -> Result<QdrantSubtreeClass> {
    subtree_status(plan).map(|status| status.class)
}

fn source_class(plan: &LogicalPlan) -> QdrantSourceClass {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let Ok(provider) = source_as_provider(&scan.source) else {
                return QdrantSourceClass::None;
            };
            if provider.as_any().is::<QdrantTableProvider>() {
                QdrantSourceClass::SingleQdrant
            } else {
                QdrantSourceClass::None
            }
        }
        LogicalPlan::Extension(extension)
            if matches!(extension.node.name(), QDRANT_COUNT_NODE_NAME | QDRANT_FACET_NODE_NAME) =>
        {
            QdrantSourceClass::SingleQdrant
        }
        _ => combine_sources(plan.inputs().into_iter().map(source_class)),
    }
}

fn combine_sources(sources: impl IntoIterator<Item = QdrantSourceClass>) -> QdrantSourceClass {
    let mut qdrant_count = 0_u8;
    let mut has_other = false;
    for source in sources {
        match source {
            QdrantSourceClass::None => {}
            QdrantSourceClass::SingleQdrant => qdrant_count = qdrant_count.saturating_add(1),
            QdrantSourceClass::MultiQdrant => qdrant_count = qdrant_count.saturating_add(2),
            QdrantSourceClass::Mixed => {
                qdrant_count = qdrant_count.saturating_add(1);
                has_other = true;
            }
        }
    }
    match (qdrant_count, has_other) {
        (0, false) => QdrantSourceClass::None,
        (1, false) => QdrantSourceClass::SingleQdrant,
        (_, false) => QdrantSourceClass::MultiQdrant,
        _ => QdrantSourceClass::Mixed,
    }
}

fn topology_class(plan: &LogicalPlan) -> QdrantTopologyClass {
    match plan {
        LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::Extension(_) => QdrantTopologyClass::Leaf,
        LogicalPlan::Aggregate(_) | LogicalPlan::Distinct(_) => {
            QdrantTopologyClass::UnaryRelationChange
        }
        _ if plan.inputs().len() > 1 => QdrantTopologyClass::MultiBranch,
        _ => QdrantTopologyClass::UnaryChain,
    }
}

fn composition_class(
    plan: &LogicalPlan,
    source: QdrantSourceClass,
    topology: QdrantTopologyClass,
    kernel: QdrantKernelClass,
) -> Result<QdrantCompositionClass> {
    if invalid_payload_access_surface(plan, source, kernel)? {
        return Ok(QdrantCompositionClass::Invalid);
    }
    if kernel == QdrantKernelClass::ExactSelf {
        return Ok(QdrantCompositionClass::Atomic);
    }
    if mergeable_raw_set_join(plan) {
        return Ok(QdrantCompositionClass::Mergeable);
    }
    if mergeable_raw_union_distinct(plan)? {
        return Ok(QdrantCompositionClass::Mergeable);
    }
    if matches!(plan, LogicalPlan::Union(_)) {
        return Ok(match kernel {
            QdrantKernelClass::ExactChild | QdrantKernelClass::ExactChildren => {
                QdrantCompositionClass::Batchable
            }
            QdrantKernelClass::None if mergeable_raw_union(plan)? => {
                QdrantCompositionClass::Mergeable
            }
            _ => QdrantCompositionClass::LocalCompose,
        });
    }
    Ok(match (source, topology, kernel) {
        (
            QdrantSourceClass::SingleQdrant,
            QdrantTopologyClass::Leaf
            | QdrantTopologyClass::UnaryChain
            | QdrantTopologyClass::UnaryRelationChange,
            QdrantKernelClass::None,
        ) => QdrantCompositionClass::Atomic,
        (
            QdrantSourceClass::MultiQdrant,
            QdrantTopologyClass::Leaf
            | QdrantTopologyClass::UnaryChain
            | QdrantTopologyClass::UnaryRelationChange,
            QdrantKernelClass::None,
        ) => QdrantCompositionClass::Coordinated,
        (QdrantSourceClass::SingleQdrant | QdrantSourceClass::MultiQdrant, _, _) => {
            QdrantCompositionClass::LocalCompose
        }
        _ => QdrantCompositionClass::LocalCompose,
    })
}

fn kernel_class(plan: &LogicalPlan, exact_self: bool) -> QdrantKernelClass {
    if exact_self || is_qdrant_relation_extension(plan) {
        return QdrantKernelClass::ExactSelf;
    }
    combine_kernels(plan.inputs().into_iter().map(|child| kernel_class(child, false)))
}

fn combine_kernels(kernels: impl IntoIterator<Item = QdrantKernelClass>) -> QdrantKernelClass {
    let kernel_count = kernels
        .into_iter()
        .map(|kernel| match kernel {
            QdrantKernelClass::None => 0_u8,
            QdrantKernelClass::ExactSelf | QdrantKernelClass::ExactChild => 1_u8,
            QdrantKernelClass::ExactChildren => 2_u8,
        })
        .fold(0_u8, u8::saturating_add);
    match kernel_count {
        0 => QdrantKernelClass::None,
        1 => QdrantKernelClass::ExactChild,
        _ => QdrantKernelClass::ExactChildren,
    }
}

fn is_qdrant_relation_extension(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Extension(extension)
            if matches!(extension.node.name(), QDRANT_COUNT_NODE_NAME | QDRANT_FACET_NODE_NAME)
    )
}

fn invalid_payload_access_surface(
    plan: &LogicalPlan,
    source: QdrantSourceClass,
    kernel: QdrantKernelClass,
) -> Result<bool> {
    if !matches!(source, QdrantSourceClass::SingleQdrant | QdrantSourceClass::MultiQdrant)
        || !matches!(kernel, QdrantKernelClass::None | QdrantKernelClass::ExactChild)
    {
        return Ok(false);
    }
    let direct_local_shell = match plan {
        LogicalPlan::Projection(projection) => {
            matches!(
                projection.input.as_ref(),
                LogicalPlan::TableScan(_) | LogicalPlan::Extension(_)
            )
        }
        LogicalPlan::Window(window) => {
            matches!(window.input.as_ref(), LogicalPlan::TableScan(_) | LogicalPlan::Extension(_))
        }
        _ => false,
    };
    if !direct_local_shell {
        return Ok(false);
    }
    plan_uses_payload_access(plan)
}

fn plan_uses_payload_access(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    let _ = plan.apply_expressions(|expr| {
        if expr.exists(|expr| Ok(logical_payload_path(expr).is_some()))? {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(found)
}

fn raw_qdrant_union(plan: &LogicalPlan) -> Result<Option<RawQdrantUnion>> {
    let LogicalPlan::Union(union) = plan else {
        return Ok(None);
    };
    let mut collection = None::<String>;
    let mut client = None;
    let mut schema = None;
    let mut payload_schema = None;
    let mut branches = vec![];
    let mut branch_ids = vec![];
    for child in &union.inputs {
        let Some(source) = qdrant_source(child.as_ref()) else {
            return Ok(None);
        };
        match &collection {
            None => collection = Some(source.collection.clone()),
            Some(current) if current == &source.collection => {}
            Some(_) => return Ok(None),
        }
        if !source.filters.iter().all(|filter| {
            QdrantFilters::supports_exact(&source.schema, &source.payload_schema, filter)
        }) {
            return Ok(None);
        }
        let filters =
            QdrantFilters::try_new(&source.schema, &source.payload_schema, &source.filters)?;
        branches.push(conjunction(source.filters));
        branch_ids.push(filters.possible_point_ids());
        if client.is_none() {
            client = Some(source.client);
        }
        if schema.is_none() {
            schema = Some(source.schema);
        }
        if payload_schema.is_none() {
            payload_schema = Some(source.payload_schema);
        }
    }
    Ok(match (collection, client, schema, payload_schema) {
        (Some(collection), Some(client), Some(schema), Some(payload_schema)) => {
            Some(RawQdrantUnion {
                collection,
                client,
                schema,
                payload_schema,
                branches,
                branch_ids,
            })
        }
        _ => None,
    })
}

fn mergeable_raw_union(plan: &LogicalPlan) -> Result<bool> {
    let Some(union) = raw_qdrant_union(plan)? else {
        return Ok(false);
    };
    let Some(branch_ids) = union.branch_ids.into_iter().collect::<Option<Vec<Vec<PointId>>>>()
    else {
        return Ok(false);
    };
    Ok(union.branches.iter().all(Option::is_some) && point_id_sets_are_disjoint(&branch_ids))
}

fn mergeable_raw_union_distinct(plan: &LogicalPlan) -> Result<bool> {
    let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) = plan else {
        return Ok(false);
    };
    raw_qdrant_union(input.as_ref()).map(|union| union.is_some())
}

fn raw_qdrant_set_join(plan: &LogicalPlan) -> Option<RawQdrantSetJoin> {
    let LogicalPlan::Join(join) = plan else {
        return None;
    };
    if join.filter.is_some()
        || join.null_aware
        || join.null_equality != NullEquality::NullEqualsNull
        || !matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        || !full_row_join_keys(join)
    {
        return None;
    }
    let left_source = raw_qdrant_set_branch_source(join.left.as_ref())?;
    let right_source = raw_qdrant_set_branch_source(join.right.as_ref())?;
    if left_source.collection != right_source.collection {
        return None;
    }
    if !left_source.filters.iter().all(|filter| {
        QdrantFilters::supports_exact(&left_source.schema, &left_source.payload_schema, filter)
    }) || !right_source.filters.iter().all(|filter| {
        QdrantFilters::supports_exact(&right_source.schema, &right_source.payload_schema, filter)
    }) {
        return None;
    }
    Some(RawQdrantSetJoin {
        collection:     left_source.collection,
        client:         left_source.client,
        schema:         left_source.schema,
        payload_schema: left_source.payload_schema,
        left_filter:    conjunction(left_source.filters),
        right_filter:   conjunction(right_source.filters),
        join_type:      join.join_type,
    })
}

fn mergeable_raw_set_join(plan: &LogicalPlan) -> bool { raw_qdrant_set_join(plan).is_some() }

fn raw_qdrant_set_branch_source(plan: &LogicalPlan) -> Option<super::common::QdrantSource> {
    let mut plan = plan;
    loop {
        match plan {
            LogicalPlan::SubqueryAlias(alias) => {
                plan = alias.input.as_ref();
            }
            LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) => {
                plan = input.as_ref();
            }
            _ => return qdrant_source(plan),
        }
    }
}

fn full_row_join_keys(join: &datafusion::logical_expr::logical_plan::Join) -> bool {
    let left_fields = join.left.schema().fields();
    let right_fields = join.right.schema().fields();
    join.on.len() == left_fields.len()
        && left_fields.len() == right_fields.len()
        && join.on.iter().zip(left_fields.iter().zip(right_fields.iter())).all(
            |((left, right), (left_field, right_field))| match (left, right) {
                (
                    datafusion::logical_expr::Expr::Column(left),
                    datafusion::logical_expr::Expr::Column(right),
                ) => left.name == *left_field.name() && right.name == *right_field.name(),
                _ => false,
            },
        )
}

fn point_id_sets_are_disjoint(id_sets: &[Vec<PointId>]) -> bool {
    let mut seen = vec![];
    for ids in id_sets {
        let mut branch_seen = vec![];
        for id in ids {
            let key = point_id_key(id);
            if branch_seen.iter().any(|seen| seen == &key) {
                continue;
            }
            if seen.iter().any(|seen| seen == &key) {
                return false;
            }
            branch_seen.push(key);
        }
        seen.extend(branch_seen);
    }
    true
}

fn point_id_key(id: &PointId) -> String {
    match id.point_id_options.as_ref() {
        Some(PointIdOptions::Num(value)) => format!("num:{value}"),
        Some(PointIdOptions::Uuid(value)) => format!("uuid:{value}"),
        None => "missing".to_owned(),
    }
}

fn mergeable_union_plan(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let (union, require_disjoint) = match plan {
        LogicalPlan::Union(_) => (raw_qdrant_union(plan)?, true),
        LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) => {
            (raw_qdrant_union(input.as_ref())?, false)
        }
        _ => return Ok(None),
    };
    let Some(union) = union else {
        return Ok(None);
    };
    let filter = merged_branch_filter(&union.branches);
    if require_disjoint {
        let Some(branch_ids) =
            union.branch_ids.iter().cloned().collect::<Option<Vec<Vec<PointId>>>>()
        else {
            return Ok(None);
        };
        if !union.branches.iter().all(Option::is_some) || !point_id_sets_are_disjoint(&branch_ids) {
            return Ok(None);
        }
    }
    let provider = Arc::new(QdrantTableProvider::new_for_planner(
        union.collection.clone(),
        union.client,
        union.schema,
        union.payload_schema,
    ));
    let builder = LogicalPlanBuilder::scan(union.collection, provider_as_source(provider), None)?;
    match filter {
        Some(filter) => builder.filter(filter)?.build().map(Some),
        None => builder.build().map(Some),
    }
}

fn merged_branch_filter(
    branches: &[Option<datafusion::logical_expr::Expr>],
) -> Option<datafusion::logical_expr::Expr> {
    if branches.iter().any(Option::is_none) {
        return None;
    }
    disjunction(branches.iter().flatten().cloned())
}

fn mergeable_set_join_plan(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let Some(join) = raw_qdrant_set_join(plan) else {
        return Ok(None);
    };
    let filter = match join.join_type {
        JoinType::LeftSemi => {
            conjunction([join.left_filter, join.right_filter].into_iter().flatten())
        }
        JoinType::LeftAnti => except_filter(join.left_filter, join.right_filter),
        _ => return Ok(None),
    };
    let provider = Arc::new(QdrantTableProvider::new_for_planner(
        join.collection.clone(),
        join.client,
        join.schema,
        join.payload_schema,
    ));
    let builder = LogicalPlanBuilder::scan(join.collection, provider_as_source(provider), None)?;
    match filter {
        Some(filter) => builder.filter(filter)?.build().map(Some),
        None => builder.build().map(Some),
    }
}

fn except_filter(
    left: Option<datafusion::logical_expr::Expr>,
    right: Option<datafusion::logical_expr::Expr>,
) -> Option<datafusion::logical_expr::Expr> {
    match (left, right) {
        (_, None) => {
            Some(datafusion::logical_expr::Expr::Literal(ScalarValue::Boolean(Some(false)), None))
        }
        (None, Some(right)) => Some(datafusion::logical_expr::Expr::Not(Box::new(right))),
        (Some(left), Some(right)) => {
            conjunction([left, datafusion::logical_expr::Expr::Not(Box::new(right))])
        }
    }
}

fn redundant_raw_qdrant_distinct_plan(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) = plan else {
        return None;
    };
    qdrant_source(input.as_ref()).map(|_| input.as_ref().clone())
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::HashSet;
    use std::fmt;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Result, ToDFSchema};
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::{
        BinaryExpr, Expr, Extension, LogicalPlan, LogicalPlanBuilder, Operator,
        UserDefinedLogicalNodeCore,
    };
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::prelude::{col, lit};
    use qdrant_client::Qdrant;

    use super::{
        QdrantCompositionClass, QdrantKernelClass, QdrantRelationPushdown, QdrantSourceClass,
        QdrantSubtreeClass, QdrantTopologyClass, subtree_class,
    };
    use crate::arrow::schema::{ID_FIELD_NAME, PAYLOAD_FIELD_NAME};
    use crate::context::plan_node::{QDRANT_COUNT_NODE_NAME, QdrantCountNode};
    use crate::pushdown::QdrantFilters;
    use crate::table::QdrantTableProvider;

    #[derive(Debug, Clone, Hash, PartialEq, Eq)]
    struct DummyQdrantNode {
        name:   &'static str,
        schema: datafusion::common::DFSchemaRef,
    }

    impl PartialOrd for DummyQdrantNode {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.name.cmp(other.name)) }
    }

    impl UserDefinedLogicalNodeCore for DummyQdrantNode {
        fn name(&self) -> &str { self.name }

        fn inputs(&self) -> Vec<&LogicalPlan> { vec![] }

        fn schema(&self) -> &datafusion::common::DFSchemaRef { &self.schema }

        fn expressions(&self) -> Vec<Expr> { vec![] }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.name)
        }

        fn with_exprs_and_inputs(
            &self,
            exprs: Vec<Expr>,
            inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            assert!(exprs.is_empty(), "{} expects no expressions", self.name);
            assert!(inputs.is_empty(), "{} expects no inputs", self.name);
            Ok(self.clone())
        }

        fn prevent_predicate_push_down_columns(&self) -> HashSet<String> { HashSet::new() }
    }

    fn count_extension_plan() -> LogicalPlan {
        let schema = Schema::new(vec![Field::new("count", DataType::Int64, false)])
            .to_dfschema_ref()
            .expect("df schema");
        LogicalPlan::Extension(Extension {
            node: Arc::new(QdrantCountNode::new(
                schema,
                Arc::new(Qdrant::from_url("http://localhost:6334").build().expect("qdrant client")),
                "docs".to_owned(),
                QdrantFilters::default(),
            )),
        })
    }

    fn payload_extension_plan() -> LogicalPlan {
        let schema = Schema::new(vec![Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true)])
            .to_dfschema_ref()
            .expect("df schema");
        LogicalPlan::Extension(Extension {
            node: Arc::new(DummyQdrantNode { name: QDRANT_COUNT_NODE_NAME, schema }),
        })
    }

    fn payload_path(path: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col(PAYLOAD_FIELD_NAME)),
            Operator::Colon,
            Box::new(lit(path)),
        ))
    }

    fn raw_scan_plan(collection: &str) -> LogicalPlan {
        let provider = QdrantTableProvider::new_test(
            collection,
            Schema::new(vec![Field::new(ID_FIELD_NAME, DataType::Utf8, false)]),
            crate::pushdown::QdrantPayloadSchema::default(),
        );
        LogicalPlanBuilder::scan(collection, provider_as_source(Arc::new(provider)), None)
            .expect("scan")
            .build()
            .expect("scan plan")
    }

    fn raw_filtered_scan_plan(collection: &str, id: &str) -> LogicalPlan {
        LogicalPlanBuilder::from(raw_scan_plan(collection))
            .filter(col(ID_FIELD_NAME).eq(lit(id)))
            .expect("filter")
            .build()
            .expect("filtered scan plan")
    }

    #[test]
    fn subtree_classifies_qdrant_extension_as_atomic_leaf() {
        let class = subtree_class(&count_extension_plan()).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::Leaf,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::ExactSelf,
        });
    }

    #[test]
    fn subtree_classifies_union_of_qdrant_extensions_as_batchable_multibranch() {
        let left = count_extension_plan();
        let right = count_extension_plan();
        let union = LogicalPlanBuilder::from(left)
            .union(right)
            .expect("union")
            .build()
            .expect("union plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::Batchable,
            kernel:      QdrantKernelClass::ExactChildren,
        });
    }

    #[test]
    fn subtree_classifies_same_collection_raw_union_as_mergeable() {
        let left = raw_filtered_scan_plan("vectors", "row-1");
        let right = raw_filtered_scan_plan("vectors", "row-2");
        let union = LogicalPlanBuilder::from(left)
            .union(right)
            .expect("union")
            .build()
            .expect("union plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::Mergeable,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_overlapping_same_collection_raw_union_as_local_compose() {
        let left = raw_filtered_scan_plan("vectors", "row-1");
        let right = raw_filtered_scan_plan("vectors", "row-1");
        let union = LogicalPlanBuilder::from(left)
            .union(right)
            .expect("union")
            .build()
            .expect("union plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::LocalCompose,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_cross_collection_raw_union_as_local_compose() {
        let left = raw_filtered_scan_plan("vectors_a", "row-1");
        let right = raw_filtered_scan_plan("vectors_b", "row-2");
        let union = LogicalPlanBuilder::from(left)
            .union(right)
            .expect("union")
            .build()
            .expect("union plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::LocalCompose,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_same_collection_raw_union_distinct_as_mergeable() {
        let union = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors", "row-1"))
            .union_distinct(raw_filtered_scan_plan("vectors", "row-1"))
            .expect("union distinct")
            .build()
            .expect("union distinct plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::UnaryRelationChange,
            composition: QdrantCompositionClass::Mergeable,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_cross_collection_raw_union_distinct_as_coordinated() {
        let union = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors_a", "row-1"))
            .union_distinct(raw_filtered_scan_plan("vectors_b", "row-1"))
            .expect("union distinct")
            .build()
            .expect("union distinct plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::UnaryRelationChange,
            composition: QdrantCompositionClass::Coordinated,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_same_collection_raw_intersect_as_mergeable() {
        let intersect = LogicalPlanBuilder::intersect(
            raw_filtered_scan_plan("vectors", "row-1"),
            raw_filtered_scan_plan("vectors", "row-1"),
            false,
        )
        .expect("intersect");
        let class = subtree_class(&intersect).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::Mergeable,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_same_collection_raw_except_as_mergeable() {
        let except = LogicalPlanBuilder::except(
            raw_filtered_scan_plan("vectors", "row-1"),
            raw_filtered_scan_plan("vectors", "row-2"),
            false,
        )
        .expect("except");
        let class = subtree_class(&except).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::Mergeable,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_non_qdrant_projection_as_local_compose_unary_chain() {
        let projection = LogicalPlanBuilder::values(vec![vec![lit(1_i64)]])
            .expect("values")
            .project(vec![col("column1")])
            .expect("projection")
            .build()
            .expect("projection plan");
        let class = subtree_class(&projection).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::None,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::LocalCompose,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_projection_over_qdrant_kernel_as_local_shell() {
        let projection = LogicalPlanBuilder::from(count_extension_plan())
            .project(vec![col("count")])
            .expect("projection")
            .build()
            .expect("projection plan");
        let class = subtree_class(&projection).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::LocalCompose,
            kernel:      QdrantKernelClass::ExactChild,
        });
    }

    #[test]
    fn subtree_classifies_payload_projection_over_qdrant_source_as_invalid() {
        let projection = LogicalPlanBuilder::from(payload_extension_plan())
            .project(vec![payload_path("tag")])
            .expect("projection")
            .build()
            .expect("projection plan");
        let class = subtree_class(&projection).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Invalid,
            kernel:      QdrantKernelClass::ExactChild,
        });
    }

    #[test]
    fn analyzer_rejects_invalid_qdrant_payload_projection() {
        let projection = LogicalPlanBuilder::from(payload_extension_plan())
            .project(vec![payload_path("tag")])
            .expect("projection")
            .build()
            .expect("projection plan");
        let error = QdrantRelationPushdown
            .analyze(projection, &ConfigOptions::default())
            .expect_err("invalid payload access should error");
        assert!(
            error.to_string().contains("unsupported qdrant payload access outside admitted kernel"),
            "{error}"
        );
    }

    #[test]
    fn analyzer_rewrites_mergeable_raw_union_to_single_filtered_scan() {
        let union = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors", "row-1"))
            .union(raw_filtered_scan_plan("vectors", "row-2"))
            .expect("union")
            .build()
            .expect("union plan");
        let analyzed = QdrantRelationPushdown
            .analyze(union, &ConfigOptions::default())
            .expect("analyzed plan");

        assert!(matches!(analyzed, LogicalPlan::Filter(_)));
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn analyzer_rewrites_mergeable_raw_union_distinct_to_single_filtered_scan() {
        let union = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors", "row-1"))
            .union_distinct(raw_filtered_scan_plan("vectors", "row-1"))
            .expect("union distinct")
            .build()
            .expect("union distinct plan");
        let analyzed = QdrantRelationPushdown
            .analyze(union, &ConfigOptions::default())
            .expect("analyzed plan");

        assert!(matches!(analyzed, LogicalPlan::Filter(_)));
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn analyzer_rewrites_raw_intersect_to_single_filtered_scan() {
        let intersect = LogicalPlanBuilder::intersect(
            raw_filtered_scan_plan("vectors", "row-1"),
            raw_filtered_scan_plan("vectors", "row-1"),
            true,
        )
        .expect("intersect");
        let analyzed = QdrantRelationPushdown
            .analyze(intersect, &ConfigOptions::default())
            .expect("analyzed plan");

        assert!(matches!(analyzed, LogicalPlan::Filter(_)));
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn analyzer_rewrites_raw_except_to_single_filtered_scan() {
        let except = LogicalPlanBuilder::except(
            raw_filtered_scan_plan("vectors", "row-1"),
            raw_filtered_scan_plan("vectors", "row-2"),
            true,
        )
        .expect("except");
        let analyzed = QdrantRelationPushdown
            .analyze(except, &ConfigOptions::default())
            .expect("analyzed plan");

        assert!(matches!(analyzed, LogicalPlan::Filter(_)));
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn analyzer_drops_redundant_raw_qdrant_distinct() {
        let distinct = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors", "row-1"))
            .distinct()
            .expect("distinct")
            .build()
            .expect("distinct plan");
        let analyzed = QdrantRelationPushdown
            .analyze(distinct, &ConfigOptions::default())
            .expect("analyzed plan");

        assert!(matches!(analyzed, LogicalPlan::Filter(_)));
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::None,
        });
    }
}
