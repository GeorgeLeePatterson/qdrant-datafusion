use datafusion::common::Result;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::LogicalPlan;

use super::{
    QDRANT_COUNT_NODE_NAME, QDRANT_FACET_NODE_NAME, QdrantCompositionClass, QdrantKernelClass,
    QdrantSourceClass, QdrantSubtreeClass, QdrantSubtreeStatus, QdrantTableProvider,
    QdrantTopologyClass,
};
use crate::pushdown::QdrantPayloadPath;

impl QdrantSubtreeStatus {
    pub(super) fn of(plan: &LogicalPlan) -> Result<Self> {
        let candidate = super::QdrantRelationCandidate::from_plan(plan)?;
        let source = QdrantSourceClass::of(plan);
        let topology = QdrantTopologyClass::of(plan);
        let kernel = QdrantKernelClass::of(plan, candidate.is_some());
        let composition = QdrantCompositionClass::of(plan, source, topology, kernel)?;
        let class = QdrantSubtreeClass { source, topology, composition, kernel };
        let candidate = if class.source == QdrantSourceClass::SingleQdrant
            && class.composition == QdrantCompositionClass::Atomic
        {
            candidate
        } else {
            None
        };
        Ok(Self { class, candidate })
    }
}

impl QdrantSourceClass {
    pub(super) fn of(plan: &LogicalPlan) -> Self {
        match plan {
            LogicalPlan::TableScan(scan) => {
                let Ok(provider) = source_as_provider(&scan.source) else {
                    return Self::None;
                };
                if provider.as_any().is::<QdrantTableProvider>() {
                    Self::SingleQdrant
                } else {
                    Self::None
                }
            }
            LogicalPlan::Extension(extension)
                if matches!(
                    extension.node.name(),
                    QDRANT_COUNT_NODE_NAME | QDRANT_FACET_NODE_NAME
                ) =>
            {
                Self::SingleQdrant
            }
            _ => Self::combine(plan.inputs().into_iter().map(Self::of)),
        }
    }

    fn combine(sources: impl IntoIterator<Item = Self>) -> Self {
        let mut qdrant_count = 0_u8;
        let mut has_other = false;
        for source in sources {
            match source {
                Self::None => {}
                Self::SingleQdrant => qdrant_count = qdrant_count.saturating_add(1),
                Self::MultiQdrant => qdrant_count = qdrant_count.saturating_add(2),
                Self::Mixed => {
                    qdrant_count = qdrant_count.saturating_add(1);
                    has_other = true;
                }
            }
        }
        match (qdrant_count, has_other) {
            (0, false) => Self::None,
            (1, false) => Self::SingleQdrant,
            (_, false) => Self::MultiQdrant,
            _ => Self::Mixed,
        }
    }
}

impl QdrantTopologyClass {
    pub(super) fn of(plan: &LogicalPlan) -> Self {
        match plan {
            LogicalPlan::TableScan(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Extension(_) => Self::Leaf,
            LogicalPlan::Aggregate(_) | LogicalPlan::Distinct(_) => Self::UnaryRelationChange,
            _ if plan.inputs().len() > 1 => Self::MultiBranch,
            _ => Self::UnaryChain,
        }
    }
}

impl QdrantKernelClass {
    pub(super) fn of(plan: &LogicalPlan, exact_self: bool) -> Self {
        if exact_self
            || matches!(
                plan,
                LogicalPlan::Extension(extension)
                    if matches!(extension.node.name(), QDRANT_COUNT_NODE_NAME | QDRANT_FACET_NODE_NAME)
            )
        {
            return Self::ExactSelf;
        }
        Self::combine(plan.inputs().into_iter().map(|child| Self::of(child, false)))
    }

    fn combine(kernels: impl IntoIterator<Item = Self>) -> Self {
        let kernel_count = kernels
            .into_iter()
            .map(|kernel| match kernel {
                Self::None => 0_u8,
                Self::ExactSelf | Self::ExactChild => 1_u8,
                Self::ExactChildren => 2_u8,
            })
            .fold(0_u8, u8::saturating_add);
        match kernel_count {
            0 => Self::None,
            1 => Self::ExactChild,
            _ => Self::ExactChildren,
        }
    }
}

impl QdrantCompositionClass {
    pub(super) fn of(
        plan: &LogicalPlan,
        source: QdrantSourceClass,
        topology: QdrantTopologyClass,
        kernel: QdrantKernelClass,
    ) -> Result<Self> {
        if invalid_payload_access_surface(plan, source, kernel)? {
            return Ok(Self::Invalid);
        }
        if kernel == QdrantKernelClass::ExactSelf {
            return Ok(Self::Atomic);
        }
        if super::RawQdrantSetJoin::from_plan(plan).is_some() {
            return Ok(Self::Mergeable);
        }
        if super::RawQdrantUnion::from_distinct_plan(plan)?.is_some() {
            return Ok(Self::Mergeable);
        }
        if matches!(plan, LogicalPlan::Union(_)) {
            return Ok(match kernel {
                QdrantKernelClass::ExactChild | QdrantKernelClass::ExactChildren => Self::Batchable,
                QdrantKernelClass::None
                    if super::RawQdrantUnion::from_plan(plan)?
                        .is_some_and(|union| union.can_union_all_merge()) =>
                {
                    Self::Mergeable
                }
                _ => Self::LocalCompose,
            });
        }
        Ok(match (source, topology, kernel) {
            (
                QdrantSourceClass::SingleQdrant,
                QdrantTopologyClass::Leaf
                | QdrantTopologyClass::UnaryChain
                | QdrantTopologyClass::UnaryRelationChange,
                QdrantKernelClass::None,
            ) => Self::Atomic,
            (
                QdrantSourceClass::MultiQdrant,
                QdrantTopologyClass::Leaf
                | QdrantTopologyClass::UnaryChain
                | QdrantTopologyClass::UnaryRelationChange,
                QdrantKernelClass::None,
            ) => Self::Coordinated,
            (QdrantSourceClass::SingleQdrant | QdrantSourceClass::MultiQdrant, _, _) => {
                Self::LocalCompose
            }
            _ => Self::LocalCompose,
        })
    }
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
    let mut found = false;
    let _ = plan.apply_expressions(|expr| {
        if expr.exists(|expr| Ok(QdrantPayloadPath::from_logical_expr(expr).is_some()))? {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(found)
}
