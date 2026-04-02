use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::{NullEquality, Result, ScalarValue};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::utils::{conjunction, disjunction};
use datafusion::logical_expr::{Distinct, Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use qdrant_client::Qdrant;
use qdrant_client::qdrant::PointId;
use qdrant_client::qdrant::point_id::PointIdOptions;

use super::state::State;
use crate::arrow::schema::QdrantFieldBinding;
use crate::pushdown::QdrantPayloadSchema;
use crate::table::QdrantTableProvider;

pub(crate) fn full_row_join_keys(join: &datafusion::logical_expr::logical_plan::Join) -> bool {
    let left_fields = join.left.schema().fields();
    let right_fields = join.right.schema().fields();
    join.on.len() == left_fields.len()
        && left_fields.len() == right_fields.len()
        && join.on.iter().zip(left_fields.iter().zip(right_fields.iter())).all(
            |((left, right), (left_field, right_field))| match (left, right) {
                (Expr::Column(left), Expr::Column(right)) => {
                    left.name == *left_field.name() && right.name == *right_field.name()
                }
                _ => false,
            },
        )
}

#[derive(Clone)]
pub(crate) struct Source {
    pub(super) client: Arc<Qdrant>,
    pub(super) collection: String,
    pub(super) schema: SchemaRef,
    pub(super) payload_schema: Arc<QdrantPayloadSchema>,
}

impl std::fmt::Debug for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Source")
            .field("client", &"Qdrant")
            .field("collection", &self.collection)
            .field("schema", &self.schema)
            .field("payload_schema", &self.payload_schema)
            .finish()
    }
}

impl Source {
    pub(super) fn client(&self) -> &Arc<Qdrant> {
        &self.client
    }

    pub(super) fn collection(&self) -> &str {
        &self.collection
    }

    pub(super) fn merge_compatible_with(&self, other: &Self) -> bool {
        self.collection == other.collection
            && Arc::ptr_eq(&self.client, &other.client)
            && format!("{:?}", self.schema) == format!("{:?}", other.schema)
            && format!("{:?}", self.payload_schema) == format!("{:?}", other.payload_schema)
    }

    pub(super) fn planner_scan(&self, filter: Option<Expr>) -> Result<LogicalPlan> {
        let provider = Arc::new(QdrantTableProvider::new_for_planner(
            self.collection.clone(),
            Arc::clone(&self.client),
            Arc::clone(&self.schema),
            Arc::clone(&self.payload_schema),
        ));
        let builder =
            LogicalPlanBuilder::scan(self.collection.clone(), provider_as_source(provider), None)?;
        match filter {
            Some(filter) => builder.filter(filter)?.build(),
            None => builder.build(),
        }
    }

    pub(crate) fn field_binding(&self, using: &str) -> Result<QdrantFieldBinding> {
        let Ok(field) = self.schema.field_with_name(using) else {
            return datafusion::common::plan_err!("query vector field '{using}' not found");
        };
        Ok(QdrantFieldBinding::from_field(field))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MergeableUnion {
    pub(super) source: Source,
    pub(super) branches: Vec<Option<Expr>>,
    pub(super) branch_ids: Vec<Option<Vec<PointId>>>,
}

impl MergeableUnion {
    pub(super) fn from_plan(plan: &LogicalPlan, children: &[State]) -> Result<Option<Self>> {
        let LogicalPlan::Union(_) = plan else {
            return Ok(None);
        };
        let mut branches = children
            .iter()
            .map(MergeableBranch::from_state)
            .collect::<Result<Vec<_>>>()?
            .into_iter();
        let Some(first) = branches.next() else {
            return Ok(None);
        };
        let Some(first) = first else {
            return Ok(None);
        };
        let source = first.source.clone();
        let mut filters = vec![first.filter];
        let mut ids = vec![first.ids];
        for branch in branches {
            let Some(branch) = branch else {
                return Ok(None);
            };
            if !source.merge_compatible_with(&branch.source) {
                return Ok(None);
            }
            filters.push(branch.filter);
            ids.push(branch.ids);
        }
        Ok(Some(Self { source, branches: filters, branch_ids: ids }))
    }
}

impl MergeableUnion {
    pub(super) fn rewrite_current(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let merged = if matches!(plan, LogicalPlan::Distinct(Distinct::All(_))) {
            self.merged_plan(false)?
        } else if self.can_union_all_merge() {
            self.merged_plan(true)?
        } else {
            None
        };
        let Some(merged) = merged else {
            return Ok(None);
        };
        match plan {
            LogicalPlan::Union(_) | LogicalPlan::Distinct(Distinct::All(_)) => Ok(Some(merged)),
            _ if plan.inputs().len() == 1 => {
                plan.with_new_exprs(plan.expressions(), vec![merged])?.recompute_schema().map(Some)
            }
            _ => Ok(None),
        }
    }

    pub(super) fn can_union_all_merge(&self) -> bool {
        self.branches.iter().all(Option::is_some) && self.point_id_sets_are_disjoint()
    }

    fn merged_plan(&self, require_disjoint: bool) -> Result<Option<LogicalPlan>> {
        if require_disjoint && !self.can_union_all_merge() {
            return Ok(None);
        }
        self.source.planner_scan(self.merged_filter()).map(Some)
    }

    fn merged_filter(&self) -> Option<Expr> {
        (!self.branches.iter().any(Option::is_none))
            .then(|| disjunction(self.branches.iter().flatten().cloned()))
            .flatten()
    }

    fn point_id_sets_are_disjoint(&self) -> bool {
        let Some(id_sets) = self.branch_ids.iter().cloned().collect::<Option<Vec<Vec<PointId>>>>()
        else {
            return false;
        };
        let mut seen = HashSet::new();
        id_sets.into_iter().all(|ids| {
            ids.into_iter()
                .map(|id| match id.point_id_options.as_ref() {
                    Some(PointIdOptions::Num(value)) => format!("num:{value}"),
                    Some(PointIdOptions::Uuid(value)) => format!("uuid:{value}"),
                    None => "missing".to_owned(),
                })
                .collect::<HashSet<_>>()
                .into_iter()
                .all(|key| seen.insert(key))
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MergeableSetJoin {
    pub(super) source: Source,
    pub(super) left_filter: Option<Expr>,
    pub(super) right_filter: Option<Expr>,
    pub(super) join_type: JoinType,
}

impl MergeableSetJoin {
    pub(super) fn from_plan(plan: &LogicalPlan, children: &[State]) -> Result<Option<Self>> {
        let LogicalPlan::Join(join) = plan else {
            return Ok(None);
        };
        if join.filter.is_some()
            || join.null_aware
            || join.null_equality != NullEquality::NullEqualsNull
            || !matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
            || !full_row_join_keys(join)
            || children.len() != 2
        {
            return Ok(None);
        }
        let Some(left) = MergeableBranch::from_state(&children[0])? else {
            return Ok(None);
        };
        let Some(right) = MergeableBranch::from_state(&children[1])? else {
            return Ok(None);
        };
        if !left.source.merge_compatible_with(&right.source) {
            return Ok(None);
        }
        Ok(Some(Self {
            source: left.source,
            left_filter: left.filter,
            right_filter: right.filter,
            join_type: join.join_type,
        }))
    }
}

impl MergeableSetJoin {
    pub(super) fn rewrite_current(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let merged = self.source.planner_scan(self.merged_filter())?;
        match plan {
            LogicalPlan::Join(_) | LogicalPlan::Distinct(Distinct::All(_)) => Ok(Some(merged)),
            _ if plan.inputs().len() == 1 => {
                plan.with_new_exprs(plan.expressions(), vec![merged])?.recompute_schema().map(Some)
            }
            _ => Ok(None),
        }
    }

    fn merged_filter(&self) -> Option<Expr> {
        match self.join_type {
            JoinType::LeftSemi => conjunction(
                [self.left_filter.clone(), self.right_filter.clone()].into_iter().flatten(),
            ),
            JoinType::LeftAnti => match (&self.left_filter, &self.right_filter) {
                (_, None) => Some(Expr::Literal(ScalarValue::Boolean(Some(false)), None)),
                (None, Some(right)) => Some(Expr::Not(Box::new(right.clone()))),
                (Some(left), Some(right)) => {
                    conjunction([left.clone(), Expr::Not(Box::new(right.clone()))])
                }
            },
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MergeableBranch {
    pub(super) source: Source,
    pub(super) filter: Option<Expr>,
    pub(super) ids: Option<Vec<PointId>>,
}

impl MergeableBranch {
    pub(super) fn from_state(state: &State) -> Result<Option<Self>> {
        let State::Source(source_state) = state else {
            return Ok(None);
        };
        let ids = source_state.filters.exact(&source_state.source)?.possible_point_ids();
        Ok(Some(Self {
            source: source_state.source.clone(),
            filter: source_state.filters.combined_expr(),
            ids,
        }))
    }
}
