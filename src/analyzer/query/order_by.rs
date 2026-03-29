use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::{Direction, OrderBy, Query, query};

use super::super::source::Source;
use super::{QueryDescriptor, bool_literal, function_args, string_literal};
use crate::expr_fn::QDRANT_ORDER_BY_SCORE_FUNCTION_NAME;
use crate::pushdown::QdrantPayloadPath;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderByQuery {
    key:        String,
    descending: bool,
}

impl OrderByQuery {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, QDRANT_ORDER_BY_SCORE_FUNCTION_NAME) else {
            return Ok(None);
        };
        if !(1..=2).contains(&args.len()) {
            return plan_err!(
                "{QDRANT_ORDER_BY_SCORE_FUNCTION_NAME} requires a payload path and an optional \
                 direction"
            );
        }
        let Some(path) = QdrantPayloadPath::from_logical_expr(&args[0]) else {
            return plan_err!(
                "{QDRANT_ORDER_BY_SCORE_FUNCTION_NAME} requires a qdrant payload path"
            );
        };
        let descending = if let Some(arg) = args.get(1) {
            match arg.clone().unalias_nested().data {
                Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(_)), _)
                | Expr::Cast(_)
                | Expr::TryCast(_) => {
                    bool_literal(arg, QDRANT_ORDER_BY_SCORE_FUNCTION_NAME, "direction")?
                }
                _ => match string_literal(arg, QDRANT_ORDER_BY_SCORE_FUNCTION_NAME, "direction")?
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "ASC" => false,
                    "DESC" => true,
                    _ => {
                        return plan_err!(
                            "{QDRANT_ORDER_BY_SCORE_FUNCTION_NAME} direction must be boolean, \
                             'asc', or 'desc'"
                        );
                    }
                },
            }
        } else {
            false
        };
        Ok(Some(Self { key: path.key().to_owned(), descending }))
    }

    pub(crate) fn same_semantics(&self, other: &Self) -> bool { self == other }

    pub(super) fn validate_on_source(&self, source: &Source) -> Result<()> {
        if source.payload_schema.field(&self.key).is_none() {
            return plan_err!("qdrant order-by field '{}' not found in payload schema", self.key);
        }
        Ok(())
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> Result<QueryDescriptor> {
        Ok(QueryDescriptor::new(
            Query {
                variant: Some(query::Variant::OrderBy(OrderBy {
                    key:        self.key.clone(),
                    direction:  Some(
                        if self.descending { Direction::Desc } else { Direction::Asc } as i32,
                    ),
                    start_from: None,
                })),
            },
            None,
        ))
    }
}
