use datafusion::common::{Result, plan_err};
use qdrant_client::qdrant::{Direction, OrderBy, Query, query};

use super::super::source::Source;
use super::{QueryDescriptor, bool_literal, string_literal};
use crate::expr_fn::{ORDER_BY_SCORE_FUNCTION_NAME, OrderByCall};
use crate::qdrant::QdrantPayloadPath;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderByQuery {
    key: String,
    descending: bool,
}

impl TryFrom<OrderByCall> for OrderByQuery {
    type Error = datafusion::error::DataFusionError;

    fn try_from(call: OrderByCall) -> Result<Self> {
        let Some(path) = QdrantPayloadPath::from_logical_expr(&call.path) else {
            return plan_err!("{ORDER_BY_SCORE_FUNCTION_NAME} requires a qdrant payload path");
        };
        let descending = if let Some(arg) = call.direction.as_ref() {
            match arg.clone().unalias_nested().data {
                datafusion::logical_expr::Expr::Literal(
                    datafusion::common::ScalarValue::Boolean(Some(_)),
                    _,
                )
                | datafusion::logical_expr::Expr::Cast(_)
                | datafusion::logical_expr::Expr::TryCast(_) => {
                    bool_literal(arg, ORDER_BY_SCORE_FUNCTION_NAME, "direction")?
                }
                _ => match string_literal(arg, ORDER_BY_SCORE_FUNCTION_NAME, "direction")?
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "ASC" => false,
                    "DESC" => true,
                    _ => {
                        return plan_err!(
                            "{ORDER_BY_SCORE_FUNCTION_NAME} direction must be boolean, 'asc', or                              'desc'"
                        );
                    }
                },
            }
        } else {
            false
        };
        Ok(Self { key: path.key().to_owned(), descending })
    }
}

impl OrderByQuery {
    pub(crate) fn same_semantics(&self, other: &Self) -> bool {
        self == other
    }

    pub(super) fn validate_on_source(&self, source: &Source) -> Result<()> {
        if source.payload_schema.field(&self.key).is_none() {
            return plan_err!("qdrant order-by field '{}' not found in payload schema", self.key);
        }
        Ok(())
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> QueryDescriptor {
        QueryDescriptor::new(
            Query {
                variant: Some(query::Variant::OrderBy(OrderBy {
                    key: self.key.clone(),
                    direction: Some(
                        if self.descending { Direction::Desc } else { Direction::Asc } as i32
                    ),
                    start_from: None,
                })),
            },
            None,
        )
    }
}
