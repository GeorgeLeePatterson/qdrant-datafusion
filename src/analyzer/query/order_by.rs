use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::{Direction, OrderBy, Query, query};

use super::super::source::Source;
use super::{QueryDescriptor, bool_literal, string_literal};
use crate::expr_fn::{ORDER_BY_SCORE_FUNCTION_NAME, OrderByCall};
use crate::qdrant::QdrantPayloadPath;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderByQuery {
    key:        String,
    cast_type:  Option<DataType>,
    descending: bool,
}

impl TryFrom<OrderByCall> for OrderByQuery {
    type Error = datafusion::error::DataFusionError;

    fn try_from(call: OrderByCall) -> Result<Self> {
        let Some((path, cast_type)) = ordering_path_expr(&call.path) else {
            return plan_err!("{ORDER_BY_SCORE_FUNCTION_NAME} requires a qdrant payload path");
        };
        let descending = if let Some(arg) = call.direction.as_ref() {
            match arg.clone().unalias_nested().data {
                Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(_)), _)
                | Expr::Cast(_)
                | Expr::TryCast(_) => bool_literal(arg, ORDER_BY_SCORE_FUNCTION_NAME, "direction")?,
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
        Ok(Self { key: path.key().to_owned(), cast_type, descending })
    }
}

fn ordering_path_expr(expr: &Expr) -> Option<(QdrantPayloadPath, Option<DataType>)> {
    match expr {
        Expr::Alias(alias) => ordering_path_expr(&alias.expr),
        Expr::Cast(cast) => {
            let (path, _) = ordering_path_expr(&cast.expr)?;
            Some((path, Some(cast.field.data_type().clone())))
        }
        Expr::TryCast(cast) => {
            let (path, _) = ordering_path_expr(&cast.expr)?;
            Some((path, Some(cast.field.data_type().clone())))
        }
        _ => QdrantPayloadPath::from_logical_expr(expr).map(|path| (path, None)),
    }
}

impl OrderByQuery {
    pub(crate) fn same_semantics(&self, other: &Self) -> bool {
        self.key == other.key && self.descending == other.descending
    }

    pub(super) fn validate_on_source(&self, source: &Source) -> Result<()> {
        let Some(field) = source.payload_schema.field_for_path(&self.key) else {
            return plan_err!(
                "qdrant order-by field '{}' is not an indexed orderable payload field",
                self.key
            );
        };
        if let Some(data_type) = self.cast_type.as_ref()
            && !field.supports_order_preserving_payload_cast(data_type)
        {
            return plan_err!(
                "qdrant order-by field '{}' does not preserve ordering under cast to {:?}",
                self.key,
                data_type
            );
        }
        if source.payload_schema.ordering_for(&self.key, self.descending).is_none() {
            return plan_err!(
                "qdrant order-by field '{}' is not an indexed orderable payload field",
                self.key
            );
        }
        Ok(())
    }

    pub(super) fn descriptor(&self, _prefetch_count: usize) -> QueryDescriptor {
        QueryDescriptor::new(
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
        )
    }
}
