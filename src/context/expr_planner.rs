use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::sql::sqlparser::ast::BinaryOperator;

use crate::expr_fn::payload_access_expr;
use crate::qdrant::{QdrantPayloadAccess, QdrantPayloadSchema};

#[derive(Debug)]
pub(crate) struct QdrantPayloadExprPlanner;

impl ExprPlanner for QdrantPayloadExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        let BinaryOperator::Custom(operator) = &expr.op else {
            return Ok(PlannerResult::Original(expr));
        };
        if operator != ":" {
            return Ok(PlannerResult::Original(expr));
        }

        let Some(access) = QdrantPayloadAccess::from_logical_parts(&expr.left, &expr.right) else {
            return Ok(PlannerResult::Original(expr));
        };
        let Some(data_type) = QdrantPayloadSchema::projection_data_type_from_metadata(
            schema.metadata(),
            access.path().key(),
        ) else {
            return Ok(PlannerResult::Original(expr));
        };
        let (payload, path) = access.into_parts();
        let Some(rewritten_expr) = payload_access_expr(payload, path, &data_type) else {
            return Ok(PlannerResult::Original(expr));
        };
        Ok(PlannerResult::Planned(rewritten_expr))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Column, DFSchema};
    use datafusion::logical_expr::planner::{PlannerResult, RawBinaryExpr};
    use datafusion::logical_expr::{Expr, ExprSchemable};
    use datafusion::prelude::lit;
    use datafusion::sql::sqlparser::ast::BinaryOperator;
    use qdrant_client::qdrant::{PayloadSchemaInfo, PayloadSchemaType};

    use super::*;
    use crate::arrow::schema::{PAYLOAD_FIELD_NAME, schema_with_payload_projection_metadata};

    fn planner_schema() -> DFSchema {
        let payload_schema =
            QdrantPayloadSchema::from(HashMap::from([("rank".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Integer as i32,
                params:    None,
                points:    None,
            })]));
        let arrow_schema = schema_with_payload_projection_metadata(
            &Schema::new(vec![Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true)]),
            &payload_schema,
        );
        DFSchema::try_from_qualified_schema("vectors", &arrow_schema).expect("df schema")
    }

    #[test]
    fn rewrites_known_payload_path_to_typed_internal_accessor() {
        let planner = QdrantPayloadExprPlanner;
        let schema = planner_schema();
        let result = planner
            .plan_binary_op(
                RawBinaryExpr {
                    op:    BinaryOperator::Custom(":".to_owned()),
                    left:  Expr::Column(Column::new_unqualified(PAYLOAD_FIELD_NAME)),
                    right: lit("rank"),
                },
                &schema,
            )
            .expect("planner result");

        let PlannerResult::Planned(expr) = result else {
            panic!("expected typed payload rewrite");
        };
        assert_eq!(expr.get_type(&schema).expect("expr type"), DataType::Int64);
        assert!(expr.to_string().contains("__qdrant_payload_int_access"), "{expr}");
    }

    #[test]
    fn leaves_unknown_payload_paths_to_default_planning() {
        let planner = QdrantPayloadExprPlanner;
        let schema = planner_schema();
        let result = planner
            .plan_binary_op(
                RawBinaryExpr {
                    op:    BinaryOperator::Custom(":".to_owned()),
                    left:  Expr::Column(Column::new_unqualified(PAYLOAD_FIELD_NAME)),
                    right: lit("unknown"),
                },
                &schema,
            )
            .expect("planner result");

        assert!(matches!(result, PlannerResult::Original(_)));
    }
}
