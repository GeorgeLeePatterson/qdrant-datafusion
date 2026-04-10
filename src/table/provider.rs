use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, SchemaExt, plan_err};
use datafusion::datasource::TableType;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use super::delete::QdrantDeleteExec;
use super::insert::QdrantInsertSink;
use super::update::QdrantUpdateExec;
use super::{QdrantScanExec, QdrantScanSpec, QdrantTableProvider};
use crate::arrow::schema::ID_FIELD_NAME;
use crate::qdrant::filter::QdrantFilters;

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for QdrantTableProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn schema(&self) -> SchemaRef { self.planning_schema() }

    fn table_type(&self) -> TableType { TableType::Base }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if QdrantFilters::supports_exact(&self.schema, &self.payload_schema, filter) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let schema = self.planning_schema();
        let pushdown = Arc::new(QdrantScanSpec::try_new(
            &schema,
            &self.payload_schema,
            projection,
            filters,
            limit,
        )?);
        Ok(Arc::new(QdrantScanExec::new(
            Arc::clone(&self.client),
            self.table.table().to_string(),
            pushdown,
            Arc::clone(&self.payload_schema),
            self.ordered_scroll_contract,
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.schema().logically_equivalent_names_and_types(&input.schema())?;
        let sink = QdrantInsertSink::new(
            Arc::clone(&self.client),
            self.table.table().to_owned(),
            self.planning_schema(),
            insert_op,
        );
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let filters = QdrantFilters::try_new(&self.schema, &self.payload_schema, &filters)?;
        Ok(Arc::new(QdrantDeleteExec::new(
            Arc::clone(&self.client),
            self.table.table().to_owned(),
            filters,
        )))
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let available_columns: Vec<&str> =
            self.schema.fields().iter().map(|field| field.name().as_str()).collect();
        let planning_schema = self.planning_schema();
        let df_schema = DFSchema::try_from(Arc::clone(&planning_schema))?;

        let mut physical_assignments = Vec::with_capacity(assignments.len());
        for (column_name, expr) in assignments {
            if self.schema.field_with_name(&column_name).is_err() {
                return plan_err!(
                    "UPDATE failed: column '{}' does not exist. Available columns: {}",
                    column_name,
                    available_columns.join(", ")
                );
            }
            if column_name == ID_FIELD_NAME {
                return plan_err!(
                    "UPDATE failed: updating '{}' is not supported on the current qdrant \
                     row-rewrite contract",
                    ID_FIELD_NAME
                );
            }
            physical_assignments.push((column_name, state.create_physical_expr(expr, &df_schema)?));
        }

        let (exact_filters, residual_filters): (Vec<_>, Vec<_>) =
            filters.into_iter().partition(|filter| {
                QdrantFilters::supports_exact(&self.schema, &self.payload_schema, filter)
            });
        let exact_filters =
            QdrantFilters::try_new(&self.schema, &self.payload_schema, &exact_filters)?;
        let residual_filters = residual_filters
            .into_iter()
            .map(|expr| state.create_physical_expr(expr, &df_schema))
            .collect::<DataFusionResult<Vec<Arc<dyn PhysicalExpr>>>>()?;

        Ok(Arc::new(QdrantUpdateExec::new(
            Arc::clone(&self.client),
            self.table.table().to_owned(),
            planning_schema,
            exact_filters,
            physical_assignments,
            residual_filters,
        )))
    }
}
