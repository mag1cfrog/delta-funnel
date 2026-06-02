//! DataFusion table provider for one Delta source.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{
    Column, DataFusionError, Result as DataFusionResult,
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};

use crate::{
    DeltaFunnelError, DeltaProtocolReport, PlannedDeltaSource, ProtocolPreflight,
    table_formats::{
        ProjectedDeltaScan, build_projected_predicated_delta_scan, delta_source_arrow_schema,
    },
};

use super::execution::DeltaScanPlanningExec;
use super::filters::DeltaFilterPushdownPlan;
use super::projection::{ProjectionPlan, plan_projection};
use super::registration::reject_mismatched_preflight;
use super::scan_plan::{ProviderScanPlan, ProviderScanPlanParts, ProviderScanPlanRequest};

pub(crate) struct DeltaTableProvider {
    source: PlannedDeltaSource,
    protocol: DeltaProtocolReport,
    schema: SchemaRef,
}

impl DeltaTableProvider {
    pub(crate) fn try_new(
        source: PlannedDeltaSource,
        preflight: ProtocolPreflight,
    ) -> Result<Self, DeltaFunnelError> {
        reject_mismatched_preflight(&source, preflight.protocol())?;
        let schema = delta_source_arrow_schema(&source).map_err(|reason| {
            DeltaFunnelError::DeltaSourceSchema {
                source_name: source.name().to_owned(),
                table_uri: source.table_uri().to_owned(),
                reason,
            }
        })?;

        Ok(Self {
            source,
            protocol: preflight.into_protocol(),
            schema,
        })
    }

    pub(crate) fn source_name(&self) -> &str {
        self.source.name()
    }

    pub(crate) fn snapshot_version(&self) -> u64 {
        self.source.version()
    }

    pub(crate) fn protocol(&self) -> &DeltaProtocolReport {
        &self.protocol
    }

    pub(crate) fn source_table_uri(&self) -> &str {
        self.source.table_uri()
    }

    fn partition_columns(&self) -> HashSet<String> {
        self.source
            .loaded_snapshot()
            .kernel_snapshot()
            .table_configuration()
            .metadata()
            .partition_columns()
            .iter()
            .cloned()
            .collect()
    }

    #[allow(dead_code)]
    pub(crate) fn plan_filters(&self, filters: &[&Expr]) -> DeltaFilterPushdownPlan {
        let unqualified_filters = filters
            .iter()
            .map(|filter| unqualify_filter_columns((*filter).clone(), &self.schema))
            .collect::<Vec<_>>();
        let unqualified_filter_refs = unqualified_filters.iter().collect::<Vec<_>>();

        DeltaFilterPushdownPlan::partition_equality_pushdown(
            &unqualified_filter_refs,
            &self.schema,
            &self.partition_columns(),
        )
    }

    #[allow(dead_code)]
    pub(crate) fn plan_scan(
        &self,
        request: ProviderScanPlanRequest,
    ) -> Result<ProviderScanPlan, DeltaFunnelError> {
        let ProjectionPlan {
            projected_schema,
            scan_projection,
            projected_column_names,
        } = self.plan_projection(request.requested_projection)?;
        let pushed_filter_plan = self.plan_pushed_filters(&request.pushed_filters);
        self.reject_unaccepted_pushed_filters(&pushed_filter_plan)?;
        let kernel_scan =
            self.build_kernel_scan(projected_column_names.as_deref(), &pushed_filter_plan)?;

        Ok(ProviderScanPlan::from_parts(ProviderScanPlanParts {
            source_name: self.source_name().to_owned(),
            table_uri: self.source.table_uri().to_owned(),
            snapshot_version: self.snapshot_version(),
            projected_schema,
            protocol: self.protocol.clone(),
            scan_projection,
            pushed_filter_plan,
            kernel_scan,
        }))
    }

    /// Plans filters that DataFusion pushed into `scan`.
    ///
    /// This uses the same issue-33 partition equality policy as
    /// `supports_filters_pushdown`, but accepts owned expressions from the scan
    /// request instead of borrowed expressions from the support callback.
    fn plan_pushed_filters(&self, pushed_filters: &[Expr]) -> DeltaFilterPushdownPlan {
        let pushed_filters = pushed_filters
            .iter()
            .map(|filter| unqualify_filter_columns(filter.clone(), &self.schema))
            .collect::<Vec<_>>();
        let pushed_filter_refs = pushed_filters.iter().collect::<Vec<_>>();
        DeltaFilterPushdownPlan::partition_equality_pushdown(
            &pushed_filter_refs,
            &self.schema,
            &self.partition_columns(),
        )
    }

    /// Rejects pushed filters that this provider cannot fully apply.
    ///
    /// DataFusion treats pushed filters as provider-owned work. If any pushed
    /// filter is unsupported, inexact, or residual, accepting the scan would
    /// risk dropping part of the original filter.
    fn reject_unaccepted_pushed_filters(
        &self,
        pushed_filter_plan: &DeltaFilterPushdownPlan,
    ) -> Result<(), DeltaFunnelError> {
        // The scan contract can only accept filters that are fully applied by
        // this provider. Anything unsupported, inexact, or residual must be
        // rejected instead of being passed to delta_kernel partially.
        if pushed_filter_plan.unsupported_count > 0
            || pushed_filter_plan.inexact_count > 0
            || pushed_filter_plan.residual_filter_count > 0
        {
            return Err(DeltaFunnelError::DeltaScanFilter {
                source_name: self.source_name().to_owned(),
                table_uri: self.source.table_uri().to_owned(),
                reason: "pushed filters must be exact partition equality predicates".to_owned(),
            });
        }

        Ok(())
    }

    /// Builds the delta_kernel scan state for a projected provider scan.
    ///
    /// The provider output schema is already determined by projection planning.
    /// This helper only decides what delta_kernel must read internally, adds
    /// hidden partition predicate columns when needed, and attaches the combined
    /// exact predicate.
    fn build_kernel_scan(
        &self,
        projected_column_names: Option<&[String]>,
        pushed_filter_plan: &DeltaFilterPushdownPlan,
    ) -> Result<ProjectedDeltaScan, DeltaFunnelError> {
        let kernel_projected_column_names =
            kernel_scan_column_names(projected_column_names, pushed_filter_plan);
        let combined_predicate = pushed_filter_plan.combined_exact_kernel_predicate();

        build_projected_predicated_delta_scan(
            &self.source,
            kernel_projected_column_names.as_deref(),
            combined_predicate,
        )
        .map_err(|source| DeltaFunnelError::DeltaScanConstruction {
            source_name: self.source_name().to_owned(),
            table_uri: self.source.table_uri().to_owned(),
            source: Box::new(source),
        })
    }

    #[allow(dead_code)]
    fn plan_projection(
        &self,
        projection: Option<Vec<usize>>,
    ) -> Result<ProjectionPlan, DeltaFunnelError> {
        plan_projection(
            self.source_name(),
            self.source.table_uri(),
            &self.schema,
            projection,
        )
    }

    #[cfg(test)]
    pub(crate) fn set_schema_for_tests(&mut self, schema: SchemaRef) {
        self.schema = schema;
    }
}

/// Builds the delta_kernel read schema column list for a scan.
///
/// DataFusion's output schema remains the requested projection, but
/// delta_kernel validates predicate columns against its scan schema. When a
/// pushed exact partition filter references a column outside the requested
/// projection, that partition column is appended here as a hidden kernel read
/// column so predicate construction can stay exact.
fn kernel_scan_column_names(
    projected_column_names: Option<&[String]>,
    pushed_filter_plan: &DeltaFilterPushdownPlan,
) -> Option<Vec<String>> {
    // No requested projection means the kernel already scans the full table
    // schema, so every partition predicate column is already available.
    let mut column_names = projected_column_names?.to_vec();

    for partition_column in pushed_filter_plan.exact_partition_column_names() {
        if !column_names.contains(&partition_column) {
            column_names.push(partition_column);
        }
    }

    Some(column_names)
}

/// Removes relation qualifiers from provider support-check filters.
///
/// DataFusion's physical planner strips qualifiers before passing filters into
/// `scan`. The support callback receives the logical filter earlier, while it
/// may still contain references like `orders.region`. Normalizing here keeps
/// `supports_filters_pushdown` and `scan` aligned without relaxing the lower
/// level Delta kernel adapter for direct qualified-column inputs.
///
/// Nested-field style references are deliberately preserved. For example,
/// `profile.age` must not become `age` just because DataFusion stores it as a
/// column with a relation component.
fn unqualify_filter_columns(filter: Expr, schema: &SchemaRef) -> Expr {
    let original_filter = filter.clone();
    match filter
        .transform(|expr| {
            if let Expr::Column(column) = expr {
                if is_relation_qualified_top_level_column(&column, schema) {
                    Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                        column.name,
                    ))))
                } else {
                    Ok(Transformed::no(Expr::Column(column)))
                }
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .data()
    {
        Ok(filter) => filter,
        Err(_error) => original_filter,
    }
}

fn is_relation_qualified_top_level_column(column: &Column, schema: &SchemaRef) -> bool {
    let flat_name = column.flat_name();
    let Some((first_segment, _remainder)) = flat_name.split_once('.') else {
        return false;
    };

    schema.field_with_name(&column.name).is_ok() && schema.field_with_name(first_segment).is_err()
}

impl fmt::Debug for DeltaTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaTableProvider")
            .field("source_name", &self.source_name())
            .field("snapshot_version", &self.snapshot_version())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let scan_plan = self
            .plan_scan(ProviderScanPlanRequest {
                requested_projection: projection.cloned(),
                pushed_filters: filters.to_vec(),
            })
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        Ok(Arc::new(DeltaScanPlanningExec::new(scan_plan)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(self.plan_filters(filters).datafusion_pushdowns())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DataFusionError;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;

    use super::super::execution::DeltaScanPlanningExec;
    use super::*;
    use crate::query_engine::datafusion::registration::{
        DeltaTableProviderConfig, register_delta_sources,
    };
    use crate::query_engine::datafusion::test_support::{
        DeltaLogTable, NESTED_SCHEMA_FIELDS_JSON, PARTITIONED_SCHEMA_FIELDS_JSON,
        register_fixture_source,
    };
    use crate::{DeltaFunnelError, DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

    #[test]
    fn datafusion_table_provider_api_symbols_are_available() -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let table: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&schema)));
        let ctx = SessionContext::new();

        ctx.register_table("orders", Arc::clone(&table))?;

        assert_eq!(table.table_type(), TableType::Base);
        assert_eq!(table.schema().as_ref(), schema.as_ref());

        Ok(())
    }

    #[test]
    fn delta_provider_exposes_logical_arrow_schema() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("schema")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;

        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let schema = provider.schema();

        assert_eq!(provider.source_name(), "orders");
        assert_eq!(provider.snapshot_version(), 1);
        assert_eq!(provider.protocol().source_name, "orders");
        assert_eq!(provider.table_type(), TableType::Base);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "customer_name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(schema.field(1).is_nullable());

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_returns_projected_non_reading_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-scan-projection")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let projection = vec![1];

        let plan = provider
            .scan(&state, Some(&projection), &[], Some(10))
            .await?;
        let delta_plan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().field(0).name(), "customer_name");
        assert_eq!(delta_plan.scan_plan().source_name, "orders");
        assert_eq!(delta_plan.scan_plan().scan_projection, Some(vec![1]));

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_without_projection_returns_full_non_reading_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-full-scan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();

        let plan = provider.scan(&state, None, &[], None).await?;
        let delta_plan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(plan.schema().fields().len(), 2);
        assert_eq!(plan.schema().field(0).name(), "id");
        assert_eq!(plan.schema().field(1).name(), "customer_name");
        assert_eq!(delta_plan.scan_plan().scan_projection, None);

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_invalid_projection_before_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-invalid-projection")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let projection = vec![2];

        let result = provider.scan(&state, Some(&projection), &[], None).await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("projection index 2 is out of bounds"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_duplicate_projection_at_public_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-duplicate-projection")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let projection = vec![1, 1];

        let result = provider.scan(&state, Some(&projection), &[], None).await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("projection index 1 is duplicated"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_unsupported_pushed_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-filter-injection")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let filter = datafusion::logical_expr::col("id").eq(datafusion::logical_expr::lit(7));

        let result = provider.scan(&state, None, &[filter], None).await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("pushed filters must be exact partition equality predicates"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_accepts_exact_partition_equality_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "table-provider-exact-partition-filter",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let filter =
            datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us-west"));

        let plan = provider.scan(&state, None, &[filter], None).await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 1);
        assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
        assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 0);
        assert_eq!(scan.scan_plan().pushed_filter_plan.pushed_filter_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_accepts_qualified_exact_partition_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "table-provider-qualified-exact-partition-filter",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let filter = Expr::Column(datafusion::common::Column::new(Some("orders"), "region"))
            .eq(datafusion::logical_expr::lit("us-west"));

        let plan = provider.scan(&state, None, &[filter], None).await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 1);
        assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
        assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 0);
        assert_eq!(scan.scan_plan().pushed_filter_plan.pushed_filter_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_convertible_filter_injection()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "table-provider-convertible-filter-injection",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let filter = datafusion::logical_expr::col("region")
            .in_list(
                vec![
                    datafusion::logical_expr::lit("us-west"),
                    datafusion::logical_expr::lit("us-east"),
                ],
                false,
            )
            .and(datafusion::logical_expr::col("id").between(
                datafusion::logical_expr::lit(10),
                datafusion::logical_expr::lit(20),
            ));

        let result = provider.scan(&state, None, &[filter], None).await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("pushed filters must be exact partition equality predicates"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_limit_does_not_change_projection_contract()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-limit-unsupported")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let projection = vec![0];

        let with_limit = provider
            .scan(&state, Some(&projection), &[], Some(1))
            .await?;
        let without_limit = provider.scan(&state, Some(&projection), &[], None).await?;
        let with_limit_scan = with_limit
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let without_limit_scan = without_limit
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(with_limit.schema(), without_limit.schema());
        assert_eq!(
            with_limit_scan.scan_plan().scan_projection,
            without_limit_scan.scan_plan().scan_projection
        );

        Ok(())
    }

    #[tokio::test]
    async fn sql_analysis_works_for_select_star_without_scan_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _table = register_fixture_source(&ctx, "orders", "select-star")?;

        let dataframe = ctx.sql("select * from orders").await?;
        let schema = dataframe.schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "customer_name");

        Ok(())
    }

    #[tokio::test]
    async fn sql_analysis_works_for_projection_without_delta_projection_config()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _table = register_fixture_source(&ctx, "orders", "projection")?;

        let dataframe = ctx.sql("select customer_name from orders").await?;
        let optimized = dataframe.into_optimized_plan()?;
        let schema = optimized.schema();

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "customer_name");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);

        Ok(())
    }

    #[tokio::test]
    async fn residual_filter_column_remains_available_below_final_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _table = register_fixture_source(&ctx, "orders", "residual-filter-projection")?;

        let dataframe = ctx
            .sql("select id from orders where customer_name = 'alice'")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(plan_display.contains("FilterExec"), "{plan_display}");
        assert!(
            plan_display.contains("DeltaScanPlanningExec"),
            "{plan_display}"
        );
        assert_eq!(physical_plan.schema().fields().len(), 1);
        assert_eq!(physical_plan.schema().field(0).name(), "id");
        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].scan_plan().scan_projection,
            Some(vec![0, 1]),
            "scan must keep the residual filter column even though final output only projects id"
        );
        assert_eq!(scans[0].schema().fields().len(), 2);
        assert_eq!(scans[0].schema().field(0).name(), "id");
        assert_eq!(scans[0].schema().field(1).name(), "customer_name");

        Ok(())
    }

    #[tokio::test]
    async fn sql_exact_partition_filter_is_pushed_without_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema(
            "sql-exact-partition-filter",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
            }],
        )?;

        let dataframe = ctx
            .sql("select id from orders where region = 'us-west'")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(!plan_display.contains("FilterExec"), "{plan_display}");
        assert_eq!(physical_plan.schema().fields().len(), 1);
        assert_eq!(physical_plan.schema().field(0).name(), "id");
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0]));
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.unsupported_count, 0);
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert_eq!(scans[0].schema().fields().len(), 1);
        assert_eq!(scans[0].schema().field(0).name(), "id");
        let kernel_names = scans[0]
            .scan_plan()
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(kernel_names, vec!["id", "region"]);

        Ok(())
    }

    #[tokio::test]
    async fn sql_analysis_works_for_join_across_registered_sources()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _orders = register_fixture_source(&ctx, "orders", "join-orders")?;
        let _customers = register_fixture_source(&ctx, "customers", "join-customers")?;

        let dataframe = ctx
            .sql(
                "select orders.id, customers.customer_name \
                 from orders join customers on orders.id = customers.id",
            )
            .await?;
        let optimized = dataframe.into_optimized_plan()?;
        let schema = optimized.schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).name(), "customer_name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

        Ok(())
    }

    #[test]
    fn provider_schema_includes_partition_columns() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "partition-schema",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;

        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let schema = provider.schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).name(), "region");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

        Ok(())
    }

    #[tokio::test]
    async fn sql_analysis_accepts_nested_source_columns_without_target_planning()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "nested-schema",
            NESTED_SCHEMA_FIELDS_JSON,
            "[]",
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let ctx = SessionContext::new();

        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
            }],
        )?;
        let dataframe = ctx.sql("select id from orders").await?;
        let schema = dataframe.schema();

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);

        Ok(())
    }

    #[test]
    fn schema_conversion_failure_reports_source_and_field_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "schema-failure",
            super::super::test_support::INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON,
            "[]",
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;

        let result = DeltaTableProvider::try_new(source, preflight);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSourceSchema {
                source_name,
                reason,
                ..
            }) if source_name == "orders"
                && reason.contains("bad_array")
                && reason.contains("delta.columnMapping.nested.ids")
        ));

        Ok(())
    }
}
