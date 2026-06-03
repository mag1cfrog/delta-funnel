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
        DeltaPartitionMetadataPredicate, DeltaPartitionNameMap, ProjectedDeltaScan,
        build_projected_predicated_delta_scan, delta_source_arrow_schema,
    },
};

use super::execution::DeltaScanPlanningExec;
use super::filters::{DeltaFilterPushdownOutcome, DeltaFilterPushdownPlan};
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
    pub(crate) fn plan_supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DeltaFilterPushdownPlan {
        let filters =
            self.normalize_provider_filters(filters.iter().map(|filter| (*filter).clone()));

        self.plan_normalized_provider_filters(&filters)
    }

    #[allow(dead_code)]
    pub(crate) fn plan_scan(
        &self,
        request: ProviderScanPlanRequest,
    ) -> Result<ProviderScanPlan, DeltaFunnelError> {
        let ProviderScanPlanRequest {
            requested_projection,
            pushed_filters,
        } = request;
        let ProjectionPlan {
            projected_schema,
            scan_projection,
            projected_column_names,
        } = self.plan_projection(requested_projection)?;
        let normalized_pushed_filters = self.normalize_provider_filters(pushed_filters);
        let pushed_filter_plan = self.plan_normalized_provider_filters(&normalized_pushed_filters);
        self.reject_unaccepted_pushed_filters(&pushed_filter_plan)?;
        let partition_metadata_filter =
            self.build_partition_metadata_filter(&normalized_pushed_filters, &pushed_filter_plan)?;
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
            partition_metadata_filter,
            kernel_scan,
        }))
    }

    /// Applies safe name normalization to provider-boundary filters.
    ///
    /// DataFusion may present relation-qualified expressions to the support
    /// callback and unqualified expressions to `scan`. This helper owns the
    /// normalization step for both entry points before strict partition
    /// pushdown planning or metadata predicate conversion.
    fn normalize_provider_filters(&self, filters: impl IntoIterator<Item = Expr>) -> Vec<Expr> {
        filters
            .into_iter()
            .map(|filter| unqualify_filter_columns(filter, &self.schema))
            .collect()
    }

    /// Plans provider-boundary filters after normalization has been applied.
    ///
    /// Keeping this as a separate step lets scan planning reuse the same
    /// normalized expressions when building the provider-owned metadata
    /// predicate, so filter classification and metadata conversion cannot see
    /// different column names.
    fn plan_normalized_provider_filters(&self, filters: &[Expr]) -> DeltaFilterPushdownPlan {
        let filter_refs = filters.iter().collect::<Vec<_>>();

        DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
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
                reason: "pushed filters must be exact partition predicates".to_owned(),
            });
        }

        Ok(())
    }

    /// Builds the provider-owned partition metadata predicate for accepted filters.
    ///
    /// This mirrors the current exact partition filter set without changing
    /// kernel pruning yet. Later scan metadata planning can apply this
    /// predicate directly to `ScanFile.partition_values` when SQL-compatible
    /// metadata semantics must be authoritative.
    fn build_partition_metadata_filter(
        &self,
        normalized_pushed_filters: &[Expr],
        pushed_filter_plan: &DeltaFilterPushdownPlan,
    ) -> Result<Option<DeltaPartitionMetadataPredicate>, DeltaFunnelError> {
        let partition_columns = self.partition_columns();
        let physical_name_lookup = DeltaPartitionNameMap::identity(&partition_columns);
        let predicates = pushed_filter_plan
            .decisions
            .iter()
            .filter(|decision| decision.outcome == DeltaFilterPushdownOutcome::Exact)
            .map(|decision| {
                let filter = normalized_pushed_filters.get(decision.input_index).ok_or_else(|| {
                    DeltaFunnelError::DeltaScanFilter {
                        source_name: self.source_name().to_owned(),
                        table_uri: self.source.table_uri().to_owned(),
                        reason: "exact pushed filter index was not found".to_owned(),
                    }
                })?;

                DeltaPartitionMetadataPredicate::from_datafusion_expr(
                    filter,
                    &self.schema,
                    &partition_columns,
                    &physical_name_lookup,
                )
                .map_err(|error| DeltaFunnelError::DeltaScanFilter {
                    source_name: self.source_name().to_owned(),
                    table_uri: self.source.table_uri().to_owned(),
                    reason: format!(
                        "exact pushed filter cannot be converted to partition metadata predicate: {error}"
                    ),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(DeltaPartitionMetadataPredicate::and_from(predicates))
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
        Ok(self
            .plan_supports_filters_pushdown(filters)
            .datafusion_pushdowns())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{DataFusionError, ScalarValue};
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

    fn scan_file_paths(
        scan: &DeltaScanPlanningExec,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        scan.scan_plan()
            .kernel_scan()
            .scan_file_paths(&scan.scan_plan().table_uri)
    }

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
            .contains("pushed filters must be exact partition predicates"))
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
    async fn table_provider_scan_accepts_exact_partition_in_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "table-provider-exact-partition-in-filter",
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
        let filter = datafusion::logical_expr::col("region").in_list(
            vec![
                datafusion::logical_expr::lit("us-west"),
                datafusion::logical_expr::lit("us-east"),
            ],
            false,
        );

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
    async fn exact_partition_predicates_prune_null_missing_and_empty_values_distinctly()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "exact-partition-null-missing-empty-pruning",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            &[
                r#""partitionValues":{"region":"us-west"}"#,
                r#""partitionValues":{"region":"us-east"}"#,
                r#""partitionValues":{"region":null}"#,
                r#""partitionValues":{"region":""}"#,
                r#""partitionValues":{}"#,
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let cases = vec![
            (
                "single non-empty value",
                datafusion::logical_expr::col("region")
                    .eq(datafusion::logical_expr::lit("us-west")),
                vec!["part-00000.parquet"],
            ),
            (
                "in list with non-empty values",
                datafusion::logical_expr::col("region").in_list(
                    vec![
                        datafusion::logical_expr::lit("us-west"),
                        datafusion::logical_expr::lit("us-east"),
                    ],
                    false,
                ),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
        ];

        for (name, filter, expected_paths) in cases {
            let plan = provider
                .scan(&state, None, std::slice::from_ref(&filter), None)
                .await?;
            let scan = plan
                .as_any()
                .downcast_ref::<DeltaScanPlanningExec>()
                .ok_or("expected DeltaScanPlanningExec")?;

            assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 1, "{name}");
            assert_eq!(
                scan.scan_plan().pushed_filter_plan.residual_filter_count,
                0,
                "{name}"
            );
            assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_empty_string_partition_literals()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "table-provider-empty-string-partition-literals",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            &[
                r#""partitionValues":{"region":"us-west"}"#,
                r#""partitionValues":{"region":""}"#,
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let filters = vec![
            (
                "empty string equality",
                datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("")),
            ),
            (
                "empty string in",
                datafusion::logical_expr::col("region").in_list(
                    vec![
                        datafusion::logical_expr::lit("us-west"),
                        datafusion::logical_expr::lit(""),
                    ],
                    false,
                ),
            ),
        ];

        for (name, filter) in filters {
            let result = provider
                .scan(&state, None, std::slice::from_ref(&filter), None)
                .await;

            assert!(
                matches!(result, Err(DataFusionError::External(error)) if error
                    .to_string()
                    .contains("pushed filters must be exact partition predicates")),
                "{name} should be rejected"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_unproven_partition_in_filters()
    -> Result<(), Box<dyn std::error::Error>> {
        const TWO_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "table-provider-unproven-partition-in-filters",
            TWO_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["region","day"]"#,
            r#""partitionValues":{"region":"us-west","day":"2026-05-31"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let filters = vec![
            (
                "empty in",
                datafusion::logical_expr::col("region").in_list(Vec::<Expr>::new(), false),
            ),
            (
                "null in",
                datafusion::logical_expr::col("region")
                    .in_list(vec![Expr::Literal(ScalarValue::Utf8(None), None)], false),
            ),
            (
                "mixed null in",
                datafusion::logical_expr::col("region").in_list(
                    vec![
                        datafusion::logical_expr::lit("us-west"),
                        Expr::Literal(ScalarValue::Utf8(None), None),
                    ],
                    false,
                ),
            ),
            (
                "wrong literal type in",
                datafusion::logical_expr::col("region")
                    .in_list(vec![datafusion::logical_expr::lit(1_i64)], false),
            ),
            (
                "data column item in",
                datafusion::logical_expr::col("region")
                    .in_list(vec![datafusion::logical_expr::col("id")], false),
            ),
            (
                "partition column item in",
                datafusion::logical_expr::col("region")
                    .in_list(vec![datafusion::logical_expr::col("day")], false),
            ),
            (
                "cast item in",
                datafusion::logical_expr::col("region").in_list(
                    vec![datafusion::logical_expr::cast(
                        datafusion::logical_expr::lit("us-west"),
                        DataType::Utf8,
                    )],
                    false,
                ),
            ),
            (
                "not in",
                datafusion::logical_expr::col("region")
                    .in_list(vec![datafusion::logical_expr::lit("us-west")], true),
            ),
        ];

        for (name, filter) in filters {
            let result = provider
                .scan(&state, None, std::slice::from_ref(&filter), None)
                .await;

            assert!(
                matches!(result, Err(DataFusionError::External(error)) if error
                    .to_string()
                    .contains("pushed filters must be exact partition predicates")),
                "{name} should be rejected"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_unproven_null_sensitive_partition_filters()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "table-provider-unproven-null-sensitive-partition-filters",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            &[
                r#""partitionValues":{"region":"us-west"}"#,
                r#""partitionValues":{"region":null}"#,
                r#""partitionValues":{}"#,
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let filters = vec![
            ("is null", datafusion::logical_expr::col("region").is_null()),
            (
                "is not null",
                datafusion::logical_expr::col("region").is_not_null(),
            ),
            (
                "not equality",
                Expr::Not(Box::new(
                    datafusion::logical_expr::col("region")
                        .eq(datafusion::logical_expr::lit("us-west")),
                )),
            ),
            (
                "not in",
                datafusion::logical_expr::col("region")
                    .in_list(vec![datafusion::logical_expr::lit("us-west")], true),
            ),
        ];

        for (name, filter) in filters {
            let result = provider
                .scan(&state, None, std::slice::from_ref(&filter), None)
                .await;

            assert!(
                matches!(result, Err(DataFusionError::External(error)) if error
                    .to_string()
                    .contains("pushed filters must be exact partition predicates")),
                "{name} should be rejected"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_mixed_boolean_partition_filters()
    -> Result<(), Box<dyn std::error::Error>> {
        const TWO_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "table-provider-mixed-boolean-partition-filters",
            TWO_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["region","day"]"#,
            r#""partitionValues":{"region":"us-west","day":"2026-05-31"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let partition_in = datafusion::logical_expr::col("region").in_list(
            vec![
                datafusion::logical_expr::lit("us-west"),
                datafusion::logical_expr::lit("us-east"),
            ],
            false,
        );
        let exact_partition_or = partition_in
            .clone()
            .or(datafusion::logical_expr::col("region")
                .eq(datafusion::logical_expr::lit("eu-central")));
        let filters = vec![
            (
                "partition in and data",
                partition_in.clone().and(
                    datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1_i64)),
                ),
            ),
            (
                "partition in or data",
                partition_in
                    .clone()
                    .or(datafusion::logical_expr::col("id")
                        .eq(datafusion::logical_expr::lit(1_i64))),
            ),
            (
                "partition equality or data",
                datafusion::logical_expr::col("region")
                    .eq(datafusion::logical_expr::lit("us-west"))
                    .or(datafusion::logical_expr::col("id")
                        .eq(datafusion::logical_expr::lit(1_i64))),
            ),
            (
                "partition in or unknown",
                partition_in
                    .clone()
                    .or(datafusion::logical_expr::col("ghost")
                        .eq(datafusion::logical_expr::lit("x"))),
            ),
            (
                "partition in or nested field",
                partition_in
                    .clone()
                    .or(datafusion::logical_expr::col("profile.age")
                        .eq(datafusion::logical_expr::lit(1_i64))),
            ),
            (
                "nested exact partition or and data",
                exact_partition_or.and(
                    datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1_i64)),
                ),
            ),
        ];

        for (name, filter) in filters {
            let result = provider
                .scan(&state, None, std::slice::from_ref(&filter), None)
                .await;

            assert!(
                matches!(result, Err(DataFusionError::External(error)) if error
                    .to_string()
                    .contains("pushed filters must be exact partition predicates")),
                "{name} should be rejected"
            );
        }

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
            .contains("pushed filters must be exact partition predicates"))
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
    async fn sql_exact_partition_in_filter_is_pushed_without_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema(
            "sql-exact-partition-in-filter",
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
            .sql("select id from orders where region in ('us-west', 'us-east')")
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
    async fn sql_partition_in_edge_variants_document_rewrite_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        const TWO_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema(
            "sql-partition-in-edge-variants",
            TWO_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["region","day"]"#,
            r#""partitionValues":{"region":"us-west","day":"2026-05-31"}"#,
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

        enum ExpectedInProbe {
            EmptyBeforeScan,
            ExactAfterRewrite,
            ResidualFilter,
        }

        let sql_cases = [
            (
                "null only",
                "select id from orders where region in (null)",
                ExpectedInProbe::EmptyBeforeScan,
            ),
            (
                "mixed null",
                "select id from orders where region in ('us-west', null)",
                ExpectedInProbe::ResidualFilter,
            ),
            (
                "wrong literal type",
                "select id from orders where region in (1)",
                ExpectedInProbe::ExactAfterRewrite,
            ),
            (
                "data column item",
                "select id from orders where region in (id)",
                ExpectedInProbe::ResidualFilter,
            ),
            (
                "partition column item",
                "select id from orders where region in (day)",
                ExpectedInProbe::ResidualFilter,
            ),
            (
                "scalar function item",
                "select id from orders where region in (lower('us-west'))",
                ExpectedInProbe::ExactAfterRewrite,
            ),
            (
                "cast item",
                "select id from orders where region in (cast('us-west' as string))",
                ExpectedInProbe::ExactAfterRewrite,
            ),
            (
                "not in",
                "select id from orders where region not in ('us-west')",
                ExpectedInProbe::ResidualFilter,
            ),
        ];

        for (name, sql, expectation) in sql_cases {
            let dataframe = ctx.sql(sql).await?;
            let physical_plan = dataframe.create_physical_plan().await?;
            let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
                .indent(true)
                .to_string();
            let mut scans = Vec::new();
            super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

            match expectation {
                ExpectedInProbe::EmptyBeforeScan => {
                    assert!(plan_display.contains("EmptyExec"), "{name}: {plan_display}");
                    assert!(scans.is_empty(), "{name}: {plan_display}");
                }
                ExpectedInProbe::ExactAfterRewrite => {
                    assert!(
                        !plan_display.contains("FilterExec"),
                        "{name}: {plan_display}"
                    );
                    assert_eq!(scans.len(), 1, "{name}: {plan_display}");
                    assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
                    assert_eq!(scans[0].scan_plan().pushed_filter_plan.unsupported_count, 0);
                    assert_eq!(
                        scans[0]
                            .scan_plan()
                            .pushed_filter_plan
                            .residual_filter_count,
                        0
                    );
                }
                ExpectedInProbe::ResidualFilter => {
                    assert!(
                        plan_display.contains("FilterExec"),
                        "{name} unexpectedly became exact:\n{plan_display}"
                    );
                    assert_eq!(scans.len(), 1, "{name}: {plan_display}");
                    assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 0);
                    assert_eq!(
                        scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                        0
                    );
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_mixed_boolean_partition_filters_keep_required_residual_filters()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema(
            "sql-mixed-boolean-partition-filters",
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

        struct SqlMixedBooleanProbe {
            name: &'static str,
            sql: &'static str,
            exact_count: usize,
        }

        let sql_cases = [
            SqlMixedBooleanProbe {
                name: "partition in and data",
                sql: "select id from orders where region in ('us-west', 'us-east') and id > 1",
                exact_count: 1,
            },
            SqlMixedBooleanProbe {
                name: "partition in or data",
                sql: "select id from orders where region in ('us-west', 'us-east') or id = 1",
                exact_count: 0,
            },
            SqlMixedBooleanProbe {
                name: "partition equality or data",
                sql: "select id from orders where region = 'us-west' or id = 1",
                exact_count: 0,
            },
            SqlMixedBooleanProbe {
                name: "partition in or nested exact partition and data",
                sql: "select id from orders where (region in ('us-west', 'us-east') \
                      or region = 'eu-central') and id > 1",
                exact_count: 1,
            },
        ];

        for case in sql_cases {
            let dataframe = ctx.sql(case.sql).await?;
            let physical_plan = dataframe.create_physical_plan().await?;
            let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
                .indent(true)
                .to_string();
            let mut scans = Vec::new();
            super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

            assert!(
                plan_display.contains("FilterExec"),
                "{} should keep a residual filter:\n{}",
                case.name,
                plan_display
            );
            assert_eq!(scans.len(), 1, "{}: {}", case.name, plan_display);
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.exact_count,
                case.exact_count,
                "{}: {}",
                case.name,
                plan_display
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0,
                "{}: {}",
                case.name,
                plan_display
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_null_missing_and_empty_partition_filters_stay_residual_without_kernel_pruning()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-null-missing-empty-partition-filters",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            &[
                r#""partitionValues":{"region":"us-west"}"#,
                r#""partitionValues":{"region":null}"#,
                r#""partitionValues":{"region":""}"#,
                r#""partitionValues":{}"#,
            ],
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

        let sql_cases = [
            ("is null", "select id from orders where region is null"),
            (
                "is not null",
                "select id from orders where region is not null",
            ),
            (
                "not equality",
                "select id from orders where not(region = 'us-west')",
            ),
            (
                "not in",
                "select id from orders where region not in ('us-west')",
            ),
            ("empty string", "select id from orders where region = ''"),
            (
                "empty string in",
                "select id from orders where region in ('us-west', '')",
            ),
        ];

        for (name, sql) in sql_cases {
            let dataframe = ctx.sql(sql).await?;
            let physical_plan = dataframe.create_physical_plan().await?;
            let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
                .indent(true)
                .to_string();
            let mut scans = Vec::new();
            super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

            assert!(
                plan_display.contains("FilterExec"),
                "{name} unexpectedly became exact:\n{plan_display}"
            );
            assert_eq!(scans.len(), 1, "{name}: {plan_display}");
            assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 0);
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0
            );
            assert_eq!(
                scan_file_paths(scans[0])?,
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00002.parquet",
                    "part-00003.parquet",
                ],
                "{name}"
            );
        }

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
