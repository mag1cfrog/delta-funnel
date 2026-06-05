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
    /// Builds the complete provider scan plan for DataFusion's scan callback.
    ///
    /// Filter planning and scan planning intentionally stay separate here. The
    /// kernel scan is only responsible for snapshot/projection planning, while
    /// exact partition filters are converted into a provider-owned metadata
    /// predicate and carried on `ProviderScanPlan` for scan-file pruning.
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
        // Build the provider-side predicate before constructing the kernel
        // scan. This makes the partition pruning contract explicit: the exact
        // filters are accepted only because this predicate can later be applied
        // to scan-file partition metadata.
        let partition_metadata_filter = self.build_provider_partition_metadata_filter(
            &normalized_pushed_filters,
            &pushed_filter_plan,
        )?;
        // Do not pass partition predicates to delta_kernel. The kernel scan
        // should enumerate files for the requested projection; provider-owned
        // metadata pruning is applied after scan metadata expansion.
        let kernel_scan = self.build_kernel_scan(projected_column_names.as_deref())?;

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
    /// The resulting predicate is stored on `ProviderScanPlan` next to the
    /// unfiltered kernel scan state. Scan-file expansion applies it to
    /// `ScanFile.partition_values` before any file paths are handed to the read
    /// path, so partition pruning stays under provider SQL semantics instead of
    /// delta_kernel predicate semantics.
    fn build_provider_partition_metadata_filter(
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
    /// Partition pushdown is provider-owned and applied to Delta scan-file
    /// metadata after kernel scan planning. This keeps delta_kernel out of
    /// partition predicate semantics and lets the kernel scan schema match the
    /// requested projection.
    fn build_kernel_scan(
        &self,
        projected_column_names: Option<&[String]>,
    ) -> Result<ProjectedDeltaScan, DeltaFunnelError> {
        let kernel_projected_column_names = projected_column_names.map(|names| names.to_vec());

        build_projected_predicated_delta_scan(
            &self.source,
            kernel_projected_column_names.as_deref(),
            // Exact partition pushdown is represented by
            // `ProviderScanPlan::partition_metadata_filter`, not by
            // delta_kernel's predicate path.
            None,
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
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
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

    const INTEGER_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"byte_part\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"short_part\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"int_part\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"long_part\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]"#;
    const BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"is_current\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]"#;

    fn scan_file_paths(
        scan: &DeltaScanPlanningExec,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let scan_plan = scan.scan_plan();
        scan_plan.kernel_scan().scan_file_paths(
            &scan_plan.table_uri,
            scan_plan.partition_metadata_filter.as_ref(),
        )
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
            (
                "is null",
                datafusion::logical_expr::col("region").is_null(),
                vec!["part-00002.parquet", "part-00004.parquet"],
            ),
            (
                "is not null",
                datafusion::logical_expr::col("region").is_not_null(),
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00003.parquet",
                ],
            ),
            (
                "not equality",
                datafusion::logical_expr::col("region")
                    .not_eq(datafusion::logical_expr::lit("us-west")),
                vec!["part-00001.parquet", "part-00003.parquet"],
            ),
            (
                "not equality expression",
                Expr::Not(Box::new(
                    datafusion::logical_expr::col("region")
                        .eq(datafusion::logical_expr::lit("us-west")),
                )),
                vec!["part-00001.parquet", "part-00003.parquet"],
            ),
            (
                "not in list",
                datafusion::logical_expr::col("region").in_list(
                    vec![
                        datafusion::logical_expr::lit("us-west"),
                        datafusion::logical_expr::lit("us-east"),
                    ],
                    true,
                ),
                vec!["part-00003.parquet"],
            ),
            (
                "not in expression",
                Expr::Not(Box::new(datafusion::logical_expr::col("region").in_list(
                    vec![
                        datafusion::logical_expr::lit("us-west"),
                        datafusion::logical_expr::lit("us-east"),
                    ],
                    false,
                ))),
                vec!["part-00003.parquet"],
            ),
            (
                "less than",
                datafusion::logical_expr::col("region")
                    .lt(datafusion::logical_expr::lit("us-west")),
                vec!["part-00001.parquet", "part-00003.parquet"],
            ),
            (
                "less than or equal",
                datafusion::logical_expr::col("region")
                    .lt_eq(datafusion::logical_expr::lit("us-east")),
                vec!["part-00001.parquet", "part-00003.parquet"],
            ),
            (
                "greater than",
                datafusion::logical_expr::col("region")
                    .gt(datafusion::logical_expr::lit("us-east")),
                vec!["part-00000.parquet"],
            ),
            (
                "greater than or equal",
                datafusion::logical_expr::col("region")
                    .gt_eq(datafusion::logical_expr::lit("us-west")),
                vec!["part-00000.parquet"],
            ),
            (
                "between",
                datafusion::logical_expr::col("region").between(
                    datafusion::logical_expr::lit("us-east"),
                    datafusion::logical_expr::lit("us-west"),
                ),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not between",
                datafusion::logical_expr::col("region").not_between(
                    datafusion::logical_expr::lit("us-east"),
                    datafusion::logical_expr::lit("us-west"),
                ),
                vec!["part-00003.parquet"],
            ),
            (
                "contradictory between",
                datafusion::logical_expr::col("region").between(
                    datafusion::logical_expr::lit("z"),
                    datafusion::logical_expr::lit("a"),
                ),
                Vec::new(),
            ),
            (
                "contradictory not between",
                datafusion::logical_expr::col("region").not_between(
                    datafusion::logical_expr::lit("z"),
                    datafusion::logical_expr::lit("a"),
                ),
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00003.parquet",
                ],
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
            assert!(
                scan.scan_plan().partition_metadata_filter.is_some(),
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
            (
                "empty string comparison",
                datafusion::logical_expr::col("region").lt(datafusion::logical_expr::lit("")),
            ),
            (
                "empty string between",
                datafusion::logical_expr::col("region").between(
                    datafusion::logical_expr::lit(""),
                    datafusion::logical_expr::lit("us-west"),
                ),
            ),
            (
                "null between",
                datafusion::logical_expr::col("region").between(
                    Expr::Literal(ScalarValue::Utf8(None), None),
                    datafusion::logical_expr::lit("us-west"),
                ),
            ),
            (
                "numeric between",
                datafusion::logical_expr::col("region").between(
                    datafusion::logical_expr::lit(7_i64),
                    datafusion::logical_expr::lit("us-west"),
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
                "not in with null",
                datafusion::logical_expr::col("region").in_list(
                    vec![
                        datafusion::logical_expr::lit("us-west"),
                        Expr::Literal(ScalarValue::Utf8(None), None),
                    ],
                    true,
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
    async fn table_provider_scan_rejects_unsafe_negated_partition_filters()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "table-provider-unsafe-negated-partition-filters",
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
                "not empty string equality",
                Expr::Not(Box::new(
                    datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("")),
                )),
            ),
            (
                "empty string not in",
                datafusion::logical_expr::col("region").in_list(
                    vec![
                        datafusion::logical_expr::lit("us-west"),
                        datafusion::logical_expr::lit(""),
                    ],
                    true,
                ),
            ),
            (
                "non literal not in",
                datafusion::logical_expr::col("region")
                    .in_list(vec![datafusion::logical_expr::col("id")], true),
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
        assert_eq!(kernel_names, vec!["id"]);

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
        assert_eq!(kernel_names, vec!["id"]);

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
                ExpectedInProbe::ExactAfterRewrite,
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
    async fn sql_null_partition_filters_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-null-partition-filters-exact",
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
            (
                "is null",
                "select id from orders where region is null",
                vec!["part-00001.parquet", "part-00003.parquet"],
            ),
            (
                "is not null",
                "select id from orders where region is not null",
                vec!["part-00000.parquet", "part-00002.parquet"],
            ),
        ];

        for (name, sql, expected_paths) in sql_cases {
            let dataframe = ctx.sql(sql).await?;
            let physical_plan = dataframe.create_physical_plan().await?;
            let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
                .indent(true)
                .to_string();
            let mut scans = Vec::new();
            super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

            assert!(
                !plan_display.contains("FilterExec"),
                "{name} unexpectedly kept a residual filter:\n{plan_display}"
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
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");

            let kernel_names = scans[0]
                .scan_plan()
                .kernel_scan()
                .kernel_schema()
                .fields()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>();
            assert_eq!(kernel_names, vec!["id"], "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_negated_partition_filters_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-negated-partition-filters-exact",
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
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
            }],
        )?;

        let sql_cases = [
            (
                "not equality",
                "select id from orders where region != 'us-west'",
                1,
                vec!["part-00001.parquet", "part-00003.parquet"],
            ),
            (
                "not equality expression",
                "select id from orders where not(region = 'us-west')",
                1,
                vec!["part-00001.parquet", "part-00003.parquet"],
            ),
            (
                "not in",
                "select id from orders where region not in ('us-west', 'us-east')",
                2,
                vec!["part-00003.parquet"],
            ),
        ];

        for (name, sql, expected_exact_count, expected_paths) in sql_cases {
            let dataframe = ctx.sql(sql).await?;
            let physical_plan = dataframe.create_physical_plan().await?;
            let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
                .indent(true)
                .to_string();
            let mut scans = Vec::new();
            super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

            assert!(
                !plan_display.contains("FilterExec"),
                "{name} unexpectedly kept a residual filter:\n{plan_display}"
            );
            assert_eq!(scans.len(), 1, "{name}: {plan_display}");
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.exact_count,
                expected_exact_count,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_partition_comparison_filters_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-partition-comparison-filters-exact",
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
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
            }],
        )?;

        let sql_cases = [
            (
                "less than",
                "select id from orders where region < 'us-west'",
                1,
                vec!["part-00001.parquet", "part-00003.parquet"],
            ),
            (
                "less than or equal",
                "select id from orders where region <= 'us-east'",
                1,
                vec!["part-00001.parquet", "part-00003.parquet"],
            ),
            (
                "greater than",
                "select id from orders where region > 'us-east'",
                1,
                vec!["part-00000.parquet"],
            ),
            (
                "reversed greater than",
                "select id from orders where 'us-east' < region",
                1,
                vec!["part-00000.parquet"],
            ),
            (
                "between",
                "select id from orders where region between 'us-east' and 'us-west'",
                2,
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not between",
                "select id from orders where region not between 'us-east' and 'us-west'",
                1,
                vec!["part-00003.parquet"],
            ),
            (
                "contradictory between",
                "select id from orders where region between 'z' and 'a'",
                2,
                Vec::new(),
            ),
            (
                "contradictory not between",
                "select id from orders where region not between 'z' and 'a'",
                1,
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00003.parquet",
                ],
            ),
        ];

        for (name, sql, expected_exact_count, expected_paths) in sql_cases {
            let dataframe = ctx.sql(sql).await?;
            let physical_plan = dataframe.create_physical_plan().await?;
            let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
                .indent(true)
                .to_string();
            let mut scans = Vec::new();
            super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

            assert!(
                !plan_display.contains("FilterExec"),
                "{name} unexpectedly kept a residual filter:\n{plan_display}"
            );
            assert_eq!(scans.len(), 1, "{name}: {plan_display}");
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.exact_count,
                expected_exact_count,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_empty_string_partition_filters_stay_residual_without_kernel_pruning()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-null-sensitive-partition-filters",
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
            (
                "empty string",
                "select id from orders where region = ''",
                0,
                0,
                false,
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00002.parquet",
                    "part-00003.parquet",
                ],
            ),
            (
                "empty string in",
                "select id from orders where region in ('us-west', '')",
                0,
                0,
                false,
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00002.parquet",
                    "part-00003.parquet",
                ],
            ),
            (
                "empty string comparison",
                "select id from orders where region < ''",
                0,
                0,
                false,
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00002.parquet",
                    "part-00003.parquet",
                ],
            ),
            (
                "empty string between",
                "select id from orders where region between '' and 'us-west'",
                1,
                1,
                true,
                vec!["part-00000.parquet", "part-00002.parquet"],
            ),
        ];

        for (
            name,
            sql,
            expected_exact_count,
            expected_pushed_filter_count,
            expected_metadata_filter,
            expected_paths,
        ) in sql_cases
        {
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
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.exact_count,
                expected_exact_count,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                expected_pushed_filter_count,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().partition_metadata_filter.is_some(),
                expected_metadata_filter,
                "{name}: {plan_display}"
            );
            assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");
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

    #[test]
    fn integer_partition_schema_maps_delta_types_to_arrow_widths()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "integer-partition-schema",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["byte_part","short_part","int_part","long_part"]"#,
            r#""partitionValues":{"byte_part":"-8","short_part":"-1024","int_part":"0","long_part":"9223372036854775807"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;

        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let schema = provider.schema();

        assert_eq!(
            schema.field_with_name("byte_part")?.data_type(),
            &DataType::Int8
        );
        assert_eq!(
            schema.field_with_name("short_part")?.data_type(),
            &DataType::Int16
        );
        assert_eq!(
            schema.field_with_name("int_part")?.data_type(),
            &DataType::Int32
        );
        assert_eq!(
            schema.field_with_name("long_part")?.data_type(),
            &DataType::Int64
        );

        Ok(())
    }

    #[test]
    fn boolean_partition_schema_maps_delta_type_to_arrow_boolean()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "boolean-partition-schema",
            BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["is_current"]"#,
            r#""partitionValues":{"is_current":"true"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;

        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let schema = provider.schema();

        assert_eq!(
            schema.field_with_name("is_current")?.data_type(),
            &DataType::Boolean
        );

        Ok(())
    }

    #[tokio::test]
    async fn boolean_partition_null_checks_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "boolean-partition-null-checks",
            BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["is_current"]"#,
            &[
                r#""partitionValues":{"is_current":"true"}"#,
                r#""partitionValues":{"is_current":"false"}"#,
                r#""partitionValues":{"is_current":null}"#,
                r#""partitionValues":{"is_current":""}"#,
                r#""partitionValues":{"is_current":"not-a-boolean"}"#,
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
        let cases = [
            (
                "is null",
                datafusion::logical_expr::col("is_current").is_null(),
                vec!["part-00002.parquet", "part-00005.parquet"],
            ),
            (
                "is not null",
                datafusion::logical_expr::col("is_current").is_not_null(),
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00003.parquet",
                    "part-00004.parquet",
                ],
            ),
        ];

        for (name, filter, expected_paths) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

            let plan = provider
                .scan(&state, Some(&vec![0]), &[filter], None)
                .await?;
            let scan = plan
                .as_any()
                .downcast_ref::<DeltaScanPlanningExec>()
                .ok_or("expected DeltaScanPlanningExec")?;

            assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 1, "{name}");
            assert_eq!(
                scan.scan_plan().pushed_filter_plan.unsupported_count,
                0,
                "{name}"
            );
            assert_eq!(
                scan.scan_plan().pushed_filter_plan.residual_filter_count,
                0,
                "{name}"
            );
            assert!(
                scan.scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn boolean_partition_equality_and_membership_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "boolean-partition-equality-membership",
            BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["is_current"]"#,
            &[
                r#""partitionValues":{"is_current":"true"}"#,
                r#""partitionValues":{"is_current":"false"}"#,
                r#""partitionValues":{"is_current":null}"#,
                r#""partitionValues":{"is_current":""}"#,
                r#""partitionValues":{"is_current":"not-a-boolean"}"#,
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
        let cases = [
            (
                "equality true",
                datafusion::logical_expr::col("is_current").eq(datafusion::logical_expr::lit(true)),
                vec!["part-00000.parquet"],
            ),
            (
                "reversed equality false",
                datafusion::logical_expr::lit(false)
                    .eq(datafusion::logical_expr::col("is_current")),
                vec!["part-00001.parquet"],
            ),
            (
                "inequality",
                datafusion::logical_expr::col("is_current")
                    .not_eq(datafusion::logical_expr::lit(true)),
                vec!["part-00001.parquet"],
            ),
            (
                "in list",
                datafusion::logical_expr::col("is_current").in_list(
                    vec![
                        datafusion::logical_expr::lit(true),
                        datafusion::logical_expr::lit(false),
                        datafusion::logical_expr::lit(true),
                    ],
                    false,
                ),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not in list",
                datafusion::logical_expr::col("is_current")
                    .in_list(vec![datafusion::logical_expr::lit(true)], true),
                vec!["part-00001.parquet"],
            ),
        ];

        for (name, filter, expected_paths) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

            let plan = provider
                .scan(&state, Some(&vec![0]), &[filter], None)
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
            assert!(
                scan.scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn boolean_partition_shorthand_is_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "boolean-partition-shorthand",
            BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["is_current"]"#,
            &[
                r#""partitionValues":{"is_current":"true"}"#,
                r#""partitionValues":{"is_current":"false"}"#,
                r#""partitionValues":{"is_current":null}"#,
                r#""partitionValues":{"is_current":""}"#,
                r#""partitionValues":{"is_current":"not-a-boolean"}"#,
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
        let cases = [
            (
                "shorthand",
                datafusion::logical_expr::col("is_current"),
                vec!["part-00000.parquet"],
            ),
            (
                "not shorthand",
                Expr::Not(Box::new(datafusion::logical_expr::col("is_current"))),
                vec!["part-00001.parquet"],
            ),
        ];

        for (name, filter, expected_paths) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

            let plan = provider
                .scan(&state, Some(&vec![0]), &[filter], None)
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
            assert!(
                scan.scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn boolean_partition_unsafe_literal_shapes_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "boolean-partition-unsafe-literal-shapes",
            BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["is_current"]"#,
            r#""partitionValues":{"is_current":"true"}"#,
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
                "string literal equality",
                datafusion::logical_expr::col("is_current")
                    .eq(datafusion::logical_expr::lit("true")),
            ),
            (
                "null equality",
                datafusion::logical_expr::col("is_current")
                    .eq(Expr::Literal(ScalarValue::Boolean(None), None)),
            ),
            (
                "null in",
                datafusion::logical_expr::col("is_current").in_list(
                    vec![
                        datafusion::logical_expr::lit(true),
                        Expr::Literal(ScalarValue::Boolean(None), None),
                    ],
                    false,
                ),
            ),
            (
                "mixed string boolean in",
                datafusion::logical_expr::col("is_current").in_list(
                    vec![
                        datafusion::logical_expr::lit(true),
                        datafusion::logical_expr::lit("false"),
                    ],
                    false,
                ),
            ),
            (
                "non literal in",
                datafusion::logical_expr::col("is_current")
                    .in_list(vec![datafusion::logical_expr::col("id")], false),
            ),
        ];
        let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );

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
    async fn boolean_partition_unsafe_ordering_shapes_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "boolean-partition-unsafe-ordering-shapes",
            BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["is_current"]"#,
            r#""partitionValues":{"is_current":"true"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let scalar_udf = create_udf(
            "boolean_identity_for_pushdown_boundary",
            vec![DataType::Boolean],
            DataType::Boolean,
            Volatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))),
        );
        let scalar_function =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![datafusion::logical_expr::col("is_current")],
            ));
        let filters = vec![
            (
                "less than",
                datafusion::logical_expr::col("is_current").lt(datafusion::logical_expr::lit(true)),
            ),
            (
                "greater than or equal",
                datafusion::logical_expr::col("is_current")
                    .gt_eq(datafusion::logical_expr::lit(false)),
            ),
            (
                "between",
                datafusion::logical_expr::col("is_current").between(
                    datafusion::logical_expr::lit(false),
                    datafusion::logical_expr::lit(true),
                ),
            ),
            (
                "not between",
                datafusion::logical_expr::col("is_current").not_between(
                    datafusion::logical_expr::lit(false),
                    datafusion::logical_expr::lit(true),
                ),
            ),
            (
                "cast operand",
                datafusion::logical_expr::col("is_current").eq(datafusion::logical_expr::cast(
                    datafusion::logical_expr::lit(true),
                    DataType::Boolean,
                )),
            ),
            (
                "scalar function operand",
                datafusion::logical_expr::col("is_current").eq(scalar_function),
            ),
        ];
        let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );

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

    #[test]
    fn integer_partition_uncoerced_literals_remain_unsupported()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "integer-partition-unsupported-boundary",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["byte_part","short_part","int_part","long_part"]"#,
            r#""partitionValues":{"byte_part":"7","short_part":"1024","int_part":"0","long_part":"9223372036854775807"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let filters = [
            datafusion::logical_expr::col("int_part").eq(datafusion::logical_expr::lit("0")),
            datafusion::logical_expr::col("int_part").between(
                datafusion::logical_expr::lit("-10"),
                datafusion::logical_expr::lit("10"),
            ),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let support = provider.supports_filters_pushdown(&filter_refs)?;
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());

        Ok(())
    }

    #[tokio::test]
    async fn integer_partition_unsafe_direct_shapes_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "integer-partition-unsafe-direct-shapes",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["long_part"]"#,
            r#""partitionValues":{"long_part":"7"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let scalar_udf = create_udf(
            "integer_identity_for_pushdown_boundary",
            vec![DataType::Int64],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(7))))),
        );
        let scalar_function =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![datafusion::logical_expr::col("long_part")],
            ));
        let filters = vec![
            (
                "null equality",
                datafusion::logical_expr::col("long_part")
                    .eq(Expr::Literal(ScalarValue::Int64(None), None)),
            ),
            (
                "empty in",
                datafusion::logical_expr::col("long_part").in_list(Vec::<Expr>::new(), false),
            ),
            (
                "null in",
                datafusion::logical_expr::col("long_part").in_list(
                    vec![
                        datafusion::logical_expr::lit(7_i64),
                        Expr::Literal(ScalarValue::Int64(None), None),
                    ],
                    false,
                ),
            ),
            (
                "mixed string numeric in",
                datafusion::logical_expr::col("long_part").in_list(
                    vec![
                        datafusion::logical_expr::lit(7_i64),
                        datafusion::logical_expr::lit("7"),
                    ],
                    false,
                ),
            ),
            (
                "non literal in",
                datafusion::logical_expr::col("long_part")
                    .in_list(vec![datafusion::logical_expr::col("id")], false),
            ),
            (
                "null between",
                datafusion::logical_expr::col("long_part").between(
                    Expr::Literal(ScalarValue::Int64(None), None),
                    datafusion::logical_expr::lit(10_i64),
                ),
            ),
            (
                "non literal between",
                datafusion::logical_expr::col("long_part").between(
                    datafusion::logical_expr::col("id"),
                    datafusion::logical_expr::lit(10_i64),
                ),
            ),
            (
                "cast operand",
                datafusion::logical_expr::col("long_part").eq(datafusion::logical_expr::cast(
                    datafusion::logical_expr::lit(7_i64),
                    DataType::Int64,
                )),
            ),
            (
                "scalar function operand",
                datafusion::logical_expr::col("long_part").eq(scalar_function),
            ),
        ];
        let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );

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
    async fn integer_partition_between_is_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "integer-partition-between",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["long_part"]"#,
            &[
                r#""partitionValues":{"long_part":"7"}"#,
                r#""partitionValues":{"long_part":"-1"}"#,
                r#""partitionValues":{"long_part":"20"}"#,
                r#""partitionValues":{"long_part":null}"#,
                r#""partitionValues":{"long_part":""}"#,
                r#""partitionValues":{"long_part":"not-an-integer"}"#,
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
        let cases = [
            (
                "between inclusive",
                datafusion::logical_expr::col("long_part").between(
                    datafusion::logical_expr::lit(-1_i64),
                    datafusion::logical_expr::lit(7_i64),
                ),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not between",
                datafusion::logical_expr::col("long_part").not_between(
                    datafusion::logical_expr::lit(-1_i64),
                    datafusion::logical_expr::lit(7_i64),
                ),
                vec!["part-00002.parquet"],
            ),
            (
                "contradictory between",
                datafusion::logical_expr::col("long_part").between(
                    datafusion::logical_expr::lit(10_i64),
                    datafusion::logical_expr::lit(-10_i64),
                ),
                Vec::<&str>::new(),
            ),
            (
                "contradictory not between",
                datafusion::logical_expr::col("long_part").not_between(
                    datafusion::logical_expr::lit(10_i64),
                    datafusion::logical_expr::lit(-10_i64),
                ),
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00002.parquet",
                ],
            ),
        ];

        for (name, filter, expected_paths) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

            let plan = provider
                .scan(&state, Some(&vec![0]), &[filter], None)
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
            assert!(
                scan.scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn integer_partition_boolean_composition_and_projection_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "integer-partition-boolean-composition",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["long_part"]"#,
            &[
                r#""partitionValues":{"long_part":"7"}"#,
                r#""partitionValues":{"long_part":"-1"}"#,
                r#""partitionValues":{"long_part":"20"}"#,
                r#""partitionValues":{"long_part":null}"#,
                r#""partitionValues":{"long_part":""}"#,
                r#""partitionValues":{"long_part":"not-an-integer"}"#,
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
        let separate_and_filters = vec![
            datafusion::logical_expr::col("long_part").gt_eq(datafusion::logical_expr::lit(-1_i64)),
            datafusion::logical_expr::col("long_part").lt(datafusion::logical_expr::lit(20_i64)),
        ];
        let whole_and_filter = datafusion::logical_expr::col("long_part")
            .gt_eq(datafusion::logical_expr::lit(-1_i64))
            .and(
                datafusion::logical_expr::col("long_part")
                    .lt(datafusion::logical_expr::lit(20_i64)),
            );
        let whole_or_filter = datafusion::logical_expr::col("long_part")
            .eq(datafusion::logical_expr::lit(7_i64))
            .or(datafusion::logical_expr::col("long_part")
                .eq(datafusion::logical_expr::lit(20_i64)));
        let whole_not_filter = Expr::Not(Box::new(
            datafusion::logical_expr::col("long_part").eq(datafusion::logical_expr::lit(7_i64)),
        ));
        let cases = [
            (
                "separate filters combine with and",
                separate_and_filters,
                2,
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "whole and",
                vec![whole_and_filter],
                1,
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "whole or",
                vec![whole_or_filter],
                1,
                vec!["part-00000.parquet", "part-00002.parquet"],
            ),
            (
                "whole not",
                vec![whole_not_filter],
                1,
                vec!["part-00001.parquet", "part-00002.parquet"],
            ),
        ];

        for (name, filters, expected_exact_count, expected_paths) in cases {
            let filter_refs = filters.iter().collect::<Vec<_>>();
            let support = provider.supports_filters_pushdown(&filter_refs)?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Exact; filters.len()],
                "{name}"
            );

            let plan = provider
                .scan(&state, Some(&vec![0]), &filters, None)
                .await?;
            let scan = plan
                .as_any()
                .downcast_ref::<DeltaScanPlanningExec>()
                .ok_or("expected DeltaScanPlanningExec")?;

            assert_eq!(
                scan.scan_plan().projected_schema.field(0).name(),
                "id",
                "{name}"
            );
            assert_eq!(
                scan.scan_plan().pushed_filter_plan.exact_count,
                expected_exact_count,
                "{name}"
            );
            assert_eq!(
                scan.scan_plan().pushed_filter_plan.residual_filter_count,
                0,
                "{name}"
            );
            assert!(
                scan.scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn integer_partition_comparisons_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "integer-partition-comparisons",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["long_part"]"#,
            &[
                r#""partitionValues":{"long_part":"7"}"#,
                r#""partitionValues":{"long_part":"-1"}"#,
                r#""partitionValues":{"long_part":null}"#,
                r#""partitionValues":{"long_part":""}"#,
                r#""partitionValues":{"long_part":"not-an-integer"}"#,
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
        let cases = [
            (
                "less than",
                datafusion::logical_expr::col("long_part").lt(datafusion::logical_expr::lit(7_i64)),
                vec!["part-00001.parquet"],
            ),
            (
                "less than or equal",
                datafusion::logical_expr::col("long_part")
                    .lt_eq(datafusion::logical_expr::lit(-1_i64)),
                vec!["part-00001.parquet"],
            ),
            (
                "greater than",
                datafusion::logical_expr::col("long_part")
                    .gt(datafusion::logical_expr::lit(-1_i64)),
                vec!["part-00000.parquet"],
            ),
            (
                "greater than or equal",
                datafusion::logical_expr::col("long_part")
                    .gt_eq(datafusion::logical_expr::lit(7_i64)),
                vec!["part-00000.parquet"],
            ),
            (
                "reversed less than",
                datafusion::logical_expr::lit(7_i64).gt(datafusion::logical_expr::col("long_part")),
                vec!["part-00001.parquet"],
            ),
        ];

        for (name, filter, expected_paths) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

            let plan = provider
                .scan(&state, Some(&vec![0]), &[filter], None)
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
            assert!(
                scan.scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn integer_partition_equality_and_membership_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "integer-partition-equality-membership",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["long_part"]"#,
            &[
                r#""partitionValues":{"long_part":"7"}"#,
                r#""partitionValues":{"long_part":"-1"}"#,
                r#""partitionValues":{"long_part":null}"#,
                r#""partitionValues":{"long_part":""}"#,
                r#""partitionValues":{"long_part":"not-an-integer"}"#,
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
        let cases = [
            (
                "equality",
                datafusion::logical_expr::col("long_part").eq(datafusion::logical_expr::lit(7_i64)),
                vec!["part-00000.parquet"],
            ),
            (
                "reversed equality",
                datafusion::logical_expr::lit(7_i64).eq(datafusion::logical_expr::col("long_part")),
                vec!["part-00000.parquet"],
            ),
            (
                "inequality",
                datafusion::logical_expr::col("long_part")
                    .not_eq(datafusion::logical_expr::lit(7_i64)),
                vec!["part-00001.parquet"],
            ),
            (
                "in list",
                datafusion::logical_expr::col("long_part").in_list(
                    vec![
                        datafusion::logical_expr::lit(7_i64),
                        datafusion::logical_expr::lit(-1_i64),
                    ],
                    false,
                ),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not in list",
                datafusion::logical_expr::col("long_part")
                    .in_list(vec![datafusion::logical_expr::lit(7_i64)], true),
                vec!["part-00001.parquet"],
            ),
        ];

        for (name, filter, expected_paths) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

            let plan = provider
                .scan(&state, Some(&vec![0]), &[filter], None)
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
            assert!(
                scan.scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[test]
    fn integer_partition_width_bounds_are_respected_for_direct_filters()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "integer-partition-width-boundaries",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["byte_part","short_part","int_part","long_part"]"#,
            r#""partitionValues":{"byte_part":"127","short_part":"32767","int_part":"2147483647","long_part":"9223372036854775807"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let exact_filters = [
            datafusion::logical_expr::col("byte_part").eq(datafusion::logical_expr::lit(127_i8)),
            datafusion::logical_expr::col("short_part")
                .eq(datafusion::logical_expr::lit(32767_i16)),
            datafusion::logical_expr::col("int_part")
                .eq(datafusion::logical_expr::lit(2147483647_i32)),
            datafusion::logical_expr::col("long_part")
                .eq(datafusion::logical_expr::lit(9223372036854775807_i64)),
            datafusion::logical_expr::col("byte_part").lt(datafusion::logical_expr::lit(127_i8)),
            datafusion::logical_expr::col("short_part")
                .gt_eq(datafusion::logical_expr::lit(32767_i16)),
            datafusion::logical_expr::col("int_part").between(
                datafusion::logical_expr::lit(-2147483648_i32),
                datafusion::logical_expr::lit(2147483647_i32),
            ),
        ];
        let unsupported_filters = [
            datafusion::logical_expr::col("byte_part").eq(datafusion::logical_expr::lit(128_i16)),
            datafusion::logical_expr::col("short_part")
                .eq(datafusion::logical_expr::lit(32768_i32)),
            datafusion::logical_expr::col("int_part")
                .eq(datafusion::logical_expr::lit(2147483648_i64)),
            datafusion::logical_expr::col("byte_part").lt(datafusion::logical_expr::lit(128_i16)),
            datafusion::logical_expr::col("short_part")
                .gt_eq(datafusion::logical_expr::lit(32768_i32)),
            datafusion::logical_expr::col("int_part").between(
                datafusion::logical_expr::lit(-2147483649_i64),
                datafusion::logical_expr::lit(2147483647_i32),
            ),
        ];
        let exact_refs = exact_filters.iter().collect::<Vec<_>>();
        let unsupported_refs = unsupported_filters.iter().collect::<Vec<_>>();

        let exact_plan = provider.plan_supports_filters_pushdown(&exact_refs);
        let unsupported_plan = provider.plan_supports_filters_pushdown(&unsupported_refs);

        assert_eq!(exact_plan.exact_count, exact_filters.len());
        assert_eq!(exact_plan.unsupported_count, 0);
        assert_eq!(unsupported_plan.exact_count, 0);
        assert_eq!(
            unsupported_plan.unsupported_count,
            unsupported_filters.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn integer_partition_null_checks_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "integer-partition-null-checks",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["long_part"]"#,
            &[
                r#""partitionValues":{"long_part":"7"}"#,
                r#""partitionValues":{"long_part":"-1"}"#,
                r#""partitionValues":{"long_part":null}"#,
                r#""partitionValues":{"long_part":""}"#,
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
        let cases = [
            (
                "is null",
                datafusion::logical_expr::col("long_part").is_null(),
                vec!["part-00002.parquet", "part-00004.parquet"],
            ),
            (
                "is not null",
                datafusion::logical_expr::col("long_part").is_not_null(),
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00003.parquet",
                ],
            ),
        ];

        for (name, filter, expected_paths) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

            let plan = provider
                .scan(&state, Some(&vec![0]), &[filter], None)
                .await?;
            let scan = plan
                .as_any()
                .downcast_ref::<DeltaScanPlanningExec>()
                .ok_or("expected DeltaScanPlanningExec")?;

            assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 1, "{name}");
            assert_eq!(
                scan.scan_plan().pushed_filter_plan.unsupported_count,
                0,
                "{name}"
            );
            assert_eq!(
                scan.scan_plan().pushed_filter_plan.residual_filter_count,
                0,
                "{name}"
            );
            assert!(
                scan.scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_integer_partition_null_checks_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-integer-partition-null-checks",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["long_part"]"#,
            &[
                r#""partitionValues":{"long_part":"7"}"#,
                r#""partitionValues":{"long_part":"-1"}"#,
                r#""partitionValues":{"long_part":null}"#,
                r#""partitionValues":{"long_part":""}"#,
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

        let cases = [
            (
                "is null",
                "select id from orders where long_part is null",
                vec!["part-00002.parquet", "part-00004.parquet"],
            ),
            (
                "is not null",
                "select id from orders where long_part is not null",
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00003.parquet",
                ],
            ),
        ];

        for (name, sql, expected_paths) in cases {
            let dataframe = ctx.sql(sql).await?;
            let physical_plan = dataframe.create_physical_plan().await?;
            let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
                .indent(true)
                .to_string();
            let mut scans = Vec::new();
            super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

            assert!(
                !plan_display.contains("FilterExec"),
                "{name} unexpectedly kept a residual filter:\n{plan_display}"
            );
            assert_eq!(scans.len(), 1, "{name}: {plan_display}");
            assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_boolean_partition_null_checks_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-boolean-partition-null-checks",
            BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["is_current"]"#,
            &[
                r#""partitionValues":{"is_current":"true"}"#,
                r#""partitionValues":{"is_current":"false"}"#,
                r#""partitionValues":{"is_current":null}"#,
                r#""partitionValues":{"is_current":""}"#,
                r#""partitionValues":{"is_current":"not-a-boolean"}"#,
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

        let cases = [
            (
                "is null",
                "select id from orders where is_current is null",
                vec!["part-00002.parquet", "part-00005.parquet"],
            ),
            (
                "is not null",
                "select id from orders where is_current is not null",
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00003.parquet",
                    "part-00004.parquet",
                ],
            ),
        ];

        for (name, sql, expected_paths) in cases {
            let dataframe = ctx.sql(sql).await?;
            let physical_plan = dataframe.create_physical_plan().await?;
            let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
                .indent(true)
                .to_string();
            let mut scans = Vec::new();
            super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

            assert!(
                !plan_display.contains("FilterExec"),
                "{name} unexpectedly kept a residual filter:\n{plan_display}"
            );
            assert_eq!(scans.len(), 1, "{name}: {plan_display}");
            assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_boolean_partition_shorthand_rewrites_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-boolean-partition-shorthand-rewrites",
            BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["is_current"]"#,
            &[
                r#""partitionValues":{"is_current":"true"}"#,
                r#""partitionValues":{"is_current":"false"}"#,
                r#""partitionValues":{"is_current":null}"#,
                r#""partitionValues":{"is_current":""}"#,
                r#""partitionValues":{"is_current":"not-a-boolean"}"#,
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

        let cases = [
            (
                "shorthand",
                "select id from orders where is_current",
                vec!["part-00000.parquet"],
            ),
            (
                "not shorthand",
                "select id from orders where not is_current",
                vec!["part-00001.parquet"],
            ),
            (
                "equality true rewrite",
                "select id from orders where is_current = true",
                vec!["part-00000.parquet"],
            ),
            (
                "inequality true rewrite",
                "select id from orders where is_current != true",
                vec!["part-00001.parquet"],
            ),
            (
                "reversed equality false rewrite",
                "select id from orders where false = is_current",
                vec!["part-00001.parquet"],
            ),
            (
                "in list rewrite",
                "select id from orders where is_current in (true, false, true)",
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not in list rewrite",
                "select id from orders where is_current not in (true)",
                vec!["part-00001.parquet"],
            ),
        ];

        for (name, sql, expected_paths) in cases {
            let dataframe = ctx.sql(sql).await?;
            let physical_plan = dataframe.create_physical_plan().await?;
            let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
                .indent(true)
                .to_string();
            let mut scans = Vec::new();
            super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

            assert!(
                !plan_display.contains("FilterExec"),
                "{name} unexpectedly kept a residual filter:\n{plan_display}"
            );
            assert_eq!(scans.len(), 1, "{name}: {plan_display}");
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.exact_count,
                1,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_integer_partition_literal_operators_are_exact_metadata_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-integer-partition-equality-membership",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["long_part"]"#,
            &[
                r#""partitionValues":{"long_part":"7"}"#,
                r#""partitionValues":{"long_part":"-1"}"#,
                r#""partitionValues":{"long_part":"20"}"#,
                r#""partitionValues":{"long_part":null}"#,
                r#""partitionValues":{"long_part":""}"#,
                r#""partitionValues":{"long_part":"not-an-integer"}"#,
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

        let cases = [
            (
                "equality",
                "select id from orders where long_part = 7",
                1,
                vec!["part-00000.parquet"],
            ),
            (
                "reversed equality",
                "select id from orders where 7 = long_part",
                1,
                vec!["part-00000.parquet"],
            ),
            (
                "in list",
                "select id from orders where long_part in (7, -1)",
                1,
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "less than",
                "select id from orders where long_part < 7",
                1,
                vec!["part-00001.parquet"],
            ),
            (
                "less than or equal",
                "select id from orders where long_part <= -1",
                1,
                vec!["part-00001.parquet"],
            ),
            (
                "greater than",
                "select id from orders where long_part > -1",
                1,
                vec!["part-00000.parquet", "part-00002.parquet"],
            ),
            (
                "reversed greater than",
                "select id from orders where 7 > long_part",
                1,
                vec!["part-00001.parquet"],
            ),
            (
                "between inclusive",
                "select id from orders where long_part between -1 and 7",
                2,
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not between",
                "select id from orders where long_part not between -1 and 7",
                1,
                vec!["part-00002.parquet"],
            ),
            (
                "contradictory between",
                "select id from orders where long_part between 10 and -10",
                2,
                Vec::<&str>::new(),
            ),
            (
                "contradictory not between",
                "select id from orders where long_part not between 10 and -10",
                1,
                vec![
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00002.parquet",
                ],
            ),
        ];

        for (name, sql, expected_exact_count, expected_paths) in cases {
            let dataframe = ctx.sql(sql).await?;
            let physical_plan = dataframe.create_physical_plan().await?;
            let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
                .indent(true)
                .to_string();
            let mut scans = Vec::new();
            super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

            assert!(
                !plan_display.contains("FilterExec"),
                "{name} unexpectedly kept a residual filter:\n{plan_display}"
            );
            assert_eq!(scans.len(), 1, "{name}: {plan_display}");
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.exact_count,
                expected_exact_count,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");
        }

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
