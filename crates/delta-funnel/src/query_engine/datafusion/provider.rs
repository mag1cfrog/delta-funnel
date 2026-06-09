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
        DeltaKernelPredicate, ProjectedDeltaScan, build_projected_predicated_delta_scan,
        datafusion_expr_to_kernel_predicate, delta_source_arrow_schema,
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
    /// Filter planning and scan planning intentionally stay separate here. Exact
    /// partition filters are converted into a kernel predicate and passed into
    /// the official delta_kernel scan path.
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
        self.reject_projected_inexact_pushed_filters_without_residual_columns(
            &scan_projection,
            &pushed_filter_plan,
        )?;
        let kernel_partition_predicate =
            self.build_kernel_partition_predicate(&pushed_filter_plan)?;
        let kernel_projected_column_names =
            Self::kernel_projected_column_names(projected_column_names, &pushed_filter_plan);
        let kernel_scan = self.build_kernel_scan(
            kernel_projected_column_names.as_deref(),
            kernel_partition_predicate.clone(),
        )?;

        Ok(ProviderScanPlan::from_parts(ProviderScanPlanParts {
            source_name: self.source_name().to_owned(),
            table_uri: self.source.table_uri().to_owned(),
            snapshot_version: self.snapshot_version(),
            projected_schema,
            protocol: self.protocol.clone(),
            scan_projection,
            pushed_filter_plan,
            partition_metadata_filter: None,
            kernel_partition_predicate,
            kernel_scan,
        }))
    }

    /// Applies safe name normalization to provider-boundary filters.
    ///
    /// DataFusion may present relation-qualified expressions to the support
    /// callback and unqualified expressions to `scan`. This helper owns the
    /// normalization step for both entry points before strict partition
    /// pushdown planning or kernel predicate conversion.
    fn normalize_provider_filters(&self, filters: impl IntoIterator<Item = Expr>) -> Vec<Expr> {
        filters
            .into_iter()
            .map(|filter| unqualify_filter_columns(filter, &self.schema))
            .collect()
    }

    /// Plans provider-boundary filters after normalization has been applied.
    ///
    /// Keeping this as a separate step lets scan planning reuse the same
    /// normalized expressions when converting accepted filters to kernel
    /// predicates, so filter classification and kernel conversion cannot see
    /// different column names.
    fn plan_normalized_provider_filters(&self, filters: &[Expr]) -> DeltaFilterPushdownPlan {
        let filter_refs = filters.iter().collect::<Vec<_>>();

        DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &self.schema,
            &self.partition_columns(),
        )
    }

    /// Rejects pushed filters that this provider cannot safely use.
    ///
    /// Exact filters must have a kernel scan filter expression that can be
    /// converted to a kernel predicate. This issue does not accept inexact
    /// pushed filters.
    fn reject_unaccepted_pushed_filters(
        &self,
        pushed_filter_plan: &DeltaFilterPushdownPlan,
    ) -> Result<(), DeltaFunnelError> {
        let missing_partition_expression = pushed_filter_plan.decisions.iter().any(|decision| {
            decision.outcome != DeltaFilterPushdownOutcome::Unsupported
                && decision.kernel_scan_filter.is_none()
        });
        if pushed_filter_plan.unsupported_count > 0
            || pushed_filter_plan.inexact_count > 0
            || missing_partition_expression
        {
            return Err(DeltaFunnelError::DeltaScanFilter {
                source_name: self.source_name().to_owned(),
                table_uri: self.source.table_uri().to_owned(),
                reason: "pushed filters must be exact partition predicates".to_owned(),
            });
        }

        Ok(())
    }

    fn reject_projected_inexact_pushed_filters_without_residual_columns(
        &self,
        scan_projection: &Option<Vec<usize>>,
        pushed_filter_plan: &DeltaFilterPushdownPlan,
    ) -> Result<(), DeltaFunnelError> {
        let Some(scan_projection) = scan_projection else {
            return Ok(());
        };

        let projected_columns = scan_projection
            .iter()
            .map(|index| self.schema.field(*index).name().as_str())
            .collect::<HashSet<_>>();

        let missing_residual_column = pushed_filter_plan
            .decisions
            .iter()
            .filter(|decision| decision.outcome == DeltaFilterPushdownOutcome::Inexact)
            .flat_map(|decision| decision.filter_analysis.referenced_columns.iter())
            .any(|column| !projected_columns.contains(column.as_str()));

        if missing_residual_column {
            return Err(DeltaFunnelError::DeltaScanFilter {
                source_name: self.source_name().to_owned(),
                table_uri: self.source.table_uri().to_owned(),
                reason: "inexact pushed filter residual columns must be projected".to_owned(),
            });
        }

        Ok(())
    }

    /// Builds the kernel partition predicate for accepted exact filters.
    ///
    /// Accepted exact filters must be enforced by the same predicate passed into
    /// `ScanBuilder::with_predicate`; the legacy metadata evaluator is not a
    /// fallback for this migration slice.
    fn build_kernel_partition_predicate(
        &self,
        pushed_filter_plan: &DeltaFilterPushdownPlan,
    ) -> Result<Option<DeltaKernelPredicate>, DeltaFunnelError> {
        let predicates = pushed_filter_plan
            .decisions
            .iter()
            .filter_map(|decision| {
                decision
                    .kernel_scan_filter
                    .as_ref()
                    .map(|filter| (decision, filter))
            })
            .map(|(_decision, kernel_scan_filter)| {
                datafusion_expr_to_kernel_predicate(kernel_scan_filter).map_err(|error| {
                    DeltaFunnelError::DeltaScanFilter {
                        source_name: self.source_name().to_owned(),
                        table_uri: self.source.table_uri().to_owned(),
                        reason: format!(
                            "exact pushed filter cannot be converted to kernel predicate: {error}"
                        ),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(DeltaKernelPredicate::and_from(predicates))
    }

    /// Expands the kernel scan schema with exact predicate columns.
    ///
    /// DataFusion output projection stays governed by `projected_schema` and
    /// `scan_projection`; this only gives delta_kernel enough schema context to
    /// validate and evaluate partition predicates during scan planning.
    fn kernel_projected_column_names(
        projected_column_names: Option<Vec<String>>,
        pushed_filter_plan: &DeltaFilterPushdownPlan,
    ) -> Option<Vec<String>> {
        let mut projected_column_names = projected_column_names?;

        for decision in &pushed_filter_plan.decisions {
            if decision.outcome != DeltaFilterPushdownOutcome::Exact {
                continue;
            }

            for column in &decision.filter_analysis.partition_columns {
                if !projected_column_names.contains(column) {
                    projected_column_names.push(column.clone());
                }
            }
        }

        Some(projected_column_names)
    }

    /// Builds the delta_kernel scan state for a projected provider scan.
    fn build_kernel_scan(
        &self,
        projected_column_names: Option<&[String]>,
        kernel_partition_predicate: Option<DeltaKernelPredicate>,
    ) -> Result<ProjectedDeltaScan, DeltaFunnelError> {
        let kernel_projected_column_names = projected_column_names.map(|names| names.to_vec());

        build_projected_predicated_delta_scan(
            &self.source,
            kernel_projected_column_names.as_deref(),
            kernel_partition_predicate,
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
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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
    const DATE_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"event_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]"#;
    const DECIMAL_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}]"#;
    const HIGH_PRECISION_DECIMAL_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"decimal(38,18)\",\"nullable\":true,\"metadata\":{}}]"#;
    const FLOATING_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"float_part\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"double_part\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]"#;
    const TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"event_ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]"#;
    const BINARY_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"payload\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}]"#;
    const TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"event_ts_ntz\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}]"#;
    const TIMESTAMP_NTZ_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#;

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
        let table = DeltaLogTable::new_with_schema_and_adds(
            "table-provider-exact-partition-filter",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            &[
                r#""partitionValues":{"region":"us-west"}"#,
                r#""partitionValues":{"region":"us-east"}"#,
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
        assert!(scan.scan_plan().partition_metadata_filter.is_none());
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_accepts_exact_partition_in_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "table-provider-exact-partition-in-filter",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            &[
                r#""partitionValues":{"region":"us-west"}"#,
                r#""partitionValues":{"region":"us-east"}"#,
                r#""partitionValues":{"region":"eu-central"}"#,
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
        assert!(scan.scan_plan().partition_metadata_filter.is_none());
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(
            scan_file_paths(scan)?,
            vec!["part-00000.parquet", "part-00001.parquet"]
        );

        Ok(())
    }

    #[tokio::test]
    async fn exact_string_partition_predicates_use_kernel_pruning()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "exact-string-partition-kernel-pruning",
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
                "empty value equality",
                datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("")),
                Vec::new(),
            ),
            (
                "is null",
                datafusion::logical_expr::col("region").is_null(),
                vec![
                    "part-00002.parquet",
                    "part-00003.parquet",
                    "part-00004.parquet",
                ],
            ),
            (
                "is not null",
                datafusion::logical_expr::col("region").is_not_null(),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "inequality non-empty value",
                datafusion::logical_expr::col("region")
                    .not_eq(datafusion::logical_expr::lit("us-west")),
                vec!["part-00001.parquet"],
            ),
            (
                "inequality empty value",
                datafusion::logical_expr::col("region").not_eq(datafusion::logical_expr::lit("")),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "in non-empty values",
                datafusion::logical_expr::col("region").in_list(
                    vec![
                        datafusion::logical_expr::lit("us-west"),
                        datafusion::logical_expr::lit("us-east"),
                        datafusion::logical_expr::lit("us-west"),
                    ],
                    false,
                ),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "in with empty literal",
                datafusion::logical_expr::col("region").in_list(
                    vec![
                        datafusion::logical_expr::lit("us-west"),
                        datafusion::logical_expr::lit(""),
                    ],
                    false,
                ),
                vec!["part-00000.parquet"],
            ),
            (
                "empty in",
                datafusion::logical_expr::col("region").in_list(Vec::<Expr>::new(), false),
                Vec::new(),
            ),
            (
                "not in non-empty values",
                datafusion::logical_expr::col("region").in_list(
                    vec![
                        datafusion::logical_expr::lit("us-west"),
                        datafusion::logical_expr::lit("us-east"),
                    ],
                    true,
                ),
                Vec::new(),
            ),
            (
                "not in with empty literal",
                datafusion::logical_expr::col("region").in_list(
                    vec![
                        datafusion::logical_expr::lit("us-west"),
                        datafusion::logical_expr::lit(""),
                    ],
                    true,
                ),
                vec!["part-00001.parquet"],
            ),
            (
                "empty not in",
                datafusion::logical_expr::col("region").in_list(Vec::<Expr>::new(), true),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "less than",
                datafusion::logical_expr::col("region")
                    .lt(datafusion::logical_expr::lit("us-west")),
                vec!["part-00001.parquet"],
            ),
            (
                "reversed less than",
                datafusion::logical_expr::lit("us-east")
                    .lt(datafusion::logical_expr::col("region")),
                vec!["part-00000.parquet"],
            ),
            (
                "greater than empty string literal",
                datafusion::logical_expr::col("region").gt(datafusion::logical_expr::lit("")),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "less than empty string literal",
                datafusion::logical_expr::col("region").lt(datafusion::logical_expr::lit("")),
                Vec::new(),
            ),
            (
                "between empty and z",
                datafusion::logical_expr::col("region").between(
                    datafusion::logical_expr::lit(""),
                    datafusion::logical_expr::lit("z"),
                ),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not between empty and z",
                datafusion::logical_expr::col("region").not_between(
                    datafusion::logical_expr::lit(""),
                    datafusion::logical_expr::lit("z"),
                ),
                Vec::new(),
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
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not equality wrapper",
                Expr::Not(Box::new(
                    datafusion::logical_expr::col("region")
                        .eq(datafusion::logical_expr::lit("us-west")),
                )),
                vec!["part-00001.parquet"],
            ),
            (
                "not empty equality wrapper",
                Expr::Not(Box::new(
                    datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("")),
                )),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not is null wrapper",
                Expr::Not(Box::new(datafusion::logical_expr::col("region").is_null())),
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not is not null wrapper",
                Expr::Not(Box::new(
                    datafusion::logical_expr::col("region").is_not_null(),
                )),
                vec![
                    "part-00002.parquet",
                    "part-00003.parquet",
                    "part-00004.parquet",
                ],
            ),
            (
                "partition-only or",
                datafusion::logical_expr::col("region")
                    .eq(datafusion::logical_expr::lit("us-west"))
                    .or(datafusion::logical_expr::col("region").is_null()),
                vec![
                    "part-00000.parquet",
                    "part-00002.parquet",
                    "part-00003.parquet",
                    "part-00004.parquet",
                ],
            ),
            (
                "equality terms in top-level and",
                datafusion::logical_expr::col("region")
                    .eq(datafusion::logical_expr::lit("us-west"))
                    .and(
                        datafusion::logical_expr::col("region")
                            .eq(datafusion::logical_expr::lit("us-west")),
                    ),
                vec!["part-00000.parquet"],
            ),
            (
                "null check and equality in top-level and",
                datafusion::logical_expr::col("region").is_not_null().and(
                    datafusion::logical_expr::col("region")
                        .eq(datafusion::logical_expr::lit("us-west")),
                ),
                vec!["part-00000.parquet"],
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
                scan.scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scan.scan_plan().kernel_partition_predicate.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_unsupported_string_partition_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "table-provider-unsupported-string-partition-shapes",
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
        let filters = vec![(
            "non literal not in",
            datafusion::logical_expr::col("region")
                .in_list(vec![datafusion::logical_expr::col("id")], true),
        )];

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
    async fn table_provider_scan_rejects_ambiguous_partition_column_references()
    -> Result<(), Box<dyn std::error::Error>> {
        const DOTTED_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"address.city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

        struct RejectedReferenceProbe {
            name: &'static str,
            schema_fields_json: &'static str,
            partition_columns_json: &'static str,
            add_partition_values_json: &'static str,
            filter: Expr,
        }

        let cases = [
            RejectedReferenceProbe {
                name: "wrong-case partition reference",
                schema_fields_json: PARTITIONED_SCHEMA_FIELDS_JSON,
                partition_columns_json: r#"["region"]"#,
                add_partition_values_json: r#""partitionValues":{"region":"us-west"}"#,
                filter: Expr::Column(datafusion::common::Column::new_unqualified("Region"))
                    .eq(datafusion::logical_expr::lit("us-west")),
            },
            RejectedReferenceProbe {
                name: "dotted partition reference",
                schema_fields_json: DOTTED_PARTITION_SCHEMA_FIELDS_JSON,
                partition_columns_json: r#"["address.city"]"#,
                add_partition_values_json: r#""partitionValues":{"address.city":"Phoenix"}"#,
                filter: datafusion::logical_expr::col("address.city")
                    .eq(datafusion::logical_expr::lit("Phoenix")),
            },
            RejectedReferenceProbe {
                name: "nested data field reference",
                schema_fields_json: NESTED_SCHEMA_FIELDS_JSON,
                partition_columns_json: "[]",
                add_partition_values_json: r#""partitionValues":{}"#,
                filter: datafusion::logical_expr::col("profile.age")
                    .gt(datafusion::logical_expr::lit(21)),
            },
        ];

        for case in cases {
            let table = DeltaLogTable::new_with_schema(
                case.name,
                case.schema_fields_json,
                case.partition_columns_json,
                case.add_partition_values_json,
            )?;
            let source = load_delta_source(DeltaSourceConfig {
                name: "orders".to_owned(),
                table_uri: table.path().to_string_lossy().to_string(),
                version: None,
            })?;
            let preflight = preflight_delta_protocol(&source)?;
            let provider = DeltaTableProvider::try_new(source, preflight)?;
            let state = SessionContext::new().state();

            let result = provider
                .scan(&state, None, std::slice::from_ref(&case.filter), None)
                .await;

            assert!(
                matches!(result, Err(DataFusionError::External(error)) if error
                    .to_string()
                    .contains("pushed filters must be exact partition predicates")),
                "{} should be rejected",
                case.name
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_mixed_partition_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "table-provider-mixed-partition-filter-rejection",
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
    async fn table_provider_scan_rejects_mixed_and_exact_filter_batch()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "table-provider-mixed-and-exact-filter-rejection",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            &[
                r#""partitionValues":{"region":"us-west"}"#,
                r#""partitionValues":{"region":"us-east"}"#,
                r#""partitionValues":{"region":"eu-central"}"#,
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
        let mixed_filter = datafusion::logical_expr::col("region")
            .in_list(
                vec![
                    datafusion::logical_expr::lit("us-west"),
                    datafusion::logical_expr::lit("us-east"),
                ],
                false,
            )
            .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1000)));
        let exact_filter =
            datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us-east"));

        let result = provider
            .scan(&state, None, &[mixed_filter, exact_filter], None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_projected_inexact_mixed_partition_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "table-provider-projected-inexact-mixed-partition-filter",
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
        let projection = vec![0];
        let filter = datafusion::logical_expr::col("region")
            .eq(datafusion::logical_expr::lit("us-west"))
            .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1)));

        let result = provider
            .scan(&state, Some(&projection), &[filter], None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_projected_mixed_partition_filter_when_residual_columns_are_projected()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "table-provider-projected-mixed-partition-filter-rejected",
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
        let projection = vec![0, 1];
        let filter = datafusion::logical_expr::col("region")
            .eq(datafusion::logical_expr::lit("us-west"))
            .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1)));

        let result = provider
            .scan(&state, Some(&projection), &[filter], None)
            .await;

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
    async fn mixed_partition_pruning_keeps_residual_column_below_final_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        const MIXED_FILTER_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema(
            "mixed-partition-pruning-residual-projection",
            MIXED_FILTER_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let probe_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let probe_preflight = preflight_delta_protocol(&probe_source)?;
        let provider = DeltaTableProvider::try_new(probe_source, probe_preflight)?;
        let mixed_filter = datafusion::logical_expr::col("region")
            .eq(datafusion::logical_expr::lit("us-west"))
            .and(
                datafusion::logical_expr::col("customer_name")
                    .eq(datafusion::logical_expr::lit("alice")),
            );

        // Direct support pins the provider's mixed-AND status. DataFusion's
        // optimizer then proves the SQL residual contract below by splitting
        // the top-level AND, pushing the exact partition equality, and keeping
        // the data filter above the scan.
        assert_eq!(
            provider.supports_filters_pushdown(&[&mixed_filter])?,
            vec![TableProviderFilterPushDown::Unsupported]
        );

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

        let optimized_dataframe = ctx
            .sql("select id from orders where region = 'us-west' and customer_name = 'alice'")
            .await?;
        let optimized_plan = optimized_dataframe.into_optimized_plan()?;
        let optimized_display = optimized_plan.display_indent().to_string();

        assert!(optimized_display.contains("Filter:"), "{optimized_display}");
        assert!(
            optimized_display.contains("customer_name"),
            "{optimized_display}"
        );
        assert!(
            optimized_display.contains("full_filters"),
            "{optimized_display}"
        );
        assert!(optimized_display.contains("region"), "{optimized_display}");

        let dataframe = ctx
            .sql("select id from orders where region = 'us-west' and customer_name = 'alice'")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        super::super::test_support::find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(plan_display.contains("FilterExec"), "{plan_display}");
        assert!(plan_display.contains("customer_name"), "{plan_display}");
        assert_eq!(physical_plan.schema().fields().len(), 1);
        assert_eq!(physical_plan.schema().field(0).name(), "id");
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 1]));
        assert_eq!(scans[0].schema().fields().len(), 2);
        assert_eq!(scans[0].schema().field(0).name(), "id");
        assert_eq!(scans[0].schema().field(1).name(), "customer_name");
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.unsupported_count, 0);
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(scans[0].scan_plan().partition_metadata_filter.is_none());
        assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());
        let kernel_names = scans[0]
            .scan_plan()
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(kernel_names, vec!["id", "customer_name", "region"]);

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
        assert!(scans[0].scan_plan().partition_metadata_filter.is_none());
        assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scans[0])?, vec!["part-00000.parquet"]);
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
    async fn sql_partition_in_filter_is_exact_kernel_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-exact-partition-in-filter",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            &[
                r#""partitionValues":{"region":"us-west"}"#,
                r#""partitionValues":{"region":"us-east"}"#,
                r#""partitionValues":{"region":"eu-central"}"#,
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
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            1
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(scans[0].scan_plan().partition_metadata_filter.is_none());
        assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(
            scan_file_paths(scans[0])?,
            vec!["part-00000.parquet", "part-00001.parquet"]
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
    async fn sql_duplicate_and_contradictory_partition_filters_are_exact_kernel_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-duplicate-contradictory-partition-filters",
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

        enum ExpectedSqlPartitionEdge {
            ExactScan {
                exact_count: usize,
                paths: Vec<&'static str>,
            },
            EmptyBeforeScan,
        }

        let sql_cases = [
            (
                "duplicate equality",
                "select id from orders where region = 'us-west' and region = 'us-west'",
                ExpectedSqlPartitionEdge::ExactScan {
                    exact_count: 1,
                    paths: vec!["part-00000.parquet"],
                },
            ),
            (
                "contradictory equality",
                "select id from orders where region = 'us-west' and region = 'us-east'",
                ExpectedSqlPartitionEdge::EmptyBeforeScan,
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
                ExpectedSqlPartitionEdge::ExactScan { exact_count, paths } => {
                    assert!(
                        !plan_display.contains("FilterExec"),
                        "{name} unexpectedly kept a residual filter:\n{plan_display}"
                    );
                    assert_eq!(scans.len(), 1, "{name}: {plan_display}");
                    assert_eq!(
                        scans[0].scan_plan().pushed_filter_plan.exact_count,
                        exact_count,
                        "{name}: {plan_display}"
                    );
                    assert_eq!(
                        scans[0]
                            .scan_plan()
                            .pushed_filter_plan
                            .residual_filter_count,
                        0,
                        "{name}: {plan_display}"
                    );
                    assert!(
                        scans[0].scan_plan().partition_metadata_filter.is_none(),
                        "{name}: {plan_display}"
                    );
                    assert!(
                        scans[0].scan_plan().kernel_partition_predicate.is_some(),
                        "{name}: {plan_display}"
                    );
                    assert_eq!(scan_file_paths(scans[0])?, paths, "{name}");
                }
                ExpectedSqlPartitionEdge::EmptyBeforeScan => {
                    assert!(plan_display.contains("EmptyExec"), "{name}: {plan_display}");
                    assert!(scans.is_empty(), "{name}: {plan_display}");
                }
            }
        }

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
    async fn sql_null_partition_filters_are_exact_kernel_pushdown()
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
                vec![
                    "part-00001.parquet",
                    "part-00002.parquet",
                    "part-00003.parquet",
                ],
            ),
            (
                "is not null",
                "select id from orders where region is not null",
                vec!["part-00000.parquet"],
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
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_some(),
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
            assert_eq!(kernel_names, vec!["id", "region"], "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_negated_partition_filters_follow_supported_kernel_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-negated-partition-filters-boundary",
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

        enum ExpectedNegatedSql {
            ExactKernel {
                exact_count: usize,
                paths: Vec<&'static str>,
            },
        }

        let sql_cases = [
            (
                "not equality",
                "select id from orders where region != 'us-west'",
                ExpectedNegatedSql::ExactKernel {
                    exact_count: 1,
                    paths: vec!["part-00001.parquet"],
                },
            ),
            (
                "not equality expression",
                "select id from orders where not(region = 'us-west')",
                ExpectedNegatedSql::ExactKernel {
                    exact_count: 1,
                    paths: vec!["part-00001.parquet"],
                },
            ),
            (
                "not in",
                "select id from orders where region not in ('us-west', 'us-east')",
                ExpectedNegatedSql::ExactKernel {
                    exact_count: 2,
                    paths: Vec::new(),
                },
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
                ExpectedNegatedSql::ExactKernel { exact_count, paths } => {
                    assert!(
                        !plan_display.contains("FilterExec"),
                        "{name} unexpectedly kept a residual filter:\n{plan_display}"
                    );
                    assert_eq!(scans.len(), 1, "{name}: {plan_display}");
                    assert_eq!(
                        scans[0].scan_plan().pushed_filter_plan.exact_count,
                        exact_count,
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
                        scans[0].scan_plan().partition_metadata_filter.is_none(),
                        "{name}"
                    );
                    assert!(
                        scans[0].scan_plan().kernel_partition_predicate.is_some(),
                        "{name}"
                    );
                    assert_eq!(scan_file_paths(scans[0])?, paths, "{name}");
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_partition_comparison_filters_are_exact_kernel_pushdown()
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
                vec!["part-00001.parquet"],
            ),
            (
                "less than or equal",
                "select id from orders where region <= 'us-east'",
                vec!["part-00001.parquet"],
            ),
            (
                "greater than",
                "select id from orders where region > 'us-east'",
                vec!["part-00000.parquet"],
            ),
            (
                "reversed greater than",
                "select id from orders where 'us-east' < region",
                vec!["part-00000.parquet"],
            ),
            (
                "between",
                "select id from orders where region between 'us-east' and 'us-west'",
                vec!["part-00000.parquet", "part-00001.parquet"],
            ),
            (
                "not between",
                "select id from orders where region not between 'us-east' and 'us-west'",
                Vec::new(),
            ),
            (
                "contradictory between",
                "select id from orders where region between 'z' and 'a'",
                Vec::new(),
            ),
            (
                "contradictory not between",
                "select id from orders where region not between 'z' and 'a'",
                vec!["part-00000.parquet", "part-00001.parquet"],
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
            assert!(
                scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
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
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_some(),
                "{name}"
            );
            assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_empty_string_partition_filters_follow_kernel_boundary()
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

        enum ExpectedEmptyStringSql {
            ExactKernel { paths: Vec<&'static str> },
        }

        let sql_cases = [
            (
                "empty string equality",
                "select id from orders where region = ''",
                ExpectedEmptyStringSql::ExactKernel { paths: Vec::new() },
            ),
            (
                "empty string in",
                "select id from orders where region in ('us-west', '')",
                ExpectedEmptyStringSql::ExactKernel {
                    paths: vec!["part-00000.parquet"],
                },
            ),
            (
                "empty string comparison",
                "select id from orders where region < ''",
                ExpectedEmptyStringSql::ExactKernel { paths: Vec::new() },
            ),
            (
                "empty string between",
                "select id from orders where region between '' and 'us-west'",
                ExpectedEmptyStringSql::ExactKernel {
                    paths: vec!["part-00000.parquet"],
                },
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
                ExpectedEmptyStringSql::ExactKernel { paths } => {
                    assert!(
                        !plan_display.contains("FilterExec"),
                        "{name} unexpectedly kept a residual filter:\n{plan_display}"
                    );
                    assert_eq!(scans.len(), 1, "{name}: {plan_display}");
                    assert!(
                        scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
                        "{name}: {plan_display}"
                    );
                    assert!(
                        scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
                        "{name}: {plan_display}"
                    );
                    assert!(scans[0].scan_plan().partition_metadata_filter.is_none());
                    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());
                    assert_eq!(scan_file_paths(scans[0])?, paths, "{name}");
                }
            }
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

    #[test]
    fn date_partition_schema_maps_delta_type_to_arrow_date32()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "date-partition-schema",
            DATE_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_date"]"#,
            r#""partitionValues":{"event_date":"2026-01-01"}"#,
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
            schema.field_with_name("event_date")?.data_type(),
            &DataType::Date32
        );

        Ok(())
    }

    #[test]
    fn decimal_partition_schema_maps_delta_type_to_arrow_decimal128()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "decimal-partition-schema",
            DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            r#""partitionValues":{"amount":"123.45"}"#,
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
            schema.field_with_name("amount")?.data_type(),
            &DataType::Decimal128(10, 2)
        );

        Ok(())
    }

    #[test]
    fn floating_partition_schema_maps_delta_types_to_arrow_float_widths()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "floating-partition-schema",
            FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["float_part","double_part"]"#,
            r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
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
            schema.field_with_name("float_part")?.data_type(),
            &DataType::Float32
        );
        assert_eq!(
            schema.field_with_name("double_part")?.data_type(),
            &DataType::Float64
        );

        Ok(())
    }

    #[test]
    fn timestamp_partition_schema_maps_delta_type_to_arrow_timestamp_microseconds()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "timestamp-partition-schema",
            TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts"]"#,
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
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
            schema.field_with_name("event_ts")?.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );

        Ok(())
    }

    #[test]
    fn timestamp_ntz_partition_schema_maps_delta_type_to_arrow_timestamp_microseconds_without_timezone()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_protocol_and_adds(
            "timestamp-ntz-partition-schema",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts_ntz"]"#,
            &[r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#],
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
            schema.field_with_name("event_ts_ntz")?.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        Ok(())
    }

    #[test]
    fn binary_partition_schema_maps_delta_type_to_arrow_binary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "binary-partition-schema",
            BINARY_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["payload"]"#,
            r#""partitionValues":{"payload":"hello"}"#,
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
            schema.field_with_name("payload")?.data_type(),
            &DataType::Binary
        );

        Ok(())
    }

    #[tokio::test]
    async fn date_partition_null_checks_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "date-partition-null-checks-boundary",
            DATE_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_date"]"#,
            &[
                r#""partitionValues":{"event_date":"2026-01-01"}"#,
                r#""partitionValues":{"event_date":"1969-12-31"}"#,
                r#""partitionValues":{"event_date":null}"#,
                r#""partitionValues":{"event_date":""}"#,
                r#""partitionValues":{"event_date":"not-a-date"}"#,
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
                datafusion::logical_expr::col("event_date").is_null(),
            ),
            (
                "is not null",
                datafusion::logical_expr::col("event_date").is_not_null(),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn date_partition_equality_and_membership_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "date-partition-equality-membership-boundary",
            DATE_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_date"]"#,
            &[
                r#""partitionValues":{"event_date":"2026-01-01"}"#,
                r#""partitionValues":{"event_date":"2024-02-29"}"#,
                r#""partitionValues":{"event_date":"1969-12-31"}"#,
                r#""partitionValues":{"event_date":null}"#,
                r#""partitionValues":{"event_date":""}"#,
                r#""partitionValues":{"event_date":"not-a-date"}"#,
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
        let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
        let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
        let pre_epoch_day = Expr::Literal(ScalarValue::Date32(Some(-1)), None);
        let cases = [
            (
                "equality",
                datafusion::logical_expr::col("event_date").eq(new_year_2026.clone()),
            ),
            (
                "reversed equality pre epoch",
                pre_epoch_day.eq(datafusion::logical_expr::col("event_date")),
            ),
            (
                "inequality",
                datafusion::logical_expr::col("event_date").not_eq(new_year_2026.clone()),
            ),
            (
                "in list",
                datafusion::logical_expr::col("event_date").in_list(
                    vec![
                        new_year_2026.clone(),
                        leap_day_2024.clone(),
                        new_year_2026.clone(),
                    ],
                    false,
                ),
            ),
            (
                "not in list",
                datafusion::logical_expr::col("event_date")
                    .in_list(vec![new_year_2026.clone()], true),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn date_partition_unsafe_literal_shapes_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "date-partition-unsafe-literal-shapes",
            DATE_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_date"]"#,
            r#""partitionValues":{"event_date":"2026-01-01"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let date = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
        let scalar_udf = create_udf(
            "date_identity_for_pushdown_boundary",
            vec![DataType::Date32],
            DataType::Date32,
            Volatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(20_454))))),
        );
        let scalar_function =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![datafusion::logical_expr::col("event_date")],
            ));
        let filters = vec![
            datafusion::logical_expr::col("event_date")
                .eq(datafusion::logical_expr::lit("2026-01-01")),
            datafusion::logical_expr::col("event_date")
                .eq(Expr::Literal(ScalarValue::Date32(None), None)),
            datafusion::logical_expr::col("event_date").eq(Expr::Literal(
                ScalarValue::Date64(Some(1_767_225_600_000)),
                None,
            )),
            datafusion::logical_expr::col("event_date").in_list(
                vec![date.clone(), Expr::Literal(ScalarValue::Date32(None), None)],
                false,
            ),
            datafusion::logical_expr::col("event_date").in_list(
                vec![date.clone(), datafusion::logical_expr::lit("2024-02-29")],
                false,
            ),
            datafusion::logical_expr::col("event_date")
                .in_list(vec![datafusion::logical_expr::col("id")], false),
            datafusion::logical_expr::col("event_date")
                .between(Expr::Literal(ScalarValue::Date32(None), None), date.clone()),
            datafusion::logical_expr::col("event_date")
                .between(datafusion::logical_expr::col("id"), date.clone()),
            datafusion::logical_expr::col("event_date").eq(datafusion::logical_expr::cast(
                date.clone(),
                DataType::Date32,
            )),
            datafusion::logical_expr::col("event_date").eq(scalar_function),
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

        for filter in filters {
            let result = provider
                .scan(&state, None, std::slice::from_ref(&filter), None)
                .await;

            assert!(
                matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates"))
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn decimal_partition_unsafe_literal_filters_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "decimal-partition-unsafe-literal-boundary",
            DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            r#""partitionValues":{"amount":"123.45"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let non_exact_scale = Expr::Literal(ScalarValue::Decimal128(Some(12_346), 10, 3), None);
        let scalar_udf = create_udf(
            "decimal_identity_for_pushdown_boundary",
            vec![DataType::Decimal128(10, 2)],
            DataType::Decimal128(10, 2),
            Volatility::Immutable,
            Arc::new(|_| {
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(12_345),
                    10,
                    2,
                )))
            }),
        );
        let scalar_function =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![datafusion::logical_expr::col("amount")],
            ));
        let filters = vec![
            (
                "non exact scale equality",
                datafusion::logical_expr::col("amount").eq(non_exact_scale.clone()),
            ),
            (
                "non exact scale ordering",
                datafusion::logical_expr::col("amount").gt(non_exact_scale.clone()),
            ),
            (
                "non exact scale in list",
                datafusion::logical_expr::col("amount")
                    .in_list(vec![amount.clone(), non_exact_scale.clone()], false),
            ),
            (
                "non exact scale between",
                datafusion::logical_expr::col("amount").between(
                    Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None),
                    non_exact_scale.clone(),
                ),
            ),
            (
                "string equality",
                datafusion::logical_expr::col("amount").eq(datafusion::logical_expr::lit("123.45")),
            ),
            (
                "integer equality",
                datafusion::logical_expr::col("amount").eq(datafusion::logical_expr::lit(123_i64)),
            ),
            (
                "float equality",
                datafusion::logical_expr::col("amount")
                    .eq(datafusion::logical_expr::lit(123.45_f64)),
            ),
            (
                "null equality",
                datafusion::logical_expr::col("amount")
                    .eq(Expr::Literal(ScalarValue::Decimal128(None, 10, 2), None)),
            ),
            (
                "cast operand",
                datafusion::logical_expr::col("amount").eq(datafusion::logical_expr::cast(
                    amount.clone(),
                    DataType::Decimal128(10, 2),
                )),
            ),
            (
                "scalar function operand",
                datafusion::logical_expr::col("amount").eq(scalar_function),
            ),
        ];
        let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

        let support = provider.supports_filters_pushdown(&filter_refs)?;
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());

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
    async fn timestamp_partition_unsafe_filters_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "timestamp-partition-unsafe-boundary",
            TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts"]"#,
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let timestamp_utc = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_600_123_456),
                Some(Arc::<str>::from("UTC")),
            ),
            None,
        );
        let timestamp_non_utc_timezone = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_600_123_456),
                Some(Arc::<str>::from("America/Phoenix")),
            ),
            None,
        );
        let timestamp_null = Expr::Literal(
            ScalarValue::TimestampMicrosecond(None, Some(Arc::<str>::from("UTC"))),
            None,
        );
        let scalar_udf = create_udf(
            "timestamp_identity_for_pushdown_boundary",
            vec![DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )],
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Volatility::Immutable,
            Arc::new(|_| {
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(1_767_225_600_123_456),
                    Some(Arc::<str>::from("UTC")),
                )))
            }),
        );
        let scalar_function =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![datafusion::logical_expr::col("event_ts")],
            ));
        let filters = vec![
            (
                "timestamp null in list",
                datafusion::logical_expr::col("event_ts").in_list(vec![timestamp_null], false),
            ),
            (
                "timestamp non utc timezone literal",
                datafusion::logical_expr::col("event_ts").eq(timestamp_non_utc_timezone),
            ),
            (
                "timestamp null literal",
                datafusion::logical_expr::col("event_ts").eq(Expr::Literal(
                    ScalarValue::TimestampMicrosecond(None, Some(Arc::<str>::from("UTC"))),
                    None,
                )),
            ),
            (
                "cast operand",
                datafusion::logical_expr::col("event_ts").eq(datafusion::logical_expr::cast(
                    timestamp_utc.clone(),
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                )),
            ),
            (
                "scalar function operand",
                datafusion::logical_expr::col("event_ts").eq(scalar_function),
            ),
            (
                "mixed partition data equality",
                datafusion::logical_expr::col("event_ts").eq(datafusion::logical_expr::col("id")),
            ),
        ];
        let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

        let support = provider.supports_filters_pushdown(&filter_refs)?;
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());

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
    async fn binary_partition_null_checks_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "binary-partition-null-checks-boundary",
            BINARY_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["payload"]"#,
            &[
                r#""partitionValues":{"payload":"hello"}"#,
                r#""partitionValues":{"payload":"world"}"#,
                r#""partitionValues":{"payload":null}"#,
                r#""partitionValues":{"payload":""}"#,
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
                datafusion::logical_expr::col("payload").is_null(),
            ),
            (
                "is not null",
                datafusion::logical_expr::col("payload").is_not_null(),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn binary_partition_equality_and_membership_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "binary-partition-equality-membership-boundary",
            BINARY_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["payload"]"#,
            &[
                r#""partitionValues":{"payload":"hello"}"#,
                r#""partitionValues":{"payload":"world"}"#,
                r#""partitionValues":{"payload":"/=%"}"#,
                r#""partitionValues":{"payload":null}"#,
                r#""partitionValues":{"payload":""}"#,
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
        let hello = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
        let world = Expr::Literal(ScalarValue::Binary(Some(b"world".to_vec())), None);
        let slash_equals_percent = Expr::Literal(ScalarValue::Binary(Some(b"/=%".to_vec())), None);
        let cases = [
            (
                "equality",
                datafusion::logical_expr::col("payload").eq(hello.clone()),
            ),
            (
                "reversed equality",
                slash_equals_percent
                    .clone()
                    .eq(datafusion::logical_expr::col("payload")),
            ),
            (
                "inequality",
                datafusion::logical_expr::col("payload").not_eq(hello.clone()),
            ),
            (
                "in list",
                datafusion::logical_expr::col("payload").in_list(
                    vec![hello.clone(), slash_equals_percent.clone(), hello.clone()],
                    false,
                ),
            ),
            (
                "not in list",
                datafusion::logical_expr::col("payload").in_list(vec![world], true),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn binary_partition_boolean_composition_and_projection_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "binary-partition-boolean-composition-boundary",
            BINARY_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["payload"]"#,
            &[
                r#""partitionValues":{"payload":"hello"}"#,
                r#""partitionValues":{"payload":"world"}"#,
                r#""partitionValues":{"payload":"/=%"}"#,
                r#""partitionValues":{"payload":null}"#,
                r#""partitionValues":{"payload":""}"#,
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
        let hello = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
        let world = Expr::Literal(ScalarValue::Binary(Some(b"world".to_vec())), None);
        let slash_equals_percent = Expr::Literal(ScalarValue::Binary(Some(b"/=%".to_vec())), None);
        let separate_and_filters = vec![
            datafusion::logical_expr::col("payload").is_not_null(),
            datafusion::logical_expr::col("payload").not_eq(world.clone()),
        ];
        let whole_and_filter = datafusion::logical_expr::col("payload")
            .in_list(
                vec![hello.clone(), slash_equals_percent.clone(), hello.clone()],
                false,
            )
            .and(datafusion::logical_expr::col("payload").is_not_null());
        let whole_or_filter = datafusion::logical_expr::col("payload")
            .eq(hello.clone())
            .or(datafusion::logical_expr::col("payload").is_null());
        let whole_not_filter =
            Expr::Not(Box::new(datafusion::logical_expr::col("payload").eq(hello)));
        let cases = [
            ("separate filters combine with and", separate_and_filters),
            ("whole and", vec![whole_and_filter]),
            ("whole or", vec![whole_or_filter]),
            ("whole not", vec![whole_not_filter]),
        ];

        for (name, filters) in cases {
            let filter_refs = filters.iter().collect::<Vec<_>>();
            let support = provider.supports_filters_pushdown(&filter_refs)?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported; filters.len()],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &filters, None).await;

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
    async fn binary_partition_unsafe_filters_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "binary-partition-unsupported-boundary",
            BINARY_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["payload"]"#,
            r#""partitionValues":{"payload":"hello"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let payload = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
        let payload_null = Expr::Literal(ScalarValue::Binary(None), None);
        let scalar_udf = create_udf(
            "binary_identity_for_pushdown_boundary",
            vec![DataType::Binary],
            DataType::Binary,
            Volatility::Immutable,
            Arc::new(|_| {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(
                    b"hello".to_vec(),
                ))))
            }),
        );
        let scalar_function =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![datafusion::logical_expr::col("payload")],
            ));
        let filters = vec![
            (
                "binary ordering",
                datafusion::logical_expr::col("payload").gt(payload.clone()),
            ),
            (
                "binary between",
                datafusion::logical_expr::col("payload").between(payload.clone(), payload.clone()),
            ),
            (
                "binary null literal",
                datafusion::logical_expr::col("payload").eq(payload_null),
            ),
            (
                "binary empty literal",
                datafusion::logical_expr::col("payload")
                    .eq(Expr::Literal(ScalarValue::Binary(Some(Vec::new())), None)),
            ),
            (
                "binary empty literal in list",
                datafusion::logical_expr::col("payload").in_list(
                    vec![
                        payload.clone(),
                        Expr::Literal(ScalarValue::Binary(Some(Vec::new())), None),
                    ],
                    false,
                ),
            ),
            (
                "string literal",
                datafusion::logical_expr::col("payload").eq(datafusion::logical_expr::lit("hello")),
            ),
            (
                "cast operand",
                datafusion::logical_expr::col("payload").eq(datafusion::logical_expr::cast(
                    payload.clone(),
                    DataType::Binary,
                )),
            ),
            (
                "scalar function operand",
                datafusion::logical_expr::col("payload").eq(scalar_function),
            ),
            (
                "mixed partition data equality",
                datafusion::logical_expr::col("payload").eq(datafusion::logical_expr::col("id")),
            ),
        ];
        let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

        let support = provider.supports_filters_pushdown(&filter_refs)?;
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());

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
    async fn timestamp_ntz_partition_unsafe_filters_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_protocol_and_adds(
            "timestamp-ntz-partition-unsafe-boundary",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts_ntz"]"#,
            &[r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let timestamp_ntz = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
            None,
        );
        let timestamp_utc = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_600_123_456),
                Some(Arc::<str>::from("UTC")),
            ),
            None,
        );
        let timestamp_null = Expr::Literal(ScalarValue::TimestampMicrosecond(None, None), None);
        let scalar_udf = create_udf(
            "timestamp_ntz_identity_for_pushdown_boundary",
            vec![DataType::Timestamp(TimeUnit::Microsecond, None)],
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Volatility::Immutable,
            Arc::new(|_| {
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(1_767_225_600_123_456),
                    None,
                )))
            }),
        );
        let scalar_function =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![datafusion::logical_expr::col("event_ts_ntz")],
            ));
        let filters = vec![
            (
                "timestamp_ntz null in list",
                datafusion::logical_expr::col("event_ts_ntz")
                    .in_list(vec![timestamp_null.clone()], false),
            ),
            (
                "timestamp_ntz utc timezone literal",
                datafusion::logical_expr::col("event_ts_ntz").eq(timestamp_utc),
            ),
            (
                "timestamp_ntz null literal",
                datafusion::logical_expr::col("event_ts_ntz").eq(timestamp_null),
            ),
            (
                "cast operand",
                datafusion::logical_expr::col("event_ts_ntz").eq(datafusion::logical_expr::cast(
                    timestamp_ntz.clone(),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                )),
            ),
            (
                "scalar function operand",
                datafusion::logical_expr::col("event_ts_ntz").eq(scalar_function),
            ),
            (
                "mixed partition data equality",
                datafusion::logical_expr::col("event_ts_ntz")
                    .eq(datafusion::logical_expr::col("id")),
            ),
        ];
        let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

        let support = provider.supports_filters_pushdown(&filter_refs)?;
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());

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
    async fn timestamp_partition_equality_and_membership_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "timestamp-partition-equality-membership-boundary",
            TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts"]"#,
            &[
                r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
                r#""partitionValues":{"event_ts":"2026-01-01 00:00:00.123456"}"#,
                r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
                r#""partitionValues":{"event_ts":null}"#,
                r#""partitionValues":{"event_ts":""}"#,
                r#""partitionValues":{"event_ts":"not-a-timestamp"}"#,
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
        let timestamp = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_600_123_456),
                Some(Arc::<str>::from("UTC")),
            ),
            None,
        );
        let low = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_599_999_999),
                Some(Arc::<str>::from("UTC")),
            ),
            None,
        );
        let cases = [
            (
                "timestamp equality",
                datafusion::logical_expr::col("event_ts").eq(timestamp.clone()),
            ),
            (
                "reversed timestamp equality",
                timestamp
                    .clone()
                    .eq(datafusion::logical_expr::col("event_ts")),
            ),
            (
                "timestamp inequality",
                datafusion::logical_expr::col("event_ts").not_eq(timestamp.clone()),
            ),
            (
                "timestamp in list",
                datafusion::logical_expr::col("event_ts").in_list(
                    vec![timestamp.clone(), low.clone(), timestamp.clone()],
                    false,
                ),
            ),
            (
                "timestamp not in list",
                datafusion::logical_expr::col("event_ts").in_list(vec![timestamp], true),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn timestamp_ntz_partition_equality_and_membership_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_protocol_and_adds(
            "timestamp-ntz-partition-equality-membership-boundary",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts_ntz"]"#,
            &[
                r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
                r#""partitionValues":{"event_ts_ntz":"2026-01-01T00:00:00.123456Z"}"#,
                r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
                r#""partitionValues":{"event_ts_ntz":null}"#,
                r#""partitionValues":{"event_ts_ntz":""}"#,
                r#""partitionValues":{"event_ts_ntz":"not-a-timestamp"}"#,
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
        let timestamp = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
            None,
        );
        let low = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_599_999_999), None),
            None,
        );
        let cases = [
            (
                "timestamp_ntz equality",
                datafusion::logical_expr::col("event_ts_ntz").eq(timestamp.clone()),
            ),
            (
                "reversed timestamp_ntz equality",
                timestamp
                    .clone()
                    .eq(datafusion::logical_expr::col("event_ts_ntz")),
            ),
            (
                "timestamp_ntz inequality",
                datafusion::logical_expr::col("event_ts_ntz").not_eq(timestamp.clone()),
            ),
            (
                "timestamp_ntz in list",
                datafusion::logical_expr::col("event_ts_ntz").in_list(
                    vec![timestamp.clone(), low.clone(), timestamp.clone()],
                    false,
                ),
            ),
            (
                "timestamp_ntz not in list",
                datafusion::logical_expr::col("event_ts_ntz").in_list(vec![timestamp], true),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn timestamp_partition_comparisons_and_between_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "timestamp-partition-comparisons-between-boundary",
            TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts"]"#,
            &[
                r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
                r#""partitionValues":{"event_ts":"2026-01-01 00:00:00.123456"}"#,
                r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
                r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123457Z"}"#,
                r#""partitionValues":{"event_ts":null}"#,
                r#""partitionValues":{"event_ts":""}"#,
                r#""partitionValues":{"event_ts":"not-a-timestamp"}"#,
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
        let target = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_600_123_456),
                Some(Arc::<str>::from("UTC")),
            ),
            None,
        );
        let low = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_599_999_999),
                Some(Arc::<str>::from("UTC")),
            ),
            None,
        );
        let high = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_600_123_457),
                Some(Arc::<str>::from("UTC")),
            ),
            None,
        );
        let cases = [
            (
                "timestamp less than",
                datafusion::logical_expr::col("event_ts").lt(target.clone()),
            ),
            (
                "timestamp less than or equal",
                datafusion::logical_expr::col("event_ts").lt_eq(low.clone()),
            ),
            (
                "timestamp greater than",
                datafusion::logical_expr::col("event_ts").gt(low.clone()),
            ),
            (
                "reversed timestamp greater than",
                high.clone().gt(datafusion::logical_expr::col("event_ts")),
            ),
            (
                "timestamp greater than or equal",
                datafusion::logical_expr::col("event_ts").gt_eq(target.clone()),
            ),
            (
                "timestamp between",
                datafusion::logical_expr::col("event_ts").between(low.clone(), target.clone()),
            ),
            (
                "timestamp not between",
                datafusion::logical_expr::col("event_ts").not_between(low.clone(), target.clone()),
            ),
            (
                "timestamp contradictory between",
                datafusion::logical_expr::col("event_ts").between(high.clone(), low.clone()),
            ),
            (
                "timestamp contradictory not between",
                datafusion::logical_expr::col("event_ts").not_between(high, low.clone()),
            ),
            (
                "timestamp and composition",
                datafusion::logical_expr::col("event_ts")
                    .gt(low)
                    .and(datafusion::logical_expr::col("event_ts").lt_eq(target)),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn timestamp_ntz_partition_comparisons_and_between_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_protocol_and_adds(
            "timestamp-ntz-partition-comparisons-between-boundary",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts_ntz"]"#,
            &[
                r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
                r#""partitionValues":{"event_ts_ntz":"2026-01-01T00:00:00.123456Z"}"#,
                r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
                r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123457"}"#,
                r#""partitionValues":{"event_ts_ntz":null}"#,
                r#""partitionValues":{"event_ts_ntz":""}"#,
                r#""partitionValues":{"event_ts_ntz":"not-a-timestamp"}"#,
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
        let target = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
            None,
        );
        let low = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_599_999_999), None),
            None,
        );
        let high = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_457), None),
            None,
        );
        let cases = [
            (
                "timestamp_ntz less than",
                datafusion::logical_expr::col("event_ts_ntz").lt(target.clone()),
            ),
            (
                "timestamp_ntz less than or equal",
                datafusion::logical_expr::col("event_ts_ntz").lt_eq(low.clone()),
            ),
            (
                "timestamp_ntz greater than",
                datafusion::logical_expr::col("event_ts_ntz").gt(low.clone()),
            ),
            (
                "reversed timestamp_ntz greater than",
                high.clone()
                    .gt(datafusion::logical_expr::col("event_ts_ntz")),
            ),
            (
                "timestamp_ntz greater than or equal",
                datafusion::logical_expr::col("event_ts_ntz").gt_eq(target.clone()),
            ),
            (
                "timestamp_ntz between",
                datafusion::logical_expr::col("event_ts_ntz").between(low.clone(), target.clone()),
            ),
            (
                "timestamp_ntz not between",
                datafusion::logical_expr::col("event_ts_ntz")
                    .not_between(low.clone(), target.clone()),
            ),
            (
                "timestamp_ntz contradictory between",
                datafusion::logical_expr::col("event_ts_ntz").between(high.clone(), low.clone()),
            ),
            (
                "timestamp_ntz contradictory not between",
                datafusion::logical_expr::col("event_ts_ntz").not_between(high, low.clone()),
            ),
            (
                "timestamp_ntz and composition",
                datafusion::logical_expr::col("event_ts_ntz")
                    .gt(low)
                    .and(datafusion::logical_expr::col("event_ts_ntz").lt_eq(target)),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn timestamp_partition_boolean_composition_and_projection_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "timestamp-partition-boolean-composition-boundary",
            TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts"]"#,
            &[
                r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
                r#""partitionValues":{"event_ts":"2026-01-01 00:00:00.123456"}"#,
                r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
                r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123457Z"}"#,
                r#""partitionValues":{"event_ts":null}"#,
                r#""partitionValues":{"event_ts":""}"#,
                r#""partitionValues":{"event_ts":"not-a-timestamp"}"#,
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
        let target = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_600_123_456),
                Some(Arc::<str>::from("UTC")),
            ),
            None,
        );
        let low = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_599_999_999),
                Some(Arc::<str>::from("UTC")),
            ),
            None,
        );
        let high = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_600_123_457),
                Some(Arc::<str>::from("UTC")),
            ),
            None,
        );
        let separate_and_filters = vec![
            datafusion::logical_expr::col("event_ts").gt_eq(low.clone()),
            datafusion::logical_expr::col("event_ts").lt(high.clone()),
        ];
        let whole_and_filter = datafusion::logical_expr::col("event_ts")
            .gt_eq(low.clone())
            .and(datafusion::logical_expr::col("event_ts").lt(high.clone()));
        let whole_or_filter = datafusion::logical_expr::col("event_ts")
            .eq(target.clone())
            .or(datafusion::logical_expr::col("event_ts").eq(high.clone()));
        let whole_not_filter = Expr::Not(Box::new(
            datafusion::logical_expr::col("event_ts").eq(target),
        ));
        let cases = [
            ("separate filters combine with and", separate_and_filters),
            ("whole and", vec![whole_and_filter]),
            ("whole or", vec![whole_or_filter]),
            ("whole not", vec![whole_not_filter]),
        ];

        for (name, filters) in cases {
            let filter_refs = filters.iter().collect::<Vec<_>>();
            let support = provider.supports_filters_pushdown(&filter_refs)?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported; filters.len()],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &filters, None).await;

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
    async fn timestamp_partition_null_checks_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "timestamp-partition-null-checks-boundary",
            TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts"]"#,
            &[
                r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
                r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
                r#""partitionValues":{"event_ts":null}"#,
                r#""partitionValues":{"event_ts":""}"#,
                r#""partitionValues":{"event_ts":"not-a-timestamp"}"#,
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
                "timestamp is null",
                datafusion::logical_expr::col("event_ts").is_null(),
            ),
            (
                "timestamp is not null",
                datafusion::logical_expr::col("event_ts").is_not_null(),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn timestamp_ntz_partition_null_checks_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_protocol_and_adds(
            "timestamp-ntz-partition-null-checks-boundary",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts_ntz"]"#,
            &[
                r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
                r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
                r#""partitionValues":{"event_ts_ntz":null}"#,
                r#""partitionValues":{"event_ts_ntz":""}"#,
                r#""partitionValues":{"event_ts_ntz":"not-a-timestamp"}"#,
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
                "timestamp_ntz is null",
                datafusion::logical_expr::col("event_ts_ntz").is_null(),
            ),
            (
                "timestamp_ntz is not null",
                datafusion::logical_expr::col("event_ts_ntz").is_not_null(),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn sql_timestamp_partition_comparisons_and_between_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-timestamp-partition-comparisons-between-residuals",
            TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts"]"#,
            &[
                r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
                r#""partitionValues":{"event_ts":"2026-01-01 00:00:00.123456"}"#,
                r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
                r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123457Z"}"#,
                r#""partitionValues":{"event_ts":null}"#,
                r#""partitionValues":{"event_ts":""}"#,
                r#""partitionValues":{"event_ts":"not-a-timestamp"}"#,
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
                "timestamp less than",
                "select id from orders where event_ts < timestamp '2026-01-01 00:00:00.123456'",
            ),
            (
                "timestamp less than or equal",
                "select id from orders where event_ts <= timestamp '2025-12-31 23:59:59.999999'",
            ),
            (
                "timestamp greater than",
                "select id from orders where event_ts > timestamp '2025-12-31 23:59:59.999999'",
            ),
            (
                "timestamp greater than or equal",
                "select id from orders where event_ts >= timestamp '2026-01-01 00:00:00.123456'",
            ),
            (
                "timestamp between inclusive",
                "select id from orders where event_ts between timestamp '2025-12-31 23:59:59.999999' and timestamp '2026-01-01 00:00:00.123456'",
            ),
            (
                "timestamp not between",
                "select id from orders where event_ts not between timestamp '2025-12-31 23:59:59.999999' and timestamp '2026-01-01 00:00:00.123456'",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_timestamp_ntz_partition_comparisons_and_between_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_protocol_and_adds(
            "sql-timestamp-ntz-partition-comparisons-between-residuals",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts_ntz"]"#,
            &[
                r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
                r#""partitionValues":{"event_ts_ntz":"2026-01-01T00:00:00.123456Z"}"#,
                r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
                r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123457"}"#,
                r#""partitionValues":{"event_ts_ntz":null}"#,
                r#""partitionValues":{"event_ts_ntz":""}"#,
                r#""partitionValues":{"event_ts_ntz":"not-a-timestamp"}"#,
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
                "timestamp_ntz less than",
                "select id from orders where event_ts_ntz < timestamp '2026-01-01 00:00:00.123456'",
            ),
            (
                "timestamp_ntz less than or equal",
                "select id from orders where event_ts_ntz <= timestamp '2025-12-31 23:59:59.999999'",
            ),
            (
                "timestamp_ntz greater than",
                "select id from orders where event_ts_ntz > timestamp '2025-12-31 23:59:59.999999'",
            ),
            (
                "timestamp_ntz greater than or equal",
                "select id from orders where event_ts_ntz >= timestamp '2026-01-01 00:00:00.123456'",
            ),
            (
                "timestamp_ntz between inclusive",
                "select id from orders where event_ts_ntz between timestamp '2025-12-31 23:59:59.999999' and timestamp '2026-01-01 00:00:00.123456'",
            ),
            (
                "timestamp_ntz not between",
                "select id from orders where event_ts_ntz not between timestamp '2025-12-31 23:59:59.999999' and timestamp '2026-01-01 00:00:00.123456'",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_timestamp_partition_equality_and_membership_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-timestamp-partition-equality-membership-residuals",
            TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts"]"#,
            &[
                r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
                r#""partitionValues":{"event_ts":"2026-01-01 00:00:00.123456"}"#,
                r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
                r#""partitionValues":{"event_ts":null}"#,
                r#""partitionValues":{"event_ts":""}"#,
                r#""partitionValues":{"event_ts":"not-a-timestamp"}"#,
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
                "timestamp equality",
                "select id from orders where event_ts = timestamp '2026-01-01 00:00:00.123456'",
            ),
            (
                "timestamp inequality",
                "select id from orders where event_ts != timestamp '2026-01-01 00:00:00.123456'",
            ),
            (
                "timestamp in list",
                "select id from orders where event_ts in (timestamp '2026-01-01 00:00:00.123456', timestamp '2025-12-31 23:59:59.999999')",
            ),
            (
                "timestamp not in list",
                "select id from orders where event_ts not in (timestamp '2026-01-01 00:00:00.123456')",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_timestamp_ntz_partition_equality_and_membership_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_protocol_and_adds(
            "sql-timestamp-ntz-partition-equality-membership-residuals",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts_ntz"]"#,
            &[
                r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
                r#""partitionValues":{"event_ts_ntz":"2026-01-01T00:00:00.123456Z"}"#,
                r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
                r#""partitionValues":{"event_ts_ntz":null}"#,
                r#""partitionValues":{"event_ts_ntz":""}"#,
                r#""partitionValues":{"event_ts_ntz":"not-a-timestamp"}"#,
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
                "timestamp_ntz equality",
                "select id from orders where event_ts_ntz = timestamp '2026-01-01 00:00:00.123456'",
            ),
            (
                "timestamp_ntz inequality",
                "select id from orders where event_ts_ntz != timestamp '2026-01-01 00:00:00.123456'",
            ),
            (
                "timestamp_ntz in list",
                "select id from orders where event_ts_ntz in (timestamp '2026-01-01 00:00:00.123456', timestamp '2025-12-31 23:59:59.999999')",
            ),
            (
                "timestamp_ntz not in list",
                "select id from orders where event_ts_ntz not in (timestamp '2026-01-01 00:00:00.123456')",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_timestamp_partition_null_checks_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-timestamp-partition-null-checks-residuals",
            TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts"]"#,
            &[
                r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
                r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
                r#""partitionValues":{"event_ts":null}"#,
                r#""partitionValues":{"event_ts":""}"#,
                r#""partitionValues":{"event_ts":"not-a-timestamp"}"#,
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
                "timestamp is null",
                "select id from orders where event_ts is null",
            ),
            (
                "timestamp is not null",
                "select id from orders where event_ts is not null",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_timestamp_ntz_partition_null_checks_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_protocol_and_adds(
            "sql-timestamp-ntz-partition-null-checks-residuals",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_ts_ntz"]"#,
            &[
                r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
                r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
                r#""partitionValues":{"event_ts_ntz":null}"#,
                r#""partitionValues":{"event_ts_ntz":""}"#,
                r#""partitionValues":{"event_ts_ntz":"not-a-timestamp"}"#,
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
                "timestamp_ntz is null",
                "select id from orders where event_ts_ntz is null",
            ),
            (
                "timestamp_ntz is not null",
                "select id from orders where event_ts_ntz is not null",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn floating_partition_value_filters_remain_unsupported_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "floating-partition-unsupported-boundary",
            FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["float_part","double_part"]"#,
            r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
        let float_nan = Expr::Literal(ScalarValue::Float32(Some(f32::NAN)), None);
        let float_infinity = Expr::Literal(ScalarValue::Float32(Some(f32::INFINITY)), None);
        let float_null = Expr::Literal(ScalarValue::Float32(None), None);
        let float_low = Expr::Literal(ScalarValue::Float32(Some(0.5)), None);
        let double_value = Expr::Literal(ScalarValue::Float64(Some(-2.25)), None);
        let double_nan = Expr::Literal(ScalarValue::Float64(Some(f64::NAN)), None);
        let double_infinity = Expr::Literal(ScalarValue::Float64(Some(f64::INFINITY)), None);
        let double_low = Expr::Literal(ScalarValue::Float64(Some(-3.0)), None);
        let scalar_udf = create_udf(
            "floating_identity_for_pushdown_boundary",
            vec![DataType::Float32],
            DataType::Float32,
            Volatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(1.5))))),
        );
        let scalar_function =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![datafusion::logical_expr::col("float_part")],
            ));
        let filters = vec![
            (
                "float nan equality",
                datafusion::logical_expr::col("float_part").eq(float_nan.clone()),
            ),
            (
                "float infinity equality",
                datafusion::logical_expr::col("float_part").eq(float_infinity.clone()),
            ),
            (
                "float null equality",
                datafusion::logical_expr::col("float_part").eq(float_null.clone()),
            ),
            (
                "float width mismatch",
                datafusion::logical_expr::col("float_part")
                    .eq(Expr::Literal(ScalarValue::Float64(Some(1.5)), None)),
            ),
            (
                "float nan in list",
                datafusion::logical_expr::col("float_part")
                    .in_list(vec![float_value.clone(), float_nan.clone()], false),
            ),
            (
                "float null in list",
                datafusion::logical_expr::col("float_part")
                    .in_list(vec![float_value.clone(), float_null.clone()], false),
            ),
            (
                "float nan ordering",
                datafusion::logical_expr::col("float_part").lt(float_nan.clone()),
            ),
            (
                "float infinity ordering",
                datafusion::logical_expr::col("float_part").gt(float_infinity),
            ),
            (
                "float nan between",
                datafusion::logical_expr::col("float_part")
                    .between(float_low.clone(), float_nan.clone()),
            ),
            (
                "float null between",
                datafusion::logical_expr::col("float_part")
                    .not_between(float_low, float_null.clone()),
            ),
            (
                "double nan equality",
                datafusion::logical_expr::col("double_part").eq(double_nan.clone()),
            ),
            (
                "double infinity equality",
                datafusion::logical_expr::col("double_part").eq(double_infinity.clone()),
            ),
            (
                "double nan in list",
                datafusion::logical_expr::col("double_part")
                    .in_list(vec![double_value.clone(), double_nan.clone()], false),
            ),
            (
                "double nan ordering",
                datafusion::logical_expr::col("double_part").lt(double_nan),
            ),
            (
                "double infinity between",
                datafusion::logical_expr::col("double_part")
                    .between(double_low.clone(), double_infinity.clone()),
            ),
            (
                "double wrong width between",
                datafusion::logical_expr::col("double_part")
                    .not_between(double_low, float_value.clone()),
            ),
            (
                "cast operand",
                datafusion::logical_expr::col("float_part").eq(datafusion::logical_expr::cast(
                    float_value.clone(),
                    DataType::Float32,
                )),
            ),
            (
                "scalar function operand",
                datafusion::logical_expr::col("float_part").eq(scalar_function),
            ),
            (
                "mixed partition data equality",
                datafusion::logical_expr::col("float_part").eq(datafusion::logical_expr::col("id")),
            ),
            (
                "and composition",
                datafusion::logical_expr::col("float_part")
                    .lt(float_null)
                    .and(datafusion::logical_expr::col("double_part").eq(double_value.clone())),
            ),
            (
                "or composition",
                datafusion::logical_expr::col("float_part")
                    .eq(float_nan)
                    .or(datafusion::logical_expr::col("double_part").gt(double_infinity)),
            ),
        ];
        let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

        let support = provider.supports_filters_pushdown(&filter_refs)?;
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());

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
    async fn floating_partition_equality_and_membership_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "floating-partition-equality-membership-boundary",
            FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["float_part","double_part"]"#,
            &[
                r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
                r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
                r#""partitionValues":{"float_part":"0.0","double_part":"-0.0"}"#,
                r#""partitionValues":{"float_part":null,"double_part":null}"#,
                r#""partitionValues":{"float_part":"","double_part":"not-a-double"}"#,
                r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
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
        let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
        let negative_zero_float = Expr::Literal(ScalarValue::Float32(Some(-0.0)), None);
        let positive_zero_float = Expr::Literal(ScalarValue::Float32(Some(0.0)), None);
        let double_value = Expr::Literal(ScalarValue::Float64(Some(-2.25)), None);
        let cases = [
            (
                "float equality",
                datafusion::logical_expr::col("float_part").eq(float_value.clone()),
            ),
            (
                "float reversed equality negative zero",
                negative_zero_float
                    .clone()
                    .eq(datafusion::logical_expr::col("float_part")),
            ),
            (
                "float positive zero does not match negative zero",
                datafusion::logical_expr::col("float_part").eq(positive_zero_float.clone()),
            ),
            (
                "float inequality",
                datafusion::logical_expr::col("float_part").not_eq(float_value.clone()),
            ),
            (
                "float in list",
                datafusion::logical_expr::col("float_part").in_list(
                    vec![float_value.clone(), negative_zero_float.clone()],
                    false,
                ),
            ),
            (
                "float not in list",
                datafusion::logical_expr::col("float_part")
                    .in_list(vec![float_value.clone()], true),
            ),
            (
                "double equality",
                datafusion::logical_expr::col("double_part").eq(double_value.clone()),
            ),
            (
                "double not in list",
                datafusion::logical_expr::col("double_part").in_list(vec![double_value], true),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn floating_partition_comparisons_and_between_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "floating-partition-comparisons-between-boundary",
            FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["float_part","double_part"]"#,
            &[
                r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
                r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
                r#""partitionValues":{"float_part":"0.0","double_part":"1.0"}"#,
                r#""partitionValues":{"float_part":null,"double_part":null}"#,
                r#""partitionValues":{"float_part":"","double_part":"not-a-double"}"#,
                r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
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
        let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
        let negative_zero_float = Expr::Literal(ScalarValue::Float32(Some(-0.0)), None);
        let positive_zero_float = Expr::Literal(ScalarValue::Float32(Some(0.0)), None);
        let double_value = Expr::Literal(ScalarValue::Float64(Some(-2.25)), None);
        let double_high = Expr::Literal(ScalarValue::Float64(Some(0.0)), None);
        let cases = [
            (
                "float less than",
                datafusion::logical_expr::col("float_part").lt(float_value.clone()),
            ),
            (
                "float less than or equal negative zero",
                datafusion::logical_expr::col("float_part").lt_eq(negative_zero_float.clone()),
            ),
            (
                "float greater than negative zero",
                datafusion::logical_expr::col("float_part").gt(negative_zero_float.clone()),
            ),
            (
                "reversed float greater than or equal",
                float_value
                    .clone()
                    .lt_eq(datafusion::logical_expr::col("float_part")),
            ),
            (
                "float between includes signed zero order",
                datafusion::logical_expr::col("float_part")
                    .between(negative_zero_float.clone(), float_value.clone()),
            ),
            (
                "float not between",
                datafusion::logical_expr::col("float_part")
                    .not_between(positive_zero_float, float_value),
            ),
            (
                "double between",
                datafusion::logical_expr::col("double_part")
                    .between(double_value.clone(), double_high.clone()),
            ),
            (
                "double not between",
                datafusion::logical_expr::col("double_part").not_between(double_value, double_high),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn floating_partition_boolean_composition_and_projection_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "floating-partition-boolean-composition-boundary",
            FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["float_part","double_part"]"#,
            &[
                r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
                r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
                r#""partitionValues":{"float_part":"0.0","double_part":"1.0"}"#,
                r#""partitionValues":{"float_part":"3.0","double_part":"4.0"}"#,
                r#""partitionValues":{"float_part":null,"double_part":null}"#,
                r#""partitionValues":{"float_part":"","double_part":"not-a-double"}"#,
                r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
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
        let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
        let negative_zero_float = Expr::Literal(ScalarValue::Float32(Some(-0.0)), None);
        let double_other = Expr::Literal(ScalarValue::Float64(Some(1.0)), None);
        let separate_and_filters = vec![
            datafusion::logical_expr::col("float_part").gt_eq(negative_zero_float.clone()),
            datafusion::logical_expr::col("float_part")
                .lt(Expr::Literal(ScalarValue::Float32(Some(3.0)), None)),
        ];
        let whole_and_filter = datafusion::logical_expr::col("float_part")
            .gt_eq(negative_zero_float.clone())
            .and(
                datafusion::logical_expr::col("float_part")
                    .lt(Expr::Literal(ScalarValue::Float32(Some(3.0)), None)),
            );
        let whole_or_filter = datafusion::logical_expr::col("float_part")
            .eq(float_value.clone())
            .or(datafusion::logical_expr::col("double_part").eq(double_other));
        let whole_not_filter = Expr::Not(Box::new(
            datafusion::logical_expr::col("float_part").eq(float_value),
        ));
        let cases = [
            ("separate filters combine with and", separate_and_filters),
            ("whole and", vec![whole_and_filter]),
            ("whole or", vec![whole_or_filter]),
            ("whole not", vec![whole_not_filter]),
        ];

        for (name, filters) in cases {
            let filter_refs = filters.iter().collect::<Vec<_>>();
            let support = provider.supports_filters_pushdown(&filter_refs)?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported; filters.len()],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &filters, None).await;

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
    async fn floating_partition_null_checks_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "floating-partition-null-checks-boundary",
            FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["float_part","double_part"]"#,
            &[
                r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
                r#""partitionValues":{"float_part":null,"double_part":"-2.25"}"#,
                r#""partitionValues":{"float_part":"1.5","double_part":null}"#,
                r#""partitionValues":{"float_part":null,"double_part":null}"#,
                r#""partitionValues":{"float_part":"","double_part":"not-a-double"}"#,
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
                "float is null",
                datafusion::logical_expr::col("float_part").is_null(),
            ),
            (
                "float is not null",
                datafusion::logical_expr::col("float_part").is_not_null(),
            ),
            (
                "double is null",
                datafusion::logical_expr::col("double_part").is_null(),
            ),
            (
                "double is not null",
                datafusion::logical_expr::col("double_part").is_not_null(),
            ),
            (
                "and composition",
                datafusion::logical_expr::col("float_part")
                    .is_null()
                    .and(datafusion::logical_expr::col("double_part").is_null()),
            ),
            (
                "or composition",
                datafusion::logical_expr::col("float_part")
                    .is_null()
                    .or(datafusion::logical_expr::col("double_part").is_null()),
            ),
            (
                "not composition",
                Expr::Not(Box::new(
                    datafusion::logical_expr::col("float_part").is_null(),
                )),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn sql_floating_partition_null_checks_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-floating-partition-null-checks-residuals",
            FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["float_part","double_part"]"#,
            &[
                r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
                r#""partitionValues":{"float_part":null,"double_part":"-2.25"}"#,
                r#""partitionValues":{"float_part":"1.5","double_part":null}"#,
                r#""partitionValues":{"float_part":null,"double_part":null}"#,
                r#""partitionValues":{"float_part":"","double_part":"not-a-double"}"#,
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
                "float is null",
                "select id from orders where float_part is null",
            ),
            (
                "double is not null",
                "select id from orders where double_part is not null",
            ),
            (
                "null check or",
                "select id from orders where float_part is null or double_part is null",
            ),
            (
                "not null check",
                "select id from orders where not(float_part is null)",
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
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.exact_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_floating_partition_equality_and_membership_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-floating-partition-equality-membership-residuals",
            FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["float_part","double_part"]"#,
            &[
                r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
                r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
                r#""partitionValues":{"float_part":"0.0","double_part":"-0.0"}"#,
                r#""partitionValues":{"float_part":null,"double_part":null}"#,
                r#""partitionValues":{"float_part":"","double_part":"not-a-double"}"#,
                r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
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
                "float equality",
                "select id from orders where float_part = cast(1.5 as float)",
            ),
            (
                "float in list",
                "select id from orders where float_part in (cast(1.5 as float), cast(-0.0 as float))",
            ),
            (
                "float inequality",
                "select id from orders where float_part != cast(1.5 as float)",
            ),
            (
                "double equality",
                "select id from orders where double_part = -2.25",
            ),
            (
                "double not in",
                "select id from orders where double_part not in (-2.25)",
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
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.exact_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_floating_partition_comparisons_and_between_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-floating-partition-comparisons-between-residuals",
            FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["float_part","double_part"]"#,
            &[
                r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
                r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
                r#""partitionValues":{"float_part":"0.0","double_part":"1.0"}"#,
                r#""partitionValues":{"float_part":null,"double_part":null}"#,
                r#""partitionValues":{"float_part":"","double_part":"not-a-double"}"#,
                r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
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
                "float less than",
                "select id from orders where float_part < cast(1.5 as float)",
            ),
            (
                "float between",
                "select id from orders where float_part between cast(-0.0 as float) and cast(1.5 as float)",
            ),
            (
                "float not between",
                "select id from orders where float_part not between cast(0.0 as float) and cast(1.5 as float)",
            ),
            (
                "double between",
                "select id from orders where double_part between -2.25 and 0.0",
            ),
            (
                "double not between",
                "select id from orders where double_part not between -2.25 and 0.0",
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
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.exact_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_floating_partition_unsafe_literal_filters_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-floating-partition-unsafe-literal-residuals",
            FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["float_part","double_part"]"#,
            &[
                r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
                r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
                r#""partitionValues":{"float_part":"0.0","double_part":"1.0"}"#,
                r#""partitionValues":{"float_part":null,"double_part":null}"#,
                r#""partitionValues":{"float_part":"","double_part":"not-a-double"}"#,
                r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
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
                "nan equality",
                "select id from orders where float_part = cast('NaN' as float)",
            ),
            (
                "infinity ordering",
                "select id from orders where double_part > cast('Infinity' as double)",
            ),
            (
                "null in list",
                "select id from orders where float_part in (cast(1.5 as float), cast(null as float))",
            ),
            (
                "wrong width equality",
                "select id from orders where float_part = cast(1.5 as double)",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn decimal_partition_comparisons_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "decimal-partition-comparisons-boundary",
            DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            &[
                r#""partitionValues":{"amount":"123.45"}"#,
                r#""partitionValues":{"amount":"0.00"}"#,
                r#""partitionValues":{"amount":"-1.23"}"#,
                r#""partitionValues":{"amount":null}"#,
                r#""partitionValues":{"amount":""}"#,
                r#""partitionValues":{"amount":"not-a-decimal"}"#,
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
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let negative_different_scale =
            Expr::Literal(ScalarValue::Decimal128(Some(-1_230), 12, 3), None);
        let cases = [
            (
                "less than",
                datafusion::logical_expr::col("amount").lt(amount.clone()),
            ),
            (
                "less than or equal",
                datafusion::logical_expr::col("amount").lt_eq(negative_different_scale),
            ),
            (
                "greater than",
                datafusion::logical_expr::col("amount").gt(zero.clone()),
            ),
            (
                "reversed greater than or equal",
                amount.lt_eq(datafusion::logical_expr::col("amount")),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn decimal_partition_between_filters_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "decimal-partition-between-boundary",
            DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            &[
                r#""partitionValues":{"amount":"123.45"}"#,
                r#""partitionValues":{"amount":"0.00"}"#,
                r#""partitionValues":{"amount":"-1.23"}"#,
                r#""partitionValues":{"amount":null}"#,
                r#""partitionValues":{"amount":""}"#,
                r#""partitionValues":{"amount":"not-a-decimal"}"#,
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
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let cases = [
            (
                "between inclusive",
                datafusion::logical_expr::col("amount").between(zero.clone(), amount.clone()),
            ),
            (
                "not between",
                datafusion::logical_expr::col("amount").not_between(zero, amount),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn decimal_partition_boolean_composition_and_projection_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "decimal-partition-boolean-composition-boundary",
            DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            &[
                r#""partitionValues":{"amount":"123.45"}"#,
                r#""partitionValues":{"amount":"0.00"}"#,
                r#""partitionValues":{"amount":"-1.23"}"#,
                r#""partitionValues":{"amount":null}"#,
                r#""partitionValues":{"amount":""}"#,
                r#""partitionValues":{"amount":"not-a-decimal"}"#,
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
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let negative = Expr::Literal(ScalarValue::Decimal128(Some(-123), 10, 2), None);
        let separate_and_filters = vec![
            datafusion::logical_expr::col("amount").gt_eq(zero.clone()),
            datafusion::logical_expr::col("amount").lt(amount.clone()),
        ];
        let whole_and_filter = datafusion::logical_expr::col("amount")
            .gt_eq(zero.clone())
            .and(datafusion::logical_expr::col("amount").lt(amount.clone()));
        let whole_or_filter = datafusion::logical_expr::col("amount")
            .eq(amount.clone())
            .or(datafusion::logical_expr::col("amount").eq(negative));
        let whole_not_filter =
            Expr::Not(Box::new(datafusion::logical_expr::col("amount").eq(amount)));
        let cases = [
            ("separate filters combine with and", separate_and_filters),
            ("whole and", vec![whole_and_filter]),
            ("whole or", vec![whole_or_filter]),
            ("whole not", vec![whole_not_filter]),
        ];

        for (name, filters) in cases {
            let filter_refs = filters.iter().collect::<Vec<_>>();
            let support = provider.supports_filters_pushdown(&filter_refs)?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported; filters.len()],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &filters, None).await;

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
    async fn decimal_partition_high_precision_values_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "decimal-partition-high-precision-boundary",
            HIGH_PRECISION_DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            &[
                r#""partitionValues":{"amount":"1.230000000000000000"}"#,
                r#""partitionValues":{"amount":"12345678901234567890.123456789012345678"}"#,
                r#""partitionValues":{"amount":"-1.230000000000000000"}"#,
                r#""partitionValues":{"amount":null}"#,
                r#""partitionValues":{"amount":"not-a-decimal"}"#,
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
        let small_amount = Expr::Literal(
            ScalarValue::Decimal128(Some(1_230_000_000_000_000_000), 38, 18),
            None,
        );
        let large_amount = Expr::Literal(
            ScalarValue::Decimal128(
                Some(12_345_678_901_234_567_890_123_456_789_012_345_678),
                38,
                18,
            ),
            None,
        );
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 38, 18), None);
        let cases = [
            (
                "high precision equality",
                datafusion::logical_expr::col("amount").eq(large_amount.clone()),
            ),
            (
                "high precision ordering",
                datafusion::logical_expr::col("amount").gt(zero.clone()),
            ),
            (
                "high precision between",
                datafusion::logical_expr::col("amount").between(zero, large_amount),
            ),
            (
                "high precision not in",
                datafusion::logical_expr::col("amount").in_list(vec![small_amount], true),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn decimal_partition_exponent_metadata_is_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "decimal-partition-exponent-metadata-boundary",
            HIGH_PRECISION_DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            &[
                r#""partitionValues":{"amount":"0E-18"}"#,
                r#""partitionValues":{"amount":"1.23E-16"}"#,
                r#""partitionValues":{"amount":"not-a-decimal"}"#,
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
        let tiny_amount = Expr::Literal(ScalarValue::Decimal128(Some(123), 38, 18), None);
        let filter = datafusion::logical_expr::col("amount").eq(tiny_amount);

        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Unsupported]);

        let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn decimal_partition_equality_and_membership_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "decimal-partition-equality-membership-boundary",
            DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            &[
                r#""partitionValues":{"amount":"123.45"}"#,
                r#""partitionValues":{"amount":"0.00"}"#,
                r#""partitionValues":{"amount":"-1.23"}"#,
                r#""partitionValues":{"amount":null}"#,
                r#""partitionValues":{"amount":""}"#,
                r#""partitionValues":{"amount":"not-a-decimal"}"#,
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
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let negative_different_scale =
            Expr::Literal(ScalarValue::Decimal128(Some(-1_230), 12, 3), None);
        let same_amount_different_scale =
            Expr::Literal(ScalarValue::Decimal128(Some(123_450), 12, 3), None);
        let cases = [
            (
                "equality",
                datafusion::logical_expr::col("amount").eq(amount.clone()),
            ),
            (
                "reversed equality different scale",
                negative_different_scale.eq(datafusion::logical_expr::col("amount")),
            ),
            (
                "inequality",
                datafusion::logical_expr::col("amount").not_eq(amount.clone()),
            ),
            (
                "in list",
                datafusion::logical_expr::col("amount").in_list(
                    vec![amount.clone(), zero.clone(), same_amount_different_scale],
                    false,
                ),
            ),
            (
                "not in list",
                datafusion::logical_expr::col("amount").in_list(vec![amount.clone()], true),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn decimal_partition_null_checks_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "decimal-partition-null-checks-boundary",
            DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            &[
                r#""partitionValues":{"amount":"123.45"}"#,
                r#""partitionValues":{"amount":"0.00"}"#,
                r#""partitionValues":{"amount":"-1.23"}"#,
                r#""partitionValues":{"amount":null}"#,
                r#""partitionValues":{"amount":""}"#,
                r#""partitionValues":{"amount":"not-a-decimal"}"#,
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
            ("is null", datafusion::logical_expr::col("amount").is_null()),
            (
                "is not null",
                datafusion::logical_expr::col("amount").is_not_null(),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn date_partition_comparisons_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "date-partition-comparisons-boundary",
            DATE_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_date"]"#,
            &[
                r#""partitionValues":{"event_date":"2026-01-01"}"#,
                r#""partitionValues":{"event_date":"2024-02-29"}"#,
                r#""partitionValues":{"event_date":"1969-12-31"}"#,
                r#""partitionValues":{"event_date":null}"#,
                r#""partitionValues":{"event_date":""}"#,
                r#""partitionValues":{"event_date":"not-a-date"}"#,
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
        let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
        let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
        let pre_epoch_day = Expr::Literal(ScalarValue::Date32(Some(-1)), None);
        let cases = [
            (
                "less than",
                datafusion::logical_expr::col("event_date").lt(new_year_2026.clone()),
            ),
            (
                "less than or equal",
                datafusion::logical_expr::col("event_date").lt_eq(pre_epoch_day),
            ),
            (
                "greater than",
                datafusion::logical_expr::col("event_date").gt(leap_day_2024),
            ),
            (
                "greater than or equal",
                datafusion::logical_expr::col("event_date").gt_eq(new_year_2026.clone()),
            ),
            (
                "reversed less than",
                new_year_2026.gt(datafusion::logical_expr::col("event_date")),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn date_partition_between_filters_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "date-partition-between-boundary",
            DATE_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_date"]"#,
            &[
                r#""partitionValues":{"event_date":"2026-01-01"}"#,
                r#""partitionValues":{"event_date":"2024-02-29"}"#,
                r#""partitionValues":{"event_date":"1969-12-31"}"#,
                r#""partitionValues":{"event_date":null}"#,
                r#""partitionValues":{"event_date":""}"#,
                r#""partitionValues":{"event_date":"not-a-date"}"#,
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
        let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
        let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
        let cases = [
            (
                "between inclusive",
                datafusion::logical_expr::col("event_date")
                    .between(leap_day_2024.clone(), new_year_2026.clone()),
            ),
            (
                "not between",
                datafusion::logical_expr::col("event_date")
                    .not_between(leap_day_2024.clone(), new_year_2026.clone()),
            ),
            (
                "contradictory between",
                datafusion::logical_expr::col("event_date")
                    .between(new_year_2026.clone(), leap_day_2024.clone()),
            ),
            (
                "contradictory not between",
                datafusion::logical_expr::col("event_date")
                    .not_between(new_year_2026.clone(), leap_day_2024.clone()),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn date_partition_boolean_composition_and_projection_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "date-partition-boolean-composition-boundary",
            DATE_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_date"]"#,
            &[
                r#""partitionValues":{"event_date":"2026-01-01"}"#,
                r#""partitionValues":{"event_date":"2024-02-29"}"#,
                r#""partitionValues":{"event_date":"1969-12-31"}"#,
                r#""partitionValues":{"event_date":null}"#,
                r#""partitionValues":{"event_date":""}"#,
                r#""partitionValues":{"event_date":"not-a-date"}"#,
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
        let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
        let next_day = Expr::Literal(ScalarValue::Date32(Some(20_455)), None);
        let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
        let pre_epoch_day = Expr::Literal(ScalarValue::Date32(Some(-1)), None);
        let separate_and_filters = vec![
            datafusion::logical_expr::col("event_date").gt_eq(leap_day_2024.clone()),
            datafusion::logical_expr::col("event_date").lt(next_day.clone()),
        ];
        let whole_and_filter = datafusion::logical_expr::col("event_date")
            .gt_eq(leap_day_2024.clone())
            .and(datafusion::logical_expr::col("event_date").lt(next_day));
        let whole_or_filter = datafusion::logical_expr::col("event_date")
            .eq(new_year_2026.clone())
            .or(datafusion::logical_expr::col("event_date").eq(pre_epoch_day));
        let whole_not_filter = Expr::Not(Box::new(
            datafusion::logical_expr::col("event_date").eq(new_year_2026),
        ));
        let cases = [
            ("separate filters combine with and", separate_and_filters),
            ("whole and", vec![whole_and_filter]),
            ("whole or", vec![whole_or_filter]),
            ("whole not", vec![whole_not_filter]),
        ];

        for (name, filters) in cases {
            let filter_refs = filters.iter().collect::<Vec<_>>();
            let support = provider.supports_filters_pushdown(&filter_refs)?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported; filters.len()],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &filters, None).await;

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
    async fn boolean_partition_null_checks_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "boolean-partition-null-checks-boundary",
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
            ),
            (
                "is not null",
                datafusion::logical_expr::col("is_current").is_not_null(),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn boolean_partition_equality_and_membership_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "boolean-partition-equality-membership-boundary",
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
            ),
            (
                "reversed equality false",
                datafusion::logical_expr::lit(false)
                    .eq(datafusion::logical_expr::col("is_current")),
            ),
            (
                "inequality",
                datafusion::logical_expr::col("is_current")
                    .not_eq(datafusion::logical_expr::lit(true)),
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
            ),
            (
                "not in list",
                datafusion::logical_expr::col("is_current")
                    .in_list(vec![datafusion::logical_expr::lit(true)], true),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn boolean_partition_multiple_filters_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "boolean-partition-multiple-filter-boundary",
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
        let filters = vec![
            datafusion::logical_expr::col("is_current").is_not_null(),
            datafusion::logical_expr::col("is_current").eq(datafusion::logical_expr::lit(true)),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );

        let result = provider.scan(&state, Some(&vec![0]), &filters, None).await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn boolean_partition_shorthand_is_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "boolean-partition-shorthand-boundary",
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
            ("shorthand", datafusion::logical_expr::col("is_current")),
            (
                "not shorthand",
                Expr::Not(Box::new(datafusion::logical_expr::col("is_current"))),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn integer_partition_between_filters_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "integer-partition-between-boundary",
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
            ),
            (
                "not between",
                datafusion::logical_expr::col("long_part").not_between(
                    datafusion::logical_expr::lit(-1_i64),
                    datafusion::logical_expr::lit(7_i64),
                ),
            ),
            (
                "contradictory between",
                datafusion::logical_expr::col("long_part").between(
                    datafusion::logical_expr::lit(10_i64),
                    datafusion::logical_expr::lit(-10_i64),
                ),
            ),
            (
                "contradictory not between",
                datafusion::logical_expr::col("long_part").not_between(
                    datafusion::logical_expr::lit(10_i64),
                    datafusion::logical_expr::lit(-10_i64),
                ),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn integer_partition_boolean_composition_and_projection_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "integer-partition-boolean-composition-boundary",
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
            ("separate filters combine with and", separate_and_filters),
            ("whole and", vec![whole_and_filter]),
            ("whole or", vec![whole_or_filter]),
            ("whole not", vec![whole_not_filter]),
        ];

        for (name, filters) in cases {
            let filter_refs = filters.iter().collect::<Vec<_>>();
            let support = provider.supports_filters_pushdown(&filter_refs)?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported; filters.len()],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &filters, None).await;

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
    async fn integer_partition_comparisons_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "integer-partition-comparisons-boundary",
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
            ),
            (
                "less than or equal",
                datafusion::logical_expr::col("long_part")
                    .lt_eq(datafusion::logical_expr::lit(-1_i64)),
            ),
            (
                "greater than",
                datafusion::logical_expr::col("long_part")
                    .gt(datafusion::logical_expr::lit(-1_i64)),
            ),
            (
                "greater than or equal",
                datafusion::logical_expr::col("long_part")
                    .gt_eq(datafusion::logical_expr::lit(7_i64)),
            ),
            (
                "reversed less than",
                datafusion::logical_expr::lit(7_i64).gt(datafusion::logical_expr::col("long_part")),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn integer_partition_equality_and_membership_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "integer-partition-equality-membership-boundary",
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
            ),
            (
                "reversed equality",
                datafusion::logical_expr::lit(7_i64).eq(datafusion::logical_expr::col("long_part")),
            ),
            (
                "inequality",
                datafusion::logical_expr::col("long_part")
                    .not_eq(datafusion::logical_expr::lit(7_i64)),
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
            ),
            (
                "not in list",
                datafusion::logical_expr::col("long_part")
                    .in_list(vec![datafusion::logical_expr::lit(7_i64)], true),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    fn integer_partition_width_bounds_remain_unsupported_for_direct_filters()
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
        let in_range_filters = [
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
        let in_range_refs = in_range_filters.iter().collect::<Vec<_>>();
        let unsupported_refs = unsupported_filters.iter().collect::<Vec<_>>();

        let in_range_plan = provider.plan_supports_filters_pushdown(&in_range_refs);
        let unsupported_plan = provider.plan_supports_filters_pushdown(&unsupported_refs);

        assert_eq!(in_range_plan.exact_count, 0);
        assert_eq!(in_range_plan.unsupported_count, in_range_filters.len());
        assert_eq!(unsupported_plan.exact_count, 0);
        assert_eq!(
            unsupported_plan.unsupported_count,
            unsupported_filters.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn integer_partition_null_checks_are_rejected_at_scan_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "integer-partition-null-checks-boundary",
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
            ),
            (
                "is not null",
                datafusion::logical_expr::col("long_part").is_not_null(),
            ),
        ];

        for (name, filter) in cases {
            let support = provider.supports_filters_pushdown(&[&filter])?;
            assert_eq!(
                support,
                vec![TableProviderFilterPushDown::Unsupported],
                "{name}"
            );

            let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

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
    async fn sql_integer_partition_null_checks_keep_residual_filter()
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
            ("is null", "select id from orders where long_part is null"),
            (
                "is not null",
                "select id from orders where long_part is not null",
            ),
        ];

        for (name, sql) in cases {
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
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_boolean_partition_null_checks_keep_residual_filter()
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
            ("is null", "select id from orders where is_current is null"),
            (
                "is not null",
                "select id from orders where is_current is not null",
            ),
        ];

        for (name, sql) in cases {
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
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_binary_partition_null_checks_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-binary-partition-null-checks",
            BINARY_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["payload"]"#,
            &[
                r#""partitionValues":{"payload":"hello"}"#,
                r#""partitionValues":{"payload":"world"}"#,
                r#""partitionValues":{"payload":null}"#,
                r#""partitionValues":{"payload":""}"#,
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
            ("is null", "select id from orders where payload is null"),
            (
                "is not null",
                "select id from orders where payload is not null",
            ),
        ];

        for (name, sql) in cases {
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
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_binary_partition_equality_and_membership_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-binary-partition-equality-membership",
            BINARY_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["payload"]"#,
            &[
                r#""partitionValues":{"payload":"hello"}"#,
                r#""partitionValues":{"payload":"world"}"#,
                r#""partitionValues":{"payload":"/=%"}"#,
                r#""partitionValues":{"payload":null}"#,
                r#""partitionValues":{"payload":""}"#,
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
                "binary literal equality",
                "select id from orders where payload = X'68656C6C6F'",
            ),
            (
                "reversed binary literal equality",
                "select id from orders where X'2F3D25' = payload",
            ),
            (
                "binary literal inequality",
                "select id from orders where payload != X'68656C6C6F'",
            ),
            (
                "binary literal in list",
                "select id from orders where payload in (X'68656C6C6F', X'2F3D25', X'68656C6C6F')",
            ),
            (
                "binary literal not in list",
                "select id from orders where payload not in (X'776F726C64')",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
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
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_date_partition_equality_and_membership_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-date-partition-equality-membership",
            DATE_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_date"]"#,
            &[
                r#""partitionValues":{"event_date":"2026-01-01"}"#,
                r#""partitionValues":{"event_date":"2024-02-29"}"#,
                r#""partitionValues":{"event_date":"1969-12-31"}"#,
                r#""partitionValues":{"event_date":null}"#,
                r#""partitionValues":{"event_date":""}"#,
                r#""partitionValues":{"event_date":"not-a-date"}"#,
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
                "date literal equality",
                "select id from orders where event_date = DATE '2026-01-01'",
            ),
            (
                "string literal equality coerced by datafusion",
                "select id from orders where event_date = '2026-01-01'",
            ),
            (
                "reversed date literal equality pre epoch",
                "select id from orders where DATE '1969-12-31' = event_date",
            ),
            (
                "date literal inequality",
                "select id from orders where event_date != DATE '2026-01-01'",
            ),
            (
                "date literal in list",
                "select id from orders where event_date in (DATE '2026-01-01', DATE '2024-02-29', DATE '2026-01-01')",
            ),
            (
                "date literal not in list",
                "select id from orders where event_date not in (DATE '2026-01-01')",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
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
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_decimal_partition_unsafe_literal_filters_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-decimal-partition-unsafe-literal-residuals",
            DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            &[
                r#""partitionValues":{"amount":"123.45"}"#,
                r#""partitionValues":{"amount":"0.00"}"#,
                r#""partitionValues":{"amount":"-1.23"}"#,
                r#""partitionValues":{"amount":null}"#,
                r#""partitionValues":{"amount":""}"#,
                r#""partitionValues":{"amount":"not-a-decimal"}"#,
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

        let cases = [(
            "string literal equality casts column to utf8",
            "select id from orders where amount = '123.45'",
        )];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
                "{name}: {plan_display}"
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_decimal_partition_comparisons_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-decimal-partition-comparisons-residuals",
            DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            &[
                r#""partitionValues":{"amount":"123.45"}"#,
                r#""partitionValues":{"amount":"0.00"}"#,
                r#""partitionValues":{"amount":"-1.23"}"#,
                r#""partitionValues":{"amount":null}"#,
                r#""partitionValues":{"amount":""}"#,
                r#""partitionValues":{"amount":"not-a-decimal"}"#,
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
                "decimal literal ordering",
                "select id from orders where amount < DECIMAL '123.45'",
            ),
            (
                "numeric literal ordering",
                "select id from orders where amount > 0.00",
            ),
            (
                "reversed decimal literal ordering different scale",
                "select id from orders where DECIMAL '-1.230' >= amount",
            ),
            (
                "decimal literal between",
                "select id from orders where amount between DECIMAL '0.00' and DECIMAL '123.45'",
            ),
            (
                "decimal literal not between",
                "select id from orders where amount not between DECIMAL '0.00' and DECIMAL '123.45'",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
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
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_decimal_partition_equality_and_membership_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-decimal-partition-equality-membership-residuals",
            DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            &[
                r#""partitionValues":{"amount":"123.45"}"#,
                r#""partitionValues":{"amount":"0.00"}"#,
                r#""partitionValues":{"amount":"-1.23"}"#,
                r#""partitionValues":{"amount":null}"#,
                r#""partitionValues":{"amount":""}"#,
                r#""partitionValues":{"amount":"not-a-decimal"}"#,
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
                "decimal literal equality",
                "select id from orders where amount = DECIMAL '123.45'",
            ),
            (
                "numeric literal equality",
                "select id from orders where amount = 123.45",
            ),
            (
                "reversed decimal literal equality different scale",
                "select id from orders where DECIMAL '-1.230' = amount",
            ),
            (
                "decimal literal inequality",
                "select id from orders where amount != DECIMAL '123.45'",
            ),
            (
                "decimal literal in list",
                "select id from orders where amount in (DECIMAL '123.45', DECIMAL '0.00', DECIMAL '123.450')",
            ),
            (
                "decimal literal not in list",
                "select id from orders where amount not in (DECIMAL '123.45')",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
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
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_decimal_partition_null_checks_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-decimal-partition-null-checks-residuals",
            DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["amount"]"#,
            &[
                r#""partitionValues":{"amount":"123.45"}"#,
                r#""partitionValues":{"amount":"0.00"}"#,
                r#""partitionValues":{"amount":"-1.23"}"#,
                r#""partitionValues":{"amount":null}"#,
                r#""partitionValues":{"amount":""}"#,
                r#""partitionValues":{"amount":"not-a-decimal"}"#,
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
            ("is null", "select id from orders where amount is null"),
            (
                "is not null",
                "select id from orders where amount is not null",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
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
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_date_partition_range_filters_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-date-partition-range-filters",
            DATE_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_date"]"#,
            &[
                r#""partitionValues":{"event_date":"2026-01-01"}"#,
                r#""partitionValues":{"event_date":"2024-02-29"}"#,
                r#""partitionValues":{"event_date":"1969-12-31"}"#,
                r#""partitionValues":{"event_date":"2026-01-02"}"#,
                r#""partitionValues":{"event_date":null}"#,
                r#""partitionValues":{"event_date":""}"#,
                r#""partitionValues":{"event_date":"not-a-date"}"#,
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
                "date literal ordering",
                "select id from orders where event_date < DATE '2026-01-02'",
            ),
            (
                "reversed date literal ordering",
                "select id from orders where DATE '2026-01-01' > event_date",
            ),
            (
                "date literal between",
                "select id from orders where event_date between DATE '2026-01-01' and DATE '2026-01-02'",
            ),
            (
                "date literal not between",
                "select id from orders where event_date not between DATE '2024-02-29' and DATE '2026-01-01'",
            ),
            (
                "contradictory date literal between",
                "select id from orders where event_date between DATE '2026-01-01' and DATE '2024-02-29'",
            ),
            (
                "contradictory date literal not between",
                "select id from orders where event_date not between DATE '2026-01-01' and DATE '2024-02-29'",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
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
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_date_partition_null_checks_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-date-partition-null-checks",
            DATE_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["event_date"]"#,
            &[
                r#""partitionValues":{"event_date":"2026-01-01"}"#,
                r#""partitionValues":{"event_date":"1969-12-31"}"#,
                r#""partitionValues":{"event_date":null}"#,
                r#""partitionValues":{"event_date":""}"#,
                r#""partitionValues":{"event_date":"not-a-date"}"#,
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
            ("is null", "select id from orders where event_date is null"),
            (
                "is not null",
                "select id from orders where event_date is not null",
            ),
        ];

        for (name, sql) in cases {
            let dataframe = ctx.sql(sql).await?;
            let physical_plan = dataframe.create_physical_plan().await?;
            let plan_display =
                datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true);
            let plan_display = plan_display.to_string();
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
                scans[0]
                    .scan_plan()
                    .pushed_filter_plan
                    .residual_filter_count,
                0
            );
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_boolean_partition_shorthand_rewrites_keep_residual_filter()
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
            ("shorthand", "select id from orders where is_current"),
            (
                "not shorthand",
                "select id from orders where not is_current",
            ),
            (
                "equality true rewrite",
                "select id from orders where is_current = true",
            ),
            (
                "inequality true rewrite",
                "select id from orders where is_current != true",
            ),
            (
                "reversed equality false rewrite",
                "select id from orders where false = is_current",
            ),
            (
                "in list rewrite",
                "select id from orders where is_current in (true, false, true)",
            ),
            (
                "not in list rewrite",
                "select id from orders where is_current not in (true)",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
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
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_boolean_partition_ordering_filters_keep_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "sql-boolean-partition-ordering-residuals",
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
            ("less than", "select id from orders where is_current < true"),
            (
                "between",
                "select id from orders where is_current between false and true",
            ),
            (
                "not between",
                "select id from orders where is_current not between false and true",
            ),
        ];

        for (name, sql) in cases {
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
            assert!(
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn sql_integer_partition_literal_operators_keep_residual_filter()
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
            ("equality", "select id from orders where long_part = 7"),
            (
                "reversed equality",
                "select id from orders where 7 = long_part",
            ),
            (
                "in list",
                "select id from orders where long_part in (7, -1)",
            ),
            ("less than", "select id from orders where long_part < 7"),
            (
                "less than or equal",
                "select id from orders where long_part <= -1",
            ),
            ("greater than", "select id from orders where long_part > -1"),
            (
                "reversed greater than",
                "select id from orders where 7 > long_part",
            ),
            (
                "between inclusive",
                "select id from orders where long_part between -1 and 7",
            ),
            (
                "not between",
                "select id from orders where long_part not between -1 and 7",
            ),
            (
                "contradictory between",
                "select id from orders where long_part between 10 and -10",
            ),
            (
                "contradictory not between",
                "select id from orders where long_part not between 10 and -10",
            ),
        ];

        for (name, sql) in cases {
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
                0,
                "{name}: {plan_display}"
            );
            assert_eq!(
                scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                0,
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
                scans[0].scan_plan().partition_metadata_filter.is_none(),
                "{name}"
            );
            assert!(
                scans[0].scan_plan().kernel_partition_predicate.is_none(),
                "{name}"
            );
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
