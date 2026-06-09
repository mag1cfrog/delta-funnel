//! Delta provider scan planning state.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Expr;

use crate::{
    DeltaProtocolReport,
    table_formats::{DeltaKernelPredicate, ProjectedDeltaScan},
};

use super::filters::DeltaFilterPushdownPlan;

/// Caller request used to build a provider scan plan.
#[allow(dead_code)]
pub(crate) struct ProviderScanPlanRequest {
    /// Requested DataFusion projection indexes against the provider logical schema.
    pub(crate) requested_projection: Option<Vec<usize>>,
    /// Filters pushed into this scan by DataFusion.
    pub(crate) pushed_filters: Vec<Expr>,
}

/// Kernel-backed scan intent for one Delta provider scan.
#[allow(dead_code)]
pub(crate) struct ProviderScanPlan {
    /// DataFusion table name for this source.
    pub(crate) source_name: String,
    /// Normalized Delta table URI for this source.
    pub(crate) table_uri: String,
    /// Resolved Delta snapshot version.
    pub(crate) snapshot_version: u64,
    /// Arrow schema expected from this provider scan.
    pub(crate) projected_schema: SchemaRef,
    /// Protocol report captured before provider registration.
    pub(crate) protocol: DeltaProtocolReport,
    /// Projection indexes accepted and used for this scan, if any.
    pub(crate) scan_projection: Option<Vec<usize>>,
    /// Structured report for filters pushed into this scan.
    pub(crate) pushed_filter_plan: DeltaFilterPushdownPlan,
    /// Kernel predicate passed to delta_kernel scan planning for partition pruning.
    pub(crate) kernel_partition_predicate: Option<DeltaKernelPredicate>,
    kernel_scan: ProjectedDeltaScan,
}

pub(super) struct ProviderScanPlanParts {
    pub(super) source_name: String,
    pub(super) table_uri: String,
    pub(super) snapshot_version: u64,
    pub(super) projected_schema: SchemaRef,
    pub(super) protocol: DeltaProtocolReport,
    pub(super) scan_projection: Option<Vec<usize>>,
    pub(super) pushed_filter_plan: DeltaFilterPushdownPlan,
    pub(super) kernel_partition_predicate: Option<DeltaKernelPredicate>,
    pub(super) kernel_scan: ProjectedDeltaScan,
}

impl ProviderScanPlan {
    pub(super) fn from_parts(parts: ProviderScanPlanParts) -> Self {
        Self {
            source_name: parts.source_name,
            table_uri: parts.table_uri,
            snapshot_version: parts.snapshot_version,
            projected_schema: parts.projected_schema,
            protocol: parts.protocol,
            scan_projection: parts.scan_projection,
            pushed_filter_plan: parts.pushed_filter_plan,
            kernel_partition_predicate: parts.kernel_partition_predicate,
            kernel_scan: parts.kernel_scan,
        }
    }

    /// Returns the private kernel scan state for later provider scan phases.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn kernel_scan(&self) -> &ProjectedDeltaScan {
        &self.kernel_scan
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use datafusion::logical_expr::{TableProviderFilterPushDown, col, lit};

    use crate::{DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

    use super::super::provider::DeltaTableProvider;
    use super::*;
    use crate::query_engine::datafusion::test_support::DeltaLogTable;

    #[test]
    fn full_projection_scan_plan_preserves_source_context() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("full-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: None,
            pushed_filters: Vec::new(),
        })?;

        assert_eq!(plan.source_name, "orders");
        assert!(plan.table_uri.starts_with("file://"));
        assert_eq!(plan.snapshot_version, 1);
        assert_eq!(plan.protocol.source_name, "orders");
        assert_eq!(plan.scan_projection, None);
        assert_eq!(plan.projected_schema.fields().len(), 2);
        assert_eq!(plan.projected_schema.field(0).name(), "id");
        assert_eq!(plan.projected_schema.field(1).name(), "customer_name");
        assert_eq!(plan.kernel_scan().kernel_schema().num_fields(), 2);
        assert!(plan.kernel_partition_predicate.is_none());
        let _ = plan.kernel_scan().kernel_scan();

        Ok(())
    }

    #[test]
    fn scan_plan_preserves_empty_pushed_filter_report() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("empty-pushed-filter-report")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: None,
            pushed_filters: Vec::new(),
        })?;

        assert!(plan.pushed_filter_plan.datafusion_pushdowns().is_empty());
        assert!(plan.pushed_filter_plan.decisions.is_empty());
        assert_eq!(plan.pushed_filter_plan.exact_count, 0);
        assert_eq!(plan.pushed_filter_plan.inexact_count, 0);
        assert_eq!(plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_plan.pushed_filter_count, 0);
        assert_eq!(plan.pushed_filter_plan.residual_filter_count, 0);
        assert!(plan.kernel_partition_predicate.is_none());

        Ok(())
    }

    #[test]
    fn scan_plan_preserves_multiple_exact_pushed_filters() -> Result<(), Box<dyn std::error::Error>>
    {
        const TWO_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "multiple-exact-pushed-filter-report",
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

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
            pushed_filters: vec![
                col("region").eq(lit("us-west")),
                col("day").eq(lit("2026-05-31")),
            ],
        })?;

        assert_eq!(
            plan.pushed_filter_plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
            ]
        );
        assert_eq!(plan.pushed_filter_plan.exact_count, 2);
        assert_eq!(plan.pushed_filter_plan.inexact_count, 0);
        assert_eq!(plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_plan.pushed_filter_count, 2);
        assert_eq!(plan.pushed_filter_plan.residual_filter_count, 0);
        assert!(plan.kernel_partition_predicate.is_some());
        assert_eq!(
            plan.kernel_scan().scan_file_paths(&plan.table_uri)?,
            vec!["part-00000.parquet"]
        );
        assert_eq!(plan.projected_schema.field(0).name(), "id");
        let kernel_names = plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(kernel_names, vec!["id", "region", "day"]);

        Ok(())
    }

    #[test]
    fn scan_plan_preserves_duplicate_exact_filters_as_distinct_inputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "duplicate-exact-pushed-filter-report",
            crate::query_engine::datafusion::test_support::PARTITIONED_SCHEMA_FIELDS_JSON,
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

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
            pushed_filters: vec![
                col("region").eq(lit("us-west")),
                col("region").eq(lit("us-west")),
            ],
        })?;

        assert_eq!(
            plan.pushed_filter_plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
            ]
        );
        assert_eq!(plan.pushed_filter_plan.decisions.len(), 2);
        assert_eq!(plan.pushed_filter_plan.exact_count, 2);
        assert_eq!(plan.pushed_filter_plan.pushed_filter_count, 2);
        assert_eq!(plan.pushed_filter_plan.residual_filter_count, 0);
        assert!(plan.kernel_partition_predicate.is_some());
        assert_eq!(
            plan.kernel_scan().scan_file_paths(&plan.table_uri)?,
            vec!["part-00000.parquet"]
        );

        Ok(())
    }

    #[test]
    fn scan_plan_multiple_exact_filters_preserve_contradictory_and_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "contradictory-exact-pushed-filter-report",
            crate::query_engine::datafusion::test_support::PARTITIONED_SCHEMA_FIELDS_JSON,
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

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
            pushed_filters: vec![
                col("region").eq(lit("us-west")),
                col("region").eq(lit("us-east")),
            ],
        })?;

        assert_eq!(
            plan.pushed_filter_plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
            ]
        );
        assert_eq!(plan.pushed_filter_plan.exact_count, 2);
        assert_eq!(plan.pushed_filter_plan.residual_filter_count, 0);
        assert!(plan.kernel_partition_predicate.is_some());
        assert!(
            plan.kernel_scan()
                .scan_file_paths(&plan.table_uri)?
                .is_empty()
        );

        Ok(())
    }

    #[test]
    fn scan_plan_exact_partition_filter_allows_projection_omission()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "exact-partition-filter-projection-omission",
            crate::query_engine::datafusion::test_support::PARTITIONED_SCHEMA_FIELDS_JSON,
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

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
            pushed_filters: vec![col("region").eq(lit("us-west"))],
        })?;

        assert_eq!(plan.pushed_filter_plan.exact_count, 1);
        assert_eq!(plan.pushed_filter_plan.residual_filter_count, 0);
        assert_eq!(plan.projected_schema.fields().len(), 1);
        assert_eq!(plan.projected_schema.field(0).name(), "id");
        assert_eq!(plan.scan_projection, Some(vec![0]));
        let kernel_names = plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(kernel_names, vec!["id", "region"]);
        assert!(plan.kernel_partition_predicate.is_some());

        Ok(())
    }

    #[test]
    fn scan_plan_accepts_exact_partition_in_filter() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "exact-in-partition-metadata-filter",
            crate::query_engine::datafusion::test_support::PARTITIONED_SCHEMA_FIELDS_JSON,
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

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
            pushed_filters: vec![
                col("region").in_list(vec![lit("us-west"), lit("us-east")], false),
            ],
        })?;

        assert_eq!(
            plan.pushed_filter_plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact]
        );
        assert_eq!(plan.pushed_filter_plan.exact_count, 1);
        assert_eq!(plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_plan.pushed_filter_count, 1);
        assert_eq!(plan.pushed_filter_plan.residual_filter_count, 0);
        assert!(plan.kernel_partition_predicate.is_some());
        assert_eq!(
            plan.kernel_scan().scan_file_paths(&plan.table_uri)?,
            vec!["part-00000.parquet", "part-00001.parquet"]
        );

        Ok(())
    }

    #[test]
    fn provider_scan_plan_dependencies_use_official_delta_kernel_only()
    -> Result<(), Box<dyn std::error::Error>> {
        let manifest =
            fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))?;

        assert!(manifest.contains("delta_kernel"));
        assert!(!manifest.contains("deltalake"));
        assert!(!manifest.contains("buoyant_kernel"));

        Ok(())
    }
}
