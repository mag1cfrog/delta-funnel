//! Delta provider scan planning state.
//!
//! The MVP scan partition planner is intentionally metadata-only and
//! in-memory. It expands the Delta Kernel scan into one metadata record per
//! active selected file, converts those records into provider-owned file tasks,
//! and groups the tasks before execution exists. This owns file metadata,
//! partition values, parsed statistics, deletion-vector metadata handles, and
//! transform handles, but it does not read Parquet data or deletion-vector
//! payloads. Memory use is therefore bounded by the number of files selected by
//! kernel partition and stats pruning for this provider scan.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Expr;
use snafu::ResultExt;

use crate::{
    DeltaFunnelError, DeltaProtocolReport,
    error::DeltaScanMetadataExpansionSnafu,
    table_formats::{
        DeltaKernelEngineContext, DeltaKernelPredicate, DeltaStorageOptions,
        KernelScanFileMetadata, KernelScanMetadataExpansion, ProjectedDeltaScan,
    },
};

use super::super::profile_query_planning_sync_result;
use super::file_task::DeltaScanFileTask;
use super::file_task_partition::{
    DeltaScanFileTaskPartitionOptions, DeltaScanFileTaskPartitionPlan,
    DeltaScanFileTaskPartitionPlanRequest,
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
    /// Source-local options forwarded to Delta Kernel object-store construction.
    pub(crate) storage_options: DeltaStorageOptions,
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
    /// Delta table partition columns retained for scan-local filter planning.
    pub(crate) partition_columns: Vec<String>,
    /// Kernel predicate passed to delta_kernel scan planning for partition pruning.
    pub(crate) kernel_partition_predicate: Option<DeltaKernelPredicate>,
    /// Kernel predicate safe to evaluate against physical Parquet rows.
    pub(crate) provider_enforced_row_predicate: Option<DeltaKernelPredicate>,
    kernel_scan: ProjectedDeltaScan,
}

/// Metadata-only expansion of one planned Delta provider scan.
///
/// This is an all-or-error boundary. A successful value owns every kernel file
/// metadata record selected for this provider scan and records whether the
/// upstream metadata iterator was exhausted. Partial expansions are not exposed
/// to file-task grouping.
#[allow(dead_code)]
pub(crate) struct ProviderScanMetadataExpansion {
    /// DataFusion table name for this source.
    pub(crate) source_name: String,
    /// Normalized Delta table URI for this source.
    pub(crate) table_uri: String,
    /// Resolved Delta snapshot version.
    pub(crate) snapshot_version: u64,
    /// File metadata records selected by Delta Kernel for this provider scan.
    pub(crate) files: Vec<KernelScanFileMetadata>,
    /// Approximate Add actions excluded during Kernel metadata planning.
    pub(crate) files_filtered_during_planning: Option<u64>,
    /// Whether the kernel scan metadata iterator was consumed to completion.
    pub(crate) scan_metadata_exhausted: bool,
}

pub(crate) struct ProviderScanPlanParts {
    pub(crate) source_name: String,
    pub(crate) table_uri: String,
    pub(crate) storage_options: DeltaStorageOptions,
    pub(crate) snapshot_version: u64,
    pub(crate) projected_schema: SchemaRef,
    pub(crate) protocol: DeltaProtocolReport,
    pub(crate) scan_projection: Option<Vec<usize>>,
    pub(crate) pushed_filter_plan: DeltaFilterPushdownPlan,
    pub(crate) partition_columns: Vec<String>,
    pub(crate) kernel_partition_predicate: Option<DeltaKernelPredicate>,
    pub(crate) provider_enforced_row_predicate: Option<DeltaKernelPredicate>,
    pub(crate) kernel_scan: ProjectedDeltaScan,
}

impl ProviderScanPlan {
    pub(crate) fn from_parts(parts: ProviderScanPlanParts) -> Self {
        Self {
            source_name: parts.source_name,
            table_uri: parts.table_uri,
            storage_options: parts.storage_options,
            snapshot_version: parts.snapshot_version,
            projected_schema: parts.projected_schema,
            protocol: parts.protocol,
            scan_projection: parts.scan_projection,
            pushed_filter_plan: parts.pushed_filter_plan,
            partition_columns: parts.partition_columns,
            kernel_partition_predicate: parts.kernel_partition_predicate,
            provider_enforced_row_predicate: parts.provider_enforced_row_predicate,
            kernel_scan: parts.kernel_scan,
        }
    }

    /// Returns the scan-local Delta table partition columns.
    #[cfg(test)]
    pub(crate) fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    /// Returns the private kernel scan state for downstream provider scan phases.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn kernel_scan(&self) -> &ProjectedDeltaScan {
        &self.kernel_scan
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn engine_context(&self) -> &std::sync::Arc<DeltaKernelEngineContext> {
        self.kernel_scan.engine_context()
    }

    /// Expands this provider scan plan into metadata-only file records.
    ///
    /// This is the provider-facing boundary for scan metadata expansion. It
    /// preserves provider context for task planning and maps kernel
    /// expansion failures into a phase-specific DeltaFunnel error.
    #[allow(dead_code)]
    pub(crate) fn expand_scan_metadata(
        &self,
    ) -> Result<ProviderScanMetadataExpansion, DeltaFunnelError> {
        let KernelScanMetadataExpansion {
            files,
            files_filtered_during_planning,
            scan_metadata_exhausted,
        } = profile_query_planning_sync_result(
            "Delta scan metadata expansion",
            "delta_scan_metadata_expansion",
            || {
                self.kernel_scan.expand_kernel_scan_metadata().context(
                    DeltaScanMetadataExpansionSnafu {
                        source_name: self.source_name.clone(),
                        table_uri: self.table_uri.clone(),
                        snapshot_version: self.snapshot_version,
                    },
                )
            },
        )?;

        Ok(ProviderScanMetadataExpansion {
            source_name: self.source_name.clone(),
            table_uri: self.table_uri.clone(),
            snapshot_version: self.snapshot_version,
            files,
            files_filtered_during_planning,
            scan_metadata_exhausted,
        })
    }

    /// Expands scan metadata and groups the resulting file tasks into partitions.
    ///
    /// Partition options are validated before Delta Kernel metadata expansion so
    /// invalid caller options fail before any scan metadata work is consumed.
    /// The returned plan is the provider execution handoff: it carries scan
    /// context plus grouped file tasks, so read execution consumes it directly
    /// instead of reloading the snapshot or re-expanding scan metadata.
    #[allow(dead_code)]
    pub(crate) fn plan_file_task_partitions(
        &self,
        options: DeltaScanFileTaskPartitionOptions,
    ) -> Result<DeltaScanFileTaskPartitionPlan, DeltaFunnelError> {
        options.validate_for_scan_context(
            &self.source_name,
            &self.table_uri,
            self.snapshot_version,
        )?;
        let metadata = self.expand_scan_metadata()?;
        profile_query_planning_sync_result(
            "Delta file task partitioning",
            "delta_file_task_partitioning",
            || metadata.into_file_task_partition_plan(options),
        )
    }
}

impl ProviderScanMetadataExpansion {
    /// Converts expanded scan metadata into provider-owned file tasks.
    #[allow(dead_code)]
    pub(crate) fn into_file_tasks(self) -> Result<Vec<DeltaScanFileTask>, DeltaFunnelError> {
        let Self {
            source_name,
            table_uri,
            snapshot_version,
            files,
            files_filtered_during_planning: _,
            scan_metadata_exhausted: _,
        } = self;

        file_tasks_from_metadata(&source_name, &table_uri, snapshot_version, files)
    }

    /// Converts expanded scan metadata into a grouped file-task partition plan.
    #[allow(dead_code)]
    pub(crate) fn into_file_task_partition_plan(
        self,
        options: DeltaScanFileTaskPartitionOptions,
    ) -> Result<DeltaScanFileTaskPartitionPlan, DeltaFunnelError> {
        let Self {
            source_name,
            table_uri,
            snapshot_version,
            files,
            files_filtered_during_planning,
            scan_metadata_exhausted,
        } = self;
        let file_tasks =
            file_tasks_from_metadata(&source_name, &table_uri, snapshot_version, files)?;

        DeltaScanFileTaskPartitionPlan::try_new(DeltaScanFileTaskPartitionPlanRequest {
            source_name,
            table_uri,
            snapshot_version,
            scan_metadata_exhausted,
            files_filtered_during_planning,
            file_tasks,
            options,
        })
    }
}

/// Converts kernel file metadata into provider file tasks with scan context.
fn file_tasks_from_metadata(
    source_name: &str,
    table_uri: &str,
    snapshot_version: u64,
    files: Vec<KernelScanFileMetadata>,
) -> Result<Vec<DeltaScanFileTask>, DeltaFunnelError> {
    files
        .into_iter()
        .map(|file| {
            DeltaScanFileTask::from_kernel_metadata(source_name, table_uri, snapshot_version, file)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    use datafusion::logical_expr::{TableProviderFilterPushDown, col, lit};
    use delta_kernel::object_store::{local::LocalFileSystem, path::Path as ObjectStorePath};

    use crate::{
        DeltaFunnelError, DeltaSourceConfig, DeltaStorageOptions, load_delta_source,
        preflight_delta_protocol,
    };

    use super::super::super::catalog::provider::DeltaTableProvider;
    use super::*;
    use crate::query_engine::datafusion::test_support::DeltaLogTable;
    use crate::table_formats::insert_url_handler;

    type CapturedStorageOptions = Arc<Mutex<Vec<DeltaStorageOptions>>>;

    fn unique_storage_scheme(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(format!("df{name}{}{nanos}", std::process::id()))
    }

    fn register_capturing_local_storage_handler(
        scheme: &str,
        table_path: &Path,
        captured: CapturedStorageOptions,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = LocalFileSystem::new_with_prefix(table_path)?;
        insert_url_handler(
            scheme,
            Arc::new(move |_url, options| {
                let options = options.into_iter().collect::<BTreeMap<_, _>>();
                captured
                    .lock()
                    .map_err(|_| delta_kernel::object_store::Error::Generic {
                        store: "capture",
                        source: std::io::Error::other("captured storage options lock poisoned")
                            .into(),
                    })?
                    .push(options);

                Ok((Box::new(store.clone()), ObjectStorePath::from("")))
            }),
        )?;

        Ok(())
    }

    #[test]
    fn full_projection_scan_plan_preserves_source_context() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("full-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
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
    fn source_planning_and_metadata_expansion_share_one_owned_kernel_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("owned-kernel-context")?;
        let scheme = unique_storage_scheme("ownedcontext")?;
        let captured = CapturedStorageOptions::default();
        register_capturing_local_storage_handler(&scheme, table.path(), Arc::clone(&captured))?;
        let options = BTreeMap::from([
            ("authorization".to_owned(), "context-secret".to_owned()),
            ("region".to_owned(), "us-east-1".to_owned()),
        ]);
        let source = load_delta_source(
            DeltaSourceConfig::new("orders", format!("{scheme}://table/"))
                .with_storage_options(options.clone()),
        )?;
        let source_context = Arc::clone(source.engine_context());
        let weak_context = Arc::downgrade(&source_context);
        let preflight = preflight_delta_protocol(&source)?;

        assert_eq!(source.version(), 1);
        assert_eq!(preflight.protocol().min_reader_version, 1);

        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: None,
            pushed_filters: Vec::new(),
        })?;
        assert!(Arc::ptr_eq(&source_context, plan.engine_context()));
        assert_eq!(plan.projected_schema.field(0).name(), "id");

        let expansion = plan.expand_scan_metadata()?;
        assert!(expansion.scan_metadata_exhausted);
        assert_eq!(
            expansion
                .files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["part-00000.parquet"]
        );
        assert_eq!(
            captured
                .lock()
                .map(|options| options.clone())
                .unwrap_or_default(),
            vec![options]
        );

        drop(expansion);
        drop(source_context);
        drop(provider);
        assert!(weak_context.upgrade().is_some());
        drop(plan);
        assert!(weak_context.upgrade().is_none());

        Ok(())
    }

    #[test]
    fn scan_plan_preserves_empty_pushed_filter_report() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("empty-pushed-filter-report")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
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
    fn provider_scan_plan_expands_metadata_with_source_context_and_pruning()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "provider-scan-metadata-expansion",
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
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
            pushed_filters: vec![col("region").eq(lit("us-west"))],
        })?;
        let expansion = plan.expand_scan_metadata()?;

        assert_eq!(expansion.source_name, "orders");
        assert_eq!(expansion.table_uri, plan.table_uri);
        assert_eq!(expansion.snapshot_version, 1);
        assert!(expansion.scan_metadata_exhausted);
        assert_eq!(expansion.files_filtered_during_planning, Some(2));
        assert_eq!(expansion.files.len(), 1);
        assert_eq!(expansion.files[0].path, "part-00000.parquet");
        assert_eq!(
            expansion.files[0]
                .partition_values
                .get("region")
                .map(String::as_str),
            Some("us-west")
        );

        Ok(())
    }

    #[test]
    fn provider_scan_plan_expands_multiple_active_files_in_kernel_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "provider-scan-metadata-multiple-files",
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
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
            pushed_filters: Vec::new(),
        })?;
        let expansion = plan.expand_scan_metadata()?;
        let paths = expansion
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>();
        let sizes = expansion
            .files
            .iter()
            .map(|file| file.size)
            .collect::<Vec<_>>();

        assert!(expansion.scan_metadata_exhausted);
        assert_eq!(expansion.files_filtered_during_planning, Some(0));
        assert_eq!(
            paths,
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet"
            ]
        );
        assert_eq!(sizes, vec![0, 0, 0]);

        Ok(())
    }

    #[test]
    fn provider_scan_metadata_expansion_converts_one_file_task_per_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "provider-scan-file-tasks",
            crate::query_engine::datafusion::test_support::PARTITIONED_SCHEMA_FIELDS_JSON,
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
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
            pushed_filters: Vec::new(),
        })?;
        let table_uri = plan.table_uri.clone();
        let tasks = plan.expand_scan_metadata()?.into_file_tasks()?;
        let paths = tasks
            .iter()
            .map(|task| task.path.as_str())
            .collect::<Vec<_>>();

        assert_eq!(tasks.len(), 2);
        assert_eq!(paths, vec!["part-00000.parquet", "part-00001.parquet"]);
        assert_eq!(tasks[0].source_name, "orders");
        assert_eq!(tasks[0].table_uri, table_uri);
        assert_eq!(tasks[0].snapshot_version, 1);
        assert_eq!(tasks[0].estimated_bytes, Some(0));

        Ok(())
    }

    #[test]
    fn provider_scan_metadata_expansion_converts_to_file_task_partition_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "provider-scan-file-task-partitions",
            crate::query_engine::datafusion::test_support::PARTITIONED_SCHEMA_FIELDS_JSON,
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
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
            pushed_filters: Vec::new(),
        })?;
        let table_uri = plan.table_uri.clone();
        let partition_plan = plan.expand_scan_metadata()?.into_file_task_partition_plan(
            DeltaScanFileTaskPartitionOptions {
                target_partitions: 1,
            },
        )?;
        let partition_paths = partition_plan.partitions[0]
            .file_tasks
            .iter()
            .map(|task| task.path.as_str())
            .collect::<Vec<_>>();

        assert_eq!(partition_plan.source_name, "orders");
        assert_eq!(partition_plan.table_uri, table_uri);
        assert_eq!(partition_plan.snapshot_version, 1);
        assert!(partition_plan.scan_metadata_exhausted);
        assert_eq!(partition_plan.files_filtered_during_planning, Some(0));
        assert_eq!(partition_plan.partitions.len(), 1);
        assert_eq!(
            partition_paths,
            vec!["part-00000.parquet", "part-00001.parquet"]
        );
        assert_eq!(partition_plan.estimated_bytes, Some(0));

        Ok(())
    }

    #[test]
    fn provider_scan_plan_rejects_zero_partition_target_before_metadata_expansion()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("provider-scan-partition-zero-target")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let mut plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: None,
            pushed_filters: Vec::new(),
        })?;
        plan.table_uri = "\nnot a valid table uri".to_owned();

        let error = match plan.plan_file_task_partitions(DeltaScanFileTaskPartitionOptions {
            target_partitions: 0,
        }) {
            Ok(_) => return Err("zero target partition planning should fail".into()),
            Err(error) => error,
        };

        match error {
            DeltaFunnelError::DeltaScanFileTaskPartitionPlanning {
                source_name,
                table_uri,
                snapshot_version,
                reason,
            } => {
                assert_eq!(source_name, "orders");
                assert_eq!(table_uri, "\nnot a valid table uri");
                assert_eq!(snapshot_version, 1);
                assert!(reason.contains("target_partitions"));
            }
            other => return Err(format!("unexpected error: {other}").into()),
        }

        Ok(())
    }

    #[test]
    fn provider_scan_plan_expands_empty_metadata_when_kernel_prunes_all_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "provider-scan-metadata-empty-pruned",
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
            storage_options: Default::default(),
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
        let expansion = plan.expand_scan_metadata()?;

        assert_eq!(expansion.source_name, "orders");
        assert_eq!(expansion.snapshot_version, 1);
        assert!(expansion.scan_metadata_exhausted);
        assert!(expansion.files.is_empty());

        Ok(())
    }

    #[test]
    fn provider_scan_plan_metadata_expansion_reuses_loaded_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("provider-scan-metadata-loaded-context")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let mut plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: None,
            pushed_filters: Vec::new(),
        })?;
        plan.table_uri = "\nnot a valid table uri".to_owned();
        plan.storage_options
            .insert("invalid-option".to_owned(), "\nsecret".to_owned());

        let expansion = plan.expand_scan_metadata()?;
        assert_eq!(expansion.source_name, "orders");
        assert_eq!(expansion.table_uri, "\nnot a valid table uri");
        assert_eq!(expansion.snapshot_version, 1);
        assert_eq!(expansion.files.len(), 1);
        assert!(expansion.scan_metadata_exhausted);

        Ok(())
    }

    #[test]
    fn provider_scan_plan_metadata_expansion_maps_kernel_errors()
    -> Result<(), Box<dyn std::error::Error>> {
        const INTEGER_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"long_part\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "provider-scan-metadata-expansion-error",
            INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["long_part"]"#,
            r#""partitionValues":{"long_part":"not-an-integer"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: None,
            pushed_filters: Vec::new(),
        })?;
        let expected_table_uri = plan.table_uri.clone();

        let error = match plan.expand_scan_metadata() {
            Ok(_) => return Err("invalid partition metadata should fail expansion".into()),
            Err(error) => error,
        };

        match error {
            DeltaFunnelError::DeltaScanMetadataExpansion {
                source_name,
                table_uri,
                snapshot_version,
                source,
            } => {
                assert_eq!(source_name, "orders");
                assert_eq!(table_uri, expected_table_uri);
                assert_eq!(snapshot_version, 1);
                assert!(source.to_string().contains("not-an-integer"));
            }
            other => return Err(format!("unexpected error: {other}").into()),
        }

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
            storage_options: Default::default(),
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
            plan.kernel_scan().scan_file_paths()?,
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
            storage_options: Default::default(),
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
            plan.kernel_scan().scan_file_paths()?,
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
            storage_options: Default::default(),
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
        assert!(plan.kernel_scan().scan_file_paths()?.is_empty());

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
            storage_options: Default::default(),
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
            storage_options: Default::default(),
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
            plan.kernel_scan().scan_file_paths()?,
            vec!["part-00000.parquet", "part-00001.parquet"]
        );

        Ok(())
    }

    #[test]
    fn provider_scan_plan_dependencies_use_official_delta_kernel_only()
    -> Result<(), Box<dyn std::error::Error>> {
        let manifest =
            fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))?;
        let dependencies = direct_manifest_dependency_names(&manifest);

        assert!(dependencies.contains(&"delta_kernel"));
        assert!(!dependencies.contains(&"deltalake"));
        assert!(!dependencies.contains(&"buoyant_kernel"));

        Ok(())
    }

    fn direct_manifest_dependency_names(manifest: &str) -> Vec<&str> {
        let mut dependency_names = Vec::new();
        let mut in_dependency_section = false;

        for line in manifest.lines() {
            let line = line.trim();
            if line.starts_with('[') && line.ends_with(']') {
                in_dependency_section = matches!(
                    line,
                    "[dependencies]" | "[dev-dependencies]" | "[build-dependencies]"
                );
                continue;
            }
            if !in_dependency_section || line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((dependency_name, _value)) = line.split_once('=') else {
                continue;
            };
            dependency_names.push(dependency_name.trim().trim_matches('"'));
        }

        dependency_names
    }
}
