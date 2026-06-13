//! Delta-aware provider scan file task model.

use std::collections::BTreeMap;

use crate::{
    DeltaFunnelError,
    error::DeltaScanFileTaskPlanningSnafu,
    table_formats::{
        KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata, KernelScanFileMetadata,
    },
};

/// Provider-owned scan task for one physical Delta add file.
///
/// Later grouping may move this task between scan partitions, but it must
/// remain individually inspectable because Delta correctness metadata belongs
/// to each physical file.
#[allow(dead_code)]
pub(crate) struct DeltaScanFileTask {
    /// DataFusion table name for this source.
    pub(crate) source_name: String,
    /// Normalized Delta table URI used to resolve the table-relative path.
    pub(crate) table_uri: String,
    /// Resolved Delta snapshot version that selected this file.
    pub(crate) snapshot_version: u64,
    /// Delta add-action path for the selected data file.
    pub(crate) path: String,
    /// File size in bytes when kernel metadata exposes a valid non-negative size.
    pub(crate) estimated_bytes: Option<u64>,
    /// Row count from parsed file statistics when available.
    pub(crate) estimated_rows: Option<u64>,
    /// Last modification timestamp in milliseconds since the Unix epoch.
    pub(crate) modification_time_ms: Option<i64>,
    /// Partition values from the Delta add action keyed by partition column.
    pub(crate) partition_values: BTreeMap<String, String>,
    /// Parsed file statistics preserved for later planning and diagnostics.
    pub(crate) stats: Option<DeltaScanFileStats>,
    /// Opaque deletion-vector metadata preserved without loading its payload.
    pub(crate) deletion_vector: KernelScanDeletionVectorMetadata,
    /// Opaque physical-to-logical transform metadata preserved without evaluation.
    pub(crate) transform: KernelPhysicalToLogicalTransform,
}

/// Parsed statistics carried by one Delta scan file task.
#[allow(dead_code)]
pub(crate) struct DeltaScanFileStats {
    /// Number of records from Delta Kernel parsed stats.
    pub(crate) num_records: Option<u64>,
}

impl DeltaScanFileTask {
    /// Converts one kernel metadata record into one provider-owned file task.
    pub(crate) fn from_kernel_metadata(
        source_name: &str,
        table_uri: &str,
        snapshot_version: u64,
        file: KernelScanFileMetadata,
    ) -> Result<Self, DeltaFunnelError> {
        let estimated_bytes =
            valid_estimated_bytes(source_name, table_uri, snapshot_version, &file)?;
        let stats = file.stats.map(|stats| DeltaScanFileStats {
            num_records: Some(stats.num_records),
        });
        let estimated_rows = stats.as_ref().and_then(|stats| stats.num_records);

        Ok(Self {
            source_name: source_name.to_owned(),
            table_uri: table_uri.to_owned(),
            snapshot_version,
            path: file.path,
            estimated_bytes,
            estimated_rows,
            modification_time_ms: Some(file.modification_time),
            partition_values: file.partition_values.into_iter().collect(),
            stats,
            deletion_vector: file.deletion_vector,
            transform: file.physical_to_logical_transform,
        })
    }
}

fn valid_estimated_bytes(
    source_name: &str,
    table_uri: &str,
    snapshot_version: u64,
    file: &KernelScanFileMetadata,
) -> Result<Option<u64>, DeltaFunnelError> {
    let bytes = match u64::try_from(file.size) {
        Ok(bytes) => bytes,
        Err(_) => {
            return DeltaScanFileTaskPlanningSnafu {
                source_name: source_name.to_owned(),
                table_uri: table_uri.to_owned(),
                snapshot_version,
                path: file.path.clone(),
                reason: format!(
                    "kernel scan file size must be non-negative, got {}",
                    file.size
                ),
            }
            .fail();
        }
    };

    Ok(Some(bytes))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        DeltaSourceConfig, load_delta_source,
        query_engine::datafusion::test_support::{DEFAULT_SCHEMA_FIELDS_JSON, DeltaLogTable},
        table_formats::{
            KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata,
            KernelScanFileMetadata, KernelScanFileStats,
            build_projected_predicated_stats_delta_scan,
        },
    };

    use super::DeltaScanFileTask;

    fn kernel_file(path: &str) -> KernelScanFileMetadata {
        KernelScanFileMetadata {
            path: path.to_owned(),
            size: 123,
            modification_time: 1587968586000,
            stats: Some(KernelScanFileStats { num_records: 7 }),
            deletion_vector: KernelScanDeletionVectorMetadata::NotPresent,
            physical_to_logical_transform: KernelPhysicalToLogicalTransform::NotRequired,
            partition_values: HashMap::from([
                ("region".to_owned(), "us-west".to_owned()),
                ("day".to_owned(), "2026-06-11".to_owned()),
            ]),
        }
    }

    #[test]
    fn file_task_preserves_kernel_file_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let task = DeltaScanFileTask::from_kernel_metadata(
            "orders",
            "file:///tmp/table",
            42,
            kernel_file("part-00000.parquet"),
        )?;

        assert_eq!(task.source_name, "orders");
        assert_eq!(task.table_uri, "file:///tmp/table");
        assert_eq!(task.snapshot_version, 42);
        assert_eq!(task.path, "part-00000.parquet");
        assert_eq!(task.estimated_bytes, Some(123));
        assert_eq!(task.estimated_rows, Some(7));
        assert_eq!(task.modification_time_ms, Some(1587968586000));
        assert_eq!(
            task.partition_values.get("region").map(String::as_str),
            Some("us-west")
        );
        assert_eq!(
            task.partition_values.get("day").map(String::as_str),
            Some("2026-06-11")
        );
        assert_eq!(
            task.stats.as_ref().and_then(|stats| stats.num_records),
            Some(7)
        );
        assert!(matches!(
            task.deletion_vector,
            KernelScanDeletionVectorMetadata::NotPresent
        ));
        assert!(matches!(
            task.transform,
            KernelPhysicalToLogicalTransform::NotRequired
        ));

        Ok(())
    }

    #[test]
    fn file_task_preserves_missing_stats_as_unknown() -> Result<(), Box<dyn std::error::Error>> {
        let mut file = kernel_file("missing-stats.parquet");
        file.stats = None;

        let task =
            DeltaScanFileTask::from_kernel_metadata("orders", "file:///tmp/table", 42, file)?;

        assert!(task.stats.is_none());
        assert_eq!(task.estimated_rows, None);

        Ok(())
    }

    #[test]
    fn file_task_model_cannot_be_reduced_to_plain_path_list_without_losing_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let task = DeltaScanFileTask::from_kernel_metadata(
            "orders",
            "file:///tmp/table",
            42,
            kernel_file("part-00000.parquet"),
        )?;

        let path_only = task.path.clone();

        assert_eq!(path_only, "part-00000.parquet");
        assert_eq!(task.source_name, "orders");
        assert_eq!(task.table_uri, "file:///tmp/table");
        assert_eq!(task.snapshot_version, 42);
        assert_eq!(task.estimated_bytes, Some(123));
        assert_eq!(task.estimated_rows, Some(7));
        assert_eq!(task.modification_time_ms, Some(1587968586000));
        assert_eq!(
            task.partition_values.get("region").map(String::as_str),
            Some("us-west")
        );
        assert_eq!(
            task.stats.as_ref().and_then(|stats| stats.num_records),
            Some(7)
        );
        assert!(matches!(
            task.deletion_vector,
            KernelScanDeletionVectorMetadata::NotPresent
        ));
        assert!(matches!(
            task.transform,
            KernelPhysicalToLogicalTransform::NotRequired
        ));

        Ok(())
    }

    #[test]
    fn file_task_preserves_deletion_vector_presence_without_payload_read()
    -> Result<(), Box<dyn std::error::Error>> {
        const DELETION_VECTOR_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#;
        let table = DeltaLogTable::new_with_schema_protocol_and_adds(
            "file-task-dv-preservation",
            DELETION_VECTOR_PROTOCOL_JSON,
            DEFAULT_SCHEMA_FIELDS_JSON,
            "[]",
            &[
                r#""partitionValues":{},"deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}"#,
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, None)?;
        let expansion = scan.expand_kernel_scan_metadata(source.table_uri())?;
        let Some(file) = expansion.files.into_iter().next() else {
            return Err("expected one deletion-vector scan file".into());
        };

        let task = DeltaScanFileTask::from_kernel_metadata(
            source.name(),
            source.table_uri(),
            source.version(),
            file,
        )?;

        assert!(task.deletion_vector.is_present());

        Ok(())
    }

    #[test]
    fn file_task_preserves_transform_presence_without_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut file = kernel_file("transform.parquet");
        file.physical_to_logical_transform =
            KernelPhysicalToLogicalTransform::test_required_column_transform("physical_id");

        let task =
            DeltaScanFileTask::from_kernel_metadata("orders", "file:///tmp/table", 42, file)?;

        assert!(task.transform.is_required());

        Ok(())
    }

    #[test]
    fn file_task_rejects_negative_kernel_size() -> Result<(), Box<dyn std::error::Error>> {
        let mut file = kernel_file("negative-size.parquet");
        file.size = -1;

        let error = match DeltaScanFileTask::from_kernel_metadata(
            "orders",
            "file:///tmp/table",
            42,
            file,
        ) {
            Ok(_) => return Err("negative size should fail task planning".into()),
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("Delta scan file task planning error"));
        assert!(display.contains("orders"));
        assert!(display.contains("snapshot version 42"));
        assert!(display.contains("negative-size.parquet"));
        assert!(display.contains("must be non-negative"));

        Ok(())
    }
}
