//! File-level Delta reader for one provider scan task.
//!
//! This module owns the internal correctness boundary between file-task
//! planning and later DataFusion stream execution. This first slice wires the
//! non-DV, non-transform path through the official kernel reader baseline.

use datafusion::arrow::record_batch::RecordBatch;
use snafu::ResultExt;

use crate::{
    DeltaFunnelError,
    error::{DeltaScanFileReadPhase, DeltaScanFileReadSnafu},
    table_formats::{
        KernelDataFileReadRequest, KernelDataFileReader, KernelDataFileReaderConfig,
        KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata, KernelScanReadSchema,
    },
};

use super::file_task::DeltaScanFileTask;

/// Reusable file-level Delta reader for one provider scan context.
#[allow(dead_code)]
pub(crate) struct DeltaFileReader {
    source_name: String,
    table_uri: String,
    snapshot_version: u64,
    data_file_reader: KernelDataFileReader,
}

/// Context required to construct the file-level reader.
#[allow(dead_code)]
pub(crate) struct DeltaFileReaderConfig<'a> {
    /// DataFusion table name for diagnostics.
    pub(crate) source_name: &'a str,
    /// Normalized Delta table URI used to resolve table-relative file paths.
    pub(crate) table_uri: &'a str,
    /// Snapshot version that selected the file tasks.
    pub(crate) snapshot_version: u64,
}

/// Request to read exactly one provider file task.
#[allow(dead_code)]
pub(crate) struct DeltaFileReadRequest<'a> {
    /// Provider-owned task for one physical Delta data file.
    pub(crate) task: &'a DeltaScanFileTask,
    /// Kernel scan schema state selected for this provider scan.
    pub(crate) read_schema: &'a KernelScanReadSchema,
}

/// Logically correct Arrow batches for one provider file task.
#[allow(dead_code)]
pub(crate) struct DeltaFileReadResult {
    /// Arrow batches ready for later DataFusion stream handoff.
    pub(crate) batches: Vec<RecordBatch>,
}

impl DeltaFileReader {
    /// Builds a file-level reader for one provider scan context.
    #[allow(dead_code)]
    pub(crate) fn try_new(config: DeltaFileReaderConfig<'_>) -> Result<Self, DeltaFunnelError> {
        let data_file_reader = KernelDataFileReader::try_new(KernelDataFileReaderConfig {
            source_name: config.source_name,
            table_uri: config.table_uri,
            snapshot_version: config.snapshot_version,
        })?;

        Ok(Self {
            source_name: config.source_name.to_owned(),
            table_uri: config.table_uri.to_owned(),
            snapshot_version: config.snapshot_version,
            data_file_reader,
        })
    }

    /// Reads one provider file task into logical Arrow batches.
    #[allow(dead_code)]
    pub(crate) fn read_file(
        &self,
        request: DeltaFileReadRequest<'_>,
    ) -> Result<DeltaFileReadResult, DeltaFunnelError> {
        self.validate_task_context(request.task)?;
        self.reject_unwired_transform(request.task)?;
        self.reject_unwired_deletion_vector(request.task)?;

        let data = self
            .data_file_reader
            .read_file_batches(KernelDataFileReadRequest {
                path: &request.task.path,
                size: request.task.estimated_bytes,
                modification_time_ms: request.task.modification_time_ms,
                schema: request.read_schema,
            })?;

        Ok(DeltaFileReadResult {
            batches: data.batches,
        })
    }

    fn validate_task_context(&self, task: &DeltaScanFileTask) -> Result<(), DeltaFunnelError> {
        if task.source_name == self.source_name
            && task.table_uri == self.table_uri
            && task.snapshot_version == self.snapshot_version
        {
            return Ok(());
        }

        Err(delta_kernel::Error::generic(
            "file task scan context does not match the file reader context",
        ))
        .context(DeltaScanFileReadSnafu {
            source_name: self.source_name.clone(),
            table_uri: self.table_uri.clone(),
            snapshot_version: self.snapshot_version,
            path: task.path.clone(),
            phase: DeltaScanFileReadPhase::FileMetadataConversion,
        })
    }

    fn reject_unwired_transform(&self, task: &DeltaScanFileTask) -> Result<(), DeltaFunnelError> {
        match &task.transform {
            KernelPhysicalToLogicalTransform::NotRequired => Ok(()),
            KernelPhysicalToLogicalTransform::Required(_) => Err(delta_kernel::Error::generic(
                "physical-to-logical transform application is owned by a later #138 slice",
            ))
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: task.path.clone(),
                phase: DeltaScanFileReadPhase::TransformApplication,
            }),
        }
    }

    fn reject_unwired_deletion_vector(
        &self,
        task: &DeltaScanFileTask,
    ) -> Result<(), DeltaFunnelError> {
        match &task.deletion_vector {
            KernelScanDeletionVectorMetadata::NotPresent => Ok(()),
            KernelScanDeletionVectorMetadata::Present(_) => Err(delta_kernel::Error::generic(
                "deletion-vector masking is owned by a later #138 slice",
            ))
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: task.path.clone(),
                phase: DeltaScanFileReadPhase::DeletionVectorMasking,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use delta_kernel::arrow::array::{Array, Int32Array, StringArray};

    use super::{DeltaFileReadRequest, DeltaFileReader, DeltaFileReaderConfig};
    use crate::query_engine::datafusion::file_task::DeltaScanFileTask;
    use crate::table_formats::RealParquetDeltaTable;
    use crate::table_formats::{
        DeltaSourceConfig, KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata,
        PlannedDeltaSource, build_projected_delta_scan,
        build_projected_predicated_stats_delta_scan, load_delta_source,
    };

    #[test]
    fn file_reader_reads_non_dv_file_rows() -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("file-reader-non-dv")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, None)?;
        let read_schema = scan.read_schema();
        let task = first_file_task(&source, &scan)?;
        let reader = test_reader(&source)?;
        let result = reader.read_file(DeltaFileReadRequest {
            task: &task,
            read_schema: &read_schema,
        })?;

        assert_eq!(result.batches.len(), 1);
        let batch = result.batches.first().ok_or("expected one record batch")?;
        assert_eq!(batch.num_rows(), table.rows());
        assert_eq!(batch.num_columns(), 2);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected id Int32Array")?;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected customer_name StringArray")?;

        assert_eq!(ids.values(), &[1, 2, 3]);
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");
        assert!(names.is_null(2));

        Ok(())
    }

    #[test]
    fn file_reader_honors_projected_schema() -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("file-reader-projection")?;
        let source = load_source("orders", &table)?;
        let projected_columns = vec!["customer_name".to_owned()];
        let scan = build_projected_delta_scan(&source, Some(&projected_columns))?;
        let read_schema = scan.read_schema();
        let task = first_file_task(&source, &scan)?;
        let reader = test_reader(&source)?;
        let result = reader.read_file(DeltaFileReadRequest {
            task: &task,
            read_schema: &read_schema,
        })?;
        let batch = result.batches.first().ok_or("expected one record batch")?;

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "customer_name");

        Ok(())
    }

    #[test]
    fn file_reader_rejects_transform_until_slice_is_wired() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = RealParquetDeltaTable::new_default("file-reader-transform-reject")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let mut task = first_file_task(&source, &scan)?;
        task.transform = KernelPhysicalToLogicalTransform::test_required_column_transform("id");
        let reader = test_reader(&source)?;
        let error = reader
            .read_file(DeltaFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .err()
            .ok_or("expected transform rejection")?;
        let display = error.to_string();

        assert!(display.contains("source `orders`"), "{display}");
        assert!(
            display.contains("physical-to-logical transform application"),
            "{display}"
        );
        assert!(display.contains(table.data_file_path()), "{display}");

        Ok(())
    }

    #[test]
    fn file_reader_rejects_deletion_vector_until_slice_is_wired()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("file-reader-dv-reject")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let mut task = first_file_task(&source, &scan)?;
        task.deletion_vector = KernelScanDeletionVectorMetadata::test_present();
        let reader = test_reader(&source)?;
        let error = reader
            .read_file(DeltaFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .err()
            .ok_or("expected DV rejection")?;
        let display = error.to_string();

        assert!(display.contains("source `orders`"), "{display}");
        assert!(display.contains("deletion-vector masking"), "{display}");
        assert!(display.contains(table.data_file_path()), "{display}");

        Ok(())
    }

    #[test]
    fn file_reader_file_metadata_error_preserves_context() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = RealParquetDeltaTable::new_default("file-reader-error-context")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let mut task = first_file_task(&source, &scan)?;
        task.path = "missing-size.parquet".to_owned();
        task.estimated_bytes = None;
        let reader = test_reader(&source)?;
        let error = reader
            .read_file(DeltaFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .err()
            .ok_or("expected missing file size error")?;
        let display = error.to_string();

        assert!(display.contains("source `orders`"), "{display}");
        assert!(display.contains("snapshot version 1"), "{display}");
        assert!(display.contains("missing-size.parquet"), "{display}");
        assert!(display.contains("file metadata conversion"), "{display}");
        assert!(display.contains("file size is required"), "{display}");

        Ok(())
    }

    fn first_file_task(
        source: &PlannedDeltaSource,
        scan: &crate::table_formats::ProjectedDeltaScan,
    ) -> Result<DeltaScanFileTask, Box<dyn std::error::Error>> {
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri())?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;

        Ok(DeltaScanFileTask::from_kernel_metadata(
            source.name(),
            source.table_uri(),
            source.version(),
            file,
        )?)
    }

    fn load_source(
        source_name: &str,
        table: &RealParquetDeltaTable,
    ) -> Result<PlannedDeltaSource, Box<dyn std::error::Error>> {
        Ok(load_delta_source(DeltaSourceConfig {
            name: source_name.to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?)
    }

    fn test_reader(
        source: &PlannedDeltaSource,
    ) -> Result<DeltaFileReader, Box<dyn std::error::Error>> {
        Ok(DeltaFileReader::try_new(DeltaFileReaderConfig {
            source_name: source.name(),
            table_uri: source.table_uri(),
            snapshot_version: source.version(),
        })?)
    }
}
