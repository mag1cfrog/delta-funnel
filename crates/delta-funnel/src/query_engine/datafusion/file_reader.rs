//! File-level Delta reader for one provider scan task.
//!
//! This module owns the internal correctness boundary between file-task
//! planning and later DataFusion stream execution. It wires the official kernel
//! reader baseline through transform application and deletion-vector masking
//! before any batch is handed to provider execution.

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::record_batch::RecordBatch;
use snafu::ResultExt;

use crate::{
    DeltaFunnelError,
    error::{DeltaScanFileReadPhase, DeltaScanFileReadSnafu},
    table_formats::{
        KernelDataFileReadRequest, KernelDataFileReader, KernelDataFileReaderConfig,
        KernelDataFileTransformRequest, KernelDeletionVectorReadRequest,
        KernelDeletionVectorReader, KernelDeletionVectorReaderConfig, KernelScanReadSchema,
        ProviderDeletionVectorSelection, ProviderDeletionVectorSelectionContext,
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
    deletion_vector_reader: KernelDeletionVectorReader,
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
    ///
    /// The result implements `IntoIterator` so provider execution can consume
    /// batches through an iterator-shaped boundary without seeing unmasked
    /// physical data.
    pub(crate) batches: Vec<RecordBatch>,
}

impl IntoIterator for DeltaFileReadResult {
    type Item = RecordBatch;
    type IntoIter = std::vec::IntoIter<RecordBatch>;

    fn into_iter(self) -> Self::IntoIter {
        self.batches.into_iter()
    }
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
        let deletion_vector_reader =
            KernelDeletionVectorReader::try_new(KernelDeletionVectorReaderConfig {
                source_name: config.source_name,
                table_uri: config.table_uri,
                snapshot_version: config.snapshot_version,
            })?;

        Ok(Self {
            source_name: config.source_name.to_owned(),
            table_uri: config.table_uri.to_owned(),
            snapshot_version: config.snapshot_version,
            data_file_reader,
            deletion_vector_reader,
        })
    }

    /// Reads one provider file task into logical Arrow batches.
    #[allow(dead_code)]
    pub(crate) fn read_file(
        &self,
        request: DeltaFileReadRequest<'_>,
    ) -> Result<DeltaFileReadResult, DeltaFunnelError> {
        self.validate_task_context(request.task)?;
        let mut deletion_vector =
            self.deletion_vector_reader
                .read_selection(KernelDeletionVectorReadRequest {
                    path: &request.task.path,
                    deletion_vector: &request.task.deletion_vector,
                })?;

        let data = self
            .data_file_reader
            .read_file_batches(KernelDataFileReadRequest {
                path: &request.task.path,
                size: request.task.estimated_bytes,
                modification_time_ms: request.task.modification_time_ms,
                schema: request.read_schema,
            })?;
        let batches = self.apply_physical_to_logical_transform(
            request.task,
            data.batches,
            request.read_schema,
        )?;
        let batches =
            self.apply_deletion_vector_mask(request.task, batches, deletion_vector.as_mut())?;
        if let Some(deletion_vector) = deletion_vector.as_mut() {
            deletion_vector.finish(self.deletion_vector_context(request.task))?;
        }

        Ok(DeltaFileReadResult { batches })
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

    fn apply_physical_to_logical_transform(
        &self,
        task: &DeltaScanFileTask,
        batches: Vec<RecordBatch>,
        read_schema: &KernelScanReadSchema,
    ) -> Result<Vec<RecordBatch>, DeltaFunnelError> {
        batches
            .into_iter()
            .map(|batch| {
                self.data_file_reader.apply_physical_to_logical_transform(
                    KernelDataFileTransformRequest {
                        path: &task.path,
                        batch,
                        schema: read_schema,
                        transform: &task.transform,
                    },
                )
            })
            .collect()
    }

    fn apply_deletion_vector_mask(
        &self,
        task: &DeltaScanFileTask,
        batches: Vec<RecordBatch>,
        mut deletion_vector: Option<&mut ProviderDeletionVectorSelection>,
    ) -> Result<Vec<RecordBatch>, DeltaFunnelError> {
        let Some(deletion_vector) = deletion_vector.as_mut() else {
            return Ok(batches);
        };

        batches
            .into_iter()
            .map(|batch| self.apply_deletion_vector_mask_to_batch(task, batch, deletion_vector))
            .collect()
    }

    fn apply_deletion_vector_mask_to_batch(
        &self,
        task: &DeltaScanFileTask,
        batch: RecordBatch,
        deletion_vector: &mut ProviderDeletionVectorSelection,
    ) -> Result<RecordBatch, DeltaFunnelError> {
        let keep_mask =
            deletion_vector.consume_batch(batch.num_rows(), self.deletion_vector_context(task))?;
        if keep_mask.iter().all(|selected| *selected) {
            return Ok(batch);
        }
        if keep_mask.iter().all(|selected| !*selected) {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        let keep_mask = BooleanArray::from(keep_mask);

        match filter_record_batch(&batch, &keep_mask) {
            Ok(filtered) => Ok(filtered),
            Err(error) => Err(delta_kernel::Error::from(error)).context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: task.path.clone(),
                phase: DeltaScanFileReadPhase::DeletionVectorMasking,
            }),
        }
    }

    fn deletion_vector_context<'a>(
        &self,
        task: &'a DeltaScanFileTask,
    ) -> ProviderDeletionVectorSelectionContext<'a> {
        ProviderDeletionVectorSelectionContext {
            source_name: &task.source_name,
            table_uri: &task.table_uri,
            snapshot_version: task.snapshot_version,
            path: &task.path,
        }
    }
}

#[cfg(test)]
mod tests {
    use delta_kernel::actions::deletion_vector::{
        DeletionVectorDescriptor, DeletionVectorStorageType,
    };
    use delta_kernel::actions::deletion_vector_writer::{
        KernelDeletionVector, StreamingDeletionVectorWriter,
    };
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
        let batch = result
            .into_iter()
            .next()
            .ok_or("expected one record batch")?;

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "customer_name");

        Ok(())
    }

    #[test]
    fn file_reader_applies_physical_to_logical_transform() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = RealParquetDeltaTable::new_default("file-reader-transform-apply")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let mut task = first_file_task(&source, &scan)?;
        task.transform =
            KernelPhysicalToLogicalTransform::test_required_orders_customer_name_replacement_transform(
                "transformed",
            );
        let reader = test_reader(&source)?;
        let result = reader.read_file(DeltaFileReadRequest {
            task: &task,
            read_schema: &read_schema,
        })?;
        let batch = result.batches.first().ok_or("expected one record batch")?;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected transformed customer_name StringArray")?;

        assert_eq!(batch.num_rows(), table.rows());
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(names.value(0), "transformed");
        assert_eq!(names.value(1), "transformed");
        assert_eq!(names.value(2), "transformed");

        Ok(())
    }

    #[test]
    fn file_reader_transform_error_preserves_context() -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("file-reader-transform-error-context")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let mut task = first_file_task(&source, &scan)?;
        task.transform =
            KernelPhysicalToLogicalTransform::test_required_column_transform("missing_physical_id");
        let reader = test_reader(&source)?;
        let error = reader
            .read_file(DeltaFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .err()
            .ok_or("expected transform error")?;
        let display = error.to_string();

        assert!(display.contains("source `orders`"), "{display}");
        assert!(display.contains("snapshot version 1"), "{display}");
        assert!(display.contains(table.data_file_path()), "{display}");
        assert!(
            display.contains("physical-to-logical transform application"),
            "{display}"
        );

        Ok(())
    }

    #[test]
    fn file_reader_applies_deletion_vector_mask() -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("file-reader-dv-mask")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let mut task = first_file_task(&source, &scan)?;
        set_task_deletion_vector(&mut task, &table, &[1])?;
        let reader = test_reader(&source)?;
        let result = reader.read_file(DeltaFileReadRequest {
            task: &task,
            read_schema: &read_schema,
        })?;

        assert_eq!(collect_ids(&result.batches)?, vec![1, 3]);

        Ok(())
    }

    #[test]
    fn file_reader_splits_deletion_vector_mask_across_batches()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_rows("file-reader-dv-multi-batch", 1003)?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let mut task = first_file_task(&source, &scan)?;
        set_task_deletion_vector(&mut task, &table, &[0, 999, 1002])?;
        let reader = test_reader(&source)?;
        let result = reader.read_file(DeltaFileReadRequest {
            task: &task,
            read_schema: &read_schema,
        })?;
        let ids = collect_ids(&result.batches)?;
        let expected = (1..=1003)
            .filter(|id| ![1, 1000, 1003].contains(id))
            .collect::<Vec<_>>();

        assert!(result.batches.len() > 1);
        assert_eq!(ids, expected);

        Ok(())
    }

    #[test]
    fn file_reader_emits_empty_batch_when_all_rows_deleted()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("file-reader-dv-all-deleted")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let mut task = first_file_task(&source, &scan)?;
        set_task_deletion_vector(&mut task, &table, &[0, 1, 2])?;
        let reader = test_reader(&source)?;
        let result = reader.read_file(DeltaFileReadRequest {
            task: &task,
            read_schema: &read_schema,
        })?;

        assert_eq!(result.batches.len(), 1);
        let batch = result.batches.first().ok_or("expected one record batch")?;
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(collect_ids(&result.batches)?, Vec::<i32>::new());

        Ok(())
    }

    #[test]
    fn file_reader_keeps_all_rows_when_dv_deletes_no_rows() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = RealParquetDeltaTable::new_default("file-reader-dv-none-deleted")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let mut task = first_file_task(&source, &scan)?;
        set_task_deletion_vector(&mut task, &table, &[])?;
        let reader = test_reader(&source)?;
        let result = reader.read_file(DeltaFileReadRequest {
            task: &task,
            read_schema: &read_schema,
        })?;

        assert_eq!(collect_ids(&result.batches)?, vec![1, 2, 3]);

        Ok(())
    }

    #[test]
    fn file_reader_rejects_overlong_deletion_vector() -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("file-reader-dv-overlong")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let mut task = first_file_task(&source, &scan)?;
        set_task_deletion_vector(&mut task, &table, &[9])?;
        let reader = test_reader(&source)?;
        let error = reader
            .read_file(DeltaFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .err()
            .ok_or("expected overlong DV error")?;
        let display = error.to_string();

        assert!(display.contains("source `orders`"), "{display}");
        assert!(
            display.contains("selection-vector length mismatch"),
            "{display}"
        );
        assert!(display.contains("unconsumed entries"), "{display}");
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

    fn set_task_deletion_vector(
        task: &mut DeltaScanFileTask,
        table: &RealParquetDeltaTable,
        deleted_rows: &[u64],
    ) -> Result<(), Box<dyn std::error::Error>> {
        task.deletion_vector = write_relative_deletion_vector(table, deleted_rows)?;

        Ok(())
    }

    fn write_relative_deletion_vector(
        table: &RealParquetDeltaTable,
        deleted_rows: &[u64],
    ) -> Result<KernelScanDeletionVectorMetadata, Box<dyn std::error::Error>> {
        const RELATIVE_DV_ID: &str = "vBn[lx{q8@P<9BNH/isA";
        const RELATIVE_DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";

        let mut buffer = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
        let mut deletion_vector = KernelDeletionVector::new();
        deletion_vector.add_deleted_row_indexes(deleted_rows);
        let write_result = writer.write_deletion_vector(deletion_vector)?;
        writer.finalize()?;
        std::fs::write(table.path().join(RELATIVE_DV_FILE), buffer)?;

        Ok(
            KernelScanDeletionVectorMetadata::test_present_from_descriptor(
                DeletionVectorDescriptor {
                    storage_type: DeletionVectorStorageType::PersistedRelative,
                    path_or_inline_dv: RELATIVE_DV_ID.to_owned(),
                    offset: Some(write_result.offset),
                    size_in_bytes: write_result.size_in_bytes,
                    cardinality: write_result.cardinality,
                },
            ),
        )
    }

    fn collect_ids(
        batches: &[datafusion::arrow::record_batch::RecordBatch],
    ) -> Result<Vec<i32>, Box<dyn std::error::Error>> {
        let mut ids = Vec::new();

        for batch in batches {
            let id_column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("expected id Int32Array")?;
            ids.extend(id_column.values());
        }

        Ok(ids)
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
