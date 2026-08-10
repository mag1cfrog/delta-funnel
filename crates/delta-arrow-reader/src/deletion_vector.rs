//! Private deletion-vector coordinate handling and Arrow masking.

use arrow::{
    array::{Array, BooleanArray, Int64Array},
    compute::filter_record_batch,
    record_batch::RecordBatch,
};
use snafu::ResultExt;

use crate::{
    DeltaReadMetrics, DeltaReaderError,
    error::DeletionVectorReadSnafu,
    kernel::{DeltaKernelEngineContext, KernelDeletionVectorHandle},
    snapshot::LoadedDeltaTableSnapshot,
};

#[allow(dead_code)]
#[derive(Clone, Default)]
pub(crate) struct DeletionVectorMetadata(Option<KernelDeletionVectorHandle>);

#[allow(dead_code)]
impl DeletionVectorMetadata {
    pub(crate) fn is_present(&self) -> bool {
        self.0.is_some()
    }

    pub(crate) fn from_kernel(handle: Option<KernelDeletionVectorHandle>) -> Self {
        Self(handle)
    }
}

#[allow(dead_code)]
pub(crate) async fn load_deletion_vector_selection(
    snapshot: &LoadedDeltaTableSnapshot,
    metadata: DeletionVectorMetadata,
    metrics: &DeltaReadMetrics,
) -> Result<Option<DeletionVectorSelection>, DeltaReaderError> {
    load_deletion_vector_selection_from_engine_context(
        std::sync::Arc::clone(snapshot.engine_context()),
        metadata,
        metrics,
    )
    .await
}

#[allow(dead_code)]
pub(crate) async fn load_deletion_vector_selection_from_engine_context(
    engine_context: std::sync::Arc<DeltaKernelEngineContext>,
    metadata: DeletionVectorMetadata,
    metrics: &DeltaReadMetrics,
) -> Result<Option<DeletionVectorSelection>, DeltaReaderError> {
    let metrics_for_task = metrics.clone();
    match tokio::task::spawn_blocking(move || {
        load_deletion_vector_selection_blocking(
            engine_context.as_ref(),
            metadata,
            &metrics_for_task,
        )
    })
    .await
    {
        Ok(result) => result,
        Err(source) => {
            metrics.record_deletion_vector_failure();
            Err(dependency_error("deletion_vector_load_task_failed", source))
        }
    }
}

#[allow(dead_code)]
pub(crate) fn load_deletion_vector_selection_blocking(
    engine_context: &DeltaKernelEngineContext,
    metadata: DeletionVectorMetadata,
    metrics: &DeltaReadMetrics,
) -> Result<Option<DeletionVectorSelection>, DeltaReaderError> {
    let Some(handle) = metadata.0 else {
        return Ok(None);
    };
    let row_indexes = match engine_context.load_deletion_vector_row_indexes(&handle) {
        Ok(row_indexes) => row_indexes,
        Err(source) => {
            metrics.record_deletion_vector_failure();
            return Err(dependency_error(
                "deletion_vector_payload_read_failed",
                source,
            ));
        }
    };

    metrics.record_deletion_vector_payload_loaded();
    DeletionVectorSelection::try_new(row_indexes, metrics.clone()).map(Some)
}

#[allow(dead_code)]
pub(crate) struct DeletionVectorSelection {
    deleted_row_indexes: Box<[u64]>,
    consumed_row_count: u64,
    original_row_index_deleted_cursor: Option<usize>,
    last_original_row_index: Option<u64>,
    access_mode: DeletionVectorAccessMode,
    metrics: DeltaReadMetrics,
    applied: bool,
    closed: bool,
}

#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum DeletionVectorAccessMode {
    Unused,
    Ordered,
    OriginalRowIndex,
}

#[allow(dead_code)]
impl DeletionVectorSelection {
    pub(crate) fn try_new(
        mut deleted_row_indexes: Vec<u64>,
        metrics: DeltaReadMetrics,
    ) -> Result<Self, DeltaReaderError> {
        deleted_row_indexes.sort_unstable();
        deleted_row_indexes.dedup();

        Ok(Self {
            deleted_row_indexes: deleted_row_indexes.into_boxed_slice(),
            consumed_row_count: 0,
            original_row_index_deleted_cursor: Some(0),
            last_original_row_index: None,
            access_mode: DeletionVectorAccessMode::Unused,
            metrics,
            applied: false,
            closed: false,
        })
    }

    pub(crate) fn mask_ordered_batch(
        &mut self,
        batch: RecordBatch,
    ) -> Result<RecordBatch, DeltaReaderError> {
        let keep_mask = self.consume_ordered_batch(batch.num_rows())?;
        self.apply_keep_mask(batch, keep_mask)
    }

    pub(crate) fn mask_original_row_indexes(
        &mut self,
        batch: RecordBatch,
        row_indexes: Option<&Int64Array>,
    ) -> Result<RecordBatch, DeltaReaderError> {
        let Some(row_indexes) = row_indexes else {
            return self.reject(
                "invalid_deletion_vector_coordinates",
                "original row indexes are missing",
            );
        };
        if row_indexes.len() != batch.num_rows() {
            return self.reject(
                "invalid_deletion_vector_coordinates",
                "original row-index count does not match the batch row count",
            );
        }
        let keep_mask = self.select_original_row_indexes(row_indexes)?;
        self.apply_keep_mask(batch, keep_mask)
    }

    fn consume_ordered_batch(&mut self, batch_len: usize) -> Result<Vec<bool>, DeltaReaderError> {
        self.require_open()?;
        self.select_mode(DeletionVectorAccessMode::Ordered)?;
        let batch_len = match u64::try_from(batch_len) {
            Ok(batch_len) => batch_len,
            Err(_) => {
                return self.reject(
                    "invalid_deletion_vector_coordinates",
                    "batch length does not fit the deletion-vector coordinate type",
                );
            }
        };
        let Some(requested_end) = self.consumed_row_count.checked_add(batch_len) else {
            return self.reject(
                "invalid_deletion_vector_coordinates",
                "ordered deletion-vector coordinate overflow",
            );
        };
        let batch_len = match usize::try_from(batch_len) {
            Ok(batch_len) => batch_len,
            Err(_) => {
                return self.reject(
                    "invalid_deletion_vector_coordinates",
                    "batch length does not fit the host index type",
                );
            }
        };
        let mut keep_mask = vec![true; batch_len];
        let deleted_start = self
            .deleted_row_indexes
            .partition_point(|row_index| *row_index < self.consumed_row_count);
        let deleted_end = self
            .deleted_row_indexes
            .partition_point(|row_index| *row_index < requested_end);
        for deleted_row_index in &self.deleted_row_indexes[deleted_start..deleted_end] {
            let batch_index = match usize::try_from(*deleted_row_index - self.consumed_row_count) {
                Ok(batch_index) => batch_index,
                Err(_) => {
                    return self.reject(
                        "invalid_deletion_vector_coordinates",
                        "deleted row index does not fit the host index type",
                    );
                }
            };
            keep_mask[batch_index] = false;
        }
        self.consumed_row_count = requested_end;

        Ok(keep_mask)
    }

    fn select_original_row_indexes(
        &mut self,
        row_indexes: &Int64Array,
    ) -> Result<Vec<bool>, DeltaReaderError> {
        self.require_open()?;
        let mut validated_row_indexes = Vec::with_capacity(row_indexes.len());
        for index in 0..row_indexes.len() {
            if row_indexes.is_null(index) {
                return self.reject(
                    "invalid_deletion_vector_coordinates",
                    "original row index is missing",
                );
            }
            let Ok(row_index) = u64::try_from(row_indexes.value(index)) else {
                return self.reject(
                    "invalid_deletion_vector_coordinates",
                    "original row index is negative",
                );
            };
            validated_row_indexes.push(row_index);
        }
        self.select_mode(DeletionVectorAccessMode::OriginalRowIndex)?;

        let mut keep_mask = Vec::with_capacity(validated_row_indexes.len());
        let mut cursor = self.original_row_index_deleted_cursor.unwrap_or(0);
        let mut last_row_index = self.last_original_row_index;
        let mut cursor_is_valid = self.original_row_index_deleted_cursor.is_some();
        for row_index in validated_row_indexes {
            if cursor_is_valid
                && last_row_index.is_some_and(|last_row_index| row_index < last_row_index)
            {
                cursor_is_valid = false;
            }

            let keep = if cursor_is_valid {
                while self
                    .deleted_row_indexes
                    .get(cursor)
                    .is_some_and(|deleted_row_index| *deleted_row_index < row_index)
                {
                    cursor += 1;
                }
                self.deleted_row_indexes
                    .get(cursor)
                    .is_none_or(|deleted_row_index| *deleted_row_index != row_index)
            } else {
                self.deleted_row_indexes.binary_search(&row_index).is_err()
            };
            keep_mask.push(keep);
            last_row_index = Some(row_index);
        }

        if cursor_is_valid {
            self.original_row_index_deleted_cursor = Some(cursor);
            self.last_original_row_index = last_row_index;
        } else {
            self.original_row_index_deleted_cursor = None;
            self.last_original_row_index = None;
        }

        Ok(keep_mask)
    }

    fn apply_keep_mask(
        &mut self,
        batch: RecordBatch,
        keep_mask: Vec<bool>,
    ) -> Result<RecordBatch, DeltaReaderError> {
        if !self.applied {
            self.metrics.record_deletion_vector_applied();
            self.applied = true;
        }
        let deleted_rows = keep_mask.iter().filter(|keep| !**keep).count();
        let result = if deleted_rows == 0 {
            Ok(batch)
        } else if deleted_rows == batch.num_rows() {
            Ok(RecordBatch::new_empty(batch.schema()))
        } else {
            filter_record_batch(&batch, &BooleanArray::from(keep_mask))
                .boxed()
                .context(DeletionVectorReadSnafu {
                    reason: "deletion_vector_masking_failed",
                })
        };

        match result {
            Ok(batch) => {
                self.metrics
                    .record_deletion_vector_rows_deleted(deleted_rows);
                Ok(batch)
            }
            Err(error) => {
                self.metrics.record_deletion_vector_failure();
                Err(error)
            }
        }
    }

    pub(crate) fn finish(&mut self) -> Result<(), DeltaReaderError> {
        self.require_open()?;
        self.closed = true;

        if self.access_mode == DeletionVectorAccessMode::OriginalRowIndex {
            return Ok(());
        }

        let consumed_deleted = self
            .deleted_row_indexes
            .partition_point(|row_index| *row_index < self.consumed_row_count);
        if consumed_deleted < self.deleted_row_indexes.len() {
            return self.reject(
                "invalid_deletion_vector_coordinates",
                "deletion-vector entries remain after physical file completion",
            );
        }

        Ok(())
    }

    fn require_open(&self) -> Result<(), DeltaReaderError> {
        if self.closed {
            self.reject(
                "invalid_deletion_vector_coordinates",
                "deletion-vector selection is already closed",
            )
        } else {
            Ok(())
        }
    }

    fn select_mode(&mut self, mode: DeletionVectorAccessMode) -> Result<(), DeltaReaderError> {
        match self.access_mode {
            DeletionVectorAccessMode::Unused => {
                self.access_mode = mode;
                Ok(())
            }
            current if current == mode => Ok(()),
            _ => self.reject(
                "invalid_deletion_vector_coordinates",
                "deletion-vector coordinate modes cannot be mixed",
            ),
        }
    }

    fn reject<T>(&self, reason: &'static str, detail: &'static str) -> Result<T, DeltaReaderError> {
        self.metrics.record_deletion_vector_rejection();
        Err(rejection_error(reason, detail))
    }
}

#[allow(dead_code)]
fn rejection_error(reason: &'static str, detail: &'static str) -> DeltaReaderError {
    dependency_error(reason, delta_kernel::Error::generic(detail))
}

fn dependency_error(
    reason: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> DeltaReaderError {
    Err::<(), _>(source)
        .boxed()
        .context(DeletionVectorReadSnafu { reason })
        .expect_err("constructed deletion-vector error")
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as _,
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow::{
        array::{ArrayRef, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use delta_kernel::actions::deletion_vector::{
        DeletionVectorDescriptor, DeletionVectorStorageType,
    };
    use delta_kernel::actions::deletion_vector_writer::{
        KernelDeletionVector, StreamingDeletionVectorWriter,
    };
    use delta_kernel::scan::state::DvInfo;

    use super::{DeletionVectorMetadata, DeletionVectorSelection, load_deletion_vector_selection};
    use crate::{
        DeltaReadMetrics, DeltaReaderBackend, DeltaReaderError, DeltaReaderPhase,
        DeltaSnapshotSelection, DeltaStorageOptions,
        kernel::{is_kernel_error, preserve_deletion_vector},
        metrics::DeltaReadMetricsConfig,
        snapshot::{LoadedDeltaTableSnapshot, load_delta_table_snapshot_blocking},
    };

    const INLINE_DV_DELETED_ROW_INDEXES: &[u64] = &[3, 4, 7, 11, 18, 29];
    const RELATIVE_DV_ID: &str = "vBn[lx{q8@P<9BNH/isA";
    const RELATIVE_DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";
    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const METADATA_JSON: &str = r#"{"metaData":{"id":"delta-arrow-reader-dv-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;

    struct DeltaLogTable(PathBuf);

    impl DeltaLogTable {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-arrow-reader-deletion-vector-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{PROTOCOL_JSON}\n{METADATA_JSON}\n"),
            )?;
            Ok(Self(path))
        }

        fn snapshot(&self) -> Result<LoadedDeltaTableSnapshot, DeltaReaderError> {
            load_delta_table_snapshot_blocking(
                &self.0.to_string_lossy(),
                &DeltaStorageOptions::new(),
                DeltaSnapshotSelection::Latest,
            )
        }
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(format!("{}-{name}-{nanos}", std::process::id()))
    }

    fn metadata(descriptor: DeletionVectorDescriptor) -> DeletionVectorMetadata {
        DeletionVectorMetadata::from_kernel(preserve_deletion_vector(descriptor.into()))
    }

    fn inline_metadata() -> Result<DeletionVectorMetadata, delta_kernel::Error> {
        DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::Inline,
            "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
            None,
            44,
            6,
        )
        .map(metadata)
    }

    fn write_relative_metadata(
        table: &DeltaLogTable,
        deleted_rows: impl IntoIterator<Item = u64>,
    ) -> Result<DeletionVectorMetadata, Box<dyn std::error::Error>> {
        let mut buffer = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
        let mut deletion_vector = KernelDeletionVector::new();
        deletion_vector.add_deleted_row_indexes(deleted_rows);
        let write_result = writer.write_deletion_vector(deletion_vector)?;
        writer.finalize()?;
        fs::write(table.0.join(RELATIVE_DV_FILE), buffer)?;

        Ok(metadata(DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::PersistedRelative,
            RELATIVE_DV_ID,
            Some(write_result.offset),
            write_result.size_in_bytes,
            write_result.cardinality,
        )?))
    }

    fn missing_relative_metadata() -> Result<DeletionVectorMetadata, delta_kernel::Error> {
        DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::PersistedRelative,
            RELATIVE_DV_ID,
            Some(1),
            36,
            2,
        )
        .map(metadata)
    }

    #[test]
    fn lazy_loader_skips_absence_and_loads_inline_selection()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("inline")?;
        let snapshot = table.snapshot()?;
        let engine_context = Arc::clone(snapshot.engine_context());
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let absent_metrics = metrics();
        let absent_metadata =
            DeletionVectorMetadata::from_kernel(preserve_deletion_vector(DvInfo::default()));
        let absent = runtime.block_on(load_deletion_vector_selection(
            &snapshot,
            absent_metadata,
            &absent_metrics,
        ))?;
        assert!(absent.is_none());
        assert_eq!(absent_metrics.snapshot().deletion_vector_payloads_loaded, 0);

        let inline_metrics = metrics();
        let inline_metadata = inline_metadata()?;
        assert!(inline_metadata.is_present());
        let selection = runtime
            .block_on(load_deletion_vector_selection(
                &snapshot,
                inline_metadata,
                &inline_metrics,
            ))?
            .expect("inline descriptor must produce a selection");
        assert_eq!(
            selection.deleted_row_indexes.as_ref(),
            INLINE_DV_DELETED_ROW_INDEXES
        );
        let metrics = inline_metrics.snapshot();
        assert_eq!(metrics.deletion_vector_payloads_loaded, 1);
        assert_eq!(metrics.deletion_vector_failures, 0);
        assert_eq!(metrics.deletion_vector_rejections, 0);
        assert!(Arc::ptr_eq(&engine_context, snapshot.engine_context()));
        Ok(())
    }

    #[test]
    fn lazy_loader_reads_relative_and_empty_kernel_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("relative")?;
        let snapshot = table.snapshot()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let relative_metrics = metrics();
        let relative_metadata = write_relative_metadata(&table, [0, 9])?;
        let selection = runtime
            .block_on(load_deletion_vector_selection(
                &snapshot,
                relative_metadata,
                &relative_metrics,
            ))?
            .expect("relative descriptor must produce a selection");
        assert_eq!(selection.deleted_row_indexes.as_ref(), [0, 9]);
        assert_eq!(
            relative_metrics.snapshot().deletion_vector_payloads_loaded,
            1
        );

        let empty_metrics = metrics();
        let empty_metadata = write_relative_metadata(&table, [])?;
        let mut empty = runtime
            .block_on(load_deletion_vector_selection(
                &snapshot,
                empty_metadata,
                &empty_metrics,
            ))?
            .expect("empty present descriptor must produce a selection");
        assert!(empty.deleted_row_indexes.is_empty());
        empty.finish()?;
        let metrics = empty_metrics.snapshot();
        assert_eq!(metrics.deletion_vector_payloads_loaded, 1);
        assert_eq!(metrics.deletion_vectors_applied, 0);
        Ok(())
    }

    #[test]
    fn lazy_loader_maps_payload_failures_once_and_redacts_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("secret-token")?;
        let snapshot = table.snapshot()?;
        let missing = missing_relative_metadata()?;
        let metrics = metrics();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let error = runtime
            .block_on(load_deletion_vector_selection(&snapshot, missing, &metrics))
            .err()
            .expect("missing payload must fail");

        assert_eq!(error.as_str(), "deletion_vector_read");
        assert_eq!(error.phase(), DeltaReaderPhase::DeletionVector);
        assert!(error.source().is_some_and(is_kernel_error));
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=deletion_vector error=deletion_vector_read reason=deletion_vector_payload_read_failed"
        );
        let debug = format!("{error:?}");
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains(RELATIVE_DV_ID));
        let metrics = metrics.snapshot();
        assert_eq!(metrics.deletion_vector_payloads_loaded, 0);
        assert_eq!(metrics.deletion_vector_failures, 1);
        assert_eq!(metrics.deletion_vector_rejections, 0);
        Ok(())
    }

    #[test]
    fn lazy_loader_leaves_malformed_and_truncated_payloads_to_kernel()
    -> Result<(), Box<dyn std::error::Error>> {
        const MALFORMED_INLINE: &str = "not-valid-inline-payload";

        let table = DeltaLogTable::new("hostile-payload")?;
        fs::write(table.0.join(RELATIVE_DV_FILE), [0_u8, 1, 2])?;
        let snapshot = table.snapshot()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let malformed = metadata(DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::Inline,
            MALFORMED_INLINE,
            None,
            4,
            1,
        )?);
        let truncated = missing_relative_metadata()?;

        for metadata in [malformed, truncated] {
            let metrics = metrics();
            let error = runtime
                .block_on(load_deletion_vector_selection(
                    &snapshot, metadata, &metrics,
                ))
                .err()
                .expect("invalid Kernel payload must fail");
            assert_eq!(error.as_str(), "deletion_vector_read");
            assert!(error.source().is_some_and(is_kernel_error));
            assert!(!error.to_string().contains(MALFORMED_INLINE));
            assert!(!format!("{error:?}").contains(RELATIVE_DV_ID));
            let metrics = metrics.snapshot();
            assert_eq!(metrics.deletion_vector_payloads_loaded, 0);
            assert_eq!(metrics.deletion_vector_failures, 1);
            assert_eq!(metrics.deletion_vector_rejections, 0);
            assert_eq!(metrics.parquet_data_file_range_get_operations, Some(0));
            assert_eq!(metrics.parquet_data_file_full_get_operations, Some(0));
        }
        Ok(())
    }

    fn metrics() -> DeltaReadMetrics {
        DeltaReadMetrics::new(DeltaReadMetricsConfig {
            snapshot_version: 7,
            reader_backend: DeltaReaderBackend::NativeAsync,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 1,
            files_planned: 1,
            files_filtered_during_planning: Some(0),
            estimated_rows: None,
            estimated_bytes: None,
        })
    }

    fn selection(
        deleted_row_indexes: Vec<u64>,
    ) -> Result<DeletionVectorSelection, DeltaReaderError> {
        DeletionVectorSelection::try_new(deleted_row_indexes, metrics())
    }

    fn batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let labels = ids.iter().map(|id| format!("row-{id}")).collect::<Vec<_>>();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(labels)) as ArrayRef,
            ],
        )
        .expect("valid test batch")
    }

    fn ids(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 id column")
            .values()
            .to_vec()
    }

    fn row_indexes(values: &[i64]) -> Int64Array {
        Int64Array::from(values.to_vec())
    }

    #[test]
    fn deleted_row_indexes_are_sorted_deduplicated_and_inverted() -> Result<(), DeltaReaderError> {
        let metrics = metrics();
        let mut selection = DeletionVectorSelection::try_new(vec![3, 1, 3], metrics.clone())?;

        assert_eq!(selection.deleted_row_indexes.as_ref(), [1, 3]);
        assert_eq!(
            selection.select_original_row_indexes(&row_indexes(&[0, 1, 2, 3, 4]))?,
            [true, false, true, false, true]
        );
        selection.finish()?;
        assert_eq!(metrics.snapshot().deletion_vector_rejections, 0);
        Ok(())
    }

    #[test]
    fn ordered_mode_tracks_physical_rows_across_batches() -> Result<(), DeltaReaderError> {
        let mut selection = selection(vec![1, 4])?;

        assert_eq!(selection.consume_ordered_batch(2)?, [true, false]);
        assert_eq!(selection.consume_ordered_batch(0)?, Vec::<bool>::new());
        assert_eq!(
            selection.consume_ordered_batch(4)?,
            [true, true, false, true]
        );
        selection.finish()?;
        Ok(())
    }

    #[test]
    fn ordered_mode_handles_none_and_all_deleted() -> Result<(), DeltaReaderError> {
        let mut none = selection(Vec::new())?;
        let mut all = selection(vec![0, 1, 2])?;

        assert_eq!(none.consume_ordered_batch(3)?, [true; 3]);
        assert_eq!(all.consume_ordered_batch(3)?, [false; 3]);
        none.finish()?;
        all.finish()?;
        Ok(())
    }

    #[test]
    fn ordered_mode_pads_live_tail_and_rejects_unconsumed_entries_and_use_after_finish()
    -> Result<(), DeltaReaderError> {
        let mut padded = selection(vec![1])?;
        assert_eq!(
            padded.consume_ordered_batch(5)?,
            [true, false, true, true, true]
        );
        padded.finish()?;

        let mut overflow = selection(Vec::new())?;
        overflow.consumed_row_count = u64::MAX;
        assert!(overflow.consume_ordered_batch(1).is_err());

        let mut underrun = selection(vec![2])?;
        underrun.consume_ordered_batch(2)?;
        assert!(underrun.finish().is_err());
        assert!(underrun.consume_ordered_batch(1).is_err());
        Ok(())
    }

    #[test]
    fn original_index_mode_handles_sparse_monotonic_batches() -> Result<(), DeltaReaderError> {
        let mut selection = selection(vec![1, 4, 7])?;

        assert_eq!(
            selection.select_original_row_indexes(&row_indexes(&[0, 1, 3]))?,
            [true, false, true]
        );
        assert_eq!(
            selection.select_original_row_indexes(&row_indexes(&[4, 8, 9]))?,
            [false, true, true]
        );
        selection.finish()?;
        Ok(())
    }

    #[test]
    fn original_index_mode_falls_back_for_unsorted_and_duplicate_rows()
    -> Result<(), DeltaReaderError> {
        let mut selection = selection(vec![1, 3])?;

        assert_eq!(
            selection.select_original_row_indexes(&row_indexes(&[3, 1, 1, 4]))?,
            [false, false, false, true]
        );
        assert_eq!(selection.original_row_index_deleted_cursor, None);
        assert_eq!(
            selection.select_original_row_indexes(&row_indexes(&[1, 2]))?,
            [false, true]
        );
        selection.finish()?;
        Ok(())
    }

    #[test]
    fn original_index_mode_rejects_missing_and_negative_indexes() -> Result<(), DeltaReaderError> {
        for indexes in [Int64Array::from(vec![Some(0), None]), row_indexes(&[-1])] {
            let mut selection = selection(vec![1])?;
            assert!(selection.select_original_row_indexes(&indexes).is_err());
        }
        Ok(())
    }

    #[test]
    fn coordinate_modes_cannot_be_mixed() -> Result<(), DeltaReaderError> {
        let mut ordered = selection(vec![1])?;
        ordered.consume_ordered_batch(1)?;
        assert!(
            ordered
                .select_original_row_indexes(&row_indexes(&[1]))
                .is_err()
        );

        let mut original = selection(vec![1])?;
        original.select_original_row_indexes(&row_indexes(&[0]))?;
        assert!(original.consume_ordered_batch(1).is_err());
        Ok(())
    }

    #[test]
    fn ordered_mode_requires_no_upfront_physical_row_count() -> Result<(), DeltaReaderError> {
        let mut selection = DeletionVectorSelection::try_new(Vec::new(), metrics())?;
        assert_eq!(selection.consume_ordered_batch(0)?, Vec::<bool>::new());
        assert_eq!(selection.consume_ordered_batch(3)?, [true; 3]);
        selection.finish()?;
        Ok(())
    }

    #[test]
    fn ordered_masking_preserves_schema_row_order_and_exact_metrics() -> Result<(), DeltaReaderError>
    {
        let metrics = metrics();
        let mut selection = DeletionVectorSelection::try_new(vec![1, 3], metrics.clone())?;
        let first = batch(&[10, 11]);
        let schema = first.schema();

        let first = selection.mask_ordered_batch(first)?;
        let second = selection.mask_ordered_batch(batch(&[12, 13, 14]))?;
        selection.finish()?;

        assert_eq!(ids(&first), [10]);
        assert_eq!(ids(&second), [12, 14]);
        assert_eq!(first.schema(), schema);
        assert_eq!(second.schema(), schema);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.deletion_vectors_applied, 1);
        assert_eq!(snapshot.deletion_vector_rows_deleted, 2);
        assert_eq!(snapshot.deletion_vector_failures, 0);
        assert_eq!(snapshot.deletion_vector_rejections, 0);
        Ok(())
    }

    #[test]
    fn dropping_selection_preserves_partial_masking_metrics() -> Result<(), DeltaReaderError> {
        let metrics = metrics();
        let mut selection = DeletionVectorSelection::try_new(vec![1, 3], metrics.clone())?;
        let masked = selection.mask_ordered_batch(batch(&[10, 11]))?;
        assert_eq!(ids(&masked), [10]);
        drop(selection);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.deletion_vectors_applied, 1);
        assert_eq!(snapshot.deletion_vector_rows_deleted, 1);
        assert_eq!(snapshot.deletion_vector_failures, 0);
        assert_eq!(snapshot.deletion_vector_rejections, 0);
        Ok(())
    }

    #[test]
    fn masking_handles_none_all_and_sparse_original_rows() -> Result<(), DeltaReaderError> {
        let mut none = selection(Vec::new())?;
        let none_batch = none.mask_ordered_batch(batch(&[10, 11, 12]))?;
        none.finish()?;
        assert_eq!(ids(&none_batch), [10, 11, 12]);

        let mut all = selection(vec![0, 1, 2])?;
        let all_batch = all.mask_ordered_batch(batch(&[10, 11, 12]))?;
        all.finish()?;
        assert_eq!(all_batch.num_rows(), 0);
        assert_eq!(all_batch.schema().field(0).name(), "id");
        assert_eq!(all_batch.schema().field(1).name(), "label");

        let mut sparse = selection(vec![2])?;
        let sparse_batch = sparse
            .mask_original_row_indexes(batch(&[10, 12, 14]), Some(&row_indexes(&[0, 2, 4])))?;
        sparse.finish()?;
        assert_eq!(ids(&sparse_batch), [10, 14]);
        Ok(())
    }

    #[test]
    fn coordinate_rejections_increment_only_rejection_metrics() -> Result<(), DeltaReaderError> {
        let mismatch_metrics = metrics();
        let mut selection = DeletionVectorSelection::try_new(vec![1], mismatch_metrics.clone())?;
        let _ = selection
            .mask_original_row_indexes(batch(&[10, 11]), Some(&row_indexes(&[0])))
            .expect_err("row-index count mismatch must fail");
        let snapshot = mismatch_metrics.snapshot();
        assert_eq!(snapshot.deletion_vectors_applied, 0);
        assert_eq!(snapshot.deletion_vector_failures, 0);
        assert_eq!(snapshot.deletion_vector_rejections, 1);

        let missing_metrics = metrics();
        let mut selection = DeletionVectorSelection::try_new(vec![1], missing_metrics.clone())?;
        let _ = selection
            .mask_original_row_indexes(batch(&[10, 11]), None)
            .expect_err("missing row indexes must fail");
        let snapshot = missing_metrics.snapshot();
        assert_eq!(snapshot.deletion_vectors_applied, 0);
        assert_eq!(snapshot.deletion_vector_failures, 0);
        assert_eq!(snapshot.deletion_vector_rejections, 1);
        Ok(())
    }

    #[test]
    fn terminal_errors_increment_exactly_one_failure_or_rejection()
    -> Result<(), Box<dyn std::error::Error>> {
        let selection_metrics = metrics();
        let mut selection = DeletionVectorSelection::try_new(vec![2], selection_metrics.clone())?;
        let _ = selection
            .finish()
            .expect_err("unconsumed selection must fail");

        let coordinate_metrics = metrics();
        let mut selection = DeletionVectorSelection::try_new(vec![1], coordinate_metrics.clone())?;
        let _ = selection
            .mask_original_row_indexes(batch(&[10]), None)
            .expect_err("missing coordinates must fail");

        let payload_metrics = metrics();
        let table = DeltaLogTable::new("terminal-classification")?;
        let snapshot = table.snapshot()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let _ = runtime
            .block_on(load_deletion_vector_selection(
                &snapshot,
                missing_relative_metadata()?,
                &payload_metrics,
            ))
            .err()
            .expect("missing payload must fail");

        for (name, snapshot, failures, rejections) in [
            ("selection", selection_metrics.snapshot(), 0, 1),
            ("coordinate", coordinate_metrics.snapshot(), 0, 1),
            ("payload", payload_metrics.snapshot(), 1, 0),
        ] {
            assert_eq!(snapshot.deletion_vector_failures, failures, "{name}");
            assert_eq!(snapshot.deletion_vector_rejections, rejections, "{name}");
            assert_eq!(failures + rejections, 1, "{name}");
        }
        Ok(())
    }

    #[test]
    fn production_boundary_reuses_kernel_context_without_an_extra_decoder_or_runtime() {
        let deletion_vector_source = include_str!("deletion_vector.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production source");
        let kernel_source = include_str!("kernel.rs");

        for forbidden in [
            "DefaultEngineBuilder",
            "store_from_url_opts",
            "Runtime::",
            "Roaring",
            "z85",
            "datafusion",
            "tracing::",
        ] {
            assert!(!deletion_vector_source.contains(forbidden), "{forbidden}");
        }
        assert_eq!(
            kernel_source.matches("DefaultEngineBuilder::new").count(),
            1
        );
        assert_eq!(kernel_source.matches("store_from_url_opts(").count(), 1);
        assert_eq!(kernel_source.matches("get_row_indexes(").count(), 1);
    }
}
