//! Private deletion-vector coordinate handling and Arrow masking.

use arrow::{array::BooleanArray, compute::filter_record_batch, record_batch::RecordBatch};
use snafu::ResultExt;

use crate::{DeltaReadMetrics, DeltaReaderError, error::DeletionVectorReadSnafu};

#[allow(dead_code)]
pub(crate) struct DeletionVectorSelection {
    deleted_row_indexes: Box<[u64]>,
    physical_row_count: Option<u64>,
    consumed_row_count: u64,
    deleted_cursor: usize,
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
        deleted_row_indexes: Vec<u64>,
        metrics: DeltaReadMetrics,
    ) -> Result<Self, DeltaReaderError> {
        if deleted_row_indexes
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            metrics.record_deletion_vector_rejection();
            return Err(rejection_error(
                "invalid_deletion_vector_selection",
                "deleted row indexes must be strictly increasing",
            ));
        }

        Ok(Self {
            deleted_row_indexes: deleted_row_indexes.into_boxed_slice(),
            physical_row_count: None,
            consumed_row_count: 0,
            deleted_cursor: 0,
            last_original_row_index: None,
            access_mode: DeletionVectorAccessMode::Unused,
            metrics,
            applied: false,
            closed: false,
        })
    }

    pub(crate) fn bind_physical_row_count(
        &mut self,
        physical_row_count: u64,
    ) -> Result<(), DeltaReaderError> {
        if self.physical_row_count.is_some() {
            return self.reject(
                "invalid_deletion_vector_selection",
                "physical row count was already bound",
            );
        }
        if self
            .deleted_row_indexes
            .last()
            .is_some_and(|row_index| *row_index >= physical_row_count)
        {
            return self.reject(
                "invalid_deletion_vector_selection",
                "deleted row index exceeds the physical row count",
            );
        }

        self.physical_row_count = Some(physical_row_count);
        Ok(())
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
        row_indexes: &[u64],
    ) -> Result<RecordBatch, DeltaReaderError> {
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
        let physical_row_count = self.require_physical_row_count()?;
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
        if requested_end > physical_row_count {
            return self.reject(
                "invalid_deletion_vector_coordinates",
                "ordered batch exceeds the physical row count",
            );
        }
        self.select_mode(DeletionVectorAccessMode::Ordered)?;

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
        while let Some(&deleted_row_index) = self.deleted_row_indexes.get(self.deleted_cursor) {
            if deleted_row_index >= requested_end {
                break;
            }
            if deleted_row_index >= self.consumed_row_count {
                let batch_index = match usize::try_from(deleted_row_index - self.consumed_row_count)
                {
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
            self.deleted_cursor += 1;
        }
        self.consumed_row_count = requested_end;

        Ok(keep_mask)
    }

    fn select_original_row_indexes(
        &mut self,
        row_indexes: &[u64],
    ) -> Result<Vec<bool>, DeltaReaderError> {
        self.require_open()?;
        let physical_row_count = self.require_physical_row_count()?;
        let mut previous = self.last_original_row_index;
        for &row_index in row_indexes {
            if row_index >= physical_row_count {
                return self.reject(
                    "invalid_deletion_vector_coordinates",
                    "original row index exceeds the physical row count",
                );
            }
            if previous.is_some_and(|last_row_index| row_index <= last_row_index) {
                return self.reject(
                    "invalid_deletion_vector_coordinates",
                    "original row indexes must be strictly increasing",
                );
            }
            previous = Some(row_index);
        }
        self.select_mode(DeletionVectorAccessMode::OriginalRowIndex)?;

        let mut keep_mask = Vec::with_capacity(row_indexes.len());
        for &row_index in row_indexes {
            while self
                .deleted_row_indexes
                .get(self.deleted_cursor)
                .is_some_and(|deleted_row_index| *deleted_row_index < row_index)
            {
                self.deleted_cursor += 1;
            }
            keep_mask.push(
                self.deleted_row_indexes
                    .get(self.deleted_cursor)
                    .is_none_or(|deleted_row_index| *deleted_row_index != row_index),
            );
            self.last_original_row_index = Some(row_index);
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

        if self.access_mode == DeletionVectorAccessMode::Ordered
            && self.consumed_row_count != self.require_physical_row_count()?
        {
            return self.reject(
                "invalid_deletion_vector_coordinates",
                "ordered deletion-vector consumption ended before the physical file",
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

    fn require_physical_row_count(&self) -> Result<u64, DeltaReaderError> {
        self.physical_row_count.ok_or_else(|| {
            self.metrics.record_deletion_vector_rejection();
            rejection_error(
                "invalid_deletion_vector_selection",
                "physical row count is not bound",
            )
        })
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
    Err::<(), _>(delta_kernel::Error::generic(detail))
        .boxed()
        .context(DeletionVectorReadSnafu { reason })
        .expect_err("constructed deletion-vector rejection")
}

#[cfg(test)]
mod tests {
    use std::{error::Error as _, sync::Arc};

    use arrow::{
        array::{ArrayRef, Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::DeletionVectorSelection;
    use crate::{
        DeltaReadMetrics, DeltaReaderBackend, DeltaReaderError, DeltaReaderPhase,
        kernel::is_kernel_error, metrics::DeltaReadMetricsConfig,
    };

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
        physical_row_count: u64,
    ) -> Result<DeletionVectorSelection, DeltaReaderError> {
        let mut selection = DeletionVectorSelection::try_new(deleted_row_indexes, metrics())?;
        selection.bind_physical_row_count(physical_row_count)?;
        Ok(selection)
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

    #[test]
    fn validates_owned_deleted_row_indexes_and_row_bound() {
        for indexes in [vec![2, 1], vec![1, 1]] {
            let error = DeletionVectorSelection::try_new(indexes, metrics())
                .err()
                .expect("unordered or duplicate indexes must fail");
            assert_eq!(error.as_str(), "deletion_vector_read");
            assert_eq!(error.phase(), DeltaReaderPhase::DeletionVector);
            assert!(error.source().is_some_and(is_kernel_error));
        }

        let mut selection = DeletionVectorSelection::try_new(vec![1, 3], metrics())
            .expect("ordered indexes are valid");
        let error = selection
            .bind_physical_row_count(3)
            .expect_err("row index equal to the row count must fail");
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=deletion_vector error=deletion_vector_read reason=invalid_deletion_vector_selection"
        );
    }

    #[test]
    fn ordered_mode_tracks_physical_rows_across_batches() -> Result<(), DeltaReaderError> {
        let mut selection = selection(vec![1, 4], 6)?;

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
        let mut none = selection(Vec::new(), 3)?;
        let mut all = selection(vec![0, 1, 2], 3)?;

        assert_eq!(none.consume_ordered_batch(3)?, [true; 3]);
        assert_eq!(all.consume_ordered_batch(3)?, [false; 3]);
        none.finish()?;
        all.finish()?;
        Ok(())
    }

    #[test]
    fn ordered_mode_rejects_overrun_underrun_and_use_after_finish() -> Result<(), DeltaReaderError>
    {
        let mut overrun = selection(vec![1], 2)?;
        assert!(overrun.consume_ordered_batch(3).is_err());

        let mut underrun = selection(vec![1], 3)?;
        underrun.consume_ordered_batch(2)?;
        assert!(underrun.finish().is_err());
        assert!(underrun.consume_ordered_batch(1).is_err());
        Ok(())
    }

    #[test]
    fn original_index_mode_handles_sparse_monotonic_batches() -> Result<(), DeltaReaderError> {
        let mut selection = selection(vec![1, 4, 7], 10)?;

        assert_eq!(
            selection.select_original_row_indexes(&[0, 1, 3])?,
            [true, false, true]
        );
        assert_eq!(
            selection.select_original_row_indexes(&[4, 8, 9])?,
            [false, true, true]
        );
        selection.finish()?;
        Ok(())
    }

    #[test]
    fn original_index_mode_rejects_duplicate_decreasing_and_out_of_domain_indexes()
    -> Result<(), DeltaReaderError> {
        for indexes in [&[1, 1][..], &[2, 1], &[1, 3]] {
            let mut selection = selection(vec![1], 3)?;
            assert!(selection.select_original_row_indexes(indexes).is_err());
        }
        Ok(())
    }

    #[test]
    fn coordinate_modes_cannot_be_mixed() -> Result<(), DeltaReaderError> {
        let mut ordered = selection(vec![1], 3)?;
        ordered.consume_ordered_batch(1)?;
        assert!(ordered.select_original_row_indexes(&[1]).is_err());

        let mut original = selection(vec![1], 3)?;
        original.select_original_row_indexes(&[0])?;
        assert!(original.consume_ordered_batch(1).is_err());
        Ok(())
    }

    #[test]
    fn physical_row_count_is_required_and_bound_once() -> Result<(), DeltaReaderError> {
        let mut selection = DeletionVectorSelection::try_new(Vec::new(), metrics())?;
        assert!(selection.consume_ordered_batch(0).is_err());
        selection.bind_physical_row_count(0)?;
        assert!(selection.bind_physical_row_count(0).is_err());
        selection.finish()?;
        Ok(())
    }

    #[test]
    fn ordered_masking_preserves_schema_row_order_and_exact_metrics() -> Result<(), DeltaReaderError>
    {
        let metrics = metrics();
        let mut selection = DeletionVectorSelection::try_new(vec![1, 3], metrics.clone())?;
        selection.bind_physical_row_count(5)?;
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
    fn masking_handles_none_all_and_sparse_original_rows() -> Result<(), DeltaReaderError> {
        let mut none = selection(Vec::new(), 3)?;
        let none_batch = none.mask_ordered_batch(batch(&[10, 11, 12]))?;
        none.finish()?;
        assert_eq!(ids(&none_batch), [10, 11, 12]);

        let mut all = selection(vec![0, 1, 2], 3)?;
        let all_batch = all.mask_ordered_batch(batch(&[10, 11, 12]))?;
        all.finish()?;
        assert_eq!(all_batch.num_rows(), 0);
        assert_eq!(all_batch.schema().field(0).name(), "id");
        assert_eq!(all_batch.schema().field(1).name(), "label");

        let mut sparse = selection(vec![2], 5)?;
        let sparse_batch = sparse.mask_original_row_indexes(batch(&[10, 12, 14]), &[0, 2, 4])?;
        sparse.finish()?;
        assert_eq!(ids(&sparse_batch), [10, 14]);
        Ok(())
    }

    #[test]
    fn coordinate_rejections_increment_only_rejection_metrics() -> Result<(), DeltaReaderError> {
        let invalid_metrics = metrics();
        let _ = DeletionVectorSelection::try_new(vec![2, 1], invalid_metrics.clone())
            .err()
            .expect("unordered indexes must fail");
        let snapshot = invalid_metrics.snapshot();
        assert_eq!(snapshot.deletion_vector_failures, 0);
        assert_eq!(snapshot.deletion_vector_rejections, 1);

        let mismatch_metrics = metrics();
        let mut selection = DeletionVectorSelection::try_new(vec![1], mismatch_metrics.clone())?;
        selection.bind_physical_row_count(3)?;
        let _ = selection
            .mask_original_row_indexes(batch(&[10, 11]), &[0])
            .expect_err("row-index count mismatch must fail");
        let snapshot = mismatch_metrics.snapshot();
        assert_eq!(snapshot.deletion_vectors_applied, 0);
        assert_eq!(snapshot.deletion_vector_failures, 0);
        assert_eq!(snapshot.deletion_vector_rejections, 1);
        Ok(())
    }
}
