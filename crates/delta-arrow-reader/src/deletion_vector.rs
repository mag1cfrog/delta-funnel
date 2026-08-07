//! Private deletion-vector coordinate handling.

use snafu::ResultExt;

use crate::{DeltaReaderError, error::DeletionVectorReadSnafu};

#[allow(dead_code)]
pub(crate) struct DeletionVectorSelection {
    deleted_row_indexes: Box<[u64]>,
    physical_row_count: Option<u64>,
    consumed_row_count: u64,
    deleted_cursor: usize,
    last_original_row_index: Option<u64>,
    access_mode: DeletionVectorAccessMode,
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
    pub(crate) fn try_new(deleted_row_indexes: Vec<u64>) -> Result<Self, DeltaReaderError> {
        if deleted_row_indexes
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return rejection(
                "invalid_deletion_vector_selection",
                "deleted row indexes must be strictly increasing",
            );
        }

        Ok(Self {
            deleted_row_indexes: deleted_row_indexes.into_boxed_slice(),
            physical_row_count: None,
            consumed_row_count: 0,
            deleted_cursor: 0,
            last_original_row_index: None,
            access_mode: DeletionVectorAccessMode::Unused,
            closed: false,
        })
    }

    pub(crate) fn bind_physical_row_count(
        &mut self,
        physical_row_count: u64,
    ) -> Result<(), DeltaReaderError> {
        if self.physical_row_count.is_some() {
            return rejection(
                "invalid_deletion_vector_selection",
                "physical row count was already bound",
            );
        }
        if self
            .deleted_row_indexes
            .last()
            .is_some_and(|row_index| *row_index >= physical_row_count)
        {
            return rejection(
                "invalid_deletion_vector_selection",
                "deleted row index exceeds the physical row count",
            );
        }

        self.physical_row_count = Some(physical_row_count);
        Ok(())
    }

    pub(crate) fn consume_ordered_batch(
        &mut self,
        batch_len: usize,
    ) -> Result<Vec<bool>, DeltaReaderError> {
        self.require_open()?;
        let physical_row_count = self.require_physical_row_count()?;
        let batch_len = u64::try_from(batch_len).map_err(|_| {
            rejection_error(
                "invalid_deletion_vector_coordinates",
                "batch length does not fit the deletion-vector coordinate type",
            )
        })?;
        let requested_end = self
            .consumed_row_count
            .checked_add(batch_len)
            .ok_or_else(|| {
                rejection_error(
                    "invalid_deletion_vector_coordinates",
                    "ordered deletion-vector coordinate overflow",
                )
            })?;
        if requested_end > physical_row_count {
            return rejection(
                "invalid_deletion_vector_coordinates",
                "ordered batch exceeds the physical row count",
            );
        }
        self.select_mode(DeletionVectorAccessMode::Ordered)?;

        let mut keep_mask = vec![
            true;
            usize::try_from(batch_len).map_err(|_| {
                rejection_error(
                    "invalid_deletion_vector_coordinates",
                    "batch length does not fit the host index type",
                )
            })?
        ];
        while let Some(&deleted_row_index) = self.deleted_row_indexes.get(self.deleted_cursor) {
            if deleted_row_index >= requested_end {
                break;
            }
            if deleted_row_index >= self.consumed_row_count {
                let batch_index = usize::try_from(deleted_row_index - self.consumed_row_count)
                    .map_err(|_| {
                        rejection_error(
                            "invalid_deletion_vector_coordinates",
                            "deleted row index does not fit the host index type",
                        )
                    })?;
                keep_mask[batch_index] = false;
            }
            self.deleted_cursor += 1;
        }
        self.consumed_row_count = requested_end;

        Ok(keep_mask)
    }

    pub(crate) fn select_original_row_indexes(
        &mut self,
        row_indexes: &[u64],
    ) -> Result<Vec<bool>, DeltaReaderError> {
        self.require_open()?;
        let physical_row_count = self.require_physical_row_count()?;
        let mut previous = self.last_original_row_index;
        for &row_index in row_indexes {
            if row_index >= physical_row_count {
                return rejection(
                    "invalid_deletion_vector_coordinates",
                    "original row index exceeds the physical row count",
                );
            }
            if previous.is_some_and(|last_row_index| row_index <= last_row_index) {
                return rejection(
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

    pub(crate) fn finish(&mut self) -> Result<(), DeltaReaderError> {
        self.require_open()?;
        self.closed = true;

        if self.access_mode == DeletionVectorAccessMode::Ordered
            && self.consumed_row_count != self.require_physical_row_count()?
        {
            return rejection(
                "invalid_deletion_vector_coordinates",
                "ordered deletion-vector consumption ended before the physical file",
            );
        }

        Ok(())
    }

    fn require_open(&self) -> Result<(), DeltaReaderError> {
        if self.closed {
            rejection(
                "invalid_deletion_vector_coordinates",
                "deletion-vector selection is already closed",
            )
        } else {
            Ok(())
        }
    }

    fn require_physical_row_count(&self) -> Result<u64, DeltaReaderError> {
        self.physical_row_count.ok_or_else(|| {
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
            _ => rejection(
                "invalid_deletion_vector_coordinates",
                "deletion-vector coordinate modes cannot be mixed",
            ),
        }
    }
}

#[allow(dead_code)]
fn rejection<T>(reason: &'static str, detail: &'static str) -> Result<T, DeltaReaderError> {
    Err(rejection_error(reason, detail))
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
    use std::error::Error as _;

    use super::DeletionVectorSelection;
    use crate::{DeltaReaderError, DeltaReaderPhase, kernel::is_kernel_error};

    fn selection(
        deleted_row_indexes: Vec<u64>,
        physical_row_count: u64,
    ) -> Result<DeletionVectorSelection, DeltaReaderError> {
        let mut selection = DeletionVectorSelection::try_new(deleted_row_indexes)?;
        selection.bind_physical_row_count(physical_row_count)?;
        Ok(selection)
    }

    #[test]
    fn validates_owned_deleted_row_indexes_and_row_bound() {
        for indexes in [vec![2, 1], vec![1, 1]] {
            let error = DeletionVectorSelection::try_new(indexes)
                .err()
                .expect("unordered or duplicate indexes must fail");
            assert_eq!(error.as_str(), "deletion_vector_read");
            assert_eq!(error.phase(), DeltaReaderPhase::DeletionVector);
            assert!(error.source().is_some_and(is_kernel_error));
        }

        let mut selection =
            DeletionVectorSelection::try_new(vec![1, 3]).expect("ordered indexes are valid");
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
        let mut selection = DeletionVectorSelection::try_new(Vec::new())?;
        assert!(selection.consume_ordered_batch(0).is_err());
        selection.bind_physical_row_count(0)?;
        assert!(selection.bind_physical_row_count(0).is_err());
        selection.finish()?;
        Ok(())
    }
}
