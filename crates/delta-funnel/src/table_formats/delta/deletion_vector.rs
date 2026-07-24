//! Private Delta Kernel deletion-vector selection adapter.
//!
//! This module keeps deletion-vector payload reads lazy and scoped to one
//! provider data-file read. Scan metadata planning preserves only descriptors;
//! execution code must come through this boundary to materialize row selection.

use crate::{
    DeltaFunnelError,
    error::{DeltaScanDeletionVectorPhase, DeltaScanDeletionVectorSnafu},
};
use snafu::ResultExt;

use super::{DeltaKernelEngineContext, KernelScanDeletionVectorMetadata, kernel};

/// Context required to construct the official-kernel DV reader baseline.
#[allow(dead_code)]
pub(crate) struct KernelDeletionVectorReaderConfig<'a> {
    /// DataFusion table name for diagnostics.
    pub(crate) source_name: &'a str,
    /// Snapshot version that selected this file.
    pub(crate) snapshot_version: u64,
    /// Source-owned Delta Kernel infrastructure.
    pub(crate) engine_context: std::sync::Arc<DeltaKernelEngineContext>,
}

/// Reusable official-kernel DV reader baseline for one provider scan context.
#[allow(dead_code)]
pub(crate) struct KernelDeletionVectorReader {
    source_name: String,
    table_uri: String,
    snapshot_version: u64,
    engine_context: std::sync::Arc<DeltaKernelEngineContext>,
}

/// Request to load the deletion vector for one provider-selected data file.
#[allow(dead_code)]
pub(crate) struct KernelDeletionVectorReadRequest<'a> {
    /// Delta add-action table-relative file path.
    pub(crate) path: &'a str,
    /// Preserved deletion-vector metadata from scan metadata expansion.
    pub(crate) deletion_vector: &'a KernelScanDeletionVectorMetadata,
}

/// Provider-owned deleted row indexes for one physical Delta data file.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProviderDeletionVectorSelection {
    deleted_row_indexes: Box<[u64]>,
    /// Original row count consumed by the ordered full-file batch path.
    consumed_row_count: u64,
    /// Cursor into `deleted_row_indexes` for globally ascending row-index lookups.
    original_row_index_deleted_cursor: Option<usize>,
    /// Last original row index seen while the cursor optimization is valid.
    last_original_row_index: Option<u64>,
    closed: bool,
    /// Tracks which row-coordinate contract this selection is using.
    ///
    /// Ordered consumption assumes each batch is read from the physical file in
    /// original row order. Original-row-index lookup allows sparse/pruned rows
    /// as long as every emitted row carries its original physical row index.
    access_mode: ProviderDeletionVectorSelectionAccessMode,
}

/// Mutually exclusive ways to apply one DV selection.
///
/// Mixing these modes can silently shift the DV coordinate system. Once a
/// reader starts consuming a file sequentially, `consumed_row_count` is the
/// original row cursor. Once a reader starts querying by original row index,
/// there is no sequential cursor to validate because pruning or pushdown may
/// skip rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderDeletionVectorSelectionAccessMode {
    /// No masking API has been chosen yet.
    Unused,
    /// Current baseline path: full-file ordered batches consume the DV cursor.
    Ordered,
    /// Optimized path: sparse emitted rows are looked up by original row index.
    OriginalRowIndex,
}

impl KernelDeletionVectorReader {
    /// Builds a reusable official-kernel DV reader for one provider scan context.
    #[allow(dead_code)]
    pub(crate) fn new(config: KernelDeletionVectorReaderConfig<'_>) -> Self {
        Self {
            source_name: config.source_name.to_owned(),
            table_uri: config.engine_context.table_url().as_str().to_owned(),
            snapshot_version: config.snapshot_version,
            engine_context: config.engine_context,
        }
    }

    /// Lazily loads the provider deleted row indexes for one selected data file.
    #[allow(dead_code)]
    pub(crate) fn read_selection(
        &self,
        request: KernelDeletionVectorReadRequest<'_>,
    ) -> Result<Option<ProviderDeletionVectorSelection>, DeltaFunnelError> {
        let KernelScanDeletionVectorMetadata::Present(handle) = request.deletion_vector else {
            return Ok(None);
        };
        let deleted_row_indexes = handle
            .dv_info
            .get_row_indexes(
                self.engine_context.as_kernel_engine(),
                self.engine_context.table_url(),
            )
            .context(DeltaScanDeletionVectorSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanDeletionVectorPhase::PayloadRead,
            })?
            .unwrap_or_default();

        Ok(Some(
            ProviderDeletionVectorSelection::from_deleted_row_indexes(deleted_row_indexes),
        ))
    }
}

impl ProviderDeletionVectorSelection {
    /// Creates an owned deleted-row index set where each entry is a physical row to drop.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn from_deleted_row_indexes(mut deleted_row_indexes: Vec<u64>) -> Self {
        deleted_row_indexes.sort_unstable();
        deleted_row_indexes.dedup();

        Self {
            deleted_row_indexes: deleted_row_indexes.into_boxed_slice(),
            consumed_row_count: 0,
            original_row_index_deleted_cursor: Some(0),
            last_original_row_index: None,
            closed: false,
            access_mode: ProviderDeletionVectorSelectionAccessMode::Unused,
        }
    }

    /// Creates an owned keep mask where `true` means emit the physical row.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn from_keep_mask(keep_mask: Vec<bool>) -> Self {
        let deleted_row_indexes = keep_mask
            .into_iter()
            .enumerate()
            .filter_map(|(index, keep)| (!keep).then_some(index as u64))
            .collect();

        Self::from_deleted_row_indexes(deleted_row_indexes)
    }

    /// Creates an all-live keep mask for tests and no-DV normalization.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn all_live(_row_count: usize) -> Self {
        Self::from_deleted_row_indexes(Vec::new())
    }

    /// Returns the number of deleted row indexes at or after the ordered cursor.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn remaining_kernel_entries(&self) -> usize {
        let consumed = self.consumed_row_count;
        let consumed_deleted = self
            .deleted_row_indexes
            .partition_point(|row_index| *row_index < consumed);
        self.deleted_row_indexes
            .len()
            .saturating_sub(consumed_deleted)
    }

    /// Consumes the next physical batch and returns an exact-length keep mask.
    ///
    /// Delta deletion vectors store only removed rows. This derives a dense
    /// keep mask for the requested batch without materializing a full-file mask.
    #[allow(dead_code)]
    pub(crate) fn consume_batch(
        &mut self,
        batch_len: usize,
        context: ProviderDeletionVectorSelectionContext<'_>,
    ) -> Result<Vec<bool>, DeltaFunnelError> {
        if self.closed {
            return Err(kernel::DeltaKernelError::generic(
                "deletion-vector selection was consumed after file completion",
            ))
            .context(DeltaScanDeletionVectorSnafu {
                source_name: context.source_name.to_owned(),
                table_uri: context.table_uri.to_owned(),
                snapshot_version: context.snapshot_version,
                path: context.path.to_owned(),
                phase: DeltaScanDeletionVectorPhase::SelectionVectorExhaustion,
            });
        }
        match self.access_mode {
            ProviderDeletionVectorSelectionAccessMode::Unused => {
                self.access_mode = ProviderDeletionVectorSelectionAccessMode::Ordered;
            }
            ProviderDeletionVectorSelectionAccessMode::Ordered => {}
            ProviderDeletionVectorSelectionAccessMode::OriginalRowIndex => {
                return Err(kernel::DeltaKernelError::generic(
                    "cannot consume deletion-vector selection sequentially after original row-index lookup",
                ))
                .context(DeltaScanDeletionVectorSnafu {
                    source_name: context.source_name.to_owned(),
                    table_uri: context.table_uri.to_owned(),
                    snapshot_version: context.snapshot_version,
                    path: context.path.to_owned(),
                    phase: DeltaScanDeletionVectorPhase::SelectionVectorExhaustion,
                });
            }
        }

        let batch_len = u64::try_from(batch_len)
            .map_err(|_| {
                kernel::DeltaKernelError::generic("selection-vector batch length overflow")
            })
            .context(DeltaScanDeletionVectorSnafu {
                source_name: context.source_name.to_owned(),
                table_uri: context.table_uri.to_owned(),
                snapshot_version: context.snapshot_version,
                path: context.path.to_owned(),
                phase: DeltaScanDeletionVectorPhase::SelectionVectorLengthMismatch,
            })?;
        let requested_end = self
            .consumed_row_count
            .checked_add(batch_len)
            .ok_or_else(|| {
                kernel::DeltaKernelError::generic("selection-vector batch length overflow")
            })
            .context(DeltaScanDeletionVectorSnafu {
                source_name: context.source_name.to_owned(),
                table_uri: context.table_uri.to_owned(),
                snapshot_version: context.snapshot_version,
                path: context.path.to_owned(),
                phase: DeltaScanDeletionVectorPhase::SelectionVectorLengthMismatch,
            })?;
        let batch_len = usize::try_from(batch_len)
            .map_err(|_| {
                kernel::DeltaKernelError::generic("selection-vector batch length overflow")
            })
            .context(DeltaScanDeletionVectorSnafu {
                source_name: context.source_name.to_owned(),
                table_uri: context.table_uri.to_owned(),
                snapshot_version: context.snapshot_version,
                path: context.path.to_owned(),
                phase: DeltaScanDeletionVectorPhase::SelectionVectorLengthMismatch,
            })?;
        let mut batch_mask = vec![true; batch_len];
        let deleted_start = self
            .deleted_row_indexes
            .partition_point(|row_index| *row_index < self.consumed_row_count);
        let deleted_end = self
            .deleted_row_indexes
            .partition_point(|row_index| *row_index < requested_end);
        for deleted_row_index in &self.deleted_row_indexes[deleted_start..deleted_end] {
            let batch_index = usize::try_from(*deleted_row_index - self.consumed_row_count)
                .map_err(|_| {
                    kernel::DeltaKernelError::generic("deleted row index does not fit host usize")
                })
                .context(DeltaScanDeletionVectorSnafu {
                    source_name: context.source_name.to_owned(),
                    table_uri: context.table_uri.to_owned(),
                    snapshot_version: context.snapshot_version,
                    path: context.path.to_owned(),
                    phase: DeltaScanDeletionVectorPhase::SelectionVectorLengthMismatch,
                })?;
            if let Some(selected) = batch_mask.get_mut(batch_index) {
                *selected = false;
            }
        }
        self.consumed_row_count = requested_end;

        Ok(batch_mask)
    }

    /// Selects rows by their original physical row indexes.
    ///
    /// This is the correctness boundary for DV-aware pruning and predicate
    /// pushdown. Optimized readers may emit only a subset of a file, but every
    /// emitted row must still carry its original physical row index so the DV
    /// can be applied over the same coordinate system as the Delta protocol.
    #[allow(dead_code)]
    pub(crate) fn select_original_row_indexes<I>(
        &mut self,
        row_indexes: I,
        context: ProviderDeletionVectorSelectionContext<'_>,
    ) -> Result<Vec<bool>, DeltaFunnelError>
    where
        I: IntoIterator<Item = u64>,
    {
        if self.closed {
            return Err(kernel::DeltaKernelError::generic(
                "deletion-vector selection was queried after file completion",
            ))
            .context(DeltaScanDeletionVectorSnafu {
                source_name: context.source_name.to_owned(),
                table_uri: context.table_uri.to_owned(),
                snapshot_version: context.snapshot_version,
                path: context.path.to_owned(),
                phase: DeltaScanDeletionVectorPhase::SelectionVectorExhaustion,
            });
        }
        match self.access_mode {
            ProviderDeletionVectorSelectionAccessMode::Unused => {
                self.access_mode = ProviderDeletionVectorSelectionAccessMode::OriginalRowIndex;
            }
            ProviderDeletionVectorSelectionAccessMode::OriginalRowIndex => {}
            ProviderDeletionVectorSelectionAccessMode::Ordered => {
                return Err(kernel::DeltaKernelError::generic(
                    "cannot query deletion-vector selection by original row index after sequential consumption",
                ))
                .context(DeltaScanDeletionVectorSnafu {
                    source_name: context.source_name.to_owned(),
                    table_uri: context.table_uri.to_owned(),
                    snapshot_version: context.snapshot_version,
                    path: context.path.to_owned(),
                    phase: DeltaScanDeletionVectorPhase::SelectionVectorExhaustion,
                });
            }
        }

        let mut keep_mask = Vec::new();
        let mut cursor = self.original_row_index_deleted_cursor.unwrap_or(0);
        let mut last_row_index = self.last_original_row_index;
        let mut cursor_is_valid = self.original_row_index_deleted_cursor.is_some();

        for row_index in row_indexes {
            if cursor_is_valid
                && last_row_index
                    .map(|last_row_index| row_index < last_row_index)
                    .unwrap_or(false)
            {
                cursor_is_valid = false;
            }

            let keep = if cursor_is_valid {
                while cursor < self.deleted_row_indexes.len()
                    && self.deleted_row_indexes[cursor] < row_index
                {
                    cursor += 1;
                }

                self.deleted_row_indexes
                    .get(cursor)
                    .map(|deleted_row_index| *deleted_row_index != row_index)
                    .unwrap_or(true)
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

    /// Closes consumption after the physical file reader reaches EOF.
    #[allow(dead_code)]
    pub(crate) fn finish(
        &mut self,
        context: ProviderDeletionVectorSelectionContext<'_>,
    ) -> Result<(), DeltaFunnelError> {
        self.closed = true;

        if self.access_mode == ProviderDeletionVectorSelectionAccessMode::OriginalRowIndex {
            // Row-index mode may intentionally skip physical rows, so there is
            // no full-file consumption invariant to check here.
            return Ok(());
        }

        let consumed_deleted = self
            .deleted_row_indexes
            .partition_point(|row_index| *row_index < self.consumed_row_count);
        if consumed_deleted < self.deleted_row_indexes.len() {
            return Err(kernel::DeltaKernelError::generic(format!(
                "selection vector has {} unconsumed entries after file completion (deleted row indexes)",
                self.deleted_row_indexes.len() - consumed_deleted
            )))
            .context(DeltaScanDeletionVectorSnafu {
                source_name: context.source_name.to_owned(),
                table_uri: context.table_uri.to_owned(),
                snapshot_version: context.snapshot_version,
                path: context.path.to_owned(),
                phase: DeltaScanDeletionVectorPhase::SelectionVectorLengthMismatch,
            });
        }

        Ok(())
    }
}

/// Diagnostic context for provider-owned selection-vector consumption.
#[allow(dead_code)]
#[derive(Clone, Copy)]
pub(crate) struct ProviderDeletionVectorSelectionContext<'a> {
    /// DataFusion table name for diagnostics.
    pub(crate) source_name: &'a str,
    /// Normalized Delta table URI for diagnostics.
    pub(crate) table_uri: &'a str,
    /// Snapshot version that selected this file.
    pub(crate) snapshot_version: u64,
    /// Delta add-action table-relative file path.
    pub(crate) path: &'a str,
}

#[cfg(test)]
mod tests {
    use super::{
        KernelDeletionVectorReadRequest, KernelDeletionVectorReader,
        KernelDeletionVectorReaderConfig, ProviderDeletionVectorSelection,
        ProviderDeletionVectorSelectionContext,
    };
    use crate::table_formats::RealParquetDeltaTable;
    use crate::table_formats::delta::kernel::{
        DeletionVectorDescriptor, DeletionVectorStorageType,
    };
    use crate::table_formats::delta::{
        DeltaSourceConfig, KernelScanDeletionVectorHandle, KernelScanDeletionVectorMetadata,
        PlannedDeltaSource, build_projected_predicated_stats_delta_scan, load_delta_source,
    };
    use delta_kernel::actions::deletion_vector_writer::{
        KernelDeletionVector, StreamingDeletionVectorWriter,
    };

    const INLINE_DV_DELETED_ROW_INDEXES: &[u64] = &[3, 4, 7, 11, 18, 29];

    #[test]
    fn consume_batch_splits_partial_selection_vectors() -> Result<(), Box<dyn std::error::Error>> {
        let mut selection =
            ProviderDeletionVectorSelection::from_keep_mask(vec![true, false, true, true, false]);
        let context = test_context();

        assert_eq!(selection.consume_batch(2, context)?, vec![true, false]);
        assert_eq!(
            selection.consume_batch(3, context)?,
            vec![true, true, false]
        );
        selection.finish(context)?;

        Ok(())
    }

    #[test]
    fn consume_batch_supports_all_live_and_all_deleted() -> Result<(), Box<dyn std::error::Error>> {
        let context = test_context();
        let mut all_live = ProviderDeletionVectorSelection::all_live(4);
        let mut all_deleted = ProviderDeletionVectorSelection::from_keep_mask(vec![false; 4]);

        assert_eq!(all_live.consume_batch(4, context)?, vec![true; 4]);
        assert_eq!(all_deleted.consume_batch(4, context)?, vec![false; 4]);
        all_live.finish(context)?;
        all_deleted.finish(context)?;

        Ok(())
    }

    #[test]
    fn consume_batch_supports_empty_batches() -> Result<(), Box<dyn std::error::Error>> {
        let context = test_context();
        let mut selection = ProviderDeletionVectorSelection::from_keep_mask(vec![false]);

        assert_eq!(selection.consume_batch(0, context)?, Vec::<bool>::new());
        assert_eq!(selection.remaining_kernel_entries(), 1);
        assert_eq!(selection.consume_batch(1, context)?, vec![false]);
        selection.finish(context)?;

        Ok(())
    }

    #[test]
    fn deleted_row_indexes_are_sorted_deduplicated_and_inverted()
    -> Result<(), Box<dyn std::error::Error>> {
        let context = test_context();
        let mut selection =
            ProviderDeletionVectorSelection::from_deleted_row_indexes(vec![3, 1, 3]);

        assert_eq!(
            selection.select_original_row_indexes(0_u64..5, context)?,
            vec![true, false, true, false, true]
        );
        selection.finish(context)?;

        Ok(())
    }

    #[test]
    fn consume_batch_pads_underlong_tail_with_live_rows() -> Result<(), Box<dyn std::error::Error>>
    {
        let context = test_context();
        let mut selection = ProviderDeletionVectorSelection::from_keep_mask(vec![true, false]);

        assert_eq!(
            selection.consume_batch(5, context)?,
            vec![true, false, true, true, true]
        );
        selection.finish(context)?;

        Ok(())
    }

    #[test]
    fn consume_batch_keeps_padding_after_underlong_tail() -> Result<(), Box<dyn std::error::Error>>
    {
        let context = test_context();
        let mut selection = ProviderDeletionVectorSelection::from_keep_mask(vec![false]);

        assert_eq!(selection.consume_batch(2, context)?, vec![false, true]);
        assert_eq!(selection.consume_batch(3, context)?, vec![true, true, true]);
        selection.finish(context)?;

        Ok(())
    }

    #[test]
    fn select_original_row_indexes_matches_ordered_full_file_oracle()
    -> Result<(), Box<dyn std::error::Error>> {
        let context = test_context();
        let mut ordered =
            ProviderDeletionVectorSelection::from_keep_mask(vec![true, false, true, false]);
        let mut row_index =
            ProviderDeletionVectorSelection::from_keep_mask(vec![true, false, true, false]);
        let mut ordered_mask = Vec::new();

        ordered_mask.extend(ordered.consume_batch(2, context)?);
        ordered_mask.extend(ordered.consume_batch(3, context)?);
        ordered_mask.extend(ordered.consume_batch(2, context)?);
        ordered.finish(context)?;

        let row_index_mask = row_index.select_original_row_indexes(0_u64..7, context)?;
        row_index.finish(context)?;

        assert_eq!(
            row_index_mask, ordered_mask,
            "row-index DV selection must match ordered full-file baseline"
        );

        Ok(())
    }

    #[test]
    fn select_original_row_indexes_supports_sparse_pruned_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let context = test_context();
        let mut selection =
            ProviderDeletionVectorSelection::from_keep_mask(vec![true, false, true, false, false]);

        assert_eq!(
            selection.select_original_row_indexes([0, 2, 4, 5, 9], context)?,
            vec![true, true, false, true, true]
        );
        selection.finish(context)?;

        Ok(())
    }

    #[test]
    fn select_original_row_indexes_advances_cursor_for_monotonic_batches()
    -> Result<(), Box<dyn std::error::Error>> {
        let context = test_context();
        let mut selection =
            ProviderDeletionVectorSelection::from_deleted_row_indexes(vec![1, 3, 5]);

        assert_eq!(
            selection.select_original_row_indexes([0, 1, 2], context)?,
            vec![true, false, true]
        );
        assert_eq!(selection.original_row_index_deleted_cursor, Some(1));

        assert_eq!(
            selection.select_original_row_indexes([3, 4, 5], context)?,
            vec![false, true, false]
        );
        assert_eq!(selection.original_row_index_deleted_cursor, Some(2));
        selection.finish(context)?;

        Ok(())
    }

    #[test]
    fn select_original_row_indexes_falls_back_for_unsorted_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let context = test_context();
        let mut selection = ProviderDeletionVectorSelection::from_deleted_row_indexes(vec![1, 3]);

        assert_eq!(
            selection.select_original_row_indexes([3, 1, 4], context)?,
            vec![false, false, true]
        );
        assert_eq!(selection.original_row_index_deleted_cursor, None);

        assert_eq!(
            selection.select_original_row_indexes([1, 2], context)?,
            vec![false, true]
        );
        selection.finish(context)?;

        Ok(())
    }

    #[test]
    fn selection_vector_access_modes_cannot_be_mixed() -> Result<(), Box<dyn std::error::Error>> {
        let context = test_context();
        let mut ordered = ProviderDeletionVectorSelection::from_keep_mask(vec![true, false]);
        let mut row_index = ProviderDeletionVectorSelection::from_keep_mask(vec![true, false]);

        ordered.consume_batch(1, context)?;
        let ordered_error = ordered
            .select_original_row_indexes([1], context)
            .err()
            .ok_or("expected ordered to row-index mode error")?;
        assert!(
            ordered_error
                .to_string()
                .contains("after sequential consumption"),
            "{ordered_error}"
        );

        row_index.select_original_row_indexes([1], context)?;
        let row_index_error = row_index
            .consume_batch(1, context)
            .err()
            .ok_or("expected row-index to ordered mode error")?;
        assert!(
            row_index_error
                .to_string()
                .contains("after original row-index lookup"),
            "{row_index_error}"
        );

        Ok(())
    }

    #[test]
    fn finish_rejects_overlong_selection_vector() -> Result<(), Box<dyn std::error::Error>> {
        let context = test_context();
        let mut selection =
            ProviderDeletionVectorSelection::from_keep_mask(vec![true, false, true, false]);
        let error = selection
            .consume_batch(2, context)
            .and_then(|_| selection.finish(context))
            .err()
            .ok_or("expected overlong selection-vector error")?;
        let display = error.to_string();

        assert!(display.contains("orders"), "{display}");
        assert!(display.contains("snapshot version 7"), "{display}");
        assert!(display.contains("part-00000.parquet"), "{display}");
        assert!(
            display.contains("selection-vector length mismatch"),
            "{display}"
        );
        assert!(display.contains("unconsumed entries"), "{display}");
        assert!(display.contains("deleted row indexes"), "{display}");

        Ok(())
    }

    #[test]
    fn consume_after_finish_reports_exhaustion() -> Result<(), Box<dyn std::error::Error>> {
        let context = test_context();
        let mut selection = ProviderDeletionVectorSelection::from_keep_mask(vec![true]);

        selection.consume_batch(1, context)?;
        selection.finish(context)?;
        let error = selection
            .consume_batch(1, context)
            .err()
            .ok_or("expected exhausted selection-vector error")?;

        assert!(error.to_string().contains("selection-vector exhaustion"));

        Ok(())
    }

    #[test]
    fn adapter_loads_real_inline_dv_selection_vector() -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("inline-dv-adapter")?;
        let source = load_source("orders", &table)?;
        let reader = test_reader(&source)?;
        let handle = inline_dv_handle();
        let metadata = KernelScanDeletionVectorMetadata::Present(handle);
        let selection = reader
            .read_selection(KernelDeletionVectorReadRequest {
                path: table.data_file_path(),
                deletion_vector: &metadata,
            })?
            .ok_or("expected inline DV selection")?;

        assert_eq!(
            selection.remaining_kernel_entries(),
            INLINE_DV_DELETED_ROW_INDEXES.len()
        );
        assert_eq!(
            selection.deleted_row_indexes.as_ref(),
            INLINE_DV_DELETED_ROW_INDEXES,
            "kernel inline DV should decode to provider deleted row indexes"
        );

        Ok(())
    }

    #[test]
    fn adapter_skips_absent_deletion_vector() -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("absent-dv-adapter")?;
        let source = load_source("orders", &table)?;
        let reader = test_reader(&source)?;
        let selection = reader.read_selection(KernelDeletionVectorReadRequest {
            path: table.data_file_path(),
            deletion_vector: &KernelScanDeletionVectorMetadata::NotPresent,
        })?;

        assert!(selection.is_none());

        Ok(())
    }

    #[test]
    fn adapter_loads_real_on_disk_dv_selection_vector() -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("on-disk-dv-adapter")?;
        let source = load_source("orders", &table)?;
        let reader = test_reader(&source)?;
        let metadata =
            KernelScanDeletionVectorMetadata::Present(write_relative_dv_handle(&table, [0, 9])?);
        let selection = reader
            .read_selection(KernelDeletionVectorReadRequest {
                path: table.data_file_path(),
                deletion_vector: &metadata,
            })?
            .ok_or("expected on-disk DV selection")?;

        assert_eq!(selection.deleted_row_indexes.as_ref(), &[0, 9]);

        Ok(())
    }

    #[test]
    fn adapter_payload_error_preserves_dv_context() -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("dv-error-context")?;
        let source = load_source("orders", &table)?;
        let reader = test_reader(&source)?;
        let metadata = KernelScanDeletionVectorMetadata::Present(relative_dv_handle());
        let error = reader
            .read_selection(KernelDeletionVectorReadRequest {
                path: "missing-dv-file.parquet",
                deletion_vector: &metadata,
            })
            .err()
            .ok_or("expected missing DV payload error")?;
        let display = error.to_string();

        assert!(display.contains("source `orders`"), "{display}");
        assert!(display.contains("snapshot version 1"), "{display}");
        assert!(display.contains("missing-dv-file.parquet"), "{display}");
        assert!(
            display.contains("deletion-vector payload read"),
            "{display}"
        );

        Ok(())
    }

    #[test]
    fn metadata_expansion_preserves_dv_without_payload_load()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("dv-metadata-is-lazy")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, None)?;
        let file = scan
            .expand_kernel_scan_metadata()?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;

        assert!(!file.deletion_vector.is_present());

        Ok(())
    }

    fn inline_dv_handle() -> KernelScanDeletionVectorHandle {
        let descriptor = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::Inline,
            path_or_inline_dv: "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L".to_owned(),
            offset: None,
            size_in_bytes: 44,
            cardinality: 6,
        };

        KernelScanDeletionVectorHandle::from_test_dv_info(descriptor.into())
    }

    fn relative_dv_handle() -> KernelScanDeletionVectorHandle {
        let descriptor = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_owned(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        };

        KernelScanDeletionVectorHandle::from_test_dv_info(descriptor.into())
    }

    fn write_relative_dv_handle<const N: usize>(
        table: &RealParquetDeltaTable,
        deleted_rows: [u64; N],
    ) -> Result<KernelScanDeletionVectorHandle, Box<dyn std::error::Error>> {
        const RELATIVE_DV_ID: &str = "vBn[lx{q8@P<9BNH/isA";
        const RELATIVE_DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";

        let mut buffer = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
        let mut deletion_vector = KernelDeletionVector::new();
        deletion_vector.add_deleted_row_indexes(deleted_rows);
        let write_result = writer.write_deletion_vector(deletion_vector)?;
        writer.finalize()?;
        std::fs::write(table.path().join(RELATIVE_DV_FILE), buffer)?;

        let descriptor = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: RELATIVE_DV_ID.to_owned(),
            offset: Some(write_result.offset),
            size_in_bytes: write_result.size_in_bytes,
            cardinality: write_result.cardinality,
        };

        Ok(KernelScanDeletionVectorHandle::from_test_dv_info(
            descriptor.into(),
        ))
    }

    fn load_source(
        source_name: &str,
        table: &RealParquetDeltaTable,
    ) -> Result<PlannedDeltaSource, Box<dyn std::error::Error>> {
        Ok(load_delta_source(DeltaSourceConfig {
            name: source_name.to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?)
    }

    fn test_reader(
        source: &PlannedDeltaSource,
    ) -> Result<KernelDeletionVectorReader, Box<dyn std::error::Error>> {
        Ok(KernelDeletionVectorReader::new(
            KernelDeletionVectorReaderConfig {
                source_name: source.name(),
                snapshot_version: source.version(),
                engine_context: std::sync::Arc::clone(source.engine_context()),
            },
        ))
    }

    fn test_context() -> ProviderDeletionVectorSelectionContext<'static> {
        ProviderDeletionVectorSelectionContext {
            source_name: "orders",
            table_uri: "s3://user:password@example.com/table?token=secret",
            snapshot_version: 7,
            path: "part-00000.parquet",
        }
    }
}
