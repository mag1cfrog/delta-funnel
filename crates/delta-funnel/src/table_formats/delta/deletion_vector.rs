//! Private Delta Kernel deletion-vector selection adapter.
//!
//! This module keeps deletion-vector payload reads lazy and scoped to one
//! provider data-file read. Scan metadata planning preserves only descriptors;
//! execution code must come through this boundary to materialize a keep mask.

use crate::{
    DeltaFunnelError,
    error::{DeltaScanDeletionVectorPhase, DeltaScanDeletionVectorSnafu},
};
use snafu::ResultExt;

use super::{KernelScanDeletionVectorMetadata, kernel};

/// Context required to construct the official-kernel DV reader baseline.
#[allow(dead_code)]
pub(crate) struct KernelDeletionVectorReaderConfig<'a> {
    /// DataFusion table name for diagnostics.
    pub(crate) source_name: &'a str,
    /// Normalized Delta table URI used to resolve table-relative DV paths.
    pub(crate) table_uri: &'a str,
    /// Snapshot version that selected this file.
    pub(crate) snapshot_version: u64,
}

/// Reusable official-kernel DV reader baseline for one provider scan context.
#[allow(dead_code)]
pub(crate) struct KernelDeletionVectorReader {
    source_name: String,
    table_uri: String,
    snapshot_version: u64,
    engine: std::sync::Arc<dyn kernel::Engine>,
}

/// Request to load the deletion vector for one provider-selected data file.
#[allow(dead_code)]
pub(crate) struct KernelDeletionVectorReadRequest<'a> {
    /// Delta add-action table-relative file path.
    pub(crate) path: &'a str,
    /// Preserved deletion-vector metadata from scan metadata expansion.
    pub(crate) deletion_vector: &'a KernelScanDeletionVectorMetadata,
}

/// Provider-owned live-row keep mask for one physical Delta data file.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProviderDeletionVectorSelection {
    keep_mask: Vec<bool>,
    consumed: usize,
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
/// reader starts consuming a file sequentially, `consumed` is the original row
/// cursor. Once a reader starts querying by original row index, there is no
/// sequential cursor to validate because pruning or pushdown may skip rows.
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
    pub(crate) fn try_new(
        config: KernelDeletionVectorReaderConfig<'_>,
    ) -> Result<Self, DeltaFunnelError> {
        const TABLE_ROOT_CONTEXT: &str = "<table-root>";

        let table_url =
            kernel::try_parse_uri(config.table_uri).context(DeltaScanDeletionVectorSnafu {
                source_name: config.source_name.to_owned(),
                table_uri: config.table_uri.to_owned(),
                snapshot_version: config.snapshot_version,
                path: TABLE_ROOT_CONTEXT.to_owned(),
                phase: DeltaScanDeletionVectorPhase::TableUriParsing,
            })?;
        let store = kernel::store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>())
            .context(DeltaScanDeletionVectorSnafu {
                source_name: config.source_name.to_owned(),
                table_uri: config.table_uri.to_owned(),
                snapshot_version: config.snapshot_version,
                path: TABLE_ROOT_CONTEXT.to_owned(),
                phase: DeltaScanDeletionVectorPhase::ObjectStoreEngineConstruction,
            })?;
        let engine = std::sync::Arc::new(kernel::DefaultEngineBuilder::new(store).build());

        Ok(Self {
            source_name: config.source_name.to_owned(),
            table_uri: config.table_uri.to_owned(),
            snapshot_version: config.snapshot_version,
            engine,
        })
    }

    /// Lazily loads the provider keep mask for one selected data file.
    #[allow(dead_code)]
    pub(crate) fn read_selection(
        &self,
        request: KernelDeletionVectorReadRequest<'_>,
    ) -> Result<Option<ProviderDeletionVectorSelection>, DeltaFunnelError> {
        let KernelScanDeletionVectorMetadata::Present(handle) = request.deletion_vector else {
            return Ok(None);
        };
        let table_url =
            kernel::try_parse_uri(&self.table_uri).context(DeltaScanDeletionVectorSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanDeletionVectorPhase::TableUriParsing,
            })?;

        let keep_mask = handle
            .dv_info
            .get_selection_vector(self.engine.as_ref(), &table_url)
            .context(DeltaScanDeletionVectorSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanDeletionVectorPhase::PayloadRead,
            })?
            .unwrap_or_default();

        Ok(Some(ProviderDeletionVectorSelection::from_keep_mask(
            keep_mask,
        )))
    }
}

impl ProviderDeletionVectorSelection {
    /// Creates an owned keep mask where `true` means emit the physical row.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn from_keep_mask(keep_mask: Vec<bool>) -> Self {
        Self {
            keep_mask,
            consumed: 0,
            closed: false,
            access_mode: ProviderDeletionVectorSelectionAccessMode::Unused,
        }
    }

    /// Creates an all-live keep mask for tests and future no-DV normalization.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn all_live(row_count: usize) -> Self {
        Self::from_keep_mask(vec![true; row_count])
    }

    /// Returns the number of kernel-provided mask entries not yet consumed.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn remaining_kernel_entries(&self) -> usize {
        self.keep_mask.len().saturating_sub(self.consumed)
    }

    /// Consumes the next physical batch and returns an exact-length keep mask.
    ///
    /// Kernel selection vectors may be shorter than the physical file when the
    /// tail rows are all live. In that case this pads only the requested batch
    /// with `true`, matching Delta Kernel's Arrow engine behavior while keeping
    /// the provider-owned cursor explicit.
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

        let requested_end = self
            .consumed
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
        let copied_start = self.consumed.min(self.keep_mask.len());
        let copied_end = requested_end.min(self.keep_mask.len());
        let mut batch_mask = self.keep_mask[copied_start..copied_end].to_vec();
        batch_mask.resize(batch_len, true);
        self.consumed = requested_end;

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

        row_indexes
            .into_iter()
            .map(|row_index| {
                let row_index = usize::try_from(row_index)
                    .map_err(|_| {
                        kernel::DeltaKernelError::generic(
                            "original row index does not fit host usize",
                        )
                    })
                    .context(DeltaScanDeletionVectorSnafu {
                        source_name: context.source_name.to_owned(),
                        table_uri: context.table_uri.to_owned(),
                        snapshot_version: context.snapshot_version,
                        path: context.path.to_owned(),
                        phase: DeltaScanDeletionVectorPhase::SelectionVectorLengthMismatch,
                    })?;

                Ok(self.keep_mask.get(row_index).copied().unwrap_or(true))
            })
            .collect()
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

        if self.consumed < self.keep_mask.len() {
            return Err(kernel::DeltaKernelError::generic(format!(
                "selection vector has {} unconsumed entries after file completion",
                self.keep_mask.len() - self.consumed
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

    const INLINE_DV_KEEP_MASK: &[bool] = &[
        true, true, true, false, false, true, true, false, true, true, true, false, true, true,
        true, true, true, true, false, true, true, true, true, true, true, true, true, true, true,
        false,
    ];

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
            INLINE_DV_KEEP_MASK.len()
        );
        assert_eq!(
            selection.keep_mask.as_slice(),
            INLINE_DV_KEEP_MASK,
            "kernel inline DV should decode to provider live-row keep mask"
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

        assert_eq!(
            selection.keep_mask.as_slice(),
            &[false, true, true, true, true, true, true, true, true, false]
        );

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
            .expand_kernel_scan_metadata(source.table_uri(), source.storage_options())?
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
        Ok(KernelDeletionVectorReader::try_new(
            KernelDeletionVectorReaderConfig {
                source_name: source.name(),
                table_uri: source.table_uri(),
                snapshot_version: source.version(),
            },
        )?)
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
