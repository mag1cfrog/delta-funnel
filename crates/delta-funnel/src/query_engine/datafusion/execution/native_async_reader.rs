//! Native async Parquet reader boundary for provider file tasks.
//!
//! The native backend uses the same Delta table URI normalization and object-store
//! construction path as the official-kernel baseline, then hands resolved
//! `object_store::Path` values to `ParquetObjectReader`. That keeps local and
//! remote table URI semantics aligned while allowing parquet-rs to issue async
//! range reads through the selected object-store handle.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, new_null_array};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use futures_util::StreamExt;
use object_store::{ObjectStore, path::Path};
use parquet::arrow::RowNumber;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ArrowReaderOptions, RowFilter};
use parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ProjectionMask};
use parquet::schema::types::{SchemaDescriptor, TypePtr};
use snafu::ResultExt;

use crate::{
    DeltaFunnelError,
    error::{DeltaScanFileReadPhase, DeltaScanFileReadSnafu},
    table_formats::{
        KernelColumnMetadataKey, KernelDataFilePredicateEvalRequest, KernelDataType,
        KernelDeletionVectorReadRequest, KernelDeletionVectorReader,
        KernelDeletionVectorReaderConfig, KernelMetadataColumnSpec, KernelMetadataValue,
        KernelPhysicalToLogicalTransform, KernelScanReadSchema, KernelSchemaRef, KernelStructField,
        ProviderDeletionVectorSelection, ProviderDeletionVectorSelectionContext,
    },
};

use super::super::planning::file_task::DeltaScanFileTask;
use super::async_scheduler::{DeltaProviderAsyncFileReadFuture, DeltaProviderAsyncFileReader};
use super::file_reader::DeltaFileReadDeletionVectorStats;
use super::native_async_row_group_pruning::native_async_pruned_row_groups;
use super::read_stats::DeltaProviderReadStats;
use super::scheduling::DeltaProviderAsyncFileReadPermit;
use crate::table_formats::{
    DeltaStorageOptions, KernelDataFileReader, KernelDataFileReaderConfig,
    KernelDataFileTransformRequest,
};

const TABLE_ROOT_CONTEXT: &str = "<table-root>";
const ORIGINAL_ROW_INDEX_COLUMN: &str = "__delta_funnel_original_row_index";

/// Context required to construct the native async reader.
#[allow(dead_code)]
pub(crate) struct DeltaNativeAsyncFileReaderConfig<'a> {
    /// DataFusion table name for diagnostics.
    pub(crate) source_name: &'a str,
    /// Normalized Delta table URI used to resolve table-relative file paths.
    pub(crate) table_uri: &'a str,
    /// Snapshot version that selected the file tasks.
    pub(crate) snapshot_version: u64,
    /// Source-local options forwarded to Delta Kernel object-store construction.
    pub(crate) storage_options: &'a DeltaStorageOptions,
}

/// Reusable native async file reader context for one provider scan.
#[allow(dead_code)]
pub(crate) struct DeltaNativeAsyncFileReader {
    source_name: String,
    table_uri: String,
    snapshot_version: u64,
    store: Arc<dyn ObjectStore>,
    data_file_reader: Arc<KernelDataFileReader>,
    deletion_vector_reader: Arc<KernelDeletionVectorReader>,
}

/// Object-store input for a single native async Parquet file read.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct DeltaNativeAsyncParquetObject {
    /// Object store selected from the Delta table URI.
    pub(crate) store: Arc<dyn ObjectStore>,
    /// Resolved object-store path for the table-relative Delta add path.
    pub(crate) path: Path,
    /// File size from Delta scan metadata.
    pub(crate) file_size: u64,
}

/// Request to read exactly one provider file task through the native async path.
#[allow(dead_code)]
pub(crate) struct DeltaNativeAsyncFileReadRequest<'a> {
    /// Provider-owned task for one physical Delta data file.
    pub(crate) task: &'a DeltaScanFileTask,
    /// Kernel scan schema state selected for this provider scan.
    pub(crate) read_schema: &'a KernelScanReadSchema,
}

/// Native async scheduler adapter for one execution partition.
#[allow(dead_code)]
pub(crate) struct DeltaNativeAsyncPartitionFileReader {
    reader: Arc<DeltaNativeAsyncFileReader>,
    read_schema: KernelScanReadSchema,
    read_stats: Arc<DeltaProviderReadStats>,
}

/// Native async batches for one provider file task.
#[allow(dead_code)]
pub(crate) struct DeltaNativeAsyncFileReadStream {
    stream: ParquetRecordBatchStream<ParquetObjectReader>,
    schema_match: NativeAsyncSchemaMatch,
    source_name: String,
    table_uri: String,
    snapshot_version: u64,
    path: String,
    read_schema: KernelScanReadSchema,
    transform: KernelPhysicalToLogicalTransform,
    data_file_reader: Arc<KernelDataFileReader>,
    include_original_row_index: bool,
    deletion_vector: Option<ProviderDeletionVectorSelection>,
    deletion_vector_stats: DeltaFileReadDeletionVectorStats,
    deletion_vector_stats_reported: DeltaFileReadDeletionVectorStats,
    _permit: Option<DeltaProviderAsyncFileReadPermit>,
}

/// Validates that the native async backend can construct its object-store path.
pub(crate) fn validate_native_async_reader_config(
    config: DeltaNativeAsyncFileReaderConfig<'_>,
) -> Result<(), DeltaFunnelError> {
    DeltaNativeAsyncFileReader::try_new(config).map(|_| ())
}

impl DeltaNativeAsyncFileReader {
    /// Builds a native async reader context for one provider scan.
    #[allow(dead_code)]
    pub(crate) fn try_new(
        config: DeltaNativeAsyncFileReaderConfig<'_>,
    ) -> Result<Self, DeltaFunnelError> {
        let table_url =
            delta_kernel::try_parse_uri(config.table_uri).context(DeltaScanFileReadSnafu {
                source_name: config.source_name.to_owned(),
                table_uri: config.table_uri.to_owned(),
                snapshot_version: config.snapshot_version,
                path: TABLE_ROOT_CONTEXT.to_owned(),
                phase: DeltaScanFileReadPhase::TableUriParsing,
            })?;
        let store = delta_kernel::engine::default::storage::store_from_url_opts(
            &table_url,
            config
                .storage_options
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        .context(DeltaScanFileReadSnafu {
            source_name: config.source_name.to_owned(),
            table_uri: config.table_uri.to_owned(),
            snapshot_version: config.snapshot_version,
            path: TABLE_ROOT_CONTEXT.to_owned(),
            phase: DeltaScanFileReadPhase::ObjectStoreEngineConstruction,
        })?;
        let data_file_reader =
            Arc::new(KernelDataFileReader::try_new(KernelDataFileReaderConfig {
                source_name: config.source_name,
                table_uri: config.table_uri,
                snapshot_version: config.snapshot_version,
                storage_options: config.storage_options,
            })?);
        let deletion_vector_reader = Arc::new(KernelDeletionVectorReader::try_new(
            KernelDeletionVectorReaderConfig {
                source_name: config.source_name,
                table_uri: config.table_uri,
                snapshot_version: config.snapshot_version,
                storage_options: config.storage_options,
            },
        )?);

        Ok(Self {
            source_name: config.source_name.to_owned(),
            table_uri: config.table_uri.to_owned(),
            snapshot_version: config.snapshot_version,
            store,
            data_file_reader,
            deletion_vector_reader,
        })
    }

    /// Resolves a Delta file task into the object-store input for Parquet reads.
    #[allow(dead_code)]
    pub(crate) fn parquet_object_for_task(
        &self,
        task: &DeltaScanFileTask,
    ) -> Result<DeltaNativeAsyncParquetObject, DeltaFunnelError> {
        self.validate_task_context(task)?;
        let table_url =
            delta_kernel::try_parse_uri(&self.table_uri).context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: task.path.clone(),
                phase: DeltaScanFileReadPhase::TableUriParsing,
            })?;
        let location = table_url
            .join(&task.path)
            .map_err(delta_kernel::Error::from)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: task.path.clone(),
                phase: DeltaScanFileReadPhase::FilePathResolution,
            })?;
        let path = Path::from_url_path(location.path())
            .map_err(delta_kernel::Error::from)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: task.path.clone(),
                phase: DeltaScanFileReadPhase::FilePathResolution,
            })?;
        let file_size = task
            .estimated_bytes
            .ok_or_else(|| {
                delta_kernel::Error::generic("file size is required for native async Parquet reads")
            })
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: task.path.clone(),
                phase: DeltaScanFileReadPhase::FileMetadataConversion,
            })?;

        Ok(DeltaNativeAsyncParquetObject {
            store: Arc::clone(&self.store),
            path,
            file_size,
        })
    }

    /// Opens one file task with parquet-rs async object-store reads.
    ///
    /// Tests use this path to exercise the native file reader without scheduler
    /// permits. Production execution uses `open_file_stream_with_permit`.
    #[cfg(test)]
    pub(crate) async fn open_file_stream(
        &self,
        request: DeltaNativeAsyncFileReadRequest<'_>,
    ) -> Result<DeltaNativeAsyncFileReadStream, DeltaFunnelError> {
        self.open_file_stream_with_permit(request, None).await
    }

    /// Opens one file task while requesting hidden original row indexes.
    #[cfg(test)]
    pub(crate) async fn open_file_stream_with_original_row_index(
        &self,
        request: DeltaNativeAsyncFileReadRequest<'_>,
    ) -> Result<DeltaNativeAsyncFileReadStream, DeltaFunnelError> {
        self.open_file_stream_internal(request, None, true).await
    }

    /// Opens one file stream and holds the scheduler permit for its lifetime.
    ///
    /// The returned stream advances Parquet IO one batch at a time. Keeping the
    /// file permit inside the stream ensures the async read limiter accounts for
    /// the file until all batches are produced or the stream is dropped.
    async fn open_file_stream_with_permit(
        &self,
        request: DeltaNativeAsyncFileReadRequest<'_>,
        permit: Option<DeltaProviderAsyncFileReadPermit>,
    ) -> Result<DeltaNativeAsyncFileReadStream, DeltaFunnelError> {
        self.open_file_stream_internal(request, permit, false).await
    }

    async fn open_file_stream_internal(
        &self,
        request: DeltaNativeAsyncFileReadRequest<'_>,
        permit: Option<DeltaProviderAsyncFileReadPermit>,
        include_original_row_index: bool,
    ) -> Result<DeltaNativeAsyncFileReadStream, DeltaFunnelError> {
        self.validate_supported_read_mode(request.task, request.read_schema)?;
        let include_original_row_index =
            include_original_row_index || request.task.deletion_vector.is_present();
        let object = self.parquet_object_for_task(request.task)?;
        let reader =
            ParquetObjectReader::new(object.store, object.path).with_file_size(object.file_size);
        let arrow_schema: SchemaRef = request
            .read_schema
            .physical_schema()
            .as_ref()
            .try_into_arrow()
            .map(Arc::new)
            .map_err(delta_kernel::Error::from)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.task.path.clone(),
                phase: DeltaScanFileReadPhase::ArrowConversion,
            })?;
        let reader_options = native_async_arrow_reader_options(include_original_row_index)
            .map_err(delta_kernel::Error::from)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.task.path.clone(),
                phase: DeltaScanFileReadPhase::RowIndexGeneration,
            })?;
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, reader_options)
            .await
            .map_err(delta_kernel::Error::from)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.task.path.clone(),
                phase: DeltaScanFileReadPhase::ParquetReadSetup,
            })?;
        let schema_match = build_native_async_schema_match(
            builder.parquet_schema(),
            builder.schema(),
            arrow_schema,
        )
        .context(DeltaScanFileReadSnafu {
            source_name: self.source_name.clone(),
            table_uri: self.table_uri.clone(),
            snapshot_version: self.snapshot_version,
            path: request.task.path.clone(),
            phase: DeltaScanFileReadPhase::ArrowConversion,
        })?;
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), schema_match.projected_roots());
        let row_groups = native_async_pruned_row_groups(builder.metadata(), request.read_schema);
        let builder = if let Some(row_groups) = row_groups {
            builder.with_row_groups(row_groups)
        } else {
            builder
        };
        // This is row-level provider enforcement, not metadata pruning. The
        // planner must only preserve `read_schema.physical_predicate` here when
        // it is acceptable for this file read to enforce that predicate before
        // rows are exposed. Inexact metadata-pruning predicates should not
        // reach this helper unless duplicate residual filtering is intentional.
        let row_filter = self.native_async_provider_enforced_row_filter(
            request.task,
            request.read_schema,
            &schema_match,
            builder.parquet_schema(),
        )?;
        let builder = if let Some(row_filter) = row_filter {
            builder.with_row_filter(row_filter)
        } else {
            builder
        };
        let stream = builder
            .with_projection(projection)
            .build()
            .map_err(delta_kernel::Error::from)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.task.path.clone(),
                phase: DeltaScanFileReadPhase::ParquetReadSetup,
            })?;
        let deletion_vector =
            self.deletion_vector_reader
                .read_selection(KernelDeletionVectorReadRequest {
                    path: &request.task.path,
                    deletion_vector: &request.task.deletion_vector,
                })?;
        let deletion_vector_stats = DeltaFileReadDeletionVectorStats {
            payload_loaded: deletion_vector.is_some(),
            ..DeltaFileReadDeletionVectorStats::default()
        };

        Ok(DeltaNativeAsyncFileReadStream {
            stream,
            schema_match,
            source_name: self.source_name.clone(),
            table_uri: self.table_uri.clone(),
            snapshot_version: self.snapshot_version,
            path: request.task.path.clone(),
            read_schema: request.read_schema.clone(),
            transform: request.task.transform.clone(),
            data_file_reader: Arc::clone(&self.data_file_reader),
            include_original_row_index,
            deletion_vector,
            deletion_vector_stats,
            deletion_vector_stats_reported: DeltaFileReadDeletionVectorStats::default(),
            _permit: permit,
        })
    }

    fn native_async_provider_enforced_row_filter(
        &self,
        task: &DeltaScanFileTask,
        read_schema: &KernelScanReadSchema,
        schema_match: &NativeAsyncSchemaMatch,
        parquet_schema: &SchemaDescriptor,
    ) -> Result<Option<RowFilter>, DeltaFunnelError> {
        if !read_schema.enforces_physical_predicate_rows() {
            return Ok(None);
        }

        let projection = ProjectionMask::roots(parquet_schema, schema_match.projected_roots());
        let data_file_reader = Arc::clone(&self.data_file_reader);
        let read_schema = read_schema.clone();
        let schema_match = schema_match.clone();
        let path = task.path.clone();
        let predicate = ArrowPredicateFn::new(projection, move |batch| {
            let batch = schema_match
                .reshape_batch_to_provider_schema(batch)
                .map_err(|error| ArrowError::ComputeError(error.to_string()))?;

            data_file_reader
                .evaluate_physical_predicate(KernelDataFilePredicateEvalRequest {
                    path: &path,
                    batch,
                    schema: &read_schema,
                })
                .map_err(|error| ArrowError::ExternalError(Box::new(error)))
        });

        Ok(Some(RowFilter::new(vec![Box::new(predicate)])))
    }
}

impl DeltaNativeAsyncFileReadStream {
    /// File-local deletion-vector metrics observed during this read.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn deletion_vector_stats(&self) -> DeltaFileReadDeletionVectorStats {
        self.deletion_vector_stats
    }

    /// Drains file-local deletion-vector metrics observed since the previous drain.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn take_deletion_vector_stats(&mut self) -> DeltaFileReadDeletionVectorStats {
        let deletion_vector_stats = DeltaFileReadDeletionVectorStats {
            payload_loaded: self.deletion_vector_stats.payload_loaded
                && !self.deletion_vector_stats_reported.payload_loaded,
            applied: self.deletion_vector_stats.applied
                && !self.deletion_vector_stats_reported.applied,
            deleted_rows: self
                .deletion_vector_stats
                .deleted_rows
                .saturating_sub(self.deletion_vector_stats_reported.deleted_rows),
        };
        if deletion_vector_stats.payload_loaded {
            self.deletion_vector_stats_reported.payload_loaded = true;
        }
        if deletion_vector_stats.applied {
            self.deletion_vector_stats_reported.applied = true;
        }
        self.deletion_vector_stats_reported.deleted_rows = self.deletion_vector_stats.deleted_rows;

        deletion_vector_stats
    }

    /// Returns the next provider-visible batch for this file.
    #[allow(dead_code)]
    pub(crate) async fn next_batch(&mut self) -> Result<Option<RecordBatch>, DeltaFunnelError> {
        self.next_batch_with_original_row_indexes()
            .await
            .map(|batch| batch.map(|(batch, _original_row_indexes)| batch))
    }

    async fn next_batch_with_original_row_indexes(
        &mut self,
    ) -> Result<Option<(RecordBatch, Option<Vec<u64>>)>, DeltaFunnelError> {
        let Some(batch) = self.stream.next().await else {
            self.finish_deletion_vector_selection()?;
            return Ok(None);
        };
        let batch = batch
            .map_err(delta_kernel::Error::from)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: self.path.clone(),
                phase: DeltaScanFileReadPhase::ParquetBatchRead,
            })?;

        let original_row_indexes = self.original_row_indexes_from_batch(&batch)?;
        let physical_batch = self.project_batch_to_schema(batch)?;
        let logical_batch = self.apply_physical_to_logical_transform(physical_batch)?;
        let logical_batch =
            self.apply_deletion_vector_mask(logical_batch, original_row_indexes.as_deref())?;

        Ok(Some((logical_batch, original_row_indexes)))
    }

    /// Shapes the Parquet batch into the provider physical scan schema.
    ///
    /// File-level schema matching is computed once when the Parquet stream is
    /// opened. Each later batch from that stream has the same schema, so the
    /// hot path only applies the precomputed column reorder, rename, or null
    /// fill needed before the kernel physical-to-logical transform runs.
    fn project_batch_to_schema(&self, batch: RecordBatch) -> Result<RecordBatch, DeltaFunnelError> {
        if !self.include_original_row_index && !self.schema_match.needs_batch_reshape {
            return Ok(batch);
        }

        self.schema_match
            .reshape_batch_to_provider_schema(batch)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: self.path.clone(),
                phase: DeltaScanFileReadPhase::ArrowConversion,
            })
    }

    fn original_row_indexes_from_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<Option<Vec<u64>>, DeltaFunnelError> {
        if !self.include_original_row_index {
            return Ok(None);
        }

        let row_index_column = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == ORIGINAL_ROW_INDEX_COLUMN)
            .ok_or_else(|| {
                delta_kernel::Error::generic("missing native async original row-index column")
            })
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: self.path.clone(),
                phase: DeltaScanFileReadPhase::RowIndexGeneration,
            })?;
        let row_indexes = batch
            .column(row_index_column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                delta_kernel::Error::generic("native async original row-index column is not Int64")
            })
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: self.path.clone(),
                phase: DeltaScanFileReadPhase::RowIndexGeneration,
            })?;

        (0..row_indexes.len())
            .map(|index| {
                if row_indexes.is_null(index) {
                    return Err(delta_kernel::Error::generic(
                        "native async original row-index column contains null",
                    ))
                    .context(DeltaScanFileReadSnafu {
                        source_name: self.source_name.clone(),
                        table_uri: self.table_uri.clone(),
                        snapshot_version: self.snapshot_version,
                        path: self.path.clone(),
                        phase: DeltaScanFileReadPhase::RowIndexGeneration,
                    });
                }

                u64::try_from(row_indexes.value(index))
                    .map_err(|_| {
                        delta_kernel::Error::generic(
                            "native async original row-index value is negative",
                        )
                    })
                    .context(DeltaScanFileReadSnafu {
                        source_name: self.source_name.clone(),
                        table_uri: self.table_uri.clone(),
                        snapshot_version: self.snapshot_version,
                        path: self.path.clone(),
                        phase: DeltaScanFileReadPhase::RowIndexGeneration,
                    })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    fn apply_physical_to_logical_transform(
        &self,
        batch: RecordBatch,
    ) -> Result<RecordBatch, DeltaFunnelError> {
        // Delta column mapping can decouple provider-visible logical names from
        // the physical Parquet column names stored in old and new data files.
        // After native async reads the provider physical schema, delegate to the
        // same kernel transform used by the baseline reader so logical names,
        // partition values, and helper columns are handled consistently.
        self.data_file_reader
            .apply_physical_to_logical_transform(KernelDataFileTransformRequest {
                path: &self.path,
                batch,
                schema: &self.read_schema,
                transform: &self.transform,
            })
    }

    fn apply_deletion_vector_mask(
        &mut self,
        batch: RecordBatch,
        original_row_indexes: Option<&[u64]>,
    ) -> Result<RecordBatch, DeltaFunnelError> {
        if self.deletion_vector.is_none() {
            return Ok(batch);
        }
        let row_indexes = original_row_indexes
            .ok_or_else(|| {
                delta_kernel::Error::generic(
                    "native async deletion-vector masking requires original row indexes",
                )
            })
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: self.path.clone(),
                phase: DeltaScanFileReadPhase::RowIndexGeneration,
            })?;
        let source_name = self.source_name.clone();
        let table_uri = self.table_uri.clone();
        let path = self.path.clone();
        let context = ProviderDeletionVectorSelectionContext {
            source_name: &source_name,
            table_uri: &table_uri,
            snapshot_version: self.snapshot_version,
            path: &path,
        };
        let keep_mask = self
            .deletion_vector
            .as_mut()
            .ok_or_else(|| {
                delta_kernel::Error::generic("missing native async deletion-vector selection")
            })
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: self.path.clone(),
                phase: DeltaScanFileReadPhase::DeletionVectorMasking,
            })?
            .select_original_row_indexes(row_indexes.iter().copied(), context)?;

        self.deletion_vector_stats.applied = true;
        let deleted_rows = keep_mask.iter().filter(|selected| !**selected).count();
        self.deletion_vector_stats.deleted_rows = self
            .deletion_vector_stats
            .deleted_rows
            .saturating_add(deleted_rows);
        if keep_mask.iter().all(|selected| *selected) {
            return Ok(batch);
        }
        if keep_mask.iter().all(|selected| !*selected) {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        let keep_mask = BooleanArray::from(keep_mask);

        filter_record_batch(&batch, &keep_mask)
            .map_err(delta_kernel::Error::from)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: self.path.clone(),
                phase: DeltaScanFileReadPhase::DeletionVectorMasking,
            })
    }

    fn finish_deletion_vector_selection(&mut self) -> Result<(), DeltaFunnelError> {
        let Some(mut deletion_vector) = self.deletion_vector.take() else {
            return Ok(());
        };
        let context = self.deletion_vector_context_for_path();

        deletion_vector.finish(context)
    }

    fn deletion_vector_context_for_path(&self) -> ProviderDeletionVectorSelectionContext<'_> {
        ProviderDeletionVectorSelectionContext {
            source_name: &self.source_name,
            table_uri: &self.table_uri,
            snapshot_version: self.snapshot_version,
            path: &self.path,
        }
    }
}

fn native_async_arrow_reader_options(
    include_original_row_index: bool,
) -> parquet::errors::Result<ArrowReaderOptions> {
    if !include_original_row_index {
        return Ok(ArrowReaderOptions::new());
    }

    let row_number_field = Arc::new(
        Field::new(ORIGINAL_ROW_INDEX_COLUMN, DataType::Int64, false)
            .with_extension_type(RowNumber),
    );

    ArrowReaderOptions::new().with_virtual_columns(vec![row_number_field])
}

#[derive(Clone)]
struct NativeAsyncSchemaMatch {
    provider_schema: SchemaRef,
    projected_roots: Vec<usize>,
    provider_columns: Vec<NativeAsyncProviderColumn>,
    needs_batch_reshape: bool,
}

#[derive(Clone, Copy)]
enum NativeAsyncProviderColumn {
    /// Column index in the projected Parquet stream batch.
    ///
    /// This is not the original Parquet file root index. The stream only emits
    /// the roots selected by `projected_roots`, in file order, so this index is
    /// relative to that projected output batch.
    ProjectedStreamColumnIndex(usize),
    /// Missing nullable provider field that must be materialized as all nulls.
    Null,
}

impl NativeAsyncSchemaMatch {
    fn projected_roots(&self) -> impl Iterator<Item = usize> + '_ {
        self.projected_roots.iter().copied()
    }

    /// Rebuilds a Parquet batch with the provider physical schema.
    ///
    /// Reordering or renaming existing columns is cheap because the Arrow arrays
    /// are shared by `Arc`. Missing nullable fields allocate all-null arrays to
    /// represent older Parquet files that predate a Delta schema evolution.
    fn reshape_batch_to_provider_schema(
        &self,
        batch: RecordBatch,
    ) -> Result<RecordBatch, delta_kernel::Error> {
        let columns = self
            .provider_columns
            .iter()
            .zip(self.provider_schema.fields())
            .map(|(column, field)| match column {
                NativeAsyncProviderColumn::ProjectedStreamColumnIndex(index) => {
                    Arc::clone(batch.column(*index))
                }
                NativeAsyncProviderColumn::Null => {
                    new_null_array(field.data_type(), batch.num_rows())
                }
            })
            .collect::<Vec<ArrayRef>>();

        RecordBatch::try_new(Arc::clone(&self.provider_schema), columns)
            .map_err(delta_kernel::Error::from)
    }
}

/// File-level schema matching result for the native async Parquet stream.
///
/// Delta schema matching is computed before the Parquet stream is built because
/// the projected stream schema strips field metadata. The result records both
/// the Parquet roots to read and how each provider physical field should be
/// materialized for every batch from this file.
fn build_native_async_schema_match(
    parquet_schema: &SchemaDescriptor,
    parquet_arrow_schema: &SchemaRef,
    provider_schema: SchemaRef,
) -> Result<NativeAsyncSchemaMatch, delta_kernel::Error> {
    let parquet_roots = parquet_schema.root_schema().get_fields();

    // Step 1: decide which Parquet root, if any, satisfies each provider
    // physical field. This is the Delta schema matching step.
    let root_matches = match_provider_fields_to_parquet_roots(
        &provider_schema,
        parquet_roots,
        parquet_arrow_schema,
    )?;

    // Step 2: turn the per-provider-field matches into the root projection
    // passed to parquet-rs. ProjectionMask has mask semantics, not ordered-list
    // semantics, so parquet-rs emits selected roots in Parquet file order. Keep
    // `projected_roots` in that same order so stream column indexes line up.
    let projected_roots = projected_roots_from_matches(&root_matches);

    // Step 3: translate each provider physical field into either a projected
    // stream column index or a nullable missing-column null fill.
    let provider_columns =
        provider_columns_from_root_matches(&root_matches, &projected_roots, &provider_schema)?;

    // Step 4: if file order/names already match the provider physical schema,
    // each Parquet batch can pass through unchanged. Otherwise the reshape step
    // only rebuilds the RecordBatch around shared arrays, except for nullable
    // missing columns that need new all-null arrays.
    let needs_batch_reshape = needs_native_async_batch_reshape(
        &provider_columns,
        &provider_schema,
        &projected_roots,
        parquet_arrow_schema,
    );

    Ok(NativeAsyncSchemaMatch {
        provider_schema,
        projected_roots,
        provider_columns,
        needs_batch_reshape,
    })
}

fn match_provider_fields_to_parquet_roots(
    provider_schema: &SchemaRef,
    parquet_roots: &[TypePtr],
    parquet_arrow_schema: &SchemaRef,
) -> Result<Vec<Option<usize>>, delta_kernel::Error> {
    provider_schema
        .fields()
        .iter()
        .map(|provider_field| {
            match_provider_field_to_parquet_root(
                provider_field,
                parquet_roots,
                parquet_arrow_schema,
            )
        })
        .collect()
}

fn projected_roots_from_matches(root_matches: &[Option<usize>]) -> Vec<usize> {
    // Keep only matched Parquet root indexes. Unmatched nullable provider fields
    // are represented by None and become null-filled columns later.
    let mut projected_roots = root_matches
        .iter()
        .filter_map(|root_index| *root_index)
        .collect::<Vec<_>>();
    projected_roots.sort_unstable();
    projected_roots.dedup();

    projected_roots
}

fn provider_columns_from_root_matches(
    root_matches: &[Option<usize>],
    projected_roots: &[usize],
    provider_schema: &SchemaRef,
) -> Result<Vec<NativeAsyncProviderColumn>, delta_kernel::Error> {
    root_matches
        .iter()
        .zip(provider_schema.fields())
        .map(|(root_index, provider_field)| match root_index {
            // Matched fields point to an index in the projected stream output,
            // not the original Parquet root index. parquet-rs emits projected
            // roots in file order, so this mapping captures any Delta/provider
            // physical schema order difference.
            Some(root_index) => projected_roots
                .iter()
                .position(|projected_root| projected_root == root_index)
                .map(NativeAsyncProviderColumn::ProjectedStreamColumnIndex)
                .ok_or_else(|| {
                    delta_kernel::Error::generic("matched Parquet root was not projected")
                }),
            // Delta schema evolution can add nullable fields after older data
            // files were written. Those files read as null for the new column.
            None if provider_field.is_nullable() => Ok(NativeAsyncProviderColumn::Null),
            // Missing required data is not representable as a valid provider
            // batch, so fail while opening the file stream, before rows emit.
            None => Err(delta_kernel::Error::generic(format!(
                "non-nullable provider field '{}' is missing from the Parquet file",
                provider_field.name()
            ))),
        })
        .collect()
}

fn needs_native_async_batch_reshape(
    provider_columns: &[NativeAsyncProviderColumn],
    provider_schema: &SchemaRef,
    projected_roots: &[usize],
    parquet_arrow_schema: &SchemaRef,
) -> bool {
    // A reshape is needed when parquet-rs projected stream order differs from
    // provider physical schema order, when field names differ and must be
    // replaced by the provider physical schema, or when nullable missing
    // columns must be synthesized.
    provider_columns
        .iter()
        .zip(provider_schema.fields())
        .enumerate()
        .any(|(provider_index, (column, provider_field))| match column {
            NativeAsyncProviderColumn::ProjectedStreamColumnIndex(stream_index) => {
                *stream_index != provider_index
                    || projected_roots
                        .get(*stream_index)
                        .and_then(|root_index| parquet_arrow_schema.fields().get(*root_index))
                        .is_none_or(|file_field| file_field.name() != provider_field.name())
            }
            NativeAsyncProviderColumn::Null => true,
        })
}

fn match_provider_field_to_parquet_root(
    provider_field: &Field,
    parquet_roots: &[TypePtr],
    parquet_arrow_schema: &SchemaRef,
) -> Result<Option<usize>, delta_kernel::Error> {
    // Delta schema matching uses field ids first. Column mapping tables can
    // rename logical or physical columns over time, but a stable field id still
    // identifies the same Delta column in Parquet metadata.
    let provider_field_id = arrow_field_id(provider_field)?;
    if let Some(field_id) = provider_field_id {
        let matches = parquet_roots
            .iter()
            .enumerate()
            .filter_map(|(index, parquet_root)| {
                (parquet_root_field_id(parquet_root) == Some(field_id)).then_some(index)
            })
            .collect::<Vec<_>>();

        match matches.as_slice() {
            [index] => {
                validate_matched_field_type(provider_field, parquet_arrow_schema.field(*index))?;
                return Ok(Some(*index));
            }
            [] => {}
            _ => {
                return Err(delta_kernel::Error::generic(format!(
                    "multiple Parquet fields matched provider field id {field_id}"
                )));
            }
        }
    }

    // Files without usable field ids, including ordinary non-column-mapping
    // tables, fall back to matching by physical field name.
    let Some((index, file_field)) = parquet_arrow_schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, file_field)| file_field.name() == provider_field.name())
    else {
        return Ok(None);
    };

    validate_matched_field_type(provider_field, file_field)?;

    Ok(Some(index))
}

fn validate_matched_field_type(
    provider_field: &Field,
    file_field: &Field,
) -> Result<(), delta_kernel::Error> {
    if file_field
        .data_type()
        .equals_datatype(provider_field.data_type())
    {
        return Ok(());
    }

    Err(delta_kernel::Error::generic(format!(
        "provider field '{}' expected Parquet type {} but found {}",
        provider_field.name(),
        provider_field.data_type(),
        file_field.data_type()
    )))
}

fn arrow_field_id(field: &Field) -> Result<Option<i32>, delta_kernel::Error> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .map(|field_id| {
            field_id.parse::<i32>().map_err(|error| {
                delta_kernel::Error::generic(format!(
                    "invalid provider field id metadata on '{}': {error}",
                    field.name()
                ))
            })
        })
        .transpose()
}

fn parquet_root_field_id(parquet_root: &TypePtr) -> Option<i32> {
    let basic_info = parquet_root.get_basic_info();
    basic_info.has_id().then(|| basic_info.id())
}

impl DeltaNativeAsyncFileReader {
    fn validate_task_context(&self, task: &DeltaScanFileTask) -> Result<(), DeltaFunnelError> {
        if task.source_name == self.source_name
            && task.table_uri == self.table_uri
            && task.snapshot_version == self.snapshot_version
        {
            return Ok(());
        }

        Err(delta_kernel::Error::generic(
            "file task scan context does not match the native async reader context",
        ))
        .context(DeltaScanFileReadSnafu {
            source_name: self.source_name.clone(),
            table_uri: self.table_uri.clone(),
            snapshot_version: self.snapshot_version,
            path: task.path.clone(),
            phase: DeltaScanFileReadPhase::FileMetadataConversion,
        })
    }

    fn validate_supported_read_mode(
        &self,
        task: &DeltaScanFileTask,
        read_schema: &KernelScanReadSchema,
    ) -> Result<(), DeltaFunnelError> {
        let reason = unsupported_native_async_physical_schema_reason(read_schema.physical_schema());

        match reason {
            Some(reason) => {
                Err(delta_kernel::Error::generic(reason)).context(DeltaScanFileReadSnafu {
                    source_name: self.source_name.clone(),
                    table_uri: self.table_uri.clone(),
                    snapshot_version: self.snapshot_version,
                    path: task.path.clone(),
                    phase: DeltaScanFileReadPhase::UnsupportedReadMode,
                })
            }
            None => Ok(()),
        }
    }
}

fn unsupported_native_async_physical_schema_reason(
    physical_schema: &KernelSchemaRef,
) -> Option<String> {
    physical_schema
        .fields()
        .find_map(|field| unsupported_native_async_field_reason(field, field.name(), true))
}

fn unsupported_native_async_field_reason(
    field: &KernelStructField,
    path: &str,
    top_level: bool,
) -> Option<String> {
    if field.is_metadata_column() {
        return Some(format!(
            "native async reader does not support generated metadata column '{path}' yet"
        ));
    }
    if field.is_internal_column() {
        return Some(format!(
            "native async reader does not support internal helper column '{path}' yet"
        ));
    }
    if is_file_path_metadata_field(field) {
        return Some(format!(
            "native async reader does not support file path metadata column '{path}' yet"
        ));
    }
    if has_nested_field_matching_metadata(field, top_level) {
        return Some(format!(
            "native async reader does not support nested field-id or physical-name matching at '{path}' yet"
        ));
    }

    unsupported_native_async_data_type_reason(field.data_type(), path)
}

fn unsupported_native_async_data_type_reason(
    data_type: &KernelDataType,
    path: &str,
) -> Option<String> {
    match data_type {
        KernelDataType::Struct(fields) | KernelDataType::Variant(fields) => {
            fields.fields().find_map(|field| {
                let child_path = format!("{path}.{}", field.name());
                unsupported_native_async_field_reason(field, &child_path, false)
            })
        }
        KernelDataType::Array(array) => {
            let child_path = format!("{path}.element");
            unsupported_native_async_data_type_reason(array.element_type(), &child_path)
        }
        KernelDataType::Map(map) => {
            let key_path = format!("{path}.key");
            unsupported_native_async_data_type_reason(map.key_type(), &key_path).or_else(|| {
                let value_path = format!("{path}.value");
                unsupported_native_async_data_type_reason(map.value_type(), &value_path)
            })
        }
        KernelDataType::Primitive(_) => None,
    }
}

fn is_file_path_metadata_field(field: &KernelStructField) -> bool {
    let Some(KernelMetadataValue::Number(field_id)) =
        field.get_config_value(&KernelColumnMetadataKey::ParquetFieldId)
    else {
        return false;
    };

    Some(*field_id) == KernelMetadataColumnSpec::FilePath.reserved_field_id()
}

fn has_nested_field_matching_metadata(field: &KernelStructField, top_level: bool) -> bool {
    let nested_metadata_keys = [
        KernelColumnMetadataKey::ColumnMappingNestedIds,
        KernelColumnMetadataKey::ParquetFieldNestedIds,
    ];
    if nested_metadata_keys
        .iter()
        .any(|key| field.get_config_value(key).is_some())
    {
        return true;
    }
    if top_level {
        return false;
    }

    [
        KernelColumnMetadataKey::ColumnMappingId,
        KernelColumnMetadataKey::ColumnMappingPhysicalName,
        KernelColumnMetadataKey::ParquetFieldId,
    ]
    .iter()
    .any(|key| field.get_config_value(key).is_some())
}

impl DeltaNativeAsyncPartitionFileReader {
    /// Builds a native async scheduler adapter for one execution partition.
    #[allow(dead_code)]
    pub(crate) fn new(
        reader: Arc<DeltaNativeAsyncFileReader>,
        read_schema: KernelScanReadSchema,
        read_stats: Arc<DeltaProviderReadStats>,
    ) -> Self {
        Self {
            reader,
            read_schema,
            read_stats,
        }
    }
}

impl DeltaProviderAsyncFileReader<DeltaScanFileTask, DeltaNativeAsyncFileReadStream>
    for DeltaNativeAsyncPartitionFileReader
{
    fn start_file_read(
        &self,
        task: DeltaScanFileTask,
        permit: DeltaProviderAsyncFileReadPermit,
    ) -> DeltaProviderAsyncFileReadFuture<DeltaNativeAsyncFileReadStream> {
        let reader = Arc::clone(&self.reader);
        let read_schema = self.read_schema.clone();
        self.read_stats.record_file_started();

        Box::pin(async move {
            reader
                .open_file_stream_with_permit(
                    DeltaNativeAsyncFileReadRequest {
                        task: &task,
                        read_schema: &read_schema,
                    },
                    Some(permit),
                )
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    use datafusion::arrow::array::{Array, Decimal128Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, col, lit};
    use delta_kernel::object_store::{memory::InMemory, path::Path as ObjectStorePath};
    use object_store::ObjectStoreExt;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
    use parquet::file::properties::WriterProperties;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::{
        DeltaNativeAsyncFileReadRequest, DeltaNativeAsyncFileReader,
        DeltaNativeAsyncFileReaderConfig, unsupported_native_async_physical_schema_reason,
        validate_native_async_reader_config,
    };
    use crate::{
        DeltaFunnelError, DeltaSourceConfig, DeltaStorageOptions,
        error::DeltaScanFileReadPhase,
        load_delta_source,
        query_engine::datafusion::{
            execution::file_reader::DeltaFileReadDeletionVectorStats,
            execution::native_async_row_group_pruning::native_async_pruned_row_groups,
            planning::file_task::DeltaScanFileTask,
        },
        table_formats::{
            KernelColumnMetadataKey, KernelDataType, KernelMetadataColumnSpec, KernelMetadataValue,
            KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata,
            KernelScanReadSchema, KernelSchemaRef, KernelStructField, KernelStructType,
            RealParquetDeltaTable, build_projected_delta_scan,
            build_projected_predicated_stats_delta_scan, datafusion_expr_to_kernel_predicate,
        },
    };

    struct TestDir {
        path: PathBuf,
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn local_table_uri(name: &str) -> Result<(TestDir, String), Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = std::env::temp_dir().join(format!(
            "{}-delta-funnel-native-async-{name}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path)?;
        let table_uri = delta_kernel::try_parse_uri(path.to_string_lossy().as_ref())?.to_string();

        Ok((TestDir { path }, table_uri))
    }

    type CapturedStorageOptions = Arc<Mutex<Vec<DeltaStorageOptions>>>;

    fn storage_options(entries: &[(&str, &str)]) -> DeltaStorageOptions {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn unique_storage_scheme(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let sanitized_name = name
            .chars()
            .filter(|character| character.is_ascii_alphanumeric())
            .collect::<String>();

        Ok(format!(
            "dfnative{sanitized_name}{}{}",
            std::process::id(),
            nanos
        ))
    }

    fn register_capturing_storage_handler(
        scheme: &str,
        captured: CapturedStorageOptions,
    ) -> Result<(), Box<dyn std::error::Error>> {
        delta_kernel::engine::default::storage::insert_url_handler(
            scheme,
            Arc::new(move |_url, options| {
                let options = options.into_iter().collect::<BTreeMap<_, _>>();
                captured
                    .lock()
                    .map_err(|_| delta_kernel::object_store::Error::Generic {
                        store: "capture",
                        source: std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "captured storage options lock poisoned",
                        )
                        .into(),
                    })?
                    .push(options);

                Ok((Box::new(InMemory::new()), ObjectStorePath::from("")))
            }),
        )?;

        Ok(())
    }

    fn captured_storage_options(captured: &CapturedStorageOptions) -> Vec<DeltaStorageOptions> {
        captured
            .lock()
            .map(|options| options.clone())
            .unwrap_or_default()
    }

    fn reader(table_uri: &str) -> Result<DeltaNativeAsyncFileReader, DeltaFunnelError> {
        let storage_options = DeltaStorageOptions::default();
        DeltaNativeAsyncFileReader::try_new(DeltaNativeAsyncFileReaderConfig {
            source_name: "orders",
            table_uri,
            snapshot_version: 42,
            storage_options: &storage_options,
        })
    }

    fn task(table_uri: &str, path: &str) -> DeltaScanFileTask {
        DeltaScanFileTask {
            source_name: "orders".to_owned(),
            table_uri: table_uri.to_owned(),
            snapshot_version: 42,
            path: path.to_owned(),
            estimated_bytes: Some(123),
            estimated_rows: None,
            modification_time_ms: Some(1587968586000),
            partition_values: BTreeMap::new(),
            stats: None,
            deletion_vector: KernelScanDeletionVectorMetadata::NotPresent,
            transform: KernelPhysicalToLogicalTransform::NotRequired,
        }
    }

    async fn collect_file_stream(
        mut stream: super::DeltaNativeAsyncFileReadStream,
    ) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>, DeltaFunnelError> {
        let mut batches = Vec::new();
        while let Some(batch) = stream.next_batch().await? {
            batches.push(batch);
        }
        Ok(batches)
    }

    fn default_read_schema(name: &str) -> Result<KernelScanReadSchema, Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default(name)?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, None)?;

        Ok(scan.read_schema())
    }

    fn default_parquet_bytes() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("customer_name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
            ],
        )?;
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;

        writer.write(&batch)?;

        Ok(writer.into_inner()?)
    }

    fn parquet_bytes(
        schema: Arc<Schema>,
        columns: Vec<Arc<dyn Array>>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;

        writer.write(&batch)?;

        Ok(writer.into_inner()?)
    }

    fn field_id_metadata(field_id: i32) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_owned(), field_id.to_string())])
    }

    fn kernel_schema(
        fields: impl IntoIterator<Item = KernelStructField>,
    ) -> Result<KernelSchemaRef, Box<dyn std::error::Error>> {
        Ok(Arc::new(KernelStructType::try_new(fields)?))
    }

    fn kernel_field_id_metadata(field_id: i64) -> [(String, KernelMetadataValue); 1] {
        [(
            KernelColumnMetadataKey::ParquetFieldId.as_ref().to_owned(),
            KernelMetadataValue::Number(field_id),
        )]
    }

    #[test]
    fn native_async_schema_gate_allows_top_level_field_id_matching()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = kernel_schema(
            [KernelStructField::new("id", KernelDataType::INTEGER, false)
                .add_metadata(kernel_field_id_metadata(1))],
        )?;

        assert_eq!(
            unsupported_native_async_physical_schema_reason(&schema),
            None
        );

        Ok(())
    }

    #[test]
    fn native_async_schema_gate_rejects_generated_metadata_columns()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = kernel_schema([KernelStructField::create_metadata_column(
            "row_index",
            KernelMetadataColumnSpec::RowIndex,
        )])?;
        let reason =
            unsupported_native_async_physical_schema_reason(&schema).ok_or("expected rejection")?;

        assert!(reason.contains("generated metadata column"));
        assert!(reason.contains("row_index"));

        Ok(())
    }

    #[test]
    fn native_async_schema_gate_rejects_internal_helper_columns()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema =
            kernel_schema([
                KernelStructField::new("helper", KernelDataType::LONG, false).as_internal_column(),
            ])?;
        let reason =
            unsupported_native_async_physical_schema_reason(&schema).ok_or("expected rejection")?;

        assert!(reason.contains("internal helper column"));
        assert!(reason.contains("helper"));

        Ok(())
    }

    #[test]
    fn native_async_schema_gate_rejects_file_path_metadata_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let file_path_field_id = KernelMetadataColumnSpec::FilePath
            .reserved_field_id()
            .ok_or("expected reserved file path field id")?;
        let schema =
            kernel_schema([
                KernelStructField::new("_file", KernelDataType::STRING, false)
                    .add_metadata(kernel_field_id_metadata(file_path_field_id)),
            ])?;
        let reason =
            unsupported_native_async_physical_schema_reason(&schema).ok_or("expected rejection")?;

        assert!(reason.contains("file path metadata column"));
        assert!(reason.contains("_file"));

        Ok(())
    }

    #[test]
    fn native_async_schema_gate_rejects_nested_field_id_matching()
    -> Result<(), Box<dyn std::error::Error>> {
        let nested_type = KernelStructType::try_new([KernelStructField::new(
            "inner",
            KernelDataType::INTEGER,
            true,
        )
        .add_metadata(kernel_field_id_metadata(2))])?;
        let schema = kernel_schema([KernelStructField::new("nested", nested_type, true)])?;
        let reason =
            unsupported_native_async_physical_schema_reason(&schema).ok_or("expected rejection")?;

        assert!(reason.contains("nested field-id or physical-name matching"));
        assert!(reason.contains("nested.inner"));

        Ok(())
    }

    #[test]
    fn native_async_reader_resolves_local_file_task_to_object_store_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, table_uri) = local_table_uri("local-path")?;
        let reader = reader(&table_uri)?;
        let object = reader.parquet_object_for_task(&task(&table_uri, "part-00000.parquet"))?;

        assert!(object.path.as_ref().ends_with("part-00000.parquet"));
        assert_eq!(object.file_size, 123);

        Ok(())
    }

    #[test]
    fn native_async_reader_constructs_memory_object_store_for_remote_like_uri()
    -> Result<(), Box<dyn std::error::Error>> {
        let table_uri = "memory:///table/root/";
        let storage_options = DeltaStorageOptions::default();
        validate_native_async_reader_config(DeltaNativeAsyncFileReaderConfig {
            source_name: "orders",
            table_uri,
            snapshot_version: 42,
            storage_options: &storage_options,
        })?;
        let reader = reader(table_uri)?;
        let object = reader.parquet_object_for_task(&task(table_uri, "part-00000.parquet"))?;

        assert_eq!(object.path.as_ref(), "table/root/part-00000.parquet");

        Ok(())
    }

    #[test]
    fn native_async_reader_config_passes_storage_options_to_each_store_construction()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheme = unique_storage_scheme("options")?;
        let captured = CapturedStorageOptions::default();
        register_capturing_storage_handler(&scheme, Arc::clone(&captured))?;
        let table_uri = format!("{scheme}://table/root/");
        let options = storage_options(&[
            ("authorization", "native-token"),
            ("endpoint", "http://storage.example"),
        ]);

        validate_native_async_reader_config(DeltaNativeAsyncFileReaderConfig {
            source_name: "orders",
            table_uri: &table_uri,
            snapshot_version: 42,
            storage_options: &options,
        })?;

        let captured_options = captured_storage_options(&captured);
        assert_eq!(captured_options.len(), 3);
        assert!(captured_options.iter().all(|captured| captured == &options));

        Ok(())
    }

    #[tokio::test]
    async fn native_async_reader_reads_remote_like_memory_object_store_parquet_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let table_uri = "memory:///table/root/";
        let reader = reader(table_uri)?;
        let read_schema = default_read_schema("native-async-memory-object-store-read")?;
        let parquet_bytes = default_parquet_bytes()?;
        let mut task = task(table_uri, "part-00000.parquet");

        task.estimated_bytes = Some(u64::try_from(parquet_bytes.len())?);
        let object = reader.parquet_object_for_task(&task)?;
        reader.store.put(&object.path, parquet_bytes.into()).await?;

        let stream = reader
            .open_file_stream(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await?;
        let batches = collect_file_stream(stream).await?;
        let batch = batches.first().ok_or("expected one remote-like batch")?;
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

        assert_eq!(batches.len(), 1);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(ids.values(), &[1, 2, 3]);
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");
        assert!(names.is_null(2));

        Ok(())
    }

    #[test]
    fn native_async_reader_rejects_unsupported_object_store_scheme() {
        let storage_options = DeltaStorageOptions::default();
        let error = validate_native_async_reader_config(DeltaNativeAsyncFileReaderConfig {
            source_name: "orders",
            table_uri: "ftp://example.com/table/",
            snapshot_version: 42,
            storage_options: &storage_options,
        })
        .expect_err("unsupported object store scheme must fail");

        assert!(matches!(
            error,
            DeltaFunnelError::DeltaScanFileRead {
                phase: DeltaScanFileReadPhase::ObjectStoreEngineConstruction,
                ..
            }
        ));
    }

    #[test]
    fn native_async_reader_requires_file_size() -> Result<(), Box<dyn std::error::Error>> {
        let table_uri = "memory:///table/root/";
        let reader = reader(table_uri)?;
        let mut task = task(table_uri, "part-00000.parquet");
        task.estimated_bytes = None;
        let error = match reader.parquet_object_for_task(&task) {
            Ok(_) => return Err("missing file size must fail".into()),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            DeltaFunnelError::DeltaScanFileRead {
                phase: DeltaScanFileReadPhase::FileMetadataConversion,
                ..
            }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn native_async_reader_matches_parquet_field_ids_before_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let table =
            RealParquetDeltaTable::new_with_column_mapping("native-async-field-id-schema-match")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, None)?;
        let read_schema = scan.read_schema();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri(), source.storage_options())?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;
        let mut task = DeltaScanFileTask::from_kernel_metadata(
            source.name(),
            source.table_uri(),
            source.version(),
            file,
        )?;
        let table_uri = "memory:///table/root/";
        let parquet_bytes = parquet_bytes(
            Arc::new(Schema::new(vec![
                Field::new("stale_customer_name", DataType::Utf8, true)
                    .with_metadata(field_id_metadata(2)),
                Field::new("stale_id", DataType::Int32, false).with_metadata(field_id_metadata(1)),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )?;
        task.table_uri = table_uri.to_owned();
        task.snapshot_version = 42;
        task.path = "part-00000.parquet".to_owned();
        task.estimated_bytes = Some(u64::try_from(parquet_bytes.len())?);
        let reader = reader(table_uri)?;
        let object = reader.parquet_object_for_task(&task)?;
        reader.store.put(&object.path, parquet_bytes.into()).await?;

        let stream = reader
            .open_file_stream(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await?;
        let batches = collect_file_stream(stream).await?;
        let batch = batches.first().ok_or("expected one record batch")?;
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

        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "customer_name");
        assert_eq!(ids.values(), &[1, 2, 3]);
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");
        assert!(names.is_null(2));

        Ok(())
    }

    #[tokio::test]
    async fn native_async_reader_fills_missing_nullable_provider_columns()
    -> Result<(), Box<dyn std::error::Error>> {
        let table_uri = "memory:///table/root/";
        let reader = reader(table_uri)?;
        let read_schema = default_read_schema("native-async-missing-nullable-column")?;
        let parquet_bytes = parquet_bytes(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let mut task = task(table_uri, "part-00000.parquet");

        task.estimated_bytes = Some(u64::try_from(parquet_bytes.len())?);
        let object = reader.parquet_object_for_task(&task)?;
        reader.store.put(&object.path, parquet_bytes.into()).await?;

        let stream = reader
            .open_file_stream(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await?;
        let batches = collect_file_stream(stream).await?;
        let batch = batches.first().ok_or("expected one record batch")?;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected customer_name StringArray")?;

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(names.len(), 3);
        assert_eq!(names.null_count(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_reader_rejects_missing_non_nullable_provider_columns()
    -> Result<(), Box<dyn std::error::Error>> {
        let table_uri = "memory:///table/root/";
        let reader = reader(table_uri)?;
        let read_schema = default_read_schema("native-async-missing-required-column")?;
        let parquet_bytes = parquet_bytes(
            Arc::new(Schema::new(vec![Field::new(
                "customer_name",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![
                Some("alice"),
                Some("bob"),
            ]))],
        )?;
        let mut task = task(table_uri, "part-00000.parquet");

        task.estimated_bytes = Some(u64::try_from(parquet_bytes.len())?);
        let object = reader.parquet_object_for_task(&task)?;
        reader.store.put(&object.path, parquet_bytes.into()).await?;
        let error = match reader
            .open_file_stream(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await
        {
            Ok(_) => return Err("missing non-nullable provider column must fail".into()),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            DeltaFunnelError::DeltaScanFileRead {
                phase: DeltaScanFileReadPhase::ArrowConversion,
                ..
            }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn native_async_reader_reads_real_non_dv_parquet_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("native-async-file-read")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, None)?;
        let read_schema = scan.read_schema();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri(), source.storage_options())?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;
        let task = DeltaScanFileTask::from_kernel_metadata(
            source.name(),
            source.table_uri(),
            source.version(),
            file,
        )?;
        let reader = DeltaNativeAsyncFileReader::try_new(DeltaNativeAsyncFileReaderConfig {
            source_name: source.name(),
            table_uri: source.table_uri(),
            snapshot_version: source.version(),
            storage_options: source.storage_options(),
        })?;

        let stream = reader
            .open_file_stream(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await?;
        assert_eq!(
            stream.deletion_vector_stats(),
            DeltaFileReadDeletionVectorStats::default()
        );
        let batches = collect_file_stream(stream).await?;

        assert_eq!(batches.len(), 1);
        let batch = batches.first().ok_or("expected one record batch")?;
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

    #[tokio::test]
    async fn native_async_reader_reads_hidden_original_row_indexes_without_exposing_helper()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("native-async-hidden-row-index")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, None)?;
        let read_schema = scan.read_schema();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri(), source.storage_options())?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;
        let task = DeltaScanFileTask::from_kernel_metadata(
            source.name(),
            source.table_uri(),
            source.version(),
            file,
        )?;
        let reader = DeltaNativeAsyncFileReader::try_new(DeltaNativeAsyncFileReaderConfig {
            source_name: source.name(),
            table_uri: source.table_uri(),
            snapshot_version: source.version(),
            storage_options: source.storage_options(),
        })?;

        let mut stream = reader
            .open_file_stream_with_original_row_index(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await?;
        let (batch, row_indexes) = stream
            .next_batch_with_original_row_indexes()
            .await?
            .ok_or("expected one record batch")?;

        assert_eq!(row_indexes, Some(vec![0, 1, 2]));
        assert_eq!(batch.num_rows(), table.rows());
        assert_eq!(batch.num_columns(), 2);
        assert!(
            batch
                .schema()
                .field_with_name(super::ORIGINAL_ROW_INDEX_COLUMN)
                .is_err()
        );
        assert!(stream.next_batch().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn native_async_reader_applies_provider_enforced_row_predicate()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("native-async-physical-predicate")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("id").gt(lit(1_i32)))?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, Some(predicate))?;
        let read_schema = scan
            .read_schema()
            .with_provider_enforced_physical_predicate_rows();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri(), source.storage_options())?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;
        let task = DeltaScanFileTask::from_kernel_metadata(
            source.name(),
            source.table_uri(),
            source.version(),
            file,
        )?;
        let reader = DeltaNativeAsyncFileReader::try_new(DeltaNativeAsyncFileReaderConfig {
            source_name: source.name(),
            table_uri: source.table_uri(),
            snapshot_version: source.version(),
            storage_options: source.storage_options(),
        })?;

        assert!(read_schema.enforces_physical_predicate_rows());

        let stream = reader
            .open_file_stream(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await?;
        let batches = collect_file_stream(stream).await?;
        let ids = batches
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("expected id Int32Array")
                    .map(|ids| ids.values().to_vec())
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![2, 3]);
        assert!(batches.iter().all(|batch| batch.num_columns() == 2));

        Ok(())
    }

    #[tokio::test]
    async fn native_async_reader_applies_deletion_vector_after_provider_enforced_row_predicate()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_deletion_vector(
            "native-async-dv-physical-predicate",
            &[1],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("id").gt(lit(1_i32)))?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, Some(predicate))?;
        let read_schema = scan
            .read_schema()
            .with_provider_enforced_physical_predicate_rows();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri(), source.storage_options())?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;
        let task = DeltaScanFileTask::from_kernel_metadata(
            source.name(),
            source.table_uri(),
            source.version(),
            file,
        )?;
        let reader = DeltaNativeAsyncFileReader::try_new(DeltaNativeAsyncFileReaderConfig {
            source_name: source.name(),
            table_uri: source.table_uri(),
            snapshot_version: source.version(),
            storage_options: source.storage_options(),
        })?;

        assert!(task.deletion_vector.is_present());
        assert!(read_schema.enforces_physical_predicate_rows());

        let stream = reader
            .open_file_stream(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await?;
        let batches = collect_file_stream(stream).await?;
        let ids = batches
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("expected id Int32Array")
                    .map(|ids| ids.values().to_vec())
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![3]);
        assert!(batches.iter().all(|batch| {
            batch
                .schema()
                .field_with_name(super::ORIGINAL_ROW_INDEX_COLUMN)
                .is_err()
        }));

        Ok(())
    }

    #[tokio::test]
    async fn native_async_reader_prunes_row_groups_with_physical_predicate_stats()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_two_row_groups_and_deletion_vector(
            "native-async-row-group-pruning",
            3,
            &[],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("id").gt(lit(3_i32)))?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, Some(predicate))?;
        let read_schema = scan.read_schema();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri(), source.storage_options())?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;
        let task = DeltaScanFileTask::from_kernel_metadata(
            source.name(),
            source.table_uri(),
            source.version(),
            file,
        )?;
        let reader = DeltaNativeAsyncFileReader::try_new(DeltaNativeAsyncFileReaderConfig {
            source_name: source.name(),
            table_uri: source.table_uri(),
            snapshot_version: source.version(),
            storage_options: source.storage_options(),
        })?;
        let object = reader.parquet_object_for_task(&task)?;
        let parquet_reader =
            ParquetObjectReader::new(object.store, object.path).with_file_size(object.file_size);
        let builder = ParquetRecordBatchStreamBuilder::new(parquet_reader).await?;

        assert_eq!(
            native_async_pruned_row_groups(builder.metadata(), &read_schema),
            Some(vec![1])
        );

        Ok(())
    }

    #[tokio::test]
    async fn native_async_row_group_pruning_preserves_negative_fixed_len_decimal_stats()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_supported_types(
            "native-async-row-group-negative-decimal",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let negative = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let predicate = datafusion_expr_to_kernel_predicate(&col("amount").lt(negative))?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, Some(predicate))?;
        let read_schema = scan.read_schema();
        let (test_dir, _table_uri) = local_table_uri("negative-decimal-row-groups")?;
        let file_path = test_dir.path.join("negative-decimal.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            true,
        )]));
        let amounts =
            Decimal128Array::from(vec![Some(-100), Some(100)]).with_precision_and_scale(10, 2)?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(amounts)])?;
        let writer_properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .build();
        let mut writer = ArrowWriter::try_new(
            fs::File::create(&file_path)?,
            schema,
            Some(writer_properties),
        )?;
        writer.write(&batch)?;
        writer.close()?;
        let parquet_reader = SerializedFileReader::new(fs::File::open(file_path)?)?;

        assert_eq!(parquet_reader.metadata().num_row_groups(), 2);
        assert_eq!(
            native_async_pruned_row_groups(parquet_reader.metadata(), &read_schema),
            Some(vec![0])
        );

        Ok(())
    }

    #[tokio::test]
    async fn native_async_reader_preserves_dv_row_indexes_after_row_group_pruning()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_two_row_groups_and_deletion_vector(
            "native-async-dv-row-group-pruning",
            3,
            &[4],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("id").gt(lit(3_i32)))?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, Some(predicate))?;
        let read_schema = scan.read_schema();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri(), source.storage_options())?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;
        let task = DeltaScanFileTask::from_kernel_metadata(
            source.name(),
            source.table_uri(),
            source.version(),
            file,
        )?;
        let reader = DeltaNativeAsyncFileReader::try_new(DeltaNativeAsyncFileReaderConfig {
            source_name: source.name(),
            table_uri: source.table_uri(),
            snapshot_version: source.version(),
            storage_options: source.storage_options(),
        })?;

        let stream = reader
            .open_file_stream(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await?;
        let batches = collect_file_stream(stream).await?;
        let ids = batches
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("expected id Int32Array")
                    .map(|ids| ids.values().to_vec())
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![4, 6]);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_reader_reads_projected_real_non_dv_parquet_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("native-async-projected-file-read")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let projected_columns = vec!["customer_name".to_owned()];
        let scan = build_projected_delta_scan(&source, Some(&projected_columns))?;
        let read_schema = scan.read_schema();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri(), source.storage_options())?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;
        let task = DeltaScanFileTask::from_kernel_metadata(
            source.name(),
            source.table_uri(),
            source.version(),
            file,
        )?;
        let reader = DeltaNativeAsyncFileReader::try_new(DeltaNativeAsyncFileReaderConfig {
            source_name: source.name(),
            table_uri: source.table_uri(),
            snapshot_version: source.version(),
            storage_options: source.storage_options(),
        })?;

        let stream = reader
            .open_file_stream(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await?;
        let batches = collect_file_stream(stream).await?;
        let batch = batches.first().ok_or("expected one record batch")?;

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "customer_name");

        Ok(())
    }
}
