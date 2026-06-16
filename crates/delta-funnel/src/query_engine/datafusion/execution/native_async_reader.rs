//! Native async Parquet reader boundary for provider file tasks.
//!
//! The native backend uses the same Delta table URI normalization and object-store
//! construction path as the official-kernel baseline, then hands resolved
//! `object_store::Path` values to `ParquetObjectReader`. That keeps local and
//! remote table URI semantics aligned while allowing parquet-rs to issue async
//! range reads through the selected object-store handle.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, ListArray, MapArray, StructArray, new_null_array,
};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Field, Fields, SchemaRef};
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

#[derive(Clone)]
enum NativeAsyncProviderColumn {
    /// Column index in the projected Parquet stream batch.
    ///
    /// This is not the original Parquet file root index. The stream only emits
    /// the roots selected by `projected_roots`, in file order, so this index is
    /// relative to that projected output batch.
    ProjectedStreamColumn {
        stream_index: usize,
        field_plan: NativeAsyncFieldPlan,
    },
    /// Missing nullable provider field that must be materialized as all nulls.
    Null,
}

#[derive(Clone)]
enum NativeAsyncFieldPlan {
    Identity,
    Struct {
        children: Vec<NativeAsyncStructChild>,
    },
    List {
        element_plan: Box<NativeAsyncFieldPlan>,
    },
    Map {
        key_plan: Box<NativeAsyncFieldPlan>,
        value_plan: Box<NativeAsyncFieldPlan>,
    },
}

impl NativeAsyncFieldPlan {
    fn is_identity(&self) -> bool {
        matches!(self, Self::Identity)
    }
}

#[derive(Clone)]
enum NativeAsyncStructChild {
    ProjectedChild {
        child_index: usize,
        field_plan: NativeAsyncFieldPlan,
    },
    Null,
}

#[derive(Clone)]
struct NativeAsyncRootMatch {
    parquet_root_index: usize,
    field_plan: NativeAsyncFieldPlan,
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
                NativeAsyncProviderColumn::ProjectedStreamColumn {
                    stream_index,
                    field_plan,
                } => reshape_array_to_provider_field(
                    Arc::clone(batch.column(*stream_index)),
                    field,
                    field_plan,
                ),
                NativeAsyncProviderColumn::Null => {
                    Ok(new_null_array(field.data_type(), batch.num_rows()))
                }
            })
            .collect::<Result<Vec<ArrayRef>, _>>()?;

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
) -> Result<Vec<Option<NativeAsyncRootMatch>>, delta_kernel::Error> {
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

fn projected_roots_from_matches(root_matches: &[Option<NativeAsyncRootMatch>]) -> Vec<usize> {
    // Keep only matched Parquet root indexes. Unmatched nullable provider fields
    // are represented by None and become null-filled columns later.
    let mut projected_roots = root_matches
        .iter()
        .filter_map(|root_match| {
            root_match
                .as_ref()
                .map(|root_match| root_match.parquet_root_index)
        })
        .collect::<Vec<_>>();
    projected_roots.sort_unstable();
    projected_roots.dedup();

    projected_roots
}

fn provider_columns_from_root_matches(
    root_matches: &[Option<NativeAsyncRootMatch>],
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
            Some(root_match) => projected_roots
                .iter()
                .position(|projected_root| projected_root == &root_match.parquet_root_index)
                .map(
                    |stream_index| NativeAsyncProviderColumn::ProjectedStreamColumn {
                        stream_index,
                        field_plan: root_match.field_plan.clone(),
                    },
                )
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
            NativeAsyncProviderColumn::ProjectedStreamColumn {
                stream_index,
                field_plan,
            } => {
                *stream_index != provider_index
                    || !field_plan.is_identity()
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
) -> Result<Option<NativeAsyncRootMatch>, delta_kernel::Error> {
    // Delta schema matching uses field ids first. Column mapping tables can
    // rename logical or physical columns over time, but a stable field id still
    // identifies the same Delta column in Parquet metadata.
    let provider_field_id = arrow_field_id(provider_field)?;
    if let Some(field_id) = provider_field_id {
        let matches = parquet_roots
            .iter()
            .enumerate()
            .filter_map(|(index, parquet_root)| {
                (parquet_field_id(parquet_root) == Some(field_id)).then_some(index)
            })
            .collect::<Vec<_>>();

        match matches.as_slice() {
            [index] => {
                let field_plan = build_matched_field_plan(
                    provider_field,
                    parquet_arrow_schema.field(*index),
                    parquet_roots[*index].as_ref(),
                    provider_field.name(),
                )?;
                return Ok(Some(NativeAsyncRootMatch {
                    parquet_root_index: *index,
                    field_plan,
                }));
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

    let field_plan = build_matched_field_plan(
        provider_field,
        file_field,
        parquet_roots[index].as_ref(),
        provider_field.name(),
    )?;

    Ok(Some(NativeAsyncRootMatch {
        parquet_root_index: index,
        field_plan,
    }))
}

fn build_matched_field_plan(
    provider_field: &Field,
    file_field: &Field,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<NativeAsyncFieldPlan, delta_kernel::Error> {
    match (provider_field.data_type(), file_field.data_type()) {
        (DataType::Struct(provider_fields), DataType::Struct(file_fields)) => {
            build_matched_struct_field_plan(
                provider_field,
                provider_fields,
                file_field,
                file_fields,
                parquet_field,
                path,
            )
        }
        (DataType::List(provider_element), DataType::List(file_element)) => {
            build_matched_list_field_plan(
                provider_field,
                provider_element,
                file_field,
                file_element,
                parquet_field,
                path,
            )
        }
        (DataType::Map(provider_entries, provider_ordered), DataType::Map(file_entries, _)) => {
            build_matched_map_field_plan(
                provider_entries,
                *provider_ordered,
                file_field,
                file_entries,
                parquet_field,
                path,
            )
        }
        _ if file_field
            .data_type()
            .equals_datatype(provider_field.data_type()) =>
        {
            Ok(NativeAsyncFieldPlan::Identity)
        }
        _ => Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected Parquet type {} but found {}",
            provider_field.data_type(),
            file_field.data_type()
        ))),
    }
}

fn build_matched_map_field_plan(
    provider_entries: &Arc<Field>,
    provider_ordered: bool,
    file_field: &Field,
    file_entries: &Arc<Field>,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<NativeAsyncFieldPlan, delta_kernel::Error> {
    let (provider_key, provider_value) = map_entry_fields(provider_entries, path)?;
    let (file_key, file_value) = map_entry_fields(file_entries, path)?;

    if !file_key
        .data_type()
        .equals_datatype(provider_key.data_type())
    {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}.key' expected Parquet type {} but found {}",
            provider_key.data_type(),
            file_key.data_type()
        )));
    }

    let value_path = format!("{path}.value");
    let parquet_value = parquet_map_value_field(parquet_field, path)?;
    let value_plan =
        build_matched_field_plan(provider_value, file_value, parquet_value, &value_path)?;
    let provider_map_type = DataType::Map(Arc::clone(provider_entries), provider_ordered);
    let key_plan = NativeAsyncFieldPlan::Identity;
    let needs_reshape = file_field.data_type() != &provider_map_type || !value_plan.is_identity();

    if needs_reshape {
        Ok(NativeAsyncFieldPlan::Map {
            key_plan: Box::new(key_plan),
            value_plan: Box::new(value_plan),
        })
    } else {
        Ok(NativeAsyncFieldPlan::Identity)
    }
}

fn map_entry_fields<'a>(
    entries: &'a Field,
    path: &str,
) -> Result<(&'a Field, &'a Field), delta_kernel::Error> {
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected map entries struct but has type {}",
            entries.data_type()
        )));
    };
    if fields.len() != 2 {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected map entries to contain key and value fields but found {}",
            fields.len()
        )));
    }

    let key = fields.first().ok_or_else(|| {
        delta_kernel::Error::generic(format!(
            "provider field '{path}' is missing map key metadata"
        ))
    })?;
    let value = fields.get(1).ok_or_else(|| {
        delta_kernel::Error::generic(format!(
            "provider field '{path}' is missing map value metadata"
        ))
    })?;

    Ok((key.as_ref(), value.as_ref()))
}

fn parquet_map_value_field<'a>(
    parquet_field: &'a parquet::schema::types::Type,
    path: &str,
) -> Result<&'a parquet::schema::types::Type, delta_kernel::Error> {
    let parquet_children = parquet_field.get_fields();
    let Some(repeated_child) = parquet_children.first() else {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected Parquet map entry metadata"
        )));
    };
    if parquet_children.len() != 1 {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected one Parquet map entry child but found {}",
            parquet_children.len()
        )));
    }

    let entry_children = repeated_child.get_fields();
    let Some(value) = entry_children.get(1) else {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected Parquet map entry key and value fields"
        )));
    };
    if entry_children.len() != 2 {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected Parquet map entry to contain two fields but found {}",
            entry_children.len()
        )));
    }

    Ok(value.as_ref())
}

fn build_matched_list_field_plan(
    provider_field: &Field,
    provider_element: &Arc<Field>,
    file_field: &Field,
    file_element: &Arc<Field>,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<NativeAsyncFieldPlan, delta_kernel::Error> {
    let element_path = format!("{path}.element");
    let parquet_element = parquet_list_element_field(parquet_field, path)?;
    let element_plan = build_matched_field_plan(
        provider_element.as_ref(),
        file_element.as_ref(),
        parquet_element,
        &element_path,
    )?;

    let needs_reshape =
        file_field.data_type() != provider_field.data_type() || !element_plan.is_identity();

    if needs_reshape {
        Ok(NativeAsyncFieldPlan::List {
            element_plan: Box::new(element_plan),
        })
    } else {
        Ok(NativeAsyncFieldPlan::Identity)
    }
}

fn parquet_list_element_field<'a>(
    parquet_field: &'a parquet::schema::types::Type,
    path: &str,
) -> Result<&'a parquet::schema::types::Type, delta_kernel::Error> {
    let parquet_children = parquet_field.get_fields();
    let Some(repeated_child) = parquet_children.first() else {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected Parquet list element metadata"
        )));
    };
    if parquet_children.len() != 1 {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected one Parquet list child but found {}",
            parquet_children.len()
        )));
    }

    let repeated_child_fields = repeated_child.get_fields();
    if repeated_child_fields.len() == 1 {
        Ok(repeated_child_fields[0].as_ref())
    } else {
        Ok(repeated_child.as_ref())
    }
}

fn build_matched_struct_field_plan(
    provider_field: &Field,
    provider_fields: &Fields,
    file_field: &Field,
    file_fields: &Fields,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<NativeAsyncFieldPlan, delta_kernel::Error> {
    let parquet_children = parquet_field.get_fields();
    if parquet_children.len() != file_fields.len() {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected Parquet struct field metadata to match Arrow child count"
        )));
    }

    let children = provider_fields
        .iter()
        .map(|provider_child| {
            let child_path = format!("{path}.{}", provider_child.name());
            match_provider_struct_child(provider_child, file_fields, parquet_children, &child_path)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let needs_reshape = file_field.data_type() != provider_field.data_type()
        || children.iter().zip(provider_fields.iter()).enumerate().any(
            |(provider_index, (child, provider_child))| match child {
                NativeAsyncStructChild::ProjectedChild {
                    child_index,
                    field_plan,
                } => {
                    *child_index != provider_index
                        || !field_plan.is_identity()
                        || file_fields
                            .get(*child_index)
                            .is_none_or(|file_child| file_child.name() != provider_child.name())
                }
                NativeAsyncStructChild::Null => true,
            },
        );

    if needs_reshape {
        Ok(NativeAsyncFieldPlan::Struct { children })
    } else {
        Ok(NativeAsyncFieldPlan::Identity)
    }
}

fn match_provider_struct_child(
    provider_child: &Field,
    file_fields: &Fields,
    parquet_children: &[TypePtr],
    path: &str,
) -> Result<NativeAsyncStructChild, delta_kernel::Error> {
    let provider_field_id = arrow_field_id(provider_child)?;
    if let Some(field_id) = provider_field_id {
        let matches = parquet_children
            .iter()
            .enumerate()
            .filter_map(|(index, parquet_child)| {
                (parquet_field_id(parquet_child) == Some(field_id)).then_some(index)
            })
            .collect::<Vec<_>>();

        match matches.as_slice() {
            [index] => {
                let file_child = file_fields.get(*index).ok_or_else(|| {
                    delta_kernel::Error::generic(format!(
                        "provider field '{path}' matched Parquet field id {field_id} without Arrow metadata"
                    ))
                })?;
                let field_plan = build_matched_field_plan(
                    provider_child,
                    file_child,
                    parquet_children[*index].as_ref(),
                    path,
                )?;
                return Ok(NativeAsyncStructChild::ProjectedChild {
                    child_index: *index,
                    field_plan,
                });
            }
            [] => {}
            _ => {
                return Err(delta_kernel::Error::generic(format!(
                    "multiple Parquet fields matched provider field id {field_id} at '{path}'"
                )));
            }
        }
    }

    let Some((index, file_child)) = file_fields
        .iter()
        .enumerate()
        .find(|(_, file_child)| file_child.name() == provider_child.name())
    else {
        if provider_child.is_nullable() {
            return Ok(NativeAsyncStructChild::Null);
        }

        return Err(delta_kernel::Error::generic(format!(
            "non-nullable provider field '{path}' is missing from the Parquet file"
        )));
    };

    let field_plan = build_matched_field_plan(
        provider_child,
        file_child,
        parquet_children[index].as_ref(),
        path,
    )?;

    Ok(NativeAsyncStructChild::ProjectedChild {
        child_index: index,
        field_plan,
    })
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

fn parquet_field_id(parquet_field: &TypePtr) -> Option<i32> {
    let basic_info = parquet_field.get_basic_info();
    basic_info.has_id().then(|| basic_info.id())
}

fn reshape_array_to_provider_field(
    array: ArrayRef,
    provider_field: &Field,
    field_plan: &NativeAsyncFieldPlan,
) -> Result<ArrayRef, delta_kernel::Error> {
    match field_plan {
        NativeAsyncFieldPlan::Identity => Ok(array),
        NativeAsyncFieldPlan::Struct { children } => {
            let DataType::Struct(provider_fields) = provider_field.data_type() else {
                return Err(delta_kernel::Error::generic(format!(
                    "provider field '{}' expected struct reshape plan but has type {}",
                    provider_field.name(),
                    provider_field.data_type()
                )));
            };
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    delta_kernel::Error::generic(format!(
                        "provider field '{}' expected Parquet struct array but found {}",
                        provider_field.name(),
                        array.data_type()
                    ))
                })?;
            let columns = children
                .iter()
                .zip(provider_fields.iter())
                .map(|(child, provider_child)| match child {
                    NativeAsyncStructChild::ProjectedChild {
                        child_index,
                        field_plan,
                    } => {
                        let child_array = struct_array.column(*child_index);
                        reshape_array_to_provider_field(
                            Arc::clone(child_array),
                            provider_child,
                            field_plan,
                        )
                    }
                    NativeAsyncStructChild::Null => Ok(new_null_array(
                        provider_child.data_type(),
                        struct_array.len(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Arc::new(StructArray::new(
                provider_fields.clone(),
                columns,
                struct_array.nulls().cloned(),
            )))
        }
        NativeAsyncFieldPlan::List { element_plan } => {
            let DataType::List(provider_element) = provider_field.data_type() else {
                return Err(delta_kernel::Error::generic(format!(
                    "provider field '{}' expected list reshape plan but has type {}",
                    provider_field.name(),
                    provider_field.data_type()
                )));
            };
            let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                delta_kernel::Error::generic(format!(
                    "provider field '{}' expected Parquet list array but found {}",
                    provider_field.name(),
                    array.data_type()
                ))
            })?;
            let values = reshape_array_to_provider_field(
                Arc::clone(list_array.values()),
                provider_element,
                element_plan,
            )?;

            ListArray::try_new(
                Arc::clone(provider_element),
                list_array.offsets().clone(),
                values,
                list_array.nulls().cloned(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(delta_kernel::Error::from)
        }
        NativeAsyncFieldPlan::Map {
            key_plan,
            value_plan,
        } => {
            let DataType::Map(provider_entries, provider_ordered) = provider_field.data_type()
            else {
                return Err(delta_kernel::Error::generic(format!(
                    "provider field '{}' expected map reshape plan but has type {}",
                    provider_field.name(),
                    provider_field.data_type()
                )));
            };
            let map_array = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                delta_kernel::Error::generic(format!(
                    "provider field '{}' expected Parquet map array but found {}",
                    provider_field.name(),
                    array.data_type()
                ))
            })?;
            let (provider_key, provider_value) =
                map_entry_fields(provider_entries, provider_field.name())?;
            let keys = reshape_array_to_provider_field(
                Arc::clone(map_array.keys()),
                provider_key,
                key_plan,
            )?;
            let values = reshape_array_to_provider_field(
                Arc::clone(map_array.values()),
                provider_value,
                value_plan,
            )?;
            let entries = StructArray::new(
                vec![
                    Arc::new(provider_key.clone()),
                    Arc::new(provider_value.clone()),
                ]
                .into(),
                vec![keys, values],
                map_array.entries().nulls().cloned(),
            );

            MapArray::try_new(
                Arc::clone(provider_entries),
                map_array.offsets().clone(),
                entries,
                map_array.nulls().cloned(),
                *provider_ordered,
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(delta_kernel::Error::from)
        }
    }
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
    physical_schema.fields().find_map(|field| {
        unsupported_native_async_field_reason(
            field,
            field.name(),
            true,
            NativeAsyncMapContext::Outside,
        )
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeAsyncMapContext {
    /// The field is not under a map key or map value subtree.
    Outside,
    /// The field is under a map key subtree. Native async does not reshape keys
    /// because that could change key equality or key/value association.
    Key,
    /// The field is under a map value subtree. Value fields can be reshaped the
    /// same way as nested struct/list fields while preserving map keys.
    Value,
}

fn unsupported_native_async_field_reason(
    field: &KernelStructField,
    path: &str,
    top_level: bool,
    map_context: NativeAsyncMapContext,
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
    // Map values can be reshaped independently, but map keys define entry
    // identity. Reject any key-side field matching until key reshaping is
    // explicitly supported.
    if matches!(map_context, NativeAsyncMapContext::Key)
        && has_field_matching_metadata(field, top_level)
    {
        return Some(format!(
            "native async reader does not support map key field-id or physical-name matching at '{path}' yet"
        ));
    }
    // Delta stores synthetic map key/value field ids on the map field itself.
    // A primitive key only needs that synthetic key id preserved by the
    // provider transform. A complex key can also need recursive key-side
    // matching, which native async intentionally rejects above.
    if has_nested_field_id_map_metadata(field)
        && matches!(field.data_type(), KernelDataType::Map(map) if !matches!(map.key_type(), KernelDataType::Primitive(_)))
    {
        return Some(format!(
            "native async reader does not support complex map key field-id or physical-name matching at '{path}' yet"
        ));
    }

    unsupported_native_async_data_type_reason(field.data_type(), path, map_context)
}

fn unsupported_native_async_data_type_reason(
    data_type: &KernelDataType,
    path: &str,
    map_context: NativeAsyncMapContext,
) -> Option<String> {
    match data_type {
        KernelDataType::Struct(fields) | KernelDataType::Variant(fields) => {
            fields.fields().find_map(|field| {
                let child_path = format!("{path}.{}", field.name());
                unsupported_native_async_field_reason(field, &child_path, false, map_context)
            })
        }
        KernelDataType::Array(array) => {
            let child_path = format!("{path}.element");
            unsupported_native_async_data_type_reason(
                array.element_type(),
                &child_path,
                map_context,
            )
        }
        KernelDataType::Map(map) => {
            let key_path = format!("{path}.key");
            unsupported_native_async_data_type_reason(
                map.key_type(),
                &key_path,
                NativeAsyncMapContext::Key,
            )
            .or_else(|| {
                let value_path = format!("{path}.value");
                unsupported_native_async_data_type_reason(
                    map.value_type(),
                    &value_path,
                    NativeAsyncMapContext::Value,
                )
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

fn has_field_matching_metadata(field: &KernelStructField, top_level: bool) -> bool {
    if has_nested_field_id_map_metadata(field) {
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

fn has_nested_field_id_map_metadata(field: &KernelStructField) -> bool {
    let nested_metadata_keys = [
        KernelColumnMetadataKey::ColumnMappingNestedIds,
        KernelColumnMetadataKey::ParquetFieldNestedIds,
    ];
    nested_metadata_keys
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

    use datafusion::arrow::array::{
        Array, ArrayRef, Decimal128Array, Int32Array, ListArray, MapArray, StringArray, StructArray,
    };
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, col, lit};
    use delta_kernel::object_store::{memory::InMemory, path::Path as ObjectStorePath};
    use object_store::ObjectStoreExt;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
    use parquet::arrow::{ArrowWriter, ProjectionMask};
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
                        source: std::io::Error::other("captured storage options lock poisoned")
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

    fn struct_field(name: &str, fields: Vec<Field>, nullable: bool) -> Field {
        Field::new(name, DataType::Struct(fields.into()), nullable)
    }

    fn struct_array(fields: Vec<Field>, columns: Vec<ArrayRef>) -> ArrayRef {
        struct_array_with_nulls(fields, columns, None)
    }

    fn struct_array_with_nulls(
        fields: Vec<Field>,
        columns: Vec<ArrayRef>,
        nulls: Option<NullBuffer>,
    ) -> ArrayRef {
        let children = fields
            .into_iter()
            .map(Arc::new)
            .zip(columns)
            .collect::<Vec<_>>();

        let fields = children
            .iter()
            .map(|(field, _column)| Arc::clone(field))
            .collect::<Vec<_>>()
            .into();
        let columns = children
            .into_iter()
            .map(|(_field, column)| column)
            .collect::<Vec<_>>();

        Arc::new(StructArray::new(fields, columns, nulls))
    }

    fn list_array(
        element: Field,
        offsets: Vec<i32>,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Result<ArrayRef, Box<dyn std::error::Error>> {
        Ok(Arc::new(ListArray::try_new(
            Arc::new(element),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            values,
            nulls,
        )?))
    }

    fn map_field(name: &str, key_field: Field, value_field: Field, nullable: bool) -> Field {
        let entries = vec![key_field, value_field].into();
        Field::new(
            name,
            DataType::Map(
                Arc::new(Field::new("entries", DataType::Struct(entries), false)),
                false,
            ),
            nullable,
        )
    }

    fn map_array(
        key_field: Field,
        value_field: Field,
        offsets: Vec<i32>,
        keys: ArrayRef,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Result<ArrayRef, Box<dyn std::error::Error>> {
        let entries = vec![key_field.clone(), value_field.clone()].into();
        Ok(Arc::new(MapArray::try_new(
            Arc::new(Field::new("entries", DataType::Struct(entries), false)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            StructArray::new(
                vec![Arc::new(key_field), Arc::new(value_field)].into(),
                vec![keys, values],
                None,
            ),
            nulls,
            false,
        )?))
    }

    fn project_parquet_batch_to_provider_schema(
        name: &str,
        file_schema: Arc<Schema>,
        columns: Vec<ArrayRef>,
        provider_schema: Arc<Schema>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let (test_dir, _table_uri) = local_table_uri(name)?;
        let file_path = test_dir.path.join("part-00000.parquet");
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), columns)?;
        let mut writer = ArrowWriter::try_new(fs::File::create(&file_path)?, file_schema, None)?;

        writer.write(&batch)?;
        writer.close()?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(file_path)?)?;
        let schema_match = super::build_native_async_schema_match(
            builder.parquet_schema(),
            builder.schema(),
            provider_schema,
        )?;
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), schema_match.projected_roots());
        let mut reader = builder.with_projection(projection).build()?;
        let projected_batch = reader
            .next()
            .transpose()?
            .ok_or("expected one projected Parquet batch")?;

        Ok(schema_match.reshape_batch_to_provider_schema(projected_batch)?)
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
    fn native_async_schema_gate_allows_nested_struct_field_id_matching()
    -> Result<(), Box<dyn std::error::Error>> {
        let nested_type = KernelStructType::try_new([KernelStructField::new(
            "inner",
            KernelDataType::INTEGER,
            true,
        )
        .add_metadata(kernel_field_id_metadata(2))])?;
        let schema = kernel_schema([KernelStructField::new("nested", nested_type, true)])?;

        assert_eq!(
            unsupported_native_async_physical_schema_reason(&schema),
            None
        );

        Ok(())
    }

    #[test]
    fn native_async_schema_gate_allows_array_nested_field_id_matching()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = kernel_schema([KernelStructField::new(
            "tags",
            delta_kernel::schema::ArrayType::new(KernelDataType::STRING, true),
            true,
        )
        .add_metadata([(
            KernelColumnMetadataKey::ColumnMappingNestedIds
                .as_ref()
                .to_owned(),
            KernelMetadataValue::String(r#"{"tags.element":2}"#.to_owned()),
        )])])?;

        assert_eq!(
            unsupported_native_async_physical_schema_reason(&schema),
            None
        );

        Ok(())
    }

    #[test]
    fn native_async_schema_gate_allows_map_value_nested_field_id_matching()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = kernel_schema([KernelStructField::new(
            "attributes",
            delta_kernel::schema::MapType::new(
                KernelDataType::STRING,
                KernelDataType::INTEGER,
                true,
            ),
            true,
        )
        .add_metadata([(
            KernelColumnMetadataKey::ColumnMappingNestedIds
                .as_ref()
                .to_owned(),
            KernelMetadataValue::String(r#"{"attributes.value":2}"#.to_owned()),
        )])])?;

        assert_eq!(
            unsupported_native_async_physical_schema_reason(&schema),
            None
        );

        Ok(())
    }

    #[test]
    fn native_async_schema_gate_rejects_map_key_field_id_matching()
    -> Result<(), Box<dyn std::error::Error>> {
        let key_type = KernelStructType::try_new([KernelStructField::new(
            "name",
            KernelDataType::STRING,
            true,
        )
        .add_metadata(kernel_field_id_metadata(2))])?;
        let schema = kernel_schema([KernelStructField::new(
            "attributes",
            delta_kernel::schema::MapType::new(
                KernelDataType::Struct(Box::new(key_type)),
                KernelDataType::INTEGER,
                true,
            ),
            true,
        )])?;
        let reason =
            unsupported_native_async_physical_schema_reason(&schema).ok_or("expected rejection")?;

        assert!(reason.contains("map key field-id"));
        assert!(reason.contains("attributes.key.name"));

        Ok(())
    }

    #[test]
    fn native_async_schema_gate_rejects_complex_map_key_nested_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = kernel_schema([KernelStructField::new(
            "attributes",
            delta_kernel::schema::MapType::new(
                delta_kernel::schema::ArrayType::new(KernelDataType::STRING, true),
                KernelDataType::INTEGER,
                true,
            ),
            true,
        )
        .add_metadata([(
            KernelColumnMetadataKey::ColumnMappingNestedIds
                .as_ref()
                .to_owned(),
            KernelMetadataValue::String(r#"{"attributes.key":2}"#.to_owned()),
        )])])?;
        let reason =
            unsupported_native_async_physical_schema_reason(&schema).ok_or("expected rejection")?;

        assert!(reason.contains("complex map key field-id"));
        assert!(reason.contains("attributes"));

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

    #[test]
    fn native_async_schema_match_recurses_by_nested_field_id_before_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_profile_fields = vec![
            Field::new("first_name", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("age", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let provider_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            provider_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![
            Field::new("stale_age", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_name", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array_with_nulls(
            file_profile_fields,
            vec![
                Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])) as ArrayRef,
            ],
            Some(NullBuffer::from(vec![true, false])),
        );

        let batch = project_parquet_batch_to_provider_schema(
            "nested-field-id-schema-match",
            file_schema,
            vec![profile],
            provider_schema,
        )?;
        let profile = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected profile StructArray")?;
        let names = profile
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected first_name StringArray")?;
        let ages = profile
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected age Int32Array")?;

        assert_eq!(profile.fields()[0].name(), "first_name");
        assert_eq!(profile.fields()[1].name(), "age");
        assert!(profile.is_valid(0));
        assert!(profile.is_null(1));
        assert_eq!(names.value(0), "alice");
        assert_eq!(ages.value(0), 34);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_reshapes_list_struct_elements_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_address_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let provider_element = Field::new(
            "item",
            DataType::Struct(provider_address_fields.into()),
            true,
        );
        let provider_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(provider_element)),
            true,
        )]));
        let file_address_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_element = Field::new(
            "item",
            DataType::Struct(file_address_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(file_element.clone())),
            true,
        )]));
        let values = struct_array(
            file_address_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let addresses = list_array(
            file_element,
            vec![0, 2, 2, 3],
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "list-struct-field-id-schema-match",
            file_schema,
            vec![addresses],
            provider_schema,
        )?;
        let addresses = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected addresses ListArray")?;
        let values = addresses
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected address element StructArray")?;
        let cities = values
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = values
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(addresses.value_offsets(), &[0, 2, 2, 3]);
        assert!(addresses.is_valid(0));
        assert!(addresses.is_null(1));
        assert!(addresses.is_valid(2));
        assert_eq!(values.fields()[0].name(), "city");
        assert_eq!(values.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_recurses_by_local_nested_name_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_profile_fields = vec![
            Field::new("age", DataType::Int32, true),
            Field::new("first_name", DataType::Utf8, true),
        ];
        let provider_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            provider_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![
            Field::new("first_name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array(
            file_profile_fields,
            vec![
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])) as ArrayRef,
                Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef,
            ],
        );

        let batch = project_parquet_batch_to_provider_schema(
            "nested-name-fallback-schema-match",
            file_schema,
            vec![profile],
            provider_schema,
        )?;
        let profile = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected profile StructArray")?;
        let ages = profile
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected age Int32Array")?;
        let names = profile
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected first_name StringArray")?;

        assert_eq!(profile.fields()[0].name(), "age");
        assert_eq!(profile.fields()[1].name(), "first_name");
        assert_eq!(ages.values(), &[34, 41]);
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");

        Ok(())
    }

    #[test]
    fn native_async_schema_match_null_fills_missing_nullable_nested_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_profile_fields = vec![
            Field::new("age", DataType::Int32, true),
            Field::new("loyalty_tier", DataType::Utf8, true),
        ];
        let provider_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            provider_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![Field::new("age", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array(
            file_profile_fields,
            vec![Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef],
        );

        let batch = project_parquet_batch_to_provider_schema(
            "nested-missing-nullable-schema-match",
            file_schema,
            vec![profile],
            provider_schema,
        )?;
        let profile = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected profile StructArray")?;
        let loyalty_tiers = profile
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected loyalty_tier StringArray")?;

        assert_eq!(profile.fields()[1].name(), "loyalty_tier");
        assert_eq!(loyalty_tiers.len(), 2);
        assert_eq!(loyalty_tiers.null_count(), 2);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_null_fills_missing_nullable_list_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_address_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("country", DataType::Utf8, true),
        ];
        let provider_element = Field::new(
            "item",
            DataType::Struct(provider_address_fields.into()),
            true,
        );
        let provider_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(provider_element)),
            true,
        )]));
        let file_address_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_element = Field::new(
            "item",
            DataType::Struct(file_address_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(file_element.clone())),
            true,
        )]));
        let values = struct_array(
            file_address_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001, 60601, 85001, 73301])) as ArrayRef],
        );
        let addresses = list_array(
            file_element,
            vec![0, 2, 2, 5],
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "list-struct-missing-nullable-schema-match",
            file_schema,
            vec![addresses],
            provider_schema,
        )?;
        let addresses = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected addresses ListArray")?;
        let values = addresses
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected address element StructArray")?;
        let countries = values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected country StringArray")?;

        assert_eq!(addresses.value_offsets(), &[0, 2, 2, 5]);
        assert!(addresses.is_null(1));
        assert_eq!(values.fields()[1].name(), "country");
        assert_eq!(countries.len(), 5);
        assert_eq!(countries.null_count(), 5);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_rejects_missing_non_nullable_list_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_address_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("required_country", DataType::Utf8, false),
        ];
        let provider_element = Field::new(
            "item",
            DataType::Struct(provider_address_fields.into()),
            true,
        );
        let provider_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(provider_element)),
            true,
        )]));
        let file_address_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_element = Field::new(
            "item",
            DataType::Struct(file_address_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(file_element.clone())),
            true,
        )]));
        let values = struct_array(
            file_address_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001])) as ArrayRef],
        );
        let addresses = list_array(file_element, vec![0, 2], values, None)?;
        let error = match project_parquet_batch_to_provider_schema(
            "list-struct-missing-required-schema-match",
            file_schema,
            vec![addresses],
            provider_schema,
        ) {
            Ok(_) => return Err("missing non-nullable list struct child must fail".into()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("non-nullable provider field"), "{error}");
        assert!(
            error.contains("addresses.element.required_country"),
            "{error}"
        );
        assert!(
            error.contains("is missing from the Parquet file"),
            "{error}"
        );

        Ok(())
    }

    #[test]
    fn native_async_schema_match_reshapes_map_value_struct_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_value_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(provider_value_fields.into()),
                true,
            ),
            true,
        )]));
        let file_value_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(file_value_fields.clone().into()),
                true,
            ),
            true,
        )]));
        let values = struct_array(
            file_value_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let attributes = map_array(
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(
                    vec![
                        Field::new("stale_zip", DataType::Int32, true)
                            .with_metadata(field_id_metadata(10)),
                        Field::new("stale_city", DataType::Utf8, true)
                            .with_metadata(field_id_metadata(11)),
                    ]
                    .into(),
                ),
                true,
            ),
            vec![0, 2, 2, 3],
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
            ])) as ArrayRef,
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "map-value-struct-field-id-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected map key StringArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map value StructArray")?;
        let cities = values
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = values
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_valid(0));
        assert!(attributes.is_null(1));
        assert!(attributes.is_valid(2));
        assert_eq!(keys.value(0), "home");
        assert_eq!(keys.value(2), "other");
        assert_eq!(values.fields()[0].name(), "city");
        assert_eq!(values.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_null_fills_missing_nullable_map_value_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_value_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("country", DataType::Utf8, true),
        ];
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(provider_value_fields.into()),
                true,
            ),
            true,
        )]));
        let file_value_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(file_value_fields.clone().into()),
                true,
            ),
            true,
        )]));
        let values = struct_array(
            file_value_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001, 60601, 85001, 73301])) as ArrayRef],
        );
        let attributes = map_array(
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(vec![Field::new("zip", DataType::Int32, true)].into()),
                true,
            ),
            vec![0, 2, 2, 5],
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
                Some("billing"),
                Some("shipping"),
            ])) as ArrayRef,
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "map-value-struct-missing-nullable-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map value StructArray")?;
        let countries = values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected country StringArray")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 5]);
        assert!(attributes.is_null(1));
        assert_eq!(values.fields()[1].name(), "country");
        assert_eq!(countries.len(), 5);
        assert_eq!(countries.null_count(), 5);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_rejects_missing_non_nullable_map_value_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_value_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("required_country", DataType::Utf8, false),
        ];
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(provider_value_fields.into()),
                true,
            ),
            true,
        )]));
        let file_value_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(file_value_fields.clone().into()),
                true,
            ),
            true,
        )]));
        let values = struct_array(
            file_value_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001])) as ArrayRef],
        );
        let attributes = map_array(
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(vec![Field::new("zip", DataType::Int32, true)].into()),
                true,
            ),
            vec![0, 2],
            Arc::new(StringArray::from(vec![Some("home"), Some("work")])) as ArrayRef,
            values,
            None,
        )?;
        let error = match project_parquet_batch_to_provider_schema(
            "map-value-struct-missing-required-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        ) {
            Ok(_) => return Err("missing non-nullable map value struct child must fail".into()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("non-nullable provider field"), "{error}");
        assert!(
            error.contains("attributes.value.required_country"),
            "{error}"
        );
        assert!(
            error.contains("is missing from the Parquet file"),
            "{error}"
        );

        Ok(())
    }

    #[test]
    fn native_async_schema_match_rejects_missing_non_nullable_nested_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_profile_fields = vec![
            Field::new("age", DataType::Int32, true),
            Field::new("required_code", DataType::Utf8, false),
        ];
        let provider_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            provider_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![Field::new("age", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array(
            file_profile_fields,
            vec![Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef],
        );
        let error = match project_parquet_batch_to_provider_schema(
            "nested-missing-required-schema-match",
            file_schema,
            vec![profile],
            provider_schema,
        ) {
            Ok(_) => return Err("missing nested required child must fail".into()),
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("non-nullable provider field"), "{display}");
        assert!(display.contains("profile.required_code"), "{display}");
        assert!(
            display.contains("is missing from the Parquet file"),
            "{display}"
        );

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
