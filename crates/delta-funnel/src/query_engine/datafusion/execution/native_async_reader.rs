//! Native async Parquet reader boundary for provider file tasks.
//!
//! The native backend uses the same Delta table URI normalization and object-store
//! construction path as the official-kernel baseline, then hands resolved
//! `object_store::Path` values to `ParquetObjectReader`. That keeps local and
//! remote table URI semantics aligned while allowing parquet-rs to issue async
//! range reads through the selected object-store handle.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, new_null_array};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::default::storage::store_from_url_opts;
use futures_util::StreamExt;
use object_store::{ObjectStore, path::Path};
use parquet::arrow::RowNumber;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
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
        KernelColumnMetadataKey, KernelDataType, KernelMetadataColumnSpec, KernelMetadataValue,
        KernelPhysicalToLogicalTransform, KernelScanReadSchema, KernelSchemaRef, KernelStructField,
    },
};

use super::super::planning::file_task::DeltaScanFileTask;
use super::async_scheduler::{DeltaProviderAsyncFileReadFuture, DeltaProviderAsyncFileReader};
use super::file_reader::DeltaFileReadDeletionVectorStats;
use super::read_stats::DeltaProviderReadStats;
use super::scheduling::DeltaProviderAsyncFileReadPermit;
use crate::table_formats::{
    KernelDataFileReader, KernelDataFileReaderConfig, KernelDataFileTransformRequest,
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
}

/// Reusable native async file reader context for one provider scan.
#[allow(dead_code)]
pub(crate) struct DeltaNativeAsyncFileReader {
    source_name: String,
    table_uri: String,
    snapshot_version: u64,
    store: Arc<dyn ObjectStore>,
    data_file_reader: Arc<KernelDataFileReader>,
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
    deletion_vector_stats: DeltaFileReadDeletionVectorStats,
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
        let store = store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>()).context(
            DeltaScanFileReadSnafu {
                source_name: config.source_name.to_owned(),
                table_uri: config.table_uri.to_owned(),
                snapshot_version: config.snapshot_version,
                path: TABLE_ROOT_CONTEXT.to_owned(),
                phase: DeltaScanFileReadPhase::ObjectStoreEngineConstruction,
            },
        )?;
        let data_file_reader =
            Arc::new(KernelDataFileReader::try_new(KernelDataFileReaderConfig {
                source_name: config.source_name,
                table_uri: config.table_uri,
                snapshot_version: config.snapshot_version,
            })?);

        Ok(Self {
            source_name: config.source_name.to_owned(),
            table_uri: config.table_uri.to_owned(),
            snapshot_version: config.snapshot_version,
            store,
            data_file_reader,
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

    /// Opens one non-DV file task with parquet-rs async object-store reads.
    ///
    /// This first native implementation intentionally supports only file tasks
    /// with async object-store Parquet reads and optional physical-to-logical
    /// transforms. Deletion vectors and kernel physical predicates remain gated
    /// until later native reader slices reopen those paths.
    #[cfg(test)]
    pub(crate) async fn open_file_stream(
        &self,
        request: DeltaNativeAsyncFileReadRequest<'_>,
    ) -> Result<DeltaNativeAsyncFileReadStream, DeltaFunnelError> {
        self.open_file_stream_with_permit(request, None).await
    }

    /// Opens one non-DV file task while requesting hidden original row indexes.
    #[cfg(test)]
    pub(crate) async fn open_file_stream_with_original_row_index(
        &self,
        request: DeltaNativeAsyncFileReadRequest<'_>,
    ) -> Result<DeltaNativeAsyncFileReadStream, DeltaFunnelError> {
        self.open_file_stream_internal(request, None, true).await
    }

    /// Opens one non-DV file stream and holds the scheduler permit for its lifetime.
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
            deletion_vector_stats: DeltaFileReadDeletionVectorStats::default(),
            _permit: permit,
        })
    }
}

impl DeltaNativeAsyncFileReadStream {
    /// File-local deletion-vector metrics observed during this read.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn deletion_vector_stats(&self) -> DeltaFileReadDeletionVectorStats {
        self.deletion_vector_stats
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
        let reason = if task.deletion_vector.is_present() {
            Some("native async reader does not support deletion-vector file tasks yet".to_owned())
        } else if read_schema.has_physical_predicate() {
            Some("native async reader does not support kernel physical predicates yet".to_owned())
        } else {
            unsupported_native_async_physical_schema_reason(read_schema.physical_schema())
        };

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
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use datafusion::arrow::array::{Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use object_store::ObjectStoreExt;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::{
        DeltaNativeAsyncFileReadRequest, DeltaNativeAsyncFileReader,
        DeltaNativeAsyncFileReaderConfig, unsupported_native_async_physical_schema_reason,
        validate_native_async_reader_config,
    };
    use crate::{
        DeltaFunnelError, DeltaSourceConfig,
        error::DeltaScanFileReadPhase,
        load_delta_source,
        query_engine::datafusion::{
            execution::file_reader::DeltaFileReadDeletionVectorStats,
            planning::file_task::DeltaScanFileTask,
        },
        table_formats::{
            KernelColumnMetadataKey, KernelDataType, KernelMetadataColumnSpec, KernelMetadataValue,
            KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata,
            KernelScanReadSchema, KernelSchemaRef, KernelStructField, KernelStructType,
            RealParquetDeltaTable, build_projected_delta_scan,
            build_projected_predicated_stats_delta_scan,
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

    fn reader(table_uri: &str) -> Result<DeltaNativeAsyncFileReader, DeltaFunnelError> {
        DeltaNativeAsyncFileReader::try_new(DeltaNativeAsyncFileReaderConfig {
            source_name: "orders",
            table_uri,
            snapshot_version: 42,
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
        validate_native_async_reader_config(DeltaNativeAsyncFileReaderConfig {
            source_name: "orders",
            table_uri,
            snapshot_version: 42,
        })?;
        let reader = reader(table_uri)?;
        let object = reader.parquet_object_for_task(&task(table_uri, "part-00000.parquet"))?;

        assert_eq!(object.path.as_ref(), "table/root/part-00000.parquet");

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
        let error = validate_native_async_reader_config(DeltaNativeAsyncFileReaderConfig {
            source_name: "orders",
            table_uri: "ftp://example.com/table/",
            snapshot_version: 42,
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
        })?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, None)?;
        let read_schema = scan.read_schema();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri())?
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
        })?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, None)?;
        let read_schema = scan.read_schema();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri())?
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
        })?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, None)?;
        let read_schema = scan.read_schema();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri())?
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
    async fn native_async_reader_reads_projected_real_non_dv_parquet_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("native-async-projected-file-read")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let projected_columns = vec!["customer_name".to_owned()];
        let scan = build_projected_delta_scan(&source, Some(&projected_columns))?;
        let read_schema = scan.read_schema();
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri())?
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
