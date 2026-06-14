//! Native async Parquet reader boundary for provider file tasks.
//!
//! The native backend uses the same Delta table URI normalization and object-store
//! construction path as the official-kernel baseline, then hands resolved
//! `object_store::Path` values to `ParquetObjectReader`. That keeps local and
//! remote table URI semantics aligned while allowing parquet-rs to issue async
//! range reads through the selected object-store handle.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::default::storage::store_from_url_opts;
use futures_util::StreamExt;
use object_store::{ObjectStore, path::Path};
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use snafu::ResultExt;

use crate::{
    DeltaFunnelError,
    error::{DeltaScanFileReadPhase, DeltaScanFileReadSnafu},
    table_formats::{KernelPhysicalToLogicalTransform, KernelScanReadSchema},
};

use super::super::planning::file_task::DeltaScanFileTask;
use super::async_scheduler::{DeltaProviderAsyncFileReadFuture, DeltaProviderAsyncFileReader};
use super::file_reader::{DeltaFileReadDeletionVectorStats, DeltaFileReadResult};
use super::read_stats::DeltaProviderReadStats;
use super::scheduling::DeltaProviderAsyncFileReadPermit;

const TABLE_ROOT_CONTEXT: &str = "<table-root>";

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

        Ok(Self {
            source_name: config.source_name.to_owned(),
            table_uri: config.table_uri.to_owned(),
            snapshot_version: config.snapshot_version,
            store,
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

    /// Reads one non-DV file task with parquet-rs async object-store reads.
    ///
    /// This first native implementation intentionally supports only file tasks
    /// whose physical Parquet batches are already provider-visible batches:
    /// no deletion vectors, no physical-to-logical transform, and no kernel
    /// physical predicate. Later slices reopen those equivalence boundaries.
    #[allow(dead_code)]
    pub(crate) async fn read_file(
        &self,
        request: DeltaNativeAsyncFileReadRequest<'_>,
    ) -> Result<DeltaFileReadResult, DeltaFunnelError> {
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
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(delta_kernel::Error::from)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.task.path.clone(),
                phase: DeltaScanFileReadPhase::ParquetReadSetup,
            })?;
        let projected_fields = arrow_schema
            .fields
            .iter()
            .map(|field| field.name().as_str());
        let projection = ProjectionMask::columns(builder.parquet_schema(), projected_fields);
        let mut stream = builder
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
        let mut batches = Vec::new();

        while let Some(batch) = stream.next().await {
            let batch =
                batch
                    .map_err(delta_kernel::Error::from)
                    .context(DeltaScanFileReadSnafu {
                        source_name: self.source_name.clone(),
                        table_uri: self.table_uri.clone(),
                        snapshot_version: self.snapshot_version,
                        path: request.task.path.clone(),
                        phase: DeltaScanFileReadPhase::ParquetBatchRead,
                    })?;
            batches.push(self.project_batch_to_schema(request.task, batch, &arrow_schema)?);
        }

        Ok(DeltaFileReadResult {
            batches,
            deletion_vector_stats: DeltaFileReadDeletionVectorStats::default(),
        })
    }

    fn project_batch_to_schema(
        &self,
        task: &DeltaScanFileTask,
        batch: RecordBatch,
        schema: &SchemaRef,
    ) -> Result<RecordBatch, DeltaFunnelError> {
        let indices = schema
            .fields
            .iter()
            .map(|field| {
                batch
                    .schema()
                    .index_of(field.name())
                    .map_err(delta_kernel::Error::from)
            })
            .collect::<Result<Vec<_>, _>>()
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: task.path.clone(),
                phase: DeltaScanFileReadPhase::ArrowConversion,
            })?;

        batch
            .project(&indices)
            .map_err(delta_kernel::Error::from)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: task.path.clone(),
                phase: DeltaScanFileReadPhase::ArrowConversion,
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
            Some("native async reader does not support deletion-vector file tasks yet")
        } else if !matches!(
            task.transform,
            KernelPhysicalToLogicalTransform::NotRequired
        ) {
            Some("native async reader does not support physical-to-logical transforms yet")
        } else if read_schema.has_physical_predicate() {
            Some("native async reader does not support kernel physical predicates yet")
        } else {
            None
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

impl DeltaProviderAsyncFileReader<DeltaScanFileTask, DeltaFileReadResult>
    for DeltaNativeAsyncPartitionFileReader
{
    fn read_file(
        &self,
        task: DeltaScanFileTask,
        permit: DeltaProviderAsyncFileReadPermit,
    ) -> DeltaProviderAsyncFileReadFuture<DeltaFileReadResult> {
        let reader = Arc::clone(&self.reader);
        let read_schema = self.read_schema.clone();
        self.read_stats.record_file_started();

        Box::pin(async move {
            let _permit = permit;
            reader
                .read_file(DeltaNativeAsyncFileReadRequest {
                    task: &task,
                    read_schema: &read_schema,
                })
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use datafusion::arrow::array::{Array, Int32Array, StringArray};

    use super::{
        DeltaNativeAsyncFileReadRequest, DeltaNativeAsyncFileReader,
        DeltaNativeAsyncFileReaderConfig, validate_native_async_reader_config,
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
            KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata,
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

        let result = reader
            .read_file(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await?;

        assert_eq!(result.batches.len(), 1);
        assert_eq!(
            result.deletion_vector_stats,
            DeltaFileReadDeletionVectorStats::default()
        );

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

        let result = reader
            .read_file(DeltaNativeAsyncFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            })
            .await?;
        let batch = result.batches.first().ok_or("expected one record batch")?;

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "customer_name");

        Ok(())
    }
}
