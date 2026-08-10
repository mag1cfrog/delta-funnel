//! NativeAsync Parquet data-file reader.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use snafu::ResultExt;

use crate::{
    DeltaReadMetrics, DeltaReaderError, DeltaReaderExecutionOptions, error::DataFileReadSnafu,
    kernel::DeltaKernelEngineContext, metered_object_store::MeteredParquetObjectStore,
    planning::DeltaScanFileTask,
};

pub(crate) struct NativeAsyncFileReader {
    engine_context: Arc<DeltaKernelEngineContext>,
    store: Arc<dyn ObjectStore>,
    execution_options: DeltaReaderExecutionOptions,
}

#[derive(Debug)]
pub(crate) struct NativeAsyncParquetObject {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) path: Path,
    pub(crate) file_size: u64,
}

impl NativeAsyncFileReader {
    pub(crate) fn new(
        engine_context: Arc<DeltaKernelEngineContext>,
        execution_options: DeltaReaderExecutionOptions,
        metrics: DeltaReadMetrics,
    ) -> Self {
        let store = Arc::new(MeteredParquetObjectStore::new(
            engine_context.object_store(),
            metrics,
        ));
        Self {
            engine_context,
            store,
            execution_options,
        }
    }

    pub(crate) async fn parquet_object_for_task(
        &self,
        task: &DeltaScanFileTask,
    ) -> Result<NativeAsyncParquetObject, DeltaReaderError> {
        let object = self.resolve_parquet_object(task)?;
        self.buffer_small_parquet_object(object).await
    }

    fn resolve_parquet_object(
        &self,
        task: &DeltaScanFileTask,
    ) -> Result<NativeAsyncParquetObject, DeltaReaderError> {
        let location = self
            .engine_context
            .table_url()
            .join(&task.path)
            .boxed()
            .context(DataFileReadSnafu {
                reason: "data_file_path_resolution_failed",
            })?;
        let path = Path::from_url_path(location.path())
            .boxed()
            .context(DataFileReadSnafu {
                reason: "data_file_path_resolution_failed",
            })?;
        let file_size = task.estimated_bytes.ok_or_else(|| {
            data_file_error(
                "data_file_size_missing",
                delta_kernel::Error::generic("file size is required for NativeAsync reads"),
            )
        })?;

        Ok(NativeAsyncParquetObject {
            store: Arc::clone(&self.store),
            path,
            file_size,
        })
    }

    async fn buffer_small_parquet_object(
        &self,
        mut object: NativeAsyncParquetObject,
    ) -> Result<NativeAsyncParquetObject, DeltaReaderError> {
        let should_buffer = self
            .execution_options
            .parquet_full_file_read_threshold()
            .and_then(|threshold| u64::try_from(threshold).ok())
            .is_some_and(|threshold| object.file_size <= threshold);
        if !should_buffer {
            return Ok(object);
        }

        let bytes = object
            .store
            .get(&object.path)
            .await
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_full_file_read_failed",
            })?
            .bytes()
            .await
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_full_file_read_failed",
            })?;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&object.path, bytes.into())
            .await
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_file_buffer_initialization_failed",
            })?;
        object.store = store;
        Ok(object)
    }
}

fn data_file_error(
    reason: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> DeltaReaderError {
    Err::<(), _>(source)
        .boxed()
        .context(DataFileReadSnafu { reason })
        .expect_err("constructed data-file error")
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs,
        path::{Path as FsPath, PathBuf},
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use delta_kernel::scan::state::{DvInfo, ScanFile};
    use object_store::ObjectStoreExt;

    use super::{NativeAsyncFileReader, data_file_error};
    use crate::{
        DeltaReadMetrics, DeltaReaderBackend, DeltaReaderError, DeltaReaderExecutionOptions,
        DeltaStorageOptions,
        kernel::{DeltaKernelEngineContext, KernelScanFileMetadata},
        metrics::DeltaReadMetricsConfig,
        planning::DeltaScanFileTask,
    };

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let path = std::env::temp_dir().join(format!(
                "delta-arrow-reader-{name}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&path)?;
            Ok(Self(path))
        }

        fn path(&self) -> &FsPath {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn metrics() -> DeltaReadMetrics {
        DeltaReadMetrics::new(DeltaReadMetricsConfig {
            snapshot_version: 1,
            reader_backend: DeltaReaderBackend::NativeAsync,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 1,
            files_planned: 1,
            files_filtered_during_planning: Some(0),
            estimated_rows: Some(1),
            estimated_bytes: Some(1),
        })
    }

    fn reader(
        root: &TestDir,
        options: DeltaReaderExecutionOptions,
        metrics: DeltaReadMetrics,
    ) -> Result<NativeAsyncFileReader, Box<dyn std::error::Error>> {
        let table_url = url::Url::from_directory_path(root.path())
            .map_err(|()| "temporary table path cannot become a file URL")?;
        let engine_context = Arc::new(DeltaKernelEngineContext::build(
            table_url,
            &DeltaStorageOptions::default(),
        )?);
        Ok(NativeAsyncFileReader::new(engine_context, options, metrics))
    }

    fn task(path: &str, file_size: Option<u64>) -> Result<DeltaScanFileTask, DeltaReaderError> {
        let size = file_size
            .map(i64::try_from)
            .transpose()
            .map_err(|error| data_file_error("test_file_size_overflow", error))?
            .unwrap_or(1);
        let mut task =
            DeltaScanFileTask::try_from_kernel(KernelScanFileMetadata::from_scan_file(ScanFile {
                path: path.to_owned(),
                size,
                modification_time: 0,
                stats: None,
                dv_info: DvInfo::default(),
                transform: None,
                partition_values: HashMap::new(),
            }))?;
        task.estimated_bytes = file_size;
        Ok(task)
    }

    #[tokio::test]
    async fn resolves_table_relative_paths_and_requires_file_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("native-object-resolution")?;
        fs::write(root.path().join("part.parquet"), b"data")?;
        let metrics = metrics();
        let reader = reader(&root, DeltaReaderExecutionOptions::new(), metrics.clone())?;

        let object = reader
            .parquet_object_for_task(&task("part.parquet", Some(4))?)
            .await?;
        assert!(object.path.as_ref().ends_with("part.parquet"));
        assert_eq!(object.file_size, 4);
        assert_eq!(
            object.store.get_range(&object.path, 1..3).await?.as_ref(),
            b"at"
        );
        assert_eq!(
            metrics.snapshot().parquet_data_file_range_get_operations,
            Some(1)
        );

        let error = reader
            .parquet_object_for_task(&task("secret-file.parquet", None)?)
            .await
            .expect_err("missing size must fail");
        assert_eq!(error.as_str(), "data_file_read");
        assert!(!error.to_string().contains("secret-file"));
        Ok(())
    }

    #[tokio::test]
    async fn threshold_buffers_only_eligible_files_with_one_metered_full_get()
    -> Result<(), Box<dyn std::error::Error>> {
        let bytes = b"0123456789abcdef";

        for (name, threshold, expect_buffered) in [
            ("disabled", None, false),
            ("below", Some(bytes.len() - 1), false),
            ("exact", Some(bytes.len()), true),
            ("above", Some(bytes.len() + 1), true),
        ] {
            let root = TestDir::new(name)?;
            fs::write(root.path().join("part.parquet"), bytes)?;
            let metrics = metrics();
            let options = DeltaReaderExecutionOptions::new()
                .with_parquet_full_file_read_threshold(threshold)?;
            let reader = reader(&root, options, metrics.clone())?;
            let object = reader
                .parquet_object_for_task(&task("part.parquet", Some(u64::try_from(bytes.len())?))?)
                .await?;
            let snapshot = metrics.snapshot();
            assert_eq!(
                snapshot.parquet_data_file_full_get_operations,
                Some(u64::from(expect_buffered)),
                "{name}"
            );
            assert_eq!(
                snapshot.parquet_data_file_bytes_received,
                Some(if expect_buffered {
                    u64::try_from(bytes.len())?
                } else {
                    0
                }),
                "{name}"
            );

            assert_eq!(
                object.store.get_range(&object.path, 2..7).await?.as_ref(),
                b"23456",
                "{name}"
            );
            assert_eq!(
                metrics.snapshot().parquet_data_file_range_get_operations,
                Some(u64::from(!expect_buffered)),
                "{name}"
            );
        }
        Ok(())
    }
}
