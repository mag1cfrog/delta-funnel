//! Object-store metering for NativeAsync Parquet data-file reads.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use futures_util::{StreamExt, stream, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
    path::Path,
};

use crate::DeltaReadMetrics;

pub(crate) struct MeteredParquetObjectStore {
    inner: Arc<dyn ObjectStore>,
    metrics: DeltaReadMetrics,
}

impl MeteredParquetObjectStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, metrics: DeltaReadMetrics) -> Self {
        Self { inner, metrics }
    }
}

impl fmt::Debug for MeteredParquetObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MeteredParquetObjectStore")
    }
}

impl fmt::Display for MeteredParquetObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MeteredParquetObjectStore")
    }
}

#[async_trait]
impl ObjectStore for MeteredParquetObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let should_meter_payload = !options.head;
        if should_meter_payload {
            if options.range.is_some() {
                self.metrics.record_parquet_data_file_range_get_operation();
            } else {
                self.metrics.record_parquet_data_file_full_get_operation();
            }
        }

        let result = self.inner.get_opts(location, options).await?;
        if should_meter_payload {
            Ok(meter_get_result(result, self.metrics.clone()))
        } else {
            Ok(result)
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

fn meter_get_result(result: GetResult, metrics: DeltaReadMetrics) -> GetResult {
    let GetResult {
        payload,
        meta,
        range,
        attributes,
    } = result;

    let payload = match payload {
        GetResultPayload::Stream(payload) => {
            let payload = payload
                .map(move |result| {
                    if let Ok(bytes) = &result {
                        metrics.record_parquet_data_file_bytes_received(bytes.len());
                    }
                    result
                })
                .boxed();
            GetResultPayload::Stream(payload)
        }
        #[cfg(not(target_arch = "wasm32"))]
        GetResultPayload::File(file, path) => {
            let local_result = GetResult {
                payload: GetResultPayload::File(file, path),
                meta: meta.clone(),
                range: range.clone(),
                attributes: attributes.clone(),
            };
            let payload = stream::once(async move {
                let bytes = local_result.bytes().await?;
                metrics.record_parquet_data_file_bytes_received(bytes.len());
                Ok(bytes)
            })
            .boxed();
            GetResultPayload::Stream(payload)
        }
    };

    GetResult {
        payload,
        meta,
        range,
        attributes,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        fs::File,
        io,
        ops::Range,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use futures_util::{StreamExt, stream, stream::BoxStream};
    use object_store::{
        Attributes, CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult,
        MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
        PutPayload, PutResult, RenameOptions, Result, memory::InMemory, path::Path,
    };

    use super::MeteredParquetObjectStore;
    use crate::{DeltaReadMetrics, DeltaReaderBackend, metrics::DeltaReadMetricsConfig};

    fn native_metrics() -> DeltaReadMetrics {
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

    async fn memory_store(metrics: DeltaReadMetrics) -> Result<MeteredParquetObjectStore> {
        let inner = Arc::new(InMemory::new());
        inner
            .put(
                &Path::from("data.parquet"),
                PutPayload::from_static(b"0123456789abcdef"),
            )
            .await?;
        Ok(MeteredParquetObjectStore::new(inner, metrics))
    }

    #[tokio::test]
    async fn bounded_and_unbounded_gets_record_exact_operations_and_bytes() -> Result<()> {
        let range_metrics = native_metrics();
        let range_store = memory_store(range_metrics.clone()).await?;
        let bytes = range_store
            .get_range(&Path::from("data.parquet"), 2..7)
            .await?;
        assert_eq!(bytes.as_ref(), b"23456");
        let snapshot = range_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(5));

        let full_metrics = native_metrics();
        let full_store = memory_store(full_metrics.clone()).await?;
        let bytes = full_store
            .get(&Path::from("data.parquet"))
            .await?
            .bytes()
            .await?;
        assert_eq!(bytes.as_ref(), b"0123456789abcdef");
        let snapshot = full_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(16));
        Ok(())
    }

    #[tokio::test]
    async fn head_failure_and_coalescing_match_frozen_attempt_semantics() -> Result<()> {
        let head_metrics = native_metrics();
        let head_store = memory_store(head_metrics.clone()).await?;
        let options = GetOptions::new().with_range(Some(1_u64..4)).with_head(true);
        let _result = head_store
            .get_opts(&Path::from("data.parquet"), options)
            .await?;
        let snapshot = head_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(0));

        let failure_metrics = native_metrics();
        let failure_store =
            MeteredParquetObjectStore::new(Arc::new(InMemory::new()), failure_metrics.clone());
        let result = failure_store
            .get_opts(
                &Path::from("missing.parquet"),
                GetOptions::new().with_range(Some(0_u64..4)),
            )
            .await;
        assert!(result.is_err());
        let snapshot = failure_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(0));

        let coalesced_metrics = native_metrics();
        let coalesced_store = memory_store(coalesced_metrics.clone()).await?;
        let bytes = coalesced_store
            .get_ranges(&Path::from("data.parquet"), &[0..4, 8..12])
            .await?;
        assert_eq!(bytes[0].as_ref(), b"0123");
        assert_eq!(bytes[1].as_ref(), b"89ab");
        let snapshot = coalesced_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(12));
        Ok(())
    }

    #[tokio::test]
    async fn stream_delivery_records_only_successful_consumed_chunks() -> Result<()> {
        let first = PutPayload::from_static(b"abc")
            .into_iter()
            .next()
            .ok_or_else(missing_chunk)?;
        let second = PutPayload::from_static(b"defgh")
            .into_iter()
            .next()
            .ok_or_else(missing_chunk)?;
        let success_metrics = native_metrics();
        let success_store = scripted_store(
            test_get_result(
                GetResultPayload::Stream(stream::iter(vec![Ok(first), Ok(second)]).boxed()),
                0..8,
            ),
            success_metrics.clone(),
        );
        let result = success_store.get(&Path::from("data.parquet")).await?;
        assert_eq!(result.meta.location, Path::from("data.parquet"));
        assert_eq!(result.meta.e_tag.as_deref(), Some("opaque-etag"));
        assert_eq!(result.range, 0..8);
        assert_eq!(result.bytes().await?.as_ref(), b"abcdefgh");
        assert_eq!(
            success_metrics.snapshot().parquet_data_file_bytes_received,
            Some(8)
        );

        let dropped_metrics = native_metrics();
        let chunks = PutPayload::from_static(b"abc")
            .into_iter()
            .chain(PutPayload::from_static(b"defgh").into_iter())
            .map(Ok)
            .collect::<Vec<_>>();
        let dropped_store = scripted_store(
            test_get_result(GetResultPayload::Stream(stream::iter(chunks).boxed()), 0..8),
            dropped_metrics.clone(),
        );
        let mut payload = dropped_store
            .get(&Path::from("data.parquet"))
            .await?
            .into_stream();
        assert_eq!(
            payload
                .next()
                .await
                .transpose()?
                .ok_or_else(missing_chunk)?
                .as_ref(),
            b"abc"
        );
        drop(payload);
        assert_eq!(
            dropped_metrics.snapshot().parquet_data_file_bytes_received,
            Some(3)
        );

        let error_metrics = native_metrics();
        let error = Error::Generic {
            store: "test",
            source: io::Error::other("payload failure").into(),
        };
        let successful = PutPayload::from_static(b"abc")
            .into_iter()
            .next()
            .ok_or_else(missing_chunk)?;
        let error_store = scripted_store(
            test_get_result(
                GetResultPayload::Stream(stream::iter(vec![Ok(successful), Err(error)]).boxed()),
                0..8,
            ),
            error_metrics.clone(),
        );
        let mut payload = error_store
            .get(&Path::from("data.parquet"))
            .await?
            .into_stream();
        assert!(payload.next().await.transpose()?.is_some());
        assert!(payload.next().await.transpose().is_err());
        assert_eq!(
            error_metrics.snapshot().parquet_data_file_bytes_received,
            Some(3)
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn local_file_payload_stays_lazy_and_yields_one_large_chunk()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let file = TemporaryTestFile::new(&vec![7_u8; 20_000])?;
        let range = 100_u64..16_500;

        let dropped_metrics = native_metrics();
        let dropped_store =
            scripted_store(file.get_result(range.clone())?, dropped_metrics.clone());
        let result = dropped_store
            .get_opts(
                &Path::from("data.parquet"),
                GetOptions::new().with_range(Some(range.clone())),
            )
            .await?;
        drop(result);
        assert_eq!(
            dropped_metrics.snapshot().parquet_data_file_bytes_received,
            Some(0)
        );

        let delivered_metrics = native_metrics();
        let delivered_store =
            scripted_store(file.get_result(range.clone())?, delivered_metrics.clone());
        let mut payload = delivered_store
            .get_opts(
                &Path::from("data.parquet"),
                GetOptions::new().with_range(Some(range.clone())),
            )
            .await?
            .into_stream();
        let bytes = payload
            .next()
            .await
            .transpose()?
            .ok_or_else(missing_chunk)?;
        assert_eq!(bytes.len(), usize::try_from(range.end - range.start)?);
        assert!(payload.next().await.is_none());
        assert_eq!(
            delivered_metrics
                .snapshot()
                .parquet_data_file_bytes_received,
            Some(range.end - range.start)
        );
        Ok(())
    }

    #[tokio::test]
    async fn delegated_operations_and_diagnostics_do_not_leak_or_meter() -> Result<()> {
        let metrics = native_metrics();
        let store = MeteredParquetObjectStore::new(Arc::new(InMemory::new()), metrics.clone());
        let first = Path::from("first.parquet");
        let second = Path::from("second.parquet");
        let third = Path::from("third.parquet");

        store.put(&first, PutPayload::from_static(b"data")).await?;
        assert!(store.list(None).next().await.transpose()?.is_some());
        store.copy(&first, &second).await?;
        store.rename(&second, &third).await?;
        store.delete(&first).await?;
        store.delete(&third).await?;
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(0));
        assert_eq!(snapshot.parquet_data_file_opened_bytes, Some(0));
        assert_eq!(format!("{store:?}"), "MeteredParquetObjectStore");

        let redacted_metrics = native_metrics();
        let redacted_store = scripted_store(
            test_get_result(GetResultPayload::Stream(stream::empty().boxed()), 0..0),
            redacted_metrics.clone(),
        );
        let location = Path::from("private/user-password-secret-token.parquet");
        let mut options = GetOptions::new().with_range(Some(987_654_321_u64..987_654_999_u64));
        options.if_match = Some("secret-conditional-header".to_owned());
        options.version = Some("secret-object-version".to_owned());
        let _result = redacted_store.get_opts(&location, options).await?;
        let diagnostics = format!(
            "{redacted_store:?} {redacted_store} {:?}",
            redacted_metrics.snapshot()
        );
        for secret in [
            "user-password-secret-token",
            "secret-conditional-header",
            "secret-object-version",
            "987654321",
            "987654999",
        ] {
            assert!(!diagnostics.contains(secret));
        }
        Ok(())
    }

    fn missing_chunk() -> Error {
        Error::Generic {
            store: "test",
            source: io::Error::other("missing test chunk").into(),
        }
    }

    fn scripted_store(result: GetResult, metrics: DeltaReadMetrics) -> MeteredParquetObjectStore {
        MeteredParquetObjectStore::new(Arc::new(ScriptedGetStore::new(result)), metrics)
    }

    fn test_get_result(payload: GetResultPayload, range: Range<u64>) -> GetResult {
        GetResult {
            payload,
            meta: ObjectMeta {
                location: Path::from("data.parquet"),
                last_modified: DateTime::<Utc>::UNIX_EPOCH,
                size: range.end,
                e_tag: Some("opaque-etag".to_owned()),
                version: Some("opaque-version".to_owned()),
            },
            range,
            attributes: Attributes::new(),
        }
    }

    struct ScriptedGetStore {
        result: Mutex<Option<GetResult>>,
        delegate: InMemory,
    }

    impl ScriptedGetStore {
        fn new(result: GetResult) -> Self {
            Self {
                result: Mutex::new(Some(result)),
                delegate: InMemory::new(),
            }
        }
    }

    impl fmt::Debug for ScriptedGetStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("ScriptedGetStore")
        }
    }

    impl fmt::Display for ScriptedGetStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("ScriptedGetStore")
        }
    }

    #[async_trait]
    impl ObjectStore for ScriptedGetStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> Result<PutResult> {
            self.delegate.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.delegate.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
            self.result
                .lock()
                .map_err(|_| missing_chunk())?
                .take()
                .ok_or_else(missing_chunk)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.delegate.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.delegate.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, Result<ObjectMeta>> {
            self.delegate.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.delegate.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.delegate.copy_opts(from, to, options).await
        }

        async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
            self.delegate.rename_opts(from, to, options).await
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    struct TemporaryTestFile {
        path: PathBuf,
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl TemporaryTestFile {
        fn new(contents: &[u8]) -> io::Result<Self> {
            static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);
            let id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "delta-arrow-reader-metered-object-store-{}-{id}",
                std::process::id()
            ));
            std::fs::write(&path, contents)?;
            Ok(Self { path })
        }

        fn get_result(&self, range: Range<u64>) -> io::Result<GetResult> {
            Ok(test_get_result(
                GetResultPayload::File(File::open(&self.path)?, self.path.clone()),
                range,
            ))
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl Drop for TemporaryTestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
