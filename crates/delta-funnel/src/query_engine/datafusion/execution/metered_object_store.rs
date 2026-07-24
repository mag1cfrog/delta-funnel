//! Object-store metering for NativeAsync Parquet data-file reads.
//!
//! The wrapper keeps the underlying store's behavior while counting GET calls
//! after normal range coalescing and bytes only as callers consume payloads.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use futures_util::{StreamExt, stream, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
    path::Path,
};

use super::read_stats::DeltaProviderReadStats;

use crate::query_engine::datafusion::profiled_object_store::await_object_store_transport;

/// Measures Parquet data-file GET operations without exposing request details.
pub(crate) struct MeteredParquetObjectStore {
    inner: Arc<dyn ObjectStore>,
    read_stats: Arc<DeltaProviderReadStats>,
}

impl MeteredParquetObjectStore {
    /// Wraps one data-file store with counters from the containing scan.
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        read_stats: Arc<DeltaProviderReadStats>,
    ) -> Self {
        Self { inner, read_stats }
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
                self.read_stats
                    .record_parquet_data_file_range_get_operation();
            } else {
                self.read_stats
                    .record_parquet_data_file_full_get_operation();
            }
        }

        let request = self.inner.get_opts(location, options);
        let result = await_object_store_transport(request).await?;
        if should_meter_payload {
            Ok(meter_get_result(result, Arc::clone(&self.read_stats)))
        } else {
            Ok(result)
        }
    }

    // Keep ObjectStore's default get_ranges implementation. It coalesces
    // logical ranges before routing each resulting request through get_opts.

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

fn meter_get_result(result: GetResult, read_stats: Arc<DeltaProviderReadStats>) -> GetResult {
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
                        read_stats.record_parquet_data_file_bytes_received(bytes.len());
                    }
                    result
                })
                .boxed();
            GetResultPayload::Stream(payload)
        }
        #[cfg(not(target_arch = "wasm32"))]
        GetResultPayload::File(file, path) => {
            // GetResult::bytes preserves the local store's one lazy range read.
            // Wrapping that future as one stream item avoids 8 KiB re-chunking.
            let local_result = GetResult {
                payload: GetResultPayload::File(file, path),
                meta: meta.clone(),
                range: range.clone(),
                attributes: attributes.clone(),
            };
            let payload = stream::once(async move {
                let bytes = local_result.bytes().await?;
                read_stats.record_parquet_data_file_bytes_received(bytes.len());
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
        fs::File,
        io,
        ops::Range,
        path::PathBuf,
        sync::{Arc, Mutex, atomic::AtomicU64, atomic::Ordering},
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
    use crate::query_engine::datafusion::execution::{
        read_stats::{DeltaProviderReadStats, DeltaProviderReadStatsConfig},
        scheduling::DeltaProviderReaderBackend,
    };

    fn native_read_stats() -> Arc<DeltaProviderReadStats> {
        Arc::new(DeltaProviderReadStats::new(DeltaProviderReadStatsConfig {
            source_name: "orders".to_owned(),
            snapshot_version: 1,
            reader_backend: DeltaProviderReaderBackend::NativeAsync,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 1,
            files_planned: 1,
            files_filtered_during_planning: Some(0),
            estimated_rows: Some(1),
            estimated_bytes: Some(1),
        }))
    }

    async fn memory_store(
        read_stats: Arc<DeltaProviderReadStats>,
    ) -> Result<MeteredParquetObjectStore> {
        let inner = Arc::new(InMemory::new());
        inner
            .put(
                &Path::from("data.parquet"),
                PutPayload::from_static(b"0123456789abcdef"),
            )
            .await?;
        Ok(MeteredParquetObjectStore::new(inner, read_stats))
    }

    #[tokio::test]
    async fn bounded_get_counts_only_one_range_operation() -> Result<()> {
        let read_stats = native_read_stats();
        let store = memory_store(Arc::clone(&read_stats)).await?;

        let bytes = store.get_range(&Path::from("data.parquet"), 2..7).await?;

        assert_eq!(bytes.as_ref(), b"23456");
        let snapshot = read_stats.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(5));
        Ok(())
    }

    #[tokio::test]
    async fn unbounded_get_counts_only_one_full_operation() -> Result<()> {
        let read_stats = native_read_stats();
        let store = memory_store(Arc::clone(&read_stats)).await?;

        let bytes = store
            .get(&Path::from("data.parquet"))
            .await?
            .bytes()
            .await?;

        assert_eq!(bytes.as_ref(), b"0123456789abcdef");
        let snapshot = read_stats.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(16));
        Ok(())
    }

    #[tokio::test]
    async fn head_get_counts_neither_operations_nor_payload_bytes() -> Result<()> {
        let read_stats = native_read_stats();
        let store = memory_store(Arc::clone(&read_stats)).await?;
        let options = GetOptions::new().with_range(Some(1_u64..4)).with_head(true);

        let _result = store.get_opts(&Path::from("data.parquet"), options).await?;

        let snapshot = read_stats.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn failed_get_still_counts_the_attempted_operation() -> Result<()> {
        let read_stats = native_read_stats();
        let store =
            MeteredParquetObjectStore::new(Arc::new(InMemory::new()), Arc::clone(&read_stats));
        let options = GetOptions::new().with_range(Some(0_u64..4));

        let result = store
            .get_opts(&Path::from("missing.parquet"), options)
            .await;

        assert!(result.is_err());
        let snapshot = read_stats.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn get_ranges_counts_requests_after_default_coalescing() -> Result<()> {
        let read_stats = native_read_stats();
        let store = memory_store(Arc::clone(&read_stats)).await?;

        let bytes = store
            .get_ranges(&Path::from("data.parquet"), &[0..4, 8..12])
            .await?;

        assert_eq!(bytes[0].as_ref(), b"0123");
        assert_eq!(bytes[1].as_ref(), b"89ab");
        let snapshot = read_stats.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(12));
        Ok(())
    }

    #[tokio::test]
    async fn successful_stream_counts_each_delivered_chunk() -> Result<()> {
        let first = PutPayload::from_static(b"abc")
            .into_iter()
            .next()
            .ok_or_else(|| Error::Generic {
                store: "test",
                source: io::Error::other("missing first test chunk").into(),
            })?;
        let second = PutPayload::from_static(b"defgh")
            .into_iter()
            .next()
            .ok_or_else(|| Error::Generic {
                store: "test",
                source: io::Error::other("missing second test chunk").into(),
            })?;
        let result = test_get_result(
            GetResultPayload::Stream(stream::iter(vec![Ok(first), Ok(second)]).boxed()),
            0..8,
        );
        let read_stats = native_read_stats();
        let store = scripted_store(result, Arc::clone(&read_stats));

        let result = store.get(&Path::from("data.parquet")).await?;
        assert_eq!(result.meta.location, Path::from("data.parquet"));
        assert_eq!(result.meta.e_tag.as_deref(), Some("opaque-etag"));
        assert_eq!(result.range, 0..8);
        assert_eq!(result.attributes, Attributes::new());
        let bytes = result.bytes().await?;

        assert_eq!(bytes.as_ref(), b"abcdefgh");
        assert_eq!(
            read_stats.snapshot().parquet_data_file_bytes_received,
            Some(8)
        );
        Ok(())
    }

    #[tokio::test]
    async fn dropping_stream_counts_only_chunks_already_delivered() -> Result<()> {
        let chunks = PutPayload::from_static(b"abc")
            .into_iter()
            .chain(PutPayload::from_static(b"defgh").into_iter())
            .map(Ok)
            .collect::<Vec<_>>();
        let result = test_get_result(GetResultPayload::Stream(stream::iter(chunks).boxed()), 0..8);
        let read_stats = native_read_stats();
        let store = scripted_store(result, Arc::clone(&read_stats));
        let mut payload = store.get(&Path::from("data.parquet")).await?.into_stream();

        let first = payload
            .next()
            .await
            .transpose()?
            .ok_or_else(|| Error::Generic {
                store: "test",
                source: io::Error::other("missing first delivered chunk").into(),
            })?;
        drop(payload);

        assert_eq!(first.as_ref(), b"abc");
        assert_eq!(
            read_stats.snapshot().parquet_data_file_bytes_received,
            Some(3)
        );
        Ok(())
    }

    #[tokio::test]
    async fn stream_error_preserves_prior_bytes_without_counting_the_error() -> Result<()> {
        let first = PutPayload::from_static(b"abc")
            .into_iter()
            .next()
            .ok_or_else(|| Error::Generic {
                store: "test",
                source: io::Error::other("missing successful test chunk").into(),
            })?;
        let error = Error::Generic {
            store: "test",
            source: io::Error::other("payload failure").into(),
        };
        let result = test_get_result(
            GetResultPayload::Stream(stream::iter(vec![Ok(first), Err(error)]).boxed()),
            0..8,
        );
        let read_stats = native_read_stats();
        let store = scripted_store(result, Arc::clone(&read_stats));
        let mut payload = store.get(&Path::from("data.parquet")).await?.into_stream();

        assert!(payload.next().await.transpose()?.is_some());
        assert!(payload.next().await.transpose().is_err());
        assert_eq!(
            read_stats.snapshot().parquet_data_file_bytes_received,
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

        let dropped_stats = native_read_stats();
        let dropped_store =
            scripted_store(file.get_result(range.clone())?, Arc::clone(&dropped_stats));
        let result = dropped_store
            .get_opts(
                &Path::from("data.parquet"),
                GetOptions::new().with_range(Some(range.clone())),
            )
            .await?;
        drop(result);
        assert_eq!(
            dropped_stats.snapshot().parquet_data_file_bytes_received,
            Some(0)
        );

        let delivered_stats = native_read_stats();
        let delivered_store = scripted_store(
            file.get_result(range.clone())?,
            Arc::clone(&delivered_stats),
        );
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
            .ok_or_else(|| Error::Generic {
                store: "test",
                source: io::Error::other("missing local file chunk").into(),
            })?;

        assert_eq!(
            bytes.len(),
            usize::try_from(range.end - range.start).unwrap_or(usize::MAX)
        );
        assert!(payload.next().await.is_none());
        assert_eq!(
            delivered_stats.snapshot().parquet_data_file_bytes_received,
            Some(range.end - range.start)
        );
        Ok(())
    }

    #[tokio::test]
    async fn delegated_non_get_operations_do_not_change_read_counters() -> Result<()> {
        let read_stats = native_read_stats();
        let store =
            MeteredParquetObjectStore::new(Arc::new(InMemory::new()), Arc::clone(&read_stats));
        let first = Path::from("first.parquet");
        let second = Path::from("second.parquet");
        let third = Path::from("third.parquet");

        store.put(&first, PutPayload::from_static(b"data")).await?;
        assert!(store.list(None).next().await.transpose()?.is_some());
        store.copy(&first, &second).await?;
        store.rename(&second, &third).await?;
        store.delete(&first).await?;
        store.delete(&third).await?;

        let snapshot = read_stats.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(0));
        assert_eq!(snapshot.parquet_data_file_opened_bytes, Some(0));
        assert_eq!(format!("{store:?}"), "MeteredParquetObjectStore");
        Ok(())
    }

    #[tokio::test]
    async fn diagnostics_do_not_expose_get_request_details() -> Result<()> {
        let result = test_get_result(GetResultPayload::Stream(stream::empty().boxed()), 0..0);
        let read_stats = native_read_stats();
        let store = scripted_store(result, Arc::clone(&read_stats));
        let location = Path::from("private/user-password-secret-token.parquet");
        let mut options = GetOptions::new().with_range(Some(987_654_321_u64..987_654_999_u64));
        options.if_match = Some("secret-conditional-header".to_owned());
        options.version = Some("secret-object-version".to_owned());

        let _result = store.get_opts(&location, options).await?;
        let diagnostics = format!("{store:?} {store} {:?}", read_stats.snapshot());

        for request_detail in [
            "user-password-secret-token",
            "secret-conditional-header",
            "secret-object-version",
            "987654321",
            "987654999",
        ] {
            assert!(!diagnostics.contains(request_detail));
        }
        Ok(())
    }

    fn scripted_store(
        result: GetResult,
        read_stats: Arc<DeltaProviderReadStats>,
    ) -> MeteredParquetObjectStore {
        MeteredParquetObjectStore::new(Arc::new(ScriptedGetStore::new(result)), read_stats)
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

    impl std::fmt::Debug for ScriptedGetStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("ScriptedGetStore")
        }
    }

    impl std::fmt::Display for ScriptedGetStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
                .map_err(|_| Error::Generic {
                    store: "test",
                    source: io::Error::other("scripted result lock poisoned").into(),
                })?
                .take()
                .ok_or_else(|| Error::Generic {
                    store: "test",
                    source: io::Error::other("scripted result already consumed").into(),
                })
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
                "delta-funnel-metered-object-store-{}-{id}",
                std::process::id()
            ));
            std::fs::write(&path, contents)?;
            Ok(Self { path })
        }

        fn get_result(&self, range: Range<u64>) -> io::Result<GetResult> {
            let file = File::open(&self.path)?;
            Ok(test_get_result(
                GetResultPayload::File(file, self.path.clone()),
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
