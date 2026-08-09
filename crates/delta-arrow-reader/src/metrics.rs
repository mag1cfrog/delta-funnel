use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::DeltaReaderBackend;

/// Immutable point-in-time metrics for one Delta scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaReadMetricsSnapshot {
    /// Delta snapshot version selected for the scan.
    pub snapshot_version: u64,
    /// Data-file reader backend selected for the scan.
    pub reader_backend: DeltaReaderBackend,
    /// Whether planning exhausted the Delta scan metadata iterator.
    pub scan_metadata_exhausted: Option<bool>,
    /// Execution partitions planned for the scan.
    pub scan_partitions_planned: u64,
    /// Data files selected during planning.
    pub files_planned: u64,
    /// Add actions filtered during planning, when known.
    pub files_filtered_during_planning: Option<u64>,
    /// Estimated output rows, when every selected file reported row statistics.
    pub estimated_rows: Option<u64>,
    /// Estimated input bytes, when every selected file reported a size.
    pub estimated_bytes: Option<u64>,
    /// Scan partitions whose execution started.
    pub scan_partitions_started: u64,
    /// Scan partitions that completed normally.
    pub scan_partitions_completed: u64,
    /// Data-file reads that started.
    pub files_started: u64,
    /// Data-file reads that completed normally.
    pub files_completed: u64,
    /// Backend-logical record batches handed to the scheduler.
    pub batches_produced: u64,
    /// Backend-logical rows handed to the scheduler.
    pub rows_produced: u64,
    /// Deletion-vector payloads loaded.
    pub deletion_vector_payloads_loaded: u64,
    /// Deletion-vector masks applied.
    pub deletion_vectors_applied: u64,
    /// Rows removed by deletion-vector masks.
    pub deletion_vector_rows_deleted: u64,
    /// Deletion-vector read or masking failures.
    pub deletion_vector_failures: u64,
    /// Deletion-vector reads rejected by safety checks.
    pub deletion_vector_rejections: u64,
    /// NativeAsync Parquet ranged GET operations, or `None` for another backend.
    pub parquet_data_file_range_get_operations: Option<u64>,
    /// NativeAsync Parquet full GET operations, or `None` for another backend.
    pub parquet_data_file_full_get_operations: Option<u64>,
    /// NativeAsync Parquet payload bytes received, or `None` for another backend.
    pub parquet_data_file_bytes_received: Option<u64>,
    /// NativeAsync Parquet file bytes opened, or `None` for another backend.
    pub parquet_data_file_opened_bytes: Option<u64>,
}

/// Shared live metrics for one Delta scan.
#[derive(Clone)]
pub struct DeltaReadMetrics {
    inner: Arc<DeltaReadMetricsInner>,
}

struct DeltaReadMetricsInner {
    snapshot_version: u64,
    reader_backend: DeltaReaderBackend,
    scan_metadata_exhausted: Option<bool>,
    scan_partitions_planned: u64,
    files_planned: u64,
    files_filtered_during_planning: Option<u64>,
    estimated_rows: Option<u64>,
    estimated_bytes: Option<u64>,
    scan_partitions_started: AtomicU64,
    scan_partitions_completed: AtomicU64,
    files_started: AtomicU64,
    files_completed: AtomicU64,
    batches_produced: AtomicU64,
    rows_produced: AtomicU64,
    deletion_vector_payloads_loaded: AtomicU64,
    deletion_vectors_applied: AtomicU64,
    deletion_vector_rows_deleted: AtomicU64,
    deletion_vector_failures: AtomicU64,
    deletion_vector_rejections: AtomicU64,
    parquet_data_file_range_get_operations: AtomicU64,
    parquet_data_file_full_get_operations: AtomicU64,
    parquet_data_file_bytes_received: AtomicU64,
    parquet_data_file_opened_bytes: AtomicU64,
}

#[allow(dead_code)]
pub(crate) struct DeltaReadMetricsConfig {
    pub(crate) snapshot_version: u64,
    pub(crate) reader_backend: DeltaReaderBackend,
    pub(crate) scan_metadata_exhausted: Option<bool>,
    pub(crate) scan_partitions_planned: usize,
    pub(crate) files_planned: usize,
    pub(crate) files_filtered_during_planning: Option<u64>,
    pub(crate) estimated_rows: Option<u64>,
    pub(crate) estimated_bytes: Option<u64>,
}

impl DeltaReadMetrics {
    #[allow(dead_code)]
    pub(crate) fn new(config: DeltaReadMetricsConfig) -> Self {
        Self {
            inner: Arc::new(DeltaReadMetricsInner {
                snapshot_version: config.snapshot_version,
                reader_backend: config.reader_backend,
                scan_metadata_exhausted: config.scan_metadata_exhausted,
                scan_partitions_planned: usize_to_u64_saturating(config.scan_partitions_planned),
                files_planned: usize_to_u64_saturating(config.files_planned),
                files_filtered_during_planning: config.files_filtered_during_planning,
                estimated_rows: config.estimated_rows,
                estimated_bytes: config.estimated_bytes,
                scan_partitions_started: AtomicU64::new(0),
                scan_partitions_completed: AtomicU64::new(0),
                files_started: AtomicU64::new(0),
                files_completed: AtomicU64::new(0),
                batches_produced: AtomicU64::new(0),
                rows_produced: AtomicU64::new(0),
                deletion_vector_payloads_loaded: AtomicU64::new(0),
                deletion_vectors_applied: AtomicU64::new(0),
                deletion_vector_rows_deleted: AtomicU64::new(0),
                deletion_vector_failures: AtomicU64::new(0),
                deletion_vector_rejections: AtomicU64::new(0),
                parquet_data_file_range_get_operations: AtomicU64::new(0),
                parquet_data_file_full_get_operations: AtomicU64::new(0),
                parquet_data_file_bytes_received: AtomicU64::new(0),
                parquet_data_file_opened_bytes: AtomicU64::new(0),
            }),
        }
    }

    /// Returns an immutable point-in-time copy of all scan metrics.
    pub fn snapshot(&self) -> DeltaReadMetricsSnapshot {
        let inner = self.inner.as_ref();
        DeltaReadMetricsSnapshot {
            snapshot_version: inner.snapshot_version,
            reader_backend: inner.reader_backend,
            scan_metadata_exhausted: inner.scan_metadata_exhausted,
            scan_partitions_planned: inner.scan_partitions_planned,
            files_planned: inner.files_planned,
            files_filtered_during_planning: inner.files_filtered_during_planning,
            estimated_rows: inner.estimated_rows,
            estimated_bytes: inner.estimated_bytes,
            scan_partitions_started: load(&inner.scan_partitions_started),
            scan_partitions_completed: load(&inner.scan_partitions_completed),
            files_started: load(&inner.files_started),
            files_completed: load(&inner.files_completed),
            batches_produced: load(&inner.batches_produced),
            rows_produced: load(&inner.rows_produced),
            deletion_vector_payloads_loaded: load(&inner.deletion_vector_payloads_loaded),
            deletion_vectors_applied: load(&inner.deletion_vectors_applied),
            deletion_vector_rows_deleted: load(&inner.deletion_vector_rows_deleted),
            deletion_vector_failures: load(&inner.deletion_vector_failures),
            deletion_vector_rejections: load(&inner.deletion_vector_rejections),
            parquet_data_file_range_get_operations: self
                .parquet_metric(&inner.parquet_data_file_range_get_operations),
            parquet_data_file_full_get_operations: self
                .parquet_metric(&inner.parquet_data_file_full_get_operations),
            parquet_data_file_bytes_received: self
                .parquet_metric(&inner.parquet_data_file_bytes_received),
            parquet_data_file_opened_bytes: self
                .parquet_metric(&inner.parquet_data_file_opened_bytes),
        }
    }

    fn parquet_metric(&self, counter: &AtomicU64) -> Option<u64> {
        match self.inner.reader_backend {
            DeltaReaderBackend::NativeAsync => Some(load(counter)),
            DeltaReaderBackend::OfficialKernel => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn record_scan_partition_started(&self) {
        saturating_fetch_add(&self.inner.scan_partitions_started, 1);
    }

    #[allow(dead_code)]
    pub(crate) fn record_scan_partition_completed(&self) {
        saturating_fetch_add(&self.inner.scan_partitions_completed, 1);
    }

    #[allow(dead_code)]
    pub(crate) fn record_file_started(&self) {
        saturating_fetch_add(&self.inner.files_started, 1);
    }

    #[allow(dead_code)]
    pub(crate) fn record_file_completed(&self) {
        saturating_fetch_add(&self.inner.files_completed, 1);
    }

    #[allow(dead_code)]
    pub(crate) fn record_batch_produced(&self, rows: usize) {
        saturating_fetch_add(&self.inner.batches_produced, 1);
        saturating_fetch_add(&self.inner.rows_produced, usize_to_u64_saturating(rows));
    }

    #[allow(dead_code)]
    pub(crate) fn record_deletion_vector_payload_loaded(&self) {
        saturating_fetch_add(&self.inner.deletion_vector_payloads_loaded, 1);
    }

    #[allow(dead_code)]
    pub(crate) fn record_deletion_vector_applied(&self) {
        saturating_fetch_add(&self.inner.deletion_vectors_applied, 1);
    }

    #[allow(dead_code)]
    pub(crate) fn record_deletion_vector_rows_deleted(&self, rows: usize) {
        saturating_fetch_add(
            &self.inner.deletion_vector_rows_deleted,
            usize_to_u64_saturating(rows),
        );
    }

    #[allow(dead_code)]
    pub(crate) fn record_deletion_vector_failure(&self) {
        saturating_fetch_add(&self.inner.deletion_vector_failures, 1);
    }

    #[allow(dead_code)]
    pub(crate) fn record_deletion_vector_rejection(&self) {
        saturating_fetch_add(&self.inner.deletion_vector_rejections, 1);
    }

    #[cfg(feature = "native-async")]
    pub(crate) fn record_parquet_data_file_range_get_operation(&self) {
        saturating_fetch_add(&self.inner.parquet_data_file_range_get_operations, 1);
    }

    #[cfg(feature = "native-async")]
    pub(crate) fn record_parquet_data_file_full_get_operation(&self) {
        saturating_fetch_add(&self.inner.parquet_data_file_full_get_operations, 1);
    }

    #[cfg(feature = "native-async")]
    pub(crate) fn record_parquet_data_file_bytes_received(&self, bytes: usize) {
        saturating_fetch_add(
            &self.inner.parquet_data_file_bytes_received,
            usize_to_u64_saturating(bytes),
        );
    }
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

#[allow(dead_code)]
pub(crate) fn saturating_fetch_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::Ordering, thread};

    use super::{DeltaReadMetrics, DeltaReadMetricsConfig, saturating_fetch_add};
    use crate::DeltaReaderBackend;

    fn metrics(reader_backend: DeltaReaderBackend) -> DeltaReadMetrics {
        DeltaReadMetrics::new(DeltaReadMetricsConfig {
            snapshot_version: 7,
            reader_backend,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 3,
            files_planned: 5,
            files_filtered_during_planning: Some(2),
            estimated_rows: Some(99),
            estimated_bytes: Some(42),
        })
    }

    #[test]
    fn snapshot_has_context_zeroes_and_backend_availability() {
        let native = metrics(DeltaReaderBackend::NativeAsync).snapshot();
        assert_eq!(native.snapshot_version, 7);
        assert_eq!(native.reader_backend, DeltaReaderBackend::NativeAsync);
        assert_eq!(native.scan_metadata_exhausted, Some(true));
        assert_eq!(native.scan_partitions_planned, 3);
        assert_eq!(native.files_planned, 5);
        assert_eq!(native.files_filtered_during_planning, Some(2));
        assert_eq!(native.estimated_rows, Some(99));
        assert_eq!(native.estimated_bytes, Some(42));
        assert_eq!(native.scan_partitions_started, 0);
        assert_eq!(native.scan_partitions_completed, 0);
        assert_eq!(native.files_started, 0);
        assert_eq!(native.files_completed, 0);
        assert_eq!(native.batches_produced, 0);
        assert_eq!(native.rows_produced, 0);
        assert_eq!(native.deletion_vector_payloads_loaded, 0);
        assert_eq!(native.deletion_vectors_applied, 0);
        assert_eq!(native.deletion_vector_rows_deleted, 0);
        assert_eq!(native.deletion_vector_failures, 0);
        assert_eq!(native.deletion_vector_rejections, 0);
        assert_eq!(native.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(native.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(native.parquet_data_file_bytes_received, Some(0));
        assert_eq!(native.parquet_data_file_opened_bytes, Some(0));

        let official = metrics(DeltaReaderBackend::OfficialKernel).snapshot();
        assert_eq!(official.parquet_data_file_range_get_operations, None);
        assert_eq!(official.parquet_data_file_full_get_operations, None);
        assert_eq!(official.parquet_data_file_bytes_received, None);
        assert_eq!(official.parquet_data_file_opened_bytes, None);
    }

    #[test]
    fn snapshot_maps_live_counters() {
        let metrics = metrics(DeltaReaderBackend::NativeAsync);
        let counters = [
            &metrics.inner.scan_partitions_started,
            &metrics.inner.scan_partitions_completed,
            &metrics.inner.files_started,
            &metrics.inner.files_completed,
            &metrics.inner.batches_produced,
            &metrics.inner.rows_produced,
            &metrics.inner.deletion_vector_payloads_loaded,
            &metrics.inner.deletion_vectors_applied,
            &metrics.inner.deletion_vector_rows_deleted,
            &metrics.inner.deletion_vector_failures,
            &metrics.inner.deletion_vector_rejections,
            &metrics.inner.parquet_data_file_range_get_operations,
            &metrics.inner.parquet_data_file_full_get_operations,
            &metrics.inner.parquet_data_file_bytes_received,
            &metrics.inner.parquet_data_file_opened_bytes,
        ];
        for (index, counter) in counters.into_iter().enumerate() {
            saturating_fetch_add(counter, u64::try_from(index + 1).expect("small test value"));
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.scan_partitions_started, 1);
        assert_eq!(snapshot.scan_partitions_completed, 2);
        assert_eq!(snapshot.files_started, 3);
        assert_eq!(snapshot.files_completed, 4);
        assert_eq!(snapshot.batches_produced, 5);
        assert_eq!(snapshot.rows_produced, 6);
        assert_eq!(snapshot.deletion_vector_payloads_loaded, 7);
        assert_eq!(snapshot.deletion_vectors_applied, 8);
        assert_eq!(snapshot.deletion_vector_rows_deleted, 9);
        assert_eq!(snapshot.deletion_vector_failures, 10);
        assert_eq!(snapshot.deletion_vector_rejections, 11);
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(12));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(13));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(14));
        assert_eq!(snapshot.parquet_data_file_opened_bytes, Some(15));
    }

    #[test]
    fn cloned_handles_saturate_under_concurrent_updates() -> Result<(), &'static str> {
        let metrics = metrics(DeltaReaderBackend::NativeAsync);
        metrics
            .inner
            .files_started
            .store(u64::MAX - 1, Ordering::Relaxed);
        let workers = (0..4)
            .map(|_| {
                let metrics = metrics.clone();
                thread::spawn(move || {
                    saturating_fetch_add(&metrics.inner.files_started, 1);
                })
            })
            .collect::<Vec<_>>();

        for worker in workers {
            worker.join().map_err(|_| "metrics worker panicked")?;
        }

        assert_eq!(metrics.snapshot().files_started, u64::MAX);
        Ok(())
    }
}
