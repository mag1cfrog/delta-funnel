//! Provider read progress counters for Delta scan execution.
//!
//! These counters are intentionally independent from DataFusion's metrics set.
//! They are the provider-owned handoff for later orchestration code that needs
//! partial progress after success, failure, or cancellation.

use std::sync::atomic::{AtomicU64, Ordering};

use super::scheduling::DeltaProviderReaderBackend;

/// Immutable view of provider read progress for one physical scan.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaProviderReadStatsSnapshot {
    /// DataFusion table name for this source.
    pub source_name: String,
    /// Delta snapshot version selected for this scan.
    pub snapshot_version: u64,
    /// Provider file-reader backend selected for this scan.
    pub reader_backend: DeltaProviderReaderBackend,
    /// Whether metadata expansion exhausted the upstream kernel scan iterator.
    pub scan_metadata_exhausted: Option<bool>,
    /// Planned DataFusion execution partitions for this scan.
    pub scan_partitions_planned: u64,
    /// Selected provider file tasks planned for this scan.
    pub files_planned: u64,
    /// Estimated output rows from planning when every selected file had stats.
    pub estimated_rows: Option<u64>,
    /// Estimated bytes from planning when every selected file had a byte size.
    pub estimated_bytes: Option<u64>,
    /// Execution partitions whose stream was started by DataFusion.
    pub scan_partitions_started: u64,
    /// Execution partitions whose stream reached normal completion.
    pub scan_partitions_completed: u64,
    /// File-read handoffs that were started.
    pub files_started: u64,
    /// File-read handoffs that finished successfully.
    pub files_completed: u64,
    /// File tasks skipped before read scheduling by dynamic partition pruning.
    pub dynamic_partition_files_pruned: u64,
    /// File tasks kept after dynamic partition pruning evaluation.
    pub dynamic_partition_files_kept: u64,
    /// Post-phase physical filters offered to the Delta dynamic filter hook.
    pub dynamic_filters_received: u64,
    /// Offered dynamic filters retained for partition pruning.
    pub dynamic_filters_accepted: u64,
    /// Offered filters rejected by the dynamic filter hook policy.
    pub dynamic_filters_unsupported: u64,
    /// Record batches sent toward DataFusion.
    pub batches_produced: u64,
    /// Rows sent toward DataFusion after transform and DV filtering.
    pub rows_produced: u64,
    /// Deletion-vector payloads loaded for selected files.
    pub deletion_vector_payloads_loaded: u64,
    /// Deletion-vector masks applied to selected files.
    pub deletion_vectors_applied: u64,
    /// Rows removed by deletion-vector masks when known.
    pub deletion_vector_rows_deleted: u64,
    /// Deletion-vector read or masking failures.
    pub deletion_vector_failures: u64,
    /// Deletion-vector reads rejected by safety gates.
    pub deletion_vector_rejections: u64,
}

/// Static context and planning estimates for one provider read stats instance.
#[allow(dead_code)]
pub(crate) struct DeltaProviderReadStatsConfig {
    /// DataFusion table name for this source.
    pub(crate) source_name: String,
    /// Delta snapshot version selected for this scan.
    pub(crate) snapshot_version: u64,
    /// Provider file-reader backend selected for this scan.
    pub(crate) reader_backend: DeltaProviderReaderBackend,
    /// Whether metadata expansion exhausted the upstream kernel scan iterator.
    pub(crate) scan_metadata_exhausted: Option<bool>,
    /// Planned DataFusion execution partitions for this scan.
    pub(crate) scan_partitions_planned: usize,
    /// Selected provider file tasks planned for this scan.
    pub(crate) files_planned: usize,
    /// Estimated output rows from planning when every selected file had stats.
    pub(crate) estimated_rows: Option<u64>,
    /// Estimated bytes from planning when every selected file had a byte size.
    pub(crate) estimated_bytes: Option<u64>,
}

/// Thread-safe provider read progress for one physical scan.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct DeltaProviderReadStats {
    source_name: String,
    snapshot_version: u64,
    reader_backend: DeltaProviderReaderBackend,
    scan_metadata_exhausted: Option<bool>,
    scan_partitions_planned: u64,
    files_planned: u64,
    estimated_rows: Option<u64>,
    estimated_bytes: Option<u64>,
    scan_partitions_started: AtomicU64,
    scan_partitions_completed: AtomicU64,
    files_started: AtomicU64,
    files_completed: AtomicU64,
    dynamic_partition_files_pruned: AtomicU64,
    dynamic_partition_files_kept: AtomicU64,
    dynamic_filters_received: AtomicU64,
    dynamic_filters_accepted: AtomicU64,
    dynamic_filters_unsupported: AtomicU64,
    batches_produced: AtomicU64,
    rows_produced: AtomicU64,
    deletion_vector_payloads_loaded: AtomicU64,
    deletion_vectors_applied: AtomicU64,
    deletion_vector_rows_deleted: AtomicU64,
    deletion_vector_failures: AtomicU64,
    deletion_vector_rejections: AtomicU64,
}

#[allow(dead_code)]
impl DeltaProviderReadStats {
    /// Creates zeroed read progress for one physical scan.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn new(config: DeltaProviderReadStatsConfig) -> Self {
        Self {
            source_name: config.source_name,
            snapshot_version: config.snapshot_version,
            reader_backend: config.reader_backend,
            scan_metadata_exhausted: config.scan_metadata_exhausted,
            scan_partitions_planned: usize_to_u64_saturating(config.scan_partitions_planned),
            files_planned: usize_to_u64_saturating(config.files_planned),
            estimated_rows: config.estimated_rows,
            estimated_bytes: config.estimated_bytes,
            scan_partitions_started: AtomicU64::new(0),
            scan_partitions_completed: AtomicU64::new(0),
            files_started: AtomicU64::new(0),
            files_completed: AtomicU64::new(0),
            dynamic_partition_files_pruned: AtomicU64::new(0),
            dynamic_partition_files_kept: AtomicU64::new(0),
            dynamic_filters_received: AtomicU64::new(0),
            dynamic_filters_accepted: AtomicU64::new(0),
            dynamic_filters_unsupported: AtomicU64::new(0),
            batches_produced: AtomicU64::new(0),
            rows_produced: AtomicU64::new(0),
            deletion_vector_payloads_loaded: AtomicU64::new(0),
            deletion_vectors_applied: AtomicU64::new(0),
            deletion_vector_rows_deleted: AtomicU64::new(0),
            deletion_vector_failures: AtomicU64::new(0),
            deletion_vector_rejections: AtomicU64::new(0),
        }
    }

    /// Returns a point-in-time copy of all counters.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn snapshot(&self) -> DeltaProviderReadStatsSnapshot {
        DeltaProviderReadStatsSnapshot {
            source_name: self.source_name.clone(),
            snapshot_version: self.snapshot_version,
            reader_backend: self.reader_backend,
            scan_metadata_exhausted: self.scan_metadata_exhausted,
            scan_partitions_planned: self.scan_partitions_planned,
            files_planned: self.files_planned,
            estimated_rows: self.estimated_rows,
            estimated_bytes: self.estimated_bytes,
            scan_partitions_started: self.scan_partitions_started.load(Ordering::Relaxed),
            scan_partitions_completed: self.scan_partitions_completed.load(Ordering::Relaxed),
            files_started: self.files_started.load(Ordering::Relaxed),
            files_completed: self.files_completed.load(Ordering::Relaxed),
            dynamic_partition_files_pruned: self
                .dynamic_partition_files_pruned
                .load(Ordering::Relaxed),
            dynamic_partition_files_kept: self.dynamic_partition_files_kept.load(Ordering::Relaxed),
            dynamic_filters_received: self.dynamic_filters_received.load(Ordering::Relaxed),
            dynamic_filters_accepted: self.dynamic_filters_accepted.load(Ordering::Relaxed),
            dynamic_filters_unsupported: self.dynamic_filters_unsupported.load(Ordering::Relaxed),
            batches_produced: self.batches_produced.load(Ordering::Relaxed),
            rows_produced: self.rows_produced.load(Ordering::Relaxed),
            deletion_vector_payloads_loaded: self
                .deletion_vector_payloads_loaded
                .load(Ordering::Relaxed),
            deletion_vectors_applied: self.deletion_vectors_applied.load(Ordering::Relaxed),
            deletion_vector_rows_deleted: self.deletion_vector_rows_deleted.load(Ordering::Relaxed),
            deletion_vector_failures: self.deletion_vector_failures.load(Ordering::Relaxed),
            deletion_vector_rejections: self.deletion_vector_rejections.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_scan_partition_started(&self) {
        saturating_fetch_add(&self.scan_partitions_started, 1);
    }

    pub(crate) fn record_scan_partition_completed(&self) {
        saturating_fetch_add(&self.scan_partitions_completed, 1);
    }

    pub(crate) fn record_file_started(&self) {
        saturating_fetch_add(&self.files_started, 1);
    }

    pub(crate) fn record_file_completed(&self) {
        saturating_fetch_add(&self.files_completed, 1);
    }

    pub(crate) fn record_dynamic_partition_file_pruned(&self) {
        saturating_fetch_add(&self.dynamic_partition_files_pruned, 1);
    }

    pub(crate) fn record_dynamic_partition_file_kept(&self) {
        saturating_fetch_add(&self.dynamic_partition_files_kept, 1);
    }

    pub(crate) fn record_dynamic_filters_received(&self, count: usize) {
        saturating_fetch_add(
            &self.dynamic_filters_received,
            usize_to_u64_saturating(count),
        );
    }

    pub(crate) fn record_dynamic_filters_accepted(&self, count: usize) {
        saturating_fetch_add(
            &self.dynamic_filters_accepted,
            usize_to_u64_saturating(count),
        );
    }

    pub(crate) fn record_dynamic_filters_unsupported(&self, count: usize) {
        saturating_fetch_add(
            &self.dynamic_filters_unsupported,
            usize_to_u64_saturating(count),
        );
    }

    pub(crate) fn record_batch_produced(&self, rows: usize) {
        saturating_fetch_add(&self.batches_produced, 1);
        saturating_fetch_add(&self.rows_produced, usize_to_u64_saturating(rows));
    }

    pub(crate) fn record_deletion_vector_payload_loaded(&self) {
        saturating_fetch_add(&self.deletion_vector_payloads_loaded, 1);
    }

    pub(crate) fn record_deletion_vector_applied(&self, deleted_rows: usize) {
        saturating_fetch_add(&self.deletion_vectors_applied, 1);
        self.record_deletion_vector_rows_deleted(deleted_rows);
    }

    pub(crate) fn record_deletion_vector_rows_deleted(&self, deleted_rows: usize) {
        saturating_fetch_add(
            &self.deletion_vector_rows_deleted,
            usize_to_u64_saturating(deleted_rows),
        );
    }

    pub(crate) fn record_deletion_vector_failure(&self) {
        saturating_fetch_add(&self.deletion_vector_failures, 1);
    }

    pub(crate) fn record_deletion_vector_rejection(&self) {
        saturating_fetch_add(&self.deletion_vector_rejections, 1);
    }
}

#[allow(dead_code)]
fn saturating_fetch_add(counter: &AtomicU64, value: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(value);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

#[allow(dead_code)]
fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::{DeltaProviderReadStats, DeltaProviderReadStatsConfig};
    use crate::query_engine::datafusion::execution::DeltaProviderReaderBackend;

    #[test]
    fn read_stats_snapshot_starts_with_context_and_zero_counters() {
        let stats = DeltaProviderReadStats::new(DeltaProviderReadStatsConfig {
            source_name: "orders".to_owned(),
            snapshot_version: 7,
            reader_backend: DeltaProviderReaderBackend::OfficialKernel,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 3,
            files_planned: 5,
            estimated_rows: Some(99),
            estimated_bytes: Some(42),
        });
        let snapshot = stats.snapshot();

        assert_eq!(snapshot.source_name, "orders");
        assert_eq!(snapshot.snapshot_version, 7);
        assert_eq!(
            snapshot.reader_backend,
            DeltaProviderReaderBackend::OfficialKernel
        );
        assert_eq!(snapshot.scan_metadata_exhausted, Some(true));
        assert_eq!(snapshot.scan_partitions_planned, 3);
        assert_eq!(snapshot.files_planned, 5);
        assert_eq!(snapshot.estimated_rows, Some(99));
        assert_eq!(snapshot.estimated_bytes, Some(42));
        assert_eq!(snapshot.scan_partitions_started, 0);
        assert_eq!(snapshot.scan_partitions_completed, 0);
        assert_eq!(snapshot.files_started, 0);
        assert_eq!(snapshot.files_completed, 0);
        assert_eq!(snapshot.dynamic_partition_files_pruned, 0);
        assert_eq!(snapshot.dynamic_partition_files_kept, 0);
        assert_eq!(snapshot.dynamic_filters_received, 0);
        assert_eq!(snapshot.dynamic_filters_accepted, 0);
        assert_eq!(snapshot.dynamic_filters_unsupported, 0);
        assert_eq!(snapshot.batches_produced, 0);
        assert_eq!(snapshot.rows_produced, 0);
        assert_eq!(snapshot.deletion_vector_payloads_loaded, 0);
        assert_eq!(snapshot.deletion_vectors_applied, 0);
        assert_eq!(snapshot.deletion_vector_rows_deleted, 0);
        assert_eq!(snapshot.deletion_vector_failures, 0);
        assert_eq!(snapshot.deletion_vector_rejections, 0);
    }

    #[test]
    fn read_stats_records_partial_progress_without_completing_failed_work() {
        let stats = DeltaProviderReadStats::new(DeltaProviderReadStatsConfig {
            source_name: "orders".to_owned(),
            snapshot_version: 7,
            reader_backend: DeltaProviderReaderBackend::OfficialKernel,
            scan_metadata_exhausted: Some(false),
            scan_partitions_planned: 1,
            files_planned: 1,
            estimated_rows: None,
            estimated_bytes: None,
        });

        stats.record_scan_partition_started();
        stats.record_file_started();
        stats.record_dynamic_partition_file_pruned();
        stats.record_dynamic_partition_file_kept();
        stats.record_dynamic_filters_received(3);
        stats.record_dynamic_filters_accepted(1);
        stats.record_dynamic_filters_unsupported(2);
        stats.record_batch_produced(3);
        stats.record_deletion_vector_payload_loaded();
        stats.record_deletion_vector_applied(1);
        stats.record_deletion_vector_failure();

        let snapshot = stats.snapshot();

        assert_eq!(snapshot.scan_partitions_started, 1);
        assert_eq!(snapshot.scan_partitions_completed, 0);
        assert_eq!(snapshot.files_started, 1);
        assert_eq!(snapshot.files_completed, 0);
        assert_eq!(snapshot.dynamic_partition_files_pruned, 1);
        assert_eq!(snapshot.dynamic_partition_files_kept, 1);
        assert_eq!(snapshot.dynamic_filters_received, 3);
        assert_eq!(snapshot.dynamic_filters_accepted, 1);
        assert_eq!(snapshot.dynamic_filters_unsupported, 2);
        assert_eq!(snapshot.batches_produced, 1);
        assert_eq!(snapshot.rows_produced, 3);
        assert_eq!(snapshot.deletion_vector_payloads_loaded, 1);
        assert_eq!(snapshot.deletion_vectors_applied, 1);
        assert_eq!(snapshot.deletion_vector_rows_deleted, 1);
        assert_eq!(snapshot.deletion_vector_failures, 1);
        assert_eq!(snapshot.deletion_vector_rejections, 0);
    }

    #[test]
    fn read_stats_updates_are_thread_safe() -> Result<(), Box<dyn std::error::Error>> {
        const THREADS: usize = 4;
        const ITERATIONS: usize = 100;

        let stats = Arc::new(DeltaProviderReadStats::new(DeltaProviderReadStatsConfig {
            source_name: "orders".to_owned(),
            snapshot_version: 7,
            reader_backend: DeltaProviderReaderBackend::OfficialKernel,
            scan_metadata_exhausted: None,
            scan_partitions_planned: THREADS,
            files_planned: THREADS,
            estimated_rows: None,
            estimated_bytes: None,
        }));
        let mut handles = Vec::new();

        for _ in 0..THREADS {
            let stats = Arc::clone(&stats);
            handles.push(thread::spawn(move || {
                for _ in 0..ITERATIONS {
                    stats.record_scan_partition_started();
                    stats.record_scan_partition_completed();
                    stats.record_file_started();
                    stats.record_file_completed();
                    stats.record_dynamic_partition_file_pruned();
                    stats.record_dynamic_partition_file_kept();
                    stats.record_dynamic_filters_received(3);
                    stats.record_dynamic_filters_accepted(1);
                    stats.record_dynamic_filters_unsupported(2);
                    stats.record_batch_produced(2);
                    stats.record_deletion_vector_payload_loaded();
                    stats.record_deletion_vector_applied(1);
                    stats.record_deletion_vector_rejection();
                }
            }));
        }

        for handle in handles {
            handle.join().map_err(|_| "stats worker panicked")?;
        }

        let snapshot = stats.snapshot();
        let expected = u64::try_from(THREADS * ITERATIONS)?;

        assert_eq!(snapshot.scan_metadata_exhausted, None);
        assert_eq!(snapshot.scan_partitions_started, expected);
        assert_eq!(snapshot.scan_partitions_completed, expected);
        assert_eq!(snapshot.files_started, expected);
        assert_eq!(snapshot.files_completed, expected);
        assert_eq!(snapshot.dynamic_partition_files_pruned, expected);
        assert_eq!(snapshot.dynamic_partition_files_kept, expected);
        assert_eq!(snapshot.dynamic_filters_received, expected * 3);
        assert_eq!(snapshot.dynamic_filters_accepted, expected);
        assert_eq!(snapshot.dynamic_filters_unsupported, expected * 2);
        assert_eq!(snapshot.batches_produced, expected);
        assert_eq!(snapshot.rows_produced, expected * 2);
        assert_eq!(snapshot.deletion_vector_payloads_loaded, expected);
        assert_eq!(snapshot.deletion_vectors_applied, expected);
        assert_eq!(snapshot.deletion_vector_rows_deleted, expected);
        assert_eq!(snapshot.deletion_vector_rejections, expected);

        Ok(())
    }
}
