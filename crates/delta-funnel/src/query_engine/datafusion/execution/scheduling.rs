//! Bounded scheduling options for Delta scan execution.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::DeltaFunnelError;

/// Native async bounded file prefetch depth selected by benchmark evidence.
pub const NATIVE_ASYNC_DEFAULT_PREFETCH_FILE_COUNT_PER_PARTITION: usize = 2;

/// Native async per-partition file-read capacity selected by benchmark evidence.
pub const NATIVE_ASYNC_DEFAULT_FILE_READS_PER_PARTITION: usize = 3;

/// Provider file-reader backend selected for Delta scan execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaProviderReaderBackend {
    /// Current official delta_kernel reader baseline.
    OfficialKernel,
    /// Native async Parquet reader for row-index-preserving file tasks.
    NativeAsync,
}

impl DeltaProviderReaderBackend {
    /// Whether this backend can apply file-read predicates before DV masking
    /// without losing each row's original physical Parquet row index.
    pub(crate) fn supports_dv_row_index_predicate_reads(self) -> bool {
        match self {
            Self::OfficialKernel => false,
            Self::NativeAsync => true,
        }
    }
}

/// Bounded scheduling options for one Delta DataFusion provider scan.
///
/// These limits apply to provider-scheduled physical Delta file reads. The
/// official-kernel sync fallback limits a conservative file-level
/// handoff: reading one file and sending its batches into the bounded
/// DataFusion output channel. The native async reader preserves the same
/// active-read semantics with async semaphore-style permits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaProviderScanExecutionOptions {
    /// File-reader backend used by normal provider scan execution.
    pub reader_backend: DeltaProviderReaderBackend,

    /// Maximum file-read handoffs that may run at once across the whole scan.
    ///
    /// This is the scan-wide concurrency cap shared by all DataFusion execution
    /// partitions for one provider scan.
    pub max_concurrent_file_reads_per_scan: usize,

    /// Maximum file-read handoffs that one execution partition may own at once.
    ///
    /// This prevents a single DataFusion execution partition from consuming all
    /// scan-wide read capacity when multiple scan partitions are active.
    pub max_concurrent_file_reads_per_partition: usize,

    /// Bounded output channel capacity for each DataFusion execution partition.
    ///
    /// This is the producer-to-DataFusion handoff queue used after file reads
    /// produce Arrow batches. A value of `1` preserves the historical behavior.
    pub output_buffer_capacity_per_partition: usize,

    /// Native async file reads to prefetch ahead of downstream demand per partition.
    ///
    /// A value of `0` keeps the native async backend fully lazy: each file is
    /// opened only after the previous file is drained. Positive values allow
    /// the native async backend to begin opening additional files while the
    /// current file is still producing batches. This setting is internal
    /// hardening and benchmark surface; the official-kernel backend ignores it.
    pub native_async_prefetch_file_count_per_partition: usize,
}

/// Sync fallback limiter for provider-scheduled file work in one Delta scan.
///
/// This limiter exists for the official-kernel synchronous iterator bridge.
/// Native async execution uses `DeltaProviderAsyncReadLimiter` at the same
/// scheduling boundary.
pub(crate) struct DeltaProviderSyncReadLimiter {
    options: DeltaProviderScanExecutionOptions,
    state: Mutex<DeltaProviderSyncReadLimiterState>,
    ready: Condvar,
}

#[derive(Debug)]
struct DeltaProviderSyncReadLimiterState {
    active_file_reads: usize,
    partition_active_file_reads: Vec<usize>,
}

/// Per-execution-partition view of a sync fallback read limiter.
#[derive(Clone)]
pub(crate) struct DeltaProviderSyncPartitionReadLimiter {
    partition: usize,
    limiter: Arc<DeltaProviderSyncReadLimiter>,
}

/// Sync fallback guard for one provider file handoff.
///
/// The current official-kernel reader exposes a synchronous file iterator, so
/// the provider limits a conservative unit: reading one Delta file and handing
/// all batches from that file to the bounded DataFusion output channel.
pub(crate) struct DeltaProviderSyncFileReadPermit {
    partition: usize,
    limiter: Arc<DeltaProviderSyncReadLimiter>,
    released: bool,
}

/// Async limiter for native provider file work in one Delta scan.
///
/// This limiter is intentionally separate from the official-kernel sync
/// fallback limiter. Native async readers must acquire both scan-wide and
/// partition-local capacity before starting file, object-store, or range-read
/// work.
#[allow(dead_code)]
pub(crate) struct DeltaProviderAsyncReadLimiter {
    options: DeltaProviderScanExecutionOptions,
    scan_permits: Arc<Semaphore>,
    partition_permits: Vec<Arc<Semaphore>>,
}

/// Per-execution-partition view of the native async read limiter.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct DeltaProviderAsyncPartitionReadLimiter {
    partition: usize,
    limiter: Arc<DeltaProviderAsyncReadLimiter>,
}

/// Owned async permit for one native provider file handoff.
#[allow(dead_code)]
pub(crate) struct DeltaProviderAsyncFileReadPermit {
    _scan_permit: OwnedSemaphorePermit,
    _partition_permit: OwnedSemaphorePermit,
}

impl DeltaProviderSyncReadLimiter {
    pub(crate) fn new(
        options: DeltaProviderScanExecutionOptions,
        partition_count: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            options,
            state: Mutex::new(DeltaProviderSyncReadLimiterState {
                active_file_reads: 0,
                partition_active_file_reads: vec![0; partition_count],
            }),
            ready: Condvar::new(),
        })
    }

    pub(crate) fn partition_limiter(
        self: &Arc<Self>,
        partition: usize,
    ) -> Result<DeltaProviderSyncPartitionReadLimiter, DeltaFunnelError> {
        let partition_count = self.lock_state().partition_active_file_reads.len();
        if partition >= partition_count {
            return Err(DeltaFunnelError::Config {
                message: format!(
                    "sync read limiter partition {partition} is out of range for {partition_count} partitions"
                ),
            });
        }

        Ok(DeltaProviderSyncPartitionReadLimiter {
            partition,
            limiter: Arc::clone(self),
        })
    }

    #[cfg(test)]
    pub(crate) fn active_file_reads(&self) -> usize {
        self.lock_state().active_file_reads
    }

    #[cfg(test)]
    fn try_acquire_file_permit(
        self: &Arc<Self>,
        partition: usize,
    ) -> Option<DeltaProviderSyncFileReadPermit> {
        let mut state = self.lock_state();
        if !self.can_acquire(&state, partition) {
            return None;
        }

        state.active_file_reads += 1;
        state.partition_active_file_reads[partition] += 1;

        Some(DeltaProviderSyncFileReadPermit {
            partition,
            limiter: Arc::clone(self),
            released: false,
        })
    }

    fn acquire_file_permit(self: &Arc<Self>, partition: usize) -> DeltaProviderSyncFileReadPermit {
        let mut state = self.lock_state();
        while !self.can_acquire(&state, partition) {
            state = self.wait_for_ready(state);
        }

        state.active_file_reads += 1;
        state.partition_active_file_reads[partition] += 1;

        DeltaProviderSyncFileReadPermit {
            partition,
            limiter: Arc::clone(self),
            released: false,
        }
    }

    fn release_file_permit(&self, partition: usize) {
        let mut state = self.lock_state();
        if state.active_file_reads > 0 {
            state.active_file_reads -= 1;
        }
        if let Some(partition_active_file_reads) =
            state.partition_active_file_reads.get_mut(partition)
            && *partition_active_file_reads > 0
        {
            *partition_active_file_reads -= 1;
        }
        self.ready.notify_all();
    }

    fn can_acquire(&self, state: &DeltaProviderSyncReadLimiterState, partition: usize) -> bool {
        state.active_file_reads < self.options.max_concurrent_file_reads_per_scan
            && state.partition_active_file_reads[partition]
                < self.options.max_concurrent_file_reads_per_partition
    }

    fn lock_state(&self) -> MutexGuard<'_, DeltaProviderSyncReadLimiterState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn wait_for_ready<'a>(
        &self,
        state: MutexGuard<'a, DeltaProviderSyncReadLimiterState>,
    ) -> MutexGuard<'a, DeltaProviderSyncReadLimiterState> {
        match self.ready.wait(state) {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[allow(dead_code)]
impl DeltaProviderAsyncReadLimiter {
    pub(crate) fn new(
        options: DeltaProviderScanExecutionOptions,
        partition_count: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            options,
            scan_permits: Arc::new(Semaphore::new(options.max_concurrent_file_reads_per_scan)),
            partition_permits: (0..partition_count)
                .map(|_| {
                    Arc::new(Semaphore::new(
                        options.max_concurrent_file_reads_per_partition,
                    ))
                })
                .collect(),
        })
    }

    pub(crate) fn partition_limiter(
        self: &Arc<Self>,
        partition: usize,
    ) -> Result<DeltaProviderAsyncPartitionReadLimiter, DeltaFunnelError> {
        let partition_count = self.partition_permits.len();
        if partition >= partition_count {
            return Err(DeltaFunnelError::Config {
                message: format!(
                    "async read limiter partition {partition} is out of range for {partition_count} partitions"
                ),
            });
        }

        Ok(DeltaProviderAsyncPartitionReadLimiter {
            partition,
            limiter: Arc::clone(self),
        })
    }

    #[cfg(test)]
    pub(crate) fn active_file_reads(&self) -> usize {
        self.options
            .max_concurrent_file_reads_per_scan
            .saturating_sub(self.scan_permits.available_permits())
    }

    #[cfg(test)]
    pub(crate) fn partition_active_file_reads(&self, partition: usize) -> Option<usize> {
        self.partition_permits.get(partition).map(|permits| {
            self.options
                .max_concurrent_file_reads_per_partition
                .saturating_sub(permits.available_permits())
        })
    }

    async fn acquire_file_permit(
        self: &Arc<Self>,
        partition: usize,
    ) -> Result<DeltaProviderAsyncFileReadPermit, DeltaFunnelError> {
        let scan_permit = Arc::clone(&self.scan_permits)
            .acquire_owned()
            .await
            .map_err(|_| DeltaFunnelError::Config {
                message: "async read limiter scan permits are closed".to_owned(),
            })?;
        let partition_permits = self.partition_permits.get(partition).ok_or_else(|| {
            DeltaFunnelError::Config {
                message: format!(
                    "async read limiter partition {partition} is out of range for {} partitions",
                    self.partition_permits.len()
                ),
            }
        })?;
        let partition_permit = Arc::clone(partition_permits)
            .acquire_owned()
            .await
            .map_err(|_| DeltaFunnelError::Config {
                message: format!("async read limiter partition {partition} permits are closed"),
            })?;

        Ok(DeltaProviderAsyncFileReadPermit {
            _scan_permit: scan_permit,
            _partition_permit: partition_permit,
        })
    }

    #[cfg(test)]
    fn try_acquire_file_permit(
        self: &Arc<Self>,
        partition: usize,
    ) -> Option<DeltaProviderAsyncFileReadPermit> {
        let scan_permit = Arc::clone(&self.scan_permits).try_acquire_owned().ok()?;
        let partition_permits = self.partition_permits.get(partition)?;
        let partition_permit = match Arc::clone(partition_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return None,
        };

        Some(DeltaProviderAsyncFileReadPermit {
            _scan_permit: scan_permit,
            _partition_permit: partition_permit,
        })
    }
}

#[allow(dead_code)]
impl DeltaProviderAsyncPartitionReadLimiter {
    pub(crate) async fn acquire_file_permit(
        &self,
    ) -> Result<DeltaProviderAsyncFileReadPermit, DeltaFunnelError> {
        self.limiter.acquire_file_permit(self.partition).await
    }

    #[cfg(test)]
    fn try_acquire_file_permit(&self) -> Option<DeltaProviderAsyncFileReadPermit> {
        self.limiter.try_acquire_file_permit(self.partition)
    }
}

impl DeltaProviderSyncPartitionReadLimiter {
    pub(crate) fn acquire_file_permit(&self) -> DeltaProviderSyncFileReadPermit {
        self.limiter.acquire_file_permit(self.partition)
    }

    #[cfg(test)]
    fn try_acquire_file_permit(&self) -> Option<DeltaProviderSyncFileReadPermit> {
        self.limiter.try_acquire_file_permit(self.partition)
    }
}

impl Drop for DeltaProviderSyncFileReadPermit {
    fn drop(&mut self) {
        if !self.released {
            self.limiter.release_file_permit(self.partition);
            self.released = true;
        }
    }
}

impl Default for DeltaProviderScanExecutionOptions {
    fn default() -> Self {
        Self {
            reader_backend: DeltaProviderReaderBackend::NativeAsync,
            max_concurrent_file_reads_per_scan: NATIVE_ASYNC_DEFAULT_FILE_READS_PER_PARTITION,
            max_concurrent_file_reads_per_partition: NATIVE_ASYNC_DEFAULT_FILE_READS_PER_PARTITION,
            output_buffer_capacity_per_partition: 1,
            native_async_prefetch_file_count_per_partition:
                NATIVE_ASYNC_DEFAULT_PREFETCH_FILE_COUNT_PER_PARTITION,
        }
    }
}

impl DeltaProviderScanExecutionOptions {
    /// Builds validated Delta provider scan execution options.
    pub fn try_new(
        max_concurrent_file_reads_per_scan: usize,
        max_concurrent_file_reads_per_partition: usize,
    ) -> Result<Self, DeltaFunnelError> {
        Self::try_new_with_reader_backend(
            DeltaProviderReaderBackend::OfficialKernel,
            max_concurrent_file_reads_per_scan,
            max_concurrent_file_reads_per_partition,
        )
    }

    /// Builds validated Delta provider scan execution options with a reader backend.
    pub fn try_new_with_reader_backend(
        reader_backend: DeltaProviderReaderBackend,
        max_concurrent_file_reads_per_scan: usize,
        max_concurrent_file_reads_per_partition: usize,
    ) -> Result<Self, DeltaFunnelError> {
        let native_async_prefetch_file_count_per_partition =
            default_native_async_prefetch_file_count_per_partition(
                reader_backend,
                max_concurrent_file_reads_per_partition,
            );
        let options = Self {
            reader_backend,
            max_concurrent_file_reads_per_scan,
            max_concurrent_file_reads_per_partition,
            output_buffer_capacity_per_partition: 1,
            native_async_prefetch_file_count_per_partition,
        };
        options.validate()?;
        Ok(options)
    }

    /// Sets the per-partition bounded output buffer capacity.
    pub fn with_output_buffer_capacity_per_partition(
        mut self,
        output_buffer_capacity_per_partition: usize,
    ) -> Result<Self, DeltaFunnelError> {
        self.output_buffer_capacity_per_partition = output_buffer_capacity_per_partition;
        self.validate()?;
        Ok(self)
    }

    /// Sets native async file-read prefetch depth per execution partition.
    pub fn with_native_async_prefetch_file_count_per_partition(
        mut self,
        native_async_prefetch_file_count_per_partition: usize,
    ) -> Result<Self, DeltaFunnelError> {
        self.native_async_prefetch_file_count_per_partition =
            native_async_prefetch_file_count_per_partition;
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn with_default_scan_wide_capacity_for_target_partitions(
        mut self,
        target_partitions: usize,
    ) -> Result<Self, DeltaFunnelError> {
        validate_positive("target_partitions", target_partitions)?;
        if self.reader_backend == DeltaProviderReaderBackend::NativeAsync {
            self.max_concurrent_file_reads_per_scan = target_partitions
                .saturating_mul(self.max_concurrent_file_reads_per_partition)
                .max(1);
        }
        self.validate()?;
        Ok(self)
    }

    /// Validates provider scan execution bounds before registration or scan execution.
    pub fn validate(&self) -> Result<(), DeltaFunnelError> {
        validate_reader_backend(self.reader_backend)?;
        validate_positive(
            "max_concurrent_file_reads_per_scan",
            self.max_concurrent_file_reads_per_scan,
        )?;
        validate_positive(
            "max_concurrent_file_reads_per_partition",
            self.max_concurrent_file_reads_per_partition,
        )?;
        validate_positive(
            "output_buffer_capacity_per_partition",
            self.output_buffer_capacity_per_partition,
        )?;
        Ok(())
    }
}

fn validate_reader_backend(
    reader_backend: DeltaProviderReaderBackend,
) -> Result<(), DeltaFunnelError> {
    match reader_backend {
        DeltaProviderReaderBackend::OfficialKernel | DeltaProviderReaderBackend::NativeAsync => {
            Ok(())
        }
    }
}

fn validate_positive(name: &'static str, value: usize) -> Result<(), DeltaFunnelError> {
    if value == 0 {
        return Err(DeltaFunnelError::Config {
            message: format!("{name} must be greater than zero"),
        });
    }

    Ok(())
}

fn default_native_async_prefetch_file_count_per_partition(
    reader_backend: DeltaProviderReaderBackend,
    max_concurrent_file_reads_per_partition: usize,
) -> usize {
    if reader_backend != DeltaProviderReaderBackend::NativeAsync {
        return 0;
    }

    max_concurrent_file_reads_per_partition
        .saturating_sub(1)
        .min(NATIVE_ASYNC_DEFAULT_PREFETCH_FILE_COUNT_PER_PARTITION)
}

#[cfg(test)]
mod tests {
    use super::{
        DeltaProviderAsyncReadLimiter, DeltaProviderReaderBackend,
        DeltaProviderScanExecutionOptions, DeltaProviderSyncReadLimiter,
        NATIVE_ASYNC_DEFAULT_FILE_READS_PER_PARTITION,
        NATIVE_ASYNC_DEFAULT_PREFETCH_FILE_COUNT_PER_PARTITION,
    };

    #[test]
    fn default_execution_options_are_valid() -> Result<(), Box<dyn std::error::Error>> {
        DeltaProviderScanExecutionOptions::default().validate()?;

        Ok(())
    }

    #[test]
    fn default_execution_options_select_native_async_backend()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = DeltaProviderScanExecutionOptions::default();

        assert_eq!(
            options.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(
            options.max_concurrent_file_reads_per_scan,
            NATIVE_ASYNC_DEFAULT_FILE_READS_PER_PARTITION
        );
        assert_eq!(
            options.max_concurrent_file_reads_per_partition,
            NATIVE_ASYNC_DEFAULT_FILE_READS_PER_PARTITION
        );
        assert_eq!(options.output_buffer_capacity_per_partition, 1);
        assert_eq!(
            options.native_async_prefetch_file_count_per_partition,
            NATIVE_ASYNC_DEFAULT_PREFETCH_FILE_COUNT_PER_PARTITION
        );
        options.validate()?;

        Ok(())
    }

    #[test]
    fn execution_options_can_select_official_kernel_backend()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::OfficialKernel,
            2,
            1,
        )?;

        assert_eq!(
            options.reader_backend,
            DeltaProviderReaderBackend::OfficialKernel
        );
        assert_eq!(options.max_concurrent_file_reads_per_scan, 2);
        assert_eq!(options.max_concurrent_file_reads_per_partition, 1);
        assert_eq!(options.output_buffer_capacity_per_partition, 1);
        assert_eq!(options.native_async_prefetch_file_count_per_partition, 0);

        Ok(())
    }

    #[test]
    fn execution_options_can_select_native_async_backend() -> Result<(), Box<dyn std::error::Error>>
    {
        let options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            2,
            1,
        )?;

        assert_eq!(
            options.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(options.max_concurrent_file_reads_per_scan, 2);
        assert_eq!(options.max_concurrent_file_reads_per_partition, 1);
        assert_eq!(options.output_buffer_capacity_per_partition, 1);
        assert_eq!(options.native_async_prefetch_file_count_per_partition, 0);

        Ok(())
    }

    #[test]
    fn native_async_backend_defaults_to_bounded_prefetch_when_capacity_allows()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            12,
            3,
        )?;

        assert_eq!(
            options.native_async_prefetch_file_count_per_partition,
            NATIVE_ASYNC_DEFAULT_PREFETCH_FILE_COUNT_PER_PARTITION
        );

        Ok(())
    }

    #[test]
    fn native_async_backend_default_prefetch_respects_partition_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            2,
            2,
        )?;

        assert_eq!(options.native_async_prefetch_file_count_per_partition, 1);

        Ok(())
    }

    #[test]
    fn native_async_backend_can_force_lazy_after_default_prefetch()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            12,
            3,
        )?
        .with_native_async_prefetch_file_count_per_partition(0)?;

        assert_eq!(options.native_async_prefetch_file_count_per_partition, 0);

        Ok(())
    }

    #[test]
    fn native_async_default_scan_wide_capacity_uses_target_partitions()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = DeltaProviderScanExecutionOptions::default()
            .with_default_scan_wide_capacity_for_target_partitions(8)?;

        assert_eq!(
            options.max_concurrent_file_reads_per_scan,
            8 * NATIVE_ASYNC_DEFAULT_FILE_READS_PER_PARTITION
        );
        assert_eq!(
            options.max_concurrent_file_reads_per_partition,
            NATIVE_ASYNC_DEFAULT_FILE_READS_PER_PARTITION
        );

        Ok(())
    }

    #[test]
    fn official_kernel_default_scan_wide_capacity_resolution_is_noop()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::OfficialKernel,
            2,
            1,
        )?
        .with_default_scan_wide_capacity_for_target_partitions(8)?;

        assert_eq!(options.max_concurrent_file_reads_per_scan, 2);
        assert_eq!(options.max_concurrent_file_reads_per_partition, 1);

        Ok(())
    }

    #[test]
    fn execution_options_can_set_output_buffer_capacity() -> Result<(), Box<dyn std::error::Error>>
    {
        let options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            2,
            1,
        )?
        .with_output_buffer_capacity_per_partition(4)?;

        assert_eq!(options.output_buffer_capacity_per_partition, 4);

        Ok(())
    }

    #[test]
    fn execution_options_can_set_native_async_prefetch_depth()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            2,
            2,
        )?
        .with_native_async_prefetch_file_count_per_partition(1)?;

        assert_eq!(options.native_async_prefetch_file_count_per_partition, 1);

        Ok(())
    }

    #[test]
    fn execution_options_reject_zero_max_concurrent_file_reads_per_scan() {
        let error = DeltaProviderScanExecutionOptions::try_new(0, 1)
            .expect_err("zero max_concurrent_file_reads_per_scan must fail");

        assert_eq!(
            error.to_string(),
            "configuration error: max_concurrent_file_reads_per_scan must be greater than zero"
        );
    }

    #[test]
    fn execution_options_reject_zero_max_concurrent_file_reads_per_partition() {
        let error = DeltaProviderScanExecutionOptions::try_new(1, 0)
            .expect_err("zero max_concurrent_file_reads_per_partition must fail");

        assert_eq!(
            error.to_string(),
            "configuration error: max_concurrent_file_reads_per_partition must be greater than zero"
        );
    }

    #[test]
    fn execution_options_reject_zero_output_buffer_capacity() {
        let error = DeltaProviderScanExecutionOptions::default()
            .with_output_buffer_capacity_per_partition(0)
            .expect_err("zero output_buffer_capacity_per_partition must fail");

        assert_eq!(
            error.to_string(),
            "configuration error: output_buffer_capacity_per_partition must be greater than zero"
        );
    }

    #[test]
    fn sync_file_permit_releases_on_drop() -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let partition = limiter.partition_limiter(0)?;

        let permit = partition.acquire_file_permit();
        assert_eq!(limiter.active_file_reads(), 1);

        drop(permit);

        assert_eq!(limiter.active_file_reads(), 0);

        Ok(())
    }

    #[test]
    fn sync_file_permit_rejects_out_of_range_partition() -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let error = match limiter.partition_limiter(1) {
            Ok(_) => return Err("out of range partition must fail".into()),
            Err(error) => error,
        };

        assert_eq!(
            error.to_string(),
            "configuration error: sync read limiter partition 1 is out of range for 1 partitions"
        );

        Ok(())
    }

    #[test]
    fn sync_file_permit_respects_global_cap() -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::try_new(1, 1)?, 2);
        let first_partition = limiter.partition_limiter(0)?;
        let second_partition = limiter.partition_limiter(1)?;

        let first_permit = first_partition.acquire_file_permit();

        assert!(second_partition.try_acquire_file_permit().is_none());
        assert_eq!(limiter.active_file_reads(), 1);

        drop(first_permit);

        let second_permit = second_partition
            .try_acquire_file_permit()
            .ok_or("expected global permit after release")?;

        assert_eq!(limiter.active_file_reads(), 1);

        drop(second_permit);

        assert_eq!(limiter.active_file_reads(), 0);

        Ok(())
    }

    #[test]
    fn sync_file_permit_respects_partition_cap() -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::try_new(2, 1)?, 2);
        let first_partition = limiter.partition_limiter(0)?;
        let second_partition = limiter.partition_limiter(1)?;

        let first_permit = first_partition.acquire_file_permit();

        assert!(first_partition.try_acquire_file_permit().is_none());

        let second_permit = second_partition
            .try_acquire_file_permit()
            .ok_or("expected another partition to use remaining global capacity")?;

        assert_eq!(limiter.active_file_reads(), 2);

        drop(first_permit);
        drop(second_permit);

        assert_eq!(limiter.active_file_reads(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn async_file_permit_rejects_out_of_range_partition()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderAsyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let error = match limiter.partition_limiter(1) {
            Ok(_) => return Err("out of range partition must fail".into()),
            Err(error) => error,
        };

        assert_eq!(
            error.to_string(),
            "configuration error: async read limiter partition 1 is out of range for 1 partitions"
        );

        Ok(())
    }

    #[tokio::test]
    async fn async_file_permit_respects_global_cap() -> Result<(), Box<dyn std::error::Error>> {
        let limiter = DeltaProviderAsyncReadLimiter::new(
            DeltaProviderScanExecutionOptions::try_new(1, 1)?,
            2,
        );
        let first_partition = limiter.partition_limiter(0)?;
        let second_partition = limiter.partition_limiter(1)?;

        let first_permit = first_partition.acquire_file_permit().await?;

        assert!(second_partition.try_acquire_file_permit().is_none());
        assert_eq!(limiter.active_file_reads(), 1);
        assert_eq!(limiter.partition_active_file_reads(0), Some(1));
        assert_eq!(limiter.partition_active_file_reads(1), Some(0));

        drop(first_permit);

        let second_permit = second_partition
            .try_acquire_file_permit()
            .ok_or("expected global permit after release")?;

        assert_eq!(limiter.active_file_reads(), 1);
        assert_eq!(limiter.partition_active_file_reads(0), Some(0));
        assert_eq!(limiter.partition_active_file_reads(1), Some(1));

        drop(second_permit);

        assert_eq!(limiter.active_file_reads(), 0);
        assert_eq!(limiter.partition_active_file_reads(1), Some(0));

        Ok(())
    }

    #[tokio::test]
    async fn async_file_permit_respects_partition_cap() -> Result<(), Box<dyn std::error::Error>> {
        let limiter = DeltaProviderAsyncReadLimiter::new(
            DeltaProviderScanExecutionOptions::try_new(2, 1)?,
            2,
        );
        let first_partition = limiter.partition_limiter(0)?;
        let second_partition = limiter.partition_limiter(1)?;

        let first_permit = first_partition.acquire_file_permit().await?;

        assert!(first_partition.try_acquire_file_permit().is_none());

        let second_permit = second_partition
            .try_acquire_file_permit()
            .ok_or("expected another partition to use remaining global capacity")?;

        assert_eq!(limiter.active_file_reads(), 2);
        assert_eq!(limiter.partition_active_file_reads(0), Some(1));
        assert_eq!(limiter.partition_active_file_reads(1), Some(1));

        drop(first_permit);
        drop(second_permit);

        assert_eq!(limiter.active_file_reads(), 0);
        assert_eq!(limiter.partition_active_file_reads(0), Some(0));
        assert_eq!(limiter.partition_active_file_reads(1), Some(0));

        Ok(())
    }

    #[tokio::test]
    async fn async_file_permit_releases_on_success() -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderAsyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let partition = limiter.partition_limiter(0)?;

        async fn successful_read(
            partition: &super::DeltaProviderAsyncPartitionReadLimiter,
        ) -> Result<(), crate::DeltaFunnelError> {
            let _permit = partition.acquire_file_permit().await?;
            Ok(())
        }

        successful_read(&partition).await?;

        assert_eq!(limiter.active_file_reads(), 0);
        assert_eq!(limiter.partition_active_file_reads(0), Some(0));

        Ok(())
    }

    #[tokio::test]
    async fn async_file_permit_releases_on_failure() -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderAsyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let partition = limiter.partition_limiter(0)?;

        async fn failing_read(
            partition: &super::DeltaProviderAsyncPartitionReadLimiter,
        ) -> Result<(), crate::DeltaFunnelError> {
            let _permit = partition.acquire_file_permit().await?;
            Err(crate::DeltaFunnelError::Config {
                message: "fake async read failure".to_owned(),
            })
        }

        let error = match failing_read(&partition).await {
            Ok(_) => return Err("fake read failure must fail".into()),
            Err(error) => error,
        };

        assert_eq!(
            error.to_string(),
            "configuration error: fake async read failure"
        );
        assert_eq!(limiter.active_file_reads(), 0);
        assert_eq!(limiter.partition_active_file_reads(0), Some(0));

        Ok(())
    }

    #[tokio::test]
    async fn async_file_permit_releases_on_drop() -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderAsyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let partition = limiter.partition_limiter(0)?;

        let permit = partition.acquire_file_permit().await?;

        assert_eq!(limiter.active_file_reads(), 1);
        assert_eq!(limiter.partition_active_file_reads(0), Some(1));

        drop(permit);

        assert_eq!(limiter.active_file_reads(), 0);
        assert_eq!(limiter.partition_active_file_reads(0), Some(0));

        Ok(())
    }

    #[test]
    fn async_limiter_source_avoids_blocking_runtime_patterns()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = include_str!("scheduling.rs");
        let async_limiter_source = source_section(
            source,
            "impl DeltaProviderAsyncReadLimiter",
            "impl DeltaProviderSyncPartitionReadLimiter",
        )?;
        let forbidden_patterns = [
            concat!("Cond", "var"),
            concat!("Mut", "ex"),
            concat!("block", "_", "on"),
            concat!("Handle", "::", "block", "_", "on"),
            concat!("Runtime", "::", "new"),
            concat!("spawn", "_", "blocking"),
        ];

        for pattern in forbidden_patterns {
            assert!(
                !async_limiter_source.contains(pattern),
                "async limiter source must not contain {pattern}"
            );
        }

        Ok(())
    }

    fn source_section<'a>(
        source: &'a str,
        start_pattern: &str,
        end_pattern: &str,
    ) -> Result<&'a str, Box<dyn std::error::Error>> {
        let start = source
            .find(start_pattern)
            .ok_or("expected source section start")?;
        let end = source[start..]
            .find(end_pattern)
            .map(|offset| start + offset)
            .ok_or("expected source section end")?;

        Ok(&source[start..end])
    }
}
