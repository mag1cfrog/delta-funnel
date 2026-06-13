//! Bounded scheduling options for Delta scan execution.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::DeltaFunnelError;

/// Bounded scheduling options for one Delta DataFusion provider scan.
///
/// These limits apply to provider-scheduled physical Delta file reads. The
/// current official-kernel sync fallback limits a conservative file-level
/// handoff: reading one file and sending its batches into the bounded
/// DataFusion output channel. A native async reader should preserve these
/// active-read semantics while enforcing them with an async semaphore-style
/// limiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaProviderScanExecutionOptions {
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
}

/// Sync fallback limiter for provider-scheduled file work in one Delta scan.
///
/// This limiter exists for the official-kernel synchronous iterator bridge.
/// A native async reader should replace this implementation at the same
/// scheduling boundary with an async permit implementation such as a
/// `tokio::sync::Semaphore`-backed limiter.
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
            max_concurrent_file_reads_per_scan: 1,
            max_concurrent_file_reads_per_partition: 1,
        }
    }
}

impl DeltaProviderScanExecutionOptions {
    /// Builds validated Delta provider scan execution options.
    pub fn try_new(
        max_concurrent_file_reads_per_scan: usize,
        max_concurrent_file_reads_per_partition: usize,
    ) -> Result<Self, DeltaFunnelError> {
        let options = Self {
            max_concurrent_file_reads_per_scan,
            max_concurrent_file_reads_per_partition,
        };
        options.validate()?;
        Ok(options)
    }

    /// Validates provider scan execution bounds before registration or scan execution.
    pub fn validate(&self) -> Result<(), DeltaFunnelError> {
        validate_positive(
            "max_concurrent_file_reads_per_scan",
            self.max_concurrent_file_reads_per_scan,
        )?;
        validate_positive(
            "max_concurrent_file_reads_per_partition",
            self.max_concurrent_file_reads_per_partition,
        )?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::{DeltaProviderScanExecutionOptions, DeltaProviderSyncReadLimiter};

    #[test]
    fn default_execution_options_are_valid() -> Result<(), Box<dyn std::error::Error>> {
        DeltaProviderScanExecutionOptions::default().validate()?;

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
}
