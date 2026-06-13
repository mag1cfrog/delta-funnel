//! Bounded scheduling options for Delta scan execution.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::DeltaFunnelError;

/// Provider-local limits for Delta scan read scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeltaProviderExecutionOptions {
    /// Maximum provider file reads admitted across one physical scan.
    pub(crate) read_parallelism: usize,
    /// Maximum provider file reads admitted by one execution partition.
    pub(crate) max_partition_read_parallelism: usize,
    /// Maximum provider files that may be active or queued for one scan.
    pub(crate) max_in_flight_files: usize,
}

/// Sync fallback limiter for provider-scheduled file work in one Delta scan.
///
/// This limiter exists for the official-kernel synchronous iterator bridge.
/// A native async reader should replace this implementation at the same
/// scheduling boundary with an async permit implementation such as a
/// `tokio::sync::Semaphore`-backed limiter.
pub(crate) struct DeltaProviderSyncReadLimiter {
    options: DeltaProviderExecutionOptions,
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
    pub(crate) fn new(options: DeltaProviderExecutionOptions, partition_count: usize) -> Arc<Self> {
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
        let global_limit = self
            .options
            .read_parallelism
            .min(self.options.max_in_flight_files);
        state.active_file_reads < global_limit
            && state.partition_active_file_reads[partition]
                < self.options.max_partition_read_parallelism
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
}

impl Drop for DeltaProviderSyncFileReadPermit {
    fn drop(&mut self) {
        if !self.released {
            self.limiter.release_file_permit(self.partition);
            self.released = true;
        }
    }
}

impl Default for DeltaProviderExecutionOptions {
    fn default() -> Self {
        Self {
            read_parallelism: 1,
            max_partition_read_parallelism: 1,
            max_in_flight_files: 1,
        }
    }
}

impl DeltaProviderExecutionOptions {
    #[allow(dead_code)]
    pub(crate) fn try_new(
        read_parallelism: usize,
        max_partition_read_parallelism: usize,
        max_in_flight_files: usize,
    ) -> Result<Self, DeltaFunnelError> {
        let options = Self {
            read_parallelism,
            max_partition_read_parallelism,
            max_in_flight_files,
        };
        options.validate()?;
        Ok(options)
    }

    pub(crate) fn validate(&self) -> Result<(), DeltaFunnelError> {
        validate_positive("read_parallelism", self.read_parallelism)?;
        validate_positive(
            "max_partition_read_parallelism",
            self.max_partition_read_parallelism,
        )?;
        validate_positive("max_in_flight_files", self.max_in_flight_files)?;
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
    use super::{DeltaProviderExecutionOptions, DeltaProviderSyncReadLimiter};

    #[test]
    fn default_execution_options_are_valid() -> Result<(), Box<dyn std::error::Error>> {
        DeltaProviderExecutionOptions::default().validate()?;

        Ok(())
    }

    #[test]
    fn execution_options_reject_zero_read_parallelism() {
        let error = DeltaProviderExecutionOptions::try_new(0, 1, 1)
            .expect_err("zero read_parallelism must fail");

        assert_eq!(
            error.to_string(),
            "configuration error: read_parallelism must be greater than zero"
        );
    }

    #[test]
    fn execution_options_reject_zero_partition_read_parallelism() {
        let error = DeltaProviderExecutionOptions::try_new(1, 0, 1)
            .expect_err("zero max_partition_read_parallelism must fail");

        assert_eq!(
            error.to_string(),
            "configuration error: max_partition_read_parallelism must be greater than zero"
        );
    }

    #[test]
    fn execution_options_reject_zero_in_flight_files() {
        let error = DeltaProviderExecutionOptions::try_new(1, 1, 0)
            .expect_err("zero max_in_flight_files must fail");

        assert_eq!(
            error.to_string(),
            "configuration error: max_in_flight_files must be greater than zero"
        );
    }

    #[test]
    fn sync_file_permit_releases_on_drop() -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderExecutionOptions::default(), 1);
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
            DeltaProviderSyncReadLimiter::new(DeltaProviderExecutionOptions::default(), 1);
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
}
