//! Private bounded scan scheduling primitives.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    DeltaReaderError, DeltaReaderExecutionOptions,
    error::{CancelledSnafu, InvalidConfigurationSnafu},
};

pub(crate) struct ScanReadLimiter {
    scan_capacity: usize,
    partition_capacity: usize,
    scan_permits: Arc<Semaphore>,
    partition_permits: Vec<Arc<Semaphore>>,
}

#[derive(Clone)]
pub(crate) struct PartitionReadLimiter {
    partition: usize,
    limiter: Arc<ScanReadLimiter>,
}

pub(crate) struct FileReadPermit {
    _partition: OwnedSemaphorePermit,
    _scan: OwnedSemaphorePermit,
}

impl ScanReadLimiter {
    pub(crate) fn new(
        options: DeltaReaderExecutionOptions,
        target_partitions: usize,
        partition_count: usize,
    ) -> Arc<Self> {
        let scan_capacity = options.resolved_max_concurrent_file_reads_per_scan(target_partitions);
        let partition_capacity = options.max_concurrent_file_reads_per_partition();
        Arc::new(Self {
            scan_capacity,
            partition_capacity,
            scan_permits: Arc::new(Semaphore::new(scan_capacity)),
            partition_permits: (0..partition_count)
                .map(|_| Arc::new(Semaphore::new(partition_capacity)))
                .collect(),
        })
    }

    pub(crate) fn partition(
        self: &Arc<Self>,
        partition: usize,
    ) -> Result<PartitionReadLimiter, DeltaReaderError> {
        if partition >= self.partition_permits.len() {
            return InvalidConfigurationSnafu {
                reason: "scan_partition_index_out_of_range",
            }
            .fail();
        }
        Ok(PartitionReadLimiter {
            partition,
            limiter: Arc::clone(self),
        })
    }

    #[cfg(test)]
    fn active_file_reads(&self) -> usize {
        self.scan_capacity
            .saturating_sub(self.scan_permits.available_permits())
    }

    #[cfg(test)]
    fn partition_active_file_reads(&self, partition: usize) -> Option<usize> {
        self.partition_permits.get(partition).map(|permits| {
            self.partition_capacity
                .saturating_sub(permits.available_permits())
        })
    }
}

impl PartitionReadLimiter {
    pub(crate) async fn acquire(&self) -> Result<FileReadPermit, DeltaReaderError> {
        let partition = Arc::clone(&self.limiter.partition_permits[self.partition])
            .acquire_owned()
            .await
            .map_err(|_| {
                CancelledSnafu {
                    reason: "partition_read_capacity_closed",
                }
                .build()
            })?;
        let scan = Arc::clone(&self.limiter.scan_permits)
            .acquire_owned()
            .await
            .map_err(|_| {
                CancelledSnafu {
                    reason: "scan_read_capacity_closed",
                }
                .build()
            })?;
        Ok(FileReadPermit {
            _partition: partition,
            _scan: scan,
        })
    }

    #[cfg(test)]
    fn try_acquire(&self) -> Option<FileReadPermit> {
        let partition = Arc::clone(&self.limiter.partition_permits[self.partition])
            .try_acquire_owned()
            .ok()?;
        let scan = Arc::clone(&self.limiter.scan_permits)
            .try_acquire_owned()
            .ok()?;
        Some(FileReadPermit {
            _partition: partition,
            _scan: scan,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::task::Poll;

    use crate::{DeltaReaderExecutionOptions, DeltaReaderPhase};

    use super::ScanReadLimiter;

    fn options(
        scan_capacity: usize,
        partition_capacity: usize,
    ) -> Result<DeltaReaderExecutionOptions, crate::DeltaReaderError> {
        DeltaReaderExecutionOptions::new()
            .with_native_async_prefetch_file_count_per_partition(0)?
            .with_max_concurrent_file_reads_per_partition(partition_capacity)?
            .with_max_concurrent_file_reads_per_scan(Some(scan_capacity))
    }

    #[tokio::test]
    async fn permits_enforce_scan_and_partition_capacity_and_release_on_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 1)?, 2, 2);
        let first = limiter.partition(0)?;
        let second = limiter.partition(1)?;

        let first_permit = first.acquire().await?;
        assert!(first.try_acquire().is_none());
        let second_permit = second.acquire().await?;
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
    async fn waiting_partition_does_not_reserve_scan_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 1)?, 2, 2);
        let first = limiter.partition(0)?;
        let second = limiter.partition(1)?;
        let first_permit = first.acquire().await?;
        let mut waiting = Box::pin(first.acquire());
        poll_fn(|context| {
            assert!(matches!(waiting.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;

        let second_permit = second.try_acquire().ok_or("scan capacity was reserved")?;
        assert_eq!(limiter.active_file_reads(), 2);

        drop(waiting);
        drop(first_permit);
        drop(second_permit);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn scan_capacity_and_partition_index_use_fixed_plan_inputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(DeltaReaderExecutionOptions::new(), 2, 1);
        assert_eq!(limiter.scan_capacity, 6);
        assert_eq!(limiter.partition_capacity, 3);

        let error = match limiter.partition(1) {
            Ok(_) => return Err("out-of-range partition must fail".into()),
            Err(error) => error,
        };
        assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
        assert_eq!(error.as_str(), "invalid_configuration");
        assert!(!error.to_string().contains('1'));
        Ok(())
    }
}
