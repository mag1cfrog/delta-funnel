//! Private bounded scan scheduling primitives.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_util::future::BoxFuture;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

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

#[derive(Clone)]
pub(crate) struct ScanCancellation {
    inner: Arc<ScanCancellationInner>,
}

struct ScanCancellationInner {
    cancelled: AtomicBool,
    notify: Notify,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FileAdmission {
    Admit,
    Skip,
}

type Admission<Task> = Arc<dyn Fn(&Task) -> Result<FileAdmission, DeltaReaderError> + Send + Sync>;
type FileExecutor<Task, Output> = Arc<
    dyn Fn(
            Task,
            FileReadPermit,
            ScanCancellation,
        ) -> BoxFuture<'static, Result<Output, DeltaReaderError>>
        + Send
        + Sync,
>;
pub(crate) type ScheduledFile<Output> =
    BoxFuture<'static, Result<Option<Output>, DeltaReaderError>>;

pub(crate) struct FileScheduler<Task, Output> {
    file_tasks: VecDeque<Task>,
    partition_limiter: PartitionReadLimiter,
    admission: Admission<Task>,
    executor: FileExecutor<Task, Output>,
    cancellation: ScanCancellation,
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

    async fn acquire_until_cancelled(
        &self,
        cancellation: &ScanCancellation,
    ) -> Result<FileReadPermit, DeltaReaderError> {
        let partition = acquire_until_cancelled(
            Arc::clone(&self.limiter.partition_permits[self.partition]),
            cancellation,
            "partition_read_capacity_closed",
        )
        .await?;
        let scan = acquire_until_cancelled(
            Arc::clone(&self.limiter.scan_permits),
            cancellation,
            "scan_read_capacity_closed",
        )
        .await?;
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

impl ScanCancellation {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(ScanCancellationInner {
                cancelled: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    pub(crate) fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::AcqRel) {
            self.inner.notify.notify_waiters();
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = self.inner.notify.notified();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

impl<Task, Output> FileScheduler<Task, Output>
where
    Task: Send + 'static,
    Output: Send + 'static,
{
    pub(crate) fn new(
        file_tasks: Vec<Task>,
        partition_limiter: PartitionReadLimiter,
        admission: Admission<Task>,
        executor: FileExecutor<Task, Output>,
        cancellation: ScanCancellation,
    ) -> Self {
        Self {
            file_tasks: file_tasks.into(),
            partition_limiter,
            admission,
            executor,
            cancellation,
        }
    }

    pub(crate) fn schedule_next(&mut self) -> Option<ScheduledFile<Output>> {
        let task = self.file_tasks.pop_front()?;
        let limiter = self.partition_limiter.clone();
        let admission = Arc::clone(&self.admission);
        let executor = Arc::clone(&self.executor);
        let cancellation = self.cancellation.clone();

        Some(Box::pin(async move {
            if cancellation.is_cancelled() {
                return CancelledSnafu {
                    reason: "scan_execution_cancelled",
                }
                .fail();
            }
            match admission(&task) {
                Ok(FileAdmission::Skip) => return Ok(None),
                Ok(FileAdmission::Admit) => {}
                Err(error) => {
                    cancellation.cancel();
                    return Err(error);
                }
            }

            let permit = limiter.acquire_until_cancelled(&cancellation).await?;
            match executor(task, permit, cancellation.clone()).await {
                Ok(output) => Ok(Some(output)),
                Err(error) => {
                    cancellation.cancel();
                    Err(error)
                }
            }
        }))
    }

    #[cfg(test)]
    fn remaining_file_tasks(&self) -> usize {
        self.file_tasks.len()
    }
}

async fn acquire_until_cancelled(
    permits: Arc<Semaphore>,
    cancellation: &ScanCancellation,
    reason: &'static str,
) -> Result<OwnedSemaphorePermit, DeltaReaderError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => CancelledSnafu { reason: "scan_execution_cancelled" }.fail(),
        permit = permits.acquire_owned() => permit.map_err(|_| CancelledSnafu { reason }.build()),
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::Poll;

    use futures_util::FutureExt;

    use crate::{DeltaReaderExecutionOptions, DeltaReaderPhase, error::InvalidConfigurationSnafu};

    use super::{FileAdmission, FileScheduler, ScanCancellation, ScanReadLimiter};

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

    #[tokio::test]
    async fn file_scheduling_is_lazy_and_runs_admission_before_permits_and_executor()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let admission_calls = Arc::new(AtomicUsize::new(0));
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let mut scheduler = FileScheduler::new(
            vec![7],
            limiter.partition(0)?,
            {
                let calls = Arc::clone(&admission_calls);
                Arc::new(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(FileAdmission::Admit)
                })
            },
            {
                let calls = Arc::clone(&executor_calls);
                Arc::new(move |task, permit, _| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let _permit = permit;
                        Ok(task)
                    }
                    .boxed()
                })
            },
            ScanCancellation::new(),
        );

        let scheduled = scheduler.schedule_next().ok_or("expected scheduled file")?;
        assert_eq!(admission_calls.load(Ordering::SeqCst), 0);
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(limiter.active_file_reads(), 0);

        assert_eq!(scheduled.await?, Some(7));
        assert_eq!(admission_calls.load(Ordering::SeqCst), 1);
        assert_eq!(executor_calls.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.active_file_reads(), 0);
        assert_eq!(scheduler.remaining_file_tasks(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn skipped_and_failed_admission_start_no_capacity_or_executor()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let mut scheduler = FileScheduler::new(
            vec![1, 2],
            limiter.partition(0)?,
            Arc::new(|task| {
                if *task == 1 {
                    Ok(FileAdmission::Skip)
                } else {
                    Err(InvalidConfigurationSnafu {
                        reason: "fake_admission_failure",
                    }
                    .build())
                }
            }),
            {
                let calls = Arc::clone(&executor_calls);
                Arc::new(move |task, permit, _| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let _permit = permit;
                        Ok(task)
                    }
                    .boxed()
                })
            },
            cancellation.clone(),
        );

        assert_eq!(
            scheduler.schedule_next().ok_or("expected skip")?.await?,
            None
        );
        let error = scheduler
            .schedule_next()
            .ok_or("expected failure")?
            .await
            .expect_err("admission must fail");
        assert_eq!(error.as_str(), "invalid_configuration");
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(limiter.active_file_reads(), 0);
        assert!(cancellation.is_cancelled());
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_interrupts_pending_capacity_without_starting_executor()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let partition = limiter.partition(0)?;
        let held = partition.acquire().await?;
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let mut scheduler = FileScheduler::new(
            vec![1],
            partition,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            {
                let calls = Arc::clone(&executor_calls);
                Arc::new(move |task, permit, _| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let _permit = permit;
                        Ok(task)
                    }
                    .boxed()
                })
            },
            cancellation.clone(),
        );
        let mut scheduled = scheduler.schedule_next().ok_or("expected scheduled file")?;
        poll_fn(|context| {
            assert!(matches!(scheduled.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;

        cancellation.cancel();
        let error = scheduled.await.expect_err("cancelled capacity must fail");
        assert_eq!(error.phase(), DeltaReaderPhase::Execution);
        assert_eq!(error.as_str(), "cancelled");
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(limiter.active_file_reads(), 1);

        drop(held);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn executor_failure_is_first_and_cancels_future_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let admission_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let mut scheduler: FileScheduler<i32, ()> = FileScheduler::new(
            vec![1, 2],
            limiter.partition(0)?,
            {
                let calls = Arc::clone(&admission_calls);
                Arc::new(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(FileAdmission::Admit)
                })
            },
            Arc::new(|_, permit, _| {
                async move {
                    let _permit = permit;
                    Err(InvalidConfigurationSnafu {
                        reason: "fake_executor_failure",
                    }
                    .build())
                }
                .boxed()
            }),
            cancellation.clone(),
        );

        let first = scheduler
            .schedule_next()
            .ok_or("expected first file")?
            .await
            .expect_err("executor must fail");
        assert_eq!(first.as_str(), "invalid_configuration");
        assert!(cancellation.is_cancelled());
        assert_eq!(limiter.active_file_reads(), 0);

        let later = scheduler
            .schedule_next()
            .ok_or("expected later file")?
            .await
            .expect_err("later work must observe cancellation");
        assert_eq!(later.as_str(), "cancelled");
        assert_eq!(admission_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
