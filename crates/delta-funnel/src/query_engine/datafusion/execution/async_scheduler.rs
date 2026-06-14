//! Lazy native async file-read scheduling boundary.
//!
//! The scheduler is demand-driven: each `next_file` call admits at most one
//! file after async read permits are acquired. It does not prebuild per-file
//! futures or buffer completed output internally.

use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use crate::DeltaFunnelError;

use super::scheduling::{DeltaProviderAsyncFileReadPermit, DeltaProviderAsyncPartitionReadLimiter};

/// Native async scheduler output buffer capacity.
///
/// The internal scheduler is demand-driven and has no completed-output buffer.
/// A later DataFusion stream adapter must document its own handoff capacity.
#[allow(dead_code)]
pub(crate) const DELTA_PROVIDER_ASYNC_SCHEDULER_OUTPUT_CAPACITY: usize = 0;

/// Future returned by a native async file reader.
#[allow(dead_code)]
pub(crate) type DeltaProviderAsyncFileReadFuture<Output> =
    Pin<Box<dyn Future<Output = Result<Output, DeltaFunnelError>> + Send>>;

/// Native async file reader used by the lazy scheduler.
#[allow(dead_code)]
pub(crate) trait DeltaProviderAsyncFileReader<Task, Output>: Send + Sync {
    /// Starts one file read after the scheduler has acquired read permits.
    fn read_file(
        &self,
        task: Task,
        permit: DeltaProviderAsyncFileReadPermit,
    ) -> DeltaProviderAsyncFileReadFuture<Output>;
}

/// Configuration for one execution partition's native async read scheduler.
#[allow(dead_code)]
pub(crate) struct DeltaProviderAsyncPartitionReadSchedulerConfig<Task, Output, Reader>
where
    Reader: DeltaProviderAsyncFileReader<Task, Output> + ?Sized,
{
    /// Planned file tasks for one DataFusion execution partition.
    pub(crate) file_tasks: Vec<Task>,
    /// Reader backend used for native async file work.
    pub(crate) reader: Arc<Reader>,
    /// Partition-local limiter view for this execution partition.
    pub(crate) partition_limiter: DeltaProviderAsyncPartitionReadLimiter,
    _output: PhantomData<Output>,
}

#[allow(dead_code)]
impl<Task, Output, Reader> DeltaProviderAsyncPartitionReadSchedulerConfig<Task, Output, Reader>
where
    Reader: DeltaProviderAsyncFileReader<Task, Output> + ?Sized,
{
    /// Builds scheduler configuration from planned file tasks and backend state.
    pub(crate) fn new(
        file_tasks: Vec<Task>,
        reader: Arc<Reader>,
        partition_limiter: DeltaProviderAsyncPartitionReadLimiter,
    ) -> Self {
        Self {
            file_tasks,
            reader,
            partition_limiter,
            _output: PhantomData,
        }
    }
}

/// Lazy native async scheduler for one DataFusion execution partition.
#[allow(dead_code)]
pub(crate) struct DeltaProviderAsyncPartitionReadScheduler<Task, Output, Reader>
where
    Reader: DeltaProviderAsyncFileReader<Task, Output> + ?Sized,
{
    file_tasks: VecDeque<Task>,
    reader: Arc<Reader>,
    partition_limiter: DeltaProviderAsyncPartitionReadLimiter,
    admitted_file_tasks: usize,
    _output: PhantomData<Output>,
}

#[allow(dead_code)]
impl<Task, Output, Reader> DeltaProviderAsyncPartitionReadScheduler<Task, Output, Reader>
where
    Reader: DeltaProviderAsyncFileReader<Task, Output> + ?Sized,
{
    /// Builds a lazy native async scheduler for one execution partition.
    pub(crate) fn new(
        config: DeltaProviderAsyncPartitionReadSchedulerConfig<Task, Output, Reader>,
    ) -> Self {
        Self {
            file_tasks: VecDeque::from(config.file_tasks),
            reader: config.reader,
            partition_limiter: config.partition_limiter,
            admitted_file_tasks: 0,
            _output: PhantomData,
        }
    }

    /// Returns the next completed file-read output.
    ///
    /// This method acquires permits before popping the next task and creating a
    /// read future. Dropping this future cancels the current admission attempt
    /// or in-flight file read and releases any acquired permits.
    pub(crate) async fn next_file(&mut self) -> Option<Result<Output, DeltaFunnelError>> {
        if self.file_tasks.is_empty() {
            return None;
        }

        let permit = match self.partition_limiter.acquire_file_permit().await {
            Ok(permit) => permit,
            Err(error) => return Some(Err(error)),
        };
        let Some(task) = self.file_tasks.pop_front() else {
            return None;
        };
        self.admitted_file_tasks = self.admitted_file_tasks.saturating_add(1);

        Some(self.reader.read_file(task, permit).await)
    }

    /// Planned file tasks not yet admitted to a read future.
    pub(crate) fn remaining_file_tasks(&self) -> usize {
        self.file_tasks.len()
    }

    /// File tasks admitted after permits were acquired.
    pub(crate) fn admitted_file_tasks(&self) -> usize {
        self.admitted_file_tasks
    }

    /// Internal completed-output buffer capacity for this scheduler.
    pub(crate) fn output_capacity(&self) -> usize {
        DELTA_PROVIDER_ASYNC_SCHEDULER_OUTPUT_CAPACITY
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{
        DELTA_PROVIDER_ASYNC_SCHEDULER_OUTPUT_CAPACITY, DeltaProviderAsyncFileReadFuture,
        DeltaProviderAsyncFileReader, DeltaProviderAsyncPartitionReadScheduler,
        DeltaProviderAsyncPartitionReadSchedulerConfig,
    };
    use crate::DeltaFunnelError;
    use crate::query_engine::datafusion::execution::scheduling::{
        DeltaProviderAsyncFileReadPermit, DeltaProviderAsyncReadLimiter,
        DeltaProviderScanExecutionOptions,
    };

    #[derive(Clone, Copy)]
    struct FakeFileTask {
        id: usize,
    }

    struct CountingAsyncFileReader {
        futures_created: Arc<AtomicUsize>,
        reads_started: Arc<AtomicUsize>,
    }

    impl CountingAsyncFileReader {
        fn new() -> Self {
            Self {
                futures_created: Arc::new(AtomicUsize::new(0)),
                reads_started: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn futures_created(&self) -> usize {
            self.futures_created.load(Ordering::SeqCst)
        }

        fn reads_started(&self) -> usize {
            self.reads_started.load(Ordering::SeqCst)
        }
    }

    impl DeltaProviderAsyncFileReader<FakeFileTask, usize> for CountingAsyncFileReader {
        fn read_file(
            &self,
            task: FakeFileTask,
            permit: DeltaProviderAsyncFileReadPermit,
        ) -> DeltaProviderAsyncFileReadFuture<usize> {
            self.futures_created.fetch_add(1, Ordering::SeqCst);
            let reads_started = Arc::clone(&self.reads_started);
            Box::pin(async move {
                let _permit = permit;
                reads_started.fetch_add(1, Ordering::SeqCst);
                Ok(task.id)
            })
        }
    }

    struct PendingAsyncFileReader {
        futures_created: Arc<AtomicUsize>,
        reads_started: Arc<AtomicUsize>,
    }

    impl PendingAsyncFileReader {
        fn new() -> Self {
            Self {
                futures_created: Arc::new(AtomicUsize::new(0)),
                reads_started: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn futures_created(&self) -> usize {
            self.futures_created.load(Ordering::SeqCst)
        }

        fn reads_started(&self) -> usize {
            self.reads_started.load(Ordering::SeqCst)
        }
    }

    impl DeltaProviderAsyncFileReader<FakeFileTask, usize> for PendingAsyncFileReader {
        fn read_file(
            &self,
            _task: FakeFileTask,
            permit: DeltaProviderAsyncFileReadPermit,
        ) -> DeltaProviderAsyncFileReadFuture<usize> {
            self.futures_created.fetch_add(1, Ordering::SeqCst);
            let reads_started = Arc::clone(&self.reads_started);
            Box::pin(async move {
                let _permit = permit;
                reads_started.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<usize, DeltaFunnelError>>().await
            })
        }
    }

    #[tokio::test]
    async fn scheduler_has_no_internal_output_buffer() -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderAsyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let partition = limiter.partition_limiter(0)?;
        let reader = Arc::new(CountingAsyncFileReader::new());
        let scheduler = DeltaProviderAsyncPartitionReadScheduler::new(
            DeltaProviderAsyncPartitionReadSchedulerConfig::new(
                vec![FakeFileTask { id: 1 }],
                reader,
                partition,
            ),
        );

        assert_eq!(scheduler.output_capacity(), 0);
        assert_eq!(DELTA_PROVIDER_ASYNC_SCHEDULER_OUTPUT_CAPACITY, 0);

        Ok(())
    }

    #[tokio::test]
    async fn scheduler_does_not_create_file_future_before_polling()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderAsyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let partition = limiter.partition_limiter(0)?;
        let reader = Arc::new(CountingAsyncFileReader::new());
        let mut scheduler = DeltaProviderAsyncPartitionReadScheduler::new(
            DeltaProviderAsyncPartitionReadSchedulerConfig::new(
                vec![FakeFileTask { id: 1 }],
                Arc::clone(&reader),
                partition,
            ),
        );

        let next = Box::pin(scheduler.next_file());

        assert_eq!(reader.futures_created(), 0);
        assert_eq!(reader.reads_started(), 0);

        drop(next);

        assert_eq!(scheduler.admitted_file_tasks(), 0);
        assert_eq!(scheduler.remaining_file_tasks(), 1);
        assert_eq!(limiter.active_file_reads(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn scheduler_creates_one_file_future_per_downstream_demand()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderAsyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let partition = limiter.partition_limiter(0)?;
        let reader = Arc::new(CountingAsyncFileReader::new());
        let mut scheduler = DeltaProviderAsyncPartitionReadScheduler::new(
            DeltaProviderAsyncPartitionReadSchedulerConfig::new(
                vec![
                    FakeFileTask { id: 1 },
                    FakeFileTask { id: 2 },
                    FakeFileTask { id: 3 },
                ],
                Arc::clone(&reader),
                partition,
            ),
        );

        assert_eq!(scheduler.remaining_file_tasks(), 3);
        assert_eq!(scheduler.admitted_file_tasks(), 0);
        assert_eq!(reader.futures_created(), 0);

        let first = scheduler
            .next_file()
            .await
            .ok_or("expected first file output")??;

        assert_eq!(first, 1);
        assert_eq!(scheduler.remaining_file_tasks(), 2);
        assert_eq!(scheduler.admitted_file_tasks(), 1);
        assert_eq!(reader.futures_created(), 1);
        assert_eq!(reader.reads_started(), 1);
        assert_eq!(limiter.active_file_reads(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn slow_downstream_demand_bounds_admitted_work() -> Result<(), Box<dyn std::error::Error>>
    {
        let limiter =
            DeltaProviderAsyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let partition = limiter.partition_limiter(0)?;
        let reader = Arc::new(CountingAsyncFileReader::new());
        let mut scheduler = DeltaProviderAsyncPartitionReadScheduler::new(
            DeltaProviderAsyncPartitionReadSchedulerConfig::new(
                vec![
                    FakeFileTask { id: 1 },
                    FakeFileTask { id: 2 },
                    FakeFileTask { id: 3 },
                ],
                Arc::clone(&reader),
                partition,
            ),
        );

        let first = scheduler
            .next_file()
            .await
            .ok_or("expected first file output")??;

        assert_eq!(first, 1);
        assert_eq!(scheduler.admitted_file_tasks(), 1);
        assert_eq!(scheduler.remaining_file_tasks(), 2);
        assert_eq!(reader.futures_created(), 1);
        assert_eq!(reader.reads_started(), 1);
        assert_eq!(limiter.active_file_reads(), 0);

        tokio::task::yield_now().await;

        assert_eq!(scheduler.admitted_file_tasks(), 1);
        assert_eq!(scheduler.remaining_file_tasks(), 2);
        assert_eq!(reader.futures_created(), 1);
        assert_eq!(reader.reads_started(), 1);
        assert_eq!(limiter.active_file_reads(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn scheduler_does_not_admit_file_work_before_permits()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderAsyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let partition = limiter.partition_limiter(0)?;
        let held_permit = partition.acquire_file_permit().await?;
        let reader = Arc::new(CountingAsyncFileReader::new());
        let mut scheduler = DeltaProviderAsyncPartitionReadScheduler::new(
            DeltaProviderAsyncPartitionReadSchedulerConfig::new(
                vec![FakeFileTask { id: 1 }],
                Arc::clone(&reader),
                partition,
            ),
        );
        let mut next = Box::pin(scheduler.next_file());

        tokio::select! {
            output = &mut next => {
                return Err(format!("file read should wait for permits: {output:?}").into());
            }
            () = tokio::task::yield_now() => {}
        }

        assert_eq!(reader.futures_created(), 0);
        assert_eq!(reader.reads_started(), 0);
        assert_eq!(limiter.active_file_reads(), 1);

        drop(next);
        drop(held_permit);

        assert_eq!(scheduler.admitted_file_tasks(), 0);
        assert_eq!(scheduler.remaining_file_tasks(), 1);
        assert_eq!(limiter.active_file_reads(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn dropping_in_flight_scheduler_future_releases_permits()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderAsyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let partition = limiter.partition_limiter(0)?;
        let reader = Arc::new(PendingAsyncFileReader::new());
        let mut scheduler = DeltaProviderAsyncPartitionReadScheduler::new(
            DeltaProviderAsyncPartitionReadSchedulerConfig::new(
                vec![FakeFileTask { id: 1 }],
                Arc::clone(&reader),
                partition,
            ),
        );
        let mut next = Box::pin(scheduler.next_file());

        tokio::select! {
            output = &mut next => {
                return Err(format!("pending file read should not complete: {output:?}").into());
            }
            () = tokio::task::yield_now() => {}
        }

        assert_eq!(reader.futures_created(), 1);
        assert_eq!(reader.reads_started(), 1);
        assert_eq!(limiter.active_file_reads(), 1);

        drop(next);

        assert_eq!(limiter.active_file_reads(), 0);
        assert_eq!(scheduler.admitted_file_tasks(), 1);
        assert_eq!(scheduler.remaining_file_tasks(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn cancellation_stops_future_file_scheduling_without_draining_prefetch()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter =
            DeltaProviderAsyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let partition = limiter.partition_limiter(0)?;
        let reader = Arc::new(PendingAsyncFileReader::new());
        let mut scheduler = DeltaProviderAsyncPartitionReadScheduler::new(
            DeltaProviderAsyncPartitionReadSchedulerConfig::new(
                vec![FakeFileTask { id: 1 }, FakeFileTask { id: 2 }],
                Arc::clone(&reader),
                partition,
            ),
        );
        let mut next = Box::pin(scheduler.next_file());

        tokio::select! {
            output = &mut next => {
                return Err(format!("pending file read should not complete: {output:?}").into());
            }
            () = tokio::task::yield_now() => {}
        }

        assert_eq!(reader.futures_created(), 1);
        assert_eq!(reader.reads_started(), 1);
        assert_eq!(limiter.active_file_reads(), 1);

        drop(next);
        tokio::task::yield_now().await;

        assert_eq!(reader.futures_created(), 1);
        assert_eq!(reader.reads_started(), 1);
        assert_eq!(scheduler.admitted_file_tasks(), 1);
        assert_eq!(scheduler.remaining_file_tasks(), 1);
        assert_eq!(limiter.active_file_reads(), 0);

        Ok(())
    }

    #[test]
    fn async_scheduler_source_avoids_blocking_runtime_patterns() {
        let source = include_str!("async_scheduler.rs");
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
                !source.contains(pattern),
                "async scheduler source must not contain {pattern}"
            );
        }
    }

    #[test]
    fn async_limiter_dependency_is_direct_production_dependency()
    -> Result<(), Box<dyn std::error::Error>> {
        let manifest = include_str!("../../../../Cargo.toml");
        let dependency_line = direct_dependency_line(manifest, "tokio")
            .ok_or("expected tokio direct dependency in production dependencies")?;

        assert!(dependency_line.contains("features"));
        assert!(dependency_line.contains("\"sync\""));

        Ok(())
    }

    fn direct_dependency_line<'a>(manifest: &'a str, dependency: &str) -> Option<&'a str> {
        let mut in_dependency_section = false;

        for line in manifest.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                in_dependency_section = trimmed == "[dependencies]";
                continue;
            }
            if !in_dependency_section {
                continue;
            }
            if trimmed.starts_with(&format!("{dependency} =")) {
                return Some(trimmed);
            }
        }

        None
    }
}
