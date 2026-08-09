//! Private bounded scan scheduling primitives.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use arrow::record_batch::RecordBatch;
use futures_util::{Stream, StreamExt, future::BoxFuture, stream::FuturesOrdered};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
};

use crate::{
    DeltaReadMetrics, DeltaReaderBackend, DeltaReaderError, DeltaReaderExecutionOptions,
    error::{CancelledSnafu, InvalidConfigurationSnafu},
    planning::{DeltaScanFileTask, DeltaScanPlan},
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

pub(crate) type FileAdmissionFn<Task> =
    Arc<dyn Fn(&Task) -> Result<FileAdmission, DeltaReaderError> + Send + Sync>;
/// Starts one admitted file while retaining its permit in the returned producer.
/// Async producers stop at cancellation boundaries. Blocking adapters may finish
/// their current safe handoff, but must not start later work after cancellation.
pub(crate) type FileExecutor<Task, Output> = Arc<
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
pub(crate) type FileBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, DeltaReaderError>> + Send + 'static>>;

pub(crate) struct FileScheduler<Task, Output> {
    file_tasks: VecDeque<Task>,
    partition_limiter: PartitionReadLimiter,
    admission: FileAdmissionFn<Task>,
    executor: FileExecutor<Task, Output>,
    cancellation: ScanCancellation,
}

type BatchResult = Result<RecordBatch, DeltaReaderError>;
type StartPartition = Box<dyn FnOnce(mpsc::Sender<BatchResult>) -> JoinHandle<()> + Send>;
type FileSetups = FuturesOrdered<ScheduledFile<FileBatchStream>>;
type ReadyFiles = VecDeque<Result<FileBatchStream, DeltaReaderError>>;

struct PartitionStart {
    output_capacity: usize,
    start: StartPartition,
}

enum PartitionStreamState {
    NotStarted(Option<PartitionStart>),
    Running {
        receiver: mpsc::Receiver<BatchResult>,
        task: JoinHandle<()>,
    },
    Finishing(JoinHandle<()>),
    Done,
}

pub(crate) struct PartitionStream {
    state: PartitionStreamState,
    cancellation: ScanCancellation,
    done: bool,
}

pub(crate) struct DeltaScanExecution {
    plan: Arc<DeltaScanPlan>,
    limiter: Arc<ScanReadLimiter>,
    cancellation: ScanCancellation,
}

impl DeltaScanExecution {
    pub(crate) fn new(plan: Arc<DeltaScanPlan>) -> Self {
        let limiter = ScanReadLimiter::new(
            plan.execution_options,
            plan.partition_target_diagnostic.target_partitions,
            plan.partitions.len(),
        );
        Self {
            plan,
            limiter,
            cancellation: ScanCancellation::new(),
        }
    }

    pub(crate) fn partition_stream(
        &self,
        partition: usize,
        admission: FileAdmissionFn<DeltaScanFileTask>,
        executor: FileExecutor<DeltaScanFileTask, FileBatchStream>,
    ) -> Result<PartitionStream, DeltaReaderError> {
        let partition_limiter = self.limiter.partition(partition)?;
        let file_tasks = self.plan.partitions[partition].file_tasks.clone();
        Ok(PartitionStream::new(
            file_tasks,
            partition_limiter,
            self.plan.execution_options,
            admission,
            executor,
            self.plan.metrics.clone(),
            self.cancellation.clone(),
        ))
    }
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

    pub(crate) fn cancel(&self) -> bool {
        let cancelled = !self.inner.cancelled.swap(true, Ordering::AcqRel);
        if cancelled {
            self.inner.notify.notify_waiters();
        }
        cancelled
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
        admission: FileAdmissionFn<Task>,
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
                Err(error) => return Err(error),
            }

            let permit = limiter.acquire_until_cancelled(&cancellation).await?;
            executor(task, permit, cancellation.clone()).await.map(Some)
        }))
    }

    #[cfg(test)]
    fn remaining_file_tasks(&self) -> usize {
        self.file_tasks.len()
    }
}

impl PartitionStream {
    pub(crate) fn new<Task>(
        file_tasks: Vec<Task>,
        partition_limiter: PartitionReadLimiter,
        options: DeltaReaderExecutionOptions,
        admission: FileAdmissionFn<Task>,
        executor: FileExecutor<Task, FileBatchStream>,
        metrics: DeltaReadMetrics,
        cancellation: ScanCancellation,
    ) -> Self
    where
        Task: Send + 'static,
    {
        let output_capacity = options.output_buffer_capacity_per_partition();
        let prefetch_file_count = match options.reader_backend() {
            DeltaReaderBackend::NativeAsync => {
                options.native_async_prefetch_file_count_per_partition()
            }
            DeltaReaderBackend::OfficialKernel => 0,
        };
        let measured_metrics = metrics.clone();
        let measured_executor = Arc::new(move |task, permit, cancellation| {
            measured_metrics.record_file_started();
            executor(task, permit, cancellation)
        });
        let scheduler = FileScheduler::new(
            file_tasks,
            partition_limiter,
            admission,
            measured_executor,
            cancellation.clone(),
        );
        let run_cancellation = cancellation.clone();
        let start = Box::new(move |output| {
            tokio::spawn(run_partition(
                output,
                scheduler,
                metrics,
                run_cancellation,
                prefetch_file_count,
            ))
        });

        Self {
            state: PartitionStreamState::NotStarted(Some(PartitionStart {
                output_capacity,
                start,
            })),
            cancellation,
            done: false,
        }
    }
}

impl Stream for PartitionStream {
    type Item = BatchResult;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                PartitionStreamState::NotStarted(start) => {
                    let Some(start) = start.take() else {
                        self.state = PartitionStreamState::Done;
                        self.done = true;
                        return Poll::Ready(None);
                    };
                    let (output, receiver) = mpsc::channel(start.output_capacity);
                    self.state = PartitionStreamState::Running {
                        receiver,
                        task: (start.start)(output),
                    };
                }
                PartitionStreamState::Running { receiver, .. } => {
                    match receiver.poll_recv(context) {
                        Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                        Poll::Ready(None) => {
                            let state =
                                std::mem::replace(&mut self.state, PartitionStreamState::Done);
                            let PartitionStreamState::Running { task, .. } = state else {
                                unreachable!("partition stream state changed during polling");
                            };
                            self.state = PartitionStreamState::Finishing(task);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                PartitionStreamState::Finishing(task) => match Pin::new(task).poll(context) {
                    Poll::Ready(Ok(())) => {
                        self.state = PartitionStreamState::Done;
                        self.done = true;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Err(_)) => {
                        self.state = PartitionStreamState::Done;
                        self.done = true;
                        return Poll::Ready(self.cancellation.cancel().then(|| {
                            Err(CancelledSnafu {
                                reason: "partition_scheduler_task_failed",
                            }
                            .build())
                        }));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                PartitionStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl Drop for PartitionStream {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        self.cancellation.cancel();
        match &self.state {
            PartitionStreamState::Running { task, .. } | PartitionStreamState::Finishing(task) => {
                task.abort()
            }
            PartitionStreamState::NotStarted(_) | PartitionStreamState::Done => {}
        }
    }
}

async fn run_partition<Task>(
    output: mpsc::Sender<BatchResult>,
    mut scheduler: FileScheduler<Task, FileBatchStream>,
    metrics: DeltaReadMetrics,
    cancellation: ScanCancellation,
    prefetch_file_count: usize,
) where
    Task: Send + 'static,
{
    metrics.record_scan_partition_started();
    let mut in_flight = FuturesOrdered::new();
    let mut ready = VecDeque::new();

    loop {
        let mut file = match take_next_file(
            &mut scheduler,
            &mut in_flight,
            &mut ready,
            prefetch_file_count,
            &cancellation,
        )
        .await
        {
            NextFile::Ready(file) => file,
            NextFile::Exhausted => {
                metrics.record_scan_partition_completed();
                return;
            }
            NextFile::Cancelled => return,
            NextFile::Error(error) => {
                send_first_error(&output, &cancellation, error).await;
                return;
            }
        };
        refill_file_setups(
            &mut scheduler,
            &mut in_flight,
            ready.len(),
            prefetch_file_count,
        );

        match drain_current_file(
            &output,
            &mut file,
            &mut scheduler,
            &mut in_flight,
            &mut ready,
            prefetch_file_count,
            &metrics,
            &cancellation,
        )
        .await
        {
            DrainFile::Completed => metrics.record_file_completed(),
            DrainFile::Cancelled => return,
            DrainFile::Error(error) => {
                send_first_error(&output, &cancellation, error).await;
                return;
            }
        }
    }
}

async fn send_first_error(
    output: &mpsc::Sender<BatchResult>,
    cancellation: &ScanCancellation,
    error: DeltaReaderError,
) {
    if cancellation.cancel() {
        let _ = output.send(Err(error)).await;
    }
}

enum NextFile {
    Ready(FileBatchStream),
    Exhausted,
    Cancelled,
    Error(DeltaReaderError),
}

enum DrainFile {
    Completed,
    Cancelled,
    Error(DeltaReaderError),
}

async fn take_next_file<Task>(
    scheduler: &mut FileScheduler<Task, FileBatchStream>,
    in_flight: &mut FileSetups,
    ready: &mut ReadyFiles,
    prefetch_file_count: usize,
    cancellation: &ScanCancellation,
) -> NextFile
where
    Task: Send + 'static,
{
    loop {
        refill_file_setups(
            scheduler,
            in_flight,
            ready.len(),
            prefetch_file_count.saturating_add(1),
        );
        if let Some(file) = ready.pop_front() {
            return match file {
                Ok(file) => NextFile::Ready(file),
                Err(error) => NextFile::Error(error),
            };
        }
        let file = tokio::select! {
            biased;
            file = in_flight.next() => file,
            () = cancellation.cancelled() => return NextFile::Cancelled,
        };
        match file {
            Some(Ok(Some(file))) => return NextFile::Ready(file),
            Some(Ok(None)) => {}
            Some(Err(error)) => return NextFile::Error(error),
            None => return NextFile::Exhausted,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drain_current_file<Task>(
    output: &mpsc::Sender<BatchResult>,
    file: &mut FileBatchStream,
    scheduler: &mut FileScheduler<Task, FileBatchStream>,
    in_flight: &mut FileSetups,
    ready: &mut ReadyFiles,
    prefetch_file_count: usize,
    metrics: &DeltaReadMetrics,
    cancellation: &ScanCancellation,
) -> DrainFile
where
    Task: Send + 'static,
{
    loop {
        let batch = if ready.is_empty() && !in_flight.is_empty() {
            tokio::select! {
                biased;
                batch = file.next() => Some(batch),
                setup = in_flight.next() => {
                    match setup {
                        Some(Ok(Some(file))) => ready.push_back(Ok(file)),
                        Some(Ok(None)) | None => {}
                        Some(Err(error)) => ready.push_back(Err(error)),
                    }
                    refill_file_setups(
                        scheduler,
                        in_flight,
                        ready.len(),
                        prefetch_file_count,
                    );
                    continue;
                }
                () = cancellation.cancelled() => return DrainFile::Cancelled,
            }
        } else {
            tokio::select! {
                biased;
                batch = file.next() => Some(batch),
                () = cancellation.cancelled() => return DrainFile::Cancelled,
            }
        };
        let Some(batch) = batch.flatten() else {
            return DrainFile::Completed;
        };
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => return DrainFile::Error(error),
        };
        let rows = batch.num_rows();
        let sent = tokio::select! {
            biased;
            () = cancellation.cancelled() => return DrainFile::Cancelled,
            sent = output.send(Ok(batch)) => sent,
        };
        if sent.is_err() {
            cancellation.cancel();
            return DrainFile::Cancelled;
        }
        metrics.record_batch_produced(rows);
    }
}

fn refill_file_setups<Task>(
    scheduler: &mut FileScheduler<Task, FileBatchStream>,
    in_flight: &mut FileSetups,
    ready_count: usize,
    target_file_count: usize,
) where
    Task: Send + 'static,
{
    while in_flight.len().saturating_add(ready_count) < target_file_count {
        let Some(file) = scheduler.schedule_next() else {
            return;
        };
        in_flight.push_back(file);
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
    use std::collections::{BTreeMap, VecDeque};
    use std::future::{pending, poll_fn};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::task::Poll;
    use std::time::Duration;

    use arrow::{
        array::{Array, Int32Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures_util::{FutureExt, StreamExt, stream};
    use tokio::{
        sync::{Barrier, Notify, mpsc},
        time::timeout,
    };

    use crate::{
        DeltaReadMetrics, DeltaReaderBackend, DeltaReaderExecutionOptions, DeltaReaderPhase,
        error::InvalidConfigurationSnafu, metrics::DeltaReadMetricsConfig,
    };

    use super::{
        BatchResult, FileAdmission, FileBatchStream, FileExecutor, FileReadPermit, FileScheduler,
        PartitionStream, PartitionStreamState, ScanCancellation, ScanReadLimiter, send_first_error,
    };

    fn options(
        scan_capacity: usize,
        partition_capacity: usize,
    ) -> Result<DeltaReaderExecutionOptions, crate::DeltaReaderError> {
        DeltaReaderExecutionOptions::new()
            .with_native_async_prefetch_file_count_per_partition(0)?
            .with_max_concurrent_file_reads_per_partition(partition_capacity)?
            .with_max_concurrent_file_reads_per_scan(Some(scan_capacity))
    }

    fn metrics() -> DeltaReadMetrics {
        DeltaReadMetrics::new(DeltaReadMetricsConfig {
            snapshot_version: 1,
            reader_backend: DeltaReaderBackend::NativeAsync,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 1,
            files_planned: 3,
            files_filtered_during_planning: None,
            estimated_rows: Some(3),
            estimated_bytes: Some(3),
        })
    }

    fn stream_options(
        output_capacity: usize,
        prefetch_file_count: usize,
    ) -> Result<DeltaReaderExecutionOptions, crate::DeltaReaderError> {
        DeltaReaderExecutionOptions::new()
            .with_native_async_prefetch_file_count_per_partition(prefetch_file_count)?
            .with_output_buffer_capacity_per_partition(output_capacity)
    }

    fn batch(ids: Vec<i32>) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(ids))],
        )?)
    }

    fn batch_ids(batch: &RecordBatch) -> Result<Vec<i32>, &'static str> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|ids| ids.values().to_vec())
            .ok_or("expected Int32 ids")
    }

    fn file_stream(permit: FileReadPermit, batches: Vec<RecordBatch>) -> FileBatchStream {
        Box::pin(stream::unfold(
            (VecDeque::from(batches), permit),
            |(mut batches, permit)| async move {
                batches
                    .pop_front()
                    .map(|batch| (Ok(batch), (batches, permit)))
            },
        ))
    }

    fn gated_file_stream(
        permit: FileReadPermit,
        batches: Vec<RecordBatch>,
        gate: Arc<Notify>,
    ) -> FileBatchStream {
        Box::pin(stream::unfold(
            (false, VecDeque::from(batches), permit, gate),
            |(wait, mut batches, permit, gate)| async move {
                let batch = batches.pop_front()?;
                if wait {
                    gate.notified().await;
                }
                Some((Ok(batch), (true, batches, permit, gate)))
            },
        ))
    }

    fn pending_file_stream(permit: FileReadPermit) -> FileBatchStream {
        Box::pin(stream::once(async move {
            let _permit = permit;
            pending::<Result<RecordBatch, crate::DeltaReaderError>>().await
        }))
    }

    fn batch_executor(
        batches: BTreeMap<i32, Vec<RecordBatch>>,
        calls: Arc<AtomicUsize>,
    ) -> FileExecutor<i32, FileBatchStream> {
        Arc::new(move |task, permit, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            let batches = batches.get(&task).cloned().unwrap_or_default();
            async move { Ok(file_stream(permit, batches)) }.boxed()
        })
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
        assert!(!cancellation.is_cancelled());
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
    async fn dropping_partition_while_waiting_for_capacity_releases_every_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 2, 2);
        let held = limiter.partition(0)?.acquire().await?;
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let mut stream = PartitionStream::new(
            vec![1],
            limiter.partition(1)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            batch_executor(BTreeMap::new(), Arc::clone(&executor_calls)),
            metrics(),
            cancellation.clone(),
        );
        let mut next = Box::pin(stream.next());
        poll_fn(|context| {
            assert!(matches!(next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        timeout(Duration::from_secs(5), async {
            while limiter.partition_active_file_reads(1) != Some(1) {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        drop(next);
        drop(stream);
        timeout(Duration::from_secs(5), async {
            while limiter.partition_active_file_reads(1) != Some(0) {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        assert!(cancellation.is_cancelled());
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(limiter.active_file_reads(), 1);
        drop(held);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn runner_cancellation_after_executor_failure_stops_future_admission()
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
        assert_eq!(limiter.active_file_reads(), 0);

        cancellation.cancel();
        let later = scheduler
            .schedule_next()
            .ok_or("expected later file")?
            .await
            .expect_err("later work must observe cancellation");
        assert_eq!(later.as_str(), "cancelled");
        assert_eq!(admission_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn partition_stream_is_lazy_and_empty_execution_completes_normally()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let metrics = metrics();
        let calls = Arc::new(AtomicUsize::new(0));
        let stream = PartitionStream::new(
            Vec::<i32>::new(),
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            batch_executor(BTreeMap::new(), Arc::clone(&calls)),
            metrics.clone(),
            ScanCancellation::new(),
        );

        assert_eq!(metrics.snapshot().scan_partitions_started, 0);
        drop(stream);
        tokio::task::yield_now().await;
        assert_eq!(metrics.snapshot().scan_partitions_started, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let mut empty = PartitionStream::new(
            Vec::<i32>::new(),
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            batch_executor(BTreeMap::new(), calls),
            metrics.clone(),
            ScanCancellation::new(),
        );
        assert!(empty.next().await.is_none());
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.scan_partitions_started, 1);
        assert_eq!(snapshot.scan_partitions_completed, 1);
        assert_eq!(snapshot.files_started, 0);
        assert_eq!(snapshot.files_completed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn sequential_partition_stream_preserves_order_and_exact_success_metrics()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let metrics = metrics();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut batches = BTreeMap::new();
        batches.insert(1, vec![batch(vec![1])?, batch(vec![2, 3])?]);
        batches.insert(2, vec![batch(vec![4])?]);
        let stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            batch_executor(batches, Arc::clone(&calls)),
            metrics.clone(),
            ScanCancellation::new(),
        );

        let batches = stream.collect::<Vec<_>>().await;
        let ids = batches
            .into_iter()
            .map(|batch| batch.map_err(Box::<dyn std::error::Error>::from))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 2, 3, 4]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(limiter.active_file_reads(), 0);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.scan_partitions_started, 1);
        assert_eq!(snapshot.scan_partitions_completed, 1);
        assert_eq!(snapshot.files_started, 2);
        assert_eq!(snapshot.files_completed, 2);
        assert_eq!(snapshot.batches_produced, 3);
        assert_eq!(snapshot.rows_produced, 4);
        Ok(())
    }

    #[tokio::test]
    async fn prefetch_zero_waits_for_current_file_exhaustion()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 2)?, 1, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Notify::new());
        let mut batches = BTreeMap::new();
        batches.insert(1, vec![batch(vec![1])?, batch(vec![2])?]);
        batches.insert(2, vec![batch(vec![3])?]);
        let executor: FileExecutor<i32, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            let gate = Arc::clone(&gate);
            Arc::new(move |task, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                let gate = Arc::clone(&gate);
                let batches = batches.get(&task).cloned().unwrap_or_default();
                async move {
                    Ok(if task == 1 {
                        gated_file_stream(permit, batches, gate)
                    } else {
                        file_stream(permit, batches)
                    })
                }
                .boxed()
            })
        };
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(3, 0)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            executor,
            metrics(),
            ScanCancellation::new(),
        );

        let first = stream.next().await.ok_or("expected first batch")??;
        assert_eq!(batch_ids(&first)?, vec![1]);
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.active_file_reads(), 1);

        gate.notify_one();
        let remaining = stream.collect::<Vec<_>>().await;
        let ids = remaining
            .into_iter()
            .map(|batch| batch.map_err(Box::<dyn std::error::Error>::from))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![2, 3]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn bounded_prefetch_overlaps_setup_and_preserves_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(3, 3)?, 1, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Notify::new());
        let mut batches = BTreeMap::new();
        for task in [1, 2, 3] {
            batches.insert(task, vec![batch(vec![task])?, batch(vec![task * 10])?]);
        }
        let executor: FileExecutor<i32, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            let gate = Arc::clone(&gate);
            Arc::new(move |task, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                let gate = Arc::clone(&gate);
                let batches = batches.get(&task).cloned().unwrap_or_default();
                async move {
                    Ok(if task == 1 {
                        gated_file_stream(permit, batches, gate)
                    } else {
                        file_stream(permit, batches)
                    })
                }
                .boxed()
            })
        };
        let mut stream = PartitionStream::new(
            vec![1, 2, 3],
            limiter.partition(0)?,
            stream_options(6, 1)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            executor,
            metrics(),
            ScanCancellation::new(),
        );

        let first = stream.next().await.ok_or("expected first batch")??;
        assert_eq!(batch_ids(&first)?, vec![1]);
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(limiter.active_file_reads(), 2);

        gate.notify_one();
        let remaining = stream.collect::<Vec<_>>().await;
        let ids = remaining
            .into_iter()
            .map(|batch| batch.map_err(Box::<dyn std::error::Error>::from))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![10, 2, 20, 3, 30]);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn prefetched_setup_error_waits_for_the_current_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 2)?, 1, 1);
        let metrics = metrics();
        let cancellation = ScanCancellation::new();
        let gate = Arc::new(Notify::new());
        let batches = vec![batch(vec![1])?, batch(vec![2])?];
        let executor_gate = Arc::clone(&gate);
        let executor: FileExecutor<i32, FileBatchStream> = Arc::new(move |task, permit, _| {
            let gate = Arc::clone(&executor_gate);
            let batches = batches.clone();
            async move {
                if task == 1 {
                    Ok(gated_file_stream(permit, batches, gate))
                } else {
                    let _permit = permit;
                    InvalidConfigurationSnafu {
                        reason: "prefetched_setup_failure",
                    }
                    .fail()
                }
            }
            .boxed()
        });
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(2, 1)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            executor,
            metrics.clone(),
            cancellation.clone(),
        );

        let first = stream.next().await.ok_or("expected first batch")??;
        assert_eq!(batch_ids(&first)?, vec![1]);
        let mut next = Box::pin(stream.next());
        poll_fn(|context| {
            assert!(matches!(next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;

        gate.notify_one();
        let second = next.await.ok_or("expected second batch")??;
        assert_eq!(batch_ids(&second)?, vec![2]);
        let error = stream
            .next()
            .await
            .ok_or("expected setup error")?
            .expect_err("prefetched setup must fail");
        assert_eq!(error.as_str(), "invalid_configuration");
        assert!(stream.next().await.is_none());
        assert!(cancellation.is_cancelled());
        assert_eq!(limiter.active_file_reads(), 0);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.files_started, 2);
        assert_eq!(snapshot.files_completed, 1);
        assert_eq!(snapshot.batches_produced, 2);
        assert_eq!(snapshot.rows_produced, 2);
        Ok(())
    }

    #[tokio::test]
    async fn bounded_handoff_and_drop_preserve_partial_metrics_and_release_permit()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let metrics = metrics();
        let mut batches = BTreeMap::new();
        batches.insert(1, vec![batch(vec![1])?, batch(vec![2])?, batch(vec![3])?]);
        let mut stream = PartitionStream::new(
            vec![1],
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            batch_executor(batches, Arc::new(AtomicUsize::new(0))),
            metrics.clone(),
            ScanCancellation::new(),
        );

        let first = stream.next().await.ok_or("expected first batch")??;
        assert_eq!(batch_ids(&first)?, vec![1]);
        for _ in 0..100 {
            if metrics.snapshot().batches_produced == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let before_drop = metrics.snapshot();
        assert_eq!(before_drop.batches_produced, 2);
        assert_eq!(before_drop.rows_produced, 2);
        assert_eq!(before_drop.files_started, 1);
        assert_eq!(before_drop.files_completed, 0);
        assert_eq!(limiter.active_file_reads(), 1);

        drop(stream);
        for _ in 0..100 {
            if limiter.active_file_reads() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(limiter.active_file_reads(), 0);
        let after_drop = metrics.snapshot();
        assert_eq!(after_drop.scan_partitions_started, 1);
        assert_eq!(after_drop.scan_partitions_completed, 0);
        assert_eq!(after_drop.files_started, 1);
        assert_eq!(after_drop.files_completed, 0);
        assert_eq!(after_drop.batches_produced, 2);
        assert_eq!(after_drop.rows_produced, 2);
        Ok(())
    }

    #[tokio::test]
    async fn file_error_is_returned_once_without_completion_metrics()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let metrics = metrics();
        let cancellation = ScanCancellation::new();
        let executor: FileExecutor<i32, FileBatchStream> = Arc::new(|_, permit, _| {
            async move {
                let error = InvalidConfigurationSnafu {
                    reason: "fake_file_failure",
                }
                .build();
                Ok(Box::pin(stream::once(async move {
                    let _permit = permit;
                    Err(error)
                })) as FileBatchStream)
            }
            .boxed()
        });
        let mut stream = PartitionStream::new(
            vec![1],
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            executor,
            metrics.clone(),
            cancellation.clone(),
        );

        let error = stream
            .next()
            .await
            .ok_or("expected file error")?
            .expect_err("file stream must fail");
        assert_eq!(error.as_str(), "invalid_configuration");
        assert!(stream.next().await.is_none());
        assert!(cancellation.is_cancelled());
        assert_eq!(limiter.active_file_reads(), 0);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.scan_partitions_started, 1);
        assert_eq!(snapshot.scan_partitions_completed, 0);
        assert_eq!(snapshot.files_started, 1);
        assert_eq!(snapshot.files_completed, 0);
        assert_eq!(snapshot.batches_produced, 0);
        assert_eq!(snapshot.rows_produced, 0);
        Ok(())
    }

    #[tokio::test]
    async fn setup_error_is_returned_once_and_cancels_future_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let admission_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let executor: FileExecutor<i32, FileBatchStream> = Arc::new(|_, permit, _| {
            async move {
                let _permit = permit;
                InvalidConfigurationSnafu {
                    reason: "fake_setup_failure",
                }
                .fail()
            }
            .boxed()
        });
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(1, 0)?,
            {
                let calls = Arc::clone(&admission_calls);
                Arc::new(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(FileAdmission::Admit)
                })
            },
            executor,
            metrics(),
            cancellation.clone(),
        );

        let error = stream
            .next()
            .await
            .ok_or("expected setup error")?
            .expect_err("file setup must fail");
        assert_eq!(error.as_str(), "invalid_configuration");
        assert!(stream.next().await.is_none());
        assert!(cancellation.is_cancelled());
        assert_eq!(admission_calls.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn admission_error_cancels_later_prefetch_before_permits()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 2)?, 1, 1);
        let admission_calls = Arc::new(AtomicUsize::new(0));
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(1, 1)?,
            {
                let calls = Arc::clone(&admission_calls);
                Arc::new(move |task| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if *task == 1 {
                        InvalidConfigurationSnafu {
                            reason: "admission_failure",
                        }
                        .fail()
                    } else {
                        Ok(FileAdmission::Admit)
                    }
                })
            },
            batch_executor(BTreeMap::new(), Arc::clone(&executor_calls)),
            metrics(),
            cancellation.clone(),
        );

        let error = stream
            .next()
            .await
            .ok_or("expected admission error")?
            .expect_err("admission must fail");
        assert_eq!(error.as_str(), "invalid_configuration");
        assert!(stream.next().await.is_none());
        assert!(cancellation.is_cancelled());
        assert_eq!(admission_calls.load(Ordering::SeqCst), 1);
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_partition_errors_are_returned_only_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 1)?, 2, 2);
        let cancellation = ScanCancellation::new();
        let metrics = metrics();
        let barrier = Arc::new(Barrier::new(2));
        let executor: FileExecutor<i32, FileBatchStream> = Arc::new(move |_, permit, _| {
            let barrier = Arc::clone(&barrier);
            async move {
                Ok(Box::pin(stream::once(async move {
                    let _permit = permit;
                    barrier.wait().await;
                    InvalidConfigurationSnafu {
                        reason: "concurrent_file_failure",
                    }
                    .fail()
                })) as FileBatchStream)
            }
            .boxed()
        });
        let mut first = PartitionStream::new(
            vec![1],
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            Arc::clone(&executor),
            metrics.clone(),
            cancellation.clone(),
        );
        let mut second = PartitionStream::new(
            vec![2],
            limiter.partition(1)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            executor,
            metrics.clone(),
            cancellation.clone(),
        );

        let results = timeout(Duration::from_secs(5), async {
            tokio::join!(first.next(), second.next())
        })
        .await?;
        let mut errors = 0;
        let mut completed = 0;
        for result in [results.0, results.1] {
            match result {
                Some(Err(error)) => {
                    assert_eq!(error.as_str(), "invalid_configuration");
                    errors += 1;
                }
                None => completed += 1,
                Some(Ok(_)) => return Err("failed partitions must not produce batches".into()),
            }
        }
        assert_eq!(errors, 1);
        assert_eq!(completed, 1);
        assert!(first.next().await.is_none());
        assert!(second.next().await.is_none());
        assert!(cancellation.is_cancelled());
        assert_eq!(limiter.active_file_reads(), 0);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.scan_partitions_started, 2);
        assert_eq!(snapshot.scan_partitions_completed, 0);
        assert_eq!(snapshot.files_started, 2);
        assert_eq!(snapshot.files_completed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn first_data_error_survives_later_scheduler_cleanup_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = ScanCancellation::new();
        let task_cancellation = cancellation.clone();
        let (output, receiver) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            send_first_error(
                &output,
                &task_cancellation,
                InvalidConfigurationSnafu {
                    reason: "first_data_error",
                }
                .build(),
            )
            .await;
            pending::<()>().await;
        });
        let abort = task.abort_handle();
        let mut stream = PartitionStream {
            state: PartitionStreamState::Running { receiver, task },
            cancellation,
            done: false,
        };

        let error = timeout(Duration::from_secs(5), stream.next())
            .await?
            .ok_or("expected first data error")?
            .expect_err("first item must be the data error");
        assert_eq!(error.as_str(), "invalid_configuration");
        abort.abort();
        assert!(
            timeout(Duration::from_secs(5), stream.next())
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn dropping_prefetched_files_releases_every_permit()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 2)?, 1, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let metrics = metrics();
        let cancellation = ScanCancellation::new();
        let executor: FileExecutor<i32, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            Arc::new(move |_, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok(pending_file_stream(permit)) }.boxed()
            })
        };
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(1, 1)?,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            executor,
            metrics.clone(),
            cancellation.clone(),
        );
        let mut next = Box::pin(stream.next());
        poll_fn(|context| {
            assert!(matches!(next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(limiter.active_file_reads(), 2);

        drop(next);
        drop(stream);
        for _ in 0..100 {
            if limiter.active_file_reads() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(cancellation.is_cancelled());
        assert_eq!(limiter.active_file_reads(), 0);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.scan_partitions_started, 1);
        assert_eq!(snapshot.scan_partitions_completed, 0);
        assert_eq!(snapshot.files_started, 2);
        assert_eq!(snapshot.files_completed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn official_kernel_disables_speculative_prefetch()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 2)?, 1, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let executor: FileExecutor<i32, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            Arc::new(move |_, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok(pending_file_stream(permit)) }.boxed()
            })
        };
        let official_options =
            stream_options(1, 1)?.with_reader_backend(DeltaReaderBackend::OfficialKernel)?;
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            official_options,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            executor,
            metrics(),
            cancellation,
        );
        let mut next = Box::pin(stream.next());
        poll_fn(|context| {
            assert!(matches!(next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.active_file_reads(), 1);

        drop(next);
        drop(stream);
        for _ in 0..100 {
            if limiter.active_file_reads() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn official_kernel_cancellation_waits_for_the_sync_safe_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(Notify::new());
        let cancellation = ScanCancellation::new();
        let executor: FileExecutor<i32, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            let finished = Arc::clone(&finished);
            let gate = Arc::clone(&gate);
            Arc::new(move |_, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                let finished = Arc::clone(&finished);
                let gate = Arc::clone(&gate);
                async move {
                    let (output, input) = mpsc::channel::<BatchResult>(1);
                    tokio::spawn(async move {
                        gate.notified().await;
                        drop(permit);
                        drop(output);
                        finished.store(true, Ordering::SeqCst);
                    });
                    Ok(Box::pin(stream::unfold(input, |mut input| async move {
                        input.recv().await.map(|batch| (batch, input))
                    })) as FileBatchStream)
                }
                .boxed()
            })
        };
        let official_options =
            stream_options(1, 0)?.with_reader_backend(DeltaReaderBackend::OfficialKernel)?;
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            official_options,
            Arc::new(|_| Ok(FileAdmission::Admit)),
            executor,
            metrics(),
            cancellation.clone(),
        );
        let mut next = Box::pin(stream.next());
        poll_fn(|context| {
            assert!(matches!(next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        timeout(Duration::from_secs(5), async {
            while calls.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        drop(next);
        drop(stream);
        assert!(cancellation.is_cancelled());
        assert_eq!(limiter.active_file_reads(), 1);
        assert!(!finished.load(Ordering::SeqCst));
        gate.notify_one();
        timeout(Duration::from_secs(5), async {
            while !finished.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[test]
    fn scheduler_source_stays_backend_neutral() {
        let source = include_str!("scheduling.rs");
        let forbidden = [
            concat!("data", "fusion"),
            concat!("par", "quet"),
            concat!("object", "_store"),
            concat!("deletion", "_vector"),
            concat!("spawn", "_blocking"),
        ];

        for pattern in forbidden {
            assert!(
                !source.contains(pattern),
                "scheduler must not contain {pattern}"
            );
        }
    }
}
