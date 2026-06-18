//! Batch pipeline foundation for query-result handoff.
//!
//! This module owns the thin async boundary between DataFusion query output and
//! downstream batch writers. The handoff is deliberately pull-driven: one
//! upstream batch is polled, one downstream write is awaited, and only then is
//! the next upstream batch polled.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::record_batch::RecordBatch,
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    physical_plan::ExecutionPlan,
};
use futures_util::{
    Stream, StreamExt,
    io::{AsyncRead, AsyncWrite},
};
use snafu::Snafu;

use crate::{DeltaFunnelError, query_engine::datafusion_query_output_stream};

/// Phase for batch pipeline setup and configuration failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchPipelinePhase {
    /// Caller-supplied batch pipeline or query-output configuration is invalid.
    Configuration,
    /// The handoff between a query output and downstream consumer cannot be set up.
    HandoffSetup,
}

impl fmt::Display for BatchPipelinePhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Configuration => "configuration",
            Self::HandoffSetup => "handoff setup",
        })
    }
}

/// Successful result for one completed query-output handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchHandoffOutcome {
    stats: BatchHandoffStats,
}

impl BatchHandoffOutcome {
    /// Returns the final per-output handoff counters.
    pub fn stats(&self) -> BatchHandoffStats {
        self.stats
    }
}

/// Terminal failure from a query-output handoff.
///
/// Upstream errors come from the DataFusion stream. Downstream errors come from
/// the batch consumer, which will later be backed by `arrow-tiberius`.
#[derive(Debug, Snafu)]
pub enum BatchHandoffError {
    /// The selected DataFusion query output could not be exposed as a stream.
    #[snafu(display("DataFusion query output handoff setup failed: {source}"))]
    QueryOutputSetup {
        /// Original DataFusion setup error.
        source: DataFusionError,
        /// Handoff counters for batches accepted before the failure.
        stats: BatchHandoffStats,
    },
    /// The upstream DataFusion stream failed before producing the next batch.
    #[snafu(display("upstream RecordBatch stream failed: {source}"))]
    Upstream {
        /// Original DataFusion error with its existing provider/query context.
        source: DataFusionError,
        /// Handoff counters for batches accepted before the failure.
        stats: BatchHandoffStats,
    },
    /// The downstream consumer rejected the current batch.
    #[snafu(display("downstream RecordBatch consumer failed: {source}"))]
    Downstream {
        /// Original downstream writer error.
        source: DeltaFunnelError,
        /// Handoff counters for batches accepted before the failure.
        stats: BatchHandoffStats,
    },
}

impl BatchHandoffError {
    /// Returns counters for batches accepted before the terminal failure.
    pub fn stats(&self) -> BatchHandoffStats {
        match self {
            Self::QueryOutputSetup { stats, .. }
            | Self::Upstream { stats, .. }
            | Self::Downstream { stats, .. } => *stats,
        }
    }
}

/// Downstream consumer for one query-output `RecordBatch`.
///
/// Implementations should return only after the batch has been accepted by the
/// downstream system. That await point is what preserves backpressure between
/// DataFusion and the downstream writer.
///
/// Consumers that need a separate finalization step, such as
/// `arrow_tiberius::BulkWriter::finish`, keep owning that step. The handoff only
/// drives per-batch writes so callers can decide whether and when finalization
/// is appropriate after success or failure.
#[async_trait]
pub trait RecordBatchConsumer: Send {
    /// Writes one batch without changing its schema, values, or row order.
    async fn write_record_batch(&mut self, batch: &RecordBatch) -> Result<(), DeltaFunnelError>;
}

#[async_trait]
impl<'client, S> RecordBatchConsumer for arrow_tiberius::BulkWriter<'client, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    async fn write_record_batch(&mut self, batch: &RecordBatch) -> Result<(), DeltaFunnelError> {
        self.write_batch(batch)
            .await
            .map(|_stats| ())
            .map_err(|source| DeltaFunnelError::MssqlWrite { source })
    }
}

/// Per-output batch handoff counters.
///
/// `input_*` counters describe batches observed from the upstream DataFusion
/// stream. `output_*` counters describe batches accepted by the downstream
/// consumer. Keeping the two sides separate lets later sink-failure handling
/// report only work that was actually accepted downstream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchHandoffStats {
    /// Batches observed from the upstream query output.
    pub input_batches: u64,
    /// Rows observed from the upstream query output.
    pub input_rows: u64,
    /// Batches accepted by the downstream consumer.
    pub output_batches: u64,
    /// Rows accepted by the downstream consumer.
    pub output_rows: u64,
}

impl BatchHandoffStats {
    /// Records one batch observed from the upstream query output.
    pub fn record_input_batch(&mut self, row_count: usize) {
        self.input_batches = self.input_batches.saturating_add(1);
        self.input_rows = self.input_rows.saturating_add(rows_to_u64(row_count));
    }

    /// Records one batch accepted by the downstream consumer.
    pub fn record_output_batch(&mut self, row_count: usize) {
        self.output_batches = self.output_batches.saturating_add(1);
        self.output_rows = self.output_rows.saturating_add(rows_to_u64(row_count));
    }
}

/// Hands one DataFusion `RecordBatch` stream to one downstream consumer.
///
/// This helper intentionally contains no queue and spawns no background task.
/// The next upstream batch is not polled until the previous downstream write
/// has completed. Empty batches are forwarded and counted when the downstream
/// consumer accepts them, matching DataFusion's stream semantics.
pub async fn handoff_record_batch_stream<S, C>(
    mut stream: S,
    consumer: &mut C,
) -> Result<BatchHandoffOutcome, BatchHandoffError>
where
    S: Stream<Item = DataFusionResult<RecordBatch>> + Unpin,
    C: RecordBatchConsumer,
{
    let mut stats = BatchHandoffStats::default();

    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|source| BatchHandoffError::Upstream { source, stats })?;
        let row_count = batch.num_rows();
        let accepted_stats = stats;

        stats.record_input_batch(row_count);
        if let Err(source) = consumer.write_record_batch(&batch).await {
            return Err(BatchHandoffError::Downstream {
                source,
                stats: accepted_stats,
            });
        }
        stats.record_output_batch(row_count);
    }

    Ok(BatchHandoffOutcome { stats })
}

/// Executes one selected DataFusion query output and hands it to a consumer.
///
/// This composes DataFusion's merged output stream execution with the
/// pull-driven batch handoff. Multi-partition query outputs are merged by
/// DataFusion before the handoff, preserving scan parallelism while keeping the
/// downstream writer serial.
pub async fn handoff_datafusion_query_output<C>(
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
    consumer: &mut C,
) -> Result<BatchHandoffOutcome, BatchHandoffError>
where
    C: RecordBatchConsumer,
{
    let stream = datafusion_query_output_stream(plan, task_context).map_err(|source| {
        BatchHandoffError::QueryOutputSetup {
            source,
            stats: BatchHandoffStats::default(),
        }
    })?;

    handoff_record_batch_stream(stream, consumer).await
}

/// Validates a future batch/query/handoff `usize` option that must be nonzero.
#[allow(dead_code)]
pub(crate) fn validate_nonzero_usize_option(
    phase: BatchPipelinePhase,
    option: &'static str,
    value: usize,
) -> Result<(), DeltaFunnelError> {
    if value == 0 {
        return Err(DeltaFunnelError::BatchPipeline {
            phase,
            option,
            message: "must be greater than zero".to_owned(),
        });
    }

    Ok(())
}

fn rows_to_u64(row_count: usize) -> u64 {
    u64::try_from(row_count).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        error::Error,
        io::Cursor,
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };

    use async_trait::async_trait;
    use datafusion::{
        arrow::{
            array::Int32Array,
            datatypes::{DataType, Field, Schema, SchemaRef},
            record_batch::RecordBatch,
        },
        error::DataFusionError,
        execution::TaskContext,
        physical_plan::{ExecutionPlan, test::TestMemoryExec},
    };
    use futures_util::{Stream, io::AllowStdIo, stream};
    use tokio::sync::oneshot;

    use super::{
        BatchHandoffError, BatchHandoffStats, BatchPipelinePhase, RecordBatchConsumer,
        handoff_datafusion_query_output, handoff_record_batch_stream, rows_to_u64,
        validate_nonzero_usize_option,
    };
    use crate::DeltaFunnelError;

    #[test]
    fn stats_start_at_zero() {
        let stats = BatchHandoffStats::default();

        assert_eq!(stats.input_batches, 0);
        assert_eq!(stats.input_rows, 0);
        assert_eq!(stats.output_batches, 0);
        assert_eq!(stats.output_rows, 0);
    }

    #[test]
    fn stats_update_input_and_output_separately() {
        let mut stats = BatchHandoffStats::default();

        stats.record_input_batch(5);
        stats.record_input_batch(7);
        stats.record_output_batch(5);

        assert_eq!(stats.input_batches, 2);
        assert_eq!(stats.input_rows, 12);
        assert_eq!(stats.output_batches, 1);
        assert_eq!(stats.output_rows, 5);
    }

    #[test]
    fn stats_updates_saturate() {
        let mut stats = BatchHandoffStats {
            input_batches: u64::MAX,
            input_rows: u64::MAX - 1,
            output_batches: u64::MAX,
            output_rows: u64::MAX - 1,
        };

        stats.record_input_batch(10);
        stats.record_output_batch(10);

        assert_eq!(stats.input_batches, u64::MAX);
        assert_eq!(stats.input_rows, u64::MAX);
        assert_eq!(stats.output_batches, u64::MAX);
        assert_eq!(stats.output_rows, u64::MAX);
    }

    #[test]
    fn rows_to_u64_returns_exact_normal_values() {
        assert_eq!(rows_to_u64(42), 42);
    }

    #[test]
    fn validation_accepts_nonzero_values() -> Result<(), DeltaFunnelError> {
        validate_nonzero_usize_option(BatchPipelinePhase::Configuration, "output_batch_size", 1)
    }

    #[test]
    fn validation_rejects_zero_values() {
        let error = validate_nonzero_usize_option(
            BatchPipelinePhase::Configuration,
            "output_batch_size",
            0,
        );

        assert!(matches!(
            error,
            Err(DeltaFunnelError::BatchPipeline {
                phase: BatchPipelinePhase::Configuration,
                option: "output_batch_size",
                ..
            })
        ));
    }

    #[test]
    fn phase_display_is_stable() {
        assert_eq!(
            BatchPipelinePhase::Configuration.to_string(),
            "configuration"
        );
        assert_eq!(
            BatchPipelinePhase::HandoffSetup.to_string(),
            "handoff setup"
        );
    }

    #[test]
    fn arrow_tiberius_bulk_writer_is_a_record_batch_consumer() {
        fn assert_consumer<C: RecordBatchConsumer>() {}

        assert_consumer::<arrow_tiberius::BulkWriter<'static, AllowStdIo<Cursor<Vec<u8>>>>>();
    }

    #[tokio::test]
    async fn handoff_forwards_batches_in_order() -> Result<(), Box<dyn Error>> {
        let batches = vec![Ok(int_batch(&[1, 2])?), Ok(int_batch(&[3, 4, 5])?)];
        let mut consumer = RecordingConsumer::default();

        let outcome = handoff_record_batch_stream(stream::iter(batches), &mut consumer).await?;

        assert_eq!(consumer.accepted_row_counts, vec![2, 3]);
        assert_eq!(
            outcome.stats(),
            BatchHandoffStats {
                input_batches: 2,
                input_rows: 5,
                output_batches: 2,
                output_rows: 5,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn handoff_counts_empty_batches_when_accepted() -> Result<(), Box<dyn Error>> {
        let batches = vec![Ok(int_batch(&[])?), Ok(int_batch(&[1, 2])?)];
        let mut consumer = RecordingConsumer::default();

        let outcome = handoff_record_batch_stream(stream::iter(batches), &mut consumer).await?;

        assert_eq!(consumer.accepted_row_counts, vec![0, 2]);
        assert_eq!(
            outcome.stats(),
            BatchHandoffStats {
                input_batches: 2,
                input_rows: 2,
                output_batches: 2,
                output_rows: 2,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn handoff_keeps_selected_output_stats_independent() -> Result<(), Box<dyn Error>> {
        let first_batches = vec![Ok(int_batch(&[1])?)];
        let second_batches = vec![Ok(int_batch(&[10, 20])?), Ok(int_batch(&[30])?)];
        let mut first_consumer = RecordingConsumer::default();
        let mut second_consumer = RecordingConsumer::default();

        let first =
            handoff_record_batch_stream(stream::iter(first_batches), &mut first_consumer).await?;
        let second =
            handoff_record_batch_stream(stream::iter(second_batches), &mut second_consumer).await?;

        assert_eq!(
            first.stats(),
            BatchHandoffStats {
                input_batches: 1,
                input_rows: 1,
                output_batches: 1,
                output_rows: 1,
            }
        );
        assert_eq!(
            second.stats(),
            BatchHandoffStats {
                input_batches: 2,
                input_rows: 3,
                output_batches: 2,
                output_rows: 3,
            }
        );
        assert_eq!(first_consumer.accepted_row_counts, vec![1]);
        assert_eq!(second_consumer.accepted_row_counts, vec![2, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn handoff_datafusion_query_output_merges_partitions() -> Result<(), Box<dyn Error>> {
        let schema = schema();
        let plan = TestMemoryExec::try_new_exec(
            &[
                vec![int_batch_with_schema(Arc::clone(&schema), &[1])?],
                vec![int_batch_with_schema(Arc::clone(&schema), &[2, 3])?],
            ],
            schema,
            None,
        )?;
        assert_eq!(plan.properties().output_partitioning().partition_count(), 2);

        let plan: Arc<dyn ExecutionPlan> = plan;
        let mut consumer = RecordingConsumer::default();

        let outcome =
            handoff_datafusion_query_output(plan, Arc::new(TaskContext::default()), &mut consumer)
                .await?;

        consumer.accepted_row_counts.sort_unstable();
        assert_eq!(consumer.accepted_row_counts, vec![1, 2]);
        assert_eq!(
            outcome.stats(),
            BatchHandoffStats {
                input_batches: 2,
                input_rows: 3,
                output_batches: 2,
                output_rows: 3,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn handoff_preserves_upstream_error_context() -> Result<(), Box<dyn Error>> {
        let batches = vec![
            Ok(int_batch(&[1, 2])?),
            Err(DataFusionError::Execution("upstream failed".to_owned())),
            Ok(int_batch(&[3])?),
        ];
        let mut consumer = RecordingConsumer::default();

        let error = handoff_record_batch_stream(stream::iter(batches), &mut consumer)
            .await
            .err()
            .ok_or("handoff should fail on upstream error")?;

        assert_eq!(consumer.accepted_row_counts, vec![2]);
        assert_eq!(
            error.stats(),
            BatchHandoffStats {
                input_batches: 1,
                input_rows: 2,
                output_batches: 1,
                output_rows: 2,
            }
        );
        assert!(matches!(error, BatchHandoffError::Upstream { .. }));
        assert!(error.to_string().contains("upstream failed"));
        Ok(())
    }

    #[tokio::test]
    async fn handoff_stops_after_downstream_failure() -> Result<(), Box<dyn Error>> {
        let batches = vec![
            Ok(int_batch(&[1, 2])?),
            Ok(int_batch(&[3, 4, 5])?),
            Ok(int_batch(&[6])?),
        ];
        let mut consumer = RecordingConsumer {
            fail_on_call: Some(1),
            ..RecordingConsumer::default()
        };

        let error = handoff_record_batch_stream(stream::iter(batches), &mut consumer)
            .await
            .err()
            .ok_or("handoff should fail on downstream error")?;

        assert_eq!(consumer.accepted_row_counts, vec![2]);
        assert_eq!(consumer.call_count, 2);
        assert_eq!(
            error.stats(),
            BatchHandoffStats {
                input_batches: 1,
                input_rows: 2,
                output_batches: 1,
                output_rows: 2,
            }
        );
        assert!(matches!(error, BatchHandoffError::Downstream { .. }));
        assert!(error.to_string().contains("consumer failed"));
        Ok(())
    }

    #[tokio::test]
    async fn slow_downstream_blocks_next_upstream_poll() -> Result<(), Box<dyn Error>> {
        let poll_count = Arc::new(AtomicUsize::new(0));
        let accepted_row_counts = Arc::new(Mutex::new(Vec::new()));
        let stream = PollCountingStream {
            batches: VecDeque::from(vec![int_batch(&[1])?, int_batch(&[2])?]),
            poll_count: Arc::clone(&poll_count),
        };
        let (release_write, wait_for_release) = oneshot::channel();
        let consumer = GatedConsumer {
            accepted_row_counts: Arc::clone(&accepted_row_counts),
            first_write_gate: Some(wait_for_release),
        };

        let task = tokio::spawn(async move {
            let mut consumer = consumer;
            handoff_record_batch_stream(stream, &mut consumer).await
        });

        tokio::task::yield_now().await;
        assert_eq!(poll_count.load(Ordering::SeqCst), 1);
        assert!(release_write.send(()).is_ok());

        let outcome = task.await??;

        assert_eq!(poll_count.load(Ordering::SeqCst), 3);
        assert_eq!(
            *accepted_row_counts.lock().map_err(|_| "mutex poisoned")?,
            vec![1, 1]
        );
        assert_eq!(
            outcome.stats(),
            BatchHandoffStats {
                input_batches: 2,
                input_rows: 2,
                output_batches: 2,
                output_rows: 2,
            }
        );
        Ok(())
    }

    #[derive(Default)]
    struct RecordingConsumer {
        accepted_row_counts: Vec<usize>,
        call_count: usize,
        fail_on_call: Option<usize>,
    }

    #[async_trait]
    impl RecordBatchConsumer for RecordingConsumer {
        async fn write_record_batch(
            &mut self,
            batch: &RecordBatch,
        ) -> Result<(), DeltaFunnelError> {
            if self.fail_on_call == Some(self.call_count) {
                self.call_count += 1;
                return Err(consumer_error("consumer failed"));
            }

            self.call_count += 1;
            self.accepted_row_counts.push(batch.num_rows());
            Ok(())
        }
    }

    struct GatedConsumer {
        accepted_row_counts: Arc<Mutex<Vec<usize>>>,
        first_write_gate: Option<oneshot::Receiver<()>>,
    }

    #[async_trait]
    impl RecordBatchConsumer for GatedConsumer {
        async fn write_record_batch(
            &mut self,
            batch: &RecordBatch,
        ) -> Result<(), DeltaFunnelError> {
            self.accepted_row_counts
                .lock()
                .map_err(|_| consumer_error("accepted rows lock poisoned"))?
                .push(batch.num_rows());

            if let Some(gate) = self.first_write_gate.take() {
                let _result = gate.await;
            }

            Ok(())
        }
    }

    struct PollCountingStream {
        batches: VecDeque<RecordBatch>,
        poll_count: Arc<AtomicUsize>,
    }

    impl Stream for PollCountingStream {
        type Item = Result<RecordBatch, DataFusionError>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.poll_count.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.batches.pop_front().map(Ok))
        }
    }

    fn int_batch(values: &[i32]) -> Result<RecordBatch, Box<dyn Error>> {
        int_batch_with_schema(schema(), values)
    }

    fn int_batch_with_schema(
        schema: SchemaRef,
        values: &[i32],
    ) -> Result<RecordBatch, Box<dyn Error>> {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
            .map_err(Into::into)
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]))
    }

    fn consumer_error(message: impl Into<String>) -> DeltaFunnelError {
        DeltaFunnelError::BatchPipeline {
            phase: BatchPipelinePhase::HandoffSetup,
            option: "record_batch_consumer",
            message: message.into(),
        }
    }
}
