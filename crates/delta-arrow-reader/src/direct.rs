//! Public DataFusion-independent Delta-to-Arrow reader.

use std::{
    collections::VecDeque,
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures_util::Stream;
use snafu::ResultExt;

use crate::{
    DeltaPredicate, DeltaProtocolInfo, DeltaReadMetrics, DeltaReaderBackend, DeltaReaderError,
    DeltaReaderExecutionOptions, DeltaSnapshotSelection, DeltaStorageOptions,
    error::{DataFileReadSnafu, InvalidConfigurationSnafu, ScanPlanningSnafu},
    kernel::{delta_predicate_kernel_pruning_is_exact, delta_predicate_to_kernel_pruning},
    planning::{
        DeltaScanPartitionTargetOptions, DeltaScanPlan, plan_scan, validate_backend_available,
    },
    predicate::{evaluate_predicate, referenced_columns, validate_predicate},
    protocol::validate_protocol,
    scheduling::{
        DeltaScanExecution, FileAdmission, FileAdmissionFn, FileBatchStream, FileExecutor,
        PartitionStream,
    },
    snapshot::{
        LoadedDeltaTableSnapshot, StagedDeltaTableSnapshot, load_delta_table_snapshot_async,
        load_delta_table_snapshot_blocking, load_staged_delta_table_snapshot_async,
        load_staged_delta_table_snapshot_blocking,
    },
};

const TRACING_TARGET: &str = "delta_arrow_reader";

/// Configures and loads one immutable Delta table snapshot.
///
/// The asynchronous path uses the caller's Tokio runtime. Scans return a
/// pull-driven stream and do not materialize the whole table.
///
/// # Example
///
/// ```no_run
/// use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTableBuilder};
/// use futures_util::TryStreamExt;
///
/// # async fn read_table() -> Result<(), Box<dyn std::error::Error>> {
/// let table = DeltaTableBuilder::new("/tmp/example-delta-table")
///     .load_async()
///     .await?;
/// let scan = table
///     .scan()
///     .with_projection(vec!["id".into(), "name".into()])
///     .with_predicate(DeltaPredicate::Compare {
///         column: "id".into(),
///         op: DeltaComparison::GtEq,
///         value: DeltaScalar::Int64(10),
///     })
///     .with_limit(100)
///     .build()
///     .await?;
/// let mut batches = scan.execute().await?;
///
/// while let Some(batch) = batches.try_next().await? {
///     println!("rows={}", batch.num_rows());
/// }
/// # Ok(())
/// # }
/// ```
pub struct DeltaTableBuilder {
    table_uri: String,
    storage_options: DeltaStorageOptions,
    snapshot_selection: DeltaSnapshotSelection,
    execution_options: DeltaReaderExecutionOptions,
}

impl DeltaTableBuilder {
    /// Creates a builder for the latest snapshot with default execution settings.
    pub fn new(table_uri: impl Into<String>) -> Self {
        Self {
            table_uri: table_uri.into(),
            storage_options: DeltaStorageOptions::new(),
            snapshot_selection: DeltaSnapshotSelection::Latest,
            execution_options: DeltaReaderExecutionOptions::new(),
        }
    }

    /// Replaces the storage options forwarded during table loading.
    pub fn with_storage_options(mut self, value: DeltaStorageOptions) -> Self {
        self.storage_options = value;
        self
    }

    /// Selects the Delta snapshot to load.
    pub const fn with_snapshot_selection(mut self, value: DeltaSnapshotSelection) -> Self {
        self.snapshot_selection = value;
        self
    }

    /// Replaces the default execution settings used by scans of this table.
    pub const fn with_execution_options(mut self, value: DeltaReaderExecutionOptions) -> Self {
        self.execution_options = value;
        self
    }

    /// Loads the snapshot on the calling thread.
    pub fn load(self) -> Result<DeltaTable, DeltaReaderError> {
        validate_direct_execution_options(self.execution_options)?;
        let snapshot = load_delta_table_snapshot_blocking(
            &self.table_uri,
            &self.storage_options,
            self.snapshot_selection,
        )?;
        Ok(DeltaTable::new(snapshot, self.execution_options))
    }

    /// Loads the snapshot through the caller-owned Tokio runtime.
    pub async fn load_async(self) -> Result<DeltaTable, DeltaReaderError> {
        validate_direct_execution_options(self.execution_options)?;
        let snapshot = load_delta_table_snapshot_async(
            self.table_uri,
            self.storage_options,
            self.snapshot_selection,
        )
        .await?;
        Ok(DeltaTable::new(snapshot, self.execution_options))
    }

    /// Loads snapshot metadata without converting its logical Arrow schema.
    pub fn load_snapshot(self) -> Result<DeltaTableSnapshot, DeltaReaderError> {
        validate_direct_execution_options(self.execution_options)?;
        let snapshot = load_staged_delta_table_snapshot_blocking(
            &self.table_uri,
            &self.storage_options,
            self.snapshot_selection,
        )?;
        Ok(DeltaTableSnapshot::new(snapshot, self.execution_options))
    }

    /// Loads snapshot metadata through the caller-owned Tokio runtime.
    pub async fn load_snapshot_async(self) -> Result<DeltaTableSnapshot, DeltaReaderError> {
        validate_direct_execution_options(self.execution_options)?;
        let snapshot = load_staged_delta_table_snapshot_async(
            self.table_uri,
            self.storage_options,
            self.snapshot_selection,
        )
        .await?;
        Ok(DeltaTableSnapshot::new(snapshot, self.execution_options))
    }
}

impl fmt::Debug for DeltaTableBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaTableBuilder")
            .field("table_uri", &"<redacted>")
            .field("storage_options", &"<redacted>")
            .field("snapshot_selection", &self.snapshot_selection)
            .field("execution_options", &self.execution_options)
            .finish()
    }
}

/// Loaded Delta snapshot metadata awaiting logical Arrow schema conversion.
pub struct DeltaTableSnapshot {
    snapshot: StagedDeltaTableSnapshot,
    execution_options: DeltaReaderExecutionOptions,
}

impl DeltaTableSnapshot {
    fn new(
        snapshot: StagedDeltaTableSnapshot,
        execution_options: DeltaReaderExecutionOptions,
    ) -> Self {
        Self {
            snapshot,
            execution_options,
        }
    }

    /// Returns the loaded Delta snapshot version.
    pub fn version(&self) -> u64 {
        self.snapshot.version()
    }

    /// Returns the loaded Delta protocol metadata.
    pub fn protocol(&self) -> &DeltaProtocolInfo {
        self.snapshot.protocol_info()
    }

    /// Returns the normalized table URI.
    ///
    /// This value may contain sensitive caller input. Do not log or expose it.
    pub fn table_uri(&self) -> &str {
        self.snapshot.table_uri()
    }

    /// Validates the loaded snapshot against the supported reader protocol.
    pub fn validate_protocol(&self) -> Result<(), DeltaReaderError> {
        validate_protocol(self.protocol())
    }

    /// Converts the logical Arrow schema and finishes constructing the table.
    pub fn into_table(self) -> Result<DeltaTable, DeltaReaderError> {
        Ok(DeltaTable::new(
            self.snapshot.into_loaded()?,
            self.execution_options,
        ))
    }
}

impl fmt::Debug for DeltaTableSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaTableSnapshot")
            .field("version", &self.version())
            .finish_non_exhaustive()
    }
}

/// One immutable loaded Delta table snapshot.
#[derive(Clone)]
pub struct DeltaTable {
    snapshot: Arc<LoadedDeltaTableSnapshot>,
    version: u64,
    execution_options: DeltaReaderExecutionOptions,
}

impl DeltaTable {
    fn new(
        snapshot: LoadedDeltaTableSnapshot,
        execution_options: DeltaReaderExecutionOptions,
    ) -> Self {
        let version = snapshot.version();
        Self {
            snapshot: Arc::new(snapshot),
            version,
            execution_options,
        }
    }

    /// Returns the loaded Delta snapshot version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the logical Arrow schema.
    pub fn schema(&self) -> &SchemaRef {
        self.snapshot.schema_ref()
    }

    /// Returns the loaded Delta protocol metadata.
    pub fn protocol(&self) -> &DeltaProtocolInfo {
        self.snapshot.protocol_info()
    }

    /// Returns the normalized table URI.
    ///
    /// This value may contain sensitive caller input. Do not log or expose it.
    pub fn table_uri(&self) -> &str {
        self.snapshot.table_uri()
    }

    #[allow(dead_code)]
    pub(crate) fn partition_columns(&self) -> &[String] {
        self.snapshot.partition_columns()
    }

    #[allow(dead_code)]
    pub(crate) fn snapshot(&self) -> &LoadedDeltaTableSnapshot {
        self.snapshot.as_ref()
    }

    /// Validates the loaded snapshot against the supported reader protocol.
    pub fn validate_protocol(&self) -> Result<(), DeltaReaderError> {
        validate_protocol(self.protocol())
    }

    /// Starts configuring a new single-use scan.
    pub fn scan(&self) -> DeltaScanBuilder<'_> {
        DeltaScanBuilder {
            table: self,
            projection: None,
            predicate: None,
            limit: None,
            target_partitions: None,
            execution_options: self.execution_options,
        }
    }
}

impl fmt::Debug for DeltaTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaTable")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// Configures one single-use direct Delta scan.
pub struct DeltaScanBuilder<'table> {
    table: &'table DeltaTable,
    projection: Option<Vec<String>>,
    predicate: Option<DeltaPredicate>,
    limit: Option<usize>,
    target_partitions: Option<usize>,
    execution_options: DeltaReaderExecutionOptions,
}

impl<'table> DeltaScanBuilder<'table> {
    /// Selects visible logical columns in caller order.
    pub fn with_projection(mut self, logical_columns: Vec<String>) -> Self {
        self.projection = Some(logical_columns);
        self
    }

    /// Replaces the exact logical row predicate.
    pub fn with_predicate(mut self, predicate: DeltaPredicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Sets the maximum number of output rows.
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Overrides the number of planned scan partitions.
    pub fn with_target_partitions(mut self, value: usize) -> Result<Self, DeltaReaderError> {
        if value == 0 {
            return InvalidConfigurationSnafu {
                reason: "scan_partition_target_must_be_positive",
            }
            .fail();
        }
        self.target_partitions = Some(value);
        Ok(self)
    }

    /// Replaces the execution settings for this scan.
    pub fn with_execution_options(
        mut self,
        value: DeltaReaderExecutionOptions,
    ) -> Result<Self, DeltaReaderError> {
        validate_direct_execution_options(value)?;
        self.execution_options = value;
        Ok(self)
    }

    /// Builds one immutable single-use scan plan without reading data files.
    pub async fn build(self) -> Result<DeltaScan, DeltaReaderError> {
        self.table.validate_protocol()?;
        validate_direct_execution_options(self.execution_options)?;
        if let Some(predicate) = self.predicate.as_ref() {
            validate_predicate(predicate, self.table.schema().as_ref())?;
        }

        let snapshot_version = self.table.version();
        let backend = self.execution_options.reader_backend();
        trace_planning_started(snapshot_version, backend);
        let snapshot = Arc::clone(&self.table.snapshot);
        let projection = self.projection;
        let predicate = self.predicate;
        let hidden_columns = predicate
            .as_ref()
            .map(referenced_columns)
            .unwrap_or_default();
        let enforce_physical_predicate_rows = predicate
            .as_ref()
            .is_some_and(delta_predicate_kernel_pruning_is_exact);
        let kernel_predicate = predicate
            .as_ref()
            .and_then(delta_predicate_to_kernel_pruning);
        let include_stats = kernel_predicate.is_some();
        let execution_options = self.execution_options;
        let target_partitions = self.target_partitions;
        let result = tokio::task::spawn_blocking(move || {
            plan_scan(
                snapshot.as_ref(),
                projection.as_deref(),
                &hidden_columns,
                kernel_predicate,
                include_stats,
                execution_options,
                DeltaScanPartitionTargetOptions {
                    explicit_target_partitions: target_partitions,
                    caller_target_partitions: None,
                },
            )
        })
        .await
        .boxed()
        .context(ScanPlanningSnafu {
            reason: "scan_planning_task_failed",
        })
        .and_then(|result| result);

        match result {
            Ok(plan) => {
                trace_planning_completed(snapshot_version, backend, plan.partitions.len());
                Ok(DeltaScan {
                    partition_count: plan.partitions.len(),
                    plan: Arc::new(plan),
                    predicate,
                    limit: self.limit,
                    enforce_physical_predicate_rows,
                })
            }
            Err(error) => {
                trace_planning_failed(snapshot_version, backend, &error);
                Err(error)
            }
        }
    }
}

/// One immutable, single-use direct Delta scan plan.
///
/// A scan cannot be cloned or executed twice.
///
/// ```compile_fail
/// use delta_arrow_reader::DeltaScan;
///
/// async fn execute_twice(scan: DeltaScan) {
///     let _ = scan.execute().await;
///     let _ = scan.execute().await;
/// }
/// ```
///
/// ```compile_fail
/// use delta_arrow_reader::DeltaScan;
///
/// fn clone_scan(scan: DeltaScan) {
///     let _ = scan.clone();
/// }
/// ```
pub struct DeltaScan {
    plan: Arc<DeltaScanPlan>,
    predicate: Option<DeltaPredicate>,
    limit: Option<usize>,
    partition_count: usize,
    enforce_physical_predicate_rows: bool,
}

impl DeltaScan {
    /// Returns the visible logical output schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.plan.projected_schema
    }

    /// Returns the number of planned execution partitions.
    pub const fn partition_count(&self) -> usize {
        self.partition_count
    }

    /// Creates the pull-driven direct Arrow batch stream.
    pub async fn execute(self) -> Result<DeltaBatchStream, DeltaReaderError> {
        let metrics = self.plan.metrics.clone();
        let schema = Arc::clone(&self.plan.projected_schema);
        let partition_count = self.plan.partitions.len();
        let snapshot_version = self.plan.snapshot_version;
        let backend = self.plan.execution_options.reader_backend();
        let projection = (self.plan.logical_schema.as_ref() != schema.as_ref())
            .then(|| (0..schema.fields().len()).collect::<Vec<_>>());
        let mut partitions = VecDeque::new();

        if self.limit != Some(0) {
            let execution = DeltaScanExecution::new(Arc::clone(&self.plan));
            let admission: FileAdmissionFn<_> = Arc::new(|_| Ok(FileAdmission::Admit));
            let executor = match backend {
                DeltaReaderBackend::NativeAsync => native_async_executor(
                    &self.plan,
                    None,
                    self.enforce_physical_predicate_rows
                        .then(|| self.plan.physical_predicate.clone())
                        .flatten(),
                )?,
                DeltaReaderBackend::OfficialKernel => official_kernel_executor(&self.plan)?,
            };
            for partition in 0..partition_count {
                partitions.push_back(execution.partition_stream(
                    partition,
                    Arc::clone(&admission),
                    Arc::clone(&executor),
                )?);
            }
        }

        Ok(DeltaBatchStream {
            schema,
            metrics,
            partitions,
            predicate: self.predicate,
            projection,
            remaining: self.limit,
            snapshot_version,
            backend,
            partition_count,
            started: false,
            done: false,
        })
    }
}

/// Pull-driven stream of finalized logical Arrow batches from one Delta scan.
///
/// The stream has no inherent whole-result collection method. Callers that
/// intentionally materialize a result must opt into a stream extension trait.
///
/// ```compile_fail
/// use delta_arrow_reader::DeltaBatchStream;
///
/// fn collect_without_opt_in(stream: DeltaBatchStream) {
///     let _ = stream.collect();
/// }
/// ```
pub struct DeltaBatchStream {
    schema: SchemaRef,
    metrics: DeltaReadMetrics,
    partitions: VecDeque<PartitionStream>,
    predicate: Option<DeltaPredicate>,
    projection: Option<Vec<usize>>,
    remaining: Option<usize>,
    snapshot_version: u64,
    backend: DeltaReaderBackend,
    partition_count: usize,
    started: bool,
    done: bool,
}

impl DeltaBatchStream {
    /// Returns the visible logical output schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns a lightweight shared handle to point-in-time scan metrics.
    pub fn metrics(&self) -> DeltaReadMetrics {
        self.metrics.clone()
    }

    fn start(&mut self) {
        if self.started {
            return;
        }
        self.started = true;
        trace_execution_started(self.snapshot_version, self.backend, self.partition_count);
        for partition in &mut self.partitions {
            partition.start();
        }
    }

    fn complete(&mut self) {
        if self.done {
            return;
        }
        self.partitions.clear();
        self.done = true;
        trace_execution_completed(self.snapshot_version, self.backend, self.partition_count);
    }

    fn fail(&mut self, error: &DeltaReaderError) {
        self.partitions.clear();
        self.done = true;
        trace_execution_failed(
            self.snapshot_version,
            self.backend,
            self.partition_count,
            error,
        );
    }

    fn finalize_batch(&self, mut batch: RecordBatch) -> Result<RecordBatch, DeltaReaderError> {
        if let Some(predicate) = self.predicate.as_ref() {
            batch = evaluate_predicate(&batch, predicate)?;
        }
        if let Some(projection) = self.projection.as_ref() {
            batch = batch
                .project(projection)
                .boxed()
                .context(DataFileReadSnafu {
                    reason: "direct_projection_failed",
                })?;
        }
        Ok(batch)
    }
}

impl Stream for DeltaBatchStream {
    type Item = Result<RecordBatch, DeltaReaderError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        this.start();

        loop {
            let Some(partition) = this.partitions.front_mut() else {
                this.complete();
                return Poll::Ready(None);
            };
            match Pin::new(partition).poll_next(context) {
                Poll::Ready(Some(Ok(batch))) => {
                    let mut batch = match this.finalize_batch(batch) {
                        Ok(batch) => batch,
                        Err(error) => {
                            this.fail(&error);
                            return Poll::Ready(Some(Err(error)));
                        }
                    };
                    if let Some(remaining) = this.remaining.as_mut() {
                        if batch.num_rows() >= *remaining {
                            batch = batch.slice(0, *remaining);
                            *remaining = 0;
                            this.complete();
                        } else {
                            *remaining -= batch.num_rows();
                        }
                    }
                    return Poll::Ready(Some(Ok(batch)));
                }
                Poll::Ready(Some(Err(error))) => {
                    this.fail(&error);
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => {
                    this.partitions.pop_front();
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Drop for DeltaBatchStream {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        self.partitions.clear();
        self.done = true;
        trace_execution_dropped(self.snapshot_version, self.backend, self.partition_count);
    }
}

fn validate_direct_execution_options(
    options: DeltaReaderExecutionOptions,
) -> Result<(), DeltaReaderError> {
    options.validate()?;
    validate_backend_available(options)?;
    Ok(())
}

#[cfg(feature = "native-async")]
pub(crate) fn native_async_executor(
    plan: &Arc<DeltaScanPlan>,
    output_batch_size: Option<usize>,
    row_predicate: Option<crate::kernel::DeltaKernelPredicate>,
) -> Result<FileExecutor<crate::planning::DeltaScanFileTask, FileBatchStream>, DeltaReaderError> {
    Ok(crate::native_async_reader::native_async_file_executor(
        plan,
        output_batch_size,
        row_predicate,
    ))
}

#[cfg(not(feature = "native-async"))]
pub(crate) fn native_async_executor(
    _plan: &Arc<DeltaScanPlan>,
    _output_batch_size: Option<usize>,
    _row_predicate: Option<crate::kernel::DeltaKernelPredicate>,
) -> Result<FileExecutor<crate::planning::DeltaScanFileTask, FileBatchStream>, DeltaReaderError> {
    crate::error::UnsupportedBackendSnafu {
        reason: "native_async_feature_disabled",
    }
    .fail()
}

#[cfg(feature = "official-kernel")]
pub(crate) fn official_kernel_executor(
    plan: &Arc<DeltaScanPlan>,
) -> Result<FileExecutor<crate::planning::DeltaScanFileTask, FileBatchStream>, DeltaReaderError> {
    Ok(crate::official_kernel_reader::official_kernel_file_executor(plan))
}

#[cfg(not(feature = "official-kernel"))]
pub(crate) fn official_kernel_executor(
    _plan: &Arc<DeltaScanPlan>,
) -> Result<FileExecutor<crate::planning::DeltaScanFileTask, FileBatchStream>, DeltaReaderError> {
    crate::error::UnsupportedBackendSnafu {
        reason: "official_kernel_feature_disabled",
    }
    .fail()
}

fn trace_planning_started(snapshot_version: u64, backend: DeltaReaderBackend) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_planning.started",
        snapshot_version,
        backend = ?backend,
        partition_count = tracing::field::Empty,
        outcome = "started"
    );
}

fn trace_planning_completed(
    snapshot_version: u64,
    backend: DeltaReaderBackend,
    partition_count: usize,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_planning.completed",
        snapshot_version,
        backend = ?backend,
        partition_count,
        outcome = "completed"
    );
}

fn trace_planning_failed(
    snapshot_version: u64,
    backend: DeltaReaderBackend,
    error: &DeltaReaderError,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_planning.failed",
        snapshot_version,
        backend = ?backend,
        partition_count = tracing::field::Empty,
        outcome = "failed",
        error_variant = error.as_str(),
        error_phase = error.phase().as_str()
    );
}

fn trace_execution_started(
    snapshot_version: u64,
    backend: DeltaReaderBackend,
    partition_count: usize,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_execution.started",
        snapshot_version,
        backend = ?backend,
        partition_count,
        outcome = "started"
    );
}

fn trace_execution_completed(
    snapshot_version: u64,
    backend: DeltaReaderBackend,
    partition_count: usize,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_execution.completed",
        snapshot_version,
        backend = ?backend,
        partition_count,
        outcome = "completed"
    );
}

fn trace_execution_failed(
    snapshot_version: u64,
    backend: DeltaReaderBackend,
    partition_count: usize,
    error: &DeltaReaderError,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_execution.failed",
        snapshot_version,
        backend = ?backend,
        partition_count,
        outcome = "failed",
        error_variant = error.as_str(),
        error_phase = error.phase().as_str()
    );
}

fn trace_execution_dropped(
    snapshot_version: u64,
    backend: DeltaReaderBackend,
    partition_count: usize,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_execution.dropped",
        snapshot_version,
        backend = ?backend,
        partition_count,
        outcome = "dropped"
    );
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        future::pending,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use futures_util::{FutureExt, StreamExt, stream};
    use tokio::{sync::Notify, time::timeout};
    use tracing::{
        Event, Level, Metadata, Subscriber,
        span::{Attributes, Id, Record},
        subscriber::{Interest, with_default},
    };

    use super::{
        DeltaBatchStream, trace_execution_completed, trace_execution_dropped,
        trace_execution_failed, trace_execution_started, trace_planning_completed,
        trace_planning_failed, trace_planning_started,
    };
    use crate::{
        DeltaReadMetrics, DeltaReaderBackend, DeltaReaderExecutionOptions,
        error::InvalidConfigurationSnafu,
        metrics::DeltaReadMetricsConfig,
        scheduling::{
            FileAdmission, FileAdmissionFn, FileBatchStream, FileExecutor, FileReadPermit,
            PartitionStream, ScanCancellation, ScanReadLimiter,
        },
    };

    #[derive(Clone, Default)]
    struct EventFields(Arc<Mutex<Vec<Vec<String>>>>);

    impl Subscriber for EventFields {
        fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
            if metadata.target() == "delta_arrow_reader" && *metadata.level() == Level::DEBUG {
                Interest::always()
            } else {
                Interest::sometimes()
            }
        }

        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.target() == "delta_arrow_reader" && *metadata.level() == Level::DEBUG
        }

        fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let metadata = event.metadata();
            assert_eq!(metadata.target(), "delta_arrow_reader");
            self.0.lock().expect("event lock").push(
                metadata
                    .fields()
                    .iter()
                    .map(|field| field.name().to_owned())
                    .collect(),
            );
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    struct ControlledMerge {
        stream: DeltaBatchStream,
        limiter: Arc<ScanReadLimiter>,
        cancellation: ScanCancellation,
        metrics: DeltaReadMetrics,
        first_partition_gate: Arc<Notify>,
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn batch(id: i32) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(vec![id]))])
            .expect("valid test batch")
    }

    fn batch_id(batch: &RecordBatch) -> i32 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 id")
            .value(0)
    }

    fn execution_options() -> Result<DeltaReaderExecutionOptions, crate::DeltaReaderError> {
        DeltaReaderExecutionOptions::new()
            .with_native_async_prefetch_file_count_per_partition(0)?
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(2))?
            .with_output_buffer_capacity_per_partition(1)
    }

    fn metrics() -> DeltaReadMetrics {
        DeltaReadMetrics::new(DeltaReadMetricsConfig {
            snapshot_version: 7,
            reader_backend: DeltaReaderBackend::NativeAsync,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 2,
            files_planned: 2,
            files_filtered_during_planning: Some(0),
            estimated_rows: Some(4),
            estimated_bytes: Some(4),
        })
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

    fn direct_stream(
        partitions: VecDeque<PartitionStream>,
        metrics: DeltaReadMetrics,
    ) -> DeltaBatchStream {
        DeltaBatchStream {
            schema: schema(),
            metrics,
            partitions,
            predicate: None,
            projection: None,
            remaining: None,
            snapshot_version: 7,
            backend: DeltaReaderBackend::NativeAsync,
            partition_count: 2,
            started: false,
            done: false,
        }
    }

    fn controlled_merge() -> Result<ControlledMerge, Box<dyn std::error::Error>> {
        let options = execution_options()?;
        let limiter = ScanReadLimiter::new(options, 2, 2);
        let cancellation = ScanCancellation::new();
        let metrics = metrics();
        let first_partition_gate = Arc::new(Notify::new());
        let executor: FileExecutor<i32, FileBatchStream> = {
            let gate = Arc::clone(&first_partition_gate);
            Arc::new(move |task, permit, _| {
                let gate = Arc::clone(&gate);
                async move {
                    let batches = vec![batch(task), batch(task * 2)];
                    Ok(if task == 1 {
                        gated_file_stream(permit, batches, gate)
                    } else {
                        file_stream(permit, batches)
                    })
                }
                .boxed()
            })
        };
        let admission: FileAdmissionFn<i32> = Arc::new(|_: &i32| Ok(FileAdmission::Admit));
        let first = PartitionStream::new(
            vec![1],
            limiter.partition(0)?,
            options,
            admission.clone(),
            Arc::clone(&executor),
            metrics.clone(),
            cancellation.clone(),
        );
        let second = PartitionStream::new(
            vec![10],
            limiter.partition(1)?,
            options,
            admission,
            executor,
            metrics.clone(),
            cancellation.clone(),
        );

        Ok(ControlledMerge {
            stream: direct_stream(VecDeque::from([first, second]), metrics.clone()),
            limiter,
            cancellation,
            metrics,
            first_partition_gate,
        })
    }

    async fn wait_for_batches(metrics: &DeltaReadMetrics, expected: u64) {
        timeout(Duration::from_secs(5), async {
            while metrics.snapshot().batches_produced < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("batch production reached expected bound");
    }

    #[test]
    fn lifecycle_tracing_has_only_bounded_fields() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = EventFields(Arc::clone(&events));
        let error = InvalidConfigurationSnafu { reason: "test" }.build();

        let _ = tracing::subscriber::set_global_default(EventFields::default());
        with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            trace_planning_started(7, DeltaReaderBackend::NativeAsync);
            trace_planning_completed(7, DeltaReaderBackend::NativeAsync, 2);
            trace_planning_failed(7, DeltaReaderBackend::NativeAsync, &error);
            trace_execution_started(7, DeltaReaderBackend::NativeAsync, 2);
            trace_execution_completed(7, DeltaReaderBackend::NativeAsync, 2);
            trace_execution_failed(7, DeltaReaderBackend::NativeAsync, 2, &error);
            trace_execution_dropped(7, DeltaReaderBackend::NativeAsync, 2);
        });
        tracing::callsite::rebuild_interest_cache();

        let events = events.lock().expect("event lock");
        assert_eq!(events.len(), 7);
        let allowed = [
            "backend",
            "error_phase",
            "error_variant",
            "event",
            "outcome",
            "partition_count",
            "snapshot_version",
        ];
        for fields in events.iter() {
            assert!(fields.iter().all(|field| allowed.contains(&field.as_str())));
            assert!(fields.contains(&"event".to_owned()));
            assert!(fields.contains(&"snapshot_version".to_owned()));
            assert!(fields.contains(&"backend".to_owned()));
            assert!(fields.contains(&"partition_count".to_owned()));
            assert!(fields.contains(&"outcome".to_owned()));
        }
    }

    #[tokio::test]
    async fn merged_stream_is_ordered_and_bounds_later_partition_queues()
    -> Result<(), Box<dyn std::error::Error>> {
        let ControlledMerge {
            mut stream,
            limiter,
            metrics,
            first_partition_gate,
            ..
        } = controlled_merge()?;

        let first = stream.next().await.ok_or("first batch missing")??;
        assert_eq!(batch_id(&first), 1);
        wait_for_batches(&metrics, 2).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(metrics.snapshot().batches_produced, 2);
        assert_eq!(limiter.active_file_reads(), 2);

        first_partition_gate.notify_one();
        let mut ids = vec![batch_id(
            &stream.next().await.ok_or("second batch missing")??,
        )];
        while let Some(batch) = stream.next().await {
            ids.push(batch_id(&batch?));
        }
        assert_eq!(ids, [2, 10, 20]);
        assert_eq!(metrics.snapshot().batches_produced, 4);
        assert_eq!(metrics.snapshot().scan_partitions_completed, 2);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn merged_stream_drop_cancels_blocked_partitions_and_releases_permits()
    -> Result<(), Box<dyn std::error::Error>> {
        let ControlledMerge {
            mut stream,
            limiter,
            cancellation,
            metrics,
            ..
        } = controlled_merge()?;

        let first = stream.next().await.ok_or("first batch missing")??;
        assert_eq!(batch_id(&first), 1);
        wait_for_batches(&metrics, 2).await;
        assert_eq!(limiter.active_file_reads(), 2);
        drop(stream);

        assert!(cancellation.is_cancelled());
        timeout(Duration::from_secs(5), async {
            while limiter.active_file_reads() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(metrics.snapshot().batches_produced, 2);
        assert_eq!(metrics.snapshot().scan_partitions_completed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn merged_stream_forwards_one_concurrent_error_and_releases_permits()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = execution_options()?;
        let limiter = ScanReadLimiter::new(options, 2, 2);
        let cancellation = ScanCancellation::new();
        let metrics = metrics();
        let executor: FileExecutor<i32, FileBatchStream> = Arc::new(|task, permit, _| {
            async move {
                Ok(if task == 1 {
                    Box::pin(stream::once(async move {
                        let _permit = permit;
                        pending::<Result<RecordBatch, crate::DeltaReaderError>>().await
                    })) as FileBatchStream
                } else {
                    Box::pin(stream::once(async move {
                        let _permit = permit;
                        Err(InvalidConfigurationSnafu {
                            reason: "controlled_partition_failure",
                        }
                        .build())
                    })) as FileBatchStream
                })
            }
            .boxed()
        });
        let admission = Arc::new(|_: &i32| Ok(FileAdmission::Admit));
        let first = PartitionStream::new(
            vec![1],
            limiter.partition(0)?,
            options,
            admission.clone(),
            Arc::clone(&executor),
            metrics.clone(),
            cancellation.clone(),
        );
        let second = PartitionStream::new(
            vec![2],
            limiter.partition(1)?,
            options,
            admission,
            executor,
            metrics.clone(),
            cancellation.clone(),
        );
        let mut stream = direct_stream(VecDeque::from([first, second]), metrics);

        let error = timeout(Duration::from_secs(5), stream.next())
            .await?
            .ok_or("error item missing")?
            .expect_err("controlled partition must fail");
        assert_eq!(error.as_str(), "invalid_configuration");
        assert!(stream.next().await.is_none());
        assert!(cancellation.is_cancelled());
        timeout(Duration::from_secs(5), async {
            while limiter.active_file_reads() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }
}
