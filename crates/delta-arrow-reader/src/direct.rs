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
    kernel::delta_predicate_to_kernel_pruning,
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
        LoadedDeltaTableSnapshot, load_delta_table_snapshot_async,
        load_delta_table_snapshot_blocking,
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
pub struct DeltaScan {
    plan: Arc<DeltaScanPlan>,
    predicate: Option<DeltaPredicate>,
    limit: Option<usize>,
    partition_count: usize,
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
                DeltaReaderBackend::NativeAsync => native_async_executor(&self.plan)?,
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
    #[cfg(not(feature = "native-async"))]
    if options.reader_backend() == DeltaReaderBackend::NativeAsync {
        return crate::error::UnsupportedBackendSnafu {
            reason: "native_async_feature_disabled",
        }
        .fail();
    }
    Ok(())
}

#[cfg(feature = "native-async")]
fn native_async_executor(
    plan: &Arc<DeltaScanPlan>,
) -> Result<FileExecutor<crate::planning::DeltaScanFileTask, FileBatchStream>, DeltaReaderError> {
    Ok(crate::native_async_reader::native_async_file_executor(
        plan, None,
    ))
}

#[cfg(not(feature = "native-async"))]
fn native_async_executor(
    _plan: &Arc<DeltaScanPlan>,
) -> Result<FileExecutor<crate::planning::DeltaScanFileTask, FileBatchStream>, DeltaReaderError> {
    crate::error::UnsupportedBackendSnafu {
        reason: "native_async_feature_disabled",
    }
    .fail()
}

#[cfg(feature = "official-kernel")]
fn official_kernel_executor(
    plan: &Arc<DeltaScanPlan>,
) -> Result<FileExecutor<crate::planning::DeltaScanFileTask, FileBatchStream>, DeltaReaderError> {
    Ok(crate::official_kernel_reader::official_kernel_file_executor(plan))
}

#[cfg(not(feature = "official-kernel"))]
fn official_kernel_executor(
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
    use std::sync::{Arc, Mutex};

    use tracing::{
        Event, Metadata, Subscriber,
        span::{Attributes, Id, Record},
        subscriber::with_default,
    };

    use super::{
        trace_execution_completed, trace_execution_dropped, trace_execution_failed,
        trace_execution_started, trace_planning_completed, trace_planning_failed,
        trace_planning_started,
    };
    use crate::{DeltaReaderBackend, error::InvalidConfigurationSnafu};

    #[derive(Clone)]
    struct EventFields(Arc<Mutex<Vec<Vec<String>>>>);

    impl Subscriber for EventFields {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
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

    #[test]
    fn lifecycle_tracing_has_only_bounded_fields() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = EventFields(Arc::clone(&events));
        let error = InvalidConfigurationSnafu { reason: "test" }.build();

        with_default(subscriber, || {
            trace_planning_started(7, DeltaReaderBackend::NativeAsync);
            trace_planning_completed(7, DeltaReaderBackend::NativeAsync, 2);
            trace_planning_failed(7, DeltaReaderBackend::NativeAsync, &error);
            trace_execution_started(7, DeltaReaderBackend::NativeAsync, 2);
            trace_execution_completed(7, DeltaReaderBackend::NativeAsync, 2);
            trace_execution_failed(7, DeltaReaderBackend::NativeAsync, 2, &error);
            trace_execution_dropped(7, DeltaReaderBackend::NativeAsync, 2);
        });

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
}
