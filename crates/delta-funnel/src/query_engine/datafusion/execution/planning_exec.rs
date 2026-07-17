//! DataFusion physical execution plan for Delta scans.

use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DataFusionResult, config::ConfigOptions};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures_util::{StreamExt, stream::FuturesOrdered};
use tokio::sync::mpsc::Sender;

use crate::DeltaFunnelError;
use crate::error::DeltaScanFileReadPhase;
use crate::table_formats::{DeltaKernelPredicate, KernelScanReadSchema};

use super::super::operator_activity::{
    current_operator_activity_context, profile_operator_activity_future,
    profile_operator_activity_sync, profile_operator_activity_sync_result,
};
use super::super::planning::dynamic_filters::{
    DeltaDynamicFilterOutcome, DeltaDynamicFilterPlan, DeltaRetainedDynamicFilter,
};
use super::super::planning::dynamic_partition_pruning::{
    DeltaDynamicPartitionKeepReason, DeltaDynamicPartitionPruningDecision,
    evaluate_dynamic_partition_filter,
};
use super::super::planning::file_task::DeltaScanFileTask;
use super::super::planning::file_task_partition::DeltaScanFileTaskPartitionPlan;
use super::super::planning::partition_target::DeltaScanPartitionTargetDecision;
use super::super::planning::scan_plan::ProviderScanPlan;
use super::async_scheduler::{
    DeltaProviderAsyncFileReadFuture, DeltaProviderAsyncPartitionReadScheduler,
    DeltaProviderAsyncPartitionReadSchedulerConfig,
};
use super::file_reader::{DeltaFileReadDeletionVectorStats, DeltaFileReadRequest};
use super::native_async_reader::{
    DeltaNativeAsyncFileReadStream, DeltaNativeAsyncFileReader, DeltaNativeAsyncFileReaderConfig,
    DeltaNativeAsyncPartitionFileReader,
};
use super::read_stats::{DeltaProviderReadStats, DeltaProviderReadStatsConfig};
use super::reader_backend::{
    DeltaProviderReaderBackendConfig, DeltaScanPartitionFileReader, build_partition_file_reader,
};
use super::scheduling::{
    DeltaProviderAsyncPartitionReadLimiter, DeltaProviderAsyncReadLimiter,
    DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions,
    DeltaProviderSyncPartitionReadLimiter, DeltaProviderSyncReadLimiter,
};

pub(crate) struct DeltaScanPlanningExec {
    scan_plan: Arc<ProviderScanPlan>,
    partition_plan: Arc<DeltaScanFileTaskPartitionPlan>,
    partition_target_decision: DeltaScanPartitionTargetDecision,
    execution_options: DeltaProviderScanExecutionOptions,
    sync_read_limiter: Arc<DeltaProviderSyncReadLimiter>,
    async_read_limiter: Arc<DeltaProviderAsyncReadLimiter>,
    read_stats: Arc<DeltaProviderReadStats>,
    properties: Arc<PlanProperties>,
    dynamic_filters: Arc<[DeltaRetainedDynamicFilter]>,
}

impl DeltaScanPlanningExec {
    pub(crate) fn new(
        scan_plan: ProviderScanPlan,
        partition_plan: DeltaScanFileTaskPartitionPlan,
        partition_target_decision: DeltaScanPartitionTargetDecision,
        execution_options: DeltaProviderScanExecutionOptions,
    ) -> Self {
        // Empty Delta scans keep the grouped plan's zero partitions. DataFusion
        // accepts this at physical planning time, and it avoids inventing empty
        // read work for a source with no selected files.
        let partition_count = partition_plan.partitions.len();
        let scan_plan = Arc::new(scan_plan);
        let partition_plan = Arc::new(partition_plan);
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&scan_plan.projected_schema)),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);

        let sync_read_limiter =
            DeltaProviderSyncReadLimiter::new(execution_options, partition_plan.partitions.len());
        let async_read_limiter =
            DeltaProviderAsyncReadLimiter::new(execution_options, partition_plan.partitions.len());
        let read_stats = Arc::new(DeltaProviderReadStats::new(DeltaProviderReadStatsConfig {
            source_name: scan_plan.source_name.clone(),
            snapshot_version: scan_plan.snapshot_version,
            reader_backend: execution_options.reader_backend,
            scan_metadata_exhausted: Some(partition_plan.scan_metadata_exhausted),
            scan_partitions_planned: partition_plan.partitions.len(),
            files_planned: partition_plan
                .partitions
                .iter()
                .map(|partition| partition.file_tasks.len())
                .sum(),
            files_filtered_during_planning: partition_plan.files_filtered_during_planning,
            estimated_rows: partition_plan.estimated_rows,
            estimated_bytes: partition_plan.estimated_bytes,
        }));

        Self {
            scan_plan,
            partition_plan,
            partition_target_decision,
            execution_options,
            sync_read_limiter,
            async_read_limiter,
            read_stats,
            properties: Arc::new(properties),
            dynamic_filters: Arc::from([]),
        }
    }

    fn with_dynamic_filters(
        &self,
        dynamic_filters: Vec<DeltaRetainedDynamicFilter>,
    ) -> Arc<dyn ExecutionPlan> {
        // DataFusion dynamic filters are runtime state shared by `Arc`.
        // Preserve every existing scan-planning handle and only replace the
        // retained filter set so the producer and scan consumer stay connected.
        Arc::new(Self {
            scan_plan: Arc::clone(&self.scan_plan),
            partition_plan: Arc::clone(&self.partition_plan),
            partition_target_decision: self.partition_target_decision,
            execution_options: self.execution_options,
            sync_read_limiter: Arc::clone(&self.sync_read_limiter),
            async_read_limiter: Arc::clone(&self.async_read_limiter),
            read_stats: Arc::clone(&self.read_stats),
            properties: Arc::clone(&self.properties),
            dynamic_filters: Arc::from(dynamic_filters),
        })
    }

    #[cfg(test)]
    pub(crate) fn scan_plan(&self) -> &ProviderScanPlan {
        self.scan_plan.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn partition_plan(&self) -> &DeltaScanFileTaskPartitionPlan {
        self.partition_plan.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn partition_target_decision(&self) -> DeltaScanPartitionTargetDecision {
        self.partition_target_decision
    }

    #[cfg(test)]
    pub(crate) fn execution_options(&self) -> DeltaProviderScanExecutionOptions {
        self.execution_options
    }

    #[cfg(test)]
    pub(crate) fn active_async_file_reads(&self) -> usize {
        self.async_read_limiter.active_file_reads()
    }

    #[cfg(test)]
    pub(crate) fn dynamic_filters(&self) -> &[DeltaRetainedDynamicFilter] {
        &self.dynamic_filters
    }

    /// Returns a cheap point-in-time snapshot of provider-owned read progress.
    ///
    /// The returned values do not update. Callers that need a later snapshot
    /// retain `read_stats_handle` instead.
    #[allow(dead_code)]
    pub(crate) fn read_stats_snapshot(&self) -> super::read_stats::DeltaProviderReadStatsSnapshot {
        self.read_stats.snapshot()
    }

    /// Returns the shared counters used to create later read stats snapshots.
    pub(crate) fn read_stats_handle(&self) -> Arc<DeltaProviderReadStats> {
        Arc::clone(&self.read_stats)
    }
}

impl fmt::Debug for DeltaScanPlanningExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaScanPlanningExec")
            .field("source_name", &self.scan_plan.source_name)
            .field("snapshot_version", &self.scan_plan.snapshot_version)
            .field("scan_projection", &self.scan_plan.scan_projection)
            .field(
                "partition_target",
                &self.partition_target_decision.target_partitions,
            )
            .field(
                "partition_target_source",
                &self.partition_target_decision.source,
            )
            .field("partition_count", &self.partition_plan.partitions.len())
            .field("execution_options", &self.execution_options)
            .field(
                "pushed_filter_count",
                &self.scan_plan.pushed_filter_plan.pushed_filter_count,
            )
            .field("dynamic_filter_count", &self.dynamic_filters.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DeltaScanPlanningExec {
    fn fmt_as(
        &self,
        display_type: DisplayFormatType,
        formatter: &mut fmt::Formatter,
    ) -> fmt::Result {
        match display_type {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                formatter,
                "DeltaScanPlanningExec: source={}, snapshot_version={}, projection={:?}, partitions={}",
                self.scan_plan.source_name,
                self.scan_plan.snapshot_version,
                self.scan_plan.scan_projection,
                self.partition_plan.partitions.len()
            ),
            DisplayFormatType::TreeRender => write!(formatter, "DeltaScanPlanningExec"),
        }
    }
}

impl ExecutionPlan for DeltaScanPlanningExec {
    fn name(&self) -> &str {
        "DeltaScanPlanningExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "DeltaScanPlanningExec does not accept child execution plans".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let Some(scan_partition) = self.partition_plan.partitions.get(partition) else {
            return Err(DataFusionError::Execution(format!(
                "Delta scan execution partition {partition} is out of range for {} planned partitions",
                self.partition_plan.partitions.len()
            )));
        };
        self.read_stats
            .record_datafusion_output_batch_size(context.session_config().batch_size());

        if scan_partition.file_tasks.is_empty() {
            return Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                &self.scan_plan.projected_schema,
            ))));
        }

        let read_schema = partition_read_schema(
            self.scan_plan.kernel_scan().read_schema(),
            &scan_partition.file_tasks,
            self.execution_options.reader_backend,
            self.scan_plan.provider_enforced_row_predicate.as_ref(),
        )?;

        match self.execution_options.reader_backend {
            DeltaProviderReaderBackend::OfficialKernel => {
                let file_reader = build_partition_file_reader(DeltaProviderReaderBackendConfig {
                    reader_backend: self.execution_options.reader_backend,
                    source_name: &self.scan_plan.source_name,
                    table_uri: &self.scan_plan.table_uri,
                    snapshot_version: self.scan_plan.snapshot_version,
                    storage_options: &self.scan_plan.storage_options,
                })
                .map_err(DataFusionError::from)?;
                let partition_limiter = self
                    .sync_read_limiter
                    .partition_limiter(partition)
                    .map_err(DataFusionError::from)?;

                Ok(sequential_scan_partition_stream(
                    Arc::clone(&self.scan_plan.projected_schema),
                    file_reader,
                    read_schema,
                    scan_partition.file_tasks.clone(),
                    partition_limiter,
                    self.execution_options.output_buffer_capacity_per_partition,
                    DeltaDynamicPartitionFileAdmission::new(
                        Arc::clone(&self.read_stats),
                        Arc::clone(&self.dynamic_filters),
                    ),
                ))
            }
            DeltaProviderReaderBackend::NativeAsync => {
                let activity = current_operator_activity_context();
                let file_reader =
                    DeltaNativeAsyncFileReader::try_new(DeltaNativeAsyncFileReaderConfig {
                        source_name: &self.scan_plan.source_name,
                        table_uri: &self.scan_plan.table_uri,
                        snapshot_version: self.scan_plan.snapshot_version,
                        storage_options: &self.scan_plan.storage_options,
                    })?;
                let partition_reader = Arc::new(DeltaNativeAsyncPartitionFileReader::new(
                    file_reader,
                    read_schema,
                    Arc::clone(&self.read_stats),
                    context.session_config().batch_size(),
                    activity.clone(),
                ));
                let partition_limiter = self
                    .async_read_limiter
                    .partition_limiter(partition)
                    .map_err(DataFusionError::from)?;

                Ok(native_async_scan_partition_stream(
                    Arc::clone(&self.scan_plan.projected_schema),
                    partition_reader,
                    scan_partition.file_tasks.clone(),
                    partition_limiter,
                    self.execution_options.output_buffer_capacity_per_partition,
                    self.execution_options
                        .native_async_prefetch_file_count_per_partition,
                    DeltaDynamicPartitionFileAdmission::new(
                        Arc::clone(&self.read_stats),
                        Arc::clone(&self.dynamic_filters),
                    ),
                ))
            }
        }
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DataFusionResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        // This scan is a leaf, so DataFusion offers filters here through the
        // "parent filters" side of the pushdown result. We preserve their order
        // so the returned `PushedDown` flags line up with DataFusion's inputs.
        let parent_filters = child_pushdown_result
            .parent_filters
            .iter()
            .map(|filter_result| Arc::clone(&filter_result.filter))
            .collect::<Vec<_>>();
        // Returning `No` keeps query correctness: DataFusion must continue to
        // treat unsupported filters as not consumed by this scan.
        let unsupported = || {
            FilterPushdownPropagation::with_parent_pushdown_result(vec![
                PushedDown::No;
                parent_filters.len()
            ])
        };

        // `Post` is DataFusion's late physical pushdown phase, after most
        // shape-changing physical optimizations have already run. Dynamic
        // filters are only safe to retain then because they carry runtime
        // producer/consumer state between physical operators. Before `Post`,
        // the plan may be rewritten in ways that can invalidate those links.
        // An empty input is a no-op and should not produce an updated scan node.
        if phase != FilterPushdownPhase::Post || parent_filters.is_empty() {
            return Ok(unsupported());
        }

        // Classification is intentionally narrower than static logical
        // pushdown. This hook only consumes runtime dynamic filters that
        // reference provider output columns proven to be Delta partition
        // columns for this scan.
        let dynamic_filter_plan = DeltaDynamicFilterPlan::from_filters(
            &parent_filters,
            &self.scan_plan.projected_schema,
            &self.scan_plan.partition_columns,
        );
        let accepted_count = dynamic_filter_plan.accepted_filters.len();
        self.read_stats
            .record_dynamic_filters_received(parent_filters.len());
        self.read_stats
            .record_dynamic_filters_accepted(accepted_count);
        self.read_stats.record_dynamic_filters_unsupported(
            parent_filters.len().saturating_sub(accepted_count),
        );

        // If nothing is accepted, leave the scan unchanged so later execution
        // cannot observe any new pruning state.
        if !dynamic_filter_plan.has_accepted_filters() {
            return Ok(unsupported());
        }

        // Mark only retained filters as pushed. Rejected dynamic filters and
        // ordinary static physical filters remain unsupported.
        let filters = dynamic_filter_plan
            .decisions
            .iter()
            .map(|decision| match decision.outcome {
                DeltaDynamicFilterOutcome::Accepted => PushedDown::Yes,
                DeltaDynamicFilterOutcome::Rejected => PushedDown::No,
            })
            .collect::<Vec<_>>();

        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(filters)
                .with_updated_node(self.with_dynamic_filters(dynamic_filter_plan.accepted_filters)),
        )
    }
}

#[cfg(test)]
fn prune_dynamic_partition_file_tasks(
    file_tasks: Vec<DeltaScanFileTask>,
    dynamic_filters: &[DeltaRetainedDynamicFilter],
    read_stats: &DeltaProviderReadStats,
) -> Vec<DeltaScanFileTask> {
    file_tasks
        .into_iter()
        .filter(|task| dynamic_partition_file_task_should_start(task, dynamic_filters, read_stats))
        .collect()
}

fn dynamic_partition_file_task_should_start(
    task: &DeltaScanFileTask,
    dynamic_filters: &[DeltaRetainedDynamicFilter],
    read_stats: &DeltaProviderReadStats,
) -> bool {
    if dynamic_filters.is_empty() {
        return true;
    }

    let mut not_pruned_missing_metadata = false;
    let mut not_pruned_unsupported_expression = false;

    for filter in dynamic_filters {
        read_stats.record_dynamic_filter_snapshot();
        match evaluate_dynamic_partition_filter(filter, task) {
            DeltaDynamicPartitionPruningDecision::Prune(_) => {
                read_stats.record_dynamic_partition_file_pruned();
                return false;
            }
            DeltaDynamicPartitionPruningDecision::Keep(reason) => {
                if dynamic_partition_keep_reason_is_missing_metadata(reason) {
                    not_pruned_missing_metadata = true;
                }
                if dynamic_partition_keep_reason_is_unsupported_expression(reason) {
                    not_pruned_unsupported_expression = true;
                }
            }
        }
    }

    if not_pruned_missing_metadata {
        read_stats.record_dynamic_partition_file_not_pruned_missing_metadata();
    }
    if not_pruned_unsupported_expression {
        read_stats.record_dynamic_partition_file_not_pruned_unsupported_expression();
    }
    read_stats.record_dynamic_partition_file_kept();
    true
}

#[derive(Clone)]
struct DeltaDynamicPartitionFileAdmission {
    read_stats: Arc<DeltaProviderReadStats>,
    dynamic_filters: Arc<[DeltaRetainedDynamicFilter]>,
}

impl DeltaDynamicPartitionFileAdmission {
    fn new(
        read_stats: Arc<DeltaProviderReadStats>,
        dynamic_filters: Arc<[DeltaRetainedDynamicFilter]>,
    ) -> Self {
        Self {
            read_stats,
            dynamic_filters,
        }
    }

    fn should_start(&self, task: &DeltaScanFileTask) -> bool {
        dynamic_partition_file_task_should_start(
            task,
            self.dynamic_filters.as_ref(),
            self.read_stats.as_ref(),
        )
    }
}

fn dynamic_partition_keep_reason_is_missing_metadata(
    reason: DeltaDynamicPartitionKeepReason,
) -> bool {
    matches!(
        reason,
        DeltaDynamicPartitionKeepReason::PartitionMetadataInvalid
            | DeltaDynamicPartitionKeepReason::PartitionValueMissing
            | DeltaDynamicPartitionKeepReason::PartitionValueUnparseable
    )
}

fn dynamic_partition_keep_reason_is_unsupported_expression(
    reason: DeltaDynamicPartitionKeepReason,
) -> bool {
    matches!(
        reason,
        DeltaDynamicPartitionKeepReason::SnapshotUnavailable
            | DeltaDynamicPartitionKeepReason::UnsupportedPartitionType
            | DeltaDynamicPartitionKeepReason::EvaluationFailed
            | DeltaDynamicPartitionKeepReason::NonBooleanResult
    )
}

fn project_scan_output_stream(
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
) -> SendableRecordBatchStream {
    let projected_schema = Arc::clone(&schema);
    let stream = stream.map(move |batch| {
        batch.and_then(|batch| project_batch_to_output_schema(batch, &projected_schema))
    });

    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

fn project_batch_to_output_schema(
    batch: RecordBatch,
    schema: &SchemaRef,
) -> DataFusionResult<RecordBatch> {
    let batch_schema = batch.schema();
    if batch_schema.fields().len() == schema.fields().len()
        && batch_schema
            .fields()
            .iter()
            .zip(schema.fields())
            .all(|(actual, expected)| actual.name() == expected.name())
    {
        return Ok(batch);
    }

    let projection = schema
        .fields()
        .iter()
        .map(|field| batch_schema.index_of(field.name()))
        .collect::<Result<Vec<_>, _>>()?;

    batch.project(&projection).map_err(DataFusionError::from)
}

fn partition_read_schema(
    read_schema: KernelScanReadSchema,
    file_tasks: &[DeltaScanFileTask],
    reader_backend: DeltaProviderReaderBackend,
    provider_enforced_row_predicate: Option<&DeltaKernelPredicate>,
) -> DataFusionResult<KernelScanReadSchema> {
    let has_deletion_vector_tasks = file_tasks
        .iter()
        .any(|task| task.deletion_vector.is_present());
    if provider_enforced_row_predicate.is_some()
        && has_deletion_vector_tasks
        && !reader_backend.supports_dv_row_index_predicate_reads()
    {
        return Err(DataFusionError::Execution(
            "Delta scan cannot drop an exact physical predicate for deletion-vector file tasks on a backend without original row-index accounting"
                .to_owned(),
        ));
    }

    let read_schema = if let Some(predicate) = provider_enforced_row_predicate {
        read_schema.with_provider_enforced_physical_predicate(predicate)
    } else {
        read_schema
    };

    if !reader_backend.supports_dv_row_index_predicate_reads() && has_deletion_vector_tasks {
        // Backends without original row-index accounting cannot safely align
        // predicate-pruned rows with DV row coordinates. Keep those backends on
        // the residual-filter path for DV-backed file tasks.
        Ok(read_schema.without_physical_predicate())
    } else {
        Ok(read_schema)
    }
}

fn sequential_scan_partition_stream(
    schema: SchemaRef,
    file_reader: Arc<dyn DeltaScanPartitionFileReader>,
    read_schema: KernelScanReadSchema,
    file_tasks: Vec<DeltaScanFileTask>,
    partition_limiter: DeltaProviderSyncPartitionReadLimiter,
    output_buffer_capacity: usize,
    admission: DeltaDynamicPartitionFileAdmission,
) -> SendableRecordBatchStream {
    let mut builder =
        RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), output_buffer_capacity);
    let output = builder.tx();
    let read_stats = Arc::clone(&admission.read_stats);
    let activity = current_operator_activity_context();

    let producer_activity = activity.clone();
    let producer = move || {
        read_stats.record_scan_partition_started();
        for task in file_tasks {
            if output.is_closed() {
                return Ok(());
            }
            if !admission.should_start(&task) {
                continue;
            }

            read_stats.record_file_started();
            let (file_result, _permit) = match profile_operator_activity_sync_result(
                producer_activity.as_ref(),
                "Delta scan file read",
                "delta_scan_file_read",
                || {
                    let permit = partition_limiter.acquire_file_permit();
                    let file_result = file_reader.read_file(DeltaFileReadRequest {
                        task: &task,
                        read_schema: &read_schema,
                    })?;
                    Ok((file_result, permit))
                },
            ) {
                Ok(file_result) => file_result,
                Err(error) => {
                    record_deletion_vector_read_error(read_stats.as_ref(), &error);
                    return Err(DataFusionError::from(error));
                }
            };
            record_deletion_vector_read_stats(
                read_stats.as_ref(),
                file_result.deletion_vector_stats,
            );

            for batch in file_result {
                let rows = batch.num_rows();
                if profile_operator_activity_sync(
                    producer_activity.as_ref(),
                    "Delta scan output send",
                    "delta_scan_output_send",
                    || output.blocking_send(Ok(batch)),
                )
                .is_err()
                {
                    return Ok(());
                }
                read_stats.record_batch_produced(rows);
            }
            read_stats.record_file_completed();
        }

        read_stats.record_scan_partition_completed();
        Ok(())
    };
    builder.spawn_blocking(move || {
        profile_operator_activity_sync_result(
            activity.as_ref(),
            "Delta scan producer",
            "delta_scan_producer_run",
            producer,
        )
    });

    project_scan_output_stream(builder.build(), schema)
}

fn native_async_scan_partition_stream(
    schema: SchemaRef,
    file_reader: Arc<DeltaNativeAsyncPartitionFileReader>,
    file_tasks: Vec<DeltaScanFileTask>,
    partition_limiter: DeltaProviderAsyncPartitionReadLimiter,
    output_buffer_capacity: usize,
    prefetch_file_count: usize,
    admission: DeltaDynamicPartitionFileAdmission,
) -> SendableRecordBatchStream {
    let mut builder =
        RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), output_buffer_capacity);
    let output = builder.tx();
    let read_stats = Arc::clone(&admission.read_stats);
    let activity = file_reader.activity_context();

    let producer_activity = activity.clone();
    let producer = async move {
        read_stats.record_scan_partition_started();
        let mut scheduler = DeltaProviderAsyncPartitionReadScheduler::new(
            DeltaProviderAsyncPartitionReadSchedulerConfig::new(
                file_tasks,
                file_reader,
                partition_limiter,
            )
            .with_task_admission_filter({
                let admission = admission.clone();
                Arc::new(move |task| admission.should_start(task))
            }),
        );
        if prefetch_file_count > 0 {
            return native_async_scan_partition_with_file_prefetch(
                output,
                scheduler,
                prefetch_file_count,
                read_stats,
            )
            .await;
        }

        while !output.is_closed() {
            let Some(file_stream) = scheduler.next_file().await else {
                read_stats.record_scan_partition_completed();
                return Ok(());
            };
            let mut file_stream = match file_stream {
                Ok(file_stream) => file_stream,
                Err(error) => {
                    record_deletion_vector_read_error(read_stats.as_ref(), &error);
                    return Err(DataFusionError::from(error));
                }
            };
            record_deletion_vector_read_stats(
                read_stats.as_ref(),
                file_stream.take_deletion_vector_stats(),
            );

            while !output.is_closed() {
                let next_batch = file_stream.next_batch().await;
                record_deletion_vector_read_stats(
                    read_stats.as_ref(),
                    file_stream.take_deletion_vector_stats(),
                );
                let batch = match next_batch {
                    Ok(Some(batch)) => batch,
                    Ok(None) => break,
                    Err(error) => {
                        record_deletion_vector_read_error(read_stats.as_ref(), &error);
                        return Err(DataFusionError::from(error));
                    }
                };
                let rows = batch.num_rows();
                if profile_operator_activity_future(
                    producer_activity.as_ref(),
                    "Delta scan output send",
                    "delta_scan_output_send_poll",
                    output.send(Ok(batch)),
                )
                .await
                .is_err()
                {
                    return Ok(());
                }
                read_stats.record_batch_produced(rows);
            }
            if output.is_closed() {
                return Ok(());
            }
            read_stats.record_file_completed();
        }

        Ok(())
    };
    match activity {
        Some(activity) => builder.spawn(activity.profile_future_with_async_wait(
            "Delta scan producer",
            "delta_scan_producer_poll",
            "Delta scan producer wait",
            "delta_scan_producer_wait",
            producer,
        )),
        None => builder.spawn(producer),
    }

    project_scan_output_stream(builder.build(), schema)
}

type NativeAsyncPartitionScheduler = DeltaProviderAsyncPartitionReadScheduler<
    DeltaScanFileTask,
    DeltaNativeAsyncFileReadStream,
    DeltaNativeAsyncPartitionFileReader,
>;
type NativeAsyncPrefetchQueue =
    FuturesOrdered<DeltaProviderAsyncFileReadFuture<DeltaNativeAsyncFileReadStream>>;
type NativeAsyncFileSetupResult = Result<DeltaNativeAsyncFileReadStream, DeltaFunnelError>;
type NativeAsyncReadyFileSetups = VecDeque<NativeAsyncFileSetupResult>;

enum NativeAsyncFileDrainResult {
    FileCompleted,
    OutputClosed,
}

enum NativeAsyncPrefetchPollResult {
    CurrentBatch(Result<Option<RecordBatch>, DeltaFunnelError>),
    PrefetchedFileSetupCompleted,
}

/// Executes one native async scan partition with bounded file-stream setup prefetch.
///
/// The active file still drives output ordering: batches are emitted only from
/// the current file stream. Prefetch only overlaps opening later file streams
/// and Parquet setup while the current stream is producing batches. The bound
/// is one current file plus `prefetch_file_count` additional ready or in-flight
/// file streams for this execution partition.
async fn native_async_scan_partition_with_file_prefetch(
    output: Sender<DataFusionResult<RecordBatch>>,
    mut scheduler: NativeAsyncPartitionScheduler,
    prefetch_file_count: usize,
    read_stats: Arc<DeltaProviderReadStats>,
) -> DataFusionResult<()> {
    let mut in_flight = FuturesOrdered::new();
    let mut ready_file_setups = VecDeque::new();
    // Before there is a current file, fill the queue with the first file plus
    // the configured prefetch window.
    refill_native_async_prefetch_queue(
        &mut scheduler,
        &mut in_flight,
        prefetch_file_count.saturating_add(1),
    );

    // `is_closed` is an early cancellation hint. The send result below is the
    // authoritative close check because the receiver can close after this
    // snapshot.
    while !output.is_closed() {
        let Some(file_stream) =
            take_next_native_async_file_setup(&mut ready_file_setups, &mut in_flight).await
        else {
            read_stats.record_scan_partition_completed();
            return Ok(());
        };
        refill_native_async_prefetch_queue(
            &mut scheduler,
            &mut in_flight,
            remaining_native_async_prefetch_capacity(prefetch_file_count, &ready_file_setups),
        );
        let mut file_stream = native_async_file_stream_from_result(file_stream, &read_stats)?;

        match drain_native_async_current_file_with_prefetch(
            &output,
            &mut file_stream,
            &mut scheduler,
            &mut in_flight,
            &mut ready_file_setups,
            prefetch_file_count,
            read_stats.as_ref(),
        )
        .await?
        {
            NativeAsyncFileDrainResult::FileCompleted => read_stats.record_file_completed(),
            NativeAsyncFileDrainResult::OutputClosed => return Ok(()),
        }
    }

    Ok(())
}

async fn take_next_native_async_file_setup(
    ready_file_setups: &mut NativeAsyncReadyFileSetups,
    in_flight: &mut NativeAsyncPrefetchQueue,
) -> Option<NativeAsyncFileSetupResult> {
    match ready_file_setups.pop_front() {
        Some(file_setup) => Some(file_setup),
        None => in_flight.next().await,
    }
}

async fn drain_native_async_current_file_with_prefetch(
    output: &Sender<DataFusionResult<RecordBatch>>,
    file_stream: &mut DeltaNativeAsyncFileReadStream,
    scheduler: &mut NativeAsyncPartitionScheduler,
    in_flight: &mut NativeAsyncPrefetchQueue,
    ready_file_setups: &mut NativeAsyncReadyFileSetups,
    prefetch_file_count: usize,
    read_stats: &DeltaProviderReadStats,
) -> DataFusionResult<NativeAsyncFileDrainResult> {
    // `is_closed` is an early cancellation hint. The send result below is the
    // authoritative close check because the receiver can close after this
    // snapshot.
    let activity = file_stream.activity_context();
    while !output.is_closed() {
        let next_batch = match poll_native_async_current_batch_or_prefetch_setup(
            file_stream,
            scheduler,
            in_flight,
            ready_file_setups,
            prefetch_file_count,
        )
        .await
        {
            NativeAsyncPrefetchPollResult::CurrentBatch(next_batch) => next_batch,
            NativeAsyncPrefetchPollResult::PrefetchedFileSetupCompleted => continue,
        };
        record_deletion_vector_read_stats(read_stats, file_stream.take_deletion_vector_stats());
        let batch = match next_batch {
            Ok(Some(batch)) => batch,
            Ok(None) => return Ok(NativeAsyncFileDrainResult::FileCompleted),
            Err(error) => {
                record_deletion_vector_read_error(read_stats, &error);
                return Err(DataFusionError::from(error));
            }
        };
        let rows = batch.num_rows();
        if profile_operator_activity_future(
            activity.as_ref(),
            "Delta scan output send",
            "delta_scan_output_send_poll",
            output.send(Ok(batch)),
        )
        .await
        .is_err()
        {
            return Ok(NativeAsyncFileDrainResult::OutputClosed);
        }
        read_stats.record_batch_produced(rows);
    }

    Ok(NativeAsyncFileDrainResult::OutputClosed)
}

/// Polls either the current file for one batch or one future-file setup.
///
/// When there is already a ready prefetched file, the prefetch window is full
/// enough and the current file gets polled directly. Otherwise this races the
/// current batch against the oldest in-flight file setup. If setup completes
/// first, it is saved in `ready_file_setups` and the caller should poll again
/// without emitting a batch.
async fn poll_native_async_current_batch_or_prefetch_setup(
    file_stream: &mut DeltaNativeAsyncFileReadStream,
    scheduler: &mut NativeAsyncPartitionScheduler,
    in_flight: &mut NativeAsyncPrefetchQueue,
    ready_file_setups: &mut NativeAsyncReadyFileSetups,
    prefetch_file_count: usize,
) -> NativeAsyncPrefetchPollResult {
    if !ready_file_setups.is_empty() || in_flight.is_empty() {
        return NativeAsyncPrefetchPollResult::CurrentBatch(file_stream.next_batch().await);
    }

    let next_file_setup = in_flight.next();
    tokio::pin!(next_file_setup);
    // This select is the prefetch overlap point. It polls the current file for
    // the next record batch and, at the same time, polls the oldest scheduled
    // future-file setup. If file setup wins, the resulting stream is held in
    // `ready_file_setups` until the current file finishes; no batches are read
    // from that prefetched stream yet.
    tokio::select! {
        next_batch = file_stream.next_batch() => {
            NativeAsyncPrefetchPollResult::CurrentBatch(next_batch)
        }
        completed_file_setup = &mut next_file_setup => {
            if let Some(completed_file_setup) = completed_file_setup {
                ready_file_setups.push_back(completed_file_setup);
            }
            refill_native_async_prefetch_queue(
                scheduler,
                in_flight,
                remaining_native_async_prefetch_capacity(
                    prefetch_file_count,
                    ready_file_setups,
                ),
            );
            NativeAsyncPrefetchPollResult::PrefetchedFileSetupCompleted
        }
    }
}

fn remaining_native_async_prefetch_capacity(
    prefetch_file_count: usize,
    ready_file_setups: &NativeAsyncReadyFileSetups,
) -> usize {
    // A ready prefetched stream already holds a file permit and counts against
    // the prefetch window even though it is not producing output yet.
    prefetch_file_count.saturating_sub(ready_file_setups.len())
}

/// Refills ordered native async file setup up to the requested in-flight target.
///
/// `target_in_flight` is the remaining setup capacity after subtracting any
/// already-ready prefetched file streams. Futures are kept in `FuturesOrdered`
/// so file streams are consumed in planned file order even when later setup
/// could complete first.
fn refill_native_async_prefetch_queue(
    scheduler: &mut NativeAsyncPartitionScheduler,
    in_flight: &mut NativeAsyncPrefetchQueue,
    target_in_flight: usize,
) {
    while in_flight.len() < target_in_flight {
        let Some(file) = scheduler.schedule_prefetch_file() else {
            return;
        };
        in_flight.push_back(file);
    }
}

/// Converts a scheduled file setup result into a stream and records setup-time DV stats.
fn native_async_file_stream_from_result(
    file_stream: Result<DeltaNativeAsyncFileReadStream, DeltaFunnelError>,
    read_stats: &DeltaProviderReadStats,
) -> DataFusionResult<DeltaNativeAsyncFileReadStream> {
    let mut file_stream = match file_stream {
        Ok(file_stream) => file_stream,
        Err(error) => {
            record_deletion_vector_read_error(read_stats, &error);
            return Err(DataFusionError::from(error));
        }
    };
    record_deletion_vector_read_stats(read_stats, file_stream.take_deletion_vector_stats());

    Ok(file_stream)
}

fn record_deletion_vector_read_stats(
    read_stats: &DeltaProviderReadStats,
    deletion_vector_stats: DeltaFileReadDeletionVectorStats,
) {
    if deletion_vector_stats.payload_loaded {
        read_stats.record_deletion_vector_payload_loaded();
    }
    if deletion_vector_stats.applied {
        read_stats.record_deletion_vector_applied(0);
    }
    if deletion_vector_stats.deleted_rows > 0 {
        read_stats.record_deletion_vector_rows_deleted(deletion_vector_stats.deleted_rows);
    }
}

fn record_deletion_vector_read_error(
    read_stats: &DeltaProviderReadStats,
    error: &DeltaFunnelError,
) {
    match error {
        DeltaFunnelError::DeltaScanDeletionVector { .. } => {
            read_stats.record_deletion_vector_failure();
        }
        DeltaFunnelError::DeltaScanFileRead {
            phase: DeltaScanFileReadPhase::DeletionVectorMasking,
            ..
        } => {
            read_stats.record_deletion_vector_failure();
        }
        DeltaFunnelError::DeltaScanFileRead {
            phase: DeltaScanFileReadPhase::DeletionVectorPredicateRejection,
            ..
        } => {
            read_stats.record_deletion_vector_rejection();
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs::{self, File};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::{Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::ScalarValue;
    use datafusion::common::config::ConfigOptions;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::Operator;
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_expr::expressions::{
        BinaryExpr, Column, DynamicFilterPhysicalExpr, in_list, is_not_null, is_null,
        lit as physical_lit,
    };
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::filter_pushdown::{
        ChildFilterPushdownResult, ChildPushdownResult, FilterPushdownPhase, PushedDown,
    };
    use datafusion::prelude::{SessionConfig, SessionContext};
    use delta_kernel::actions::deletion_vector::{
        DeletionVectorDescriptor, DeletionVectorStorageType,
    };
    use futures_util::StreamExt;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::super::file_reader::{DeltaFileReadRequest, DeltaFileReadResult};
    use super::{
        DeltaProviderScanExecutionOptions, DeltaProviderSyncReadLimiter,
        DeltaScanPartitionFileReader,
    };
    use crate::error::{DeltaScanDeletionVectorPhase, DeltaScanFileReadPhase};
    use crate::query_engine::datafusion::catalog::registration::{
        DeltaTableProviderConfig, register_delta_sources,
        register_delta_sources_with_scan_execution_options,
    };
    use crate::query_engine::datafusion::execution::DeltaProviderReaderBackend;
    use crate::query_engine::datafusion::execution::read_stats::{
        DeltaProviderReadStats, DeltaProviderReadStatsConfig, DeltaProviderReadStatsSnapshot,
    };
    use crate::query_engine::datafusion::planning::dynamic_filters::DeltaDynamicFilterPlan;
    use crate::query_engine::datafusion::planning::file_task::DeltaScanFileTask;
    use crate::query_engine::datafusion::test_support::{
        DEFAULT_SCHEMA_FIELDS_JSON, DeltaLogTable, PARTITIONED_SCHEMA_FIELDS_JSON,
        find_delta_scan_plans, register_fixture_source,
    };
    use crate::table_formats::{
        KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata, RealParquetDeltaTable,
        build_projected_predicated_stats_delta_scan, datafusion_expr_to_kernel_predicate,
    };
    use crate::{
        DeltaSourceConfig, QueryOptions, datafusion_session_context, load_delta_source,
        preflight_delta_protocol,
    };

    const TWO_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

    async fn collect_sql_with_reader_backend(
        table_uri: &str,
        reader_backend: DeltaProviderReaderBackend,
        sql: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let (formatted, _read_stats) =
            collect_sql_with_reader_backend_and_stats(table_uri, reader_backend, sql).await?;

        Ok(formatted)
    }

    async fn collect_sql_with_reader_backend_and_stats(
        table_uri: &str,
        reader_backend: DeltaProviderReaderBackend,
        sql: &str,
    ) -> Result<(String, DeltaProviderReadStatsSnapshot), Box<dyn std::error::Error>> {
        let (result, read_stats) =
            collect_batches_with_reader_backend_and_stats(table_uri, reader_backend, sql).await?;

        Ok((pretty_format_batches(&result)?.to_string(), read_stats))
    }

    async fn collect_batches_with_reader_backend_and_stats(
        table_uri: &str,
        reader_backend: DeltaProviderReaderBackend,
        sql: &str,
    ) -> Result<(Vec<RecordBatch>, DeltaProviderReadStatsSnapshot), Box<dyn std::error::Error>>
    {
        let ctx = SessionContext::new();
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table_uri.to_owned(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options =
            DeltaProviderScanExecutionOptions::try_new_with_reader_backend(reader_backend, 1, 1)?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            reader_backend
        );

        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let read_stats = scans[0].read_stats_snapshot();

        Ok((result, read_stats))
    }

    async fn partitioned_scan_physical_plan(
        name: &str,
    ) -> Result<(DeltaLogTable, Arc<dyn ExecutionPlan>), Box<dyn std::error::Error>> {
        partitioned_scan_physical_plan_with_schema(
            name,
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
            "select id, region from orders",
        )
        .await
    }

    async fn partitioned_scan_physical_plan_with_schema(
        name: &str,
        schema_fields_json: &str,
        partition_columns_json: &str,
        add_partition_values_json: &str,
        sql: &str,
    ) -> Result<(DeltaLogTable, Arc<dyn ExecutionPlan>), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema(
            name,
            schema_fields_json,
            partition_columns_json,
            add_partition_values_json,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
        )?;

        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;

        Ok((table, physical_plan))
    }

    fn physical_column(
        name: &str,
        index: usize,
    ) -> Arc<dyn datafusion::physical_plan::PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    fn dynamic_filter(
        children: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    ) -> Arc<dyn datafusion::physical_plan::PhysicalExpr> {
        dynamic_filter_state(children)
    }

    fn dynamic_filter_state(
        children: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    ) -> Arc<DynamicFilterPhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(children, physical_lit(true)))
    }

    fn hook_input(
        filters: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    ) -> ChildPushdownResult {
        ChildPushdownResult {
            parent_filters: filters
                .into_iter()
                .map(|filter| ChildFilterPushdownResult {
                    filter,
                    child_results: Vec::new(),
                })
                .collect(),
            self_filters: Vec::new(),
        }
    }

    fn pushed_down_yes(value: PushedDown) -> bool {
        matches!(value, PushedDown::Yes)
    }

    fn retained_partition_dynamic_filter(
        dynamic: Arc<DynamicFilterPhysicalExpr>,
    ) -> Result<
        crate::query_engine::datafusion::planning::dynamic_filters::DeltaRetainedDynamicFilter,
        String,
    > {
        let provider_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, true),
        ]));
        let filter: Arc<dyn datafusion::physical_plan::PhysicalExpr> = dynamic;
        let plan = DeltaDynamicFilterPlan::from_filters(
            std::slice::from_ref(&filter),
            &provider_schema,
            &["region".to_owned()],
        );

        plan.accepted_filters
            .first()
            .cloned()
            .ok_or_else(|| "expected retained dynamic filter".to_owned())
    }

    fn fake_task_with_region(path: &str, region: &str) -> DeltaScanFileTask {
        let mut task = fake_task(path);
        task.partition_values
            .insert("region".to_owned(), region.to_owned());
        task
    }

    fn empty_dynamic_admission(
        read_stats: &Arc<DeltaProviderReadStats>,
    ) -> super::DeltaDynamicPartitionFileAdmission {
        super::DeltaDynamicPartitionFileAdmission::new(Arc::clone(read_stats), Arc::from([]))
    }

    fn register_allowed_regions(
        ctx: &SessionContext,
        regions: Vec<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "region",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(regions))],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("allowed_regions", Arc::new(table))?;

        Ok(())
    }

    #[tokio::test]
    async fn physical_pushdown_retains_partition_dynamic_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, physical_plan) =
            partitioned_scan_physical_plan("dynamic-filter-partition-hook").await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);

        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![dynamic_filter(vec![physical_column("region", 1)])]),
            &ConfigOptions::new(),
        )?;
        let updated_node = result
            .updated_node
            .as_ref()
            .ok_or("expected updated DeltaScanPlanningExec")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(result.filters.len(), 1);
        assert!(pushed_down_yes(result.filters[0]));
        assert_eq!(updated_scan.dynamic_filters().len(), 1);
        assert_eq!(
            updated_scan.dynamic_filters()[0].partition_columns[0].name,
            "region"
        );
        assert_eq!(
            updated_scan.dynamic_filters()[0].partition_columns[0].index,
            1
        );
        let stats = updated_scan.read_stats_snapshot();
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);

        Ok(())
    }

    #[tokio::test]
    async fn physical_pushdown_records_mixed_dynamic_filter_hook_stats()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, physical_plan) =
            partitioned_scan_physical_plan("dynamic-filter-hook-stats-mixed").await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);

        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![
                dynamic_filter(vec![physical_column("region", 1)]),
                dynamic_filter(vec![physical_column("id", 0)]),
            ]),
            &ConfigOptions::new(),
        )?;
        let updated_node = result
            .updated_node
            .as_ref()
            .ok_or("expected updated DeltaScanPlanningExec")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let stats = updated_scan.read_stats_snapshot();

        assert_eq!(result.filters.len(), 2);
        assert!(pushed_down_yes(result.filters[0]));
        assert!(!pushed_down_yes(result.filters[1]));
        assert_eq!(updated_scan.dynamic_filters().len(), 1);
        assert_eq!(stats.dynamic_filters_received, 2);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 1);

        Ok(())
    }

    #[test]
    fn dynamic_partition_pruning_without_filters_keeps_tasks_without_stats() {
        let read_stats = test_read_stats();
        let tasks = vec![
            fake_task_with_region("part-00000.parquet", "us-west"),
            fake_task_with_region("part-00001.parquet", "us-east"),
        ];

        let surviving = super::prune_dynamic_partition_file_tasks(tasks, &[], read_stats.as_ref());

        let stats = read_stats.snapshot();
        assert_eq!(surviving.len(), 2);
        assert_eq!(stats.dynamic_partition_files_pruned, 0);
        assert_eq!(stats.dynamic_partition_files_kept, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 0);
        assert_eq!(stats.dynamic_partition_files_not_pruned_missing_metadata, 0);
        assert_eq!(
            stats.dynamic_partition_files_not_pruned_unsupported_expression,
            0
        );
    }

    #[test]
    fn dynamic_partition_pruning_keeps_only_tasks_not_rejected_by_filters()
    -> Result<(), Box<dyn std::error::Error>> {
        let read_stats = test_read_stats();
        let dynamic = dynamic_filter_state(vec![physical_column("region", 1)]);
        dynamic.update(Arc::new(BinaryExpr::new(
            physical_column("region", 1),
            Operator::Eq,
            physical_lit("us-west"),
        )))?;
        let retained = retained_partition_dynamic_filter(dynamic)?;
        let tasks = vec![
            fake_task_with_region("part-00000.parquet", "us-west"),
            fake_task_with_region("part-00001.parquet", "us-east"),
            fake_task("part-00002.parquet"),
        ];

        let surviving = super::prune_dynamic_partition_file_tasks(
            tasks,
            std::slice::from_ref(&retained),
            read_stats.as_ref(),
        );

        let stats = read_stats.snapshot();
        assert_eq!(
            surviving
                .iter()
                .map(|task| task.path.as_str())
                .collect::<Vec<_>>(),
            vec!["part-00000.parquet", "part-00002.parquet"]
        );
        assert_eq!(stats.dynamic_partition_files_pruned, 1);
        assert_eq!(stats.dynamic_partition_files_kept, 2);
        assert_eq!(stats.dynamic_filter_snapshots, 3);
        assert_eq!(stats.dynamic_partition_files_not_pruned_missing_metadata, 1);
        assert_eq!(
            stats.dynamic_partition_files_not_pruned_unsupported_expression,
            0
        );
        assert_eq!(stats.files_started, 0);

        Ok(())
    }

    #[test]
    fn dynamic_partition_pruning_counts_unsupported_keep_reasons_once_per_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let read_stats = test_read_stats();
        let dynamic = dynamic_filter_state(vec![physical_column("region", 1)]);
        dynamic.update(physical_lit("not a boolean result"))?;
        let retained = retained_partition_dynamic_filter(dynamic)?;
        let tasks = vec![fake_task_with_region("part-00000.parquet", "us-west")];

        let surviving = super::prune_dynamic_partition_file_tasks(
            tasks,
            std::slice::from_ref(&retained),
            read_stats.as_ref(),
        );

        let stats = read_stats.snapshot();
        assert_eq!(
            surviving
                .iter()
                .map(|task| task.path.as_str())
                .collect::<Vec<_>>(),
            vec!["part-00000.parquet"]
        );
        assert_eq!(stats.dynamic_partition_files_pruned, 0);
        assert_eq!(stats.dynamic_partition_files_kept, 1);
        assert_eq!(stats.dynamic_filter_snapshots, 1);
        assert_eq!(stats.dynamic_partition_files_not_pruned_missing_metadata, 0);
        assert_eq!(
            stats.dynamic_partition_files_not_pruned_unsupported_expression,
            1
        );
        assert_eq!(stats.files_started, 0);

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_partition_pruning_skips_file_before_read_handoff()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema(
            "dynamic-partition-pruning-execution",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
        )?;
        let dataframe = ctx.sql("select id, region from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);

        let dynamic = dynamic_filter_state(vec![physical_column("region", 1)]);
        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![
                Arc::clone(&dynamic) as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            ]),
            &ConfigOptions::new(),
        )?;
        dynamic.update(Arc::new(BinaryExpr::new(
            physical_column("region", 1),
            Operator::Eq,
            physical_lit("us-east"),
        )))?;
        let updated_node = result.updated_node.ok_or("expected updated scan")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        let mut stream = updated_scan.execute(0, ctx.task_ctx())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let stats = updated_scan.read_stats_snapshot();

        assert!(batches.is_empty());
        assert_eq!(stats.dynamic_partition_files_pruned, 1);
        assert_eq!(stats.dynamic_partition_files_kept, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 1);
        assert_eq!(stats.files_started, 0);
        assert_eq!(stats.files_completed, 0);

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_partition_pruning_reads_kept_real_file_and_skips_rejected_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table =
            RealParquetDeltaTable::new_with_two_partition_values("dynamic-pruning-mixed-read")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;
        let dataframe = ctx
            .sql("select id, customer_name, region from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].read_stats_snapshot().files_planned, 2);

        let dynamic = dynamic_filter_state(vec![physical_column("region", 2)]);
        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![
                Arc::clone(&dynamic) as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            ]),
            &ConfigOptions::new(),
        )?;
        dynamic.update(Arc::new(BinaryExpr::new(
            physical_column("region", 2),
            Operator::Eq,
            physical_lit("us-west"),
        )))?;
        let updated_node = result.updated_node.ok_or("expected updated scan")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        let mut stream = updated_scan.execute(0, ctx.task_ctx())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let formatted = pretty_format_batches(&batches)?.to_string();
        let stats = updated_scan.read_stats_snapshot();
        let kept_file_bytes = updated_scan
            .partition_plan()
            .partitions
            .iter()
            .flat_map(|partition| &partition.file_tasks)
            .filter(|task| {
                task.partition_values.get("region").map(String::as_str) == Some("us-west")
            })
            .filter_map(|task| task.estimated_bytes)
            .sum::<u64>();

        assert_eq!(
            formatted,
            [
                "+----+---------------+---------+",
                "| id | customer_name | region  |",
                "+----+---------------+---------+",
                "| 1  | west-1        | us-west |",
                "| 2  | west-2        | us-west |",
                "+----+---------------+---------+",
            ]
            .join("\n")
        );
        assert_eq!(stats.files_planned, 2);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 1);
        assert_eq!(stats.parquet_data_file_opened_bytes, Some(kept_file_bytes));
        assert!(
            stats
                .estimated_bytes
                .is_some_and(|planned_bytes| kept_file_bytes < planned_bytes)
        );
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 2);
        assert_eq!(stats.dynamic_partition_files_pruned, 1);
        assert_eq!(stats.dynamic_partition_files_kept, 1);
        assert_eq!(stats.batches_produced, u64::try_from(batches.len())?);
        assert_eq!(stats.rows_produced, 2);
        assert_eq!(
            stats.files_planned,
            stats
                .files_started
                .saturating_add(stats.dynamic_partition_files_pruned)
        );

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_partition_unsupported_snapshot_reads_all_real_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_two_partition_values(
            "dynamic-pruning-unsupported-snapshot",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;
        let dataframe = ctx
            .sql("select id, customer_name, region from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].read_stats_snapshot().files_planned, 2);

        let dynamic = dynamic_filter_state(vec![physical_column("region", 2)]);
        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![
                Arc::clone(&dynamic) as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            ]),
            &ConfigOptions::new(),
        )?;
        dynamic.update(physical_lit("not a boolean result"))?;
        let updated_node = result.updated_node.ok_or("expected updated scan")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        let mut stream = updated_scan.execute(0, ctx.task_ctx())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let formatted = pretty_format_batches(&batches)?.to_string();
        let stats = updated_scan.read_stats_snapshot();

        assert_eq!(
            formatted,
            [
                "+----+---------------+---------+",
                "| id | customer_name | region  |",
                "+----+---------------+---------+",
                "| 1  | west-1        | us-west |",
                "| 2  | west-2        | us-west |",
                "| 3  | east-3        | us-east |",
                "| 4  | east-4        | us-east |",
                "+----+---------------+---------+",
            ]
            .join("\n")
        );
        assert_eq!(stats.files_planned, 2);
        assert_eq!(stats.files_started, 2);
        assert_eq!(stats.files_completed, 2);
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 2);
        assert_eq!(stats.dynamic_partition_files_pruned, 0);
        assert_eq!(stats.dynamic_partition_files_kept, 2);
        assert_eq!(stats.dynamic_partition_files_not_pruned_missing_metadata, 0);
        assert_eq!(
            stats.dynamic_partition_files_not_pruned_unsupported_expression,
            2
        );
        assert_eq!(stats.rows_produced, 4);

        Ok(())
    }

    #[tokio::test]
    async fn sql_join_dynamic_filter_prunes_delta_scan_partition_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
            .set_bool(
                "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
                true,
            );
        let ctx = SessionContext::new_with_config(config);
        register_allowed_regions(&ctx, vec!["us-west"])?;
        let table = RealParquetDeltaTable::new_with_two_partition_values(
            "dynamic-pruning-sql-join-filter",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;

        let dataframe = ctx
            .sql(
                "select o.id, o.customer_name, o.region \
                 from allowed_regions r \
                 join orders o on r.region = o.region \
                 order by o.id",
            )
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1, "{plan_display}");
        assert!(
            plan_display.contains("HashJoinExec"),
            "expected hash join plan with dynamic filter pushdown enabled:\n{plan_display}"
        );
        assert_eq!(scans[0].dynamic_filters().len(), 1, "{plan_display}");
        assert_eq!(scans[0].read_stats_snapshot().files_planned, 2);

        let batches =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&batches)?.to_string();
        let stats = scans[0].read_stats_snapshot();

        assert_eq!(
            formatted,
            [
                "+----+---------------+---------+",
                "| id | customer_name | region  |",
                "+----+---------------+---------+",
                "| 1  | west-1        | us-west |",
                "| 2  | west-2        | us-west |",
                "+----+---------------+---------+",
            ]
            .join("\n")
        );
        assert_eq!(stats.files_planned, 2);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 1);
        assert!(stats.files_planned > stats.files_started);
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);
        assert_eq!(stats.dynamic_partition_files_pruned, 1);
        assert_eq!(stats.dynamic_partition_files_kept, 1);
        assert_eq!(
            stats.files_planned,
            stats
                .files_started
                .saturating_add(stats.dynamic_partition_files_pruned)
        );

        Ok(())
    }

    #[tokio::test]
    async fn sql_join_dynamic_filter_kept_file_still_applies_deletion_vector()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
            .set_bool(
                "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
                true,
            );
        let ctx = SessionContext::new_with_config(config);
        register_allowed_regions(&ctx, vec!["us-west"])?;
        let table = RealParquetDeltaTable::new_with_partition_value_and_deletion_vector(
            "dynamic-pruning-kept-dv-file",
            "us-west",
            &[1],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;

        let dataframe = ctx
            .sql(
                "select o.region, o.id \
                 from allowed_regions r \
                 join orders o on r.region = o.region \
                 order by o.id",
            )
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1, "{plan_display}");
        assert_eq!(scans[0].dynamic_filters().len(), 1, "{plan_display}");
        assert_eq!(scans[0].read_stats_snapshot().files_planned, 1);

        let batches =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&batches)?.to_string();
        let stats = scans[0].read_stats_snapshot();

        assert_eq!(
            formatted,
            [
                "+---------+----+",
                "| region  | id |",
                "+---------+----+",
                "| us-west | 1  |",
                "| us-west | 3  |",
                "+---------+----+",
            ]
            .join("\n")
        );
        assert_eq!(stats.files_planned, 1);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 1);
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 1);
        assert_eq!(stats.dynamic_partition_files_pruned, 0);
        assert_eq!(stats.dynamic_partition_files_kept, 1);
        assert_eq!(stats.deletion_vector_payloads_loaded, 1);
        assert_eq!(stats.deletion_vectors_applied, 1);
        assert_eq!(stats.deletion_vector_rows_deleted, 1);
        assert_eq!(stats.deletion_vector_failures, 0);
        assert_eq!(stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn sql_join_dynamic_pruning_preserves_residual_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
            .set_bool(
                "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
                true,
            );
        let ctx = SessionContext::new_with_config(config);
        register_allowed_regions(&ctx, vec!["us-west"])?;
        let table = RealParquetDeltaTable::new_with_two_partition_values(
            "dynamic-pruning-residual-filter",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;

        let dataframe = ctx
            .sql(
                "select o.id, o.customer_name, o.region \
                 from allowed_regions r \
                 join orders o on r.region = o.region \
                 where o.customer_name like 'west-1%' \
                 order by o.id",
            )
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1, "{plan_display}");
        assert!(
            plan_display.contains("FilterExec"),
            "expected residual filter above the Delta provider scan:\n{plan_display}"
        );
        assert_eq!(scans[0].dynamic_filters().len(), 1, "{plan_display}");
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            0
        );
        assert_eq!(scans[0].read_stats_snapshot().files_planned, 2);

        let batches =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&batches)?.to_string();
        let stats = scans[0].read_stats_snapshot();

        assert_eq!(
            formatted,
            [
                "+----+---------------+---------+",
                "| id | customer_name | region  |",
                "+----+---------------+---------+",
                "| 1  | west-1        | us-west |",
                "+----+---------------+---------+",
            ]
            .join("\n")
        );
        assert_eq!(stats.files_planned, 2);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 1);
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);
        assert_eq!(stats.dynamic_partition_files_pruned, 1);
        assert_eq!(stats.dynamic_partition_files_kept, 1);
        assert_eq!(stats.rows_produced, 2);
        assert_eq!(
            stats.files_planned,
            stats
                .files_started
                .saturating_add(stats.dynamic_partition_files_pruned)
        );

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_partition_false_snapshot_prunes_all_real_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table =
            RealParquetDeltaTable::new_with_two_partition_values("dynamic-pruning-false-snapshot")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;

        let dataframe = ctx
            .sql("select id, customer_name, region from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].read_stats_snapshot().files_planned, 2);

        let dynamic = dynamic_filter_state(vec![physical_column("region", 2)]);
        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![
                Arc::clone(&dynamic) as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            ]),
            &ConfigOptions::new(),
        )?;
        dynamic.update(physical_lit(false))?;
        let updated_node = result.updated_node.ok_or("expected updated scan")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        let mut stream = updated_scan.execute(0, ctx.task_ctx())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let formatted = pretty_format_batches(&batches)?.to_string();
        let stats = updated_scan.read_stats_snapshot();

        assert_eq!(formatted, ["++", "++"].join("\n"));
        assert_eq!(stats.files_planned, 2);
        assert_eq!(stats.files_started, 0);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 2);
        assert_eq!(stats.dynamic_partition_files_pruned, 2);
        assert_eq!(stats.dynamic_partition_files_kept, 0);
        assert_eq!(
            stats.files_planned,
            stats
                .files_started
                .saturating_add(stats.dynamic_partition_files_pruned)
        );

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_partition_multi_column_conjunction_prunes_partial_matches()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table =
            RealParquetDeltaTable::new_with_two_partition_columns("dynamic-pruning-two-columns")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;

        let dataframe = ctx
            .sql("select id, customer_name, region, event_date from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].read_stats_snapshot().files_planned, 4);

        let dynamic = dynamic_filter_state(vec![
            physical_column("region", 2),
            physical_column("event_date", 3),
        ]);
        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![
                Arc::clone(&dynamic) as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            ]),
            &ConfigOptions::new(),
        )?;
        let region_match = Arc::new(BinaryExpr::new(
            physical_column("region", 2),
            Operator::Eq,
            physical_lit("us-west"),
        ));
        let date_match = Arc::new(BinaryExpr::new(
            physical_column("event_date", 3),
            Operator::Eq,
            physical_lit(ScalarValue::Date32(Some(20_454))),
        ));
        dynamic.update(Arc::new(BinaryExpr::new(
            region_match,
            Operator::And,
            date_match,
        )))?;
        let updated_node = result.updated_node.ok_or("expected updated scan")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        let mut stream = updated_scan.execute(0, ctx.task_ctx())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let formatted = pretty_format_batches(&batches)?.to_string();
        let stats = updated_scan.read_stats_snapshot();

        assert_eq!(
            formatted,
            [
                "+----+---------------+---------+------------+",
                "| id | customer_name | region  | event_date |",
                "+----+---------------+---------+------------+",
                "| 1  | west-2026-1   | us-west | 2026-01-01 |",
                "+----+---------------+---------+------------+",
            ]
            .join("\n")
        );
        assert_eq!(stats.files_planned, 4);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 1);
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 4);
        assert_eq!(stats.dynamic_partition_files_pruned, 3);
        assert_eq!(stats.dynamic_partition_files_kept, 1);
        assert_eq!(
            stats.files_planned,
            stats
                .files_started
                .saturating_add(stats.dynamic_partition_files_pruned)
        );

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_partition_multi_column_disjunction_keeps_partial_matches()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table =
            RealParquetDeltaTable::new_with_two_partition_columns("dynamic-pruning-two-column-or")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;

        let dataframe = ctx
            .sql("select id, customer_name, region, event_date from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].read_stats_snapshot().files_planned, 4);

        let dynamic = dynamic_filter_state(vec![
            physical_column("region", 2),
            physical_column("event_date", 3),
        ]);
        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![
                Arc::clone(&dynamic) as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            ]),
            &ConfigOptions::new(),
        )?;
        let region_match = Arc::new(BinaryExpr::new(
            physical_column("region", 2),
            Operator::Eq,
            physical_lit("us-west"),
        ));
        let date_match = Arc::new(BinaryExpr::new(
            physical_column("event_date", 3),
            Operator::Eq,
            physical_lit(ScalarValue::Date32(Some(20_454))),
        ));
        dynamic.update(Arc::new(BinaryExpr::new(
            region_match,
            Operator::Or,
            date_match,
        )))?;
        let updated_node = result.updated_node.ok_or("expected updated scan")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        let mut stream = updated_scan.execute(0, ctx.task_ctx())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let formatted = pretty_format_batches(&batches)?.to_string();
        let stats = updated_scan.read_stats_snapshot();

        assert_eq!(
            formatted,
            [
                "+----+---------------+---------+------------+",
                "| id | customer_name | region  | event_date |",
                "+----+---------------+---------+------------+",
                "| 1  | west-2026-1   | us-west | 2026-01-01 |",
                "| 2  | west-2025-2   | us-west | 2025-01-01 |",
                "| 3  | east-2026-3   | us-east | 2026-01-01 |",
                "+----+---------------+---------+------------+",
            ]
            .join("\n")
        );
        assert_eq!(stats.files_planned, 4);
        assert_eq!(stats.files_started, 3);
        assert_eq!(stats.files_completed, 3);
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 4);
        assert_eq!(stats.dynamic_partition_files_pruned, 1);
        assert_eq!(stats.dynamic_partition_files_kept, 3);
        assert_eq!(
            stats.files_planned,
            stats
                .files_started
                .saturating_add(stats.dynamic_partition_files_pruned)
        );

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_partition_is_null_keeps_only_null_partition()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table =
            RealParquetDeltaTable::new_with_null_partition_value("dynamic-pruning-null-region")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;

        let dataframe = ctx
            .sql("select id, customer_name, region from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].read_stats_snapshot().files_planned, 3);

        let dynamic = dynamic_filter_state(vec![physical_column("region", 2)]);
        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![
                Arc::clone(&dynamic) as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            ]),
            &ConfigOptions::new(),
        )?;
        dynamic.update(is_null(physical_column("region", 2))?)?;
        let updated_node = result.updated_node.ok_or("expected updated scan")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        let mut stream = updated_scan.execute(0, ctx.task_ctx())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let formatted = pretty_format_batches(&batches)?.to_string();
        let stats = updated_scan.read_stats_snapshot();

        assert_eq!(
            formatted,
            [
                "+----+---------------+--------+",
                "| id | customer_name | region |",
                "+----+---------------+--------+",
                "| 1  | null-region-1 |        |",
                "+----+---------------+--------+",
            ]
            .join("\n")
        );
        assert_eq!(stats.files_planned, 3);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 1);
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 3);
        assert_eq!(stats.dynamic_partition_files_pruned, 2);
        assert_eq!(stats.dynamic_partition_files_kept, 1);
        assert_eq!(stats.dynamic_partition_files_not_pruned_missing_metadata, 1);
        assert_eq!(
            stats.files_planned,
            stats
                .files_started
                .saturating_add(stats.dynamic_partition_files_pruned)
        );

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_partition_in_list_with_null_keeps_unknown_partitions()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_null_partition_value(
            "dynamic-pruning-in-list-null-region",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;

        let dataframe = ctx
            .sql("select id, customer_name, region from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].read_stats_snapshot().files_planned, 3);

        let dynamic = dynamic_filter_state(vec![physical_column("region", 2)]);
        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![
                Arc::clone(&dynamic) as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            ]),
            &ConfigOptions::new(),
        )?;
        dynamic.update(in_list(
            physical_column("region", 2),
            vec![
                physical_lit("us-west"),
                physical_lit(ScalarValue::Utf8(None)),
            ],
            &false,
            scans[0].schema().as_ref(),
        )?)?;
        let updated_node = result.updated_node.ok_or("expected updated scan")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        let mut stream = updated_scan.execute(0, ctx.task_ctx())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let formatted = pretty_format_batches(&batches)?.to_string();
        let stats = updated_scan.read_stats_snapshot();

        assert_eq!(
            formatted,
            [
                "+----+---------------+---------+",
                "| id | customer_name | region  |",
                "+----+---------------+---------+",
                "| 1  | null-region-1 |         |",
                "| 2  | west-2        | us-west |",
                "| 3  | east-3        | us-east |",
                "+----+---------------+---------+",
            ]
            .join("\n")
        );
        assert_eq!(stats.files_planned, 3);
        assert_eq!(stats.files_started, 3);
        assert_eq!(stats.files_completed, 3);
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 3);
        assert_eq!(stats.dynamic_partition_files_pruned, 0);
        assert_eq!(stats.dynamic_partition_files_kept, 3);
        assert_eq!(stats.dynamic_partition_files_not_pruned_missing_metadata, 1);
        assert_eq!(
            stats.dynamic_partition_files_not_pruned_unsupported_expression,
            0
        );
        assert_eq!(
            stats.files_planned,
            stats
                .files_started
                .saturating_add(stats.dynamic_partition_files_pruned)
        );

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_partition_is_not_null_keeps_unproven_null_partition()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_null_partition_value(
            "dynamic-pruning-non-null-region",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;

        let dataframe = ctx
            .sql("select id, customer_name, region from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].read_stats_snapshot().files_planned, 3);

        let dynamic = dynamic_filter_state(vec![physical_column("region", 2)]);
        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![
                Arc::clone(&dynamic) as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            ]),
            &ConfigOptions::new(),
        )?;
        dynamic.update(is_not_null(physical_column("region", 2))?)?;
        let updated_node = result.updated_node.ok_or("expected updated scan")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        let mut stream = updated_scan.execute(0, ctx.task_ctx())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let formatted = pretty_format_batches(&batches)?.to_string();
        let stats = updated_scan.read_stats_snapshot();

        assert_eq!(
            formatted,
            [
                "+----+---------------+---------+",
                "| id | customer_name | region  |",
                "+----+---------------+---------+",
                "| 1  | null-region-1 |         |",
                "| 2  | west-2        | us-west |",
                "| 3  | east-3        | us-east |",
                "+----+---------------+---------+",
            ]
            .join("\n")
        );
        assert_eq!(stats.files_planned, 3);
        assert_eq!(stats.files_started, 3);
        assert_eq!(stats.files_completed, 3);
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 1);
        assert_eq!(stats.dynamic_filters_unsupported, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 3);
        assert_eq!(stats.dynamic_partition_files_pruned, 0);
        assert_eq!(stats.dynamic_partition_files_kept, 3);
        // Delta Kernel exposes this null partition as absent from the string
        // metadata map. Keep it for `IS NOT NULL` because pruning requires a
        // positive proof that the file cannot match.
        assert_eq!(stats.dynamic_partition_files_not_pruned_missing_metadata, 1);
        assert_eq!(
            stats.files_planned,
            stats
                .files_started
                .saturating_add(stats.dynamic_partition_files_pruned)
        );

        Ok(())
    }

    #[tokio::test]
    async fn physical_pushdown_retains_multi_partition_dynamic_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, physical_plan) = partitioned_scan_physical_plan_with_schema(
            "dynamic-filter-multi-partition-hook",
            TWO_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["region","event_date"]"#,
            r#""partitionValues":{"region":"us-west","event_date":"2025-01-01"}"#,
            "select id, region, event_date from orders",
        )
        .await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);

        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![dynamic_filter(vec![
                physical_column("event_date", 2),
                physical_column("region", 1),
            ])]),
            &ConfigOptions::new(),
        )?;
        let updated_node = result
            .updated_node
            .as_ref()
            .ok_or("expected updated DeltaScanPlanningExec")?;
        let updated_scan = updated_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(result.filters.len(), 1);
        assert!(pushed_down_yes(result.filters[0]));
        assert_eq!(updated_scan.dynamic_filters().len(), 1);
        assert_eq!(
            updated_scan.dynamic_filters()[0]
                .partition_columns
                .iter()
                .map(|column| (column.name.as_str(), column.index))
                .collect::<Vec<_>>(),
            vec![("region", 1), ("event_date", 2)]
        );

        Ok(())
    }

    #[tokio::test]
    async fn physical_pushdown_rejects_data_dynamic_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, physical_plan) =
            partitioned_scan_physical_plan("dynamic-filter-data-rejected").await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);

        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![dynamic_filter(vec![physical_column("id", 0)])]),
            &ConfigOptions::new(),
        )?;

        assert_eq!(result.filters.len(), 1);
        assert!(!pushed_down_yes(result.filters[0]));
        assert!(result.updated_node.is_none());
        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 0);
        assert_eq!(stats.dynamic_filters_unsupported, 1);

        Ok(())
    }

    #[tokio::test]
    async fn physical_pushdown_rejects_unknown_dynamic_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, physical_plan) =
            partitioned_scan_physical_plan("dynamic-filter-unknown-rejected").await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);

        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![dynamic_filter(vec![physical_column("ghost", 99)])]),
            &ConfigOptions::new(),
        )?;

        assert_eq!(result.filters.len(), 1);
        assert!(!pushed_down_yes(result.filters[0]));
        assert!(result.updated_node.is_none());
        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 0);
        assert_eq!(stats.dynamic_filters_unsupported, 1);

        Ok(())
    }

    #[tokio::test]
    async fn physical_pushdown_rejects_mixed_partition_and_data_dynamic_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, physical_plan) =
            partitioned_scan_physical_plan("dynamic-filter-mixed-rejected").await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);

        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![dynamic_filter(vec![
                physical_column("id", 0),
                physical_column("region", 1),
            ])]),
            &ConfigOptions::new(),
        )?;

        assert_eq!(result.filters.len(), 1);
        assert!(!pushed_down_yes(result.filters[0]));
        assert!(result.updated_node.is_none());
        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 0);
        assert_eq!(stats.dynamic_filters_unsupported, 1);

        Ok(())
    }

    #[tokio::test]
    async fn physical_pushdown_rejects_non_dynamic_filter() -> Result<(), Box<dyn std::error::Error>>
    {
        let (_table, physical_plan) =
            partitioned_scan_physical_plan("dynamic-filter-static-rejected").await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);
        let filter = Arc::new(BinaryExpr::new(
            physical_column("region", 1),
            Operator::Eq,
            physical_lit("us-west"),
        ));

        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![filter]),
            &ConfigOptions::new(),
        )?;

        assert_eq!(result.filters.len(), 1);
        assert!(!pushed_down_yes(result.filters[0]));
        assert!(result.updated_node.is_none());
        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.dynamic_filters_received, 1);
        assert_eq!(stats.dynamic_filters_accepted, 0);
        assert_eq!(stats.dynamic_filters_unsupported, 1);

        Ok(())
    }

    #[tokio::test]
    async fn physical_pushdown_accepts_filters_only_in_post_phase()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, physical_plan) =
            partitioned_scan_physical_plan("dynamic-filter-pre-rejected").await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);

        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Pre,
            hook_input(vec![dynamic_filter(vec![physical_column("region", 1)])]),
            &ConfigOptions::new(),
        )?;

        assert_eq!(result.filters.len(), 1);
        assert!(!pushed_down_yes(result.filters[0]));
        assert!(result.updated_node.is_none());
        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.dynamic_filters_received, 0);
        assert_eq!(stats.dynamic_filters_accepted, 0);
        assert_eq!(stats.dynamic_filters_unsupported, 0);

        Ok(())
    }

    #[tokio::test]
    async fn physical_pushdown_preserves_dynamic_filters_across_plan_rebuild()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, physical_plan) =
            partitioned_scan_physical_plan("dynamic-filter-plan-rebuild").await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        assert_eq!(scans.len(), 1);

        let result = scans[0].handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![dynamic_filter(vec![physical_column("region", 1)])]),
            &ConfigOptions::new(),
        )?;
        let updated_node = result.updated_node.ok_or("expected updated scan")?;
        let rebuilt_node = Arc::clone(&updated_node).with_new_children(Vec::new())?;
        let rebuilt_scan = rebuilt_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let reset_node = updated_node.reset_state()?;
        let reset_scan = reset_node
            .as_any()
            .downcast_ref::<super::DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(rebuilt_scan.dynamic_filters().len(), 1);
        assert_eq!(reset_scan.dynamic_filters().len(), 1);
        let debug_display = format!("{rebuilt_scan:?}");
        assert!(
            debug_display.contains("dynamic_filter_count: 1"),
            "{debug_display}"
        );
        let plan_display = datafusion::physical_plan::displayable(rebuilt_node.as_ref())
            .one_line()
            .to_string();
        assert!(
            plan_display.contains("DeltaScanPlanningExec:"),
            "{plan_display}"
        );
        assert!(plan_display.contains("partitions="), "{plan_display}");
        assert!(!plan_display.contains("DynamicFilter"), "{plan_display}");
        assert!(
            Arc::clone(&rebuilt_node)
                .with_new_children(vec![Arc::clone(&rebuilt_node)])
                .is_err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn sql_limit_stays_above_non_reading_delta_scan() -> Result<(), Box<dyn std::error::Error>>
    {
        let ctx = SessionContext::new();
        let _table = register_fixture_source(&ctx, "orders", "limit-above-scan")?;

        let dataframe = ctx.sql("select id from orders limit 1").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(plan_display.contains("GlobalLimitExec"), "{plan_display}");
        assert!(
            plan_display.contains("DeltaScanPlanningExec"),
            "{plan_display}"
        );
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0]));
        assert_eq!(
            scans[0].execution_options().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(
            scans[0]
                .execution_options()
                .max_concurrent_file_reads_per_partition,
            3
        );
        assert_eq!(
            scans[0]
                .execution_options()
                .max_concurrent_file_reads_per_scan,
            Some(scans[0].partition_target_decision().target_partitions * 3)
        );
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.source_name, "orders");
        assert_eq!(read_stats.snapshot_version, 1);
        assert_eq!(
            read_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(read_stats.scan_metadata_exhausted, Some(true));
        assert_eq!(
            read_stats.scan_partitions_planned,
            u64::try_from(scans[0].partition_plan().partitions.len())?
        );
        assert_eq!(
            read_stats.files_planned,
            u64::try_from(
                scans[0]
                    .partition_plan()
                    .partitions
                    .iter()
                    .map(|partition| partition.file_tasks.len())
                    .sum::<usize>()
            )?
        );
        assert_eq!(
            read_stats.files_filtered_during_planning,
            scans[0].partition_plan().files_filtered_during_planning
        );
        assert_eq!(
            read_stats.estimated_rows,
            scans[0].partition_plan().estimated_rows
        );
        assert_eq!(
            read_stats.estimated_bytes,
            scans[0].partition_plan().estimated_bytes
        );
        assert_eq!(read_stats.scan_partitions_started, 0);
        assert_eq!(read_stats.files_started, 0);
        assert_eq!(read_stats.batches_produced, 0);
        assert_eq!(read_stats.rows_produced, 0);

        Ok(())
    }

    #[tokio::test]
    async fn execution_returns_no_rows_for_empty_delta_scan()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_adds(
            "empty-execution",
            DEFAULT_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            &[],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::OfficialKernel,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select * from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;

        assert_eq!(scans.len(), 1);
        assert!(result.is_empty());
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.scan_partitions_planned, 0);
        assert_eq!(read_stats.files_planned, 0);
        assert_eq!(read_stats.estimated_rows, Some(0));
        assert_eq!(read_stats.estimated_bytes, Some(0));
        assert_eq!(read_stats.scan_partitions_started, 0);
        assert_eq!(read_stats.scan_partitions_completed, 0);
        assert_eq!(read_stats.files_started, 0);
        assert_eq!(read_stats.files_completed, 0);
        assert_eq!(read_stats.batches_produced, 0);
        assert_eq!(read_stats.rows_produced, 0);
        assert_eq!(read_stats.deletion_vector_payloads_loaded, 0);
        assert_eq!(read_stats.deletion_vectors_applied, 0);
        assert_eq!(read_stats.deletion_vector_rows_deleted, 0);
        assert_eq!(read_stats.deletion_vector_failures, 0);
        assert_eq!(read_stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn execution_rejects_out_of_range_partition() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _table = register_fixture_source(&ctx, "orders", "out-of-range-partition")?;

        let dataframe = ctx.sql("select * from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        let scan = scans[0];
        let out_of_range_partition = scan.partition_plan().partitions.len();

        let result = scan.execute(out_of_range_partition, ctx.task_ctx());

        assert!(matches!(
            result,
            Err(error) if error.to_string().contains(
                "Delta scan execution partition 1 is out of range for 1 planned partitions"
            )
        ));

        Ok(())
    }

    #[tokio::test]
    async fn default_execution_reads_real_delta_file() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _table = register_real_parquet_source(&ctx, "orders", "default-execution-real-read")?;

        let dataframe = ctx
            .sql("select id, customer_name from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].execution_options().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );

        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&result)?.to_string();

        assert_eq!(
            formatted,
            [
                "+----+---------------+",
                "| id | customer_name |",
                "+----+---------------+",
                "| 1  | alice         |",
                "| 2  | bob           |",
                "| 3  |               |",
                "+----+---------------+",
            ]
            .join("\n")
        );

        Ok(())
    }

    #[tokio::test]
    async fn provider_execution_records_configured_datafusion_batch_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = datafusion_session_context(QueryOptions {
            target_partitions: Some(1),
            output_batch_size: Some(13),
        })?;
        let _table = register_real_parquet_source(&ctx, "orders", "execution-records-batch-size")?;

        let dataframe = ctx.sql("select id from orders order by id").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].read_stats_snapshot().datafusion_output_batch_size,
            None
        );

        let _result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let read_stats = scans[0].read_stats_snapshot();

        assert_eq!(read_stats.datafusion_output_batch_size, Some(13));
        assert_eq!(read_stats.scan_partitions_started, 1);

        Ok(())
    }

    #[tokio::test]
    async fn explicit_reader_backend_flows_through_provider_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_default("explicit-reader-backend")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::OfficialKernel,
            2,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].execution_options(), execution_options);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::OfficialKernel
        );

        Ok(())
    }

    #[tokio::test]
    async fn default_execution_options_resolve_scan_capacity_from_target_partitions()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_sized_adds(
            "auto-default-execution-options",
            DEFAULT_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            &[
                (r#""partitionValues":{}"#, 10),
                (r#""partitionValues":{}"#, 10),
                (r#""partitionValues":{}"#, 10),
                (r#""partitionValues":{}"#, 10),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let default_options = DeltaProviderScanExecutionOptions::default();
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(4),
            }],
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_target_decision().target_partitions, 4);
        assert_eq!(
            scans[0]
                .execution_options()
                .max_concurrent_file_reads_per_scan,
            Some(4 * default_options.max_concurrent_file_reads_per_partition)
        );

        Ok(())
    }

    #[tokio::test]
    async fn partial_execution_options_resolve_scan_capacity_from_target_partitions()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_sized_adds(
            "partial-execution-options",
            DEFAULT_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            &[
                (r#""partitionValues":{}"#, 10),
                (r#""partitionValues":{}"#, 10),
                (r#""partitionValues":{}"#, 10),
                (r#""partitionValues":{}"#, 10),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions {
            max_concurrent_file_reads_per_partition: 2,
            ..DeltaProviderScanExecutionOptions::default()
        };
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(4),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_target_decision().target_partitions, 4);
        assert_eq!(
            scans[0]
                .execution_options()
                .max_concurrent_file_reads_per_scan,
            Some(8)
        );

        Ok(())
    }

    #[tokio::test]
    async fn explicit_execution_options_keep_explicit_scan_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_sized_adds(
            "explicit-default-valued-execution-options",
            DEFAULT_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            &[
                (r#""partitionValues":{}"#, 10),
                (r#""partitionValues":{}"#, 10),
                (r#""partitionValues":{}"#, 10),
                (r#""partitionValues":{}"#, 10),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            3,
            3,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(4),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_target_decision().target_partitions, 4);
        assert_eq!(scans[0].execution_options(), execution_options);
        assert_eq!(
            scans[0]
                .execution_options()
                .max_concurrent_file_reads_per_scan,
            Some(3)
        );

        Ok(())
    }

    #[tokio::test]
    async fn native_async_backend_reads_projected_non_dv_file_through_provider_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_default("native-async-provider-read")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            execution_options,
        )?;

        let dataframe = ctx
            .sql("select customer_name from orders order by customer_name nulls last")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].execution_options(), execution_options);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );

        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&result)?.to_string();

        assert_eq!(
            formatted,
            [
                "+---------------+",
                "| customer_name |",
                "+---------------+",
                "| alice         |",
                "| bob           |",
                "|               |",
                "+---------------+",
            ]
            .join("\n")
        );
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.files_started, 1);
        assert_eq!(read_stats.files_completed, 1);
        assert_eq!(read_stats.rows_produced, 3);
        assert!(
            read_stats
                .parquet_data_file_range_get_operations
                .is_some_and(|operations| operations > 0)
        );
        assert_eq!(read_stats.parquet_data_file_full_get_operations, Some(0));
        assert!(
            read_stats
                .parquet_data_file_bytes_received
                .zip(read_stats.parquet_data_file_opened_bytes)
                .is_some_and(|(received, opened)| received < opened)
        );
        assert_eq!(
            read_stats.parquet_data_file_opened_bytes,
            read_stats.estimated_bytes
        );

        Ok(())
    }

    #[tokio::test]
    async fn native_async_backend_enforces_exact_data_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_default("native-async-residual-filter")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            execution_options,
        )?;

        let dataframe = ctx
            .sql("select customer_name from orders where id > 1")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(!plan_display.contains("FilterExec"), "{plan_display}");
        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            1
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .has_provider_enforced_row_predicate()
        );
        assert!(
            scans[0]
                .scan_plan()
                .kernel_scan()
                .read_schema()
                .has_physical_predicate()
        );

        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&result)?.to_string();

        assert_eq!(
            formatted,
            [
                "+---------------+",
                "| customer_name |",
                "+---------------+",
                "| bob           |",
                "|               |",
                "+---------------+",
            ]
            .join("\n")
        );

        Ok(())
    }

    #[tokio::test]
    async fn native_async_column_mapping_transform_emits_logical_column_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_column_mapping(
            "native-async-column-mapping-transform",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            execution_options,
        )?;

        let dataframe = ctx
            .sql("select customer_name, id from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert!(
            scans[0].partition_plan().partitions[0].file_tasks[0]
                .transform
                .is_required()
        );

        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        assert!(result.iter().all(|batch| {
            let schema = batch.schema();
            schema.fields().len() == 2
                && schema.field(0).name() == "customer_name"
                && schema.field(1).name() == "id"
                && schema.field_with_name("phys_customer_name").is_err()
                && schema.field_with_name("phys_id").is_err()
        }));
        let formatted = pretty_format_batches(&result)?.to_string();

        assert_eq!(
            formatted,
            [
                "+---------------+----+",
                "| customer_name | id |",
                "+---------------+----+",
                "| alice         | 1  |",
                "| bob           | 2  |",
                "|               | 3  |",
                "+---------------+----+",
            ]
            .join("\n")
        );

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_column_mapping_transform()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_column_mapping(
            "native-async-column-mapping-equivalence",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select customer_name, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let native = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);
        assert!(
            !native.contains("phys_customer_name") && !native.contains("phys_id"),
            "{native}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_partition_transform()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_partition_value(
            "native-async-partition-transform-equivalence",
            "us-west",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select region, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let native = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_supported_data_types()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_supported_types(
            "native-async-supported-types-equivalence",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "\
            select \
                id, \
                customer_name, \
                active, \
                payload, \
                event_date, \
                event_ts, \
                amount, \
                score_f32, \
                score_f64, \
                attributes, \
                tags \
            from orders \
            order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let native = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_mixed_timestamp_physical_types()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_mixed_timestamp_physical_types(
            "native-async-mixed-timestamp-physical-types",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "\
            select \
                id, \
                customer_name, \
                event_ts \
            from orders \
            order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let native = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_mixed_timestamp_physical_types_with_utc_nanoseconds()
    -> Result<(), Box<dyn std::error::Error>> {
        let table =
            RealParquetDeltaTable::new_with_mixed_timestamp_physical_types_with_utc_nanoseconds(
                "native-async-mixed-timestamp-physical-types-with-utc-nanoseconds",
            )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "\
            select \
                id, \
                customer_name, \
                event_ts \
            from orders \
            order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let native = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_mixed_nested_timestamp_physical_types()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_mixed_nested_timestamp_physical_types(
            "native-async-mixed-nested-timestamp-physical-types",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "\
            select \
                id, \
                profile \
            from orders \
            order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let native = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_nested_struct_name_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_reordered_nested_struct_fields(
            "native-async-nested-struct-name-fallback",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select profile, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("profile"), "{native}");
        assert!(native.contains("first_name"), "{native}");
        assert!(native.contains("age"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_nested_column_mapping_transform()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_nested_column_mapping(
            "native-async-nested-column-mapping-equivalence",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select profile, customer_name, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("first_name"), "{native}");
        assert!(native.contains("age"), "{native}");
        assert!(!native.contains("phys_profile"), "{native}");
        assert!(!native.contains("phys_first_name"), "{native}");
        assert!(!native.contains("phys_age"), "{native}");
        assert!(!native.contains("stale_first_name"), "{native}");
        assert!(!native.contains("stale_age"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_projected_nested_column_mapping()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_nested_column_mapping(
            "native-async-projected-nested-column-mapping",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select profile from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("profile"), "{native}");
        assert!(native.contains("first_name"), "{native}");
        assert!(native.contains("age"), "{native}");
        assert!(!native.contains("phys_profile"), "{native}");
        assert!(!native.contains("stale_first_name"), "{native}");
        assert!(!native.contains("stale_age"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_array_struct_name_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_reordered_array_struct_fields(
            "native-async-array-struct-name-fallback",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select addresses, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("addresses"), "{native}");
        assert!(native.contains("city"), "{native}");
        assert!(native.contains("zip"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_missing_nullable_array_struct_field()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_missing_nullable_array_struct_field(
            "native-async-missing-nullable-array-struct",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select addresses, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("country"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_array_column_mapping_transform()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_array_column_mapping(
            "native-async-array-column-mapping-equivalence",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select addresses, customer_name, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("addresses"), "{native}");
        assert!(native.contains("city"), "{native}");
        assert!(native.contains("zip"), "{native}");
        assert!(!native.contains("phys_addresses"), "{native}");
        assert!(!native.contains("phys_city"), "{native}");
        assert!(!native.contains("phys_zip"), "{native}");
        assert!(!native.contains("stale_city"), "{native}");
        assert!(!native.contains("stale_zip"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_array_struct_leaf_cast()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_array_struct_long_zip_leaf_cast(
            "native-async-array-struct-leaf-cast",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select addresses, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let native = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_rejects_missing_non_nullable_array_struct_field_before_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_missing_non_nullable_array_struct_field(
            "native-async-missing-required-array-struct",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select addresses, id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );

        let error =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await;
        let error = match error {
            Ok(_) => {
                return Err(
                    "missing native async array struct non-nullable field must fail".into(),
                );
            }
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("Arrow conversion"), "{display}");
        assert!(display.contains("non-nullable provider field"), "{display}");
        assert!(
            display.contains("addresses.element.required_country"),
            "{display}"
        );
        assert!(
            display.contains("is missing from the Parquet file"),
            "{display}"
        );
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.files_started, 1);
        assert_eq!(read_stats.files_completed, 0);
        assert_eq!(read_stats.batches_produced, 0);
        assert_eq!(read_stats.rows_produced, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_map_value_struct_name_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_reordered_map_value_struct_fields(
            "native-async-map-value-struct-name-fallback",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select attributes, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("attributes"), "{native}");
        assert!(native.contains("city"), "{native}");
        assert!(native.contains("zip"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_missing_nullable_map_value_struct_field()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_missing_nullable_map_value_struct_field(
            "native-async-missing-nullable-map-value-struct",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select attributes, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("country"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_rejects_missing_non_nullable_map_value_struct_field_before_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_missing_non_nullable_map_value_struct_field(
            "native-async-missing-required-map-value-struct",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select attributes, id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );

        let error =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await;
        let error = match error {
            Ok(_) => {
                return Err("missing native async map value non-nullable field must fail".into());
            }
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("Arrow conversion"), "{display}");
        assert!(display.contains("non-nullable provider field"), "{display}");
        assert!(
            display.contains("attributes.value.required_country"),
            "{display}"
        );
        assert!(
            display.contains("is missing from the Parquet file"),
            "{display}"
        );
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.files_started, 1);
        assert_eq!(read_stats.files_completed, 0);
        assert_eq!(read_stats.batches_produced, 0);
        assert_eq!(read_stats.rows_produced, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_map_key_leaf_cast()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_map_long_key_leaf_cast(
            "native-async-map-key-leaf-cast",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select attributes, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let native = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_map_column_mapping_transform()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_map_column_mapping(
            "native-async-map-column-mapping-equivalence",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select attributes, customer_name, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("attributes"), "{native}");
        assert!(native.contains("city"), "{native}");
        assert!(native.contains("zip"), "{native}");
        assert!(!native.contains("phys_attributes"), "{native}");
        assert!(!native.contains("phys_city"), "{native}");
        assert!(!native.contains("phys_zip"), "{native}");
        assert!(!native.contains("stale_city"), "{native}");
        assert!(!native.contains("stale_zip"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_map_key_value_column_mapping_transform()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_map_key_value_column_mapping(
            "native-async-map-key-value-column-mapping-equivalence",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select attributes, customer_name, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("attributes"), "{native}");
        assert!(native.contains("city"), "{native}");
        assert!(native.contains("zip"), "{native}");
        assert!(native.contains("label"), "{native}");
        assert!(native.contains("score"), "{native}");
        assert!(!native.contains("phys_attributes"), "{native}");
        assert!(!native.contains("phys_key_city"), "{native}");
        assert!(!native.contains("phys_key_zip"), "{native}");
        assert!(!native.contains("phys_value_label"), "{native}");
        assert!(!native.contains("phys_value_score"), "{native}");
        assert!(!native.contains("stale_key_city"), "{native}");
        assert!(!native.contains("stale_key_zip"), "{native}");
        assert!(!native.contains("stale_value_label"), "{native}");
        assert!(!native.contains("stale_value_score"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_map_list_key_struct_name_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_reordered_map_list_key_struct_fields(
            "native-async-map-list-key-struct-name-fallback",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select attributes, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("attributes"), "{native}");
        assert!(native.contains("city"), "{native}");
        assert!(native.contains("zip"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_nested_map_key_struct_name_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_reordered_nested_map_key_struct_fields(
            "native-async-nested-map-key-struct-name-fallback",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select attributes, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("attributes"), "{native}");
        assert!(native.contains("city"), "{native}");
        assert!(native.contains("zip"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_missing_nullable_nested_struct_field()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_missing_nullable_nested_struct_field(
            "native-async-missing-nullable-nested-struct",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select profile, id from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(
            native_stats.reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(native, official);
        assert!(native.contains("loyalty_tier"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_rejects_missing_non_nullable_nested_struct_field_before_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_missing_non_nullable_nested_struct_field(
            "native-async-missing-required-nested-struct",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select profile, id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );

        let error =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await;
        let error = match error {
            Ok(_) => return Err("missing native async nested non-nullable field must fail".into()),
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("Arrow conversion"), "{display}");
        assert!(display.contains("non-nullable provider field"), "{display}");
        assert!(display.contains("profile.required_code"), "{display}");
        assert!(
            display.contains("is missing from the Parquet file"),
            "{display}"
        );
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.files_started, 1);
        assert_eq!(read_stats.files_completed, 0);
        assert_eq!(read_stats.batches_produced, 0);
        assert_eq!(read_stats.rows_produced, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_missing_nullable_columns()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_missing_nullable_column(
            "native-async-missing-nullable-equivalence",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select id, customer_name, loyalty_tier from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let native = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);
        assert!(native.contains("loyalty_tier"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_rejects_missing_non_nullable_columns_before_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_missing_non_nullable_column(
            "native-async-missing-non-nullable-error",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx
            .sql("select id, customer_name, required_code from orders")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );

        let error =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await;
        let error = match error {
            Ok(_) => return Err("missing native async non-nullable column must fail".into()),
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("Arrow conversion"), "{display}");
        assert!(display.contains("non-nullable provider field"), "{display}");
        assert!(display.contains("required_code"), "{display}");
        assert!(
            display.contains("is missing from the Parquet file"),
            "{display}"
        );
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.files_started, 1);
        assert_eq!(read_stats.files_completed, 0);
        assert_eq!(read_stats.batches_produced, 0);
        assert_eq!(read_stats.rows_produced, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_name_fallback_reordering()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_reordered_physical_columns(
            "native-async-name-fallback-reordering",
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select id, customer_name from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let native = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);
        assert!(native.contains("| id | customer_name |"), "{native}");
        assert!(native.contains("| 1  | alice         |"), "{native}");

        Ok(())
    }

    #[tokio::test]
    async fn native_async_matches_official_kernel_for_simple_deletion_vector()
    -> Result<(), Box<dyn std::error::Error>> {
        let table =
            RealParquetDeltaTable::new_with_deletion_vector("native-async-simple-dv", &[1])?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select id, customer_name from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);
        assert!(native.contains("| 1  | alice         |"), "{native}");
        assert!(native.contains("| 3  |               |"), "{native}");
        assert!(!native.contains("| 2  | bob           |"), "{native}");
        assert!(
            !native.contains("__delta_funnel_original_row_index"),
            "{native}"
        );
        assert_eq!(native_stats.deletion_vector_payloads_loaded, 1);
        assert_eq!(native_stats.deletion_vectors_applied, 1);
        assert_eq!(native_stats.deletion_vector_rows_deleted, 1);
        assert_eq!(native_stats.deletion_vector_failures, 0);
        assert_eq!(native_stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_exact_predicate_can_select_only_deleted_dv_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_deletion_vector(
            "native-async-dv-exact-predicate-only-deleted",
            &[1],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders where id = 2").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(!plan_display.contains("FilterExec"), "{plan_display}");
        assert_eq!(physical_plan.schema().field(0).name(), "id");
        assert_eq!(scans.len(), 1);
        assert!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .has_provider_enforced_row_predicate()
        );

        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let ids = collect_batch_ids(&result)?;

        assert!(ids.is_empty());
        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.rows_produced, 0);
        assert_eq!(stats.deletion_vector_payloads_loaded, 1);
        assert_eq!(stats.deletion_vectors_applied, 1);
        assert_eq!(stats.deletion_vector_rows_deleted, 1);
        assert_eq!(stats.deletion_vector_failures, 0);
        assert_eq!(stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_exact_predicate_preserves_rows_not_targeted_by_dv()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_deletion_vector(
            "native-async-dv-exact-predicate-non-overlap",
            &[1],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders where id = 1").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(!plan_display.contains("FilterExec"), "{plan_display}");
        assert_eq!(physical_plan.schema().field(0).name(), "id");
        assert_eq!(scans.len(), 1);
        assert!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .has_provider_enforced_row_predicate()
        );

        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let ids = collect_batch_ids(&result)?;

        assert_eq!(ids, vec![1]);
        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.rows_produced, 1);
        assert_eq!(stats.deletion_vector_payloads_loaded, 1);
        assert_eq!(stats.deletion_vectors_applied, 1);
        assert_eq!(stats.deletion_vector_rows_deleted, 0);
        assert_eq!(stats.deletion_vector_failures, 0);
        assert_eq!(stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_exact_predicate_can_select_no_dv_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_deletion_vector(
            "native-async-dv-exact-predicate-no-rows",
            &[1],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders where id > 99").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(!plan_display.contains("FilterExec"), "{plan_display}");
        assert_eq!(physical_plan.schema().field(0).name(), "id");
        assert_eq!(scans.len(), 1);
        assert!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .has_provider_enforced_row_predicate()
        );

        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let ids = collect_batch_ids(&result)?;

        assert!(ids.is_empty());
        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.rows_produced, 0);
        assert_eq!(stats.deletion_vector_failures, 0);
        assert_eq!(stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_deletion_vector_preserves_row_indexes_across_batches()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_rows_and_deletion_vector(
            "native-async-dv-multi-batch-row-index",
            9000,
            &[8191, 8192, 8999],
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select id from orders order by id";
        let (official, _official_stats) = collect_batches_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_batches_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;
        let official_ids = collect_batch_ids(&official)?;
        let native_ids = collect_batch_ids(&native)?;

        assert_eq!(native_ids, official_ids);
        assert!(!native_ids.contains(&8192));
        assert!(!native_ids.contains(&8193));
        assert!(!native_ids.contains(&9000));
        assert!(native.iter().all(|batch| {
            let schema = batch.schema();
            schema.fields().len() == 1 && schema.field(0).name() == "id"
        }));
        assert!(native.len() > 1);
        assert_eq!(native_stats.deletion_vector_payloads_loaded, 1);
        assert_eq!(native_stats.deletion_vectors_applied, 1);
        assert_eq!(native_stats.deletion_vector_rows_deleted, 3);
        assert_eq!(native_stats.deletion_vector_failures, 0);
        assert_eq!(native_stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_deletion_vector_preserves_row_indexes_across_row_groups()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_two_row_groups_and_deletion_vector(
            "native-async-dv-multi-row-group-row-index",
            3000,
            &[2999, 3000, 5999],
        )?;
        let parquet_file = File::open(table.path().join(table.data_file_path()))?;
        let parquet_reader = SerializedFileReader::new(parquet_file)?;
        assert!(parquet_reader.metadata().num_row_groups() > 1);
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select id from orders order by id";
        let (official, _official_stats) = collect_batches_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_batches_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;
        let official_ids = collect_batch_ids(&official)?;
        let native_ids = collect_batch_ids(&native)?;

        assert_eq!(native_ids, official_ids);
        assert!(!native_ids.contains(&3000));
        assert!(!native_ids.contains(&3001));
        assert!(!native_ids.contains(&6000));
        assert!(native.iter().all(|batch| {
            let schema = batch.schema();
            schema.fields().len() == 1 && schema.field(0).name() == "id"
        }));
        assert_eq!(native_stats.deletion_vector_payloads_loaded, 1);
        assert_eq!(native_stats.deletion_vectors_applied, 1);
        assert_eq!(native_stats.deletion_vector_rows_deleted, 3);
        assert_eq!(native_stats.deletion_vector_failures, 0);
        assert_eq!(native_stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_deletion_vector_handles_sparse_deleted_row_indexes()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_rows_and_deletion_vector(
            "native-async-dv-sparse-deleted-row-indexes",
            40_000,
            &[0, 19_999, 39_999],
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select id from orders order by id";
        let (official, _official_stats) = collect_batches_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_batches_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;
        let official_ids = collect_batch_ids(&official)?;
        let native_ids = collect_batch_ids(&native)?;

        assert_eq!(native_ids, official_ids);
        assert_eq!(native_ids.len(), 39_997);
        assert!(!native_ids.contains(&1));
        assert!(!native_ids.contains(&20_000));
        assert!(!native_ids.contains(&40_000));
        assert!(native.iter().all(|batch| {
            let schema = batch.schema();
            schema.fields().len() == 1 && schema.field(0).name() == "id"
        }));
        assert_eq!(native_stats.rows_produced, 39_997);
        assert_eq!(native_stats.deletion_vector_payloads_loaded, 1);
        assert_eq!(native_stats.deletion_vectors_applied, 1);
        assert_eq!(native_stats.deletion_vector_rows_deleted, 3);
        assert_eq!(native_stats.deletion_vector_failures, 0);
        assert_eq!(native_stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_deletion_vector_preserves_all_live_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let table =
            RealParquetDeltaTable::new_with_deletion_vector("native-async-dv-all-live", &[])?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select id, customer_name from orders order by id";
        let official = collect_sql_with_reader_backend(
            &table_uri,
            DeltaProviderReaderBackend::OfficialKernel,
            sql,
        )
        .await?;
        let (native, native_stats) = collect_sql_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert_eq!(native, official);
        assert!(native.contains("| 1  | alice         |"), "{native}");
        assert!(native.contains("| 2  | bob           |"), "{native}");
        assert!(native.contains("| 3  |               |"), "{native}");
        assert_eq!(native_stats.deletion_vector_payloads_loaded, 1);
        assert_eq!(native_stats.deletion_vectors_applied, 1);
        assert_eq!(native_stats.deletion_vector_rows_deleted, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_deletion_vector_keeps_schema_when_all_rows_deleted()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_deletion_vector(
            "native-async-dv-all-deleted",
            &[0, 1, 2],
        )?;
        let table_uri = table.path().to_string_lossy().to_string();
        let sql = "select id, customer_name from orders order by id";
        let (native, native_stats) = collect_batches_with_reader_backend_and_stats(
            &table_uri,
            DeltaProviderReaderBackend::NativeAsync,
            sql,
        )
        .await?;

        assert!(collect_batch_ids(&native)?.is_empty());
        assert!(native.iter().all(|batch| {
            let schema = batch.schema();
            schema.fields().len() == 2
                && schema.field(0).name() == "id"
                && schema.field(1).name() == "customer_name"
        }));
        assert_eq!(native_stats.deletion_vector_payloads_loaded, 1);
        assert_eq!(native_stats.deletion_vectors_applied, 1);
        assert_eq!(native_stats.deletion_vector_rows_deleted, 3);
        assert_eq!(native_stats.deletion_vector_failures, 0);
        assert_eq!(native_stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_deletion_vector_payload_error_records_failure_metric()
    -> Result<(), Box<dyn std::error::Error>> {
        const RELATIVE_DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";

        let ctx = SessionContext::new();
        let table =
            RealParquetDeltaTable::new_with_deletion_vector("native-async-dv-payload-error", &[1])?;
        fs::remove_file(table.path().join(RELATIVE_DV_FILE))?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await;
        let error = match result {
            Ok(_) => return Err("missing native async DV payload must fail".into()),
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("source `orders`"), "{display}");
        assert!(display.contains("snapshot version 1"), "{display}");
        assert!(display.contains("part-00000.parquet"), "{display}");
        assert!(
            display.contains("deletion-vector payload read"),
            "{display}"
        );
        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.batches_produced, 0);
        assert_eq!(stats.rows_produced, 0);
        assert_eq!(stats.deletion_vector_payloads_loaded, 0);
        assert_eq!(stats.deletion_vectors_applied, 0);
        assert_eq!(stats.deletion_vector_rows_deleted, 0);
        assert_eq!(stats.deletion_vector_failures, 1);
        assert_eq!(stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_backend_preserves_file_order_in_one_partition()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_two_files("native-async-file-order")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions[0].file_tasks.len(), 2);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );

        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let ids = collect_batch_ids(&result)?;

        assert_eq!(ids, vec![1, 2, 3, 4]);
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.files_started, 2);
        assert_eq!(read_stats.files_completed, 2);
        assert_eq!(read_stats.rows_produced, 4);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_backend_preserves_multi_batch_row_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_rows("native-async-multi-batch-order", 9000)?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let ids = collect_batch_ids(&result)?;
        let expected_ids = (1..=9000).collect::<Vec<_>>();

        assert!(result.len() > 1);
        assert_eq!(ids, expected_ids);
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.files_started, 1);
        assert_eq!(read_stats.files_completed, 1);
        assert_eq!(read_stats.batches_produced, u64::try_from(result.len())?);
        assert_eq!(read_stats.rows_produced, 9000);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_backend_reports_missing_file_read_setup_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema_and_sized_adds(
            "native-async-missing-file",
            DEFAULT_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            &[(r#""partitionValues":{}"#, 123)],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );

        let error =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await;
        let error = match error {
            Ok(_) => return Err("missing native async Parquet file must fail".into()),
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("source `orders`"), "{display}");
        assert!(display.contains("snapshot version 1"), "{display}");
        assert!(display.contains("part-00000.parquet"), "{display}");
        assert!(display.contains("Parquet read setup"), "{display}");
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.files_started, 1);
        assert_eq!(read_stats.files_completed, 0);
        assert_eq!(read_stats.parquet_data_file_opened_bytes, Some(123));
        assert!(
            read_stats
                .parquet_data_file_range_get_operations
                .is_some_and(|operations| operations > 0)
        );
        assert_eq!(read_stats.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(read_stats.parquet_data_file_bytes_received, Some(0));
        assert_eq!(read_stats.batches_produced, 0);
        assert_eq!(read_stats.rows_produced, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_stream_drop_stops_future_file_scheduling()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_two_large_files(
            "native-async-drop-scheduling",
            20_000,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions[0].file_tasks.len(), 2);

        let mut stream = scans[0].execute(0, ctx.task_ctx())?;
        let first = stream.next().await.ok_or("expected first batch")??;

        assert_eq!(
            first
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("expected Int32Array")?
                .value(0),
            1
        );

        drop(stream);

        for _ in 0..1000 {
            let stats = scans[0].read_stats_snapshot();
            if stats.files_started == 1
                && stats.files_completed == 0
                && scans[0].active_async_file_reads() == 0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.scan_partitions_started, 1);
        assert_eq!(stats.scan_partitions_completed, 0);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(scans[0].active_async_file_reads(), 0);
        assert!((1..=2).contains(&stats.batches_produced));
        assert!((1..=16_384).contains(&stats.rows_produced));

        Ok(())
    }

    #[tokio::test]
    async fn native_async_deletion_vector_stream_drop_records_partial_progress()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_rows_and_deletion_vector(
            "native-async-dv-drop-partial-progress",
            20_000,
            &[0, 8191, 8192, 19_999],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );

        let mut stream = scans[0].execute(0, ctx.task_ctx())?;
        let first = stream.next().await.ok_or("expected first batch")??;
        let first_ids = batch_ids(&first)?;

        assert_eq!(first_ids.first().copied(), Some(2));
        assert!(!first_ids.contains(&1));
        assert!(!first_ids.contains(&8192));

        drop(stream);

        for _ in 0..1000 {
            let stats = scans[0].read_stats_snapshot();
            if stats.files_started == 1
                && stats.files_completed == 0
                && scans[0].active_async_file_reads() == 0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.scan_partitions_started, 1);
        assert_eq!(stats.scan_partitions_completed, 0);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(scans[0].active_async_file_reads(), 0);
        assert!((1..=2).contains(&stats.batches_produced));
        assert!((1..=16_384).contains(&stats.rows_produced));
        assert_eq!(stats.deletion_vector_payloads_loaded, 1);
        assert_eq!(stats.deletion_vectors_applied, 1);
        assert!((1..=3).contains(&stats.deletion_vector_rows_deleted));
        assert_eq!(stats.deletion_vector_failures, 0);
        assert_eq!(stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_stream_backpressure_bounds_future_file_scheduling()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_two_large_files(
            "native-async-backpressure-scheduling",
            20_000,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions[0].file_tasks.len(), 2);

        let mut stream = scans[0].execute(0, ctx.task_ctx())?;
        let first = stream.next().await.ok_or("expected first batch")??;
        let first_ids = batch_ids(&first)?;
        let stats_after_first_batch = scans[0].read_stats_snapshot();

        assert_eq!(first_ids.first().copied(), Some(1));
        assert_eq!(stats_after_first_batch.files_started, 1);
        assert_eq!(stats_after_first_batch.files_completed, 0);
        assert_eq!(stats_after_first_batch.scan_partitions_completed, 0);
        assert_eq!(scans[0].active_async_file_reads(), 1);

        let remaining = datafusion::physical_plan::common::collect(stream).await?;
        let mut ids = first_ids;
        ids.extend(collect_batch_ids(&remaining)?);
        let expected_ids = (1..=40_000).collect::<Vec<_>>();

        assert_eq!(ids, expected_ids);
        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.scan_partitions_completed, 1);
        assert_eq!(stats.files_started, 2);
        assert_eq!(stats.files_completed, 2);
        assert_eq!(stats.rows_produced, 40_000);
        assert_eq!(scans[0].active_async_file_reads(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn native_async_prefetch_preserves_file_order_and_releases_permits()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table =
            RealParquetDeltaTable::new_with_two_large_files("native-async-prefetch-order", 9000)?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            2,
            2,
        )?
        .with_native_async_prefetch_file_count_per_partition(1)?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select id from orders").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let ids = collect_batch_ids(&result)?;
        let expected_ids = (1..=18_000).collect::<Vec<_>>();

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0]
                .execution_options()
                .native_async_prefetch_file_count_per_partition,
            1
        );
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions[0].file_tasks.len(), 2);
        assert_eq!(ids, expected_ids);
        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.scan_partitions_completed, 1);
        assert_eq!(stats.files_started, 2);
        assert_eq!(stats.files_completed, 2);
        assert_eq!(stats.parquet_data_file_opened_bytes, stats.estimated_bytes);
        assert_eq!(stats.rows_produced, 18_000);
        assert_eq!(scans[0].active_async_file_reads(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn projection_execution_emits_requested_columns() -> Result<(), Box<dyn std::error::Error>>
    {
        let ctx = SessionContext::new();
        let _table =
            register_real_parquet_source(&ctx, "orders", "projection-execution-real-read")?;

        let dataframe = ctx
            .sql("select customer_name from orders order by customer_name nulls last")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![1]));

        let result = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
        assert!(
            result
                .iter()
                .all(|batch| batch.schema().fields().len() == 1)
        );
        let formatted = pretty_format_batches(&result)?.to_string();

        assert_eq!(
            formatted,
            [
                "+---------------+",
                "| customer_name |",
                "+---------------+",
                "| alice         |",
                "| bob           |",
                "|               |",
                "+---------------+",
            ]
            .join("\n")
        );

        Ok(())
    }

    #[tokio::test]
    async fn partition_value_transform_materializes_logical_partition_column()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_partition_value(
            "partition-transform-real-read",
            "us-west",
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::OfficialKernel,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            execution_options,
        )?;

        let dataframe = ctx.sql("select region, id from orders order by id").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 2]));
        assert!(
            scans[0].partition_plan().partitions[0].file_tasks[0]
                .transform
                .is_required()
        );

        let result = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
        assert!(result.iter().all(|batch| {
            let schema = batch.schema();
            schema.fields().len() == 2
                && schema.field(0).name() == "region"
                && schema.field(1).name() == "id"
        }));
        let formatted = pretty_format_batches(&result)?.to_string();

        assert_eq!(
            formatted,
            [
                "+---------+----+",
                "| region  | id |",
                "+---------+----+",
                "| us-west | 1  |",
                "| us-west | 2  |",
                "| us-west | 3  |",
                "+---------+----+",
            ]
            .join("\n")
        );

        Ok(())
    }

    #[tokio::test]
    async fn column_mapping_transform_emits_logical_column_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table =
            RealParquetDeltaTable::new_with_column_mapping("column-mapping-transform-real-read")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
        )?;

        let dataframe = ctx
            .sql("select customer_name, id from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert!(
            scans[0].partition_plan().partitions[0].file_tasks[0]
                .transform
                .is_required()
        );

        let result = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
        assert!(result.iter().all(|batch| {
            let schema = batch.schema();
            schema.fields().len() == 2
                && schema.field(0).name() == "customer_name"
                && schema.field(1).name() == "id"
                && schema.field_with_name("phys_customer_name").is_err()
                && schema.field_with_name("phys_id").is_err()
        }));
        let formatted = pretty_format_batches(&result)?.to_string();

        assert_eq!(
            formatted,
            [
                "+---------------+----+",
                "| customer_name | id |",
                "+---------------+----+",
                "| alice         | 1  |",
                "| bob           | 2  |",
                "|               | 3  |",
                "+---------------+----+",
            ]
            .join("\n")
        );

        Ok(())
    }

    #[tokio::test]
    async fn residual_filter_runs_above_provider_output() -> Result<(), Box<dyn std::error::Error>>
    {
        let ctx = SessionContext::new();
        let _table = register_real_parquet_source(&ctx, "orders", "residual-filter-real-read")?;

        let dataframe = ctx
            .sql("select id, customer_name from orders where customer_name like 'a%'")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(plan_display.contains("FilterExec"), "{plan_display}");
        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            0
        );

        let result = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&result)?.to_string();

        assert_eq!(
            formatted,
            [
                "+----+---------------+",
                "| id | customer_name |",
                "+----+---------------+",
                "| 1  | alice         |",
                "+----+---------------+",
            ]
            .join("\n")
        );

        Ok(())
    }

    #[tokio::test]
    async fn grouped_partition_execution_reads_multiple_file_tasks()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_two_files("grouped-partition-real-read")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: Some(1),
            }],
        )?;

        let dataframe = ctx
            .sql("select id, customer_name from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions.len(), 1);
        assert_eq!(scans[0].partition_plan().partitions[0].file_tasks.len(), 2);

        let result = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&result)?.to_string();

        assert_eq!(
            formatted,
            [
                "+----+---------------+",
                "| id | customer_name |",
                "+----+---------------+",
                "| 1  | file-a-1      |",
                "| 2  | file-a-2      |",
                "| 3  | file-b-3      |",
                "| 4  | file-b-4      |",
                "+----+---------------+",
            ]
            .join("\n")
        );

        Ok(())
    }

    #[tokio::test]
    async fn deletion_vector_execution_filters_deleted_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table =
            RealParquetDeltaTable::new_with_deletion_vector("dv-execution-real-read", &[1])?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
        )?;

        let dataframe = ctx
            .sql("select id, customer_name from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);
        let result =
            datafusion::physical_plan::collect(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&result)?.to_string();

        assert_eq!(
            formatted,
            [
                "+----+---------------+",
                "| id | customer_name |",
                "+----+---------------+",
                "| 1  | alice         |",
                "| 3  |               |",
                "+----+---------------+",
            ]
            .join("\n")
        );
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.deletion_vector_payloads_loaded, 1);
        assert_eq!(read_stats.deletion_vectors_applied, 1);
        assert_eq!(read_stats.deletion_vector_rows_deleted, 1);
        assert_eq!(read_stats.deletion_vector_failures, 0);
        assert_eq!(read_stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn deletion_vector_execution_matches_large_file_oracle_without_helper_columns()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_rows_and_deletion_vector(
            "dv-large-file-oracle-real-read",
            1003,
            &[0, 999, 1002],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
        )?;

        let dataframe = ctx.sql("select id from orders order by id").await?;
        let result = dataframe.collect().await?;
        let ids = result
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let expected = (1..=1003)
            .filter(|id| ![1, 1000, 1003].contains(id))
            .collect::<Vec<_>>();

        assert!(result.iter().all(|batch| {
            let schema = batch.schema();
            schema.fields().len() == 1 && schema.field(0).name() == "id"
        }));
        assert_eq!(ids, expected);

        Ok(())
    }

    #[tokio::test]
    async fn deletion_vector_execution_keeps_data_filter_correct_when_read_predicate_is_gated()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_rows_and_deletion_vector(
            "dv-data-filter-read-predicate-gated",
            1003,
            &[0, 999, 1002],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::OfficialKernel,
            1,
            1,
        )?;
        register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            execution_options,
        )?;

        let id_dataframe = ctx
            .sql("select id from orders where id > 1000 order by id")
            .await?;
        let id_result = id_dataframe.collect().await?;
        let ids = id_result
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![1001, 1002]);

        let name_dataframe = ctx
            .sql("select customer_name from orders where id > 1000 order by customer_name")
            .await?;
        let name_result = name_dataframe.collect().await?;
        let formatted = pretty_format_batches(&name_result)?.to_string();

        assert!(name_result.iter().all(|batch| {
            let schema = batch.schema();
            schema.fields().len() == 1 && schema.field(0).name() == "customer_name"
        }));
        assert_eq!(
            formatted,
            [
                "+---------------+",
                "| customer_name |",
                "+---------------+",
                "| customer-1001 |",
                "| customer-1002 |",
                "+---------------+",
            ]
            .join("\n")
        );

        Ok(())
    }

    #[tokio::test]
    async fn deletion_vector_execution_preserves_transformed_logical_columns()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_partition_value_and_deletion_vector(
            "dv-partition-transform-real-read",
            "us-west",
            &[1],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
        )?;

        let dataframe = ctx.sql("select region, id from orders order by id").await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert!(
            scans[0].partition_plan().partitions[0].file_tasks[0]
                .transform
                .is_required()
        );

        let result = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
        assert!(result.iter().all(|batch| {
            let schema = batch.schema();
            schema.fields().len() == 2
                && schema.field(0).name() == "region"
                && schema.field(1).name() == "id"
        }));
        let formatted = pretty_format_batches(&result)?.to_string();

        assert_eq!(
            formatted,
            [
                "+---------+----+",
                "| region  | id |",
                "+---------+----+",
                "| us-west | 1  |",
                "| us-west | 3  |",
                "+---------+----+",
            ]
            .join("\n")
        );

        Ok(())
    }

    #[tokio::test]
    async fn sequential_stream_emits_before_exhausting_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table = RealParquetDeltaTable::new_default("incremental-stream-read-schema")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = crate::table_formats::build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let read_count = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(FakePartitionFileReader {
            read_count: Arc::clone(&read_count),
            schema: Arc::clone(&schema),
            fail_path: None,
        });
        let sync_read_limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let read_stats = Arc::new(DeltaProviderReadStats::new(DeltaProviderReadStatsConfig {
            source_name: "orders".to_owned(),
            snapshot_version: 1,
            reader_backend: DeltaProviderReaderBackend::OfficialKernel,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 1,
            files_planned: 3,
            files_filtered_during_planning: None,
            estimated_rows: Some(4),
            estimated_bytes: Some(3),
        }));
        let mut stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            read_schema,
            vec![
                fake_task("part-00000.parquet"),
                fake_task("part-00001.parquet"),
                fake_task("part-00002.parquet"),
            ],
            sync_read_limiter.partition_limiter(0)?,
            1,
            empty_dynamic_admission(&read_stats),
        );

        let first = stream.next().await.ok_or("expected first batch")??;

        assert_eq!(batch_ids(&first)?, vec![1]);
        assert!(
            read_count.load(Ordering::SeqCst) < 3,
            "stream should yield the first batch before exhausting all files"
        );

        let remaining = datafusion::physical_plan::common::collect(stream).await?;
        let remaining_ids = remaining
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(remaining_ids, vec![2, 3, 4]);
        assert_eq!(read_count.load(Ordering::SeqCst), 3);
        let stats = read_stats.snapshot();
        assert_eq!(stats.scan_partitions_started, 1);
        assert_eq!(stats.scan_partitions_completed, 1);
        assert_eq!(stats.files_started, 3);
        assert_eq!(stats.files_completed, 3);
        assert_eq!(stats.batches_produced, 4);
        assert_eq!(stats.rows_produced, 4);

        Ok(())
    }

    #[tokio::test]
    async fn sequential_stream_releases_file_permit_after_success()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table = RealParquetDeltaTable::new_default("permit-success-read-schema")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = crate::table_formats::build_projected_delta_scan(&source, None)?;
        let sync_read_limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let reader = Arc::new(FakePartitionFileReader {
            read_count: Arc::new(AtomicUsize::new(0)),
            schema: Arc::clone(&schema),
            fail_path: None,
        });
        let read_stats = test_read_stats();
        let stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![fake_task("part-00001.parquet")],
            sync_read_limiter.partition_limiter(0)?,
            1,
            empty_dynamic_admission(&read_stats),
        );

        let batches = datafusion::physical_plan::common::collect(stream).await?;

        assert_eq!(batches.len(), 1);
        assert_eq!(sync_read_limiter.active_file_reads(), 0);
        let stats = read_stats.snapshot();
        assert_eq!(stats.scan_partitions_started, 1);
        assert_eq!(stats.scan_partitions_completed, 1);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 1);
        assert_eq!(stats.batches_produced, 1);
        assert_eq!(stats.rows_produced, 1);

        Ok(())
    }

    #[tokio::test]
    async fn concurrent_partition_streams_share_read_stats()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table = RealParquetDeltaTable::new_default("multi-partition-read-stats")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = crate::table_formats::build_projected_delta_scan(&source, None)?;
        let read_count = Arc::new(AtomicUsize::new(0));
        let reader: Arc<dyn DeltaScanPartitionFileReader> = Arc::new(FakePartitionFileReader {
            read_count: Arc::clone(&read_count),
            schema: Arc::clone(&schema),
            fail_path: None,
        });
        let sync_read_limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 2);
        let read_stats = Arc::new(DeltaProviderReadStats::new(DeltaProviderReadStatsConfig {
            source_name: "orders".to_owned(),
            snapshot_version: 1,
            reader_backend: DeltaProviderReaderBackend::OfficialKernel,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 2,
            files_planned: 2,
            files_filtered_during_planning: None,
            estimated_rows: Some(3),
            estimated_bytes: Some(2),
        }));
        let stream_a = super::sequential_scan_partition_stream(
            Arc::clone(&schema),
            Arc::clone(&reader),
            scan.read_schema(),
            vec![fake_task("part-00000.parquet")],
            sync_read_limiter.partition_limiter(0)?,
            1,
            empty_dynamic_admission(&read_stats),
        );
        let stream_b = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![fake_task("part-00001.parquet")],
            sync_read_limiter.partition_limiter(1)?,
            1,
            empty_dynamic_admission(&read_stats),
        );

        let (batches_a, batches_b) = tokio::try_join!(
            datafusion::physical_plan::common::collect(stream_a),
            datafusion::physical_plan::common::collect(stream_b)
        )?;

        let mut ids = batches_a
            .iter()
            .chain(batches_b.iter())
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        ids.sort_unstable();

        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(read_count.load(Ordering::SeqCst), 2);
        assert_eq!(sync_read_limiter.active_file_reads(), 0);
        let stats = read_stats.snapshot();
        assert_eq!(stats.scan_partitions_planned, 2);
        assert_eq!(stats.files_planned, 2);
        assert_eq!(stats.scan_partitions_started, 2);
        assert_eq!(stats.scan_partitions_completed, 2);
        assert_eq!(stats.files_started, 2);
        assert_eq!(stats.files_completed, 2);
        assert_eq!(stats.batches_produced, 3);
        assert_eq!(stats.rows_produced, 3);
        assert_eq!(stats.deletion_vector_failures, 0);
        assert_eq!(stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn sequential_stream_drop_stops_future_file_scheduling()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table = RealParquetDeltaTable::new_default("stream-drop-read-schema")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = crate::table_formats::build_projected_delta_scan(&source, None)?;
        let read_count = Arc::new(AtomicUsize::new(0));
        let sync_read_limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let reader = Arc::new(FakePartitionFileReader {
            read_count: Arc::clone(&read_count),
            schema: Arc::clone(&schema),
            fail_path: None,
        });
        let read_stats = test_read_stats();
        let mut stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![
                fake_task("part-many-batches.parquet"),
                fake_task("part-00001.parquet"),
            ],
            sync_read_limiter.partition_limiter(0)?,
            1,
            empty_dynamic_admission(&read_stats),
        );

        let first = stream.next().await.ok_or("expected first batch")??;

        assert_eq!(batch_ids(&first)?, vec![1]);
        assert_eq!(read_count.load(Ordering::SeqCst), 1);

        drop(stream);

        for _ in 0..1000 {
            if sync_read_limiter.active_file_reads() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        assert_eq!(read_count.load(Ordering::SeqCst), 1);
        assert_eq!(sync_read_limiter.active_file_reads(), 0);
        let stats = read_stats.snapshot();
        assert_eq!(stats.scan_partitions_started, 1);
        assert_eq!(stats.scan_partitions_completed, 0);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 0);
        assert!((1..=2).contains(&stats.batches_produced));
        assert!((1..=2).contains(&stats.rows_produced));

        Ok(())
    }

    #[tokio::test]
    async fn sequential_stream_drop_preserves_dynamic_pruning_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table = RealParquetDeltaTable::new_default("stream-drop-dynamic-pruning")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = crate::table_formats::build_projected_delta_scan(&source, None)?;
        let read_count = Arc::new(AtomicUsize::new(0));
        let sync_read_limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let reader = Arc::new(FakePartitionFileReader {
            read_count: Arc::clone(&read_count),
            schema: Arc::clone(&schema),
            fail_path: None,
        });
        let read_stats = Arc::new(DeltaProviderReadStats::new(DeltaProviderReadStatsConfig {
            source_name: "orders".to_owned(),
            snapshot_version: 1,
            reader_backend: DeltaProviderReaderBackend::OfficialKernel,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 1,
            files_planned: 3,
            files_filtered_during_planning: None,
            estimated_rows: Some(4),
            estimated_bytes: Some(3),
        }));
        let dynamic = dynamic_filter_state(vec![physical_column("region", 1)]);
        dynamic.update(Arc::new(BinaryExpr::new(
            physical_column("region", 1),
            Operator::Eq,
            physical_lit("us-west"),
        )))?;
        let retained = retained_partition_dynamic_filter(dynamic)?;
        let admission = super::DeltaDynamicPartitionFileAdmission::new(
            Arc::clone(&read_stats),
            Arc::from([retained]),
        );
        let mut stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![
                fake_task_with_region("part-00000.parquet", "us-east"),
                fake_task_with_region("part-many-batches.parquet", "us-west"),
                fake_task_with_region("part-00001.parquet", "us-west"),
            ],
            sync_read_limiter.partition_limiter(0)?,
            1,
            admission,
        );

        let first = stream.next().await.ok_or("expected first batch")??;

        assert_eq!(batch_ids(&first)?, vec![1]);
        assert_eq!(read_count.load(Ordering::SeqCst), 1);

        drop(stream);

        for _ in 0..1000 {
            if sync_read_limiter.active_file_reads() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        assert_eq!(read_count.load(Ordering::SeqCst), 1);
        assert_eq!(sync_read_limiter.active_file_reads(), 0);
        let stats = read_stats.snapshot();
        assert_eq!(stats.files_planned, 3);
        assert_eq!(stats.scan_partitions_started, 1);
        assert_eq!(stats.scan_partitions_completed, 0);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.dynamic_filter_snapshots, 2);
        assert_eq!(stats.dynamic_partition_files_pruned, 1);
        assert_eq!(stats.dynamic_partition_files_kept, 1);
        assert_eq!(stats.dynamic_partition_files_not_pruned_missing_metadata, 0);
        assert_eq!(
            stats.dynamic_partition_files_not_pruned_unsupported_expression,
            0
        );
        assert!((1..=2).contains(&stats.batches_produced));
        assert!((1..=2).contains(&stats.rows_produced));

        Ok(())
    }

    #[tokio::test]
    async fn sequential_stream_late_dynamic_filter_prunes_only_future_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table = RealParquetDeltaTable::new_default("late-dynamic-pruning")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = crate::table_formats::build_projected_delta_scan(&source, None)?;
        let read_count = Arc::new(AtomicUsize::new(0));
        let sync_read_limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let reader = Arc::new(FakePartitionFileReader {
            read_count: Arc::clone(&read_count),
            schema: Arc::clone(&schema),
            fail_path: None,
        });
        let read_stats = Arc::new(DeltaProviderReadStats::new(DeltaProviderReadStatsConfig {
            source_name: "orders".to_owned(),
            snapshot_version: 1,
            reader_backend: DeltaProviderReaderBackend::OfficialKernel,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 1,
            files_planned: 3,
            files_filtered_during_planning: None,
            estimated_rows: Some(5),
            estimated_bytes: Some(3),
        }));
        let dynamic = dynamic_filter_state(vec![physical_column("region", 1)]);
        let retained = retained_partition_dynamic_filter(Arc::clone(&dynamic))?;
        let admission = super::DeltaDynamicPartitionFileAdmission::new(
            Arc::clone(&read_stats),
            Arc::from([retained]),
        );
        let mut stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![
                fake_task_with_region("part-many-batches.parquet", "us-east"),
                fake_task_with_region("part-00002.parquet", "us-east"),
                fake_task_with_region("part-00001.parquet", "us-west"),
            ],
            sync_read_limiter.partition_limiter(0)?,
            1,
            admission,
        );

        let first = stream.next().await.ok_or("expected first batch")??;
        let stats_after_first = read_stats.snapshot();

        assert_eq!(batch_ids(&first)?, vec![1]);
        assert_eq!(read_count.load(Ordering::SeqCst), 1);
        assert_eq!(stats_after_first.files_started, 1);
        assert_eq!(stats_after_first.files_completed, 0);
        assert_eq!(stats_after_first.dynamic_filter_snapshots, 1);
        assert_eq!(stats_after_first.dynamic_partition_files_pruned, 0);
        assert_eq!(stats_after_first.dynamic_partition_files_kept, 1);

        dynamic.update(Arc::new(BinaryExpr::new(
            physical_column("region", 1),
            Operator::Eq,
            physical_lit("us-west"),
        )))?;

        let mut batches = vec![first];
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let ids = batches
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let stats = read_stats.snapshot();

        assert_eq!(ids, vec![1, 2, 4, 3]);
        assert_eq!(read_count.load(Ordering::SeqCst), 2);
        assert_eq!(sync_read_limiter.active_file_reads(), 0);
        assert_eq!(stats.files_planned, 3);
        assert_eq!(stats.scan_partitions_started, 1);
        assert_eq!(stats.scan_partitions_completed, 1);
        assert_eq!(stats.files_started, 2);
        assert_eq!(stats.files_completed, 2);
        assert_eq!(stats.dynamic_filter_snapshots, 3);
        assert_eq!(stats.dynamic_partition_files_pruned, 1);
        assert_eq!(stats.dynamic_partition_files_kept, 2);
        assert_eq!(stats.dynamic_partition_files_not_pruned_missing_metadata, 0);
        assert_eq!(
            stats.dynamic_partition_files_not_pruned_unsupported_expression,
            0
        );
        assert_eq!(stats.batches_produced, 4);
        assert_eq!(stats.rows_produced, 4);
        assert_eq!(
            stats.files_planned,
            stats
                .files_started
                .saturating_add(stats.dynamic_partition_files_pruned)
        );

        Ok(())
    }

    #[tokio::test]
    async fn sequential_stream_releases_file_permit_after_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table = RealParquetDeltaTable::new_default("permit-failure-read-schema")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = crate::table_formats::build_projected_delta_scan(&source, None)?;
        let sync_read_limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let reader = Arc::new(FakePartitionFileReader {
            read_count: Arc::new(AtomicUsize::new(0)),
            schema: Arc::clone(&schema),
            fail_path: Some("part-00001.parquet"),
        });
        let read_stats = test_read_stats();
        let stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![fake_task("part-00001.parquet")],
            sync_read_limiter.partition_limiter(0)?,
            1,
            empty_dynamic_admission(&read_stats),
        );

        let error = datafusion::physical_plan::common::collect(stream)
            .await
            .expect_err("fake read failure must reach DataFusion");

        assert!(
            error.to_string().contains("fake file read failure"),
            "{error}"
        );
        assert_eq!(sync_read_limiter.active_file_reads(), 0);
        let stats = read_stats.snapshot();
        assert_eq!(stats.scan_partitions_started, 1);
        assert_eq!(stats.scan_partitions_completed, 0);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.batches_produced, 0);
        assert_eq!(stats.rows_produced, 0);
        assert_eq!(stats.deletion_vector_failures, 0);
        assert_eq!(stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn sequential_stream_records_deletion_vector_failure_metric()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table = RealParquetDeltaTable::new_default("dv-failure-metric-read-schema")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let scan = crate::table_formats::build_projected_delta_scan(&source, None)?;
        let sync_read_limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let reader = Arc::new(FakePartitionFileReader {
            read_count: Arc::new(AtomicUsize::new(0)),
            schema: Arc::clone(&schema),
            fail_path: Some("part-dv-failure.parquet"),
        });
        let mut task = fake_task("part-dv-failure.parquet");
        task.deletion_vector = fake_deletion_vector_metadata();
        let read_stats = test_read_stats();
        let stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![task],
            sync_read_limiter.partition_limiter(0)?,
            1,
            empty_dynamic_admission(&read_stats),
        );

        let error = datafusion::physical_plan::common::collect(stream)
            .await
            .expect_err("fake DV failure must reach DataFusion");

        assert!(
            error
                .to_string()
                .contains("fake deletion-vector payload failure"),
            "{error}"
        );
        assert_eq!(sync_read_limiter.active_file_reads(), 0);
        let stats = read_stats.snapshot();
        assert_eq!(stats.scan_partitions_started, 1);
        assert_eq!(stats.scan_partitions_completed, 0);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.batches_produced, 0);
        assert_eq!(stats.rows_produced, 0);
        assert_eq!(stats.deletion_vector_failures, 1);
        assert_eq!(stats.deletion_vector_rejections, 0);

        Ok(())
    }

    #[tokio::test]
    async fn sequential_stream_records_deletion_vector_rejection_metric()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table = RealParquetDeltaTable::new_default("dv-rejection-metric-read-schema")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("id").gt(lit(1_i32)))?;
        let scan =
            build_projected_predicated_stats_delta_scan(&source, None, Some(predicate.clone()))?;
        let sync_read_limiter =
            DeltaProviderSyncReadLimiter::new(DeltaProviderScanExecutionOptions::default(), 1);
        let reader = Arc::new(FakePartitionFileReader {
            read_count: Arc::new(AtomicUsize::new(0)),
            schema: Arc::clone(&schema),
            fail_path: Some("part-dv-rejection.parquet"),
        });
        let mut task = fake_task("part-dv-rejection.parquet");
        task.deletion_vector = fake_deletion_vector_metadata();
        let read_stats = test_read_stats();
        let stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![task],
            sync_read_limiter.partition_limiter(0)?,
            1,
            empty_dynamic_admission(&read_stats),
        );

        let error = datafusion::physical_plan::common::collect(stream)
            .await
            .expect_err("fake DV rejection must reach DataFusion");

        assert!(
            error
                .to_string()
                .contains("fake deletion-vector predicate rejection"),
            "{error}"
        );
        assert_eq!(sync_read_limiter.active_file_reads(), 0);
        let stats = read_stats.snapshot();
        assert_eq!(stats.scan_partitions_started, 1);
        assert_eq!(stats.scan_partitions_completed, 0);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.batches_produced, 0);
        assert_eq!(stats.rows_produced, 0);
        assert_eq!(stats.deletion_vector_failures, 0);
        assert_eq!(stats.deletion_vector_rejections, 1);

        Ok(())
    }

    #[test]
    fn partition_read_schema_strips_dv_physical_predicate_only_for_backends_without_row_indexes()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("dv-partition-read-schema-gate")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("id").gt(lit(1_i32)))?;
        let scan =
            build_projected_predicated_stats_delta_scan(&source, None, Some(predicate.clone()))?;
        let read_schema = scan.read_schema();
        let mut dv_task = fake_task("part-00000.parquet");
        dv_task.deletion_vector = fake_deletion_vector_metadata();

        assert!(read_schema.has_physical_predicate());
        assert!(
            super::partition_read_schema(
                read_schema.clone(),
                &[fake_task("part-00000.parquet")],
                DeltaProviderReaderBackend::OfficialKernel,
                None,
            )?
            .has_physical_predicate()
        );
        assert!(
            !super::partition_read_schema(
                read_schema.clone(),
                std::slice::from_ref(&dv_task),
                DeltaProviderReaderBackend::OfficialKernel,
                None,
            )?
            .has_physical_predicate()
        );
        assert!(
            super::partition_read_schema(
                read_schema.clone(),
                std::slice::from_ref(&dv_task),
                DeltaProviderReaderBackend::NativeAsync,
                None,
            )?
            .has_physical_predicate()
        );
        let error = match super::partition_read_schema(
            read_schema,
            &[dv_task],
            DeltaProviderReaderBackend::OfficialKernel,
            Some(&predicate),
        ) {
            Ok(_) => {
                return Err("exact DV physical predicate must not be silently dropped".into());
            }
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("cannot drop an exact physical predicate"));
        assert!(display.contains("original row-index accounting"));

        Ok(())
    }

    #[test]
    fn provider_execution_boundary_avoids_runtime_creation_and_sync_bridge() {
        let checked_sources = [
            (
                "catalog/provider.rs",
                include_str!("../catalog/provider.rs"),
            ),
            (
                "execution/planning_exec.rs",
                include_str!("planning_exec.rs"),
            ),
        ];
        let forbidden_patterns = [
            concat!("block", "_", "on"),
            concat!("tokio", "::", "spawn"),
            concat!("tokio", "::", "task", "::", "spawn_blocking"),
            concat!("tokio", "::", "runtime"),
            concat!("Runtime", "::", "new"),
            concat!("Builder", "::", "new_current_thread"),
            concat!("Builder", "::", "new_multi_thread"),
        ];

        for (source_path, source) in checked_sources {
            for forbidden_pattern in forbidden_patterns {
                assert!(
                    !source.contains(forbidden_pattern),
                    "{source_path} must not contain `{forbidden_pattern}`"
                );
            }
        }
    }

    struct FakePartitionFileReader {
        read_count: Arc<AtomicUsize>,
        schema: datafusion::arrow::datatypes::SchemaRef,
        fail_path: Option<&'static str>,
    }

    impl DeltaScanPartitionFileReader for FakePartitionFileReader {
        fn read_file(
            &self,
            request: DeltaFileReadRequest<'_>,
        ) -> Result<DeltaFileReadResult, crate::DeltaFunnelError> {
            self.read_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_path == Some(request.task.path.as_str()) {
                return Err(fake_file_read_error(request.task.path.as_str()));
            }

            let ids = match request.task.path.as_str() {
                "part-00000.parquet" => vec![vec![1], vec![2]],
                "part-many-batches.parquet" => vec![vec![1], vec![2], vec![4]],
                "part-00001.parquet" => vec![vec![3]],
                "part-00002.parquet" => vec![vec![4]],
                path => {
                    return Err(crate::DeltaFunnelError::Config {
                        message: format!("unexpected fake file task `{path}`"),
                    });
                }
            };
            let batches = ids
                .into_iter()
                .map(|ids| id_batch(Arc::clone(&self.schema), ids))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| crate::DeltaFunnelError::Config {
                    message: format!("fake batch construction failed: {error}"),
                })?;

            Ok(DeltaFileReadResult {
                batches,
                deletion_vector_stats: Default::default(),
            })
        }
    }

    fn fake_task(path: &str) -> DeltaScanFileTask {
        DeltaScanFileTask {
            source_name: "orders".to_owned(),
            table_uri: "file:///tmp/table".to_owned(),
            snapshot_version: 1,
            path: path.to_owned(),
            estimated_bytes: Some(1),
            estimated_rows: Some(1),
            modification_time_ms: Some(1),
            partition_values: BTreeMap::new(),
            stats: None,
            deletion_vector: KernelScanDeletionVectorMetadata::NotPresent,
            transform: KernelPhysicalToLogicalTransform::NotRequired,
        }
    }

    fn test_read_stats() -> Arc<DeltaProviderReadStats> {
        Arc::new(DeltaProviderReadStats::new(DeltaProviderReadStatsConfig {
            source_name: "orders".to_owned(),
            snapshot_version: 1,
            reader_backend: DeltaProviderReaderBackend::OfficialKernel,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 1,
            files_planned: 2,
            files_filtered_during_planning: None,
            estimated_rows: Some(3),
            estimated_bytes: Some(2),
        }))
    }

    fn fake_deletion_vector_metadata() -> KernelScanDeletionVectorMetadata {
        KernelScanDeletionVectorMetadata::test_present_from_descriptor(DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "fake-dv".to_owned(),
            offset: Some(0),
            size_in_bytes: 1,
            cardinality: 1,
        })
    }

    fn fake_file_read_error(path: &str) -> crate::DeltaFunnelError {
        match path {
            "part-dv-failure.parquet" => crate::DeltaFunnelError::DeltaScanDeletionVector {
                source_name: "orders".to_owned(),
                table_uri: "file:///tmp/table".to_owned(),
                snapshot_version: 1,
                path: path.to_owned(),
                phase: DeltaScanDeletionVectorPhase::PayloadRead,
                source: Box::new(delta_kernel::Error::generic(
                    "fake deletion-vector payload failure",
                )),
            },
            "part-dv-rejection.parquet" => crate::DeltaFunnelError::DeltaScanFileRead {
                source_name: "orders".to_owned(),
                table_uri: "file:///tmp/table".to_owned(),
                snapshot_version: 1,
                path: path.to_owned(),
                phase: DeltaScanFileReadPhase::DeletionVectorPredicateRejection,
                source: Box::new(delta_kernel::Error::generic(
                    "fake deletion-vector predicate rejection",
                )),
            },
            _ => crate::DeltaFunnelError::Config {
                message: "fake file read failure".to_owned(),
            },
        }
    }

    fn id_batch(
        schema: datafusion::arrow::datatypes::SchemaRef,
        ids: Vec<i32>,
    ) -> Result<RecordBatch, datafusion::arrow::error::ArrowError> {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))])
    }

    fn batch_ids(batch: &RecordBatch) -> Result<Vec<i32>, Box<dyn std::error::Error>> {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected Int32Array")?;

        Ok(ids.values().to_vec())
    }

    fn collect_batch_ids(batches: &[RecordBatch]) -> Result<Vec<i32>, Box<dyn std::error::Error>> {
        let mut ids = Vec::new();
        for batch in batches {
            ids.extend(batch_ids(batch)?);
        }

        Ok(ids)
    }

    fn register_real_parquet_source(
        ctx: &SessionContext,
        source_name: &str,
        fixture_name: &str,
    ) -> Result<RealParquetDeltaTable, Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default(fixture_name)?;
        let source = load_delta_source(DeltaSourceConfig {
            name: source_name.to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        register_delta_sources(
            ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
        )?;

        Ok(table)
    }
}
