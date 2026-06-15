//! DataFusion physical execution plan for Delta scans.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};

use crate::DeltaFunnelError;
use crate::error::DeltaScanFileReadPhase;
use crate::table_formats::KernelScanReadSchema;

use super::super::planning::file_task::DeltaScanFileTask;
use super::super::planning::file_task_partition::DeltaScanFileTaskPartitionPlan;
use super::super::planning::partition_target::DeltaScanPartitionTargetDecision;
use super::super::planning::scan_plan::ProviderScanPlan;
use super::async_scheduler::{
    DeltaProviderAsyncPartitionReadScheduler, DeltaProviderAsyncPartitionReadSchedulerConfig,
};
use super::file_reader::DeltaFileReadRequest;
use super::native_async_reader::{
    DeltaNativeAsyncFileReader, DeltaNativeAsyncFileReaderConfig,
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
    scan_plan: ProviderScanPlan,
    partition_plan: DeltaScanFileTaskPartitionPlan,
    partition_target_decision: DeltaScanPartitionTargetDecision,
    execution_options: DeltaProviderScanExecutionOptions,
    sync_read_limiter: Arc<DeltaProviderSyncReadLimiter>,
    async_read_limiter: Arc<DeltaProviderAsyncReadLimiter>,
    read_stats: Arc<DeltaProviderReadStats>,
    properties: Arc<PlanProperties>,
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
        // read work before provider execution exists.
        let partition_count = partition_plan.partitions.len();
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
        }
    }

    #[cfg(test)]
    pub(crate) fn scan_plan(&self) -> &ProviderScanPlan {
        &self.scan_plan
    }

    #[cfg(test)]
    pub(crate) fn partition_plan(&self) -> &DeltaScanFileTaskPartitionPlan {
        &self.partition_plan
    }

    #[cfg(test)]
    pub(crate) fn partition_target_decision(&self) -> DeltaScanPartitionTargetDecision {
        self.partition_target_decision
    }

    #[cfg(test)]
    pub(crate) fn execution_options(&self) -> DeltaProviderScanExecutionOptions {
        self.execution_options
    }

    /// Returns a cheap point-in-time snapshot of provider-owned read progress.
    ///
    /// This is the internal handoff for later orchestration and progress
    /// reporting. It intentionally stays separate from DataFusion metrics so
    /// callers can inspect partial progress after success, failure, or stream
    /// cancellation.
    #[allow(dead_code)]
    pub(crate) fn read_stats_snapshot(&self) -> super::read_stats::DeltaProviderReadStatsSnapshot {
        self.read_stats.snapshot()
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
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let Some(scan_partition) = self.partition_plan.partitions.get(partition) else {
            return Err(DataFusionError::Execution(format!(
                "Delta scan execution partition {partition} is out of range for {} planned partitions",
                self.partition_plan.partitions.len()
            )));
        };

        if scan_partition.file_tasks.is_empty() {
            return Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                &self.scan_plan.projected_schema,
            ))));
        }

        let file_tasks = scan_partition.file_tasks.clone();
        let read_schema =
            partition_read_schema(self.scan_plan.kernel_scan().read_schema(), &file_tasks);

        match self.execution_options.reader_backend {
            DeltaProviderReaderBackend::OfficialKernel => {
                let file_reader = build_partition_file_reader(DeltaProviderReaderBackendConfig {
                    reader_backend: self.execution_options.reader_backend,
                    source_name: &self.scan_plan.source_name,
                    table_uri: &self.scan_plan.table_uri,
                    snapshot_version: self.scan_plan.snapshot_version,
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
                    file_tasks,
                    partition_limiter,
                    Arc::clone(&self.read_stats),
                ))
            }
            DeltaProviderReaderBackend::NativeAsync => {
                let file_reader = Arc::new(DeltaNativeAsyncFileReader::try_new(
                    DeltaNativeAsyncFileReaderConfig {
                        source_name: &self.scan_plan.source_name,
                        table_uri: &self.scan_plan.table_uri,
                        snapshot_version: self.scan_plan.snapshot_version,
                    },
                )?);
                let partition_reader = Arc::new(DeltaNativeAsyncPartitionFileReader::new(
                    file_reader,
                    read_schema,
                    Arc::clone(&self.read_stats),
                ));
                let partition_limiter = self
                    .async_read_limiter
                    .partition_limiter(partition)
                    .map_err(DataFusionError::from)?;

                Ok(native_async_scan_partition_stream(
                    Arc::clone(&self.scan_plan.projected_schema),
                    partition_reader,
                    file_tasks,
                    partition_limiter,
                    Arc::clone(&self.read_stats),
                ))
            }
        }
    }
}

fn partition_read_schema(
    read_schema: KernelScanReadSchema,
    file_tasks: &[DeltaScanFileTask],
) -> KernelScanReadSchema {
    if file_tasks
        .iter()
        .any(|task| task.deletion_vector.is_present())
    {
        // Temporary #142 gate: the official-kernel reader does not expose
        // original row indexes for predicate-filtered batches, so DV-backed
        // partitions must not push physical predicates into Parquet reads.
        // The #145 native async reader owns reopening this path through hidden
        // row-index metadata and the DV row-index oracle.
        read_schema.without_physical_predicate()
    } else {
        read_schema
    }
}

fn sequential_scan_partition_stream(
    schema: SchemaRef,
    file_reader: Arc<dyn DeltaScanPartitionFileReader>,
    read_schema: KernelScanReadSchema,
    file_tasks: Vec<DeltaScanFileTask>,
    partition_limiter: DeltaProviderSyncPartitionReadLimiter,
    read_stats: Arc<DeltaProviderReadStats>,
) -> SendableRecordBatchStream {
    let mut builder = RecordBatchReceiverStreamBuilder::new(schema, 1);
    let output = builder.tx();

    builder.spawn_blocking(move || {
        read_stats.record_scan_partition_started();
        for task in file_tasks {
            if output.is_closed() {
                return Ok(());
            }

            read_stats.record_file_started();
            let _permit = partition_limiter.acquire_file_permit();
            let file_result = match file_reader.read_file(DeltaFileReadRequest {
                task: &task,
                read_schema: &read_schema,
            }) {
                Ok(file_result) => file_result,
                Err(error) => {
                    record_deletion_vector_read_error(read_stats.as_ref(), &error);
                    return Err(DataFusionError::from(error));
                }
            };
            let deletion_vector_stats = file_result.deletion_vector_stats;
            if deletion_vector_stats.payload_loaded {
                read_stats.record_deletion_vector_payload_loaded();
            }
            if deletion_vector_stats.applied {
                read_stats.record_deletion_vector_applied(deletion_vector_stats.deleted_rows);
            }

            for batch in file_result {
                let rows = batch.num_rows();
                if output.blocking_send(Ok(batch)).is_err() {
                    return Ok(());
                }
                read_stats.record_batch_produced(rows);
            }
            read_stats.record_file_completed();
        }

        read_stats.record_scan_partition_completed();
        Ok(())
    });

    builder.build()
}

fn native_async_scan_partition_stream(
    schema: SchemaRef,
    file_reader: Arc<DeltaNativeAsyncPartitionFileReader>,
    file_tasks: Vec<DeltaScanFileTask>,
    partition_limiter: DeltaProviderAsyncPartitionReadLimiter,
    read_stats: Arc<DeltaProviderReadStats>,
) -> SendableRecordBatchStream {
    let mut builder = RecordBatchReceiverStreamBuilder::new(schema, 1);
    let output = builder.tx();

    builder.spawn(async move {
        read_stats.record_scan_partition_started();
        let mut scheduler = DeltaProviderAsyncPartitionReadScheduler::new(
            DeltaProviderAsyncPartitionReadSchedulerConfig::new(
                file_tasks,
                file_reader,
                partition_limiter,
            ),
        );

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
            let deletion_vector_stats = file_stream.deletion_vector_stats();
            if deletion_vector_stats.payload_loaded {
                read_stats.record_deletion_vector_payload_loaded();
            }
            if deletion_vector_stats.applied {
                read_stats.record_deletion_vector_applied(deletion_vector_stats.deleted_rows);
            }

            while !output.is_closed() {
                let batch = match file_stream.next_batch().await {
                    Ok(Some(batch)) => batch,
                    Ok(None) => break,
                    Err(error) => {
                        record_deletion_vector_read_error(read_stats.as_ref(), &error);
                        return Err(DataFusionError::from(error));
                    }
                };
                let rows = batch.num_rows();
                if output.send(Ok(batch)).await.is_err() {
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
    });

    builder.build()
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use delta_kernel::actions::deletion_vector::{
        DeletionVectorDescriptor, DeletionVectorStorageType,
    };
    use futures_util::StreamExt;

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
        DeltaProviderReadStats, DeltaProviderReadStatsConfig,
    };
    use crate::query_engine::datafusion::planning::file_task::DeltaScanFileTask;
    use crate::query_engine::datafusion::test_support::{
        DEFAULT_SCHEMA_FIELDS_JSON, DeltaLogTable, find_delta_scan_plans, register_fixture_source,
    };
    use crate::table_formats::{
        KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata, RealParquetDeltaTable,
        build_projected_predicated_stats_delta_scan, datafusion_expr_to_kernel_predicate,
    };
    use crate::{DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

    async fn collect_sql_with_reader_backend(
        table_uri: &str,
        reader_backend: DeltaProviderReaderBackend,
        sql: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table_uri.to_owned(),
            version: None,
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

        let result = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;

        Ok(pretty_format_batches(&result)?.to_string())
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
            scans[0].execution_options(),
            super::DeltaProviderScanExecutionOptions::default()
        );
        let read_stats = scans[0].read_stats_snapshot();
        assert_eq!(read_stats.source_name, "orders");
        assert_eq!(read_stats.snapshot_version, 1);
        assert_eq!(
            read_stats.reader_backend,
            DeltaProviderReaderBackend::OfficialKernel
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
    async fn sequential_execution_reads_real_delta_file() -> Result<(), Box<dyn std::error::Error>>
    {
        let ctx = SessionContext::new();
        let _table =
            register_real_parquet_source(&ctx, "orders", "sequential-execution-real-read")?;

        let dataframe = ctx
            .sql("select id, customer_name from orders order by id")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].execution_options().reader_backend,
            DeltaProviderReaderBackend::OfficialKernel
        );
        assert_eq!(
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::OfficialKernel
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
    async fn explicit_reader_backend_flows_through_provider_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_default("explicit-reader-backend")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
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
    async fn native_async_backend_reads_projected_non_dv_file_through_provider_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_default("native-async-provider-read")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
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

        Ok(())
    }

    #[tokio::test]
    async fn native_async_backend_keeps_data_filter_as_residual()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_default("native-async-residual-filter")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
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
            .sql("select customer_name from orders where id > 1 order by id")
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
            scans[0].read_stats_snapshot().reader_backend,
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            0
        );
        assert!(
            !scans[0]
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
    async fn native_async_backend_preserves_file_order_in_one_partition()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = RealParquetDeltaTable::new_with_two_files("native-async-file-order")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
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
            if stats.files_started == 1 && stats.files_completed == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        let stats = scans[0].read_stats_snapshot();
        assert_eq!(stats.scan_partitions_started, 1);
        assert_eq!(stats.scan_partitions_completed, 0);
        assert_eq!(stats.files_started, 1);
        assert_eq!(stats.files_completed, 0);
        assert!((1..=2).contains(&stats.batches_produced));
        assert!((1..=16_384).contains(&stats.rows_produced));

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
            Arc::clone(&read_stats),
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
            Arc::clone(&read_stats),
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
            estimated_rows: Some(3),
            estimated_bytes: Some(2),
        }));
        let stream_a = super::sequential_scan_partition_stream(
            Arc::clone(&schema),
            Arc::clone(&reader),
            scan.read_schema(),
            vec![fake_task("part-00000.parquet")],
            sync_read_limiter.partition_limiter(0)?,
            Arc::clone(&read_stats),
        );
        let stream_b = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![fake_task("part-00001.parquet")],
            sync_read_limiter.partition_limiter(1)?,
            Arc::clone(&read_stats),
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
            Arc::clone(&read_stats),
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
    async fn sequential_stream_releases_file_permit_after_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table = RealParquetDeltaTable::new_default("permit-failure-read-schema")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
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
            Arc::clone(&read_stats),
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
            Arc::clone(&read_stats),
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
        })?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("id").gt(lit(1_i32)))?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, Some(predicate))?;
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
            Arc::clone(&read_stats),
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
    fn partition_read_schema_strips_physical_predicate_only_for_dv_tasks()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("dv-partition-read-schema-gate")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("id").gt(lit(1_i32)))?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, Some(predicate))?;
        let read_schema = scan.read_schema();
        let mut dv_task = fake_task("part-00000.parquet");
        dv_task.deletion_vector = fake_deletion_vector_metadata();

        assert!(read_schema.has_physical_predicate());
        assert!(
            super::partition_read_schema(read_schema.clone(), &[fake_task("part-00000.parquet")])
                .has_physical_predicate()
        );
        assert!(!super::partition_read_schema(read_schema, &[dv_task]).has_physical_predicate());

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
