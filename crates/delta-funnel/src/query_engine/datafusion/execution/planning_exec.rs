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
use crate::table_formats::KernelScanReadSchema;

use super::super::planning::file_task::DeltaScanFileTask;
use super::super::planning::file_task_partition::DeltaScanFileTaskPartitionPlan;
use super::super::planning::partition_target::DeltaScanPartitionTargetDecision;
use super::super::planning::scan_plan::ProviderScanPlan;
use super::file_reader::{DeltaFileReadRequest, DeltaFileReader, DeltaFileReaderConfig};
use super::scheduling::{
    DeltaProviderScanExecutionOptions, DeltaProviderSyncPartitionReadLimiter,
    DeltaProviderSyncReadLimiter,
};

pub(crate) struct DeltaScanPlanningExec {
    scan_plan: ProviderScanPlan,
    partition_plan: DeltaScanFileTaskPartitionPlan,
    partition_target_decision: DeltaScanPartitionTargetDecision,
    execution_options: DeltaProviderScanExecutionOptions,
    sync_read_limiter: Arc<DeltaProviderSyncReadLimiter>,
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

        Self {
            scan_plan,
            partition_plan,
            partition_target_decision,
            execution_options,
            sync_read_limiter,
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

        let file_reader: Arc<dyn DeltaScanPartitionFileReader> = Arc::new(
            DeltaFileReader::try_new(DeltaFileReaderConfig {
                source_name: &self.scan_plan.source_name,
                table_uri: &self.scan_plan.table_uri,
                snapshot_version: self.scan_plan.snapshot_version,
            })
            .map_err(DataFusionError::from)?,
        );
        let file_tasks = scan_partition.file_tasks.clone();
        let read_schema =
            partition_read_schema(self.scan_plan.kernel_scan().read_schema(), &file_tasks);
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
        ))
    }
}

trait DeltaScanPartitionFileReader: Send + Sync {
    fn read_file(
        &self,
        request: DeltaFileReadRequest<'_>,
    ) -> Result<super::file_reader::DeltaFileReadResult, DeltaFunnelError>;
}

impl DeltaScanPartitionFileReader for DeltaFileReader {
    fn read_file(
        &self,
        request: DeltaFileReadRequest<'_>,
    ) -> Result<super::file_reader::DeltaFileReadResult, DeltaFunnelError> {
        Self::read_file(self, request)
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
) -> SendableRecordBatchStream {
    let mut builder = RecordBatchReceiverStreamBuilder::new(schema, 1);
    let output = builder.tx();

    builder.spawn_blocking(move || {
        for task in file_tasks {
            if output.is_closed() {
                return Ok(());
            }

            let _permit = partition_limiter.acquire_file_permit();
            let file_result = file_reader
                .read_file(DeltaFileReadRequest {
                    task: &task,
                    read_schema: &read_schema,
                })
                .map_err(DataFusionError::from)?;

            for batch in file_result {
                if output.blocking_send(Ok(batch)).is_err() {
                    return Ok(());
                }
            }
        }

        Ok(())
    });

    builder.build()
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
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use futures_util::StreamExt;

    use super::super::file_reader::{DeltaFileReadRequest, DeltaFileReadResult};
    use super::{
        DeltaProviderScanExecutionOptions, DeltaProviderSyncReadLimiter,
        DeltaScanPartitionFileReader,
    };
    use crate::query_engine::datafusion::catalog::registration::{
        DeltaTableProviderConfig, register_delta_sources,
    };
    use crate::query_engine::datafusion::planning::file_task::DeltaScanFileTask;
    use crate::query_engine::datafusion::test_support::{
        DEFAULT_SCHEMA_FIELDS_JSON, DeltaLogTable, find_delta_scan_plans, register_fixture_source,
    };
    use crate::table_formats::{
        KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata, RealParquetDeltaTable,
    };
    use crate::{DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

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
        let result = dataframe.collect().await?;

        assert!(result.is_empty());

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
        let result = dataframe.collect().await?;
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
        let result = dataframe.collect().await?;
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

        let dataframe = ctx
            .sql("select id from orders where id > 1000 order by id")
            .await?;
        let result = dataframe.collect().await?;
        let ids = result
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![1001, 1002]);

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
        let mut stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            read_schema,
            vec![
                fake_task("part-00000.parquet"),
                fake_task("part-00001.parquet"),
            ],
            sync_read_limiter.partition_limiter(0)?,
        );

        let first = stream.next().await.ok_or("expected first batch")??;

        assert_eq!(batch_ids(&first)?, vec![1]);
        assert_eq!(
            read_count.load(Ordering::SeqCst),
            1,
            "stream should yield the first batch before reading the second file"
        );

        let remaining = datafusion::physical_plan::common::collect(stream).await?;
        let remaining_ids = remaining
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(remaining_ids, vec![2, 3]);
        assert_eq!(read_count.load(Ordering::SeqCst), 2);

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
        let stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![fake_task("part-00001.parquet")],
            sync_read_limiter.partition_limiter(0)?,
        );

        let batches = datafusion::physical_plan::common::collect(stream).await?;

        assert_eq!(batches.len(), 1);
        assert_eq!(sync_read_limiter.active_file_reads(), 0);

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
        let mut stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![
                fake_task("part-many-batches.parquet"),
                fake_task("part-00001.parquet"),
            ],
            sync_read_limiter.partition_limiter(0)?,
        );

        let first = stream.next().await.ok_or("expected first batch")??;

        assert_eq!(batch_ids(&first)?, vec![1]);
        assert_eq!(read_count.load(Ordering::SeqCst), 1);

        drop(stream);

        for _ in 0..100 {
            if sync_read_limiter.active_file_reads() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert_eq!(read_count.load(Ordering::SeqCst), 1);
        assert_eq!(sync_read_limiter.active_file_reads(), 0);

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
        let stream = super::sequential_scan_partition_stream(
            schema,
            reader,
            scan.read_schema(),
            vec![fake_task("part-00001.parquet")],
            sync_read_limiter.partition_limiter(0)?,
        );

        let error = datafusion::physical_plan::common::collect(stream)
            .await
            .expect_err("fake read failure must reach DataFusion");

        assert!(
            error.to_string().contains("fake file read failure"),
            "{error}"
        );
        assert_eq!(sync_read_limiter.active_file_reads(), 0);

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
                return Err(crate::DeltaFunnelError::Config {
                    message: "fake file read failure".to_owned(),
                });
            }

            let ids = match request.task.path.as_str() {
                "part-00000.parquet" => vec![vec![1], vec![2]],
                "part-many-batches.parquet" => vec![vec![1], vec![2], vec![4]],
                "part-00001.parquet" => vec![vec![3]],
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

            Ok(DeltaFileReadResult { batches })
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
