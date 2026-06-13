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

use crate::table_formats::KernelScanReadSchema;

use super::super::planning::file_task::DeltaScanFileTask;
use super::super::planning::file_task_partition::DeltaScanFileTaskPartitionPlan;
use super::super::planning::partition_target::DeltaScanPartitionTargetDecision;
use super::super::planning::scan_plan::ProviderScanPlan;
use super::file_reader::{DeltaFileReadRequest, DeltaFileReader, DeltaFileReaderConfig};

pub(crate) struct DeltaScanPlanningExec {
    scan_plan: ProviderScanPlan,
    partition_plan: DeltaScanFileTaskPartitionPlan,
    partition_target_decision: DeltaScanPartitionTargetDecision,
    properties: Arc<PlanProperties>,
}

impl DeltaScanPlanningExec {
    pub(crate) fn new(
        scan_plan: ProviderScanPlan,
        partition_plan: DeltaScanFileTaskPartitionPlan,
        partition_target_decision: DeltaScanPartitionTargetDecision,
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

        Self {
            scan_plan,
            partition_plan,
            partition_target_decision,
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

        let file_reader = DeltaFileReader::try_new(DeltaFileReaderConfig {
            source_name: &self.scan_plan.source_name,
            table_uri: &self.scan_plan.table_uri,
            snapshot_version: self.scan_plan.snapshot_version,
        })
        .map_err(DataFusionError::from)?;
        let read_schema = self.scan_plan.kernel_scan().read_schema();
        let file_tasks = scan_partition.file_tasks.clone();

        Ok(sequential_scan_partition_stream(
            Arc::clone(&self.scan_plan.projected_schema),
            file_reader,
            read_schema,
            file_tasks,
        ))
    }
}

fn sequential_scan_partition_stream(
    schema: SchemaRef,
    file_reader: DeltaFileReader,
    read_schema: KernelScanReadSchema,
    file_tasks: Vec<DeltaScanFileTask>,
) -> SendableRecordBatchStream {
    let mut builder = RecordBatchReceiverStreamBuilder::new(schema, 1);
    let output = builder.tx();

    builder.spawn_blocking(move || {
        for task in file_tasks {
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
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;

    use crate::query_engine::datafusion::catalog::registration::{
        DeltaTableProviderConfig, register_delta_sources,
    };
    use crate::query_engine::datafusion::test_support::{
        DEFAULT_SCHEMA_FIELDS_JSON, DeltaLogTable, find_delta_scan_plans, register_fixture_source,
    };
    use crate::table_formats::RealParquetDeltaTable;
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
