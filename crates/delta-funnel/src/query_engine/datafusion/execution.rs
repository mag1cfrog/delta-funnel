//! DataFusion physical execution plan for Delta scans.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result as DataFusionResult, not_impl_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

use super::planning::file_task_partition::DeltaScanFileTaskPartitionPlan;
use super::planning::partition_target::DeltaScanPartitionTargetDecision;
use super::planning::scan_plan::ProviderScanPlan;

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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        not_impl_err!("Delta scan partition planning is complete; read execution is owned by #4")
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use crate::query_engine::datafusion::registration::{
        DeltaTableProviderConfig, register_delta_sources,
    };
    use crate::query_engine::datafusion::test_support::{
        DeltaLogTable, PARTITIONED_SCHEMA_FIELDS_JSON, find_delta_scan_plans,
        register_fixture_source,
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

        Ok(())
    }

    #[tokio::test]
    async fn execution_fails_at_deliberate_delta_scan_stub()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _table = register_fixture_source(&ctx, "orders", "execution-stub")?;

        let dataframe = ctx.sql("select * from orders").await?;
        let result = dataframe.collect().await;

        assert!(matches!(
            result,
            Err(error) if error
                .to_string()
                .contains("Delta scan partition planning is complete; read execution is owned by #4")
        ));

        Ok(())
    }

    #[tokio::test]
    async fn exact_partition_filter_execution_still_stops_at_scan_stub()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema(
            "execution-stub-exact-partition-filter",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
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
            .sql("select id from orders where region = 'us-west'")
            .await?;
        let result = dataframe.collect().await;

        assert!(matches!(
            result,
            Err(error) if error
                .to_string()
                .contains("Delta scan partition planning is complete; read execution is owned by #4")
        ));

        Ok(())
    }

    #[tokio::test]
    async fn mixed_partition_filter_execution_still_stops_at_scan_stub()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let table = DeltaLogTable::new_with_schema(
            "execution-stub-mixed-partition-filter",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
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

        let sql = "select id from orders where region = 'us-west' and id > 1";
        let plan_dataframe = ctx.sql(sql).await?;
        let physical_plan = plan_dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(plan_display.contains("FilterExec"), "{plan_display}");
        assert_eq!(scans.len(), 1);
        assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.inexact_count, 1);

        let collect_dataframe = ctx.sql(sql).await?;
        let result = collect_dataframe.collect().await;

        assert!(matches!(
            result,
            Err(error) if error
                .to_string()
                .contains("Delta scan partition planning is complete; read execution is owned by #4")
        ));

        Ok(())
    }
}
