//! DataFusion integration.

use std::{collections::HashSet, error::Error, fmt, sync::Arc};

use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    EmptyRecordBatchStream, ExecutionPlan, SendableRecordBatchStream,
    coalesce_partitions::CoalescePartitionsExec,
};
use delta_arrow_reader::{
    DeltaDataFusionMetrics, DeltaDataFusionMetricsSnapshot, collect_delta_datafusion_metrics,
};

use crate::DeltaFunnelError;

mod catalog;
pub(crate) mod execution_profile;
mod operator_activity;
mod planning_activity;
mod profiled_execution;
mod session;

#[cfg(feature = "perfetto-profile")]
pub(crate) use operator_activity::initialize_datafusion_task_tracing;
pub(crate) use operator_activity::instrument_query_execution_plan;
pub(crate) use planning_activity::with_query_planning_activity;
pub(crate) use profiled_execution::{
    QueryTraceIdentity, profiled_datafusion_query_output_stream_with_effective_root,
};

pub use catalog::registration::{
    RegisteredDeltaSource, RegisteredDeltaSources, register_delta_sources,
    register_delta_sources_with_scan_execution_options,
};
pub(crate) use catalog::registration::{
    register_delta_source_with_scan_options, reject_existing_delta_registration_name,
    validate_delta_table_snapshot_protocol,
};
pub use session::{QueryOptions, datafusion_session_config, datafusion_session_context};

/// One standalone metrics handle for a Delta provider scan.
#[derive(Clone)]
pub(crate) struct DeltaProviderReadStatsHandle {
    metrics: DeltaDataFusionMetrics,
}

impl DeltaProviderReadStatsHandle {
    pub(crate) fn same_instance(&self, other: &Self) -> bool {
        self.metrics.same_instance(&other.metrics)
    }

    pub(crate) fn snapshot(&self) -> DeltaDataFusionMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub(crate) fn source_name(&self) -> Option<&str> {
        self.metrics.source_name()
    }
}

pub(crate) fn delta_datafusion_metrics_for_plan(
    plan: &dyn ExecutionPlan,
) -> Option<DeltaProviderReadStatsHandle> {
    let plan = operator_activity::unprofiled_execution_plan(plan);
    if !plan.children().is_empty() {
        return None;
    }
    let metrics = collect_delta_datafusion_metrics(plan);
    let [metrics] = metrics.as_slice() else {
        return None;
    };
    Some(DeltaProviderReadStatsHandle {
        metrics: metrics.clone(),
    })
}

impl From<DeltaFunnelError> for DataFusionError {
    fn from(error: DeltaFunnelError) -> Self {
        Self::External(Box::new(error))
    }
}

/// Collects distinct shared read stats counters without retaining the physical plan.
///
/// Repeated references to the same metrics instance are omitted while
/// preserving the first-seen physical-plan traversal order.
pub(crate) fn collect_delta_provider_read_stats_handles(
    plan: &dyn ExecutionPlan,
) -> Vec<DeltaProviderReadStatsHandle> {
    fn collect(
        plan: &dyn ExecutionPlan,
        seen: &mut HashSet<usize>,
        found: &mut Vec<DeltaProviderReadStatsHandle>,
    ) {
        let plan = operator_activity::unprofiled_execution_plan(plan);
        let plan_identity = plan as *const dyn ExecutionPlan as *const () as usize;
        if !seen.insert(plan_identity) {
            return;
        }

        let children = plan.children();
        if let Some(handle) = delta_datafusion_metrics_for_plan(plan)
            && !found.iter().any(|existing| existing.same_instance(&handle))
        {
            found.push(handle);
        }
        for child in children {
            collect(child.as_ref(), seen, found);
        }
    }

    let mut seen = HashSet::new();
    let mut found = Vec::new();
    collect(plan, &mut seen, &mut found);
    found
}

/// Creates point-in-time snapshots from shared live read counters.
pub(crate) fn snapshot_delta_provider_read_stats(
    handles: &[DeltaProviderReadStatsHandle],
) -> Vec<DeltaDataFusionMetricsSnapshot> {
    handles.iter().map(|stats| stats.snapshot()).collect()
}

#[derive(Clone)]
pub(crate) struct NamedDeltaDataFusionMetricsSnapshot {
    pub(crate) source_name: String,
    pub(crate) snapshot: DeltaDataFusionMetricsSnapshot,
}

pub(crate) fn collect_named_delta_datafusion_metrics(
    plan: &dyn ExecutionPlan,
) -> Vec<NamedDeltaDataFusionMetricsSnapshot> {
    collect_delta_datafusion_metrics(plan)
        .into_iter()
        .map(|metrics| NamedDeltaDataFusionMetricsSnapshot {
            source_name: metrics.source_name().unwrap_or_default().to_owned(),
            snapshot: metrics.snapshot(),
        })
        .collect()
}

/// Executes one selected DataFusion query output as a single merged stream.
///
/// DataFusion physical plans can have multiple output partitions. This helper
/// uses DataFusion's own `execute_stream` behavior, which merges those
/// partitions into one `RecordBatch` stream while still letting partition tasks
/// run concurrently. DeltaFunnel's downstream MSSQL writer can then stay a
/// single awaited consumer without forcing serial partition execution.
pub fn datafusion_query_output_stream(
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let DFQueryExecution {
        stream,
        effective_profile_root,
    } = datafusion_query_output_stream_with_effective_root(plan, task_context)
        .map_err(|failure| failure.source)?;
    drop(effective_profile_root);
    Ok(stream)
}

pub(crate) struct DFQueryExecution {
    pub(crate) stream: SendableRecordBatchStream,
    pub(crate) effective_profile_root: Arc<dyn ExecutionPlan>,
}

#[derive(Debug)]
pub(crate) struct DFQueryExecutionSetupError {
    pub(crate) source: DataFusionError,
    pub(crate) effective_profile_root: Arc<dyn ExecutionPlan>,
}

impl fmt::Display for DFQueryExecutionSetupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl Error for DFQueryExecutionSetupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

pub(crate) fn datafusion_query_output_stream_with_effective_root(
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
) -> Result<DFQueryExecution, DFQueryExecutionSetupError> {
    let (effective_profile_root, execute) = prepare_datafusion_query_output(plan);
    execute_datafusion_query_output(effective_profile_root, execute, task_context)
}

pub(super) fn prepare_datafusion_query_output(
    plan: Arc<dyn ExecutionPlan>,
) -> (Arc<dyn ExecutionPlan>, bool) {
    // Keep these branches in sync with DataFusion's `execute_stream`.
    match plan.properties().output_partitioning().partition_count() {
        // DataFusion returns an empty stream without executing a partition, but
        // profiling still needs the real planned root.
        0 => (plan, false),
        // The only output partition has the zero-based index 0.
        1 => (plan, true),
        2.. => {
            // The wrapper exposes one output partition at index 0 and consumes
            // every output partition from the original plan.
            (
                Arc::new(CoalescePartitionsExec::new(plan)) as Arc<dyn ExecutionPlan>,
                true,
            )
        }
    }
}

pub(super) fn execute_datafusion_query_output(
    effective_profile_root: Arc<dyn ExecutionPlan>,
    execute: bool,
    task_context: Arc<TaskContext>,
) -> Result<DFQueryExecution, DFQueryExecutionSetupError> {
    let stream = if execute {
        effective_profile_root
            .execute(0, task_context)
            .map_err(|source| DFQueryExecutionSetupError {
                source,
                effective_profile_root: Arc::clone(&effective_profile_root),
            })?
    } else {
        Box::pin(EmptyRecordBatchStream::new(effective_profile_root.schema()))
    };
    Ok(DFQueryExecution {
        stream,
        effective_profile_root,
    })
}

#[cfg(test)]
pub(crate) mod test_support {
    #![allow(dead_code, missing_docs)]

    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use datafusion::catalog::{CatalogProvider, SchemaProvider};
    use datafusion::common::{DataFusionError, Result as DataFusionResult};
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;
    use delta_arrow_reader::{DeltaDataFusionScanOptions, DeltaTableBuilder, register_delta_table};

    pub(crate) struct DeltaLogTable {
        path: PathBuf,
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl DeltaLogTable {
        pub(crate) fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_schema(
                name,
                DEFAULT_SCHEMA_FIELDS_JSON,
                "[]",
                r#""partitionValues":{}"#,
            )
        }

        pub(crate) fn new_with_schema(
            name: &str,
            schema_fields_json: &str,
            partition_columns_json: &str,
            add_partition_values_json: &str,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_schema_and_adds(
                name,
                schema_fields_json,
                partition_columns_json,
                &[add_partition_values_json],
            )
        }

        pub(crate) fn new_with_schema_and_adds(
            name: &str,
            schema_fields_json: &str,
            partition_columns_json: &str,
            add_partition_values_jsons: &[&str],
        ) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_schema_protocol_and_adds(
                name,
                PROTOCOL_JSON,
                schema_fields_json,
                partition_columns_json,
                add_partition_values_jsons,
            )
        }

        pub(crate) fn new_with_schema_protocol_and_adds(
            name: &str,
            protocol_json: &str,
            schema_fields_json: &str,
            partition_columns_json: &str,
            add_partition_values_jsons: &[&str],
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let add_partition_values_and_sizes = add_partition_values_jsons
                .iter()
                .map(|partition_values_json| (*partition_values_json, 0))
                .collect::<Vec<_>>();

            Self::new_with_schema_protocol_and_sized_adds(
                name,
                protocol_json,
                schema_fields_json,
                partition_columns_json,
                &add_partition_values_and_sizes,
            )
        }

        pub(crate) fn new_with_schema_and_sized_adds(
            name: &str,
            schema_fields_json: &str,
            partition_columns_json: &str,
            add_partition_values_and_sizes: &[(&str, u64)],
        ) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_schema_protocol_and_sized_adds(
                name,
                PROTOCOL_JSON,
                schema_fields_json,
                partition_columns_json,
                add_partition_values_and_sizes,
            )
        }

        pub(crate) fn new_with_schema_protocol_and_sized_adds(
            name: &str,
            protocol_json: &str,
            schema_fields_json: &str,
            partition_columns_json: &str,
            add_partition_values_and_sizes: &[(&str, u64)],
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-funnel-datafusion-provider-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!(
                    "{}\n{}\n",
                    protocol_json,
                    metadata_json(schema_fields_json, partition_columns_json)
                ),
            )?;
            let add_actions = add_partition_values_and_sizes
                .iter()
                .enumerate()
                .map(|(index, (partition_values_json, size))| {
                    add_json(
                        &format!("part-{index:05}.parquet"),
                        partition_values_json,
                        *size,
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            fs::write(
                log_path.join("00000000000000000001.json"),
                format!("{add_actions}\n"),
            )?;

            Ok(Self { path })
        }

        pub(crate) fn path(&self) -> &Path {
            &self.path
        }
    }

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    pub(crate) const DEFAULT_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    pub(crate) const PARTITIONED_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    pub(crate) const NESTED_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]"#;
    pub(crate) const DEEP_NESTED_WITH_CITY_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"address\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    pub(crate) const INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"bad_array\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{\"delta.columnMapping.nested.ids\":\"not an object\"}}]"#;

    fn metadata_json(schema_fields_json: &str, partition_columns_json: &str) -> String {
        format!(
            r#"{{"metaData":{{"id":"delta-funnel-test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":{schema_fields_json}}}","partitionColumns":{partition_columns_json},"configuration":{{}},"createdTime":1587968585495}}}}"#
        )
    }

    fn add_json(path: &str, partition_values_json: &str, size: u64) -> String {
        format!(
            r#"{{"add":{{"path":"{path}",{partition_values_json},"size":{size},"modificationTime":1587968586000,"dataChange":true}}}}"#
        )
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

        Ok(format!("{}-{}-{nanos}", std::process::id(), name))
    }

    pub(crate) fn register_fixture_source(
        ctx: &SessionContext,
        source_name: &str,
        fixture_name: &str,
    ) -> Result<DeltaLogTable, Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new(fixture_name)?;
        let loaded = DeltaTableBuilder::new(table.path.to_string_lossy()).load()?;
        register_delta_table(
            ctx,
            source_name,
            loaded,
            DeltaDataFusionScanOptions::default(),
        )?;

        Ok(table)
    }

    #[derive(Debug, Default)]
    pub(crate) struct FailsOnCustomersSchemaProvider {
        tables: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
        allow_customers: AtomicBool,
    }

    impl FailsOnCustomersSchemaProvider {
        pub(crate) fn allow_customers(&self) {
            self.allow_customers.store(true, Ordering::Relaxed);
        }

        fn tables(&self) -> MutexGuard<'_, HashMap<String, Arc<dyn TableProvider>>> {
            self.tables
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    #[async_trait]
    impl SchemaProvider for FailsOnCustomersSchemaProvider {
        fn table_names(&self) -> Vec<String> {
            self.tables().keys().cloned().collect()
        }

        async fn table(
            &self,
            name: &str,
        ) -> DataFusionResult<Option<Arc<dyn TableProvider>>, DataFusionError> {
            Ok(self.tables().get(name).cloned())
        }

        fn register_table(
            &self,
            name: String,
            table: Arc<dyn TableProvider>,
        ) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
            if name == "customers" && !self.allow_customers.load(Ordering::Relaxed) {
                return Err(DataFusionError::Execution(
                    "forced customers registration failure".to_owned(),
                ));
            }

            Ok(self.tables().insert(name, table))
        }

        fn deregister_table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
            Ok(self.tables().remove(name))
        }

        fn table_exist(&self, name: &str) -> bool {
            self.tables().contains_key(name)
        }
    }

    #[derive(Debug)]
    pub(crate) struct SingleSchemaCatalogProvider {
        schema: Arc<dyn SchemaProvider>,
    }

    impl SingleSchemaCatalogProvider {
        pub(crate) fn new(schema: Arc<dyn SchemaProvider>) -> Self {
            Self { schema }
        }
    }

    impl CatalogProvider for SingleSchemaCatalogProvider {
        fn schema_names(&self) -> Vec<String> {
            vec!["public".to_owned()]
        }

        fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            (name == "public").then(|| Arc::clone(&self.schema))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc, time::Duration};

    use datafusion::{
        arrow::{
            array::Int32Array,
            datatypes::{DataType, Field, Schema, SchemaRef},
            record_batch::RecordBatch,
        },
        common::DataFusionError,
        execution::TaskContext,
        physical_plan::{
            ExecutionPlan,
            coalesce_partitions::CoalescePartitionsExec,
            execute_stream,
            test::{
                TestMemoryExec, assert_is_pending,
                exec::{
                    BarrierExec, BlockingExec, ErrorExec, MockExec,
                    assert_strong_count_converges_to_zero,
                },
            },
            union::UnionExec,
        },
        prelude::SessionContext,
    };
    use futures_util::{FutureExt, StreamExt, TryStreamExt};

    use super::{
        collect_delta_provider_read_stats_handles, datafusion_query_output_stream,
        datafusion_query_output_stream_with_effective_root, test_support::register_fixture_source,
    };

    #[tokio::test]
    async fn read_stats_handles_deduplicate_repeated_metrics_instance() -> Result<(), Box<dyn Error>>
    {
        let context = SessionContext::new();
        let _table = register_fixture_source(&context, "orders", "shared-scan-handle")?;
        let plan = delta_plan(&context).await?;
        let original = collect_delta_provider_read_stats_handles(plan.as_ref());
        let repeated_plan = UnionExec::try_new(vec![Arc::clone(&plan), plan])?;

        let found = collect_delta_provider_read_stats_handles(repeated_plan.as_ref());

        assert_eq!(original.len(), 1);
        assert_eq!(found.len(), 1);
        assert!(found[0].same_instance(&original[0]));
        Ok(())
    }

    #[tokio::test]
    async fn read_stats_handles_keep_distinct_metrics_in_first_seen_order()
    -> Result<(), Box<dyn Error>> {
        let context = SessionContext::new();
        let _table = register_fixture_source(&context, "orders", "distinct-scan-handles")?;
        let first_plan = delta_plan(&context).await?;
        let second_plan = delta_plan(&context).await?;
        let first = collect_delta_provider_read_stats_handles(first_plan.as_ref());
        let second = collect_delta_provider_read_stats_handles(second_plan.as_ref());
        let combined_plan =
            UnionExec::try_new(vec![Arc::clone(&second_plan), first_plan, second_plan])?;

        let found = collect_delta_provider_read_stats_handles(combined_plan.as_ref());

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert!(!first[0].same_instance(&second[0]));
        assert_eq!(found.len(), 2);
        assert!(found[0].same_instance(&second[0]));
        assert!(found[1].same_instance(&first[0]));
        Ok(())
    }

    #[tokio::test]
    async fn query_output_stream_effective_root_matches_datafusion_for_all_partition_counts()
    -> Result<(), Box<dyn Error>> {
        let schema = schema();
        let cases: [Vec<Vec<i32>>; 3] = [vec![], vec![vec![1, 2]], vec![vec![1, 2], vec![3, 4]]];

        for partition_values in cases {
            let partitions = partition_values
                .iter()
                .map(|values| int_batch(Arc::clone(&schema), values).map(|batch| vec![batch]))
                .collect::<Result<Vec<_>, _>>()?;
            let plan: Arc<dyn ExecutionPlan> =
                TestMemoryExec::try_new_exec(&partitions, Arc::clone(&schema), None)?;
            let mut expected = collect_stream_batch_values(execute_stream(
                Arc::clone(&plan),
                Arc::new(TaskContext::default()),
            )?)
            .await?;

            let output = datafusion_query_output_stream_with_effective_root(
                Arc::clone(&plan),
                Arc::new(TaskContext::default()),
            )?;
            let actual_schema = output.stream.schema();
            let mut actual = collect_stream_batch_values(output.stream).await?;

            if partition_values.len() < 2 {
                assert!(Arc::ptr_eq(&output.effective_profile_root, &plan));
            } else {
                let effective_root = output
                    .effective_profile_root
                    .downcast_ref::<CoalescePartitionsExec>()
                    .ok_or("expected CoalescePartitionsExec")?;
                assert!(Arc::ptr_eq(effective_root.input(), &plan));
                // DataFusion does not guarantee ordering between partitions.
                expected.sort_unstable();
                actual.sort_unstable();
            }
            assert_eq!(actual_schema, schema);
            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn query_output_stream_effective_root_matches_datafusion_setup_errors()
    -> Result<(), Box<dyn Error>> {
        let expected = setup_error_message(execute_stream(
            Arc::new(ErrorExec::new()),
            Arc::new(TaskContext::default()),
        ))?;
        let actual = setup_error_message(datafusion_query_output_stream_with_effective_root(
            Arc::new(ErrorExec::new()),
            Arc::new(TaskContext::default()),
        ))?;

        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn query_output_stream_effective_root_matches_datafusion_stream_errors()
    -> Result<(), Box<dyn Error>> {
        let expected = first_stream_error(execute_stream(
            stream_error_plan()?,
            Arc::new(TaskContext::default()),
        )?)
        .await?;
        let actual = first_stream_error(
            datafusion_query_output_stream_with_effective_root(
                stream_error_plan()?,
                Arc::new(TaskContext::default()),
            )?
            .stream,
        )
        .await?;

        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn query_output_stream_effective_root_preserves_backpressure_and_wakes()
    -> Result<(), Box<dyn Error>> {
        let mut expected = exercise_backpressure_and_wakes(ExecutionPath::DataFusion).await?;
        let mut actual = exercise_backpressure_and_wakes(ExecutionPath::DeltaFunnel).await?;

        expected.sort_unstable();
        actual.sort_unstable();
        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn query_output_stream_effective_root_matches_datafusion_cancellation()
    -> Result<(), Box<dyn Error>> {
        assert_cancellation_releases_plan(ExecutionPath::DataFusion).await?;
        assert_cancellation_releases_plan(ExecutionPath::DeltaFunnel).await?;
        Ok(())
    }

    #[tokio::test]
    async fn public_query_output_stream_still_merges_multiple_partitions()
    -> Result<(), Box<dyn Error>> {
        let schema = schema();
        let plan = TestMemoryExec::try_new_exec(
            &[
                vec![int_batch(Arc::clone(&schema), &[1, 2])?],
                vec![int_batch(Arc::clone(&schema), &[3, 4])?],
            ],
            schema,
            None,
        )?;
        let stream = datafusion_query_output_stream(plan, Arc::new(TaskContext::default()))?;
        let mut values = collect_stream_values(stream).await?;

        values.sort_unstable();

        assert_eq!(values, vec![1, 2, 3, 4]);
        Ok(())
    }

    #[test]
    fn public_query_output_stream_does_not_retain_zero_partition_root() -> Result<(), Box<dyn Error>>
    {
        let partitions: Vec<Vec<RecordBatch>> = Vec::new();
        let plan: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&partitions, schema(), None)?;
        let weak_plan = Arc::downgrade(&plan);

        let stream = datafusion_query_output_stream(plan, Arc::new(TaskContext::default()))?;

        assert!(weak_plan.upgrade().is_none());
        drop(stream);
        Ok(())
    }

    async fn delta_plan(
        context: &SessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>, Box<dyn Error>> {
        Ok(context
            .sql("select * from orders")
            .await?
            .create_physical_plan()
            .await?)
    }

    fn int_batch(schema: SchemaRef, values: &[i32]) -> Result<RecordBatch, Box<dyn Error>> {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
            .map_err(Into::into)
    }

    fn batch_values(batch: &RecordBatch) -> Result<Vec<i32>, Box<dyn Error>> {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected Int32Array")?;

        Ok((0..values.len()).map(|index| values.value(index)).collect())
    }

    async fn collect_stream_values(
        stream: datafusion::physical_plan::SendableRecordBatchStream,
    ) -> Result<Vec<i32>, Box<dyn Error>> {
        Ok(collect_stream_batch_values(stream)
            .await?
            .into_iter()
            .flatten()
            .collect())
    }

    async fn collect_stream_batch_values(
        mut stream: datafusion::physical_plan::SendableRecordBatchStream,
    ) -> Result<Vec<Vec<i32>>, Box<dyn Error>> {
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch_values(&batch?)?);
        }
        Ok(batches)
    }

    fn setup_error_message<T, E>(result: Result<T, E>) -> Result<String, Box<dyn Error>>
    where
        E: Error,
    {
        match result {
            Ok(_) => Err("expected stream setup error".into()),
            Err(error) => Ok(error.to_string()),
        }
    }

    fn stream_error_plan() -> Result<Arc<dyn ExecutionPlan>, Box<dyn Error>> {
        let schema = schema();
        let success: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            vec![Ok(int_batch(Arc::clone(&schema), &[1])?)],
            Arc::clone(&schema),
        ));
        let failure: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            vec![Err(DataFusionError::Execution(
                "injected stream failure".to_owned(),
            ))],
            schema,
        ));
        Ok(UnionExec::try_new(vec![success, failure])?)
    }

    async fn first_stream_error(
        mut stream: datafusion::physical_plan::SendableRecordBatchStream,
    ) -> Result<String, Box<dyn Error>> {
        while let Some(batch) = stream.next().await {
            if let Err(error) = batch {
                return Ok(error.to_string());
            }
        }
        Err("expected stream error".into())
    }

    enum ExecutionPath {
        DataFusion,
        DeltaFunnel,
    }

    async fn exercise_backpressure_and_wakes(
        path: ExecutionPath,
    ) -> Result<Vec<i32>, Box<dyn Error>> {
        let schema = schema();
        let partitions = (0..2)
            .map(|partition| {
                (0..8)
                    .map(|offset| int_batch(Arc::clone(&schema), &[partition * 8 + offset]))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let plan = Arc::new(
            BarrierExec::new(partitions, Arc::clone(&schema))
                .with_log(false)
                .with_finish_barrier(),
        );
        let execution_plan: Arc<dyn ExecutionPlan> = plan.clone();
        let stream = match path {
            ExecutionPath::DataFusion => {
                execute_stream(execution_plan, Arc::new(TaskContext::default()))?
            }
            ExecutionPath::DeltaFunnel => {
                datafusion_query_output_stream_with_effective_root(
                    execution_plan,
                    Arc::new(TaskContext::default()),
                )?
                .stream
            }
        };

        tokio::time::timeout(Duration::from_secs(5), plan.wait()).await?;
        let drained_without_consumer = tokio::time::timeout(Duration::from_millis(100), async {
            while !plan.is_finish_barrier_reached() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(drained_without_consumer.is_err());

        let collection = tokio::spawn(async move { stream.try_collect::<Vec<_>>().await });
        tokio::time::timeout(Duration::from_secs(5), plan.wait_finish()).await?;
        let batches = tokio::time::timeout(Duration::from_secs(5), collection).await???;
        let mut values = Vec::new();
        for batch in &batches {
            values.extend(batch_values(batch)?);
        }
        Ok(values)
    }

    async fn assert_cancellation_releases_plan(path: ExecutionPath) -> Result<(), Box<dyn Error>> {
        let plan = Arc::new(BlockingExec::new(schema(), 2));
        let refs = plan.refs();
        let execution_plan: Arc<dyn ExecutionPlan> = plan;
        let (mut stream, effective_profile_root) = match path {
            ExecutionPath::DataFusion => (
                execute_stream(execution_plan, Arc::new(TaskContext::default()))?,
                None,
            ),
            ExecutionPath::DeltaFunnel => {
                let execution = datafusion_query_output_stream_with_effective_root(
                    execution_plan,
                    Arc::new(TaskContext::default()),
                )?;
                (execution.stream, Some(execution.effective_profile_root))
            }
        };
        let mut next = stream.next().boxed();

        assert_is_pending(&mut next);
        drop(next);
        drop(stream);
        drop(effective_profile_root);
        assert_strong_count_converges_to_zero(refs).await;
        Ok(())
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]))
    }
}
