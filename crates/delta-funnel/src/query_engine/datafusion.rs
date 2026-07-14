//! DataFusion integration.

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};

use crate::DeltaFunnelError;

mod catalog;
mod execution;
mod execution_profile;
mod planning;
mod session;

pub use catalog::registration::{
    DeltaTableProviderConfig, RegisteredDeltaSource, RegisteredDeltaSources,
    register_delta_sources, register_delta_sources_with_scan_execution_options,
};
pub(crate) use catalog::registration::{
    register_delta_source_with_scan_execution_options, reject_existing_delta_registration_name,
};
pub use execution::{
    DeltaProviderReadStatsSnapshot, DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions,
};
pub use planning::partition_target::{
    DeltaScanPartitionTargetDiagnosticInput, DeltaScanPartitionTargetDiagnosticOutput,
    DeltaScanPartitionTargetDiagnosticSource, DeltaScanPartitionTargetLocalEnvironmentDiagnostic,
    DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus,
    delta_scan_partition_target_local_environment_diagnostic,
    derive_delta_scan_partition_target_diagnostic,
};
pub use session::{QueryOptions, datafusion_session_config, datafusion_session_context};

/// Shared live read counters for one physical Delta scan.
pub(crate) type DeltaProviderReadStatsHandle = Arc<execution::read_stats::DeltaProviderReadStats>;

impl From<DeltaFunnelError> for DataFusionError {
    fn from(error: DeltaFunnelError) -> Self {
        Self::External(Box::new(error))
    }
}

/// Collects provider-owned Delta read stats snapshots from a DataFusion
/// physical plan.
#[must_use]
pub fn collect_delta_provider_read_stats(
    plan: &dyn ExecutionPlan,
) -> Vec<DeltaProviderReadStatsSnapshot> {
    snapshot_delta_provider_read_stats(&collect_delta_provider_read_stats_handles(plan))
}

/// Collects distinct shared read stats counters without retaining the physical plan.
///
/// Repeated references to the same `Arc` identity are omitted while preserving
/// the first-seen physical-plan traversal order.
pub(crate) fn collect_delta_provider_read_stats_handles(
    plan: &dyn ExecutionPlan,
) -> Vec<DeltaProviderReadStatsHandle> {
    let mut found = Vec::new();
    collect_delta_provider_read_stats_handles_into(plan, &mut found);
    found
}

fn collect_delta_provider_read_stats_handles_into(
    plan: &dyn ExecutionPlan,
    found: &mut Vec<DeltaProviderReadStatsHandle>,
) {
    if let Some(scan) = plan
        .as_any()
        .downcast_ref::<execution::DeltaScanPlanningExec>()
    {
        let handle = scan.read_stats_handle();
        if !found.iter().any(|found| Arc::ptr_eq(found, &handle)) {
            found.push(handle);
        }
    }
    for child in plan.children() {
        collect_delta_provider_read_stats_handles_into(child.as_ref(), found);
    }
}

/// Creates point-in-time snapshots from shared live read counters.
pub(crate) fn snapshot_delta_provider_read_stats(
    handles: &[DeltaProviderReadStatsHandle],
) -> Vec<DeltaProviderReadStatsSnapshot> {
    handles.iter().map(|stats| stats.snapshot()).collect()
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
    datafusion::physical_plan::execute_stream(plan, task_context)
}

#[cfg(test)]
pub(crate) mod test_support {
    #![allow(missing_docs)]

    use std::any::Any;
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
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;

    use crate::query_engine::datafusion::catalog::registration::{
        DeltaTableProviderConfig, register_delta_sources,
    };
    use crate::query_engine::datafusion::execution::DeltaScanPlanningExec;
    use crate::{DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

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
        let source = load_delta_source(DeltaSourceConfig {
            name: source_name.to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
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

    pub(crate) fn find_delta_scan_plans<'a>(
        plan: &'a dyn ExecutionPlan,
        found: &mut Vec<&'a DeltaScanPlanningExec>,
    ) {
        if let Some(scan) = plan.as_any().downcast_ref::<DeltaScanPlanningExec>() {
            found.push(scan);
        }
        for child in plan.children() {
            find_delta_scan_plans(child.as_ref(), found);
        }
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
        fn as_any(&self) -> &dyn Any {
            self
        }

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
        fn as_any(&self) -> &dyn Any {
            self
        }

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
    use std::{error::Error, sync::Arc};

    use datafusion::{
        arrow::{
            array::Int32Array,
            datatypes::{DataType, Field, Schema, SchemaRef},
            record_batch::RecordBatch,
        },
        execution::TaskContext,
        physical_plan::{ExecutionPlan, test::TestMemoryExec, union::UnionExec},
        prelude::SessionContext,
    };
    use futures_util::StreamExt;

    use super::{
        collect_delta_provider_read_stats_handles, datafusion_query_output_stream,
        test_support::register_fixture_source,
    };

    #[tokio::test]
    async fn read_stats_handles_deduplicate_repeated_plan_identity() -> Result<(), Box<dyn Error>> {
        let context = SessionContext::new();
        let _table = register_fixture_source(&context, "orders", "shared-scan-handle")?;
        let plan = delta_plan(&context).await?;
        let original = collect_delta_provider_read_stats_handles(plan.as_ref());
        let repeated_plan = UnionExec::try_new(vec![Arc::clone(&plan), plan])?;

        let found = collect_delta_provider_read_stats_handles(repeated_plan.as_ref());

        assert_eq!(original.len(), 1);
        assert_eq!(found.len(), 1);
        assert!(Arc::ptr_eq(&found[0], &original[0]));
        Ok(())
    }

    #[tokio::test]
    async fn read_stats_handles_keep_distinct_identities_in_first_seen_order()
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
        assert!(!Arc::ptr_eq(&first[0], &second[0]));
        assert_eq!(found.len(), 2);
        assert!(Arc::ptr_eq(&found[0], &second[0]));
        assert!(Arc::ptr_eq(&found[1], &first[0]));
        Ok(())
    }

    #[tokio::test]
    async fn query_output_stream_merges_multi_partition_plan() -> Result<(), Box<dyn Error>> {
        let schema = schema();
        let plan = TestMemoryExec::try_new_exec(
            &[
                vec![int_batch(Arc::clone(&schema), &[1, 2])?],
                vec![int_batch(Arc::clone(&schema), &[3, 4])?],
            ],
            Arc::clone(&schema),
            None,
        )?;
        assert_eq!(plan.properties().output_partitioning().partition_count(), 2);

        let plan: Arc<dyn ExecutionPlan> = plan;
        let mut stream = datafusion_query_output_stream(plan, Arc::new(TaskContext::default()))?;
        let mut values = Vec::new();

        while let Some(batch) = stream.next().await {
            values.extend(batch_values(&batch?)?);
        }
        values.sort_unstable();

        assert_eq!(values, vec![1, 2, 3, 4]);
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

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]))
    }
}
