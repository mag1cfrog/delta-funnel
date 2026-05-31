//! DataFusion integration.

mod execution;
mod filters;
mod projection;
mod provider;
mod registration;
mod scan_plan;

pub use registration::{
    DeltaTableProviderConfig, RegisteredDeltaSource, RegisteredDeltaSources, register_delta_sources,
};

#[cfg(test)]
mod test_support {
    #![allow(missing_docs)]

    use std::any::Any;
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use datafusion::catalog::{CatalogProvider, SchemaProvider};
    use datafusion::common::{DataFusionError, Result as DataFusionResult};
    use datafusion::datasource::TableProvider;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;

    use crate::query_engine::datafusion::execution::DeltaScanPlanningExec;
    use crate::query_engine::datafusion::registration::{
        DeltaTableProviderConfig, register_delta_sources,
    };
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
            let path = Path::new("target")
                .join("delta-funnel-datafusion-provider-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!(
                    "{}\n{}\n",
                    PROTOCOL_JSON,
                    metadata_json(schema_fields_json, partition_columns_json)
                ),
            )?;
            fs::write(
                log_path.join("00000000000000000001.json"),
                format!(
                    "{}\n",
                    add_json("part-00001.parquet", add_partition_values_json)
                ),
            )?;

            Ok(Self { path })
        }

        pub(crate) fn path(&self) -> &Path {
            &self.path
        }
    }

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const DEFAULT_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    pub(crate) const PARTITIONED_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    pub(crate) const NESTED_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]"#;
    pub(crate) const DEEP_NESTED_WITH_CITY_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"address\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    pub(crate) const INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"bad_array\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{\"delta.columnMapping.nested.ids\":\"not an object\"}}]"#;

    fn metadata_json(schema_fields_json: &str, partition_columns_json: &str) -> String {
        format!(
            r#"{{"metaData":{{"id":"delta-funnel-test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":{schema_fields_json}}}","partitionColumns":{partition_columns_json},"configuration":{{}},"createdTime":1587968585495}}}}"#
        )
    }

    fn add_json(path: &str, partition_values_json: &str) -> String {
        format!(
            r#"{{"add":{{"path":"{path}",{partition_values_json},"size":0,"modificationTime":1587968586000,"dataChange":true}}}}"#
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
        })?;
        let preflight = preflight_delta_protocol(&source)?;

        register_delta_sources(
            ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
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
    }

    impl FailsOnCustomersSchemaProvider {
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
            if name == "customers" {
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
