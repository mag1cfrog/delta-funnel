//! DataFusion integration.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{Result as DataFusionResult, not_impl_err};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::{
    DeltaFunnelError, DeltaProtocolReport, PlannedDeltaSource, ProtocolPreflight,
    table_formats::{delta_source_arrow_schema, validate_table_source_names},
};

/// Delta source and preflight state used to build a DataFusion table provider.
pub struct DeltaTableProviderConfig {
    /// Loaded Delta source.
    pub source: PlannedDeltaSource,
    /// Successful Delta protocol preflight for the source.
    pub protocol: ProtocolPreflight,
}

/// Registered Delta sources visible to a DataFusion session.
#[derive(Debug, Clone)]
pub struct RegisteredDeltaSources {
    /// Per-source registration reports.
    pub sources: Vec<RegisteredDeltaSource>,
}

/// One registered Delta source.
#[derive(Debug, Clone)]
pub struct RegisteredDeltaSource {
    /// DataFusion table name for this source.
    pub name: String,
    /// Resolved Delta snapshot version.
    pub snapshot_version: u64,
    /// Logical Arrow schema exposed to DataFusion.
    pub schema: SchemaRef,
    /// Protocol report captured before registration.
    pub protocol: DeltaProtocolReport,
}

pub(crate) struct DeltaTableProvider {
    source: PlannedDeltaSource,
    protocol: DeltaProtocolReport,
    schema: SchemaRef,
}

/// Registers preflighted Delta sources into a DataFusion session.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::DeltaSourceSchema`] when a source schema cannot
/// be converted to Arrow, or [`DeltaFunnelError::DataFusionRegistration`] when
/// DataFusion rejects a table registration.
pub fn register_delta_sources(
    ctx: &SessionContext,
    configs: Vec<DeltaTableProviderConfig>,
) -> Result<RegisteredDeltaSources, DeltaFunnelError> {
    reject_duplicate_registration_names(&configs)?;

    let sources = configs
        .into_iter()
        .map(|config| register_delta_source(ctx, config))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RegisteredDeltaSources { sources })
}

fn reject_duplicate_registration_names(
    configs: &[DeltaTableProviderConfig],
) -> Result<(), DeltaFunnelError> {
    validate_table_source_names(configs.iter().map(|config| config.source.name()))
}

fn register_delta_source(
    ctx: &SessionContext,
    config: DeltaTableProviderConfig,
) -> Result<RegisteredDeltaSource, DeltaFunnelError> {
    let provider = DeltaTableProvider::try_new(config.source, config.protocol)?;
    let registered = RegisteredDeltaSource {
        name: provider.source_name().to_owned(),
        snapshot_version: provider.snapshot_version(),
        schema: provider.schema(),
        protocol: provider.protocol().clone(),
    };
    let table_uri = provider.source.table_uri().to_owned();

    ctx.register_table(registered.name.as_str(), Arc::new(provider))
        .map_err(|error| DeltaFunnelError::DataFusionRegistration {
            source_name: registered.name.clone(),
            table_uri,
            reason: error.to_string(),
        })?;

    Ok(registered)
}

impl DeltaTableProvider {
    fn try_new(
        source: PlannedDeltaSource,
        preflight: ProtocolPreflight,
    ) -> Result<Self, DeltaFunnelError> {
        let schema = delta_source_arrow_schema(&source).map_err(|reason| {
            DeltaFunnelError::DeltaSourceSchema {
                source_name: source.name().to_owned(),
                table_uri: source.table_uri().to_owned(),
                reason,
            }
        })?;

        Ok(Self {
            source,
            protocol: preflight.protocol,
            schema,
        })
    }

    fn source_name(&self) -> &str {
        self.source.name()
    }

    fn snapshot_version(&self) -> u64 {
        self.source.version()
    }

    fn protocol(&self) -> &DeltaProtocolReport {
        &self.protocol
    }
}

impl fmt::Debug for DeltaTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaTableProvider")
            .field("source_name", &self.source_name())
            .field("snapshot_version", &self.snapshot_version())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Delta scan execution is owned by a later implementation issue")
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::prelude::SessionContext;

    use super::{DeltaTableProvider, DeltaTableProviderConfig, register_delta_sources};
    use crate::{DeltaFunnelError, DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

    struct DeltaLogTable {
        path: PathBuf,
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl DeltaLogTable {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_schema(
                name,
                DEFAULT_SCHEMA_FIELDS_JSON,
                "[]",
                r#""partitionValues":{}"#,
            )
        }

        fn new_with_schema(
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
    }

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const DEFAULT_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    const PARTITIONED_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    const NESTED_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]"#;
    const INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"bad_array\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{\"delta.columnMapping.nested.ids\":\"not an object\"}}]"#;

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

    fn register_fixture_source(
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

    #[test]
    fn datafusion_table_provider_api_symbols_are_available() -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let table: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&schema)));
        let ctx = SessionContext::new();

        ctx.register_table("orders", Arc::clone(&table))?;

        assert_eq!(table.table_type(), TableType::Base);
        assert_eq!(table.schema().as_ref(), schema.as_ref());

        Ok(())
    }

    #[test]
    fn delta_provider_exposes_logical_arrow_schema() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("schema")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;

        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let schema = provider.schema();

        assert_eq!(provider.source_name(), "orders");
        assert_eq!(provider.snapshot_version(), 1);
        assert_eq!(provider.protocol().source_name, "orders");
        assert_eq!(provider.table_type(), TableType::Base);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "customer_name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(schema.field(1).is_nullable());

        Ok(())
    }

    #[test]
    fn registers_preflighted_delta_source() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("registration")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let ctx = SessionContext::new();

        let registered = register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
            }],
        )?;

        assert_eq!(registered.sources.len(), 1);
        assert_eq!(registered.sources[0].name, "orders");
        assert_eq!(registered.sources[0].snapshot_version, 1);
        assert_eq!(registered.sources[0].schema.field(0).name(), "id");
        assert_eq!(registered.sources[0].protocol.source_name, "orders");

        Ok(())
    }

    #[tokio::test]
    async fn catalog_inspection_exposes_registered_provider_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _table = register_fixture_source(&ctx, "orders", "catalog-inspection")?;

        let catalog = ctx.catalog("datafusion").ok_or("missing default catalog")?;
        let schema = catalog.schema("public").ok_or("missing default schema")?;
        let provider = schema
            .table("orders")
            .await?
            .ok_or("missing registered table provider")?;
        let provider_schema = provider.schema();

        assert!(schema.table_names().contains(&"orders".to_owned()));
        assert_eq!(provider.table_type(), TableType::Base);
        assert_eq!(provider_schema.fields().len(), 2);
        assert_eq!(provider_schema.field(0).name(), "id");
        assert_eq!(provider_schema.field(0).data_type(), &DataType::Int32);
        assert!(!provider_schema.field(0).is_nullable());
        assert_eq!(provider_schema.field(1).name(), "customer_name");
        assert_eq!(provider_schema.field(1).data_type(), &DataType::Utf8);
        assert!(provider_schema.field(1).is_nullable());

        Ok(())
    }

    #[test]
    fn registration_failure_reports_source_context() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("registration-failure")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let ctx = SessionContext::new();
        let placeholder_schema = Arc::new(Schema::new(vec![Field::new(
            "existing",
            DataType::Utf8,
            true,
        )]));

        ctx.register_table("orders", Arc::new(EmptyTable::new(placeholder_schema)))?;
        let result = register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
            }],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration {
                source_name,
                reason,
                ..
            }) if source_name == "orders" && reason.contains("already exists")
        ));

        Ok(())
    }

    #[test]
    fn duplicate_registration_names_fail_before_partial_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("duplicate-orders")?;
        let customers = DeltaLogTable::new("duplicate-customers")?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "Orders".to_owned(),
            table_uri: customers.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let orders_preflight = preflight_delta_protocol(&orders_source)?;
        let customers_preflight = preflight_delta_protocol(&customers_source)?;
        let ctx = SessionContext::new();

        let result = register_delta_sources(
            &ctx,
            vec![
                DeltaTableProviderConfig {
                    source: orders_source,
                    protocol: orders_preflight,
                },
                DeltaTableProviderConfig {
                    source: customers_source,
                    protocol: customers_preflight,
                },
            ],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "Orders"
        ));
        assert!(!ctx.table_exist("orders")?);

        Ok(())
    }

    #[tokio::test]
    async fn sql_analysis_works_for_select_star_without_scan_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _table = register_fixture_source(&ctx, "orders", "select-star")?;

        let dataframe = ctx.sql("select * from orders").await?;
        let schema = dataframe.schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "customer_name");

        Ok(())
    }

    #[tokio::test]
    async fn sql_analysis_works_for_projection_without_delta_projection_config()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _table = register_fixture_source(&ctx, "orders", "projection")?;

        let dataframe = ctx.sql("select customer_name from orders").await?;
        let optimized = dataframe.into_optimized_plan()?;
        let schema = optimized.schema();

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "customer_name");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);

        Ok(())
    }

    #[tokio::test]
    async fn sql_analysis_works_for_join_across_registered_sources()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _orders = register_fixture_source(&ctx, "orders", "join-orders")?;
        let _customers = register_fixture_source(&ctx, "customers", "join-customers")?;

        let dataframe = ctx
            .sql(
                "select orders.id, customers.customer_name \
                 from orders join customers on orders.id = customers.id",
            )
            .await?;
        let optimized = dataframe.into_optimized_plan()?;
        let schema = optimized.schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).name(), "customer_name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

        Ok(())
    }

    #[test]
    fn provider_schema_includes_partition_columns() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "partition-schema",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;

        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let schema = provider.schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).name(), "region");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

        Ok(())
    }

    #[tokio::test]
    async fn sql_analysis_accepts_nested_source_columns_without_target_planning()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "nested-schema",
            NESTED_SCHEMA_FIELDS_JSON,
            "[]",
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let ctx = SessionContext::new();

        register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
            }],
        )?;
        let dataframe = ctx.sql("select id from orders").await?;
        let schema = dataframe.schema();

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);

        Ok(())
    }

    #[test]
    fn schema_conversion_failure_reports_source_and_field_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "schema-failure",
            INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON,
            "[]",
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;

        let result = DeltaTableProvider::try_new(source, preflight);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSourceSchema {
                source_name,
                reason,
                ..
            }) if source_name == "orders"
                && reason.contains("bad_array")
                && reason.contains("delta.columnMapping.nested.ids")
        ));

        Ok(())
    }
}
