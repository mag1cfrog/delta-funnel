//! DataFusion integration.

use std::any::Any;
use std::collections::HashSet;
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
    redaction::sanitize_uri_for_display,
    table_formats::{
        ProjectedDeltaScan, build_projected_delta_scan, delta_source_arrow_schema,
        validate_table_source_names,
    },
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

/// Caller request used to build a provider scan plan.
#[allow(dead_code)]
pub(crate) struct ProviderScanPlanRequest {
    /// Requested DataFusion projection indexes against the provider logical schema.
    pub(crate) requested_projection: Option<Vec<usize>>,
}

/// Kernel-backed scan intent for one Delta provider scan.
#[allow(dead_code)]
pub(crate) struct ProviderScanPlan {
    /// DataFusion table name for this source.
    pub(crate) source_name: String,
    /// Normalized Delta table URI for this source.
    pub(crate) table_uri: String,
    /// Resolved Delta snapshot version.
    pub(crate) snapshot_version: u64,
    /// Arrow schema expected from this provider scan.
    pub(crate) projected_schema: SchemaRef,
    /// Protocol report captured before provider registration.
    pub(crate) protocol: DeltaProtocolReport,
    /// Projection indexes accepted and used for this scan, if any.
    pub(crate) scan_projection: Option<Vec<usize>>,
    kernel_scan: ProjectedDeltaScan,
}

impl ProviderScanPlan {
    /// Returns the private kernel scan state for later provider scan phases.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn kernel_scan(&self) -> &ProjectedDeltaScan {
        &self.kernel_scan
    }
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
    let providers = configs
        .into_iter()
        .map(|config| DeltaTableProvider::try_new(config.source, config.protocol))
        .collect::<Result<Vec<_>, _>>()?;

    reject_existing_registration_names(ctx, &providers)?;

    let sources = register_delta_providers(ctx, providers)?;

    Ok(RegisteredDeltaSources { sources })
}

fn reject_duplicate_registration_names(
    configs: &[DeltaTableProviderConfig],
) -> Result<(), DeltaFunnelError> {
    validate_table_source_names(configs.iter().map(|config| config.source.name()))
}

fn reject_existing_registration_names(
    ctx: &SessionContext,
    providers: &[DeltaTableProvider],
) -> Result<(), DeltaFunnelError> {
    let state = ctx.state();
    let catalog_options = &state.config_options().catalog;
    let default_catalog = ctx.catalog(&catalog_options.default_catalog);
    let default_schema = default_catalog
        .as_ref()
        .and_then(|catalog| catalog.schema(&catalog_options.default_schema));
    let existing_names = default_schema
        .as_ref()
        .map_or_else(Vec::new, |schema| schema.table_names());

    for provider in providers {
        if let Some(existing_name) = existing_names
            .iter()
            .find(|existing_name| existing_name.eq_ignore_ascii_case(provider.source_name()))
        {
            return Err(DeltaFunnelError::DataFusionRegistration {
                source_name: provider.source_name().to_owned(),
                table_uri: provider.source.table_uri().to_owned(),
                reason: format!("table already exists: {existing_name}"),
            });
        }
    }

    Ok(())
}

fn register_delta_provider(
    ctx: &SessionContext,
    provider: DeltaTableProvider,
) -> Result<RegisteredDeltaSource, DeltaFunnelError> {
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

fn register_delta_providers(
    ctx: &SessionContext,
    providers: Vec<DeltaTableProvider>,
) -> Result<Vec<RegisteredDeltaSource>, DeltaFunnelError> {
    let mut registered_sources = Vec::with_capacity(providers.len());
    let mut registered_names = Vec::with_capacity(providers.len());

    for provider in providers {
        let registered = match register_delta_provider(ctx, provider) {
            Ok(registered) => registered,
            Err(error) => {
                rollback_registered_delta_sources(ctx, &registered_names);
                return Err(error);
            }
        };

        registered_names.push(registered.name.clone());
        registered_sources.push(registered);
    }

    Ok(registered_sources)
}

fn rollback_registered_delta_sources(ctx: &SessionContext, names: &[String]) {
    for name in names.iter().rev() {
        let _ = ctx.deregister_table(name.as_str());
    }
}

impl DeltaTableProvider {
    fn try_new(
        source: PlannedDeltaSource,
        preflight: ProtocolPreflight,
    ) -> Result<Self, DeltaFunnelError> {
        reject_mismatched_preflight(&source, preflight.protocol())?;
        let schema = delta_source_arrow_schema(&source).map_err(|reason| {
            DeltaFunnelError::DeltaSourceSchema {
                source_name: source.name().to_owned(),
                table_uri: source.table_uri().to_owned(),
                reason,
            }
        })?;

        Ok(Self {
            source,
            protocol: preflight.into_protocol(),
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

    #[allow(dead_code)]
    fn plan_scan(
        &self,
        request: ProviderScanPlanRequest,
    ) -> Result<ProviderScanPlan, DeltaFunnelError> {
        let ProjectionPlan {
            projected_schema,
            scan_projection,
            projected_column_names,
        } = self.plan_projection(request.requested_projection)?;
        let kernel_scan =
            build_projected_delta_scan(&self.source, projected_column_names.as_deref()).map_err(
                |source| DeltaFunnelError::DeltaScanConstruction {
                    source_name: self.source_name().to_owned(),
                    table_uri: self.source.table_uri().to_owned(),
                    source: Box::new(source),
                },
            )?;

        Ok(ProviderScanPlan {
            source_name: self.source_name().to_owned(),
            table_uri: self.source.table_uri().to_owned(),
            snapshot_version: self.snapshot_version(),
            projected_schema,
            protocol: self.protocol.clone(),
            scan_projection,
            kernel_scan,
        })
    }

    #[allow(dead_code)]
    fn plan_projection(
        &self,
        projection: Option<Vec<usize>>,
    ) -> Result<ProjectionPlan, DeltaFunnelError> {
        let Some(projection) = projection else {
            return Ok(ProjectionPlan {
                projected_schema: self.schema(),
                scan_projection: None,
                projected_column_names: None,
            });
        };

        reject_duplicate_projection_indexes(&projection).map_err(|reason| {
            DeltaFunnelError::DeltaScanProjection {
                source_name: self.source_name().to_owned(),
                table_uri: self.source.table_uri().to_owned(),
                reason,
            }
        })?;

        let projected_column_names = projection
            .iter()
            .map(|index| {
                self.schema.fields().get(*index).map_or_else(
                    || {
                        Err(DeltaFunnelError::DeltaScanProjection {
                            source_name: self.source_name().to_owned(),
                            table_uri: self.source.table_uri().to_owned(),
                            reason: format!(
                                "projection index {index} is out of bounds for schema with {} fields",
                                self.schema.fields().len()
                            ),
                        })
                    },
                    |field| Ok(field.name().to_owned()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let projected_schema =
            Arc::new(self.schema.as_ref().project(&projection).map_err(|error| {
                DeltaFunnelError::DeltaScanProjection {
                    source_name: self.source_name().to_owned(),
                    table_uri: self.source.table_uri().to_owned(),
                    reason: error.to_string(),
                }
            })?);

        Ok(ProjectionPlan {
            projected_schema,
            scan_projection: Some(projection),
            projected_column_names: Some(projected_column_names),
        })
    }
}

#[allow(dead_code)]
struct ProjectionPlan {
    projected_schema: SchemaRef,
    scan_projection: Option<Vec<usize>>,
    projected_column_names: Option<Vec<String>>,
}

#[allow(dead_code)]
fn reject_duplicate_projection_indexes(projection: &[usize]) -> Result<(), String> {
    let mut seen = HashSet::with_capacity(projection.len());

    for index in projection {
        if !seen.insert(*index) {
            return Err(format!("projection index {index} is duplicated"));
        }
    }

    Ok(())
}

fn reject_mismatched_preflight(
    source: &PlannedDeltaSource,
    protocol: &DeltaProtocolReport,
) -> Result<(), DeltaFunnelError> {
    let source_table_uri = sanitize_uri_for_display(source.table_uri());

    if protocol.source_name != source.name()
        || protocol.snapshot_version != source.version()
        || protocol.table_uri != source_table_uri
    {
        return Err(DeltaFunnelError::DataFusionRegistration {
            source_name: source.name().to_owned(),
            table_uri: source.table_uri().to_owned(),
            reason: format!(
                "protocol preflight belongs to source `{}` at snapshot version {} ({})",
                protocol.source_name, protocol.snapshot_version, protocol.table_uri
            ),
        });
    }

    Ok(())
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
    use std::any::Any;
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::{CatalogProvider, SchemaProvider};
    use datafusion::common::{DataFusionError, Result as DataFusionResult};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::{
        DeltaTableProvider, DeltaTableProviderConfig, ProviderScanPlanRequest,
        register_delta_sources,
    };
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

    #[derive(Debug, Default)]
    struct FailsOnCustomersSchemaProvider {
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
    struct SingleSchemaCatalogProvider {
        schema: Arc<dyn SchemaProvider>,
    }

    impl SingleSchemaCatalogProvider {
        fn new(schema: Arc<dyn SchemaProvider>) -> Self {
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
    fn full_projection_scan_plan_preserves_source_context() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("full-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: None,
        })?;

        assert_eq!(plan.source_name, "orders");
        assert!(plan.table_uri.starts_with("file://"));
        assert_eq!(plan.snapshot_version, 1);
        assert_eq!(plan.protocol.source_name, "orders");
        assert_eq!(plan.scan_projection, None);
        assert_eq!(plan.projected_schema.fields().len(), 2);
        assert_eq!(plan.projected_schema.field(0).name(), "id");
        assert_eq!(plan.projected_schema.field(1).name(), "customer_name");
        assert_eq!(plan.kernel_scan().kernel_schema().num_fields(), 2);
        let _ = plan.kernel_scan().kernel_scan();

        Ok(())
    }

    #[test]
    fn projected_scan_plan_preserves_requested_order() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("ordered-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![1, 0]),
        })?;

        assert_eq!(plan.scan_projection, Some(vec![1, 0]));
        assert_eq!(plan.projected_schema.fields().len(), 2);
        assert_eq!(plan.projected_schema.field(0).name(), "customer_name");
        assert_eq!(plan.projected_schema.field(1).name(), "id");
        assert_eq!(plan.kernel_scan().kernel_schema().num_fields(), 2);
        let kernel_names = plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(kernel_names, vec!["customer_name", "id"]);

        Ok(())
    }

    #[test]
    fn single_column_scan_plan_projects_kernel_and_arrow_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("single-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![1]),
        })?;

        assert_eq!(plan.projected_schema.fields().len(), 1);
        assert_eq!(plan.projected_schema.field(0).name(), "customer_name");
        assert_eq!(plan.projected_schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(plan.kernel_scan().kernel_schema().num_fields(), 1);
        let kernel_field = plan
            .kernel_scan()
            .kernel_schema()
            .field_at_index(0)
            .ok_or("missing projected kernel field")?;
        assert_eq!(kernel_field.name(), "customer_name");

        Ok(())
    }

    #[test]
    fn empty_projection_scan_plan_is_valid_for_count_style_scans()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("empty-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![]),
        })?;

        assert_eq!(plan.scan_projection, Some(vec![]));
        assert_eq!(plan.projected_schema.fields().len(), 0);
        assert_eq!(plan.kernel_scan().kernel_schema().num_fields(), 0);

        Ok(())
    }

    #[test]
    fn duplicate_projection_indexes_fail_before_kernel_scan_construction()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("duplicate-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let result = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![1, 1]),
        });

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaScanProjection {
                source_name,
                reason,
                ..
            }) if source_name == "orders"
                && reason.contains("projection index 1 is duplicated")
        ));

        Ok(())
    }

    #[test]
    fn invalid_projection_index_fails_before_execution() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("invalid-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let result = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![2]),
        });

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaScanProjection {
                source_name,
                reason,
                ..
            }) if source_name == "orders"
                && reason.contains("projection index 2 is out of bounds")
        ));

        Ok(())
    }

    #[test]
    fn hostile_projection_index_fails_without_overflow_or_panic()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("hostile-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let result = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![usize::MAX]),
        });

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaScanProjection {
                source_name,
                reason,
                ..
            }) if source_name == "orders"
                && reason.contains("projection index")
                && reason.contains("out of bounds")
        ));

        Ok(())
    }

    #[test]
    fn schema_drift_between_arrow_and_kernel_fails_instead_of_full_scan_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("schema-drift-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let mut provider = DeltaTableProvider::try_new(source, preflight)?;
        provider.schema = Arc::new(Schema::new(vec![Field::new(
            "ghost_column",
            DataType::Utf8,
            true,
        )]));

        let result = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
        });

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaScanConstruction {
                source_name,
                source,
                ..
            }) if source_name == "orders"
                && source.to_string().contains("ghost_column")
        ));

        Ok(())
    }

    #[test]
    fn scan_construction_error_display_escapes_control_characters()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("schema-drift-redaction-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let mut provider = DeltaTableProvider::try_new(source, preflight)?;
        provider.schema = Arc::new(Schema::new(vec![Field::new(
            "ghost\ncolumn",
            DataType::Utf8,
            true,
        )]));

        let error = match provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
        }) {
            Ok(_) => return Err("tampered schema should not build a kernel scan".into()),
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("ghost\\ncolumn"));
        assert!(!display.contains("ghost\ncolumn"));

        Ok(())
    }

    #[test]
    fn provider_scan_plan_dependencies_use_official_delta_kernel_only()
    -> Result<(), Box<dyn std::error::Error>> {
        let manifest =
            fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))?;

        assert!(manifest.contains("delta_kernel"));
        assert!(!manifest.contains("deltalake"));
        assert!(!manifest.contains("buoyant_kernel"));

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
    fn mismatched_preflight_is_rejected_before_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("mismatched-preflight-orders")?;
        let customers = DeltaLogTable::new("mismatched-preflight-customers")?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "customers".to_owned(),
            table_uri: customers.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let customers_preflight = preflight_delta_protocol(&customers_source)?;
        let ctx = SessionContext::new();

        let result = register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source: orders_source,
                protocol: customers_preflight,
            }],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration {
                source_name,
                reason,
                ..
            }) if source_name == "orders"
                && reason.contains("protocol preflight belongs to source `customers`")
        ));
        assert!(!ctx.table_exist("orders")?);

        Ok(())
    }

    #[test]
    fn existing_table_conflict_fails_before_partial_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("existing-conflict-orders")?;
        let customers = DeltaLogTable::new("existing-conflict-customers")?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "customers".to_owned(),
            table_uri: customers.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let orders_preflight = preflight_delta_protocol(&orders_source)?;
        let customers_preflight = preflight_delta_protocol(&customers_source)?;
        let ctx = SessionContext::new();
        let placeholder_schema = Arc::new(Schema::new(vec![Field::new(
            "existing",
            DataType::Utf8,
            true,
        )]));

        ctx.register_table("customers", Arc::new(EmptyTable::new(placeholder_schema)))?;
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
            Err(DeltaFunnelError::DataFusionRegistration {
                source_name,
                reason,
                ..
            }) if source_name == "customers" && reason.contains("already exists")
        ));
        assert!(!ctx.table_exist("orders")?);
        assert!(ctx.table_exist("customers")?);

        Ok(())
    }

    #[test]
    fn late_registration_failure_rolls_back_prior_sources() -> Result<(), Box<dyn std::error::Error>>
    {
        let orders = DeltaLogTable::new("rollback-orders")?;
        let customers = DeltaLogTable::new("rollback-customers")?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "customers".to_owned(),
            table_uri: customers.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let orders_preflight = preflight_delta_protocol(&orders_source)?;
        let customers_preflight = preflight_delta_protocol(&customers_source)?;
        let ctx = SessionContext::new();
        let failing_schema: Arc<dyn SchemaProvider> =
            Arc::new(FailsOnCustomersSchemaProvider::default());

        ctx.register_catalog(
            "datafusion",
            Arc::new(SingleSchemaCatalogProvider::new(Arc::clone(
                &failing_schema,
            ))),
        );
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
            Err(DeltaFunnelError::DataFusionRegistration {
                source_name,
                reason,
                ..
            }) if source_name == "customers"
                && reason.contains("forced customers registration failure")
        ));
        assert!(!ctx.table_exist("orders")?);
        assert!(!ctx.table_exist("customers")?);

        Ok(())
    }

    #[test]
    fn existing_table_conflict_uses_configured_default_catalog_and_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("custom-default-orders")?;
        let customers = DeltaLogTable::new("custom-default-customers")?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "customers".to_owned(),
            table_uri: customers.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let orders_preflight = preflight_delta_protocol(&orders_source)?;
        let customers_preflight = preflight_delta_protocol(&customers_source)?;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_default_catalog_and_schema("custom_catalog", "custom_schema"),
        );
        let placeholder_schema = Arc::new(Schema::new(vec![Field::new(
            "existing",
            DataType::Utf8,
            true,
        )]));

        ctx.register_table("customers", Arc::new(EmptyTable::new(placeholder_schema)))?;
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
            Err(DeltaFunnelError::DataFusionRegistration {
                source_name,
                reason,
                ..
            }) if source_name == "customers" && reason.contains("already exists")
        ));
        assert!(!ctx.table_exist("orders")?);
        assert!(ctx.table_exist("customers")?);

        Ok(())
    }

    #[test]
    fn schema_conversion_failure_fails_before_partial_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("schema-partial-orders")?;
        let customers = DeltaLogTable::new_with_schema(
            "schema-partial-customers",
            INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON,
            "[]",
            r#""partitionValues":{}"#,
        )?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "customers".to_owned(),
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
            Err(DeltaFunnelError::DeltaSourceSchema {
                source_name,
                reason,
                ..
            }) if source_name == "customers"
                && reason.contains("bad_array")
                && reason.contains("delta.columnMapping.nested.ids")
        ));
        assert!(!ctx.table_exist("orders")?);
        assert!(!ctx.table_exist("customers")?);

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
                .contains("Delta scan execution is owned by a later implementation issue")
        ));

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
