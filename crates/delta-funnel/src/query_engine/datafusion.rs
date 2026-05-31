//! DataFusion integration.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result as DataFusionResult, not_impl_err};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;

use crate::{
    DeltaFunnelError, DeltaProtocolReport, PlannedDeltaSource, ProtocolPreflight,
    redaction::sanitize_uri_for_display,
    table_formats::{
        ProjectedDeltaScan, build_projected_delta_scan, delta_source_arrow_schema,
        validate_table_source_names,
    },
};

mod filters;

use filters::ProviderFilterPlan;

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
    /// Structured report for filters pushed into this scan.
    pub(crate) pushed_filter_plan: ProviderFilterPlan,
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

struct DeltaScanPlanningExec {
    scan_plan: ProviderScanPlan,
    properties: Arc<PlanProperties>,
}

impl DeltaScanPlanningExec {
    fn new(scan_plan: ProviderScanPlan) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&scan_plan.projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);

        Self {
            scan_plan,
            properties: Arc::new(properties),
        }
    }

    #[cfg(test)]
    fn scan_plan(&self) -> &ProviderScanPlan {
        &self.scan_plan
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
                "DeltaScanPlanningExec: source={}, snapshot_version={}, projection={:?}",
                self.scan_plan.source_name,
                self.scan_plan.snapshot_version,
                self.scan_plan.scan_projection
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
        not_impl_err!("Delta scan read execution is owned by #17 and #4")
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

    fn partition_columns(&self) -> HashSet<String> {
        self.source
            .loaded_snapshot()
            .kernel_snapshot()
            .table_configuration()
            .metadata()
            .partition_columns()
            .iter()
            .cloned()
            .collect()
    }

    #[allow(dead_code)]
    fn plan_filters(&self, filters: &[&Expr]) -> ProviderFilterPlan {
        ProviderFilterPlan::unsupported(filters, &self.schema, &self.partition_columns())
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
            pushed_filter_plan: ProviderFilterPlan::empty_pushed(),
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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !filters.is_empty() {
            return Err(DataFusionError::Plan(
                "Delta scan received pushed filters even though filter pushdown is unsupported"
                    .to_owned(),
            ));
        }

        let scan_plan = self
            .plan_scan(ProviderScanPlanRequest {
                requested_projection: projection.cloned(),
            })
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        Ok(Arc::new(DeltaScanPlanningExec::new(scan_plan)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(self.plan_filters(filters).pushdown_statuses)
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
    use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::logical_expr::{
        ColumnarValue, Expr, TableProviderFilterPushDown, Volatility, cast, col, create_udf, lit,
    };
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::filters::{ProviderFilterPushdownKind, ProviderFilterReason};
    use super::{
        DeltaScanPlanningExec, DeltaTableProvider, DeltaTableProviderConfig,
        ProviderScanPlanRequest, register_delta_sources,
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

    fn find_delta_scan_plans<'a>(
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

    #[tokio::test]
    async fn table_provider_scan_returns_projected_non_reading_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-scan-projection")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let projection = vec![1];

        let plan = provider
            .scan(&state, Some(&projection), &[], Some(10))
            .await?;
        let delta_plan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().field(0).name(), "customer_name");
        assert_eq!(delta_plan.scan_plan().source_name, "orders");
        assert_eq!(delta_plan.scan_plan().scan_projection, Some(vec![1]));

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_without_projection_returns_full_non_reading_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-full-scan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();

        let plan = provider.scan(&state, None, &[], None).await?;
        let delta_plan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(plan.schema().fields().len(), 2);
        assert_eq!(plan.schema().field(0).name(), "id");
        assert_eq!(plan.schema().field(1).name(), "customer_name");
        assert_eq!(delta_plan.scan_plan().scan_projection, None);

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_invalid_projection_before_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-invalid-projection")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let projection = vec![2];

        let result = provider.scan(&state, Some(&projection), &[], None).await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("projection index 2 is out of bounds"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_duplicate_projection_at_public_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-duplicate-projection")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let projection = vec![1, 1];

        let result = provider.scan(&state, Some(&projection), &[], None).await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("projection index 1 is duplicated"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_rejects_direct_filter_injection()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-filter-injection")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let filter = datafusion::logical_expr::col("id").eq(datafusion::logical_expr::lit(7));

        let result = provider.scan(&state, None, &[filter], None).await;

        assert!(
            matches!(result, Err(DataFusionError::Plan(message)) if message
            .contains("filter pushdown is unsupported"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn table_provider_scan_limit_does_not_change_projection_contract()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("table-provider-limit-unsupported")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();
        let projection = vec![0];

        let with_limit = provider
            .scan(&state, Some(&projection), &[], Some(1))
            .await?;
        let without_limit = provider.scan(&state, Some(&projection), &[], None).await?;
        let with_limit_scan = with_limit
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let without_limit_scan = without_limit
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(with_limit.schema(), without_limit.schema());
        assert_eq!(
            with_limit_scan.scan_plan().scan_projection,
            without_limit_scan.scan_plan().scan_projection
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

        Ok(())
    }

    #[test]
    fn filter_pushdown_is_explicitly_unsupported_for_all_filters()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("filter-pushdown-unsupported")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let id_filter = datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1));
        let name_filter =
            datafusion::logical_expr::col("customer_name").eq(datafusion::logical_expr::lit("a"));

        let support = provider.supports_filters_pushdown(&[&id_filter, &name_filter])?;

        assert_eq!(
            support,
            vec![
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported
            ]
        );

        Ok(())
    }

    #[test]
    fn filter_plan_empty_input_has_consistent_zero_counts() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("empty-filter-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_filters(&[]);

        assert!(plan.pushdown_statuses.is_empty());
        assert!(plan.decisions.is_empty());
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, 0);

        Ok(())
    }

    #[test]
    fn filter_plan_preserves_order_duplicates_and_column_classification()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "ordered-filter-plan",
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
        let region_filter = col("region").eq(lit("us-west"));
        let id_filter = col("id").gt(lit(1));
        let id_filter_duplicate = col("id").gt(lit(1));
        let unknown_filter = col("ghost_column").eq(lit("x"));
        let internal_filter = col("__delta_funnel_file_id").eq(lit("part-00001.parquet"));

        let plan = provider.plan_filters(&[
            &region_filter,
            &id_filter,
            &id_filter_duplicate,
            &unknown_filter,
            &internal_filter,
        ]);

        assert_eq!(
            plan.pushdown_statuses,
            vec![
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 5);
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, 5);
        assert_eq!(
            plan.decisions
                .iter()
                .map(|decision| decision.input_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );

        assert_eq!(
            plan.decisions[0].kind,
            ProviderFilterPushdownKind::Unsupported
        );
        assert_eq!(
            plan.decisions[0].reason,
            ProviderFilterReason::UnsupportedInitialPolicy
        );
        assert_eq!(plan.decisions[0].referenced_columns, vec!["region"]);
        assert_eq!(plan.decisions[0].partition_columns, vec!["region"]);
        assert!(plan.decisions[0].data_columns.is_empty());
        assert!(plan.decisions[0].unknown_columns.is_empty());

        assert_eq!(plan.decisions[1].referenced_columns, vec!["id"]);
        assert_eq!(plan.decisions[1].data_columns, vec!["id"]);
        assert!(plan.decisions[1].partition_columns.is_empty());
        assert_eq!(plan.decisions[2].referenced_columns, vec!["id"]);
        assert_eq!(
            plan.decisions[3].reason,
            ProviderFilterReason::UnsupportedUnknownColumn
        );
        assert_eq!(plan.decisions[3].unknown_columns, vec!["ghost_column"]);
        assert_eq!(
            plan.decisions[4].reason,
            ProviderFilterReason::UnsupportedInternalColumn
        );
        assert_eq!(
            plan.decisions[4].unknown_columns,
            vec!["__delta_funnel_file_id"]
        );

        Ok(())
    }

    #[test]
    fn filter_plan_marks_complex_expression_shapes_unsupported()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("complex-filter-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let cast_filter = cast(col("id"), DataType::Int64).eq(lit(7_i64));
        let and_filter = col("id")
            .gt(lit(1))
            .and(col("customer_name").eq(lit("alice")));
        let or_filter = col("id")
            .gt(lit(1))
            .or(col("customer_name").eq(lit("alice")));
        let not_filter = Expr::Not(Box::new(col("id").gt(lit(1))));
        let scalar_udf = create_udf(
            "is_interesting",
            vec![DataType::Utf8],
            DataType::Boolean,
            Volatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))))),
        );
        let scalar_function_filter =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![col("customer_name")],
            ));

        let plan = provider.plan_filters(&[
            &cast_filter,
            &and_filter,
            &or_filter,
            &not_filter,
            &scalar_function_filter,
        ]);

        assert_eq!(plan.unsupported_count, 5);
        assert_eq!(plan.residual_filter_count, 5);
        assert_eq!(
            plan.decisions
                .iter()
                .map(|decision| decision.reason)
                .collect::<Vec<_>>(),
            vec![
                ProviderFilterReason::UnsupportedExpressionShape,
                ProviderFilterReason::UnsupportedExpressionShape,
                ProviderFilterReason::UnsupportedExpressionShape,
                ProviderFilterReason::UnsupportedExpressionShape,
                ProviderFilterReason::UnsupportedExpressionShape
            ]
        );

        Ok(())
    }

    #[test]
    fn filter_plan_tracks_nested_field_reference_as_unsupported_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "nested-filter-plan",
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
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let nested_filter = col("profile.age").gt(lit(21));

        let plan = provider.plan_filters(&[&nested_filter]);

        assert_eq!(
            plan.pushdown_statuses,
            vec![TableProviderFilterPushDown::Unsupported]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 1);
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, 1);
        assert_eq!(plan.decisions[0].referenced_columns, vec!["profile.age"]);
        assert_eq!(plan.decisions[0].unknown_columns, vec!["profile.age"]);
        assert_eq!(
            plan.decisions[0].reason,
            ProviderFilterReason::UnsupportedUnknownColumn
        );

        Ok(())
    }

    #[test]
    fn filter_planning_contract_does_not_call_kernel_or_read_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src")
                .join("query_engine")
                .join("datafusion")
                .join("filters.rs"),
        )?;

        assert!(!source.contains("with_predicate"));
        assert!(!source.contains("with_filter"));
        assert!(!source.contains("RecordBatch"));
        assert!(!source.to_ascii_lowercase().contains("parquet"));

        Ok(())
    }

    #[test]
    fn filter_plan_reason_codes_are_control_character_safe()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("filter-plan-control-characters")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let hostile_filter = col("ghost\ncolumn").eq(lit("x"));

        let plan = provider.plan_filters(&[&hostile_filter]);
        let reason_code = plan.decisions[0].reason.code();

        assert_eq!(
            plan.decisions[0].reason,
            ProviderFilterReason::UnsupportedUnknownColumn
        );
        assert!(!reason_code.contains('\n'));
        assert!(!reason_code.contains('\r'));
        assert!(!reason_code.contains('\t'));
        assert_eq!(reason_code, "unsupported_unknown_column");

        Ok(())
    }

    #[test]
    fn scan_plan_preserves_empty_pushed_filter_report() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("empty-pushed-filter-report")?;
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

        assert!(plan.pushed_filter_plan.pushdown_statuses.is_empty());
        assert!(plan.pushed_filter_plan.decisions.is_empty());
        assert_eq!(plan.pushed_filter_plan.exact_count, 0);
        assert_eq!(plan.pushed_filter_plan.inexact_count, 0);
        assert_eq!(plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_plan.pushed_filter_count, 0);
        assert_eq!(plan.pushed_filter_plan.residual_filter_count, 0);

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
    async fn residual_filter_column_remains_available_below_final_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _table = register_fixture_source(&ctx, "orders", "residual-filter-projection")?;

        let dataframe = ctx
            .sql("select id from orders where customer_name = 'alice'")
            .await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(plan_display.contains("FilterExec"), "{plan_display}");
        assert!(
            plan_display.contains("DeltaScanPlanningExec"),
            "{plan_display}"
        );
        assert_eq!(physical_plan.schema().fields().len(), 1);
        assert_eq!(physical_plan.schema().field(0).name(), "id");
        assert_eq!(scans.len(), 1);
        assert_eq!(
            scans[0].scan_plan().scan_projection,
            Some(vec![0, 1]),
            "scan must keep the residual filter column even though final output only projects id"
        );
        assert_eq!(scans[0].schema().fields().len(), 2);
        assert_eq!(scans[0].schema().field(0).name(), "id");
        assert_eq!(scans[0].schema().field(1).name(), "customer_name");

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
                .contains("Delta scan read execution is owned by #17 and #4")
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
