//! Rust backing model for lazy query-load orchestration.
//!
//! This module owns the high-level session and request data shapes that will
//! later back the Python `Session` and `Table` API. It intentionally does not
//! load Delta sources, parse SQL, contact SQL Server, or execute rows.

use std::fmt;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;

use crate::{
    DeltaFunnelError, DeltaProtocolReport, DeltaProviderScanExecutionOptions, DeltaSourceConfig,
    DeltaTableProviderConfig, MssqlConnectionConfig, MssqlSchemaPlanOptions, MssqlTargetConfig,
    MssqlWorkflowWriteOptions, MssqlWriteOptions, QueryOptions, RegisteredDeltaSource,
    datafusion_session_context, default_mssql_write_options, load_delta_source,
    preflight_delta_protocol, redaction::sanitize_text_for_display,
    register_delta_sources_with_scan_execution_options,
};

/// Query-load action mode requested by a caller.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RunMode {
    /// Plan and execute the selected output workflow.
    #[default]
    Execute,
    /// Reuse planning paths without row production or SQL Server write effects.
    DryRun,
}

/// Batch-shaping options reserved for the orchestrator output stream.
///
/// Issue #237 owns the stream adapter that will enforce these bounds. This
/// early data shape validates local values so session construction can fail
/// before any source reads or target side effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchOptions {
    max_rows_per_batch: usize,
    max_buffered_batches: usize,
    max_buffered_rows: usize,
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            max_rows_per_batch: 100_000,
            max_buffered_batches: 1,
            max_buffered_rows: 100_000,
        }
    }
}

impl BatchOptions {
    /// Builds validated batch-shaping options.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaFunnelError::BatchPipeline`] when any bound is zero.
    pub fn try_new(
        max_rows_per_batch: usize,
        max_buffered_batches: usize,
        max_buffered_rows: usize,
    ) -> Result<Self, DeltaFunnelError> {
        let options = Self {
            max_rows_per_batch,
            max_buffered_batches,
            max_buffered_rows,
        };
        options.validate()?;
        Ok(options)
    }

    /// Returns the maximum rows allowed in one shaped output batch.
    #[must_use]
    pub const fn max_rows_per_batch(&self) -> usize {
        self.max_rows_per_batch
    }

    /// Returns the maximum shaped batches buffered by the orchestrator.
    #[must_use]
    pub const fn max_buffered_batches(&self) -> usize {
        self.max_buffered_batches
    }

    /// Returns the maximum shaped rows buffered by the orchestrator.
    #[must_use]
    pub const fn max_buffered_rows(&self) -> usize {
        self.max_buffered_rows
    }

    /// Validates batch-shaping bounds before execution side effects.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaFunnelError::BatchPipeline`] when any bound is zero.
    pub fn validate(&self) -> Result<(), DeltaFunnelError> {
        validate_nonzero_batch_option("max_rows_per_batch", self.max_rows_per_batch)?;
        validate_nonzero_batch_option("max_buffered_batches", self.max_buffered_batches)?;
        validate_nonzero_batch_option("max_buffered_rows", self.max_buffered_rows)?;
        Ok(())
    }
}

/// Validation options that can be checked before workflow side effects.
///
/// Rich row-count and target-side validation belongs to issue #10. This type
/// exists so the session API can carry validation intent without starting
/// validation I/O in the session-model slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationOptions {
    require_successful_planning: bool,
}

impl Default for ValidationOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidationOptions {
    /// Creates default local validation options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            require_successful_planning: true,
        }
    }

    /// Returns whether planning failures should be treated as terminal.
    #[must_use]
    pub const fn require_successful_planning(&self) -> bool {
        self.require_successful_planning
    }

    /// Validates local validation options before workflow side effects.
    ///
    /// # Errors
    ///
    /// Currently returns `Ok(())` for all representable values. The method is
    /// intentionally present so later validation options can be wired through
    /// the same pre-side-effect path.
    pub const fn validate(&self) -> Result<(), DeltaFunnelError> {
        let _ = self.require_successful_planning;
        Ok(())
    }
}

/// Session-wide options for lazy query-load orchestration.
#[derive(Clone)]
pub struct SessionOptions {
    query_options: QueryOptions,
    provider_scan_options: DeltaProviderScanExecutionOptions,
    batch_options: BatchOptions,
    mssql_schema_options: MssqlSchemaPlanOptions,
    mssql_write_options: MssqlWriteOptions,
    mssql_workflow_options: MssqlWorkflowWriteOptions,
    validation_options: ValidationOptions,
    default_mssql_connection: Option<MssqlConnectionConfig>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            query_options: QueryOptions::default(),
            provider_scan_options: DeltaProviderScanExecutionOptions::default(),
            batch_options: BatchOptions::default(),
            mssql_schema_options: MssqlSchemaPlanOptions::default(),
            mssql_write_options: default_mssql_write_options(),
            mssql_workflow_options: MssqlWorkflowWriteOptions::default(),
            validation_options: ValidationOptions::default(),
            default_mssql_connection: None,
        }
    }
}

impl SessionOptions {
    /// Creates default session options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets DataFusion query execution options.
    #[must_use]
    pub const fn with_query_options(mut self, query_options: QueryOptions) -> Self {
        self.query_options = query_options;
        self
    }

    /// Sets Delta provider scan execution options.
    #[must_use]
    pub const fn with_provider_scan_options(
        mut self,
        provider_scan_options: DeltaProviderScanExecutionOptions,
    ) -> Self {
        self.provider_scan_options = provider_scan_options;
        self
    }

    /// Sets orchestrator batch-shaping options.
    #[must_use]
    pub const fn with_batch_options(mut self, batch_options: BatchOptions) -> Self {
        self.batch_options = batch_options;
        self
    }

    /// Sets SQL Server schema planning options.
    #[must_use]
    pub const fn with_mssql_schema_options(
        mut self,
        mssql_schema_options: MssqlSchemaPlanOptions,
    ) -> Self {
        self.mssql_schema_options = mssql_schema_options;
        self
    }

    /// Sets SQL Server write options.
    #[must_use]
    pub const fn with_mssql_write_options(
        mut self,
        mssql_write_options: MssqlWriteOptions,
    ) -> Self {
        self.mssql_write_options = mssql_write_options;
        self
    }

    /// Sets SQL Server multi-output workflow options.
    #[must_use]
    pub const fn with_mssql_workflow_options(
        mut self,
        mssql_workflow_options: MssqlWorkflowWriteOptions,
    ) -> Self {
        self.mssql_workflow_options = mssql_workflow_options;
        self
    }

    /// Sets locally checkable validation options.
    #[must_use]
    pub const fn with_validation_options(mut self, validation_options: ValidationOptions) -> Self {
        self.validation_options = validation_options;
        self
    }

    /// Sets the session-level default SQL Server connection.
    #[must_use]
    pub fn with_default_mssql_connection(
        mut self,
        default_mssql_connection: MssqlConnectionConfig,
    ) -> Self {
        self.default_mssql_connection = Some(default_mssql_connection);
        self
    }

    /// Returns DataFusion query execution options.
    #[must_use]
    pub const fn query_options(&self) -> QueryOptions {
        self.query_options
    }

    /// Returns Delta provider scan execution options.
    #[must_use]
    pub const fn provider_scan_options(&self) -> DeltaProviderScanExecutionOptions {
        self.provider_scan_options
    }

    /// Returns orchestrator batch-shaping options.
    #[must_use]
    pub const fn batch_options(&self) -> BatchOptions {
        self.batch_options
    }

    /// Returns SQL Server schema planning options.
    #[must_use]
    pub const fn mssql_schema_options(&self) -> MssqlSchemaPlanOptions {
        self.mssql_schema_options
    }

    /// Returns SQL Server write options.
    #[must_use]
    pub const fn mssql_write_options(&self) -> MssqlWriteOptions {
        self.mssql_write_options
    }

    /// Returns SQL Server multi-output workflow options.
    #[must_use]
    pub const fn mssql_workflow_options(&self) -> MssqlWorkflowWriteOptions {
        self.mssql_workflow_options
    }

    /// Returns locally checkable validation options.
    #[must_use]
    pub const fn validation_options(&self) -> ValidationOptions {
        self.validation_options
    }

    /// Returns the optional session-level default SQL Server connection.
    #[must_use]
    pub fn default_mssql_connection(&self) -> Option<&MssqlConnectionConfig> {
        self.default_mssql_connection.as_ref()
    }

    /// Validates local options before workflow side effects.
    ///
    /// # Errors
    ///
    /// Returns the first validation error from query, provider, batch,
    /// workflow, or local validation options.
    pub fn validate(&self) -> Result<(), DeltaFunnelError> {
        self.query_options.validate()?;
        self.provider_scan_options.validate()?;
        self.batch_options.validate()?;
        self.mssql_workflow_options.validate()?;
        self.validation_options.validate()?;
        Ok(())
    }
}

impl fmt::Debug for SessionOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionOptions")
            .field("query_options", &self.query_options)
            .field("provider_scan_options", &self.provider_scan_options)
            .field("batch_options", &self.batch_options)
            .field("mssql_schema_options", &self.mssql_schema_options)
            .field("mssql_write_options", &self.mssql_write_options)
            .field("mssql_workflow_options", &self.mssql_workflow_options)
            .field("validation_options", &self.validation_options)
            .field(
                "default_mssql_connection",
                &self
                    .default_mssql_connection
                    .as_ref()
                    .map(MssqlConnectionConfig::summary),
            )
            .finish()
    }
}

/// Lazy table identity owned by a query-load session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LazyTable {
    id: LazyTableId,
    kind: LazyTableKind,
    name: String,
}

impl LazyTable {
    /// Creates a placeholder lazy table handle for future registration slices.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn placeholder(id: u64, kind: LazyTableKind) -> Self {
        Self {
            id: LazyTableId(id),
            kind,
            name: format!("table_{id}"),
        }
    }

    fn delta_source(id: u64, name: String) -> Self {
        Self {
            id: LazyTableId(id),
            kind: LazyTableKind::DeltaSource,
            name,
        }
    }

    /// Returns the stable session-local table id.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id.0
    }

    /// Returns the lazy table kind.
    #[must_use]
    pub const fn kind(&self) -> LazyTableKind {
        self.kind
    }

    /// Returns the session-owned table name for this lazy table.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LazyTableId(u64);

/// Kind of lazy table represented by a session handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LazyTableKind {
    /// Registered Delta source table.
    DeltaSource,
    /// SQL-derived table.
    DerivedSql,
}

/// MSSQL output target selected from a lazy table.
#[derive(Clone, PartialEq, Eq)]
pub struct MssqlOutputTarget {
    output_name: String,
    target: MssqlTargetConfig,
    run_mode: RunMode,
}

impl MssqlOutputTarget {
    /// Creates an MSSQL output target request.
    #[must_use]
    pub fn new(
        output_name: impl Into<String>,
        target: MssqlTargetConfig,
        run_mode: RunMode,
    ) -> Self {
        Self {
            output_name: output_name.into(),
            target,
            run_mode,
        }
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the SQL Server target config.
    #[must_use]
    pub const fn target(&self) -> &MssqlTargetConfig {
        &self.target
    }

    /// Returns the requested run mode.
    #[must_use]
    pub const fn run_mode(&self) -> RunMode {
        self.run_mode
    }
}

impl fmt::Debug for MssqlOutputTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlOutputTarget")
            .field("output_name", &sanitize_text_for_display(&self.output_name))
            .field("target", &self.target)
            .field("run_mode", &self.run_mode)
            .finish()
    }
}

/// Planned output write request before schema planning or execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputWritePlan {
    table: LazyTable,
    target: MssqlOutputTarget,
}

impl OutputWritePlan {
    /// Creates an output write request for a lazy table.
    #[must_use]
    pub const fn new(table: LazyTable, target: MssqlOutputTarget) -> Self {
        Self { table, target }
    }

    /// Returns the selected lazy table.
    #[must_use]
    pub const fn table(&self) -> &LazyTable {
        &self.table
    }

    /// Returns the selected MSSQL output target.
    #[must_use]
    pub const fn target(&self) -> &MssqlOutputTarget {
        &self.target
    }
}

/// Registered Delta source tracked by a query-load session.
#[derive(Clone, PartialEq, Eq)]
pub struct RegisteredSessionSource {
    table: LazyTable,
    snapshot_version: u64,
    schema: SchemaRef,
    protocol: DeltaProtocolReport,
}

impl RegisteredSessionSource {
    fn from_registered(table: LazyTable, registered: RegisteredDeltaSource) -> Self {
        Self {
            table,
            snapshot_version: registered.snapshot_version,
            schema: registered.schema,
            protocol: registered.protocol,
        }
    }

    /// Returns the lazy table handle for this registered source.
    #[must_use]
    pub const fn table(&self) -> &LazyTable {
        &self.table
    }

    /// Returns the DataFusion table name for this source.
    #[must_use]
    pub fn name(&self) -> &str {
        self.table.name()
    }

    /// Returns the resolved Delta snapshot version.
    #[must_use]
    pub const fn snapshot_version(&self) -> u64 {
        self.snapshot_version
    }

    /// Returns the logical Arrow schema exposed to DataFusion.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the sanitized protocol report captured before registration.
    #[must_use]
    pub const fn protocol(&self) -> &DeltaProtocolReport {
        &self.protocol
    }
}

impl fmt::Debug for RegisteredSessionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredSessionSource")
            .field("table", &self.table)
            .field("snapshot_version", &self.snapshot_version)
            .field("schema", &self.schema)
            .field("protocol", &self.protocol)
            .finish()
    }
}

/// Rust backing session for lazy query-load workflows.
pub struct DeltaFunnelSession {
    options: SessionOptions,
    context: SessionContext,
    next_table_id: u64,
    sources: Vec<RegisteredSessionSource>,
}

impl DeltaFunnelSession {
    /// Builds a new session with validated local options.
    ///
    /// # Errors
    ///
    /// Returns the first local option validation failure before any source
    /// loading, SQL planning, SQL Server connection, or row execution.
    pub fn new(options: SessionOptions) -> Result<Self, DeltaFunnelError> {
        options.validate()?;
        let context = datafusion_session_context(options.query_options())?;
        Ok(Self {
            options,
            context,
            next_table_id: 0,
            sources: Vec::new(),
        })
    }

    /// Returns the validated session options.
    #[must_use]
    pub const fn options(&self) -> &SessionOptions {
        &self.options
    }

    /// Returns the DataFusion session context owned by this orchestrator.
    ///
    /// The session context is exposed so later planning steps can analyze SQL
    /// against registered session aliases. Delta source registration should
    /// still go through [`DeltaFunnelSession::delta_lake`] so the orchestrator's
    /// source reports stay aligned with the DataFusion catalog.
    #[must_use]
    pub const fn context(&self) -> &SessionContext {
        &self.context
    }

    /// Returns the next unassigned session-local lazy table id.
    #[must_use]
    pub const fn next_table_id(&self) -> u64 {
        self.next_table_id
    }

    /// Registers one Delta source and returns its lazy table handle.
    ///
    /// The method performs source setup only: Delta snapshot metadata loading,
    /// protocol preflight, and DataFusion table registration. It does not scan
    /// data files for row production, parse user SQL, contact SQL Server, or
    /// execute an output action.
    ///
    /// # Errors
    ///
    /// Returns the first Delta source loading, protocol preflight, duplicate
    /// alias, schema conversion, or DataFusion registration error. Session
    /// source state is updated only after the DataFusion registration succeeds.
    pub fn delta_lake(&mut self, source: DeltaSourceConfig) -> Result<LazyTable, DeltaFunnelError> {
        self.reject_registered_source_name(&source.name)?;
        let planned = load_delta_source(source)?;
        let preflight = preflight_delta_protocol(&planned)?;
        let registered = register_delta_sources_with_scan_execution_options(
            &self.context,
            vec![DeltaTableProviderConfig {
                source: planned,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            self.options.provider_scan_options(),
        )?;
        let registered =
            registered
                .sources
                .into_iter()
                .next()
                .ok_or_else(|| DeltaFunnelError::Config {
                    message: "Delta source registration returned no registered source".to_owned(),
                })?;
        let table = self.allocate_delta_source_table(registered.name.clone());
        let session_source = RegisteredSessionSource::from_registered(table.clone(), registered);
        self.sources.push(session_source);
        Ok(table)
    }

    /// Returns registered Delta source reports in registration order.
    #[must_use]
    pub fn sources(&self) -> &[RegisteredSessionSource] {
        &self.sources
    }

    /// Finds a registered Delta source by alias using unquoted SQL semantics.
    #[must_use]
    pub fn registered_source(&self, name: &str) -> Option<&RegisteredSessionSource> {
        self.sources
            .iter()
            .find(|source| source.name().eq_ignore_ascii_case(name))
    }

    fn allocate_delta_source_table(&mut self, name: String) -> LazyTable {
        let id = self.next_table_id;
        self.next_table_id = self.next_table_id.saturating_add(1);
        LazyTable::delta_source(id, name)
    }

    fn reject_registered_source_name(&self, name: &str) -> Result<(), DeltaFunnelError> {
        if self.registered_source(name).is_some() {
            return Err(DeltaFunnelError::DuplicateSourceName {
                name: name.to_owned(),
            });
        }
        Ok(())
    }
}

impl fmt::Debug for DeltaFunnelSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaFunnelSession")
            .field("options", &self.options)
            .field("sources", &self.sources)
            .field("next_table_id", &self.next_table_id)
            .finish_non_exhaustive()
    }
}

fn validate_nonzero_batch_option(
    option: &'static str,
    value: usize,
) -> Result<(), DeltaFunnelError> {
    if value == 0 {
        return Err(DeltaFunnelError::BatchPipeline {
            phase: crate::BatchPipelinePhase::Configuration,
            option,
            message: "must be greater than zero".to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        BatchPipelinePhase, DeltaProviderReaderBackend, DeltaStorageOptions, LoadMode,
        MssqlTargetTable,
    };

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
            Self::new_with_protocol(name, PROTOCOL_JSON)
        }

        fn new_with_protocol(
            name: &str,
            protocol_json: &str,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-funnel-orchestrator-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!(
                    "{}\n{}\n",
                    protocol_json,
                    metadata_json(DEFAULT_SCHEMA_FIELDS_JSON)
                ),
            )?;
            fs::write(
                log_path.join("00000000000000000001.json"),
                format!("{}\n", add_json("part-00000.parquet")),
            )?;

            Ok(Self { path })
        }

        fn uri(&self) -> String {
            self.path.to_string_lossy().to_string()
        }
    }

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const UNSUPPORTED_PROTOCOL_JSON: &str =
        r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":2}}"#;
    const DEFAULT_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

    fn metadata_json(schema_fields_json: &str) -> String {
        format!(
            r#"{{"metaData":{{"id":"delta-funnel-test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":{schema_fields_json}}}","partitionColumns":[],"configuration":{{}},"createdTime":1587968585495}}}}"#
        )
    }

    fn add_json(path: &str) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true}}}}"#
        )
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(format!("{}-{name}-{nanos}", std::process::id()))
    }

    fn secret_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse-primary"))
    }

    #[test]
    fn default_session_constructs_datafusion_context() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;

        assert_eq!(session.options().query_options(), QueryOptions::default());
        assert!(
            session
                .options()
                .validation_options()
                .require_successful_planning()
        );
        assert_eq!(
            session.context().state().config().target_partitions(),
            datafusion::prelude::SessionConfig::new().target_partitions()
        );
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[test]
    fn session_applies_query_options_to_datafusion_context() -> Result<(), DeltaFunnelError> {
        let session =
            DeltaFunnelSession::new(SessionOptions::new().with_query_options(QueryOptions {
                target_partitions: Some(3),
                output_batch_size: Some(11),
            }))?;

        assert_eq!(session.context().state().config().target_partitions(), 3);
        assert_eq!(session.context().state().config().batch_size(), 11);
        Ok(())
    }

    #[test]
    fn query_option_validation_failure_reaches_session_construction() {
        let error =
            DeltaFunnelSession::new(SessionOptions::new().with_query_options(QueryOptions {
                target_partitions: Some(0),
                output_batch_size: None,
            }));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::BatchPipeline {
                phase: BatchPipelinePhase::Configuration,
                option: "target_partitions",
                ..
            })
        ));
    }

    #[test]
    fn provider_option_validation_failure_reaches_session_construction() {
        let error = DeltaFunnelSession::new(SessionOptions::new().with_provider_scan_options(
            DeltaProviderScanExecutionOptions {
                reader_backend: DeltaProviderReaderBackend::OfficialKernel,
                max_concurrent_file_reads_per_scan: 1,
                max_concurrent_file_reads_per_partition: 1,
                output_buffer_capacity_per_partition: 0,
                native_async_prefetch_file_count_per_partition: 0,
            },
        ));

        assert!(matches!(error, Err(DeltaFunnelError::Config { .. })));
    }

    #[test]
    fn batch_option_validation_failure_reaches_session_construction() {
        let error =
            DeltaFunnelSession::new(SessionOptions::new().with_batch_options(BatchOptions {
                max_rows_per_batch: 0,
                max_buffered_batches: 1,
                max_buffered_rows: 1,
            }));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::BatchPipeline {
                phase: BatchPipelinePhase::Configuration,
                option: "max_rows_per_batch",
                ..
            })
        ));
    }

    #[test]
    fn workflow_parallelism_validation_failure_reaches_session_construction() {
        let error = DeltaFunnelSession::new(SessionOptions::new().with_mssql_workflow_options(
            MssqlWorkflowWriteOptions::new().with_max_parallel_outputs(2),
        ));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("parallel MSSQL output writers are not supported")
        ));
    }

    #[test]
    fn workflow_zero_parallelism_validation_failure_reaches_session_construction() {
        let error = DeltaFunnelSession::new(SessionOptions::new().with_mssql_workflow_options(
            MssqlWorkflowWriteOptions::new().with_max_parallel_outputs(0),
        ));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("max_parallel_outputs must be at least 1")
        ));
    }

    #[test]
    fn session_debug_redacts_default_mssql_connection() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;

        let debug = format!("{session:?}");
        assert!(debug.contains("warehouse-primary"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }

    #[test]
    fn output_request_shapes_preserve_table_target_and_run_mode() -> Result<(), DeltaFunnelError> {
        let table = LazyTable::placeholder(7, LazyTableKind::DerivedSql);
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_load_mode(LoadMode::CreateAndLoad)
            .with_connection(secret_connection()?);
        let target = MssqlOutputTarget::new("orders_output", target_config, RunMode::DryRun);
        let plan = OutputWritePlan::new(table.clone(), target.clone());

        assert_eq!(table.id(), 7);
        assert_eq!(table.kind(), LazyTableKind::DerivedSql);
        assert_eq!(target.output_name(), "orders_output");
        assert_eq!(target.run_mode(), RunMode::DryRun);
        assert_eq!(target.target().load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(plan.table(), &table);
        assert_eq!(plan.target(), &target);

        let debug = format!("{target:?}");
        assert!(debug.contains("orders_output"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }

    #[test]
    fn delta_lake_registers_source_and_returns_lazy_table() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let lazy = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        assert_eq!(lazy.id(), 0);
        assert_eq!(lazy.kind(), LazyTableKind::DeltaSource);
        assert_eq!(lazy.name(), "orders");
        assert_eq!(session.next_table_id(), 1);
        assert_eq!(session.sources().len(), 1);
        let registered = session
            .registered_source("ORDERS")
            .ok_or("expected registered source")?;
        assert_eq!(registered.table(), &lazy);
        assert_eq!(registered.name(), "orders");
        assert_eq!(registered.snapshot_version(), 1);
        assert_eq!(registered.protocol().source_name, "orders");
        assert_eq!(registered.schema().fields().len(), 2);

        Ok(())
    }

    #[test]
    fn delta_lake_registers_multiple_distinct_sources() -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("orders")?;
        let customers = DeltaLogTable::new("customers")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let orders = session.delta_lake(DeltaSourceConfig::new("orders", orders.uri()))?;
        let customers = session.delta_lake(DeltaSourceConfig::new("customers", customers.uri()))?;

        assert_eq!(orders.id(), 0);
        assert_eq!(customers.id(), 1);
        assert_eq!(session.sources().len(), 2);
        assert!(session.registered_source("orders").is_some());
        assert!(session.registered_source("customers").is_some());
        Ok(())
    }

    #[test]
    fn duplicate_source_alias_fails_before_loading_second_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session.delta_lake(DeltaSourceConfig::new("ORDERS", ""));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "ORDERS"
        ));
        assert_eq!(session.sources().len(), 1);
        assert_eq!(session.next_table_id(), 1);
        Ok(())
    }

    #[test]
    fn invalid_source_alias_fails_before_registration() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("select", ""));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::InvalidSourceName { name, .. }) if name == "select"
        ));
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[test]
    fn protocol_preflight_failure_does_not_register_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol("unsupported", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("unsupported", table.uri()));

        let display = format!("{}", error.as_ref().err().ok_or("expected error")?);
        assert!(display.contains("unsupported"));
        assert!(display.contains("unsupported Delta minReaderVersion"));
        assert!(matches!(
            error,
            Err(DeltaFunnelError::DeltaProtocolCompatibility { .. })
        ));
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn protocol_preflight_failure_does_not_leak_datafusion_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol("unsupported", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("unsupported", table.uri()));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DeltaProtocolCompatibility { .. })
        ));
        assert!(session.context().table("unsupported").await.is_err());
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn registered_source_sql_analysis_does_not_read_data_files_for_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let dataframe = session
            .context()
            .sql("select id, customer_name from orders")
            .await?;
        let schema = dataframe.schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "customer_name");
        assert_eq!(session.sources().len(), 1);
        Ok(())
    }

    #[test]
    fn source_debug_does_not_expose_storage_option_values() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("storage-options")?;
        let mut storage_options = DeltaStorageOptions::new();
        storage_options.insert(
            "AWS_SECRET_ACCESS_KEY".to_owned(),
            "super-secret".to_owned(),
        );
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        session.delta_lake(
            DeltaSourceConfig::new("orders", table.uri()).with_storage_options(storage_options),
        )?;

        let debug = format!("{session:?}");
        assert!(debug.contains("orders"));
        assert!(!debug.contains("super-secret"));
        assert!(!debug.contains("AWS_SECRET_ACCESS_KEY"));
        Ok(())
    }

    #[test]
    fn source_registration_honors_configured_provider_options()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("configured-provider")?;
        let mut session =
            DeltaFunnelSession::new(SessionOptions::new().with_provider_scan_options(
                DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
                    DeltaProviderReaderBackend::OfficialKernel,
                    2,
                    1,
                )?,
            ))?;

        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        assert_eq!(session.sources().len(), 1);
        assert!(session.registered_source("orders").is_some());
        Ok(())
    }
}
