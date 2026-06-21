//! Rust backing model for lazy query-load orchestration.
//!
//! This module owns the high-level session and request data shapes that will
//! later back the Python `Session` and `Table` API. It intentionally does not
//! load Delta sources, parse SQL, contact SQL Server, or execute rows.

use std::fmt;

use datafusion::prelude::SessionContext;

use crate::{
    DeltaFunnelError, DeltaProviderScanExecutionOptions, MssqlConnectionConfig,
    MssqlSchemaPlanOptions, MssqlTargetConfig, MssqlWorkflowWriteOptions, MssqlWriteOptions,
    QueryOptions, datafusion_session_context, default_mssql_write_options,
    redaction::sanitize_text_for_display,
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
}

impl LazyTable {
    /// Creates a placeholder lazy table handle for future registration slices.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn placeholder(id: u64, kind: LazyTableKind) -> Self {
        Self {
            id: LazyTableId(id),
            kind,
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

/// Rust backing session for lazy query-load workflows.
pub struct DeltaFunnelSession {
    options: SessionOptions,
    context: SessionContext,
    next_table_id: u64,
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
        })
    }

    /// Returns the validated session options.
    #[must_use]
    pub const fn options(&self) -> &SessionOptions {
        &self.options
    }

    /// Returns the owned DataFusion session context.
    #[must_use]
    pub const fn context(&self) -> &SessionContext {
        &self.context
    }

    /// Returns the next unassigned session-local lazy table id.
    #[must_use]
    pub const fn next_table_id(&self) -> u64 {
        self.next_table_id
    }
}

impl fmt::Debug for DeltaFunnelSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaFunnelSession")
            .field("options", &self.options)
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
    use super::*;
    use crate::{BatchPipelinePhase, DeltaProviderReaderBackend, LoadMode, MssqlTargetTable};

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
}
