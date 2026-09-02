use std::fmt;

use delta_arrow_reader::{DeltaScanExecutionOptions, datafusion::IntraFileRepartitioning};

use crate::{
    DeltaFunnelError, MssqlConnectionConfig, MssqlSchemaPlanOptions, MssqlWorkflowWriteOptions,
    MssqlWriteBackend, QueryOptions, ValidationOptions, default_mssql_write_backend,
};

/// Session-wide options for lazy query-load orchestration.
#[derive(Clone)]
pub struct SessionOptions {
    query_options: QueryOptions,
    provider_scan_options: DeltaScanExecutionOptions,
    provider_file_repartitioning: IntraFileRepartitioning,
    provider_use_view_types: bool,
    mssql_schema_options: MssqlSchemaPlanOptions,
    mssql_write_backend: MssqlWriteBackend,
    mssql_workflow_options: MssqlWorkflowWriteOptions,
    validation_options: ValidationOptions,
    default_mssql_connection: Option<MssqlConnectionConfig>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            query_options: QueryOptions::default(),
            provider_scan_options: DeltaScanExecutionOptions::default(),
            provider_file_repartitioning: IntraFileRepartitioning::default(),
            provider_use_view_types: false,
            mssql_schema_options: MssqlSchemaPlanOptions::default(),
            mssql_write_backend: default_mssql_write_backend(),
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
        provider_scan_options: DeltaScanExecutionOptions,
    ) -> Self {
        self.provider_scan_options = provider_scan_options;
        self
    }

    /// Sets when Delta providers may split Parquet files into ranged scan tasks.
    #[must_use]
    pub const fn with_provider_file_repartitioning(
        mut self,
        file_repartitioning: IntraFileRepartitioning,
    ) -> Self {
        self.provider_file_repartitioning = file_repartitioning;
        self
    }

    /// Selects Arrow view arrays for Delta provider string and binary data columns.
    #[must_use]
    pub const fn with_provider_use_view_types(mut self, use_view_types: bool) -> Self {
        self.provider_use_view_types = use_view_types;
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

    /// Sets SQL Server write backend selection.
    #[must_use]
    pub const fn with_mssql_write_backend(
        mut self,
        mssql_write_backend: MssqlWriteBackend,
    ) -> Self {
        self.mssql_write_backend = mssql_write_backend;
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
    pub const fn provider_scan_options(&self) -> DeltaScanExecutionOptions {
        self.provider_scan_options
    }

    /// Returns when Delta providers may split Parquet files into ranged scan tasks.
    #[must_use]
    pub const fn provider_file_repartitioning(&self) -> IntraFileRepartitioning {
        self.provider_file_repartitioning
    }

    /// Returns whether Delta providers use Arrow view arrays for string and binary data columns.
    #[must_use]
    pub const fn provider_use_view_types(&self) -> bool {
        self.provider_use_view_types
    }

    /// Returns SQL Server schema planning options.
    #[must_use]
    pub const fn mssql_schema_options(&self) -> MssqlSchemaPlanOptions {
        self.mssql_schema_options
    }

    /// Returns SQL Server write backend selection.
    #[must_use]
    pub const fn mssql_write_backend(&self) -> MssqlWriteBackend {
        self.mssql_write_backend
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
    /// Returns the first validation error from query, provider, workflow, or
    /// local validation options.
    pub fn validate(&self) -> Result<(), DeltaFunnelError> {
        self.query_options.validate()?;
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
            .field(
                "provider_file_repartitioning",
                &self.provider_file_repartitioning,
            )
            .field("provider_use_view_types", &self.provider_use_view_types)
            .field("mssql_schema_options", &self.mssql_schema_options)
            .field("mssql_write_backend", &self.mssql_write_backend)
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

#[cfg(test)]
mod tests {
    use crate::{
        BatchPipelinePhase, DeltaFunnelError, MssqlConnectionConfig, MssqlWorkflowWriteOptions,
        QueryOptions,
    };
    use delta_arrow_reader::{DeltaScanExecutionOptions, datafusion::IntraFileRepartitioning};

    use super::SessionOptions;
    use crate::orchestrator::DeltaFunnelSession;

    #[test]
    fn default_session_constructs_datafusion_context() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;

        assert_eq!(session.options().query_options(), QueryOptions::default());
        assert_eq!(
            session.options().provider_file_repartitioning(),
            IntraFileRepartitioning::WhenBelowTarget
        );
        assert!(!session.options().provider_use_view_types());
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
    fn session_can_enable_provider_view_types() -> Result<(), DeltaFunnelError> {
        let session =
            DeltaFunnelSession::new(SessionOptions::new().with_provider_use_view_types(true))?;

        assert!(session.options().provider_use_view_types());
        Ok(())
    }

    #[test]
    fn session_can_rebalance_provider_files() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(
            SessionOptions::new()
                .with_provider_file_repartitioning(IntraFileRepartitioning::Always),
        )?;

        assert_eq!(
            session.options().provider_file_repartitioning(),
            IntraFileRepartitioning::Always
        );
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
    fn provider_option_validation_rejects_invalid_values_before_session_construction() {
        let error = DeltaScanExecutionOptions::new().with_output_buffer_batches_per_partition(0);

        assert!(error.is_err());
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
        let connection = MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse-primary");
        let session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(connection),
        )?;

        let debug = format!("{session:?}");
        assert!(debug.contains("warehouse-primary"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }
}
