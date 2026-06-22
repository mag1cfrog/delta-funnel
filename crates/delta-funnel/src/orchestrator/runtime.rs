//! Explicit runtime boundary for blocking host integrations.
//!
//! The core session API remains async-friendly. This wrapper owns the Tokio
//! runtime needed by synchronous hosts such as a future PyO3 package, while
//! provider, handoff, and sink modules stay async/pull-driven and do not own a
//! hidden process-level runtime.

use tokio::runtime::{Builder, Runtime};

use crate::{
    DeltaFunnelError, DeltaFunnelSession, LazyTable, MssqlDryRunOutputReport,
    MssqlDryRunWorkflowReport, MssqlWriteReport, OutputWritePlan, WriteAllOptions, WriteAllReport,
};

/// Blocking runtime boundary for high-level DeltaFunnel session actions.
///
/// This type is intended to be owned by synchronous host bindings. Constructing
/// it only creates a Tokio runtime; it does not register sources, plan SQL,
/// execute DataFusion, contact SQL Server, or write rows.
///
/// The blocking methods are intended for non-async host threads. Rust async
/// callers should use [`DeltaFunnelSession`] async methods directly.
pub struct DeltaFunnelRuntime {
    runtime: Runtime,
}

impl DeltaFunnelRuntime {
    /// Creates a multi-threaded Tokio runtime for blocking host integrations.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if Tokio cannot create the runtime.
    pub fn new() -> Result<Self, DeltaFunnelError> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .thread_name("delta-funnel-runtime")
            .build()
            .map_err(|error| DeltaFunnelError::Config {
                message: format!("failed to create DeltaFunnel runtime: {error}"),
            })?;

        Ok(Self { runtime })
    }

    /// Runs async SQL table planning for a synchronous host.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::table_from_sql`].
    pub fn table_from_sql(
        &self,
        session: &mut DeltaFunnelSession,
        sql: &str,
    ) -> Result<LazyTable, DeltaFunnelError> {
        self.runtime.block_on(session.table_from_sql(sql))
    }

    /// Runs a single-output dry run through the high-level session API.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::dry_run_to_mssql`].
    pub fn dry_run_to_mssql(
        &self,
        session: &DeltaFunnelSession,
        request: &OutputWritePlan,
    ) -> Result<MssqlDryRunOutputReport, DeltaFunnelError> {
        session.dry_run_to_mssql(request)
    }

    /// Runs a multi-output dry run through the high-level session API.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::dry_run_all_to_mssql`].
    pub fn dry_run_all_to_mssql(
        &self,
        session: &DeltaFunnelSession,
        requests: &[OutputWritePlan],
    ) -> Result<MssqlDryRunWorkflowReport, DeltaFunnelError> {
        session.dry_run_all_to_mssql(requests)
    }

    /// Blocks on one selected output write.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::write_to_mssql`].
    pub fn write_to_mssql(
        &self,
        session: &DeltaFunnelSession,
        request: &OutputWritePlan,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        self.runtime.block_on(session.write_to_mssql(request))
    }

    /// Blocks on the default multi-output write workflow.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::write_all`].
    pub fn write_all(
        &self,
        session: &DeltaFunnelSession,
        requests: &[OutputWritePlan],
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        self.runtime.block_on(session.write_all(requests))
    }

    /// Blocks on the multi-output write workflow with explicit options.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::write_all_with_options`].
    pub fn write_all_with_options(
        &self,
        session: &DeltaFunnelSession,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        self.runtime
            .block_on(session.write_all_with_options(requests, options))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        LoadMode, MssqlConnectionConfig, MssqlOutputTarget, MssqlTargetConfig, MssqlTargetTable,
        RunMode, SessionOptions,
    };

    fn secret_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse-primary"))
    }

    fn output_request(
        table: LazyTable,
        output_name: &str,
        target_table: &str,
    ) -> Result<OutputWritePlan, DeltaFunnelError> {
        let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", target_table)?)
            .with_load_mode(LoadMode::AppendExisting);

        Ok(OutputWritePlan::new(
            table,
            MssqlOutputTarget::new(output_name, target, RunMode::DryRun),
        ))
    }

    #[test]
    fn runtime_constructs_without_starting_session_work() -> Result<(), DeltaFunnelError> {
        let _runtime = DeltaFunnelRuntime::new()?;

        Ok(())
    }

    #[test]
    fn runtime_drives_table_sql_and_single_output_dry_run() -> Result<(), DeltaFunnelError> {
        let runtime = DeltaFunnelRuntime::new()?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let output = runtime.table_from_sql(&mut session, "select 1 as id")?;
        let request = output_request(output, "orders_output", "orders_sink")?;

        let report = runtime.dry_run_to_mssql(&session, &request)?;

        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.run_mode(), RunMode::DryRun);
        assert!(!report.sql_server_contacted());
        assert!(!report.row_production_started());
        Ok(())
    }

    #[test]
    fn runtime_drives_multi_output_dry_run() -> Result<(), DeltaFunnelError> {
        let runtime = DeltaFunnelRuntime::new()?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = runtime.table_from_sql(&mut session, "select 1 as id")?;
        let east = runtime.table_from_sql(&mut session, "select 2 as id")?;
        let west = output_request(west, "west_output", "west_orders")?;
        let east = output_request(east, "east_output", "east_orders")?;

        let report = runtime.dry_run_all_to_mssql(&session, &[west, east])?;

        assert_eq!(report.len(), 2);
        assert_eq!(report.outputs()[0].output_name(), "west_output");
        assert_eq!(report.outputs()[1].output_name(), "east_output");
        assert!(!report.sql_server_contacted());
        assert!(!report.row_production_started());
        Ok(())
    }

    #[test]
    fn runtime_preserves_sanitized_session_errors() -> Result<(), DeltaFunnelError> {
        let runtime = DeltaFunnelRuntime::new()?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let output = runtime.table_from_sql(&mut session, "select 1 as id")?;
        let request = output_request(output, "orders_output", "orders_sink")?;

        let error = runtime.dry_run_to_mssql(&session, &request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        Ok(())
    }

    #[test]
    fn lower_level_modules_do_not_create_hidden_tokio_runtimes() {
        let low_level_sources = [
            include_str!("../pipeline/batch_handoff.rs"),
            include_str!("../query_engine/datafusion/execution.rs"),
            include_str!("../query_engine/datafusion/execution/planning_exec.rs"),
            include_str!("../sql_server/execution/connection.rs"),
            include_str!("../sql_server/execution/sink.rs"),
            include_str!("../sql_server/execution/workflow.rs"),
            include_str!("../sql_server/execution/write.rs"),
        ];

        for source in low_level_sources {
            assert!(!source.contains("tokio::runtime"));
            assert!(!source.contains("Runtime::new"));
            assert!(!source.contains("block_on"));
        }
    }
}
