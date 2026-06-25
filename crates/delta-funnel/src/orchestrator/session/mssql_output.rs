use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlTargetOutputPlan, MssqlWriteOptions,
    MssqlWriteReport, ResolvedMssqlTarget, plan_mssql_target_for_resolved_output,
    write_output_batches_to_mssql,
};

use super::{DeltaFunnelSession, OutputWritePlan, PlannedMssqlOutput, RunMode};

impl DeltaFunnelSession {
    /// Plans one lazy table as an MSSQL output without executing the table.
    ///
    /// The selected table must be owned by this session. The method uses the
    /// table's logical Arrow schema, resolves the effective SQL Server
    /// connection from the output override or session default, and reuses the
    /// SQL Server schema, DDL, and lifecycle planning rules. It intentionally
    /// performs no SQL Server I/O, physical DataFusion planning, row reads,
    /// batch streaming, or writer construction.
    ///
    /// # Errors
    ///
    /// Returns an MSSQL planning error when the selected table is not known to
    /// this session, the target has no effective connection, the output or
    /// target config is invalid, the schema cannot be mapped to SQL Server, or
    /// the requested load mode is not supported by the current target planner.
    pub fn plan_mssql_output(
        &self,
        request: &OutputWritePlan,
    ) -> Result<PlannedMssqlOutput, DeltaFunnelError> {
        let schema = self.schema_for_lazy_table(request.table())?;
        let resolved_target =
            request
                .target()
                .target()
                .resolve(crate::MssqlTargetResolutionContext {
                    output_name: Some(request.target().output_name()),
                    default_connection: self.options.default_mssql_connection(),
                })?;
        let output_plan = plan_mssql_target_for_resolved_output(
            schema.as_ref(),
            &resolved_target,
            self.options.mssql_schema_options(),
        )?;

        Ok(PlannedMssqlOutput::new(
            request.clone(),
            resolved_target,
            output_plan,
        ))
    }

    /// Writes one selected lazy table to SQL Server.
    ///
    /// The method reuses the session output planner, builds a DataFusion physical
    /// plan for the selected lazy table, exposes DataFusion's merged
    /// `RecordBatch` stream, and hands that stream directly to the existing
    /// one-output MSSQL sink. It does not implement SQL Server lifecycle,
    /// writer, cleanup, retry, or stream buffering behavior itself.
    ///
    /// # Errors
    ///
    /// Returns the first planning, DataFusion stream setup, upstream stream, SQL
    /// Server connection, lifecycle, schema validation, write, or cleanup error.
    pub async fn write_to_mssql(
        &self,
        request: &OutputWritePlan,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        self.write_to_mssql_with_writer(request, &mut MssqlPublicOneOutputWriter)
            .await
    }

    pub(crate) async fn write_to_mssql_with_writer<W>(
        &self,
        request: &OutputWritePlan,
        writer: &mut W,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        W: OrchestratorMssqlOutputWriter,
    {
        ensure_execute_run_mode(request.target().run_mode())?;
        let planned = self.plan_mssql_output(request)?;
        let output_schema = Arc::clone(self.schema_for_lazy_table(planned.table())?);
        let batches = self.batch_stream_for_lazy_table(planned.table()).await?;

        writer
            .write_output(
                output_schema,
                planned.output_plan().clone(),
                planned.resolved_target().clone(),
                batches,
                self.options.mssql_write_options(),
            )
            .await
    }
}

#[async_trait]
pub(crate) trait OrchestratorMssqlOutputWriter: Send {
    async fn write_output(
        &mut self,
        output_schema: SchemaRef,
        output_plan: MssqlTargetOutputPlan,
        resolved_target: ResolvedMssqlTarget,
        batches: MssqlOutputBatchStream,
        write_options: MssqlWriteOptions,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>;
}

struct MssqlPublicOneOutputWriter;

#[async_trait]
impl OrchestratorMssqlOutputWriter for MssqlPublicOneOutputWriter {
    async fn write_output(
        &mut self,
        output_schema: SchemaRef,
        output_plan: MssqlTargetOutputPlan,
        resolved_target: ResolvedMssqlTarget,
        batches: MssqlOutputBatchStream,
        write_options: MssqlWriteOptions,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        write_output_batches_to_mssql(
            output_schema.as_ref(),
            resolved_target,
            output_plan.schema_plan_options(),
            batches,
            write_options,
        )
        .await
    }
}

fn ensure_execute_run_mode(run_mode: RunMode) -> Result<(), DeltaFunnelError> {
    match run_mode {
        RunMode::Execute => Ok(()),
        RunMode::DryRun => Err(DeltaFunnelError::MssqlWorkflowPlanning {
            message:
                "write_to_mssql requires RunMode::Execute; use dry_run_to_mssql for dry-run planning"
                    .to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DeltaFunnelError, DeltaSourceConfig, LoadMode, MssqlConnectionSource, MssqlOutputTarget,
        MssqlTargetConfig, MssqlTargetTable,
    };

    use super::super::{
        DeltaFunnelSession, LazyTable, LazyTableKind, OutputWritePlan, RunMode, SessionOptions,
        test_support::{
            DeltaLogTable, UNSUPPORTED_SCHEMA_FIELDS_JSON, execute_output_request, output_request,
            override_connection, secret_connection,
        },
    };

    #[test]
    fn plan_mssql_output_uses_source_schema_and_session_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source.clone(),
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(planned.request(), &request);
        assert_eq!(planned.table(), &source);
        assert_eq!(planned.target().run_mode(), RunMode::DryRun);
        assert_eq!(planned.output_plan().output_name(), "orders_output");
        assert_eq!(planned.output_plan().target_table().schema(), Some("dbo"));
        assert_eq!(planned.output_plan().target_table().table(), "orders_sink");
        assert_eq!(
            planned.output_plan().connection_source(),
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(
            planned.output_plan().connection().display_label(),
            Some("warehouse-primary")
        );
        assert_eq!(planned.output_plan().schema_mappings().len(), 2);
        assert_eq!(
            planned.output_plan().schema_mappings()[0].arrow().name(),
            "id"
        );
        assert_eq!(
            planned.output_plan().schema_mappings()[1].arrow().name(),
            "customer_name"
        );
        assert_eq!(planned.output_plan().create_table_sql(), None);
        Ok(())
    }

    #[tokio::test]
    async fn plan_mssql_output_uses_pending_derived_schema_without_row_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session.table_from_sql("select id from orders").await?;
        let request = output_request(
            derived.clone(),
            "derived_orders_output",
            "derived_orders",
            LoadMode::CreateAndLoad,
        )?;

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(planned.table(), &derived);
        assert_eq!(planned.output_plan().load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(planned.output_plan().schema_mappings().len(), 1);
        assert_eq!(
            planned.output_plan().schema_mappings()[0].arrow().name(),
            "id"
        );
        let create_table_sql = planned
            .output_plan()
            .create_table_sql()
            .ok_or("expected create table SQL")?;
        assert!(create_table_sql.contains("[dbo].[derived_orders]"));
        Ok(())
    }

    #[tokio::test]
    async fn plan_mssql_output_accepts_registered_derived_alias_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let alias = session.register_alias("customer_names", &derived)?;
        let request = output_request(
            alias.clone(),
            "customer_names_output",
            "customer_names_sink",
            LoadMode::AppendExisting,
        )?;

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(planned.table(), &alias);
        assert_eq!(planned.output_plan().schema_mappings().len(), 1);
        assert_eq!(
            planned.output_plan().schema_mappings()[0].arrow().name(),
            "customer_name"
        );
        Ok(())
    }

    #[test]
    fn plan_mssql_output_connection_override_wins_without_mutating_session_default()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders_sink")?)
            .with_connection(override_connection()?);
        let request = OutputWritePlan::new(
            source,
            MssqlOutputTarget::new("orders_output", target_config, RunMode::DryRun),
        );

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(
            planned.output_plan().connection_source(),
            MssqlConnectionSource::TargetOverride
        );
        assert_eq!(
            planned.output_plan().connection().display_label(),
            Some("warehouse-override")
        );
        assert_eq!(
            session
                .options()
                .default_mssql_connection()
                .ok_or("expected default connection")?
                .summary()
                .display_label(),
            Some("warehouse-primary")
        );
        Ok(())
    }

    #[test]
    fn plan_mssql_output_missing_effective_connection_fails_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = execute_output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_rejects_replace_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(source, "orders_output", "orders_sink", LoadMode::Replace)?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlLifecyclePlanning { output_name, message })
                if output_name == "orders_output" && message.contains("replace load mode")
        ));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_rejects_unknown_lazy_table_before_target_planning()
    -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let request = output_request(
            LazyTable::placeholder(42, LazyTableKind::DeltaSource),
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("not registered in this session")
        ));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_rejects_invalid_output_name() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(source, "  ", "orders_sink", LoadMode::AppendExisting)?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::InvalidMssqlOutputIdentity { output_name, .. })
                if output_name == "  "
        ));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_rejects_invalid_target_identifier_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders\narchive",
            LoadMode::CreateAndLoad,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            &error,
            Err(DeltaFunnelError::MssqlDdlTargetIdentifier { output_name, .. })
                if output_name == "orders_output"
        ));
        let display = error.err().ok_or("expected error")?.to_string();
        assert!(!display.contains('\n'));
        assert!(display.contains("control characters"));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_reports_unsupported_source_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let table =
            DeltaLogTable::new_with_schema("unsupported-schema", UNSUPPORTED_SCHEMA_FIELDS_JSON)?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source =
            session.delta_lake(DeltaSourceConfig::new("unsupported_schema", table.uri()))?;
        let request = output_request(
            source,
            "unsupported_output",
            "unsupported_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlSchemaPlanning {
                output_name,
                diagnostics,
            }) if output_name == "unsupported_output" && !diagnostics.is_empty()
        ));
        Ok(())
    }

    #[test]
    fn planned_mssql_output_debug_redacts_connection_material()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders_sink")?)
            .with_connection(override_connection()?);
        let request = OutputWritePlan::new(
            source,
            MssqlOutputTarget::new("orders_output", target_config, RunMode::DryRun),
        );

        let planned = session.plan_mssql_output(&request)?;
        let debug = format!("{planned:?}");

        assert!(debug.contains("orders_output"));
        assert!(debug.contains("warehouse-override"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("override-secret"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }
}
