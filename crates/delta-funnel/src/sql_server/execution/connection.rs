//! SQL Server connection request planning through arrow-tiberius.

use std::fmt;

use arrow_schema::Schema;

use crate::{DeltaFunnelError, PhaseTimingReport, report::PhaseTimer};

use super::{
    LoadMode, MssqlConnectedLifecycleClient, MssqlConnectionConfig, MssqlSchemaPlanOptions,
    MssqlTargetCleanupStatus, MssqlTargetOutputPlan, MssqlWriteFailureContext, MssqlWriteOptions,
    MssqlWritePhase, ResolvedMssqlTarget, plan_mssql_output_schema, plan_mssql_target_output,
};
use super::{MssqlPreparedTarget, write::initialize_mssql_bulk_writer};

const SQL_SERVER_CONNECTION_PHASE: &str = "sql_server_connection";

/// Private execution request for connecting one planned SQL Server output.
pub(crate) struct MssqlOutputConnectionRequest {
    output_plan: MssqlTargetOutputPlan,
    connection: MssqlConnectionConfig,
}

impl MssqlOutputConnectionRequest {
    /// Returns the redacted target output plan.
    #[must_use]
    pub(crate) const fn output_plan(&self) -> &MssqlTargetOutputPlan {
        &self.output_plan
    }

    fn cleanup_before_target_creation(&self) -> MssqlTargetCleanupStatus {
        match self.output_plan.load_mode() {
            LoadMode::AppendExisting => MssqlTargetCleanupStatus::NotApplicable,
            LoadMode::CreateAndLoad => MssqlTargetCleanupStatus::NotAttempted,
            LoadMode::Replace => MssqlTargetCleanupStatus::NotAttempted,
        }
    }
}

impl fmt::Debug for MssqlOutputConnectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlOutputConnectionRequest")
            .field("output_plan", &self.output_plan)
            .field("connection", &self.connection.summary())
            .finish()
    }
}

/// Connected SQL Server client for one planned output.
#[derive(Debug)]
pub(crate) struct MssqlConnectedOutputClient {
    output_plan: MssqlTargetOutputPlan,
    client: arrow_tiberius::ConnectedMssqlClient,
    phase_timings: Vec<PhaseTimingReport>,
}

impl MssqlConnectedOutputClient {
    /// Returns the redacted target output plan paired with this connection.
    #[must_use]
    pub(crate) const fn output_plan(&self) -> &MssqlTargetOutputPlan {
        &self.output_plan
    }

    /// Returns the connected arrow-tiberius client.
    #[must_use]
    pub(crate) fn client(&mut self) -> &mut arrow_tiberius::ConnectedMssqlClient {
        &mut self.client
    }

    /// Returns phase timings recorded while creating this connection.
    #[must_use]
    pub(crate) fn phase_timings(&self) -> &[PhaseTimingReport] {
        &self.phase_timings
    }

    /// Returns the lifecycle operation adapter for this output connection.
    pub(crate) fn lifecycle_client(&mut self) -> MssqlConnectedLifecycleClient<'_> {
        MssqlConnectedLifecycleClient::new(&self.output_plan, &mut self.client)
    }

    /// Initializes the bulk writer after target lifecycle preparation.
    pub(crate) async fn initialize_bulk_writer(
        &mut self,
        prepared_target: &MssqlPreparedTarget,
        options: MssqlWriteOptions,
    ) -> Result<arrow_tiberius::ConnectedBulkWriter<'_>, DeltaFunnelError> {
        initialize_mssql_bulk_writer(
            &mut self.client,
            &self.output_plan,
            prepared_target,
            options,
        )
        .await
    }
}

/// Plans the private connection request for one resolved SQL Server output.
pub(crate) fn plan_mssql_output_connection_request(
    output_schema: impl AsRef<Schema>,
    resolved_target: ResolvedMssqlTarget,
    options: MssqlSchemaPlanOptions,
) -> Result<MssqlOutputConnectionRequest, DeltaFunnelError> {
    let connection = resolved_target.connection().clone();
    let schema_plan = plan_mssql_output_schema(output_schema, &resolved_target, options)?;
    let output_plan = plan_mssql_target_output(schema_plan)?;

    Ok(MssqlOutputConnectionRequest {
        output_plan,
        connection,
    })
}

/// Connects one planned SQL Server output through arrow-tiberius.
pub(crate) async fn connect_mssql_output_client(
    request: MssqlOutputConnectionRequest,
) -> Result<MssqlConnectedOutputClient, DeltaFunnelError> {
    let cleanup = request.cleanup_before_target_creation();
    let connect_timer = PhaseTimer::start(SQL_SERVER_CONNECTION_PHASE);
    let connect_result = arrow_tiberius::connect_mssql_client_from_ado_string(
        request.connection.connection_string(),
    )
    .await;
    let client = match connect_result {
        Ok(client) => client,
        Err(source) => {
            return Err(connect_error(
                &request,
                cleanup,
                connect_timer.failed(),
                source,
            ));
        }
    };

    Ok(MssqlConnectedOutputClient {
        output_plan: request.output_plan,
        client,
        phase_timings: vec![connect_timer.completed()],
    })
}

fn connect_error(
    request: &MssqlOutputConnectionRequest,
    cleanup: MssqlTargetCleanupStatus,
    timing: PhaseTimingReport,
    source: arrow_tiberius::Error,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWritePhase {
        context: Box::new(
            MssqlWriteFailureContext::from_output_plan(
                request.output_plan(),
                MssqlWritePhase::Connect,
                0,
                0,
                0,
                false,
                cleanup,
            )
            .with_phase_timings(vec![timing]),
        ),
        message: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use arrow_tiberius::PlanOptions;

    use super::*;
    use crate::{
        MssqlConnectionConfig, MssqlConnectionSource, MssqlTargetConfig,
        MssqlTargetResolutionContext, MssqlTargetTable, MssqlWriteReport, PhaseStatus,
    };

    fn secret_connection(
        label: &str,
        secret: &str,
    ) -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(format!(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password={secret}"
        ))?
        .with_display_label(label))
    }

    fn orders_schema() -> Schema {
        Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
        ])
    }

    fn resolved_target(
        output_name: &str,
        load_mode: LoadMode,
        connection: &MssqlConnectionConfig,
    ) -> Result<ResolvedMssqlTarget, DeltaFunnelError> {
        MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_load_mode(load_mode)
            .resolve(MssqlTargetResolutionContext {
                output_name: Some(output_name),
                default_connection: Some(connection),
            })
    }

    fn assert_connect_phase_error(
        error: DeltaFunnelError,
        expected_cleanup: MssqlTargetCleanupStatus,
    ) -> Result<String, DeltaFunnelError> {
        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MssqlWritePhase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::Connect);
        assert_eq!(context.stats().rows_written(), 0);
        assert_eq!(context.stats().batches_written(), 0);
        assert!(!context.partial_write_possible());
        assert_eq!(context.cleanup(), expected_cleanup);
        assert_eq!(
            context.connection().display_label(),
            Some("warehouse-primary")
        );
        let timing = context
            .phase_timings()
            .iter()
            .find(|timing| timing.phase_name() == SQL_SERVER_CONNECTION_PHASE)
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "missing SQL Server connection phase timing".to_owned(),
            })?;
        assert_eq!(timing.status(), PhaseStatus::failed());
        assert!(timing.elapsed_micros().is_some());
        Ok(message)
    }

    #[test]
    fn compatible_arrow_tiberius_connection_api_is_available() {
        let _connect = arrow_tiberius::connect_mssql_client_from_ado_string;
        let client_type = std::any::type_name::<arrow_tiberius::ConnectedMssqlClient>();

        assert!(client_type.contains("ConnectedMssqlClient"));
    }

    #[test]
    fn mssql_connection_uses_arrow_tiberius_without_direct_transport_dependencies() {
        let manifest = include_str!("../../../Cargo.toml");
        let dependencies = direct_manifest_dependency_names(manifest);

        assert!(dependencies.contains(&"arrow-tiberius"));
        assert_eq!(
            direct_manifest_dependency_version(manifest, "arrow-tiberius"),
            Some("0.1.4")
        );
        assert!(!dependencies.contains(&"tiberius"));
        assert!(!dependencies.contains(&"tiberius-raw-bulk"));
        assert!(!dependencies.contains(&"tokio-util"));
    }

    #[test]
    fn dependency_alignment_doc_tracks_arrow_tiberius_observability_release() {
        let docs = include_str!("../../../../../docs/dependency-alignment.md");

        assert!(docs.contains("arrow-tiberius = \"0.1.4\""));
        assert!(docs.contains("tiberius-raw-bulk =0.12.3-raw-bulk.14"));
        assert!(docs.contains("arrow_tiberius"));
        assert!(docs.contains("tiberius_raw_bulk::protocol"));
        assert!(!docs.contains("arrow-tiberius = \"0.1.3\""));
        assert!(!docs.contains("arrow-tiberius = \"0.1.1\""));
        assert!(!docs.contains("arrow-tiberius = \"0.1.2\""));
        assert!(!docs.contains("raw-bulk.13"));
        assert!(!docs.contains("pre-publish"));
    }

    #[test]
    fn connection_module_stays_before_lifecycle_sql_and_row_writing_boundaries() {
        let source = include_str!("connection.rs");
        let forbidden_patterns = [
            concat!("execute", "_statement"),
            concat!("table", "_exists"),
            concat!("Record", "Batch"),
            concat!("data", "fusion"),
            concat!("write", "_batch"),
        ];

        for pattern in forbidden_patterns {
            assert!(!source.contains(pattern), "unexpected `{pattern}`");
        }
    }

    #[test]
    fn connected_output_client_exposes_writer_initialization_boundary() {
        let source = include_str!("connection.rs");

        assert!(source.contains("initialize_bulk_writer"));
        assert!(source.contains("initialize_mssql_bulk_writer"));
        assert!(source.contains("MssqlPreparedTarget"));
    }

    #[test]
    fn connection_request_pairs_raw_connection_with_redacted_output_plan()
    -> Result<(), DeltaFunnelError> {
        let connection = secret_connection("warehouse-primary", "secret-token")?;
        let request = plan_mssql_output_connection_request(
            orders_schema(),
            resolved_target("orders", LoadMode::AppendExisting, &connection)?,
            PlanOptions::default(),
        )?;

        assert_eq!(request.output_plan().output_name(), "orders");
        assert_eq!(
            request.output_plan().connection_source(),
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(
            request.connection.connection_string(),
            connection.connection_string()
        );
        assert_eq!(
            request.cleanup_before_target_creation(),
            MssqlTargetCleanupStatus::NotApplicable
        );
        Ok(())
    }

    #[test]
    fn matching_redacted_summaries_do_not_drive_request_pairing() -> Result<(), DeltaFunnelError> {
        let west_connection = secret_connection("warehouse-primary", "west-secret")?;
        let east_connection = secret_connection("warehouse-primary", "east-secret")?;
        let west_request = plan_mssql_output_connection_request(
            orders_schema(),
            resolved_target("west", LoadMode::AppendExisting, &west_connection)?,
            PlanOptions::default(),
        )?;
        let east_request = plan_mssql_output_connection_request(
            orders_schema(),
            resolved_target("east", LoadMode::AppendExisting, &east_connection)?,
            PlanOptions::default(),
        )?;

        assert_eq!(
            west_request.output_plan().connection(),
            east_request.output_plan().connection()
        );
        assert_eq!(
            west_request.connection.connection_string(),
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=west-secret"
        );
        assert_eq!(
            east_request.connection.connection_string(),
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=east-secret"
        );
        assert_eq!(west_request.output_plan().output_name(), "west");
        assert_eq!(east_request.output_plan().output_name(), "east");
        Ok(())
    }

    #[test]
    fn create_and_load_connection_request_reports_cleanup_not_attempted()
    -> Result<(), DeltaFunnelError> {
        let connection = secret_connection("warehouse-primary", "secret-token")?;
        let request = plan_mssql_output_connection_request(
            orders_schema(),
            resolved_target("orders", LoadMode::CreateAndLoad, &connection)?,
            PlanOptions::default(),
        )?;

        assert_eq!(
            request.cleanup_before_target_creation(),
            MssqlTargetCleanupStatus::NotAttempted
        );
        Ok(())
    }

    #[test]
    fn redacted_connection_shapes_do_not_leak_raw_connection_string() -> Result<(), DeltaFunnelError>
    {
        let connection = secret_connection("warehouse-primary", "secret-token")?;
        let request = plan_mssql_output_connection_request(
            orders_schema(),
            resolved_target("orders", LoadMode::AppendExisting, &connection)?,
            PlanOptions::default(),
        )?;
        let report = MssqlWriteReport::from_output_plan(
            request.output_plan(),
            0,
            0,
            0,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
        );
        let context = MssqlWriteFailureContext::from_output_plan(
            request.output_plan(),
            MssqlWritePhase::Connect,
            0,
            0,
            0,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
        );

        for debug in [
            format!("{request:?}"),
            format!("{:?}", request.output_plan()),
            format!("{:?}", request.output_plan().target()),
            format!("{report:?}"),
            format!("{context:?}"),
        ] {
            assert!(!debug.contains("secret-token"));
            assert!(!debug.contains("server=tcp:sql.example.com"));
            assert!(debug.contains("warehouse-primary"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn invalid_connection_string_is_mapped_to_connect_phase() -> Result<(), DeltaFunnelError>
    {
        let connection = MssqlConnectionConfig::new("not an ado string")?
            .with_display_label("warehouse-primary");
        let request = plan_mssql_output_connection_request(
            orders_schema(),
            resolved_target("orders", LoadMode::AppendExisting, &connection)?,
            PlanOptions::default(),
        )?;

        let error = connect_mssql_output_client(request)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected invalid connection string to fail".to_owned(),
            })?;

        let message = assert_connect_phase_error(error, MssqlTargetCleanupStatus::NotApplicable)?;
        assert!(!message.contains("not an ado string"));
        Ok(())
    }

    #[test]
    fn tcp_connect_failure_is_mapped_to_connect_phase() -> Result<(), DeltaFunnelError> {
        let connection = secret_connection("warehouse-primary", "secret-token")?;
        let request = plan_mssql_output_connection_request(
            orders_schema(),
            resolved_target("orders", LoadMode::CreateAndLoad, &connection)?,
            PlanOptions::default(),
        )?;
        let error = connect_error(
            &request,
            request.cleanup_before_target_creation(),
            PhaseTimingReport::failed(SQL_SERVER_CONNECTION_PHASE, std::time::Duration::ZERO),
            arrow_tiberius::Error::ConnectionTcpConnect {
                source: std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "connection refused",
                ),
            },
        );

        let message = assert_connect_phase_error(error, MssqlTargetCleanupStatus::NotAttempted)?;
        assert!(message.contains("TCP connection to SQL Server failed"));
        assert!(!message.contains("secret-token"));
        Ok(())
    }

    fn direct_manifest_dependency_names(manifest: &str) -> Vec<&str> {
        let mut dependency_names = Vec::new();
        let mut in_dependency_section = false;

        for line in manifest.lines() {
            let line = line.trim();
            if line.starts_with('[') && line.ends_with(']') {
                in_dependency_section = matches!(
                    line,
                    "[dependencies]" | "[dev-dependencies]" | "[build-dependencies]"
                );
                continue;
            }
            if !in_dependency_section || line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((dependency_name, _value)) = line.split_once('=') else {
                continue;
            };
            dependency_names.push(dependency_name.trim().trim_matches('"'));
        }

        dependency_names
    }

    fn direct_manifest_dependency_version<'a>(
        manifest: &'a str,
        expected_dependency: &str,
    ) -> Option<&'a str> {
        let mut in_dependency_section = false;

        for line in manifest.lines() {
            let line = line.trim();
            if line.starts_with('[') && line.ends_with(']') {
                in_dependency_section = line == "[dependencies]";
                continue;
            }
            if !in_dependency_section || line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((dependency_name, value)) = line.split_once('=') else {
                continue;
            };
            if dependency_name.trim().trim_matches('"') != expected_dependency {
                continue;
            }
            return Some(value.trim().trim_matches('"'));
        }

        None
    }
}
