//! SQL Server connection request planning through arrow-tiberius.

use std::fmt;

use arrow_schema::Schema;

use crate::DeltaFunnelError;

use super::{
    LoadMode, MssqlConnectionConfig, MssqlSchemaPlanOptions, MssqlTargetCleanupStatus,
    MssqlTargetOutputPlan, MssqlWriteFailureContext, MssqlWritePhase, ResolvedMssqlTarget,
    plan_mssql_output_schema, plan_mssql_target_output,
};

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
    let client = arrow_tiberius::connect_mssql_client_from_ado_string(
        request.connection.connection_string(),
    )
    .await
    .map_err(|source| connect_error(&request, cleanup, source))?;

    Ok(MssqlConnectedOutputClient {
        output_plan: request.output_plan,
        client,
    })
}

fn connect_error(
    request: &MssqlOutputConnectionRequest,
    cleanup: MssqlTargetCleanupStatus,
    source: arrow_tiberius::Error,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWritePhase {
        context: Box::new(MssqlWriteFailureContext::from_output_plan(
            request.output_plan(),
            MssqlWritePhase::Connect,
            0,
            0,
            0,
            false,
            cleanup,
        )),
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
        MssqlTargetResolutionContext, MssqlTargetTable, MssqlWriteReport,
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
        Ok(message)
    }

    #[test]
    fn compatible_arrow_tiberius_connection_api_is_available() {
        let _connect = arrow_tiberius::connect_mssql_client_from_ado_string;
        let client_type = std::any::type_name::<arrow_tiberius::ConnectedMssqlClient>();

        assert!(client_type.contains("ConnectedMssqlClient"));
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
}
