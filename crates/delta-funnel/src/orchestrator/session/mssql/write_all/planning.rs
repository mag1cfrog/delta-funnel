use std::collections::BTreeSet;

use crate::{DeltaFunnelError, support::sanitize_text_for_display};

use super::super::super::{DeltaFunnelSession, OutputWritePlan, PlannedMssqlOutput, RunMode};

impl DeltaFunnelSession {
    #[allow(dead_code)]
    pub(crate) fn plan_write_all_outputs(
        &self,
        requests: &[OutputWritePlan],
    ) -> Result<Vec<PlannedMssqlOutput>, DeltaFunnelError> {
        ensure_unique_write_all_output_names(requests)?;

        requests
            .iter()
            .map(|request| {
                ensure_write_all_execute_run_mode(request.target().run_mode())?;
                self.plan_mssql_output(request)
            })
            .collect()
    }
}

fn ensure_write_all_execute_run_mode(run_mode: RunMode) -> Result<(), DeltaFunnelError> {
    match run_mode {
        RunMode::Execute => Ok(()),
        RunMode::DryRun => Err(DeltaFunnelError::MssqlWorkflowPlanning {
            message:
                "write_all requires RunMode::Execute; use dry_run_all_to_mssql for dry-run planning"
                    .to_owned(),
        }),
    }
}

pub(crate) fn ensure_unique_write_all_output_names(
    requests: &[OutputWritePlan],
) -> Result<(), DeltaFunnelError> {
    let mut output_names = BTreeSet::new();
    for request in requests {
        let output_name = request.target().output_name();
        if !output_names.insert(output_name) {
            return Err(DeltaFunnelError::MssqlWorkflowPlanning {
                message: format!(
                    "write_all output names must be unique; duplicate output name `{}`",
                    sanitize_text_for_display(output_name)
                ),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::super::super::{
        DeltaFunnelSession, SessionOptions,
        test_support::{execute_output_request, output_request, secret_connection},
    };
    use crate::{DeltaFunnelError, LoadMode};

    #[tokio::test]
    async fn plan_write_all_outputs_plans_valid_outputs_in_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::CreateAndLoad)?;

        let planned = session.plan_write_all_outputs(&[west, east])?;

        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].output_plan().output_name(), "west_output");
        assert_eq!(
            planned[0].output_plan().target_table().table(),
            "west_orders"
        );
        assert_eq!(planned[1].output_plan().output_name(), "east_output");
        assert_eq!(
            planned[1].output_plan().target_table().table(),
            "east_orders"
        );
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_duplicate_output_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west = execute_output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = execute_output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_write_all_outputs(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all output names must be unique")
                    && message.contains("orders_output")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_missing_connection_before_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let error = session.plan_write_all_outputs(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "west_output"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_replace_before_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = execute_output_request(east, "east_output", "east_orders", LoadMode::Replace)?;

        let error = session.plan_write_all_outputs(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlLifecyclePlanning { output_name, message })
                if output_name == "east_output"
                    && message.contains("replace load mode is reserved")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_dry_run_before_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;

        let error = session.plan_write_all_outputs(&[west]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all requires RunMode::Execute")
                    && message.contains("dry_run_all_to_mssql")
        ));
        Ok(())
    }
}
