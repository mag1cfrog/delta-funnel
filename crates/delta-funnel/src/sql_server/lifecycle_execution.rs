//! SQL Server target lifecycle execution data shapes.
//!
//! This module owns the redacted reporting boundary for preparing one planned
//! target before writer construction. Actual SQL Server probes and DDL
//! execution land in later slices.

use arrow_tiberius::{SqlExecutionOutcome, TableName};
use async_trait::async_trait;

use crate::DeltaFunnelError;

use super::{
    LoadMode, MssqlConnectionSource, MssqlConnectionSummary, MssqlTargetCleanupStatus,
    MssqlTargetOutputPlan, MssqlTargetTable, MssqlTargetTableState,
};

/// Fakeable SQL Server target lifecycle operation boundary.
#[allow(dead_code)]
#[async_trait]
pub(crate) trait MssqlTargetLifecycleClient: Send {
    /// Returns whether the target table exists in SQL Server metadata.
    async fn table_exists(&mut self, table: &TableName) -> arrow_tiberius::Result<bool>;

    /// Executes one prepared lifecycle SQL statement.
    async fn execute_statement(&mut self, sql: &str)
    -> arrow_tiberius::Result<SqlExecutionOutcome>;
}

#[async_trait]
impl MssqlTargetLifecycleClient for arrow_tiberius::ConnectedMssqlClient {
    async fn table_exists(&mut self, table: &TableName) -> arrow_tiberius::Result<bool> {
        arrow_tiberius::ConnectedMssqlClient::table_exists(self, table).await
    }

    async fn execute_statement(
        &mut self,
        sql: &str,
    ) -> arrow_tiberius::Result<SqlExecutionOutcome> {
        arrow_tiberius::ConnectedMssqlClient::execute_statement(self, sql).await
    }
}

/// Connected lifecycle client paired with the planned output it prepares.
#[allow(dead_code)]
pub(crate) struct MssqlConnectedLifecycleClient<'client> {
    output_plan: &'client MssqlTargetOutputPlan,
    client: &'client mut arrow_tiberius::ConnectedMssqlClient,
}

impl<'client> MssqlConnectedLifecycleClient<'client> {
    /// Pairs a connected arrow-tiberius client with its redacted output plan.
    #[must_use]
    pub(crate) fn new(
        output_plan: &'client MssqlTargetOutputPlan,
        client: &'client mut arrow_tiberius::ConnectedMssqlClient,
    ) -> Self {
        Self {
            output_plan,
            client,
        }
    }

    /// Returns the redacted target output plan paired with this connection.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) const fn output_plan(&self) -> &MssqlTargetOutputPlan {
        self.output_plan
    }
}

#[async_trait]
impl MssqlTargetLifecycleClient for MssqlConnectedLifecycleClient<'_> {
    async fn table_exists(&mut self, table: &TableName) -> arrow_tiberius::Result<bool> {
        MssqlTargetLifecycleClient::table_exists(self.client, table).await
    }

    async fn execute_statement(
        &mut self,
        sql: &str,
    ) -> arrow_tiberius::Result<SqlExecutionOutcome> {
        MssqlTargetLifecycleClient::execute_statement(self.client, sql).await
    }
}

/// Side effect completed while preparing a SQL Server target for loading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MssqlPreparedTargetAction {
    /// An append-existing target was verified to exist.
    VerifiedExisting,
    /// A create-and-load target table was created by DeltaFunnel.
    CreatedTable,
}

impl MssqlPreparedTargetAction {
    const fn cleanup_status(self) -> MssqlTargetCleanupStatus {
        match self {
            Self::VerifiedExisting => MssqlTargetCleanupStatus::NotApplicable,
            Self::CreatedTable => MssqlTargetCleanupStatus::NotAttempted,
        }
    }
}

/// Redacted report for a prepared SQL Server target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlPreparedTargetReport {
    output_name: String,
    target_table: MssqlTargetTable,
    load_mode: LoadMode,
    connection_source: MssqlConnectionSource,
    connection: MssqlConnectionSummary,
    expected_target_state: MssqlTargetTableState,
    action: MssqlPreparedTargetAction,
    cleanup: MssqlTargetCleanupStatus,
}

impl MssqlPreparedTargetReport {
    /// Builds a report from an already planned SQL Server output target.
    pub fn from_output_plan(
        output_plan: &MssqlTargetOutputPlan,
        action: MssqlPreparedTargetAction,
    ) -> Result<Self, DeltaFunnelError> {
        ensure_action_matches_load_mode(output_plan, action)?;

        Ok(Self {
            output_name: output_plan.output_name().to_owned(),
            target_table: output_plan.target_table().clone(),
            load_mode: output_plan.load_mode(),
            connection_source: output_plan.connection_source(),
            connection: output_plan.connection().clone(),
            expected_target_state: output_plan.lifecycle_plan().expected_target_state(),
            action,
            cleanup: action.cleanup_status(),
        })
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the effective target table.
    #[must_use]
    pub const fn target_table(&self) -> &MssqlTargetTable {
        &self.target_table
    }

    /// Returns the requested target lifecycle mode.
    #[must_use]
    pub const fn load_mode(&self) -> LoadMode {
        self.load_mode
    }

    /// Returns where the effective connection came from.
    #[must_use]
    pub const fn connection_source(&self) -> MssqlConnectionSource {
        self.connection_source
    }

    /// Returns the redacted effective connection summary.
    #[must_use]
    pub const fn connection(&self) -> &MssqlConnectionSummary {
        &self.connection
    }

    /// Returns the expected live target table state before loading.
    #[must_use]
    pub const fn expected_target_state(&self) -> MssqlTargetTableState {
        self.expected_target_state
    }

    /// Returns the side effect completed while preparing the target.
    #[must_use]
    pub const fn action(&self) -> MssqlPreparedTargetAction {
        self.action
    }

    /// Returns cleanup reporting state for DeltaFunnel-owned target cleanup.
    #[must_use]
    pub const fn cleanup(&self) -> MssqlTargetCleanupStatus {
        self.cleanup
    }
}

/// Prepared SQL Server target identity and redacted report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlPreparedTarget {
    table_name: TableName,
    report: MssqlPreparedTargetReport,
}

impl MssqlPreparedTarget {
    /// Builds a prepared target shape from an already planned SQL Server output.
    pub fn from_output_plan(
        output_plan: &MssqlTargetOutputPlan,
        action: MssqlPreparedTargetAction,
    ) -> Result<Self, DeltaFunnelError> {
        let table_name =
            table_name_from_target(output_plan.output_name(), output_plan.target_table())?;
        let report = MssqlPreparedTargetReport::from_output_plan(output_plan, action)?;

        Ok(Self { table_name, report })
    }

    /// Returns the arrow-tiberius target table identity.
    #[must_use]
    pub const fn table_name(&self) -> &TableName {
        &self.table_name
    }

    /// Returns the bracket-quoted target table SQL identity.
    #[must_use]
    pub fn quoted_table_sql(&self) -> String {
        self.table_name.quoted_sql()
    }

    /// Returns the redacted prepared target report.
    #[must_use]
    pub const fn report(&self) -> &MssqlPreparedTargetReport {
        &self.report
    }
}

pub(crate) fn table_name_from_target(
    output_name: &str,
    table: &MssqlTargetTable,
) -> Result<TableName, DeltaFunnelError> {
    match table.schema() {
        Some(schema) => TableName::new(schema, table.table()),
        None => TableName::unqualified(table.table()),
    }
    .map_err(|source| DeltaFunnelError::MssqlDdlTargetIdentifier {
        output_name: output_name.to_owned(),
        source,
    })
}

fn ensure_action_matches_load_mode(
    output_plan: &MssqlTargetOutputPlan,
    action: MssqlPreparedTargetAction,
) -> Result<(), DeltaFunnelError> {
    let allowed = matches!(
        (output_plan.load_mode(), action),
        (
            LoadMode::AppendExisting,
            MssqlPreparedTargetAction::VerifiedExisting
        ) | (
            LoadMode::CreateAndLoad,
            MssqlPreparedTargetAction::CreatedTable
        )
    );

    if allowed {
        return Ok(());
    }

    Err(DeltaFunnelError::MssqlLifecyclePlanning {
        output_name: output_plan.output_name().to_owned(),
        message: "prepared target action does not match load mode".to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use arrow_tiberius::PlanOptions;
    use async_trait::async_trait;

    use super::*;
    use crate::{
        MssqlConnectionConfig, MssqlConnectionSource, MssqlTargetConfig,
        MssqlTargetResolutionContext, plan_mssql_output_schema, plan_mssql_target_output,
    };

    fn secret_connection(label: &str) -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label(label))
    }

    fn output_plan(
        output_name: &str,
        load_mode: LoadMode,
        table: MssqlTargetTable,
    ) -> Result<MssqlTargetOutputPlan, DeltaFunnelError> {
        let connection = secret_connection("warehouse-primary")?;
        let target = MssqlTargetConfig::new(table)
            .with_load_mode(load_mode)
            .resolve(MssqlTargetResolutionContext {
                output_name: Some(output_name),
                default_connection: Some(&connection),
            })?;
        let schema = Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
        ]);
        let schema_plan = plan_mssql_output_schema(&schema, &target, PlanOptions::default())?;

        plan_mssql_target_output(schema_plan)
    }

    #[derive(Default)]
    struct RecordingLifecycleClient {
        calls: Vec<String>,
        exists: bool,
        rows_affected: Vec<u64>,
    }

    #[async_trait]
    impl MssqlTargetLifecycleClient for RecordingLifecycleClient {
        async fn table_exists(&mut self, table: &TableName) -> arrow_tiberius::Result<bool> {
            self.calls.push(format!("probe {}", table.quoted_sql()));
            Ok(self.exists)
        }

        async fn execute_statement(
            &mut self,
            sql: &str,
        ) -> arrow_tiberius::Result<SqlExecutionOutcome> {
            self.calls.push(format!("execute {sql}"));
            Ok(SqlExecutionOutcome {
                rows_affected: self.rows_affected.clone(),
            })
        }
    }

    #[test]
    fn connected_arrow_tiberius_client_implements_lifecycle_client_boundary() {
        fn assert_lifecycle_client<C: MssqlTargetLifecycleClient>() {}

        assert_lifecycle_client::<arrow_tiberius::ConnectedMssqlClient>();
    }

    #[tokio::test]
    async fn lifecycle_client_boundary_is_fakeable() -> arrow_tiberius::Result<()> {
        let table = TableName::new("dbo", "orders")?;
        let mut client = RecordingLifecycleClient {
            exists: true,
            rows_affected: vec![1],
            ..RecordingLifecycleClient::default()
        };

        let exists = MssqlTargetLifecycleClient::table_exists(&mut client, &table).await?;
        let outcome = MssqlTargetLifecycleClient::execute_statement(
            &mut client,
            "CREATE TABLE [dbo].[orders] ([id] bigint)",
        )
        .await?;

        assert!(exists);
        assert_eq!(outcome.rows_affected, vec![1]);
        assert_eq!(
            client.calls,
            vec![
                "probe [dbo].[orders]".to_owned(),
                "execute CREATE TABLE [dbo].[orders] ([id] bigint)".to_owned(),
            ]
        );
        Ok(())
    }

    #[test]
    fn qualified_target_table_converts_to_arrow_tiberius_table_name() -> Result<(), DeltaFunnelError>
    {
        let table = MssqlTargetTable::new("dbo", "orders")?;

        let table_name = table_name_from_target("orders_output", &table)?;

        assert_eq!(table_name.quoted_sql(), "[dbo].[orders]");
        Ok(())
    }

    #[test]
    fn unqualified_target_table_keeps_identifier_unqualified() -> Result<(), DeltaFunnelError> {
        let table = MssqlTargetTable::unqualified("orders")?;

        let table_name = table_name_from_target("orders_output", &table)?;

        assert_eq!(table_name.quoted_sql(), "[orders]");
        Ok(())
    }

    #[test]
    fn special_target_identifiers_use_arrow_tiberius_escaping() -> Result<(), DeltaFunnelError> {
        let table = MssqlTargetTable::new("dbo.part", "target]part")?;

        let table_name = table_name_from_target("orders_output", &table)?;

        assert_eq!(table_name.quoted_sql(), "[dbo.part].[target]]part]");
        Ok(())
    }

    #[test]
    fn append_existing_prepared_target_reports_verified_existing() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;

        let prepared = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::VerifiedExisting,
        )?;
        let report = prepared.report();

        assert_eq!(prepared.quoted_table_sql(), "[dbo].[orders]");
        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.target_table().schema(), Some("dbo"));
        assert_eq!(report.target_table().table(), "orders");
        assert_eq!(report.load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            report.connection_source(),
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(
            report.connection().display_label(),
            Some("warehouse-primary")
        );
        assert_eq!(
            report.expected_target_state(),
            MssqlTargetTableState::Exists
        );
        assert_eq!(report.action(), MssqlPreparedTargetAction::VerifiedExisting);
        assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        Ok(())
    }

    #[test]
    fn create_and_load_prepared_target_reports_created_table() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::unqualified("orders")?,
        )?;

        let prepared = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::CreatedTable,
        )?;
        let report = prepared.report();

        assert_eq!(prepared.table_name().quoted_sql(), "[orders]");
        assert_eq!(report.load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(
            report.expected_target_state(),
            MssqlTargetTableState::Absent
        );
        assert_eq!(report.action(), MssqlPreparedTargetAction::CreatedTable);
        assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotAttempted);
        Ok(())
    }

    #[test]
    fn prepared_target_action_must_match_load_mode() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;

        let error = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::CreatedTable,
        )
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected mismatched prepared target action error".to_owned(),
        })?;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlLifecyclePlanning { .. }
        ));
        assert!(error.to_string().contains("does not match load mode"));
        Ok(())
    }

    #[test]
    fn prepared_target_reports_do_not_expose_connection_secrets() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;

        let prepared = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::CreatedTable,
        )?;
        let combined = format!("{output_plan:?}\n{prepared:?}");

        assert!(!combined.contains("secret-token"));
        assert!(!combined.contains("password"));
        assert!(!combined.contains("server=tcp"));
        assert!(combined.contains("warehouse-primary"));
        Ok(())
    }
}
