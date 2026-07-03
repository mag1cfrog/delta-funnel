//! SQL Server target lifecycle execution data shapes.
//!
//! This module owns the redacted reporting boundary for preparing one planned
//! target before writer construction. Actual SQL Server probes and DDL
//! execution land in later slices.

use arrow_tiberius::{SqlExecutionOutcome, TableName, create_table_sql_from_mappings};
use async_trait::async_trait;

use crate::DeltaFunnelError;

use super::{
    LoadMode, MssqlConnectionSource, MssqlConnectionSummary, MssqlLifecycleExecutionGuardrail,
    MssqlTargetCleanupStatus, MssqlTargetOutputPlan, MssqlTargetTable, MssqlTargetTableState,
    MssqlWriteFailureContext, MssqlWritePhase,
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

/// Prepares one planned SQL Server target before writer construction.
#[allow(dead_code)]
pub(crate) async fn prepare_mssql_target_lifecycle<C>(
    output_plan: &MssqlTargetOutputPlan,
    client: &mut C,
) -> Result<MssqlPreparedTarget, DeltaFunnelError>
where
    C: MssqlTargetLifecycleClient + ?Sized,
{
    match output_plan.load_mode() {
        LoadMode::AppendExisting => prepare_mssql_append_existing_target(output_plan, client).await,
        LoadMode::CreateAndLoad => prepare_mssql_create_and_load_target(output_plan, client).await,
        LoadMode::Replace => prepare_mssql_replace_target(output_plan, client).await,
    }
}

/// Prepares an append-existing SQL Server target before writer construction.
pub(crate) async fn prepare_mssql_append_existing_target<C>(
    output_plan: &MssqlTargetOutputPlan,
    client: &mut C,
) -> Result<MssqlPreparedTarget, DeltaFunnelError>
where
    C: MssqlTargetLifecycleClient + ?Sized,
{
    ensure_append_existing_output(output_plan)?;
    ensure_lifecycle_guardrail(
        output_plan,
        MssqlLifecycleExecutionGuardrail::TargetTableExistenceProbe,
    )?;
    let table_name = table_name_from_target(output_plan.output_name(), output_plan.target_table())?;
    let exists = client
        .table_exists(&table_name)
        .await
        .map_err(|source| prepare_target_lifecycle_error(output_plan, source.to_string()))?;

    if !exists {
        return Err(prepare_target_lifecycle_error(
            output_plan,
            format!(
                "append-existing target table {} does not exist",
                table_name.quoted_sql()
            ),
        ));
    }

    MssqlPreparedTarget::from_output_plan(output_plan, MssqlPreparedTargetAction::VerifiedExisting)
}

/// Prepares a create-and-load SQL Server target before writer construction.
pub(crate) async fn prepare_mssql_create_and_load_target<C>(
    output_plan: &MssqlTargetOutputPlan,
    client: &mut C,
) -> Result<MssqlPreparedTarget, DeltaFunnelError>
where
    C: MssqlTargetLifecycleClient + ?Sized,
{
    ensure_create_and_load_output(output_plan)?;
    ensure_lifecycle_guardrail(
        output_plan,
        MssqlLifecycleExecutionGuardrail::TargetTableExistenceProbe,
    )?;
    ensure_lifecycle_guardrail(
        output_plan,
        MssqlLifecycleExecutionGuardrail::CreateTableDdlExecution,
    )?;
    let table_name = table_name_from_target(output_plan.output_name(), output_plan.target_table())?;
    let exists = client
        .table_exists(&table_name)
        .await
        .map_err(|source| prepare_target_lifecycle_error(output_plan, source.to_string()))?;

    if exists {
        return Err(prepare_target_lifecycle_error(
            output_plan,
            format!(
                "create-and-load target table {} already exists",
                table_name.quoted_sql()
            ),
        ));
    }

    let create_table_sql = output_plan.create_table_sql().ok_or_else(|| {
        prepare_target_lifecycle_error(
            output_plan,
            "create-and-load target preparation requires planned create-table SQL",
        )
    })?;
    client
        .execute_statement(create_table_sql)
        .await
        .map_err(|source| prepare_target_lifecycle_error(output_plan, source.to_string()))?;

    MssqlPreparedTarget::from_output_plan(output_plan, MssqlPreparedTargetAction::CreatedTable)
}

/// Prepares a replace SQL Server target by creating a private staging table.
pub(crate) async fn prepare_mssql_replace_target<C>(
    output_plan: &MssqlTargetOutputPlan,
    client: &mut C,
) -> Result<MssqlPreparedTarget, DeltaFunnelError>
where
    C: MssqlTargetLifecycleClient + ?Sized,
{
    ensure_replace_output(output_plan)?;
    ensure_lifecycle_guardrail(
        output_plan,
        MssqlLifecycleExecutionGuardrail::TargetTableExistenceProbe,
    )?;
    ensure_lifecycle_guardrail(
        output_plan,
        MssqlLifecycleExecutionGuardrail::CreateTableDdlExecution,
    )?;
    let final_table_name =
        table_name_from_target(output_plan.output_name(), output_plan.target_table())?;
    client
        .table_exists(&final_table_name)
        .await
        .map_err(|source| prepare_target_lifecycle_error(output_plan, source.to_string()))?;

    let staging_table_name = available_replace_staging_table(output_plan, client).await?;
    let create_table_sql =
        create_table_sql_from_mappings(&staging_table_name, output_plan.schema_mappings());
    client
        .execute_statement(&create_table_sql)
        .await
        .map_err(|source| prepare_target_lifecycle_error(output_plan, source.to_string()))?;

    MssqlPreparedTarget::from_table_name(
        output_plan,
        staging_table_name,
        MssqlPreparedTargetAction::CreatedStagingTable,
    )
}

/// Cleans up a DeltaFunnel-created SQL Server target after a later failure.
#[allow(dead_code)]
pub(crate) async fn cleanup_mssql_prepared_target<C>(
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: Option<&MssqlPreparedTarget>,
    client: &mut C,
) -> Result<MssqlTargetCleanupStatus, DeltaFunnelError>
where
    C: MssqlTargetLifecycleClient + ?Sized,
{
    let Some(prepared_target) = prepared_target else {
        return Ok(cleanup_before_target_creation(output_plan));
    };
    ensure_prepared_target_matches_output_plan(output_plan, prepared_target)?;

    match prepared_target.report().action() {
        MssqlPreparedTargetAction::VerifiedExisting => Ok(MssqlTargetCleanupStatus::NotApplicable),
        MssqlPreparedTargetAction::CreatedTable
        | MssqlPreparedTargetAction::CreatedStagingTable => {
            let drop_table_sql = format!("DROP TABLE {}", prepared_target.quoted_table_sql());
            client
                .execute_statement(&drop_table_sql)
                .await
                .map_err(|source| cleanup_error(output_plan, source.to_string()))?;

            Ok(MssqlTargetCleanupStatus::Succeeded)
        }
    }
}

/// Atomically swaps a prepared replace staging table into the final target name.
#[allow(dead_code)]
pub(crate) async fn swap_mssql_replace_target<C>(
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
    client: &mut C,
) -> Result<(), DeltaFunnelError>
where
    C: MssqlTargetLifecycleClient + ?Sized,
{
    ensure_replace_output(output_plan)?;
    if !prepared_target_matches_output_plan(output_plan, prepared_target) {
        return Err(swap_target_error(
            output_plan,
            "prepared target does not match output plan",
            false,
        ));
    }
    if prepared_target.report().action() != MssqlPreparedTargetAction::CreatedStagingTable {
        return Err(swap_target_error(
            output_plan,
            "replace swap requires a prepared staging table",
            false,
        ));
    }

    let final_table_name =
        table_name_from_target(output_plan.output_name(), output_plan.target_table())?;
    let backup_table_name = available_replace_backup_table(output_plan, client).await?;
    let swap_sql = replace_swap_sql(
        &final_table_name,
        prepared_target.table_name(),
        &backup_table_name,
    );

    client
        .execute_statement(&swap_sql)
        .await
        .map_err(|source| swap_target_error(output_plan, source.to_string(), true))?;

    Ok(())
}

/// Side effect completed while preparing a SQL Server target for loading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MssqlPreparedTargetAction {
    /// An append-existing target was verified to exist.
    VerifiedExisting,
    /// A create-and-load target table was created by DeltaFunnel.
    CreatedTable,
    /// A replace staging table was created by DeltaFunnel.
    CreatedStagingTable,
}

impl MssqlPreparedTargetAction {
    const fn cleanup_status(self) -> MssqlTargetCleanupStatus {
        match self {
            Self::VerifiedExisting => MssqlTargetCleanupStatus::NotApplicable,
            Self::CreatedTable | Self::CreatedStagingTable => {
                MssqlTargetCleanupStatus::NotAttempted
            }
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
        Self::from_table_name(output_plan, table_name, action)
    }

    pub(crate) fn from_table_name(
        output_plan: &MssqlTargetOutputPlan,
        table_name: TableName,
        action: MssqlPreparedTargetAction,
    ) -> Result<Self, DeltaFunnelError> {
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
        ) | (
            LoadMode::Replace,
            MssqlPreparedTargetAction::CreatedStagingTable
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

#[allow(dead_code)]
fn ensure_append_existing_output(
    output_plan: &MssqlTargetOutputPlan,
) -> Result<(), DeltaFunnelError> {
    if output_plan.load_mode() == LoadMode::AppendExisting {
        return Ok(());
    }

    Err(DeltaFunnelError::MssqlLifecyclePlanning {
        output_name: output_plan.output_name().to_owned(),
        message: "append-existing target preparation requires append-existing load mode".to_owned(),
    })
}

#[allow(dead_code)]
fn ensure_create_and_load_output(
    output_plan: &MssqlTargetOutputPlan,
) -> Result<(), DeltaFunnelError> {
    if output_plan.load_mode() == LoadMode::CreateAndLoad {
        return Ok(());
    }

    Err(DeltaFunnelError::MssqlLifecyclePlanning {
        output_name: output_plan.output_name().to_owned(),
        message: "create-and-load target preparation requires create-and-load load mode".to_owned(),
    })
}

#[allow(dead_code)]
fn ensure_replace_output(output_plan: &MssqlTargetOutputPlan) -> Result<(), DeltaFunnelError> {
    if output_plan.load_mode() == LoadMode::Replace {
        return Ok(());
    }

    Err(DeltaFunnelError::MssqlLifecyclePlanning {
        output_name: output_plan.output_name().to_owned(),
        message: "replace target preparation requires replace load mode".to_owned(),
    })
}

#[allow(dead_code)]
fn ensure_lifecycle_guardrail(
    output_plan: &MssqlTargetOutputPlan,
    guardrail: MssqlLifecycleExecutionGuardrail,
) -> Result<(), DeltaFunnelError> {
    if output_plan
        .lifecycle_plan()
        .execution_guardrails()
        .contains(&guardrail)
    {
        return Ok(());
    }

    Err(DeltaFunnelError::MssqlLifecyclePlanning {
        output_name: output_plan.output_name().to_owned(),
        message: format!("target lifecycle plan does not allow {guardrail:?}"),
    })
}

#[allow(dead_code)]
fn ensure_prepared_target_matches_output_plan(
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
) -> Result<(), DeltaFunnelError> {
    if prepared_target_matches_output_plan(output_plan, prepared_target) {
        return Ok(());
    }

    Err(cleanup_error(
        output_plan,
        "prepared target does not match output plan",
    ))
}

fn prepared_target_matches_output_plan(
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
) -> bool {
    let report = prepared_target.report();
    report.output_name() == output_plan.output_name()
        && report.target_table() == output_plan.target_table()
        && report.load_mode() == output_plan.load_mode()
        && report.connection_source() == output_plan.connection_source()
        && report.connection() == output_plan.connection()
        && report.expected_target_state() == output_plan.lifecycle_plan().expected_target_state()
}

#[allow(dead_code)]
fn prepare_target_lifecycle_error(
    output_plan: &MssqlTargetOutputPlan,
    message: impl Into<String>,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWritePhase {
        context: Box::new(MssqlWriteFailureContext::from_output_plan(
            output_plan,
            MssqlWritePhase::PrepareTargetLifecycle,
            0,
            0,
            0,
            false,
            cleanup_before_target_creation(output_plan),
        )),
        message: message.into(),
    }
}

#[allow(dead_code)]
fn cleanup_error(
    output_plan: &MssqlTargetOutputPlan,
    message: impl Into<String>,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWritePhase {
        context: Box::new(MssqlWriteFailureContext::from_output_plan(
            output_plan,
            MssqlWritePhase::Cleanup,
            0,
            0,
            0,
            false,
            MssqlTargetCleanupStatus::Failed,
        )),
        message: message.into(),
    }
}

#[allow(dead_code)]
fn swap_target_error(
    output_plan: &MssqlTargetOutputPlan,
    message: impl Into<String>,
    partial_write_possible: bool,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWritePhase {
        context: Box::new(MssqlWriteFailureContext::from_output_plan(
            output_plan,
            MssqlWritePhase::SwapTarget,
            0,
            0,
            0,
            partial_write_possible,
            MssqlTargetCleanupStatus::NotAttempted,
        )),
        message: message.into(),
    }
}

#[allow(dead_code)]
fn cleanup_before_target_creation(output_plan: &MssqlTargetOutputPlan) -> MssqlTargetCleanupStatus {
    match output_plan.load_mode() {
        LoadMode::AppendExisting => MssqlTargetCleanupStatus::NotApplicable,
        LoadMode::CreateAndLoad | LoadMode::Replace => MssqlTargetCleanupStatus::NotAttempted,
    }
}

async fn available_replace_staging_table<C>(
    output_plan: &MssqlTargetOutputPlan,
    client: &mut C,
) -> Result<TableName, DeltaFunnelError>
where
    C: MssqlTargetLifecycleClient + ?Sized,
{
    for attempt in 0..100_u8 {
        let table_name = replace_staging_table_name(output_plan, attempt)?;
        let exists = client
            .table_exists(&table_name)
            .await
            .map_err(|source| prepare_target_lifecycle_error(output_plan, source.to_string()))?;
        if !exists {
            return Ok(table_name);
        }
    }

    Err(prepare_target_lifecycle_error(
        output_plan,
        "could not find an available replace staging table name",
    ))
}

fn replace_staging_table_name(
    output_plan: &MssqlTargetOutputPlan,
    attempt: u8,
) -> Result<TableName, DeltaFunnelError> {
    let suffix = format!("__df_replace_{attempt}");
    replace_auxiliary_table_name(output_plan, &suffix)
}

async fn available_replace_backup_table<C>(
    output_plan: &MssqlTargetOutputPlan,
    client: &mut C,
) -> Result<TableName, DeltaFunnelError>
where
    C: MssqlTargetLifecycleClient + ?Sized,
{
    for attempt in 0..100_u8 {
        let table_name = replace_backup_table_name(output_plan, attempt)?;
        let exists = client
            .table_exists(&table_name)
            .await
            .map_err(|source| swap_target_error(output_plan, source.to_string(), false))?;
        if !exists {
            return Ok(table_name);
        }
    }

    Err(swap_target_error(
        output_plan,
        "could not find an available replace backup table name",
        false,
    ))
}

fn replace_backup_table_name(
    output_plan: &MssqlTargetOutputPlan,
    attempt: u8,
) -> Result<TableName, DeltaFunnelError> {
    let suffix = format!("__df_backup_{attempt}");
    replace_auxiliary_table_name(output_plan, &suffix)
}

fn replace_auxiliary_table_name(
    output_plan: &MssqlTargetOutputPlan,
    suffix: &str,
) -> Result<TableName, DeltaFunnelError> {
    const MAX_IDENTIFIER_CHARS: usize = 128;
    let max_base_chars = MAX_IDENTIFIER_CHARS.saturating_sub(suffix.chars().count());
    let base = output_plan
        .target_table()
        .table()
        .chars()
        .take(max_base_chars)
        .collect::<String>();
    let table_name = format!("{base}{suffix}");

    match output_plan.target_table().schema() {
        Some(schema) => TableName::new(schema, table_name),
        None => TableName::unqualified(table_name),
    }
    .map_err(|source| DeltaFunnelError::MssqlDdlTargetIdentifier {
        output_name: output_plan.output_name().to_owned(),
        source,
    })
}

fn replace_swap_sql(
    final_table: &TableName,
    staging_table: &TableName,
    backup_table: &TableName,
) -> String {
    let lock_resource = replace_lock_resource(final_table);
    let final_object = final_table.quoted_sql();
    let staging_object = staging_table.quoted_sql();
    let backup_object = backup_table.quoted_sql();
    let final_table_name = final_table.table().as_str();
    let backup_table_name = backup_table.table().as_str();

    format!(
        concat!(
            "SET XACT_ABORT ON;\n",
            "BEGIN TRY\n",
            "    BEGIN TRANSACTION;\n",
            "    DECLARE @delta_funnel_lock_result int;\n",
            "    EXEC @delta_funnel_lock_result = sys.sp_getapplock ",
            "@Resource = {lock_resource}, ",
            "@LockMode = 'Exclusive', ",
            "@LockOwner = 'Transaction', ",
            "@LockTimeout = 0;\n",
            "    IF @delta_funnel_lock_result < 0\n",
            "        THROW 51000, 'DeltaFunnel replace lock was not acquired', 1;\n",
            "    IF OBJECT_ID({final_object}, N'U') IS NULL\n",
            "        THROW 51001, 'DeltaFunnel replace target table is missing', 1;\n",
            "    IF OBJECT_ID({staging_object}, N'U') IS NULL\n",
            "        THROW 51002, 'DeltaFunnel replace staging table is missing', 1;\n",
            "    IF OBJECT_ID({backup_object}, N'U') IS NOT NULL\n",
            "        THROW 51003, 'DeltaFunnel replace backup table already exists', 1;\n",
            "    EXEC sys.sp_rename @objname = {final_object}, ",
            "@newname = {backup_table_name}, @objtype = 'OBJECT';\n",
            "    EXEC sys.sp_rename @objname = {staging_object}, ",
            "@newname = {final_table_name}, @objtype = 'OBJECT';\n",
            "    DROP TABLE {backup_table};\n",
            "    COMMIT TRANSACTION;\n",
            "END TRY\n",
            "BEGIN CATCH\n",
            "    IF @@TRANCOUNT > 0\n",
            "        ROLLBACK TRANSACTION;\n",
            "    THROW;\n",
            "END CATCH;"
        ),
        lock_resource = sql_nvarchar_literal(&lock_resource),
        final_object = sql_nvarchar_literal(&final_object),
        staging_object = sql_nvarchar_literal(&staging_object),
        backup_object = sql_nvarchar_literal(&backup_object),
        backup_table_name = sql_nvarchar_literal(backup_table_name),
        final_table_name = sql_nvarchar_literal(final_table_name),
        backup_table = backup_object,
    )
}

fn replace_lock_resource(final_table: &TableName) -> String {
    const MAX_RESOURCE_CHARS: usize = 255;
    const HASH_CHARS: usize = 16;
    const SEPARATOR_CHARS: usize = 1;

    let resource = format!("delta-funnel:replace:{}", final_table.quoted_sql());
    if resource.chars().count() <= MAX_RESOURCE_CHARS {
        return resource;
    }

    let prefix_chars = MAX_RESOURCE_CHARS - HASH_CHARS - SEPARATOR_CHARS;
    let prefix = resource.chars().take(prefix_chars).collect::<String>();
    format!("{prefix}:{:016x}", stable_resource_hash(&resource))
}

fn stable_resource_hash(value: &str) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn sql_nvarchar_literal(value: &str) -> String {
    format!("N'{}'", value.replace('\'', "''"))
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

    fn test_table_name(schema: &str, table: &str) -> Result<TableName, DeltaFunnelError> {
        TableName::new(schema, table).map_err(|error| DeltaFunnelError::Config {
            message: error.to_string(),
        })
    }

    fn replace_staging_prepared_target(
        output_plan: &MssqlTargetOutputPlan,
        table: &str,
    ) -> Result<MssqlPreparedTarget, DeltaFunnelError> {
        MssqlPreparedTarget::from_table_name(
            output_plan,
            test_table_name("dbo", table)?,
            MssqlPreparedTargetAction::CreatedStagingTable,
        )
    }

    #[derive(Default)]
    struct RecordingLifecycleClient {
        calls: Vec<String>,
        exists: bool,
        existence_results: Vec<bool>,
        rows_affected: Vec<u64>,
        table_exists_error: Option<String>,
        execute_statement_error: Option<String>,
    }

    #[async_trait]
    impl MssqlTargetLifecycleClient for RecordingLifecycleClient {
        async fn table_exists(&mut self, table: &TableName) -> arrow_tiberius::Result<bool> {
            self.calls.push(format!("probe {}", table.quoted_sql()));
            if let Some(reason) = self.table_exists_error.take() {
                return Err(arrow_tiberius::Error::TableExistsUnexpectedResult { reason });
            }

            if !self.existence_results.is_empty() {
                return Ok(self.existence_results.remove(0));
            }

            Ok(self.exists)
        }

        async fn execute_statement(
            &mut self,
            sql: &str,
        ) -> arrow_tiberius::Result<SqlExecutionOutcome> {
            self.calls.push(format!("execute {sql}"));
            if let Some(reason) = self.execute_statement_error.take() {
                return Err(arrow_tiberius::Error::InvalidIdentifier { reason });
            }

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

    #[tokio::test]
    async fn target_lifecycle_preparation_dispatches_append_existing()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            exists: true,
            ..RecordingLifecycleClient::default()
        };

        let prepared = prepare_mssql_target_lifecycle(&output_plan, &mut client).await?;

        assert_eq!(
            prepared.report().action(),
            MssqlPreparedTargetAction::VerifiedExisting
        );
        assert_eq!(client.calls, vec!["probe [dbo].[orders]".to_owned()]);
        Ok(())
    }

    #[tokio::test]
    async fn target_lifecycle_preparation_dispatches_create_and_load()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            exists: false,
            ..RecordingLifecycleClient::default()
        };

        let prepared = prepare_mssql_target_lifecycle(&output_plan, &mut client).await?;

        assert_eq!(
            prepared.report().action(),
            MssqlPreparedTargetAction::CreatedTable
        );
        assert_eq!(client.calls.len(), 2);
        assert_eq!(client.calls[0], "probe [dbo].[orders]");
        assert!(client.calls[1].starts_with("execute CREATE TABLE [dbo].[orders]"));
        Ok(())
    }

    #[tokio::test]
    async fn target_lifecycle_preparation_dispatches_replace_to_staging()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            existence_results: vec![true, false],
            ..RecordingLifecycleClient::default()
        };

        let prepared = prepare_mssql_target_lifecycle(&output_plan, &mut client).await?;

        assert_eq!(prepared.quoted_table_sql(), "[dbo].[orders__df_replace_0]");
        assert_eq!(
            prepared.report().action(),
            MssqlPreparedTargetAction::CreatedStagingTable
        );
        assert_eq!(
            prepared.report().cleanup(),
            MssqlTargetCleanupStatus::NotAttempted
        );
        assert_eq!(client.calls.len(), 3);
        assert_eq!(client.calls[0], "probe [dbo].[orders]");
        assert_eq!(client.calls[1], "probe [dbo].[orders__df_replace_0]");
        assert!(client.calls[2].starts_with("execute CREATE TABLE [dbo].[orders__df_replace_0]"));
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_preparation_verifies_existing_target() -> Result<(), DeltaFunnelError>
    {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            exists: true,
            ..RecordingLifecycleClient::default()
        };

        let prepared = prepare_mssql_append_existing_target(&output_plan, &mut client).await?;

        assert_eq!(prepared.quoted_table_sql(), "[dbo].[orders]");
        assert_eq!(
            prepared.report().action(),
            MssqlPreparedTargetAction::VerifiedExisting
        );
        assert_eq!(
            prepared.report().cleanup(),
            MssqlTargetCleanupStatus::NotApplicable
        );
        assert_eq!(client.calls, vec!["probe [dbo].[orders]".to_owned()]);
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_preparation_fails_when_target_is_absent()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            exists: false,
            ..RecordingLifecycleClient::default()
        };

        let error = prepare_mssql_append_existing_target(&output_plan, &mut client)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected absent append-existing target error".to_owned(),
            })?;

        assert_prepare_target_lifecycle_error(
            error,
            "append-existing target table [dbo].[orders] does not exist",
            MssqlTargetCleanupStatus::NotApplicable,
        )?;
        assert_eq!(client.calls, vec!["probe [dbo].[orders]".to_owned()]);
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_preparation_maps_probe_errors_to_lifecycle_phase()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            table_exists_error: Some("metadata query failed\nfor test".to_owned()),
            ..RecordingLifecycleClient::default()
        };

        let error = prepare_mssql_append_existing_target(&output_plan, &mut client)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected probe error".to_owned(),
            })?;

        assert_prepare_target_lifecycle_error(
            error,
            r"metadata query failed\nfor test",
            MssqlTargetCleanupStatus::NotApplicable,
        )?;
        assert_eq!(client.calls, vec!["probe [dbo].[orders]".to_owned()]);
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_preparation_rejects_create_and_load_before_probe()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            exists: true,
            ..RecordingLifecycleClient::default()
        };

        let error = prepare_mssql_append_existing_target(&output_plan, &mut client)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected append-only preparation error".to_owned(),
            })?;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlLifecyclePlanning { .. }
        ));
        assert!(error.to_string().contains("requires append-existing"));
        assert!(client.calls.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn create_and_load_preparation_creates_absent_target() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let create_table_sql = output_plan
            .create_table_sql()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected planned create-table SQL".to_owned(),
            })?
            .to_owned();
        let mut client = RecordingLifecycleClient {
            exists: false,
            rows_affected: vec![0],
            ..RecordingLifecycleClient::default()
        };

        let prepared = prepare_mssql_create_and_load_target(&output_plan, &mut client).await?;

        assert_eq!(prepared.quoted_table_sql(), "[dbo].[orders]");
        assert_eq!(
            prepared.report().action(),
            MssqlPreparedTargetAction::CreatedTable
        );
        assert_eq!(
            prepared.report().cleanup(),
            MssqlTargetCleanupStatus::NotAttempted
        );
        assert_eq!(
            client.calls,
            vec![
                "probe [dbo].[orders]".to_owned(),
                format!("execute {create_table_sql}"),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_and_load_preparation_fails_when_target_already_exists()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            exists: true,
            ..RecordingLifecycleClient::default()
        };

        let error = prepare_mssql_create_and_load_target(&output_plan, &mut client)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected existing create-and-load target error".to_owned(),
            })?;

        assert_prepare_target_lifecycle_error(
            error,
            "create-and-load target table [dbo].[orders] already exists",
            MssqlTargetCleanupStatus::NotAttempted,
        )?;
        assert_eq!(client.calls, vec!["probe [dbo].[orders]".to_owned()]);
        Ok(())
    }

    #[tokio::test]
    async fn create_and_load_preparation_maps_probe_errors_to_lifecycle_phase()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            table_exists_error: Some("metadata query failed\nfor test".to_owned()),
            ..RecordingLifecycleClient::default()
        };

        let error = prepare_mssql_create_and_load_target(&output_plan, &mut client)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected create-and-load probe error".to_owned(),
            })?;

        assert_prepare_target_lifecycle_error(
            error,
            r"metadata query failed\nfor test",
            MssqlTargetCleanupStatus::NotAttempted,
        )?;
        assert_eq!(client.calls, vec!["probe [dbo].[orders]".to_owned()]);
        Ok(())
    }

    #[tokio::test]
    async fn create_and_load_preparation_maps_ddl_errors_to_lifecycle_phase()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            exists: false,
            execute_statement_error: Some("DDL failed\nfor test".to_owned()),
            ..RecordingLifecycleClient::default()
        };

        let error = prepare_mssql_create_and_load_target(&output_plan, &mut client)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected create-and-load DDL error".to_owned(),
            })?;

        assert_prepare_target_lifecycle_error(
            error,
            r"DDL failed\nfor test",
            MssqlTargetCleanupStatus::NotAttempted,
        )?;
        assert_eq!(client.calls.len(), 2);
        assert_eq!(client.calls[0], "probe [dbo].[orders]");
        assert!(
            client.calls[1].starts_with("execute CREATE TABLE [dbo].[orders]"),
            "unexpected DDL call: {}",
            client.calls[1]
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_and_load_preparation_rejects_append_existing_before_probe()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            exists: false,
            ..RecordingLifecycleClient::default()
        };

        let error = prepare_mssql_create_and_load_target(&output_plan, &mut client)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected create-and-load mode error".to_owned(),
            })?;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlLifecyclePlanning { .. }
        ));
        assert!(error.to_string().contains("requires create-and-load"));
        assert!(client.calls.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn replace_preparation_creates_available_staging_target() -> Result<(), DeltaFunnelError>
    {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            existence_results: vec![true, false],
            rows_affected: vec![0],
            ..RecordingLifecycleClient::default()
        };

        let prepared = prepare_mssql_replace_target(&output_plan, &mut client).await?;

        assert_eq!(prepared.quoted_table_sql(), "[dbo].[orders__df_replace_0]");
        assert_eq!(
            prepared.report().action(),
            MssqlPreparedTargetAction::CreatedStagingTable
        );
        assert_eq!(client.calls.len(), 3);
        assert_eq!(client.calls[0], "probe [dbo].[orders]");
        assert_eq!(client.calls[1], "probe [dbo].[orders__df_replace_0]");
        assert!(client.calls[2].starts_with("execute CREATE TABLE [dbo].[orders__df_replace_0]"));
        assert!(client.calls[2].contains("[order_id] bigint NOT NULL"));
        assert!(client.calls[2].contains("[status] nvarchar(max) NULL"));
        Ok(())
    }

    #[tokio::test]
    async fn replace_preparation_creates_staging_when_final_target_is_absent()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            existence_results: vec![false, false],
            rows_affected: vec![0],
            ..RecordingLifecycleClient::default()
        };

        let prepared = prepare_mssql_replace_target(&output_plan, &mut client).await?;

        assert_eq!(prepared.quoted_table_sql(), "[dbo].[orders__df_replace_0]");
        assert_eq!(
            prepared.report().action(),
            MssqlPreparedTargetAction::CreatedStagingTable
        );
        assert_eq!(
            client.calls,
            vec![
                "probe [dbo].[orders]",
                "probe [dbo].[orders__df_replace_0]",
                "execute CREATE TABLE [dbo].[orders__df_replace_0] (\n    [order_id] bigint NOT NULL,\n    [status] nvarchar(max) NULL\n);"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn replace_preparation_skips_existing_staging_name() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            existence_results: vec![true, true, false],
            ..RecordingLifecycleClient::default()
        };

        let prepared = prepare_mssql_replace_target(&output_plan, &mut client).await?;

        assert_eq!(prepared.quoted_table_sql(), "[dbo].[orders__df_replace_1]");
        assert_eq!(client.calls.len(), 4);
        assert_eq!(
            &client.calls[..3],
            [
                "probe [dbo].[orders]",
                "probe [dbo].[orders__df_replace_0]",
                "probe [dbo].[orders__df_replace_1]",
            ]
        );
        assert!(client.calls[3].starts_with("execute CREATE TABLE [dbo].[orders__df_replace_1]"));
        Ok(())
    }

    #[tokio::test]
    async fn replace_preparation_rejects_create_and_load_before_probe()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient {
            exists: true,
            ..RecordingLifecycleClient::default()
        };

        let error = prepare_mssql_replace_target(&output_plan, &mut client)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected replace preparation mode error".to_owned(),
            })?;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlLifecyclePlanning { .. }
        ));
        assert!(error.to_string().contains("requires replace"));
        assert!(client.calls.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_returns_not_applicable_without_append_existing_target_creation()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let prepared = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::VerifiedExisting,
        )?;
        let mut client = RecordingLifecycleClient::default();

        let cleanup =
            cleanup_mssql_prepared_target(&output_plan, Some(&prepared), &mut client).await?;

        assert_eq!(cleanup, MssqlTargetCleanupStatus::NotApplicable);
        assert!(client.calls.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_returns_not_attempted_before_create_and_load_target_creation()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let mut client = RecordingLifecycleClient::default();

        let cleanup = cleanup_mssql_prepared_target(&output_plan, None, &mut client).await?;

        assert_eq!(cleanup, MssqlTargetCleanupStatus::NotAttempted);
        assert!(client.calls.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_drops_deltafunnel_created_target_with_quoted_table_name()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo.part", "target]part")?,
        )?;
        let prepared = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::CreatedTable,
        )?;
        let mut client = RecordingLifecycleClient::default();

        let cleanup =
            cleanup_mssql_prepared_target(&output_plan, Some(&prepared), &mut client).await?;

        assert_eq!(cleanup, MssqlTargetCleanupStatus::Succeeded);
        assert_eq!(
            client.calls,
            vec!["execute DROP TABLE [dbo.part].[target]]part]".to_owned()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_drops_deltafunnel_created_replace_staging_target()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let staging_table = TableName::new("dbo", "orders__df_replace_0").map_err(|error| {
            DeltaFunnelError::Config {
                message: error.to_string(),
            }
        })?;
        let prepared = MssqlPreparedTarget::from_table_name(
            &output_plan,
            staging_table,
            MssqlPreparedTargetAction::CreatedStagingTable,
        )?;
        let mut client = RecordingLifecycleClient::default();

        let cleanup =
            cleanup_mssql_prepared_target(&output_plan, Some(&prepared), &mut client).await?;

        assert_eq!(cleanup, MssqlTargetCleanupStatus::Succeeded);
        assert_eq!(
            client.calls,
            vec!["execute DROP TABLE [dbo].[orders__df_replace_0]".to_owned()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_errors_map_to_cleanup_phase() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let prepared = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::CreatedTable,
        )?;
        let mut client = RecordingLifecycleClient {
            execute_statement_error: Some("DROP failed\nfor test".to_owned()),
            ..RecordingLifecycleClient::default()
        };

        let error = cleanup_mssql_prepared_target(&output_plan, Some(&prepared), &mut client)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected cleanup error".to_owned(),
            })?;

        assert_cleanup_error(error, r"DROP failed\nfor test")?;
        assert_eq!(
            client.calls,
            vec!["execute DROP TABLE [dbo].[orders]".to_owned()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_rejects_prepared_target_from_different_output_without_drop()
    -> Result<(), DeltaFunnelError> {
        let current_output_plan = output_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let other_output_plan = output_plan(
            "other_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "other_orders")?,
        )?;
        let prepared = MssqlPreparedTarget::from_output_plan(
            &other_output_plan,
            MssqlPreparedTargetAction::CreatedTable,
        )?;
        let mut client = RecordingLifecycleClient::default();

        let error =
            cleanup_mssql_prepared_target(&current_output_plan, Some(&prepared), &mut client)
                .await
                .err()
                .ok_or_else(|| DeltaFunnelError::Config {
                    message: "expected mismatched cleanup target error".to_owned(),
                })?;

        assert_cleanup_error(error, "prepared target does not match output plan")?;
        assert!(client.calls.is_empty());
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
    fn replace_prepared_target_reports_created_staging_table() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::unqualified("orders")?,
        )?;
        let staging_table = TableName::unqualified("orders__df_replace_0").map_err(|error| {
            DeltaFunnelError::Config {
                message: error.to_string(),
            }
        })?;

        let prepared = MssqlPreparedTarget::from_table_name(
            &output_plan,
            staging_table,
            MssqlPreparedTargetAction::CreatedStagingTable,
        )?;
        let report = prepared.report();

        assert_eq!(prepared.table_name().quoted_sql(), "[orders__df_replace_0]");
        assert_eq!(report.target_table().table(), "orders");
        assert_eq!(report.load_mode(), LoadMode::Replace);
        assert_eq!(
            report.expected_target_state(),
            MssqlTargetTableState::ExistsOrAbsent
        );
        assert_eq!(
            report.action(),
            MssqlPreparedTargetAction::CreatedStagingTable
        );
        assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotAttempted);
        Ok(())
    }

    #[test]
    fn replace_swap_sql_uses_transactional_rename_and_escaped_literals()
    -> Result<(), DeltaFunnelError> {
        let final_table = test_table_name("dbo", "orders")?;
        let staging_table = test_table_name("dbo", "orders__df_replace_0")?;
        let backup_table = test_table_name("dbo", "orders__df_backup_0")?;

        let sql = replace_swap_sql(&final_table, &staging_table, &backup_table);

        assert!(sql.starts_with("SET XACT_ABORT ON;"));
        assert!(sql.contains("BEGIN TRANSACTION;"));
        assert!(sql.contains("sys.sp_getapplock"));
        assert!(sql.contains("@LockOwner = 'Transaction'"));
        assert!(sql.contains("@LockTimeout = 0"));
        assert!(sql.contains("ROLLBACK TRANSACTION;"));
        assert!(sql.contains(
            "EXEC sys.sp_rename @objname = N'[dbo].[orders]', @newname = N'orders__df_backup_0', @objtype = 'OBJECT';"
        ));
        assert!(sql.contains(
            "EXEC sys.sp_rename @objname = N'[dbo].[orders__df_replace_0]', @newname = N'orders', @objtype = 'OBJECT';"
        ));
        assert!(sql.contains("DROP TABLE [dbo].[orders__df_backup_0];"));
        assert_eq!(
            sql_nvarchar_literal("tenant's.orders"),
            "N'tenant''s.orders'"
        );
        Ok(())
    }

    #[test]
    fn replace_lock_resource_keeps_long_target_names_distinct() -> Result<(), DeltaFunnelError> {
        let schema = "s".repeat(128);
        let table_prefix = "t".repeat(127);
        let first_table = test_table_name(&schema, &format!("{table_prefix}a"))?;
        let second_table = test_table_name(&schema, &format!("{table_prefix}b"))?;

        let first_lock = replace_lock_resource(&first_table);
        let second_lock = replace_lock_resource(&second_table);

        assert_eq!(first_lock.chars().count(), 255);
        assert_eq!(second_lock.chars().count(), 255);
        assert_ne!(first_lock, second_lock);
        Ok(())
    }

    #[tokio::test]
    async fn replace_swap_skips_existing_backup_name_and_executes_batch()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let prepared = replace_staging_prepared_target(&output_plan, "orders__df_replace_0")?;
        let mut client = RecordingLifecycleClient {
            existence_results: vec![true, false],
            rows_affected: vec![0],
            ..RecordingLifecycleClient::default()
        };

        swap_mssql_replace_target(&output_plan, &prepared, &mut client).await?;

        assert_eq!(client.calls.len(), 3);
        assert_eq!(client.calls[0], "probe [dbo].[orders__df_backup_0]");
        assert_eq!(client.calls[1], "probe [dbo].[orders__df_backup_1]");
        assert!(client.calls[2].contains(
            "EXEC sys.sp_rename @objname = N'[dbo].[orders]', @newname = N'orders__df_backup_1'"
        ));
        assert!(client.calls[2].contains("DROP TABLE [dbo].[orders__df_backup_1];"));
        Ok(())
    }

    #[tokio::test]
    async fn replace_swap_maps_execute_errors_to_swap_phase() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let prepared = replace_staging_prepared_target(&output_plan, "orders__df_replace_0")?;
        let mut client = RecordingLifecycleClient {
            existence_results: vec![false],
            execute_statement_error: Some("swap failed\nfor test".to_owned()),
            ..RecordingLifecycleClient::default()
        };

        let error = swap_mssql_replace_target(&output_plan, &prepared, &mut client)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected swap execution error".to_owned(),
            })?;

        assert_swap_target_error(error, r"swap failed\nfor test", true)?;
        assert_eq!(client.calls.len(), 2);
        assert_eq!(client.calls[0], "probe [dbo].[orders__df_backup_0]");
        assert!(client.calls[1].starts_with("execute SET XACT_ABORT ON;"));
        Ok(())
    }

    #[tokio::test]
    async fn replace_swap_rejects_mismatched_prepared_target_without_sql()
    -> Result<(), DeltaFunnelError> {
        let current_output_plan = output_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let other_output_plan = output_plan(
            "other_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "other_orders")?,
        )?;
        let prepared =
            replace_staging_prepared_target(&other_output_plan, "other_orders__df_replace_0")?;
        let mut client = RecordingLifecycleClient::default();

        let error = swap_mssql_replace_target(&current_output_plan, &prepared, &mut client)
            .await
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected mismatched swap target error".to_owned(),
            })?;

        assert_swap_target_error(error, "prepared target does not match output plan", false)?;
        assert!(client.calls.is_empty());
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

    fn assert_prepare_target_lifecycle_error(
        error: DeltaFunnelError,
        expected_message: &str,
        expected_cleanup: MssqlTargetCleanupStatus,
    ) -> Result<(), DeltaFunnelError> {
        let display = error.to_string();
        assert!(display.contains("orders_output"));
        assert!(display.contains("prepare target lifecycle"));
        assert!(display.contains(expected_message));
        assert!(!display.contains("secret-token"));
        assert!(!display.contains("server=tcp"));
        let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MssqlWritePhase error".to_owned(),
            });
        };

        assert_eq!(context.phase(), MssqlWritePhase::PrepareTargetLifecycle);
        assert_eq!(context.output_name(), "orders_output");
        assert_eq!(context.stats().rows_written(), 0);
        assert_eq!(context.stats().batches_written(), 0);
        assert!(!context.partial_write_possible());
        assert_eq!(context.cleanup(), expected_cleanup);
        Ok(())
    }

    fn assert_cleanup_error(
        error: DeltaFunnelError,
        expected_message: &str,
    ) -> Result<(), DeltaFunnelError> {
        let display = error.to_string();
        assert!(display.contains("orders_output"));
        assert!(display.contains("cleanup"));
        assert!(display.contains(expected_message));
        assert!(!display.contains("secret-token"));
        assert!(!display.contains("server=tcp"));
        let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MssqlWritePhase error".to_owned(),
            });
        };

        assert_eq!(context.phase(), MssqlWritePhase::Cleanup);
        assert_eq!(context.output_name(), "orders_output");
        assert_eq!(context.stats().rows_written(), 0);
        assert_eq!(context.stats().batches_written(), 0);
        assert!(!context.partial_write_possible());
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Failed);
        Ok(())
    }

    fn assert_swap_target_error(
        error: DeltaFunnelError,
        expected_message: &str,
        expected_partial_write_possible: bool,
    ) -> Result<(), DeltaFunnelError> {
        let display = error.to_string();
        assert!(display.contains("orders_output"));
        assert!(display.contains("swap target"));
        assert!(display.contains(expected_message));
        assert!(!display.contains("secret-token"));
        assert!(!display.contains("server=tcp"));
        let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MssqlWritePhase error".to_owned(),
            });
        };

        assert_eq!(context.phase(), MssqlWritePhase::SwapTarget);
        assert_eq!(context.output_name(), "orders_output");
        assert_eq!(context.stats().rows_written(), 0);
        assert_eq!(context.stats().batches_written(), 0);
        assert_eq!(
            context.partial_write_possible(),
            expected_partial_write_possible
        );
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::NotAttempted);
        Ok(())
    }
}
