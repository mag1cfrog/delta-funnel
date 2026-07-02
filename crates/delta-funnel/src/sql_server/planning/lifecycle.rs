//! SQL Server table lifecycle planning.

use crate::DeltaFunnelError;

use super::{LoadMode, MssqlDdlPlan, MssqlSchemaPlan, MssqlTargetSummary};

/// Expected live state of the target table before loading starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MssqlTargetTableState {
    /// Target table should already exist before loading.
    Exists,
    /// Target table should not exist before loading and should be created.
    Absent,
}

/// Whether guarded execution may proceed after lifecycle planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MssqlLifecycleGuardrailPolicy {
    /// Planning succeeded and later execution phases may perform their work.
    AllowedAfterPlanning,
    /// Planning failed, so later execution phases must not run.
    ForbiddenAfterPlanningFailure,
}

/// An execution operation guarded by successful lifecycle planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MssqlLifecycleExecutionGuardrail {
    /// Delta source scan execution must wait for lifecycle planning.
    DeltaSourceScanExecution,
    /// DataFusion physical plan execution must wait for lifecycle planning.
    DataFusionPhysicalPlanExecution,
    /// SQL Server connection attempts must wait for lifecycle planning.
    SqlServerConnectionAttempt,
    /// Target table existence probes must wait for lifecycle planning.
    TargetTableExistenceProbe,
    /// Create-table DDL execution must wait for lifecycle planning.
    CreateTableDdlExecution,
    /// Bulk writer construction must wait for lifecycle planning.
    BulkWriterConstruction,
    /// RecordBatch handoff polling must wait for lifecycle planning.
    RecordBatchHandoffPolling,
}

/// Planned SQL Server table lifecycle behavior for one selected output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlLifecyclePlan {
    target: MssqlTargetSummary,
    expected_target_state: MssqlTargetTableState,
    create_table_sql_required: bool,
    create_table_sql_present: bool,
    executable_in_mvp: bool,
    guardrail_policy: MssqlLifecycleGuardrailPolicy,
    execution_guardrails: &'static [MssqlLifecycleExecutionGuardrail],
}

impl MssqlLifecyclePlan {
    /// Returns the redacted resolved target summary.
    #[must_use]
    pub fn target(&self) -> &MssqlTargetSummary {
        &self.target
    }

    /// Returns the expected live target table state before loading.
    #[must_use]
    pub const fn expected_target_state(&self) -> MssqlTargetTableState {
        self.expected_target_state
    }

    /// Returns true when create-table SQL is required by the lifecycle mode.
    #[must_use]
    pub const fn create_table_sql_required(&self) -> bool {
        self.create_table_sql_required
    }

    /// Returns true when create-table SQL is present in the associated DDL plan.
    #[must_use]
    pub const fn create_table_sql_present(&self) -> bool {
        self.create_table_sql_present
    }

    /// Returns true when this lifecycle mode can be executed in the MVP.
    #[must_use]
    pub const fn executable_in_mvp(&self) -> bool {
        self.executable_in_mvp
    }

    /// Returns whether later execution phases may proceed.
    #[must_use]
    pub const fn guardrail_policy(&self) -> MssqlLifecycleGuardrailPolicy {
        self.guardrail_policy
    }

    /// Returns guarded execution operations in the order they should run.
    #[must_use]
    pub const fn execution_guardrails(&self) -> &[MssqlLifecycleExecutionGuardrail] {
        self.execution_guardrails
    }
}

const APPEND_EXISTING_EXECUTION_GUARDRAILS: &[MssqlLifecycleExecutionGuardrail] = &[
    MssqlLifecycleExecutionGuardrail::SqlServerConnectionAttempt,
    MssqlLifecycleExecutionGuardrail::TargetTableExistenceProbe,
    MssqlLifecycleExecutionGuardrail::BulkWriterConstruction,
    MssqlLifecycleExecutionGuardrail::DeltaSourceScanExecution,
    MssqlLifecycleExecutionGuardrail::DataFusionPhysicalPlanExecution,
    MssqlLifecycleExecutionGuardrail::RecordBatchHandoffPolling,
];

const CREATE_AND_LOAD_EXECUTION_GUARDRAILS: &[MssqlLifecycleExecutionGuardrail] = &[
    MssqlLifecycleExecutionGuardrail::SqlServerConnectionAttempt,
    MssqlLifecycleExecutionGuardrail::TargetTableExistenceProbe,
    MssqlLifecycleExecutionGuardrail::CreateTableDdlExecution,
    MssqlLifecycleExecutionGuardrail::BulkWriterConstruction,
    MssqlLifecycleExecutionGuardrail::DeltaSourceScanExecution,
    MssqlLifecycleExecutionGuardrail::DataFusionPhysicalPlanExecution,
    MssqlLifecycleExecutionGuardrail::RecordBatchHandoffPolling,
];

const REPLACE_EXECUTION_GUARDRAILS: &[MssqlLifecycleExecutionGuardrail] = &[
    MssqlLifecycleExecutionGuardrail::SqlServerConnectionAttempt,
    MssqlLifecycleExecutionGuardrail::TargetTableExistenceProbe,
    MssqlLifecycleExecutionGuardrail::CreateTableDdlExecution,
    MssqlLifecycleExecutionGuardrail::BulkWriterConstruction,
    MssqlLifecycleExecutionGuardrail::DeltaSourceScanExecution,
    MssqlLifecycleExecutionGuardrail::DataFusionPhysicalPlanExecution,
    MssqlLifecycleExecutionGuardrail::RecordBatchHandoffPolling,
];

/// Plans lifecycle behavior for one selected output.
///
/// This function is deterministic and performs no I/O or execution. Errors return no
/// partial lifecycle plan, which keeps later execution code from accidentally
/// acting on a failed lifecycle decision.
pub fn plan_mssql_lifecycle(
    schema_plan: &MssqlSchemaPlan,
    ddl_plan: Option<&MssqlDdlPlan>,
) -> Result<MssqlLifecyclePlan, DeltaFunnelError> {
    let target = schema_plan.target();

    if let Some(ddl_plan) = ddl_plan {
        ensure_matching_target(target, ddl_plan.target())?;
    }

    match target.load_mode() {
        LoadMode::AppendExisting => plan_append_existing_lifecycle(target, ddl_plan),
        LoadMode::CreateAndLoad => plan_create_and_load_lifecycle(target, ddl_plan),
        LoadMode::Replace => plan_replace_lifecycle(target, ddl_plan),
    }
}

fn plan_append_existing_lifecycle(
    target: &MssqlTargetSummary,
    ddl_plan: Option<&MssqlDdlPlan>,
) -> Result<MssqlLifecyclePlan, DeltaFunnelError> {
    if ddl_plan.and_then(MssqlDdlPlan::create_table_sql).is_some() {
        return Err(lifecycle_error(
            target,
            "append-existing lifecycle must not carry create-table SQL",
        ));
    }

    Ok(MssqlLifecyclePlan {
        target: target.clone(),
        expected_target_state: MssqlTargetTableState::Exists,
        create_table_sql_required: false,
        create_table_sql_present: false,
        executable_in_mvp: true,
        guardrail_policy: MssqlLifecycleGuardrailPolicy::AllowedAfterPlanning,
        execution_guardrails: APPEND_EXISTING_EXECUTION_GUARDRAILS,
    })
}

fn plan_create_and_load_lifecycle(
    target: &MssqlTargetSummary,
    ddl_plan: Option<&MssqlDdlPlan>,
) -> Result<MssqlLifecyclePlan, DeltaFunnelError> {
    let create_table_sql_present = ddl_plan.and_then(MssqlDdlPlan::create_table_sql).is_some();
    if !create_table_sql_present {
        return Err(lifecycle_error(
            target,
            "create-and-load lifecycle requires planned create-table SQL",
        ));
    }

    Ok(MssqlLifecyclePlan {
        target: target.clone(),
        expected_target_state: MssqlTargetTableState::Absent,
        create_table_sql_required: true,
        create_table_sql_present,
        executable_in_mvp: true,
        guardrail_policy: MssqlLifecycleGuardrailPolicy::AllowedAfterPlanning,
        execution_guardrails: CREATE_AND_LOAD_EXECUTION_GUARDRAILS,
    })
}

fn plan_replace_lifecycle(
    target: &MssqlTargetSummary,
    ddl_plan: Option<&MssqlDdlPlan>,
) -> Result<MssqlLifecyclePlan, DeltaFunnelError> {
    let create_table_sql_present = ddl_plan
        .map(MssqlDdlPlan::create_table_sql_present)
        .unwrap_or(false);
    if !create_table_sql_present {
        return Err(lifecycle_error(
            target,
            "replace lifecycle requires planned create-table SQL",
        ));
    }

    Ok(MssqlLifecyclePlan {
        target: target.clone(),
        expected_target_state: MssqlTargetTableState::Exists,
        create_table_sql_required: true,
        create_table_sql_present,
        executable_in_mvp: true,
        guardrail_policy: MssqlLifecycleGuardrailPolicy::AllowedAfterPlanning,
        execution_guardrails: REPLACE_EXECUTION_GUARDRAILS,
    })
}

fn ensure_matching_target(
    schema_target: &MssqlTargetSummary,
    ddl_target: &MssqlTargetSummary,
) -> Result<(), DeltaFunnelError> {
    if schema_target.output_name() != ddl_target.output_name()
        || schema_target.table() != ddl_target.table()
        || schema_target.connection_source() != ddl_target.connection_source()
        || schema_target.connection() != ddl_target.connection()
    {
        return Err(lifecycle_error(
            schema_target,
            "schema plan and DDL plan targets must match",
        ));
    }

    Ok(())
}

fn lifecycle_error(target: &MssqlTargetSummary, message: impl Into<String>) -> DeltaFunnelError {
    DeltaFunnelError::MssqlLifecyclePlanning {
        output_name: target.output_name().to_owned(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use arrow_tiberius::PlanOptions;

    use super::*;
    use crate::{
        MssqlConnectionConfig, MssqlTargetConfig, MssqlTargetResolutionContext, MssqlTargetTable,
        plan_mssql_create_table_ddl, plan_mssql_output_schema,
    };

    fn secret_connection(label: &str) -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label(label))
    }

    fn schema_plan(
        output_name: &str,
        load_mode: LoadMode,
        table: MssqlTargetTable,
    ) -> Result<MssqlSchemaPlan, DeltaFunnelError> {
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

        plan_mssql_output_schema(&schema, &target, PlanOptions::default())
    }

    #[test]
    fn append_existing_reports_expected_existing_behavior() -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            "orders_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let ddl_plan = plan_mssql_create_table_ddl(&schema_plan)?;

        let lifecycle = plan_mssql_lifecycle(&schema_plan, Some(&ddl_plan))?;

        assert_eq!(lifecycle.target().output_name(), "orders_output");
        assert_eq!(
            lifecycle.expected_target_state(),
            MssqlTargetTableState::Exists
        );
        assert!(!lifecycle.create_table_sql_required());
        assert!(!lifecycle.create_table_sql_present());
        assert!(lifecycle.executable_in_mvp());
        assert_eq!(
            lifecycle.guardrail_policy(),
            MssqlLifecycleGuardrailPolicy::AllowedAfterPlanning
        );
        assert_eq!(
            lifecycle.execution_guardrails(),
            APPEND_EXISTING_EXECUTION_GUARDRAILS
        );
        Ok(())
    }

    #[test]
    fn create_and_load_reports_expected_absent_behavior() -> Result<(), DeltaFunnelError> {
        let primary_schema_plan = schema_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let ddl_plan = plan_mssql_create_table_ddl(&primary_schema_plan)?;

        let lifecycle = plan_mssql_lifecycle(&primary_schema_plan, Some(&ddl_plan))?;

        assert_eq!(
            lifecycle.expected_target_state(),
            MssqlTargetTableState::Absent
        );
        assert!(lifecycle.create_table_sql_required());
        assert!(lifecycle.create_table_sql_present());
        assert!(lifecycle.executable_in_mvp());
        assert_eq!(
            lifecycle.guardrail_policy(),
            MssqlLifecycleGuardrailPolicy::AllowedAfterPlanning
        );
        assert_eq!(
            lifecycle.execution_guardrails(),
            CREATE_AND_LOAD_EXECUTION_GUARDRAILS
        );
        Ok(())
    }

    #[test]
    fn lifecycle_reports_guarded_execution_by_load_mode() -> Result<(), DeltaFunnelError> {
        let append_schema_plan = schema_plan(
            "append_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let append_ddl_plan = plan_mssql_create_table_ddl(&append_schema_plan)?;
        let append_lifecycle = plan_mssql_lifecycle(&append_schema_plan, Some(&append_ddl_plan))?;

        assert_eq!(
            append_lifecycle.execution_guardrails(),
            &[
                MssqlLifecycleExecutionGuardrail::SqlServerConnectionAttempt,
                MssqlLifecycleExecutionGuardrail::TargetTableExistenceProbe,
                MssqlLifecycleExecutionGuardrail::BulkWriterConstruction,
                MssqlLifecycleExecutionGuardrail::DeltaSourceScanExecution,
                MssqlLifecycleExecutionGuardrail::DataFusionPhysicalPlanExecution,
                MssqlLifecycleExecutionGuardrail::RecordBatchHandoffPolling,
            ]
        );

        let create_schema_plan = schema_plan(
            "create_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders_archive")?,
        )?;
        let create_ddl_plan = plan_mssql_create_table_ddl(&create_schema_plan)?;
        let create_lifecycle = plan_mssql_lifecycle(&create_schema_plan, Some(&create_ddl_plan))?;

        assert_eq!(
            create_lifecycle.execution_guardrails(),
            &[
                MssqlLifecycleExecutionGuardrail::SqlServerConnectionAttempt,
                MssqlLifecycleExecutionGuardrail::TargetTableExistenceProbe,
                MssqlLifecycleExecutionGuardrail::CreateTableDdlExecution,
                MssqlLifecycleExecutionGuardrail::BulkWriterConstruction,
                MssqlLifecycleExecutionGuardrail::DeltaSourceScanExecution,
                MssqlLifecycleExecutionGuardrail::DataFusionPhysicalPlanExecution,
                MssqlLifecycleExecutionGuardrail::RecordBatchHandoffPolling,
            ]
        );
        Ok(())
    }

    #[test]
    fn create_and_load_requires_planned_create_table_sql() -> Result<(), DeltaFunnelError> {
        let primary_schema_plan = schema_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;

        let error = plan_mssql_lifecycle(&primary_schema_plan, None)
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected missing create-table SQL error".to_owned(),
            })?;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlLifecyclePlanning { .. }
        ));
        assert!(
            error
                .to_string()
                .contains("requires planned create-table SQL")
        );
        Ok(())
    }

    #[test]
    fn replace_reports_expected_existing_target_and_create_sql() -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let ddl_plan = plan_mssql_create_table_ddl(&schema_plan)?;

        let lifecycle = plan_mssql_lifecycle(&schema_plan, Some(&ddl_plan))?;

        assert_eq!(
            lifecycle.expected_target_state(),
            MssqlTargetTableState::Exists
        );
        assert!(lifecycle.create_table_sql_required());
        assert!(lifecycle.create_table_sql_present());
        assert!(lifecycle.executable_in_mvp());
        assert_eq!(
            lifecycle.guardrail_policy(),
            MssqlLifecycleGuardrailPolicy::AllowedAfterPlanning
        );
        assert_eq!(
            lifecycle.execution_guardrails(),
            REPLACE_EXECUTION_GUARDRAILS
        );
        Ok(())
    }

    #[test]
    fn replace_requires_planned_create_table_sql() -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;

        let error = plan_mssql_lifecycle(&schema_plan, None)
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected missing replace create-table SQL error".to_owned(),
            })?;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlLifecyclePlanning { .. }
        ));
        assert!(
            error
                .to_string()
                .contains("requires planned create-table SQL")
        );
        Ok(())
    }

    #[test]
    fn lifecycle_failures_block_guarded_execution() -> Result<(), DeltaFunnelError> {
        let missing_create_sql_plan = schema_plan(
            "create_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders_archive")?,
        )?;
        let append_plan = schema_plan(
            "append_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders_existing")?,
        )?;
        let create_plan = schema_plan(
            "append_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders_existing")?,
        )?;
        let contradictory_ddl_plan = plan_mssql_create_table_ddl(&create_plan)?;

        let cases = [
            (
                "missing create-table SQL",
                plan_mssql_lifecycle(&missing_create_sql_plan, None),
            ),
            (
                "append-existing create-table SQL contradiction",
                plan_mssql_lifecycle(&append_plan, Some(&contradictory_ddl_plan)),
            ),
        ];

        for (case_name, result) in cases {
            let mut attempted_execution = Vec::new();
            let error = match result {
                Ok(_lifecycle) => {
                    fake_guarded_execution(ALL_GUARDED_EXECUTION, &mut attempted_execution);
                    return Err(DeltaFunnelError::Config {
                        message: format!("expected {case_name} lifecycle error"),
                    });
                }
                Err(error) => error,
            };

            assert!(matches!(
                error,
                DeltaFunnelError::MssqlLifecyclePlanning { .. }
            ));
            assert!(attempted_execution.is_empty());
        }

        Ok(())
    }

    #[test]
    fn append_existing_rejects_create_table_sql_contradiction() -> Result<(), DeltaFunnelError> {
        let append_plan = schema_plan(
            "orders_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let create_plan = schema_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let ddl_plan = plan_mssql_create_table_ddl(&create_plan)?;

        let error = plan_mssql_lifecycle(&append_plan, Some(&ddl_plan))
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected append/create SQL contradiction".to_owned(),
            })?;

        assert!(
            error
                .to_string()
                .contains("must not carry create-table SQL")
        );
        Ok(())
    }

    #[test]
    fn mismatched_schema_and_ddl_targets_are_rejected() -> Result<(), DeltaFunnelError> {
        let primary_schema_plan = schema_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let other_schema_plan = schema_plan(
            "other_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "other_orders")?,
        )?;
        let ddl_plan = plan_mssql_create_table_ddl(&other_schema_plan)?;

        let error = plan_mssql_lifecycle(&primary_schema_plan, Some(&ddl_plan))
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected target mismatch error".to_owned(),
            })?;

        assert!(error.to_string().contains("targets must match"));
        Ok(())
    }

    #[test]
    fn reports_and_errors_do_not_expose_connection_secrets() -> Result<(), DeltaFunnelError> {
        let create_schema_plan = schema_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let ddl_plan = plan_mssql_create_table_ddl(&create_schema_plan)?;
        let lifecycle = plan_mssql_lifecycle(&create_schema_plan, Some(&ddl_plan))?;

        let combined = format!("{lifecycle:?}");
        assert!(!combined.contains("secret-token"));
        assert!(!combined.contains("password"));
        assert!(!combined.contains("server=tcp"));
        assert!(combined.contains("warehouse-primary"));
        Ok(())
    }

    const ALL_GUARDED_EXECUTION: &[MssqlLifecycleExecutionGuardrail] = &[
        MssqlLifecycleExecutionGuardrail::DeltaSourceScanExecution,
        MssqlLifecycleExecutionGuardrail::DataFusionPhysicalPlanExecution,
        MssqlLifecycleExecutionGuardrail::SqlServerConnectionAttempt,
        MssqlLifecycleExecutionGuardrail::TargetTableExistenceProbe,
        MssqlLifecycleExecutionGuardrail::CreateTableDdlExecution,
        MssqlLifecycleExecutionGuardrail::BulkWriterConstruction,
        MssqlLifecycleExecutionGuardrail::RecordBatchHandoffPolling,
    ];

    fn fake_guarded_execution(
        guardrails: &[MssqlLifecycleExecutionGuardrail],
        attempted_execution: &mut Vec<MssqlLifecycleExecutionGuardrail>,
    ) {
        attempted_execution.extend(guardrails.iter().copied());
    }
}
