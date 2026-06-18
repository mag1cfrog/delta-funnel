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

/// Whether SQL Server side effects may proceed after planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MssqlLifecycleSideEffectPolicy {
    /// Planning succeeded and later execution phases may perform their work.
    AllowedAfterPlanning,
    /// Planning failed, so later execution phases must not perform side effects.
    ForbiddenAfterPlanningFailure,
}

/// Planned SQL Server table lifecycle behavior for one selected output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlLifecyclePlan {
    target: MssqlTargetSummary,
    expected_target_state: MssqlTargetTableState,
    create_table_sql_required: bool,
    create_table_sql_present: bool,
    executable_in_mvp: bool,
    side_effect_policy: MssqlLifecycleSideEffectPolicy,
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

    /// Returns whether later side-effect phases may proceed.
    #[must_use]
    pub const fn side_effect_policy(&self) -> MssqlLifecycleSideEffectPolicy {
        self.side_effect_policy
    }
}

/// Plans lifecycle behavior for one selected output.
///
/// This function is deterministic and side-effect free. Errors return no
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
        LoadMode::Replace => Err(lifecycle_error(
            target,
            "replace load mode is reserved and cannot be planned for execution",
        )),
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
        side_effect_policy: MssqlLifecycleSideEffectPolicy::AllowedAfterPlanning,
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
        side_effect_policy: MssqlLifecycleSideEffectPolicy::AllowedAfterPlanning,
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
            lifecycle.side_effect_policy(),
            MssqlLifecycleSideEffectPolicy::AllowedAfterPlanning
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
            lifecycle.side_effect_policy(),
            MssqlLifecycleSideEffectPolicy::AllowedAfterPlanning
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
    fn replace_is_rejected_without_lifecycle_artifact() -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;

        let error = plan_mssql_lifecycle(&schema_plan, None)
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected replace lifecycle error".to_owned(),
            })?;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlLifecyclePlanning { .. }
        ));
        assert!(error.to_string().contains("replace load mode is reserved"));
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
        let replace_plan = schema_plan(
            "replace_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;
        let error = plan_mssql_lifecycle(&replace_plan, None)
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected replace lifecycle error".to_owned(),
            })?;

        let combined = format!("{lifecycle:?}\n{error}");
        assert!(!combined.contains("secret-token"));
        assert!(!combined.contains("password"));
        assert!(!combined.contains("server=tcp"));
        assert!(combined.contains("warehouse-primary"));
        Ok(())
    }
}
