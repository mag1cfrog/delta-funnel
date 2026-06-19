//! SQL Server target output planning.

use arrow_schema::Schema;
use arrow_tiberius::SchemaMapping;

use crate::DeltaFunnelError;

use super::{
    LoadMode, MssqlConnectionConfig, MssqlConnectionSource, MssqlConnectionSummary, MssqlDdlPlan,
    MssqlLifecyclePlan, MssqlSchemaDiagnostic, MssqlSchemaPlan, MssqlSchemaPlanOptions,
    MssqlTargetConfig, MssqlTargetResolutionContext, MssqlTargetSummary, MssqlTargetTable,
    plan_mssql_create_table_ddl, plan_mssql_lifecycle, plan_mssql_output_schema,
};

/// Complete SQL Server target planning artifact for one selected output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlTargetOutputPlan {
    schema_plan: MssqlSchemaPlan,
    ddl_plan: MssqlDdlPlan,
    lifecycle_plan: MssqlLifecyclePlan,
}

impl MssqlTargetOutputPlan {
    /// Returns the redacted resolved target summary.
    #[must_use]
    pub fn target(&self) -> &MssqlTargetSummary {
        self.schema_plan.target()
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        self.target().output_name()
    }

    /// Returns the effective target table.
    #[must_use]
    pub fn target_table(&self) -> &MssqlTargetTable {
        self.target().table()
    }

    /// Returns the requested target lifecycle mode.
    #[must_use]
    pub fn load_mode(&self) -> LoadMode {
        self.target().load_mode()
    }

    /// Returns where the effective connection came from.
    #[must_use]
    pub fn connection_source(&self) -> MssqlConnectionSource {
        self.target().connection_source()
    }

    /// Returns the redacted effective connection summary.
    #[must_use]
    pub fn connection(&self) -> &MssqlConnectionSummary {
        self.target().connection()
    }

    /// Returns the planned Arrow-to-MSSQL schema mapping.
    #[must_use]
    pub fn schema_plan(&self) -> &MssqlSchemaPlan {
        &self.schema_plan
    }

    /// Returns the Arrow-to-MSSQL planning options used for this output.
    #[must_use]
    pub fn schema_plan_options(&self) -> MssqlSchemaPlanOptions {
        self.schema_plan.plan_options()
    }

    /// Returns planned Arrow-to-MSSQL column mappings in output field order.
    #[must_use]
    pub fn schema_mappings(&self) -> &[SchemaMapping] {
        self.schema_plan.mappings()
    }

    /// Returns structured schema diagnostic report entries.
    #[must_use]
    pub fn schema_diagnostics(&self) -> &[MssqlSchemaDiagnostic] {
        self.schema_plan.diagnostic_reports()
    }

    /// Returns the planned SQL Server DDL artifact.
    #[must_use]
    pub fn ddl_plan(&self) -> &MssqlDdlPlan {
        &self.ddl_plan
    }

    /// Returns planned `CREATE TABLE` SQL when required by the lifecycle mode.
    #[must_use]
    pub fn create_table_sql(&self) -> Option<&str> {
        self.ddl_plan.create_table_sql()
    }

    /// Returns the planned SQL Server lifecycle report.
    #[must_use]
    pub fn lifecycle_plan(&self) -> &MssqlLifecyclePlan {
        &self.lifecycle_plan
    }
}

/// Plans one selected output schema for SQL Server writing.
///
/// This is the high-level per-output target planning API. It resolves the
/// target configuration, plans Arrow-to-MSSQL schema mappings, plans optional
/// create-table DDL, and attaches the lifecycle report. It performs no SQL
/// Server I/O, DataFusion execution, row reads, or batch handoff polling.
pub fn plan_mssql_target_for_output(
    output_schema: impl AsRef<Schema>,
    output_name: &str,
    target_config: &MssqlTargetConfig,
    default_connection: Option<&MssqlConnectionConfig>,
    options: MssqlSchemaPlanOptions,
) -> Result<MssqlTargetOutputPlan, DeltaFunnelError> {
    let target = target_config.resolve(MssqlTargetResolutionContext {
        output_name: Some(output_name),
        default_connection,
    })?;
    let schema_plan = plan_mssql_output_schema(output_schema, &target, options)?;

    plan_mssql_target_output(schema_plan)
}

/// Builds a complete target output plan from an existing schema plan.
///
/// This function only composes planning reports. It performs no I/O or
/// execution, and it returns no partial target output plan on failure.
pub fn plan_mssql_target_output(
    schema_plan: MssqlSchemaPlan,
) -> Result<MssqlTargetOutputPlan, DeltaFunnelError> {
    if schema_plan.target().load_mode() == LoadMode::Replace {
        plan_mssql_lifecycle(&schema_plan, None)?;
        return Err(DeltaFunnelError::MssqlLifecyclePlanning {
            output_name: schema_plan.target().output_name().to_owned(),
            message: "replace load mode is reserved and cannot produce a target output plan"
                .to_owned(),
        });
    }

    let ddl_plan = plan_mssql_create_table_ddl(&schema_plan)?;
    let lifecycle_plan = plan_mssql_lifecycle(&schema_plan, Some(&ddl_plan))?;

    Ok(MssqlTargetOutputPlan {
        schema_plan,
        ddl_plan,
        lifecycle_plan,
    })
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use arrow_tiberius::{DiagnosticCode, DiagnosticSeverity, PlanOptions};

    use super::*;
    use crate::{
        MssqlConnectionConfig, MssqlConnectionSource, MssqlLifecycleExecutionGuardrail,
        MssqlTargetConfig, MssqlTargetResolutionContext, MssqlTargetTable, MssqlTargetTableState,
        plan_mssql_output_schema,
    };

    fn secret_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse-primary"))
    }

    fn schema_plan(
        output_name: &str,
        load_mode: LoadMode,
        table: MssqlTargetTable,
    ) -> Result<MssqlSchemaPlan, DeltaFunnelError> {
        let connection = secret_connection()?;
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

    fn orders_schema() -> Schema {
        Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
        ])
    }

    fn west_output_schema() -> Schema {
        Schema::new(vec![
            Field::new("west_order_id", DataType::Int64, false),
            Field::new("west_customer", DataType::Utf8, true),
        ])
    }

    fn east_output_schema() -> Schema {
        Schema::new(vec![
            Field::new("east_order_id", DataType::Int64, false),
            Field::new("east_total", DataType::Float64, true),
        ])
    }

    fn duplicate_field_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("id", DataType::Utf8, true),
        ])
    }

    fn unsupported_field_schema() -> Schema {
        Schema::new(vec![Field::new(
            "items",
            DataType::new_list(DataType::Int64, true),
            true,
        )])
    }

    #[test]
    fn high_level_api_plans_append_existing_output() -> Result<(), DeltaFunnelError> {
        let default_connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);

        let output_plan = plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            Some(&default_connection),
            PlanOptions::default(),
        )?;

        assert_eq!(output_plan.target().output_name(), "orders_output");
        assert_eq!(output_plan.target().table().schema(), Some("dbo"));
        assert_eq!(output_plan.target().table().table(), "orders");
        assert_eq!(output_plan.target().load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            output_plan.target().connection_source(),
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(output_plan.ddl_plan().create_table_sql(), None);
        assert_eq!(
            output_plan.lifecycle_plan().expected_target_state(),
            MssqlTargetTableState::Exists
        );
        Ok(())
    }

    #[test]
    fn high_level_api_plans_create_and_load_output() -> Result<(), DeltaFunnelError> {
        let default_connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_load_mode(LoadMode::CreateAndLoad);

        let output_plan = plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            Some(&default_connection),
            PlanOptions::default(),
        )?;

        assert_eq!(output_plan.target().load_mode(), LoadMode::CreateAndLoad);
        assert!(
            output_plan
                .ddl_plan()
                .create_table_sql()
                .unwrap_or_default()
                .starts_with("CREATE TABLE [dbo].[orders]")
        );
        assert_eq!(
            output_plan.lifecycle_plan().expected_target_state(),
            MssqlTargetTableState::Absent
        );
        Ok(())
    }

    #[test]
    fn composed_report_exposes_structured_python_ready_fields() -> Result<(), DeltaFunnelError> {
        let default_connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_load_mode(LoadMode::CreateAndLoad);

        let output_plan = plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            Some(&default_connection),
            PlanOptions::default(),
        )?;

        assert_eq!(output_plan.output_name(), "orders_output");
        assert_eq!(output_plan.target_table().schema(), Some("dbo"));
        assert_eq!(output_plan.target_table().table(), "orders");
        assert_eq!(output_plan.load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(
            output_plan.connection_source(),
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(
            output_plan.connection().display_label(),
            Some("warehouse-primary")
        );
        assert_eq!(output_plan.schema_mappings()[0].arrow().name(), "order_id");
        assert_eq!(output_plan.schema_mappings()[1].arrow().name(), "status");
        assert_eq!(output_plan.schema_plan_options(), PlanOptions::default());
        assert!(output_plan.schema_diagnostics().is_empty());
        assert!(
            output_plan
                .create_table_sql()
                .unwrap_or_default()
                .starts_with("CREATE TABLE [dbo].[orders]")
        );
        assert_eq!(
            output_plan.lifecycle_plan().expected_target_state(),
            MssqlTargetTableState::Absent
        );
        Ok(())
    }

    #[test]
    fn output_plan_exposes_schema_plan_options() -> Result<(), DeltaFunnelError> {
        let default_connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);
        let options = PlanOptions {
            string_policy: arrow_tiberius::StringPolicy::NVarChar(128),
            ..PlanOptions::default()
        };

        let output_plan = plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            Some(&default_connection),
            options,
        )?;

        assert_eq!(output_plan.schema_plan_options(), options);
        Ok(())
    }

    #[test]
    fn high_level_api_returns_structured_missing_connection_error() -> Result<(), DeltaFunnelError>
    {
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);

        let error = plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            None,
            PlanOptions::default(),
        )
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected missing connection error".to_owned(),
        })?;

        assert!(matches!(
            error,
            DeltaFunnelError::MissingMssqlConnection { .. }
        ));
        Ok(())
    }

    #[test]
    fn high_level_api_preserves_duplicate_field_error() -> Result<(), DeltaFunnelError> {
        let default_connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);

        let error = plan_mssql_target_for_output(
            duplicate_field_schema(),
            "orders_output",
            &target_config,
            Some(&default_connection),
            PlanOptions::default(),
        )
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected duplicate field error".to_owned(),
        })?;

        assert!(matches!(
            error,
            DeltaFunnelError::DuplicateMssqlOutputField {
                output_name,
                field_name,
                first_index: 0,
                duplicate_index: 1,
            } if output_name == "orders_output" && field_name == "id"
        ));
        Ok(())
    }

    #[test]
    fn high_level_api_preserves_structured_schema_diagnostics() -> Result<(), DeltaFunnelError> {
        let default_connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);

        let error = plan_mssql_target_for_output(
            unsupported_field_schema(),
            "orders_output",
            &target_config,
            Some(&default_connection),
            PlanOptions::default(),
        )
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected schema planning error".to_owned(),
        })?;

        let DeltaFunnelError::MssqlSchemaPlanning {
            output_name,
            diagnostics,
        } = error
        else {
            return Err(DeltaFunnelError::Config {
                message: "expected MssqlSchemaPlanning error".to_owned(),
            });
        };

        assert_eq!(output_name, "orders_output");
        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.severity(), DiagnosticSeverity::Error);
        assert_eq!(diagnostic.code(), DiagnosticCode::UnsupportedArrowType);
        let field = diagnostic.field().ok_or_else(|| DeltaFunnelError::Config {
            message: "expected field diagnostic context".to_owned(),
        })?;
        assert_eq!(field.index(), 0);
        assert_eq!(field.name(), "items");
        Ok(())
    }

    #[test]
    fn high_level_api_plans_outputs_independently() -> Result<(), DeltaFunnelError> {
        let default_connection = secret_connection()?.with_display_label("context-default");
        let east_connection = MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=east;user=admin;password=east-secret",
        )?
        .with_display_label("east-override");
        let west_target = MssqlTargetConfig::new(MssqlTargetTable::new("reporting", "west")?);
        let east_target = MssqlTargetConfig::new(MssqlTargetTable::new("warehouse", "east")?)
            .with_load_mode(LoadMode::CreateAndLoad)
            .with_connection(east_connection);

        let west_plan = plan_mssql_target_for_output(
            west_output_schema(),
            "west_output",
            &west_target,
            Some(&default_connection),
            PlanOptions::default(),
        )?;
        let east_plan = plan_mssql_target_for_output(
            east_output_schema(),
            "east_output",
            &east_target,
            Some(&default_connection),
            PlanOptions::default(),
        )?;

        assert_eq!(west_plan.target().output_name(), "west_output");
        assert_eq!(west_plan.target().table().schema(), Some("reporting"));
        assert_eq!(west_plan.target().table().table(), "west");
        assert_eq!(west_plan.target().load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            west_plan.target().connection_source(),
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(west_plan.ddl_plan().create_table_sql(), None);
        assert_eq!(
            west_plan.schema_mappings()[0].arrow().name(),
            "west_order_id"
        );
        assert_eq!(
            west_plan.schema_mappings()[1].arrow().name(),
            "west_customer"
        );

        assert_eq!(east_plan.target().output_name(), "east_output");
        assert_eq!(east_plan.target().table().schema(), Some("warehouse"));
        assert_eq!(east_plan.target().table().table(), "east");
        assert_eq!(east_plan.target().load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(
            east_plan.target().connection_source(),
            MssqlConnectionSource::TargetOverride
        );
        assert!(
            east_plan
                .create_table_sql()
                .unwrap_or_default()
                .starts_with("CREATE TABLE [warehouse].[east]")
        );
        assert_eq!(
            east_plan.schema_mappings()[0].arrow().name(),
            "east_order_id"
        );
        assert_eq!(east_plan.schema_mappings()[1].arrow().name(), "east_total");

        let debug = format!("{west_plan:?}\n{east_plan:?}");
        assert!(debug.contains("context-default"));
        assert!(debug.contains("east-override"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("east-secret"));
        assert!(!debug.contains("password"));
        Ok(())
    }

    #[test]
    fn append_existing_output_plan_includes_lifecycle_report() -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            "orders_output",
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;

        let output_plan = plan_mssql_target_output(schema_plan)?;

        assert_eq!(
            output_plan.schema_plan().target().output_name(),
            "orders_output"
        );
        assert_eq!(output_plan.ddl_plan().create_table_sql(), None);
        assert_eq!(
            output_plan.lifecycle_plan().expected_target_state(),
            MssqlTargetTableState::Exists
        );
        assert!(output_plan.lifecycle_plan().executable_in_mvp());
        Ok(())
    }

    #[test]
    fn create_and_load_output_plan_includes_ddl_and_lifecycle_report()
    -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;

        let output_plan = plan_mssql_target_output(schema_plan)?;

        assert!(
            output_plan
                .ddl_plan()
                .create_table_sql()
                .unwrap_or_default()
                .starts_with("CREATE TABLE [dbo].[orders]")
        );
        assert_eq!(
            output_plan.lifecycle_plan().expected_target_state(),
            MssqlTargetTableState::Absent
        );
        assert!(
            output_plan
                .lifecycle_plan()
                .execution_guardrails()
                .contains(&MssqlLifecycleExecutionGuardrail::CreateTableDdlExecution)
        );
        Ok(())
    }

    #[test]
    fn replace_is_rejected_by_lifecycle_before_target_output_plan() -> Result<(), DeltaFunnelError>
    {
        let schema_plan = schema_plan(
            "orders_output",
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;

        let error = plan_mssql_target_output(schema_plan).err().ok_or_else(|| {
            DeltaFunnelError::Config {
                message: "expected replace output planning error".to_owned(),
            }
        })?;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlLifecyclePlanning { .. }
        ));
        assert!(error.to_string().contains("replace load mode is reserved"));
        Ok(())
    }

    #[test]
    fn output_plan_reports_do_not_expose_connection_secrets() -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            "orders_output",
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
        )?;

        let output_plan = plan_mssql_target_output(schema_plan)?;
        let debug = format!("{output_plan:?}");

        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        assert!(debug.contains("warehouse-primary"));
        Ok(())
    }
}
