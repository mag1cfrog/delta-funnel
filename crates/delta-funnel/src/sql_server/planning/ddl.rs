//! SQL Server create-table DDL planning through arrow-tiberius.

use arrow_tiberius::create_table_sql_from_mappings;

use crate::DeltaFunnelError;

use super::{LoadMode, MssqlSchemaPlan, MssqlTargetSummary, table_name_from_target};

/// Planned SQL Server DDL artifacts for one selected output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlDdlPlan {
    target: MssqlTargetSummary,
    create_table_sql_present: bool,
    create_table_sql: Option<String>,
}

impl MssqlDdlPlan {
    /// Returns the redacted resolved target summary.
    #[must_use]
    pub fn target(&self) -> &MssqlTargetSummary {
        &self.target
    }

    /// Returns true when create-table SQL is planned for the lifecycle.
    #[must_use]
    pub const fn create_table_sql_present(&self) -> bool {
        self.create_table_sql_present
    }

    /// Returns concrete planned `CREATE TABLE` SQL when the target name is known at planning time.
    ///
    /// Replace mode chooses its collision-free staging table during execution, so
    /// the plan records that create-table SQL is present without exposing final-target SQL.
    #[must_use]
    pub fn create_table_sql(&self) -> Option<&str> {
        self.create_table_sql.as_deref()
    }
}

/// Plans optional create-table DDL for one selected output schema plan.
///
/// This function is side-effect free. It validates target identifiers through
/// arrow-tiberius and delegates SQL rendering to arrow-tiberius.
pub fn plan_mssql_create_table_ddl(
    schema_plan: &MssqlSchemaPlan,
) -> Result<MssqlDdlPlan, DeltaFunnelError> {
    let target = schema_plan.target();
    let (create_table_sql_present, create_table_sql) = match target.load_mode() {
        LoadMode::AppendExisting => (false, None),
        LoadMode::CreateAndLoad => {
            if schema_plan.mappings().is_empty() {
                return Err(DeltaFunnelError::MssqlDdlPlanning {
                    output_name: target.output_name().to_owned(),
                    message: "create-table DDL requires at least one schema mapping".to_owned(),
                });
            }

            let table = table_name_from_target(target.output_name(), target.table())?;
            (
                true,
                Some(create_table_sql_from_mappings(
                    &table,
                    schema_plan.mappings(),
                )),
            )
        }
        LoadMode::Replace => {
            if schema_plan.mappings().is_empty() {
                return Err(DeltaFunnelError::MssqlDdlPlanning {
                    output_name: target.output_name().to_owned(),
                    message: "create-table DDL requires at least one schema mapping".to_owned(),
                });
            }

            table_name_from_target(target.output_name(), target.table())?;
            (true, None)
        }
    };

    Ok(MssqlDdlPlan {
        target: target.clone(),
        create_table_sql_present,
        create_table_sql,
    })
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use arrow_tiberius::PlanOptions;

    use super::*;
    use crate::{
        MssqlConnectionConfig, MssqlTargetConfig, MssqlTargetResolutionContext, MssqlTargetTable,
        plan_mssql_output_schema,
    };

    fn secret_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse-primary"))
    }

    fn schema_plan(
        load_mode: LoadMode,
        table: MssqlTargetTable,
        schema: Schema,
    ) -> Result<MssqlSchemaPlan, DeltaFunnelError> {
        let connection = secret_connection()?;
        let target = MssqlTargetConfig::new(table)
            .with_load_mode(load_mode)
            .resolve(MssqlTargetResolutionContext {
                output_name: Some("orders_output"),
                default_connection: Some(&connection),
            })?;

        plan_mssql_output_schema(&schema, &target, PlanOptions::default())
    }

    fn orders_schema() -> Schema {
        Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
        ])
    }

    #[test]
    fn create_and_load_produces_create_table_sql_through_arrow_tiberius()
    -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders")?,
            orders_schema(),
        )?;

        let ddl_plan = plan_mssql_create_table_ddl(&schema_plan)?;

        assert_eq!(ddl_plan.target().output_name(), "orders_output");
        assert!(ddl_plan.create_table_sql_present());
        assert_eq!(
            ddl_plan.create_table_sql(),
            Some(
                "CREATE TABLE [dbo].[orders] (\n    [order_id] bigint NOT NULL,\n    [status] nvarchar(max) NULL\n);"
            )
        );
        Ok(())
    }

    #[test]
    fn append_existing_leaves_create_table_sql_absent() -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            LoadMode::AppendExisting,
            MssqlTargetTable::new("dbo", "orders")?,
            orders_schema(),
        )?;

        let ddl_plan = plan_mssql_create_table_ddl(&schema_plan)?;

        assert!(!ddl_plan.create_table_sql_present());
        assert_eq!(ddl_plan.create_table_sql(), None);
        Ok(())
    }

    #[test]
    fn unqualified_target_table_is_quoted_by_arrow_tiberius() -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            LoadMode::CreateAndLoad,
            MssqlTargetTable::unqualified("orders")?,
            orders_schema(),
        )?;

        let ddl_plan = plan_mssql_create_table_ddl(&schema_plan)?;

        assert!(
            ddl_plan
                .create_table_sql()
                .unwrap_or_default()
                .starts_with("CREATE TABLE [orders]")
        );
        Ok(())
    }

    #[test]
    fn reserved_and_special_identifiers_follow_arrow_tiberius_quoting()
    -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("select", "order]items")?,
            orders_schema(),
        )?;

        let ddl_plan = plan_mssql_create_table_ddl(&schema_plan)?;

        assert!(
            ddl_plan
                .create_table_sql()
                .unwrap_or_default()
                .starts_with("CREATE TABLE [select].[order]]items]")
        );
        Ok(())
    }

    #[test]
    fn invalid_target_identifiers_fail_before_sql_execution() -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders\narchive")?,
            orders_schema(),
        )?;

        let error = plan_mssql_create_table_ddl(&schema_plan)
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected identifier error".to_owned(),
            })?;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlDdlTargetIdentifier { .. }
        ));
        let display = error.to_string();
        assert!(!display.contains('\n'));
        assert!(display.contains("control characters"));
        Ok(())
    }

    #[test]
    fn replace_load_mode_records_create_table_requirement_without_final_target_sql()
    -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            LoadMode::Replace,
            MssqlTargetTable::new("dbo", "orders")?,
            orders_schema(),
        )?;

        let ddl_plan = plan_mssql_create_table_ddl(&schema_plan)?;

        assert!(ddl_plan.create_table_sql_present());
        assert_eq!(ddl_plan.create_table_sql(), None);
        Ok(())
    }

    #[test]
    fn errors_and_reports_do_not_expose_connection_secrets() -> Result<(), DeltaFunnelError> {
        let schema_plan = schema_plan(
            LoadMode::CreateAndLoad,
            MssqlTargetTable::new("dbo", "orders\narchive")?,
            orders_schema(),
        )?;

        let debug = format!("{schema_plan:?}");
        let error = plan_mssql_create_table_ddl(&schema_plan)
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected identifier error".to_owned(),
            })?;
        let combined = format!("{debug}\n{error}");

        assert!(!combined.contains("secret-token"));
        assert!(!combined.contains("password"));
        assert!(!combined.contains("server=tcp"));
        assert!(combined.contains("warehouse-primary"));
        Ok(())
    }
}
