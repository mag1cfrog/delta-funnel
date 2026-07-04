//! SQL Server schema planning through arrow-tiberius.

use std::collections::{HashMap, hash_map::Entry};

use arrow_schema::Schema;
use arrow_tiberius::{
    Diagnostic, DiagnosticCode, DiagnosticSet, DiagnosticSeverity, MssqlProfile, SchemaMapping,
    plan_arrow_schema_to_mssql_mappings,
};

pub use arrow_tiberius::{
    BinaryPolicy as MssqlBinaryPolicy, Date64Policy as MssqlDate64Policy,
    Decimal256Policy as MssqlDecimal256Policy, DecimalPolicy as MssqlDecimalPolicy,
    FloatPolicy as MssqlFloatPolicy, NanosecondPolicy as MssqlNanosecondPolicy,
    PlanOptions as MssqlSchemaPlanOptions, StringPolicy as MssqlStringPolicy,
    TimestampPolicy as MssqlTimestampPolicy, TimezonePolicy as MssqlTimezonePolicy,
    UInt64Policy as MssqlUInt64Policy,
};

use crate::{
    DeltaFunnelError,
    error::{
        DuplicateMssqlOutputFieldSnafu, InvalidMssqlOutputIdentitySnafu, MssqlSchemaPlanningSnafu,
    },
};

use super::{MssqlTargetSummary, ResolvedMssqlTarget};

/// Planned SQL Server schema mapping for one selected output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlSchemaPlan {
    target: MssqlTargetSummary,
    plan_options: MssqlSchemaPlanOptions,
    mappings: Vec<SchemaMapping>,
    diagnostics: DiagnosticSet,
    diagnostic_reports: Vec<MssqlSchemaDiagnostic>,
}

impl MssqlSchemaPlan {
    /// Returns the redacted resolved target summary.
    #[must_use]
    pub fn target(&self) -> &MssqlTargetSummary {
        &self.target
    }

    /// Returns the Arrow-to-MSSQL planning options used for this plan.
    #[must_use]
    pub const fn plan_options(&self) -> MssqlSchemaPlanOptions {
        self.plan_options
    }

    /// Returns planned Arrow-to-MSSQL column mappings in output field order.
    #[must_use]
    pub fn mappings(&self) -> &[SchemaMapping] {
        &self.mappings
    }

    /// Returns non-fatal diagnostics returned by arrow-tiberius.
    #[must_use]
    pub fn diagnostics(&self) -> &DiagnosticSet {
        &self.diagnostics
    }

    /// Returns DeltaFunnel-owned diagnostic report entries.
    #[must_use]
    pub fn diagnostic_reports(&self) -> &[MssqlSchemaDiagnostic] {
        &self.diagnostic_reports
    }
}

/// DeltaFunnel-owned schema planning diagnostic entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlSchemaDiagnostic {
    output_name: String,
    severity: DiagnosticSeverity,
    code: DiagnosticCode,
    message: String,
    field: Option<MssqlSchemaDiagnosticField>,
    row: Option<usize>,
}

impl MssqlSchemaDiagnostic {
    fn from_arrow_tiberius(output_name: &str, diagnostic: &Diagnostic) -> Self {
        Self {
            output_name: output_name.to_owned(),
            severity: diagnostic.severity(),
            code: diagnostic.code(),
            message: diagnostic.message().to_owned(),
            field: diagnostic.field().map(MssqlSchemaDiagnosticField::from),
            row: diagnostic.row(),
        }
    }

    /// Returns the selected output name associated with this diagnostic.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns diagnostic severity.
    #[must_use]
    pub const fn severity(&self) -> DiagnosticSeverity {
        self.severity
    }

    /// Returns machine-readable diagnostic code.
    #[must_use]
    pub const fn code(&self) -> DiagnosticCode {
        self.code
    }

    /// Returns the diagnostic message from arrow-tiberius.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns optional field context.
    #[must_use]
    pub fn field(&self) -> Option<&MssqlSchemaDiagnosticField> {
        self.field.as_ref()
    }

    /// Returns optional row context.
    #[must_use]
    pub const fn row(&self) -> Option<usize> {
        self.row
    }
}

/// Field context for a DeltaFunnel-owned schema planning diagnostic entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlSchemaDiagnosticField {
    index: usize,
    name: String,
}

impl MssqlSchemaDiagnosticField {
    /// Returns the output field index.
    #[must_use]
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Returns the output field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl From<&arrow_tiberius::FieldRef> for MssqlSchemaDiagnosticField {
    fn from(field: &arrow_tiberius::FieldRef) -> Self {
        Self {
            index: field.index(),
            name: field.name().to_owned(),
        }
    }
}

/// Plans one selected output Arrow schema for the resolved SQL Server target.
///
/// This function owns DeltaFunnel orchestration concerns only. Arrow-to-MSSQL
/// type mapping and identifier validation are delegated to arrow-tiberius.
/// Callers may pass an Arrow `Schema`, `SchemaRef`, or borrowed schema.
pub fn plan_mssql_output_schema(
    schema: impl AsRef<Schema>,
    target: &ResolvedMssqlTarget,
    options: MssqlSchemaPlanOptions,
) -> Result<MssqlSchemaPlan, DeltaFunnelError> {
    let schema = schema.as_ref();
    validate_output_identity(target.output_name())?;
    validate_unique_output_field_names(target.output_name(), schema)?;

    let outcome = plan_arrow_schema_to_mssql_mappings(
        schema,
        MssqlProfile::sql_server_2016_compat_100(),
        options,
    )
    .map_err(|source| match source {
        arrow_tiberius::Error::Planning { diagnostics } => MssqlSchemaPlanningSnafu {
            output_name: target.output_name().to_owned(),
            diagnostics,
        }
        .build(),
        source => DeltaFunnelError::MssqlSchemaPlanningFailed {
            output_name: target.output_name().to_owned(),
            source,
        },
    })?;

    let (mappings, diagnostics) = outcome.into_parts();
    let diagnostic_reports = mssql_schema_diagnostic_reports(target.output_name(), &diagnostics);

    Ok(MssqlSchemaPlan {
        target: target.summary(),
        plan_options: options,
        mappings,
        diagnostics,
        diagnostic_reports,
    })
}

/// Converts arrow-tiberius diagnostics into DeltaFunnel-owned report entries.
#[must_use]
pub fn mssql_schema_diagnostic_reports(
    output_name: &str,
    diagnostics: &DiagnosticSet,
) -> Vec<MssqlSchemaDiagnostic> {
    diagnostics
        .all()
        .iter()
        .map(|diagnostic| MssqlSchemaDiagnostic::from_arrow_tiberius(output_name, diagnostic))
        .collect()
}

fn validate_output_identity(output_name: &str) -> Result<(), DeltaFunnelError> {
    if output_name.trim().is_empty() || output_name == "<unnamed>" {
        return InvalidMssqlOutputIdentitySnafu {
            output_name: output_name.to_owned(),
            reason: "selected output name must be provided",
        }
        .fail();
    }

    Ok(())
}

fn validate_unique_output_field_names(
    output_name: &str,
    schema: &Schema,
) -> Result<(), DeltaFunnelError> {
    let mut first_indexes = HashMap::with_capacity(schema.fields().len());

    for (index, field) in schema.fields().iter().enumerate() {
        match first_indexes.entry(field.name().as_str()) {
            Entry::Occupied(entry) => {
                return DuplicateMssqlOutputFieldSnafu {
                    output_name: output_name.to_owned(),
                    field_name: field.name().clone(),
                    first_index: *entry.get(),
                    duplicate_index: index,
                }
                .fail();
            }
            Entry::Vacant(entry) => {
                entry.insert(index);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow_tiberius::{
        DiagnosticCode, DiagnosticSeverity, MssqlType, MssqlTypeLength, PlanOptions,
    };

    use super::*;
    use crate::{
        LoadMode, MssqlConnectionConfig, MssqlConnectionSource, MssqlTargetConfig,
        MssqlTargetResolutionContext, MssqlTargetTable,
    };

    fn resolved_target() -> Result<ResolvedMssqlTarget, DeltaFunnelError> {
        let connection = MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse-primary");
        let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_load_mode(LoadMode::CreateAndLoad)
            .with_connection(connection);

        target.resolve(MssqlTargetResolutionContext {
            output_name: Some("orders_output"),
            default_connection: None,
        })
    }

    #[test]
    fn simple_arrow_fields_produce_mappings() -> Result<(), DeltaFunnelError> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("customer", DataType::Utf8, true),
        ]);
        let target = resolved_target()?;

        let plan = plan_mssql_output_schema(&schema, &target, PlanOptions::default())?;

        assert_eq!(plan.target().output_name(), "orders_output");
        assert_eq!(plan.target().table().schema(), Some("dbo"));
        assert_eq!(plan.target().table().table(), "orders");
        assert_eq!(plan.target().load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(
            plan.target().connection_source(),
            MssqlConnectionSource::TargetOverride
        );
        assert_eq!(plan.mappings().len(), 2);
        assert_eq!(plan.mappings()[0].arrow().name(), "id");
        assert_eq!(plan.mappings()[0].mssql().name().as_str(), "id");
        assert_eq!(plan.mappings()[0].mssql().ty(), &MssqlType::BigInt);
        assert_eq!(plan.mappings()[1].arrow().name(), "customer");
        assert_eq!(plan.mappings()[1].mssql().name().as_str(), "customer");
        assert_eq!(
            plan.mappings()[1].mssql().ty(),
            &MssqlType::NVarChar(MssqlTypeLength::Max)
        );
        assert!(plan.diagnostics().is_empty());
        assert!(plan.diagnostic_reports().is_empty());
        Ok(())
    }

    #[test]
    fn aliases_and_reordered_fields_determine_target_column_order() -> Result<(), DeltaFunnelError>
    {
        let schema = Schema::new(vec![
            Field::new("gross_total", DataType::Float64, true),
            Field::new("order_id", DataType::Int32, false),
        ]);
        let target = resolved_target()?;

        let plan = plan_mssql_output_schema(&schema, &target, PlanOptions::default())?;

        let columns = plan
            .mappings()
            .iter()
            .map(|mapping| mapping.mssql().name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(columns, vec!["gross_total", "order_id"]);
        assert_eq!(plan.mappings()[0].arrow().index(), 0);
        assert_eq!(plan.mappings()[1].arrow().index(), 1);
        Ok(())
    }

    #[test]
    fn accepts_datafusion_output_schema_ref() -> Result<(), DeltaFunnelError> {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
        ]));
        let target = resolved_target()?;

        let plan = plan_mssql_output_schema(schema, &target, PlanOptions::default())?;

        assert_eq!(plan.mappings().len(), 2);
        assert_eq!(plan.mappings()[0].arrow().name(), "order_id");
        assert_eq!(plan.mappings()[1].arrow().name(), "status");
        Ok(())
    }

    #[test]
    fn nullable_fields_are_preserved_in_mssql_mappings() -> Result<(), DeltaFunnelError> {
        let schema = Schema::new(vec![
            Field::new("required_id", DataType::Int32, false),
            Field::new("optional_note", DataType::Utf8, true),
        ]);
        let target = resolved_target()?;

        let plan = plan_mssql_output_schema(&schema, &target, PlanOptions::default())?;

        assert!(!plan.mappings()[0].arrow().nullable());
        assert!(!plan.mappings()[0].mssql().nullable());
        assert!(plan.mappings()[1].arrow().nullable());
        assert!(plan.mappings()[1].mssql().nullable());
        Ok(())
    }

    #[test]
    fn duplicate_output_field_names_fail_before_arrow_tiberius() -> Result<(), DeltaFunnelError> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("id", DataType::Utf8, true),
        ]);
        let target = resolved_target()?;

        let error = plan_mssql_output_schema(&schema, &target, PlanOptions::default())
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected duplicate field error".to_owned(),
            })?;

        assert!(matches!(
            error,
            DeltaFunnelError::DuplicateMssqlOutputField {
                first_index: 0,
                duplicate_index: 1,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn missing_output_identity_fails_before_arrow_tiberius() -> Result<(), DeltaFunnelError> {
        let connection = MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?;
        let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?).resolve(
            MssqlTargetResolutionContext {
                output_name: None,
                default_connection: Some(&connection),
            },
        )?;
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let error = plan_mssql_output_schema(&schema, &target, PlanOptions::default())
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected output identity error".to_owned(),
            })?;

        assert!(matches!(
            error,
            DeltaFunnelError::InvalidMssqlOutputIdentity { .. }
        ));
        let display = error.to_string();
        assert!(!display.contains("secret-token"));
        assert!(!display.contains("password"));
        Ok(())
    }

    #[test]
    fn unsupported_arrow_types_surface_structured_diagnostics() -> Result<(), DeltaFunnelError> {
        let schema = Schema::new(vec![Field::new(
            "items",
            DataType::new_list(DataType::Int64, true),
            true,
        )]);
        let target = resolved_target()?;

        let error = plan_mssql_output_schema(&schema, &target, PlanOptions::default())
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
        assert!(diagnostic.message().contains("nested"));

        let reports = mssql_schema_diagnostic_reports(&output_name, &diagnostics);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].output_name(), "orders_output");
        assert_eq!(reports[0].severity(), DiagnosticSeverity::Error);
        assert_eq!(reports[0].code(), DiagnosticCode::UnsupportedArrowType);
        assert_eq!(reports[0].message(), diagnostic.message());
        let report_field = reports[0].field().ok_or_else(|| DeltaFunnelError::Config {
            message: "expected field report context".to_owned(),
        })?;
        assert_eq!(report_field.index(), 0);
        assert_eq!(report_field.name(), "items");
        assert_eq!(reports[0].row(), None);
        Ok(())
    }

    #[test]
    fn policy_sensitive_options_are_passed_to_arrow_tiberius() -> Result<(), DeltaFunnelError> {
        let schema = Schema::new(vec![Field::new("customer", DataType::Utf8, true)]);
        let target = resolved_target()?;
        let options = MssqlSchemaPlanOptions {
            string_policy: MssqlStringPolicy::NVarChar(128),
            ..PlanOptions::default()
        };

        let plan = plan_mssql_output_schema(&schema, &target, options)?;

        assert_eq!(
            plan.mappings()[0].mssql().ty(),
            &MssqlType::NVarChar(MssqlTypeLength::Bounded(128))
        );
        assert_eq!(plan.plan_options(), options);
        Ok(())
    }

    #[test]
    fn policy_option_types_are_available_through_delta_funnel_names() {
        let options = MssqlSchemaPlanOptions {
            string_policy: MssqlStringPolicy::NVarChar(64),
            binary_policy: MssqlBinaryPolicy::VarBinary(128),
            timezone_policy: MssqlTimezonePolicy::DateTimeOffset,
            timestamp_policy: MssqlTimestampPolicy::DateTime,
            nanosecond_policy: MssqlNanosecondPolicy::RoundTo100ns,
            uint64_policy: MssqlUInt64Policy::Decimal20_0,
            decimal_policy: MssqlDecimalPolicy::NormalizeNegativeScale,
            decimal256_policy: MssqlDecimal256Policy::Reject,
            float_policy: MssqlFloatPolicy::RejectNonFinite,
            date64_policy: MssqlDate64Policy::TimestampDateTime2,
        };

        assert_eq!(options.string_policy, MssqlStringPolicy::NVarChar(64));
        assert_eq!(options.binary_policy, MssqlBinaryPolicy::VarBinary(128));
        assert_eq!(options.timezone_policy, MssqlTimezonePolicy::DateTimeOffset);
        assert_eq!(options.timestamp_policy, MssqlTimestampPolicy::DateTime);
        assert_eq!(
            options.nanosecond_policy,
            MssqlNanosecondPolicy::RoundTo100ns
        );
        assert_eq!(options.uint64_policy, MssqlUInt64Policy::Decimal20_0);
        assert_eq!(
            options.decimal_policy,
            MssqlDecimalPolicy::NormalizeNegativeScale
        );
        assert_eq!(options.decimal256_policy, MssqlDecimal256Policy::Reject);
        assert_eq!(options.float_policy, MssqlFloatPolicy::RejectNonFinite);
        assert_eq!(options.date64_policy, MssqlDate64Policy::TimestampDateTime2);
    }

    #[test]
    fn timestamp_policy_can_plan_legacy_datetime() -> Result<(), DeltaFunnelError> {
        let schema = Schema::new(vec![Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        let target = resolved_target()?;
        let options = MssqlSchemaPlanOptions {
            timestamp_policy: MssqlTimestampPolicy::DateTime,
            ..PlanOptions::default()
        };

        let plan = plan_mssql_output_schema(&schema, &target, options)?;

        assert_eq!(plan.mappings()[0].mssql().ty(), &MssqlType::DateTime);
        Ok(())
    }

    #[test]
    fn diagnostics_retain_output_and_field_context() -> Result<(), DeltaFunnelError> {
        let schema = Schema::new(vec![Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("America/Phoenix"))),
            true,
        )]);
        let target = resolved_target()?;

        let error = plan_mssql_output_schema(&schema, &target, PlanOptions::default())
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected timezone planning error".to_owned(),
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
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(
            diagnostic.code(),
            DiagnosticCode::ProfileDependentConversion
        );
        let field = diagnostic.field().ok_or_else(|| DeltaFunnelError::Config {
            message: "expected field diagnostic context".to_owned(),
        })?;
        assert_eq!(field.index(), 0);
        assert_eq!(field.name(), "created_at");
        Ok(())
    }

    #[test]
    fn reports_errors_and_debug_output_redact_secret_material() -> Result<(), DeltaFunnelError> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let target = resolved_target()?;

        let plan = plan_mssql_output_schema(&schema, &target, PlanOptions::default())?;
        let debug = format!("{plan:?}");

        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        assert!(debug.contains("warehouse-primary"));

        let duplicate_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("id", DataType::Utf8, true),
        ]);
        let error = plan_mssql_output_schema(&duplicate_schema, &target, PlanOptions::default())
            .err()
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "expected duplicate field error".to_owned(),
            })?;
        let display = error.to_string();

        assert!(!display.contains("secret-token"));
        assert!(!display.contains("password"));
        assert!(!display.contains("server=tcp"));
        Ok(())
    }
}
