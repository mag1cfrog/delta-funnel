//! SQL Server target configuration and redacted connection resolution.

use std::fmt;

use crate::{
    DeltaFunnelError,
    error::{MissingMssqlConnectionSnafu, MssqlTargetConfigSnafu},
    support::sanitize_text_for_display,
};

use snafu::ensure;

/// SQL Server table lifecycle mode requested for one selected output.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LoadMode {
    /// Write rows into an already existing target table.
    #[default]
    AppendExisting,
    /// Plan a target table definition and then load rows into the new table.
    CreateAndLoad,
    /// Write rows to a staging table, validate them, then swap it into the target name.
    Replace,
}

/// A SQL Server connection configuration with secret-bearing material hidden from display.
#[derive(Clone, PartialEq, Eq)]
pub struct MssqlConnectionConfig {
    connection_string: String,
    display_label: Option<String>,
}

impl MssqlConnectionConfig {
    /// Creates a connection config from the raw connection string used by later I/O code.
    pub fn new(connection_string: impl Into<String>) -> Result<Self, DeltaFunnelError> {
        let connection_string = connection_string.into();
        ensure!(
            !connection_string.trim().is_empty(),
            MssqlTargetConfigSnafu {
                option: "connection.connection_string",
                message: "connection string must not be empty"
            }
        );

        Ok(Self {
            connection_string,
            display_label: None,
        })
    }

    /// Adds a caller-provided non-secret label for diagnostics and reports.
    #[must_use]
    pub fn with_display_label(mut self, label: impl Into<String>) -> Self {
        self.display_label = Some(label.into());
        self
    }

    /// Returns the raw connection string for later connection construction.
    ///
    /// Callers should avoid formatting this value in errors, logs, or reports.
    #[must_use]
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Returns a redacted summary safe for diagnostics and planning reports.
    #[must_use]
    pub fn summary(&self) -> MssqlConnectionSummary {
        MssqlConnectionSummary {
            display_label: self.display_label.clone(),
        }
    }
}

impl fmt::Debug for MssqlConnectionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlConnectionConfig")
            .field("connection_string", &"<redacted>")
            .field("summary", &self.summary())
            .finish()
    }
}

impl fmt::Display for MssqlConnectionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.summary().fmt(formatter)
    }
}

/// Redacted SQL Server connection context suitable for reports and errors.
#[derive(Clone, PartialEq, Eq)]
pub struct MssqlConnectionSummary {
    display_label: Option<String>,
}

impl MssqlConnectionSummary {
    /// Returns the optional caller-provided non-secret display label.
    #[must_use]
    pub fn display_label(&self) -> Option<&str> {
        self.display_label.as_deref()
    }
}

impl fmt::Debug for MssqlConnectionSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlConnectionSummary")
            .field(
                "display_label",
                &self.display_label.as_deref().map(sanitize_text_for_display),
            )
            .finish()
    }
}

impl fmt::Display for MssqlConnectionSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.display_label.as_deref() {
            Some(label) => {
                write!(
                    formatter,
                    "MSSQL connection `{}`",
                    sanitize_text_for_display(label)
                )
            }
            None => formatter.write_str("MSSQL connection <redacted>"),
        }
    }
}

/// SQL Server table identity before arrow-tiberius identifier validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlTargetTable {
    schema: Option<String>,
    table: String,
}

impl MssqlTargetTable {
    /// Creates a schema-qualified target table identity.
    ///
    /// Full SQL Server identifier validation is delegated to `arrow-tiberius`
    /// in the later DDL planning slice. This constructor only rejects empty
    /// config fields so target identity cannot collapse into an ambiguous string.
    pub fn new(
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<Self, DeltaFunnelError> {
        let schema = schema.into();
        let table = table.into();
        ensure!(
            !schema.trim().is_empty(),
            MssqlTargetConfigSnafu {
                option: "target.schema",
                message: "schema must not be empty"
            }
        );
        ensure!(
            !table.trim().is_empty(),
            MssqlTargetConfigSnafu {
                option: "target.table",
                message: "table must not be empty"
            }
        );

        Ok(Self {
            schema: Some(schema),
            table,
        })
    }

    /// Creates an unqualified target table identity.
    pub fn unqualified(table: impl Into<String>) -> Result<Self, DeltaFunnelError> {
        let table = table.into();
        ensure!(
            !table.trim().is_empty(),
            MssqlTargetConfigSnafu {
                option: "target.table",
                message: "table must not be empty"
            }
        );

        Ok(Self {
            schema: None,
            table,
        })
    }

    /// Returns the optional target schema name.
    #[must_use]
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Returns the target table name.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }
}

/// Target configuration for writing one selected output to SQL Server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlTargetConfig {
    table: MssqlTargetTable,
    load_mode: LoadMode,
    connection: Option<MssqlConnectionConfig>,
}

impl MssqlTargetConfig {
    /// Creates an append-existing target config for the given table identity.
    #[must_use]
    pub fn new(table: MssqlTargetTable) -> Self {
        Self {
            table,
            load_mode: LoadMode::default(),
            connection: None,
        }
    }

    /// Sets the requested target lifecycle mode.
    #[must_use]
    pub fn with_load_mode(mut self, load_mode: LoadMode) -> Self {
        self.load_mode = load_mode;
        self
    }

    /// Sets a per-output connection override.
    #[must_use]
    pub fn with_connection(mut self, connection: MssqlConnectionConfig) -> Self {
        self.connection = Some(connection);
        self
    }

    /// Returns the target table identity.
    #[must_use]
    pub fn table(&self) -> &MssqlTargetTable {
        &self.table
    }

    /// Returns the requested target lifecycle mode.
    #[must_use]
    pub fn load_mode(&self) -> LoadMode {
        self.load_mode
    }

    /// Returns the optional per-output connection override.
    #[must_use]
    pub fn connection(&self) -> Option<&MssqlConnectionConfig> {
        self.connection.as_ref()
    }

    /// Resolves this target against context-level defaults.
    pub fn resolve(
        &self,
        context: MssqlTargetResolutionContext<'_>,
    ) -> Result<ResolvedMssqlTarget, DeltaFunnelError> {
        let output_name = context.output_name.unwrap_or("<unnamed>").to_owned();
        let (connection, connection_source) = self
            .connection
            .as_ref()
            .map(|connection| (connection, MssqlConnectionSource::TargetOverride))
            .or_else(|| {
                context
                    .default_connection
                    .map(|connection| (connection, MssqlConnectionSource::ContextDefault))
            })
            .ok_or_else(|| {
                MissingMssqlConnectionSnafu {
                    output_name: output_name.clone(),
                }
                .build()
            })?;

        Ok(ResolvedMssqlTarget {
            output_name,
            table: self.table.clone(),
            load_mode: self.load_mode,
            connection: connection.clone(),
            connection_source,
        })
    }
}

/// Context defaults available while resolving one selected output target.
#[derive(Debug, Clone, Copy, Default)]
pub struct MssqlTargetResolutionContext<'a> {
    /// Selected output name used for reports and errors.
    pub output_name: Option<&'a str>,
    /// Context-level default connection used when the target has no override.
    pub default_connection: Option<&'a MssqlConnectionConfig>,
}

/// Where an effective SQL Server connection came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MssqlConnectionSource {
    /// The selected output target supplied its own connection.
    TargetOverride,
    /// The selected output used the context/session default connection.
    ContextDefault,
}

/// Resolved SQL Server target for one selected output.
#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedMssqlTarget {
    output_name: String,
    table: MssqlTargetTable,
    load_mode: LoadMode,
    connection: MssqlConnectionConfig,
    connection_source: MssqlConnectionSource,
}

impl ResolvedMssqlTarget {
    /// Returns the selected output name associated with this target.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the target table identity.
    #[must_use]
    pub fn table(&self) -> &MssqlTargetTable {
        &self.table
    }

    /// Returns the requested target lifecycle mode.
    #[must_use]
    pub fn load_mode(&self) -> LoadMode {
        self.load_mode
    }

    /// Returns the effective connection.
    #[must_use]
    pub fn connection(&self) -> &MssqlConnectionConfig {
        &self.connection
    }

    /// Returns where the effective connection came from.
    #[must_use]
    pub fn connection_source(&self) -> MssqlConnectionSource {
        self.connection_source
    }

    /// Returns a redacted planning summary for diagnostics and Python-facing reports.
    #[must_use]
    pub fn summary(&self) -> MssqlTargetSummary {
        MssqlTargetSummary {
            output_name: self.output_name.clone(),
            table: self.table.clone(),
            load_mode: self.load_mode,
            connection_source: self.connection_source,
            connection: self.connection.summary(),
        }
    }
}

impl fmt::Debug for ResolvedMssqlTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedMssqlTarget")
            .field("output_name", &sanitize_text_for_display(&self.output_name))
            .field("table", &self.table)
            .field("load_mode", &self.load_mode)
            .field("connection", &self.connection.summary())
            .field("connection_source", &self.connection_source)
            .finish()
    }
}

/// Redacted report for one resolved SQL Server target.
#[derive(Clone, PartialEq, Eq)]
pub struct MssqlTargetSummary {
    output_name: String,
    table: MssqlTargetTable,
    load_mode: LoadMode,
    connection_source: MssqlConnectionSource,
    connection: MssqlConnectionSummary,
}

impl MssqlTargetSummary {
    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the target table identity.
    #[must_use]
    pub fn table(&self) -> &MssqlTargetTable {
        &self.table
    }

    /// Returns the requested target lifecycle mode.
    #[must_use]
    pub fn load_mode(&self) -> LoadMode {
        self.load_mode
    }

    /// Returns where the effective connection came from.
    #[must_use]
    pub fn connection_source(&self) -> MssqlConnectionSource {
        self.connection_source
    }

    /// Returns the redacted effective connection summary.
    #[must_use]
    pub fn connection(&self) -> &MssqlConnectionSummary {
        &self.connection
    }
}

impl fmt::Debug for MssqlTargetSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlTargetSummary")
            .field("output_name", &sanitize_text_for_display(&self.output_name))
            .field("table", &self.table)
            .field("load_mode", &self.load_mode)
            .field("connection_source", &self.connection_source)
            .field("connection", &self.connection)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret_connection(label: &str) -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label(label))
    }

    #[test]
    fn target_override_connection_wins_over_context_default() -> Result<(), DeltaFunnelError> {
        let default_connection = secret_connection("context-default")?;
        let override_connection = secret_connection("target-override")?;
        let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "west_orders")?)
            .with_connection(override_connection.clone());

        let resolved = target.resolve(MssqlTargetResolutionContext {
            output_name: Some("west"),
            default_connection: Some(&default_connection),
        })?;

        assert_eq!(
            resolved.connection().connection_string(),
            override_connection.connection_string()
        );
        assert_eq!(
            resolved.connection_source(),
            MssqlConnectionSource::TargetOverride
        );
        assert_eq!(resolved.output_name(), "west");
        Ok(())
    }

    #[test]
    fn context_default_connection_is_used_without_override() -> Result<(), DeltaFunnelError> {
        let default_connection = secret_connection("context-default")?;
        let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "east_orders")?);

        let resolved = target.resolve(MssqlTargetResolutionContext {
            output_name: Some("east"),
            default_connection: Some(&default_connection),
        })?;

        assert_eq!(
            resolved.connection().connection_string(),
            default_connection.connection_string()
        );
        assert_eq!(
            resolved.connection_source(),
            MssqlConnectionSource::ContextDefault
        );
        Ok(())
    }

    #[test]
    fn missing_effective_connection_returns_structured_error() -> Result<(), DeltaFunnelError> {
        let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "west_orders")?);

        let error = target
            .resolve(MssqlTargetResolutionContext {
                output_name: Some("west\norders"),
                default_connection: None,
            })
            .err();

        assert!(matches!(
            error,
            Some(DeltaFunnelError::MissingMssqlConnection { .. })
        ));
        let display = error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "expected error".to_owned());
        assert!(!display.contains('\n'));
        assert!(display.contains(r"west\norders"));
        Ok(())
    }

    #[test]
    fn targets_resolve_independently_for_multiple_outputs() -> Result<(), DeltaFunnelError> {
        let default_connection = secret_connection("context-default")?;
        let east_connection = secret_connection("east-override")?;
        let west = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "west_orders")?);
        let east = MssqlTargetConfig::new(MssqlTargetTable::new("reporting", "east_orders")?)
            .with_load_mode(LoadMode::CreateAndLoad)
            .with_connection(east_connection.clone());

        let resolved_west = west.resolve(MssqlTargetResolutionContext {
            output_name: Some("west"),
            default_connection: Some(&default_connection),
        })?;
        let resolved_east = east.resolve(MssqlTargetResolutionContext {
            output_name: Some("east"),
            default_connection: Some(&default_connection),
        })?;

        assert_eq!(resolved_west.table().schema(), Some("dbo"));
        assert_eq!(resolved_west.table().table(), "west_orders");
        assert_eq!(resolved_west.load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            resolved_west.connection_source(),
            MssqlConnectionSource::ContextDefault
        );

        assert_eq!(resolved_east.table().schema(), Some("reporting"));
        assert_eq!(resolved_east.table().table(), "east_orders");
        assert_eq!(resolved_east.load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(
            resolved_east.connection_source(),
            MssqlConnectionSource::TargetOverride
        );
        assert_eq!(
            resolved_east.connection().connection_string(),
            east_connection.connection_string()
        );
        Ok(())
    }

    #[test]
    fn load_modes_are_explicit() {
        assert_eq!(LoadMode::default(), LoadMode::AppendExisting);
        assert_eq!(LoadMode::AppendExisting, LoadMode::AppendExisting);
        assert_eq!(LoadMode::CreateAndLoad, LoadMode::CreateAndLoad);
        assert_eq!(LoadMode::Replace, LoadMode::Replace);
    }

    #[test]
    fn connection_debug_display_and_reports_redact_secret_material() -> Result<(), DeltaFunnelError>
    {
        let connection = secret_connection("warehouse-primary")?;
        let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_connection(connection.clone());
        let resolved = target.resolve(MssqlTargetResolutionContext {
            output_name: Some("orders"),
            default_connection: None,
        })?;

        let combined = format!(
            "{connection:?}\n{connection}\n{resolved:?}\n{:?}",
            resolved.summary()
        );

        assert!(!combined.contains("secret-token"));
        assert!(!combined.contains("password"));
        assert!(!combined.contains("server=tcp"));
        assert!(combined.contains("warehouse-primary"));
        Ok(())
    }

    #[test]
    fn table_identity_keeps_schema_and_table_separate() -> Result<(), DeltaFunnelError> {
        let qualified = MssqlTargetTable::new("dbo", "orders")?;
        let unqualified = MssqlTargetTable::unqualified("orders")?;

        assert_eq!(qualified.schema(), Some("dbo"));
        assert_eq!(qualified.table(), "orders");
        assert_eq!(unqualified.schema(), None);
        assert_eq!(unqualified.table(), "orders");
        Ok(())
    }

    #[test]
    fn empty_table_identity_is_rejected() {
        assert!(matches!(
            MssqlConnectionConfig::new(" "),
            Err(DeltaFunnelError::MssqlTargetConfig { .. })
        ));
        assert!(matches!(
            MssqlTargetTable::new(" ", "orders"),
            Err(DeltaFunnelError::MssqlTargetConfig { .. })
        ));
        assert!(matches!(
            MssqlTargetTable::unqualified(" "),
            Err(DeltaFunnelError::MssqlTargetConfig { .. })
        ));
    }
}
