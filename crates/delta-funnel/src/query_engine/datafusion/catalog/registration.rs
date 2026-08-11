//! DataFusion session registration for Delta sources.

use std::error::Error as _;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;
use delta_arrow_reader::{
    DeltaDataFusionScanOptions, DeltaProtocolInfo, DeltaReaderError, DeltaReaderExecutionOptions,
    DeltaTable, DeltaTableSnapshot, register_delta_table,
};

use crate::{
    DeltaFunnelError, DeltaProtocolReport,
    error::DataFusionRegistrationSnafu,
    observability,
    progress::{ProgressEvent, ProgressPhase, ProgressReporter},
    support::sanitize_uri_for_display,
    table_formats::validate_table_source_names,
};

/// Registered Delta sources visible to a DataFusion session.
#[derive(Debug, Clone)]
pub struct RegisteredDeltaSources {
    /// Per-source registration reports.
    pub sources: Vec<RegisteredDeltaSource>,
}

/// One registered Delta source.
#[derive(Debug, Clone)]
pub struct RegisteredDeltaSource {
    /// DataFusion table name for this source.
    pub name: String,
    /// Sanitized normalized Delta table URI context.
    pub table_uri: String,
    /// Resolved Delta snapshot version.
    pub snapshot_version: u64,
    /// Logical Arrow schema exposed to DataFusion.
    pub schema: SchemaRef,
    /// Protocol report captured before registration.
    pub protocol: DeltaProtocolReport,
}

/// Registers loaded Delta sources into a DataFusion session.
///
/// Each tuple contains the table name, loaded table, and optional scan partition target.
///
/// # Errors
///
/// Returns a source-name or DataFusion registration error before leaving a partial catalog.
pub fn register_delta_sources(
    ctx: &SessionContext,
    sources: Vec<(String, DeltaTable, Option<usize>)>,
) -> Result<RegisteredDeltaSources, DeltaFunnelError> {
    register_delta_sources_with_options(ctx, sources, DeltaReaderExecutionOptions::default())
}

/// Registers loaded Delta sources with explicit reader execution bounds.
///
/// # Errors
///
/// Returns a configuration, source-name, or DataFusion registration error before leaving a
/// partial catalog.
pub fn register_delta_sources_with_scan_execution_options(
    ctx: &SessionContext,
    sources: Vec<(String, DeltaTable, Option<usize>)>,
    execution_options: DeltaReaderExecutionOptions,
) -> Result<RegisteredDeltaSources, DeltaFunnelError> {
    execution_options
        .validate()
        .map_err(map_reader_configuration_error)?;
    register_delta_sources_with_options(ctx, sources, execution_options)
}

pub(crate) fn register_delta_source_with_scan_execution_options(
    ctx: &SessionContext,
    source_name: String,
    table: DeltaTable,
    scan_target_partitions: Option<usize>,
    execution_options: DeltaReaderExecutionOptions,
    reporter: Option<&ProgressReporter>,
) -> Result<RegisteredDeltaSource, DeltaFunnelError> {
    execution_options
        .validate()
        .map_err(map_reader_configuration_error)?;
    validate_table_source_names([source_name.as_str()])?;
    reject_existing_delta_registration_name(ctx, &source_name, table.table_uri())?;
    emit_registration_phase(reporter, ProgressPhase::RegisteringDeltaSource);
    register_delta_table_with_tracing(
        ctx,
        source_name,
        table,
        DeltaDataFusionScanOptions {
            execution_options,
            target_partitions: scan_target_partitions,
        },
    )
}

fn emit_registration_phase(reporter: Option<&ProgressReporter>, phase: ProgressPhase) {
    if let Some(reporter) = reporter {
        reporter.emit(&ProgressEvent::phase_changed(phase, None));
    }
}

fn register_delta_sources_with_options(
    ctx: &SessionContext,
    sources: Vec<(String, DeltaTable, Option<usize>)>,
    execution_options: DeltaReaderExecutionOptions,
) -> Result<RegisteredDeltaSources, DeltaFunnelError> {
    validate_table_source_names(sources.iter().map(|(name, _, _)| name.as_str()))?;
    for (name, table, _) in &sources {
        validate_delta_table_protocol(name, table)?;
    }
    for (name, table, _) in &sources {
        reject_existing_delta_registration_name(ctx, name, table.table_uri())?;
    }

    let mut registered = Vec::with_capacity(sources.len());
    for (name, table, target_partitions) in sources {
        let options = DeltaDataFusionScanOptions {
            execution_options,
            target_partitions,
        };
        match register_delta_table_with_tracing(ctx, name, table, options) {
            Ok(source) => registered.push(source),
            Err(error) => {
                rollback_registered_delta_sources(
                    ctx,
                    &registered
                        .iter()
                        .map(|source: &RegisteredDeltaSource| source.name.clone())
                        .collect::<Vec<_>>(),
                );
                return Err(error);
            }
        }
    }

    Ok(RegisteredDeltaSources {
        sources: registered,
    })
}

/// Rejects a case-insensitive conflict in DataFusion's default catalog.
pub(crate) fn reject_existing_delta_registration_name(
    ctx: &SessionContext,
    source_name: &str,
    table_uri: &str,
) -> Result<(), DeltaFunnelError> {
    let state = ctx.state();
    let catalog_options = &state.config_options().catalog;
    let default_catalog = ctx.catalog(&catalog_options.default_catalog);
    let default_schema = default_catalog
        .as_ref()
        .and_then(|catalog| catalog.schema(&catalog_options.default_schema));
    let existing_names = default_schema
        .as_ref()
        .map_or_else(Vec::new, |schema| schema.table_names());

    if let Some(existing_name) = existing_names
        .iter()
        .find(|existing_name| existing_name.eq_ignore_ascii_case(source_name))
    {
        return DataFusionRegistrationSnafu {
            source_name: source_name.to_owned(),
            table_uri: table_uri.to_owned(),
            reason: format!("table already exists: {existing_name}"),
        }
        .fail();
    }

    Ok(())
}

fn register_delta_table_with_tracing(
    ctx: &SessionContext,
    source_name: String,
    table: DeltaTable,
    options: DeltaDataFusionScanOptions,
) -> Result<RegisteredDeltaSource, DeltaFunnelError> {
    let table_uri = table.table_uri().to_owned();
    let snapshot_version = table.version();
    let registered = RegisteredDeltaSource {
        name: source_name.clone(),
        table_uri: sanitize_uri_for_display(&table_uri),
        snapshot_version,
        schema: table.schema().clone(),
        protocol: delta_protocol_report(&source_name, &table),
    };
    observability::datafusion_registration_started(&source_name, snapshot_version);

    let result = register_delta_table(ctx, source_name.clone(), table, options)
        .map(|_| registered)
        .map_err(|error| map_registration_error(&source_name, &table_uri, error));
    match &result {
        Ok(registered) => {
            observability::datafusion_registration_completed(&registered.name, snapshot_version);
        }
        Err(error) => {
            observability::datafusion_registration_failed(&source_name, snapshot_version, error);
        }
    }
    result
}

fn rollback_registered_delta_sources(ctx: &SessionContext, names: &[String]) {
    for name in names.iter().rev() {
        let _ = ctx.deregister_table(name.as_str());
    }
}

pub(crate) fn delta_protocol_report(source_name: &str, table: &DeltaTable) -> DeltaProtocolReport {
    protocol_report(
        source_name,
        table.table_uri(),
        table.version(),
        table.protocol(),
    )
}

fn protocol_report(
    source_name: &str,
    table_uri: &str,
    snapshot_version: u64,
    protocol: &DeltaProtocolInfo,
) -> DeltaProtocolReport {
    DeltaProtocolReport {
        source_name: source_name.to_owned(),
        table_uri: sanitize_uri_for_display(table_uri),
        snapshot_version,
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features: protocol.reader_features().to_vec(),
        writer_features: protocol.writer_features().to_vec(),
    }
}

pub(crate) fn validate_delta_table_protocol(
    source_name: &str,
    table: &DeltaTable,
) -> Result<(), DeltaFunnelError> {
    validate_protocol(
        source_name,
        table.table_uri(),
        table.version(),
        table.protocol(),
        table.validate_protocol(),
    )
}

pub(crate) fn validate_delta_table_snapshot_protocol(
    source_name: &str,
    snapshot: &DeltaTableSnapshot,
) -> Result<(), DeltaFunnelError> {
    validate_protocol(
        source_name,
        snapshot.table_uri(),
        snapshot.version(),
        snapshot.protocol(),
        snapshot.validate_protocol(),
    )
}

fn validate_protocol(
    source_name: &str,
    table_uri: &str,
    snapshot_version: u64,
    protocol: &DeltaProtocolInfo,
    validation: Result<(), DeltaReaderError>,
) -> Result<(), DeltaFunnelError> {
    if validation.is_ok() {
        return Ok(());
    }

    let reason = if !matches!(protocol.min_reader_version(), 1..=3) {
        format!(
            "unsupported Delta minReaderVersion {}",
            protocol.min_reader_version()
        )
    } else {
        let unsupported = protocol
            .first_unsupported_reader_feature()
            .unwrap_or_default();
        format!(
            "unsupported Delta reader feature `{}`",
            unsupported
                .chars()
                .flat_map(char::escape_default)
                .collect::<String>()
        )
    };
    let protocol = protocol_report(source_name, table_uri, snapshot_version, protocol);
    Err(DeltaFunnelError::DeltaProtocolCompatibility {
        source_name: protocol.source_name,
        table_uri: protocol.table_uri,
        snapshot_version: protocol.snapshot_version,
        reason,
    })
}

fn map_reader_configuration_error(error: DeltaReaderError) -> DeltaFunnelError {
    DeltaFunnelError::Config {
        message: error.to_string(),
    }
}

fn map_registration_error(
    source_name: &str,
    table_uri: &str,
    error: DeltaReaderError,
) -> DeltaFunnelError {
    DeltaFunnelError::DataFusionRegistration {
        source_name: source_name.to_owned(),
        table_uri: sanitize_uri_for_display(table_uri),
        reason: error
            .source()
            .map_or_else(|| error.to_string(), ToString::to_string),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::{TableType, empty::EmptyTable};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use delta_arrow_reader::{DeltaReaderBackend, DeltaTableBuilder};

    use super::*;
    use crate::query_engine::datafusion::test_support::{
        DEFAULT_SCHEMA_FIELDS_JSON, DeltaLogTable, FailsOnCustomersSchemaProvider,
        SingleSchemaCatalogProvider, register_fixture_source,
    };

    const UNSUPPORTED_PROTOCOL_JSON: &str =
        r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":2}}"#;

    fn load(table: &DeltaLogTable) -> Result<DeltaTable, DeltaReaderError> {
        DeltaTableBuilder::new(table.path().to_string_lossy()).load()
    }

    #[test]
    fn registers_loaded_delta_source() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("registration")?;
        let context = SessionContext::new();

        let registered =
            register_delta_sources(&context, vec![("orders".to_owned(), load(&table)?, None)])?;

        let source = &registered.sources[0];
        assert_eq!(source.name, "orders");
        assert!(source.table_uri.starts_with("file://"));
        assert_eq!(source.snapshot_version, 1);
        assert_eq!(source.schema.field(0).name(), "id");
        assert_eq!(source.protocol.source_name, "orders");
        assert!(context.table_exist("orders")?);
        Ok(())
    }

    #[test]
    fn registration_accepts_native_async_backend() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("registration-native")?;
        let context = SessionContext::new();
        let options = DeltaReaderExecutionOptions::new()
            .with_reader_backend(DeltaReaderBackend::NativeAsync)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_output_buffer_capacity_per_partition(1)?;

        register_delta_sources_with_scan_execution_options(
            &context,
            vec![("orders".to_owned(), load(&table)?, None)],
            options,
        )?;

        assert!(context.table_exist("orders")?);
        Ok(())
    }

    #[tokio::test]
    async fn catalog_inspection_exposes_registered_schema() -> Result<(), Box<dyn std::error::Error>>
    {
        let context = SessionContext::new();
        let _table = register_fixture_source(&context, "orders", "catalog-inspection")?;
        let catalog = context.catalog("datafusion").ok_or("missing catalog")?;
        let schema = catalog.schema("public").ok_or("missing schema")?;
        let provider = schema.table("orders").await?.ok_or("missing provider")?;

        assert_eq!(provider.table_type(), TableType::Base);
        assert_eq!(provider.schema().field(0).data_type(), &DataType::Int32);
        Ok(())
    }

    #[test]
    fn existing_conflict_fails_before_partial_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("conflict-orders")?;
        let customers = DeltaLogTable::new("conflict-customers")?;
        let context = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "existing",
            DataType::Utf8,
            true,
        )]));
        context.register_table("customers", Arc::new(EmptyTable::new(schema)))?;

        let result = register_delta_sources(
            &context,
            vec![
                ("orders".to_owned(), load(&orders)?, None),
                ("customers".to_owned(), load(&customers)?, None),
            ],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration { source_name, .. })
                if source_name == "customers"
        ));
        assert!(!context.table_exist("orders")?);
        assert!(context.table_exist("customers")?);
        Ok(())
    }

    #[test]
    fn protocol_failure_precedes_partial_registration() -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("protocol-orders")?;
        let customers = DeltaLogTable::new_with_schema_protocol_and_adds(
            "protocol-customers",
            UNSUPPORTED_PROTOCOL_JSON,
            DEFAULT_SCHEMA_FIELDS_JSON,
            "[]",
            &[r#""partitionValues":{}"#],
        )?;
        let context = SessionContext::new();

        let result = register_delta_sources(
            &context,
            vec![
                ("orders".to_owned(), load(&orders)?, None),
                ("customers".to_owned(), load(&customers)?, None),
            ],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaProtocolCompatibility { source_name, reason, .. })
                if source_name == "customers"
                    && reason == "unsupported Delta minReaderVersion 99"
        ));
        assert!(!context.table_exist("orders")?);
        assert!(!context.table_exist("customers")?);
        Ok(())
    }

    #[test]
    fn existing_conflict_uses_the_configured_default_catalog_and_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("custom-conflict-orders")?;
        let customers = DeltaLogTable::new("custom-conflict-customers")?;
        let context = SessionContext::new_with_config(
            SessionConfig::new().with_default_catalog_and_schema("custom", "schema"),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "existing",
            DataType::Utf8,
            true,
        )]));
        context.register_table("customers", Arc::new(EmptyTable::new(schema)))?;

        let result = register_delta_sources(
            &context,
            vec![
                ("orders".to_owned(), load(&orders)?, None),
                ("customers".to_owned(), load(&customers)?, None),
            ],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration { source_name, .. })
                if source_name == "customers"
        ));
        assert!(!context.table_exist("orders")?);
        assert!(context.table_exist("customers")?);
        Ok(())
    }

    #[test]
    fn late_failure_rolls_back_prior_sources() -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("rollback-orders")?;
        let customers = DeltaLogTable::new("rollback-customers")?;
        let context = SessionContext::new();
        let schema: Arc<dyn datafusion::catalog::SchemaProvider> =
            Arc::new(FailsOnCustomersSchemaProvider::default());
        context.register_catalog(
            "datafusion",
            Arc::new(SingleSchemaCatalogProvider::new(schema)),
        );

        let result = register_delta_sources(
            &context,
            vec![
                ("orders".to_owned(), load(&orders)?, None),
                ("customers".to_owned(), load(&customers)?, None),
            ],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration { source_name, .. })
                if source_name == "customers"
        ));
        assert!(!context.table_exist("orders")?);
        assert!(!context.table_exist("customers")?);
        Ok(())
    }

    #[test]
    fn duplicate_names_fail_before_partial_registration() -> Result<(), Box<dyn std::error::Error>>
    {
        let orders = DeltaLogTable::new("duplicate-orders")?;
        let customers = DeltaLogTable::new("duplicate-customers")?;
        let context = SessionContext::new();

        let result = register_delta_sources(
            &context,
            vec![
                ("orders".to_owned(), load(&orders)?, None),
                ("Orders".to_owned(), load(&customers)?, None),
            ],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "Orders"
        ));
        assert!(!context.table_exist("orders")?);
        Ok(())
    }
}
