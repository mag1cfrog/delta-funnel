//! DataFusion session registration for Delta sources.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;

use crate::{
    DeltaFunnelError, DeltaProtocolReport, PlannedDeltaSource, ProtocolPreflight,
    error::DataFusionRegistrationSnafu, redaction::sanitize_uri_for_display,
    table_formats::validate_table_source_names,
};

use super::super::execution::DeltaProviderScanExecutionOptions;
use super::provider::DeltaTableProvider;

/// Delta source and preflight state used to build a DataFusion table provider.
pub struct DeltaTableProviderConfig {
    /// Loaded Delta source.
    pub source: PlannedDeltaSource,
    /// Successful Delta protocol preflight for the source.
    pub protocol: ProtocolPreflight,
    /// Optional DeltaFunnel scan file task partition target override.
    ///
    /// When set, this value wins over DataFusion's session target and automatic
    /// machine fallback policy for this Delta source only.
    pub scan_target_partitions: Option<usize>,
}

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
    /// Resolved Delta snapshot version.
    pub snapshot_version: u64,
    /// Logical Arrow schema exposed to DataFusion.
    pub schema: SchemaRef,
    /// Protocol report captured before registration.
    pub protocol: DeltaProtocolReport,
}

/// Registers preflighted Delta sources into a DataFusion session.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::DeltaSourceSchema`] when a source schema cannot
/// be converted to Arrow, or [`DeltaFunnelError::DataFusionRegistration`] when
/// DataFusion rejects a table registration.
pub fn register_delta_sources(
    ctx: &SessionContext,
    configs: Vec<DeltaTableProviderConfig>,
) -> Result<RegisteredDeltaSources, DeltaFunnelError> {
    register_delta_sources_with_options(
        ctx,
        configs,
        DeltaProviderScanExecutionOptions::default(),
        true,
    )
}

/// Registers preflighted Delta sources with explicit provider execution bounds.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::Config`] when execution bounds are invalid,
/// [`DeltaFunnelError::DeltaSourceSchema`] when a source schema cannot be
/// converted to Arrow, or [`DeltaFunnelError::DataFusionRegistration`] when
/// DataFusion rejects a table registration.
pub fn register_delta_sources_with_scan_execution_options(
    ctx: &SessionContext,
    configs: Vec<DeltaTableProviderConfig>,
    execution_options: DeltaProviderScanExecutionOptions,
) -> Result<RegisteredDeltaSources, DeltaFunnelError> {
    execution_options.validate()?;
    register_delta_sources_with_options(ctx, configs, execution_options, false)
}

fn register_delta_sources_with_options(
    ctx: &SessionContext,
    configs: Vec<DeltaTableProviderConfig>,
    execution_options: DeltaProviderScanExecutionOptions,
    resolve_default_scan_wide_capacity: bool,
) -> Result<RegisteredDeltaSources, DeltaFunnelError> {
    reject_duplicate_registration_names(&configs)?;
    let providers = configs
        .into_iter()
        .map(|config| {
            DeltaTableProvider::try_new_with_execution_options_and_default_capacity_resolution(
                config.source,
                config.protocol,
                config.scan_target_partitions,
                execution_options,
                resolve_default_scan_wide_capacity,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    reject_existing_registration_names(ctx, &providers)?;

    let sources = register_delta_providers(ctx, providers)?;

    Ok(RegisteredDeltaSources { sources })
}

fn reject_duplicate_registration_names(
    configs: &[DeltaTableProviderConfig],
) -> Result<(), DeltaFunnelError> {
    validate_table_source_names(configs.iter().map(|config| config.source.name()))
}

fn reject_existing_registration_names(
    ctx: &SessionContext,
    providers: &[DeltaTableProvider],
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

    for provider in providers {
        if let Some(existing_name) = existing_names
            .iter()
            .find(|existing_name| existing_name.eq_ignore_ascii_case(provider.source_name()))
        {
            return DataFusionRegistrationSnafu {
                source_name: provider.source_name().to_owned(),
                table_uri: provider.source_table_uri().to_owned(),
                reason: format!("table already exists: {existing_name}"),
            }
            .fail();
        }
    }

    Ok(())
}

fn register_delta_provider(
    ctx: &SessionContext,
    provider: DeltaTableProvider,
) -> Result<RegisteredDeltaSource, DeltaFunnelError> {
    let registered = RegisteredDeltaSource {
        name: provider.source_name().to_owned(),
        snapshot_version: provider.snapshot_version(),
        schema: provider.schema(),
        protocol: provider.protocol().clone(),
    };
    let table_uri = provider.source_table_uri().to_owned();

    if let Err(error) = ctx.register_table(registered.name.as_str(), Arc::new(provider)) {
        return DataFusionRegistrationSnafu {
            source_name: registered.name.clone(),
            table_uri,
            reason: error.to_string(),
        }
        .fail();
    }

    Ok(registered)
}

fn register_delta_providers(
    ctx: &SessionContext,
    providers: Vec<DeltaTableProvider>,
) -> Result<Vec<RegisteredDeltaSource>, DeltaFunnelError> {
    let mut registered_sources = Vec::with_capacity(providers.len());
    let mut registered_names = Vec::with_capacity(providers.len());

    for provider in providers {
        let registered = match register_delta_provider(ctx, provider) {
            Ok(registered) => registered,
            Err(error) => {
                rollback_registered_delta_sources(ctx, &registered_names);
                return Err(error);
            }
        };

        registered_names.push(registered.name.clone());
        registered_sources.push(registered);
    }

    Ok(registered_sources)
}

fn rollback_registered_delta_sources(ctx: &SessionContext, names: &[String]) {
    for name in names.iter().rev() {
        let _ = ctx.deregister_table(name.as_str());
    }
}

pub(super) fn reject_mismatched_preflight(
    source: &PlannedDeltaSource,
    protocol: &DeltaProtocolReport,
) -> Result<(), DeltaFunnelError> {
    let source_table_uri = sanitize_uri_for_display(source.table_uri());

    if protocol.source_name != source.name()
        || protocol.snapshot_version != source.version()
        || protocol.table_uri != source_table_uri
    {
        return DataFusionRegistrationSnafu {
            source_name: source.name().to_owned(),
            table_uri: source.table_uri().to_owned(),
            reason: format!(
                "protocol preflight belongs to source `{}` at snapshot version {} ({})",
                protocol.source_name, protocol.snapshot_version, protocol.table_uri
            ),
        }
        .fail();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::TableType;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;
    use crate::query_engine::datafusion::execution::DeltaProviderReaderBackend;
    use crate::query_engine::datafusion::test_support::{
        DeltaLogTable, FailsOnCustomersSchemaProvider, INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON,
        SingleSchemaCatalogProvider, register_fixture_source,
    };
    use crate::{DeltaFunnelError, DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

    #[test]
    fn registers_preflighted_delta_source() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("registration")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let ctx = SessionContext::new();

        let registered = register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
        )?;

        assert_eq!(registered.sources.len(), 1);
        assert_eq!(registered.sources[0].name, "orders");
        assert_eq!(registered.sources[0].snapshot_version, 1);
        assert_eq!(registered.sources[0].schema.field(0).name(), "id");
        assert_eq!(registered.sources[0].protocol.source_name, "orders");

        Ok(())
    }

    #[test]
    fn registration_rejects_zero_execution_bounds_before_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("registration-zero-execution-bounds")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let ctx = SessionContext::new();

        let result = register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            DeltaProviderScanExecutionOptions {
                reader_backend: DeltaProviderReaderBackend::OfficialKernel,
                max_concurrent_file_reads_per_scan: 0,
                max_concurrent_file_reads_per_partition: 1,
                output_buffer_capacity_per_partition: 1,
                native_async_prefetch_file_count_per_partition: 0,
            },
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::Config { message })
                if message == "max_concurrent_file_reads_per_scan must be greater than zero"
        ));
        assert!(!ctx.table_exist("orders")?);

        Ok(())
    }

    #[test]
    fn registration_accepts_native_async_backend_for_local_file_uri()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("registration-native-async-local-file")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let ctx = SessionContext::new();

        let registered = register_delta_sources_with_scan_execution_options(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            DeltaProviderScanExecutionOptions {
                reader_backend: DeltaProviderReaderBackend::NativeAsync,
                max_concurrent_file_reads_per_scan: 1,
                max_concurrent_file_reads_per_partition: 1,
                output_buffer_capacity_per_partition: 1,
                native_async_prefetch_file_count_per_partition: 0,
            },
        )?;

        assert_eq!(registered.sources.len(), 1);
        assert_eq!(registered.sources[0].name, "orders");
        assert!(ctx.table_exist("orders")?);

        Ok(())
    }

    #[tokio::test]
    async fn catalog_inspection_exposes_registered_provider_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let _table = register_fixture_source(&ctx, "orders", "catalog-inspection")?;

        let catalog = ctx.catalog("datafusion").ok_or("missing default catalog")?;
        let schema = catalog.schema("public").ok_or("missing default schema")?;
        let provider = schema
            .table("orders")
            .await?
            .ok_or("missing registered table provider")?;
        let provider_schema = provider.schema();

        assert!(schema.table_names().contains(&"orders".to_owned()));
        assert_eq!(provider.table_type(), TableType::Base);
        assert_eq!(provider_schema.fields().len(), 2);
        assert_eq!(provider_schema.field(0).name(), "id");
        assert_eq!(provider_schema.field(0).data_type(), &DataType::Int32);
        assert!(!provider_schema.field(0).is_nullable());
        assert_eq!(provider_schema.field(1).name(), "customer_name");
        assert_eq!(provider_schema.field(1).data_type(), &DataType::Utf8);
        assert!(provider_schema.field(1).is_nullable());

        Ok(())
    }

    #[test]
    fn registration_failure_reports_source_context() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("registration-failure")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let ctx = SessionContext::new();
        let placeholder_schema = Arc::new(Schema::new(vec![Field::new(
            "existing",
            DataType::Utf8,
            true,
        )]));

        ctx.register_table("orders", Arc::new(EmptyTable::new(placeholder_schema)))?;
        let result = register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration {
                source_name,
                reason,
                ..
            }) if source_name == "orders" && reason.contains("already exists")
        ));

        Ok(())
    }

    #[test]
    fn mismatched_preflight_is_rejected_before_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("mismatched-preflight-orders")?;
        let customers = DeltaLogTable::new("mismatched-preflight-customers")?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "customers".to_owned(),
            table_uri: customers.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let customers_preflight = preflight_delta_protocol(&customers_source)?;
        let ctx = SessionContext::new();

        let result = register_delta_sources(
            &ctx,
            vec![DeltaTableProviderConfig {
                source: orders_source,
                protocol: customers_preflight,
                scan_target_partitions: None,
            }],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration {
                source_name,
                reason,
                ..
            }) if source_name == "orders"
                && reason.contains("protocol preflight belongs to source `customers`")
        ));
        assert!(!ctx.table_exist("orders")?);

        Ok(())
    }

    #[test]
    fn existing_table_conflict_fails_before_partial_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("existing-conflict-orders")?;
        let customers = DeltaLogTable::new("existing-conflict-customers")?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "customers".to_owned(),
            table_uri: customers.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let orders_preflight = preflight_delta_protocol(&orders_source)?;
        let customers_preflight = preflight_delta_protocol(&customers_source)?;
        let ctx = SessionContext::new();
        let placeholder_schema = Arc::new(Schema::new(vec![Field::new(
            "existing",
            DataType::Utf8,
            true,
        )]));

        ctx.register_table("customers", Arc::new(EmptyTable::new(placeholder_schema)))?;
        let result = register_delta_sources(
            &ctx,
            vec![
                DeltaTableProviderConfig {
                    source: orders_source,
                    protocol: orders_preflight,
                    scan_target_partitions: None,
                },
                DeltaTableProviderConfig {
                    source: customers_source,
                    protocol: customers_preflight,
                    scan_target_partitions: None,
                },
            ],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration {
                source_name,
                reason,
                ..
            }) if source_name == "customers" && reason.contains("already exists")
        ));
        assert!(!ctx.table_exist("orders")?);
        assert!(ctx.table_exist("customers")?);

        Ok(())
    }

    #[test]
    fn late_registration_failure_rolls_back_prior_sources() -> Result<(), Box<dyn std::error::Error>>
    {
        let orders = DeltaLogTable::new("rollback-orders")?;
        let customers = DeltaLogTable::new("rollback-customers")?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "customers".to_owned(),
            table_uri: customers.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let orders_preflight = preflight_delta_protocol(&orders_source)?;
        let customers_preflight = preflight_delta_protocol(&customers_source)?;
        let ctx = SessionContext::new();
        let failing_schema: Arc<dyn datafusion::catalog::SchemaProvider> =
            Arc::new(FailsOnCustomersSchemaProvider::default());

        ctx.register_catalog(
            "datafusion",
            Arc::new(SingleSchemaCatalogProvider::new(Arc::clone(
                &failing_schema,
            ))),
        );
        let result = register_delta_sources(
            &ctx,
            vec![
                DeltaTableProviderConfig {
                    source: orders_source,
                    protocol: orders_preflight,
                    scan_target_partitions: None,
                },
                DeltaTableProviderConfig {
                    source: customers_source,
                    protocol: customers_preflight,
                    scan_target_partitions: None,
                },
            ],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration {
                source_name,
                reason,
                ..
            }) if source_name == "customers"
                && reason.contains("forced customers registration failure")
        ));
        assert!(!ctx.table_exist("orders")?);
        assert!(!ctx.table_exist("customers")?);

        Ok(())
    }

    #[test]
    fn existing_table_conflict_uses_configured_default_catalog_and_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("custom-default-orders")?;
        let customers = DeltaLogTable::new("custom-default-customers")?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "customers".to_owned(),
            table_uri: customers.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let orders_preflight = preflight_delta_protocol(&orders_source)?;
        let customers_preflight = preflight_delta_protocol(&customers_source)?;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_default_catalog_and_schema("custom_catalog", "custom_schema"),
        );
        let placeholder_schema = Arc::new(Schema::new(vec![Field::new(
            "existing",
            DataType::Utf8,
            true,
        )]));

        ctx.register_table("customers", Arc::new(EmptyTable::new(placeholder_schema)))?;
        let result = register_delta_sources(
            &ctx,
            vec![
                DeltaTableProviderConfig {
                    source: orders_source,
                    protocol: orders_preflight,
                    scan_target_partitions: None,
                },
                DeltaTableProviderConfig {
                    source: customers_source,
                    protocol: customers_preflight,
                    scan_target_partitions: None,
                },
            ],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration {
                source_name,
                reason,
                ..
            }) if source_name == "customers" && reason.contains("already exists")
        ));
        assert!(!ctx.table_exist("orders")?);
        assert!(ctx.table_exist("customers")?);

        Ok(())
    }

    #[test]
    fn schema_conversion_failure_fails_before_partial_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("schema-partial-orders")?;
        let customers = DeltaLogTable::new_with_schema(
            "schema-partial-customers",
            INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON,
            "[]",
            r#""partitionValues":{}"#,
        )?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "customers".to_owned(),
            table_uri: customers.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let orders_preflight = preflight_delta_protocol(&orders_source)?;
        let customers_preflight = preflight_delta_protocol(&customers_source)?;
        let ctx = SessionContext::new();

        let result = register_delta_sources(
            &ctx,
            vec![
                DeltaTableProviderConfig {
                    source: orders_source,
                    protocol: orders_preflight,
                    scan_target_partitions: None,
                },
                DeltaTableProviderConfig {
                    source: customers_source,
                    protocol: customers_preflight,
                    scan_target_partitions: None,
                },
            ],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSourceSchema {
                source_name,
                reason,
                ..
            }) if source_name == "customers"
                && reason.contains("bad_array")
                && reason.contains("delta.columnMapping.nested.ids")
        ));
        assert!(!ctx.table_exist("orders")?);
        assert!(!ctx.table_exist("customers")?);

        Ok(())
    }

    #[test]
    fn duplicate_registration_names_fail_before_partial_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("duplicate-orders")?;
        let customers = DeltaLogTable::new("duplicate-customers")?;
        let orders_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: orders.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let customers_source = load_delta_source(DeltaSourceConfig {
            name: "Orders".to_owned(),
            table_uri: customers.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let orders_preflight = preflight_delta_protocol(&orders_source)?;
        let customers_preflight = preflight_delta_protocol(&customers_source)?;
        let ctx = SessionContext::new();

        let result = register_delta_sources(
            &ctx,
            vec![
                DeltaTableProviderConfig {
                    source: orders_source,
                    protocol: orders_preflight,
                    scan_target_partitions: None,
                },
                DeltaTableProviderConfig {
                    source: customers_source,
                    protocol: customers_preflight,
                    scan_target_partitions: None,
                },
            ],
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "Orders"
        ));
        assert!(!ctx.table_exist("orders")?);

        Ok(())
    }
}
