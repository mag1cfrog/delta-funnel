//! Delta table-format source loading.

use crate::DeltaFunnelError;

mod kernel;
mod protocol;
mod snapshot;
mod uri;

use super::validate_table_source_names;
use kernel::{ArrowSchemaRef, Version, snapshot_arrow_schema};
pub(crate) use kernel::{
    DeltaKernelPredicate, DeltaKernelPredicateAdapterError, datafusion_expr_to_kernel_predicate,
};
pub use protocol::{
    DeltaProtocolReport, ProtocolPreflight, preflight_delta_protocol, preflight_delta_sources,
};
use snapshot::{LoadedDeltaTableSnapshot, load_delta_table_snapshot};

/// Caller-provided configuration for one named Delta source.
pub struct DeltaSourceConfig {
    /// DataFusion table name that will identify this source.
    pub name: String,
    /// Caller-provided Delta table location.
    pub table_uri: String,
    /// Optional fixed Delta table version.
    pub version: Option<Version>,
}

/// Loaded named Delta source state.
pub struct PlannedDeltaSource {
    name: String,
    requested_table_uri: String,
    snapshot: LoadedDeltaTableSnapshot,
}

/// Kernel-backed scan state for one projected Delta scan.
#[allow(dead_code)]
pub(crate) struct ProjectedDeltaScan {
    scan: kernel::Scan,
    kernel_schema: kernel::KernelSchemaRef,
}

impl ProjectedDeltaScan {
    /// Returns the projected kernel schema selected for this scan.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn kernel_schema(&self) -> &kernel::KernelSchemaRef {
        &self.kernel_schema
    }

    /// Returns the kernel scan handle that later metadata planning will consume.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn kernel_scan(&self) -> &kernel::Scan {
        &self.scan
    }
}

impl PlannedDeltaSource {
    /// DataFusion table name for this source.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Caller-provided Delta table location.
    #[must_use]
    pub fn requested_table_uri(&self) -> &str {
        &self.requested_table_uri
    }

    /// Normalized Delta table URI used for snapshot loading.
    #[must_use]
    pub fn table_uri(&self) -> &str {
        self.loaded_snapshot().table_uri()
    }

    /// Loaded Delta table version.
    #[must_use]
    pub fn version(&self) -> Version {
        self.loaded_snapshot().version()
    }

    pub(crate) fn loaded_snapshot(&self) -> &LoadedDeltaTableSnapshot {
        &self.snapshot
    }
}

pub(crate) fn delta_source_arrow_schema(
    source: &PlannedDeltaSource,
) -> Result<ArrowSchemaRef, String> {
    snapshot_arrow_schema(source.loaded_snapshot().kernel_snapshot())
        .map_err(|error| error.to_string())
}

/// Builds kernel-backed scan state for a loaded Delta source projection.
#[allow(dead_code)]
pub(crate) fn build_projected_delta_scan(
    source: &PlannedDeltaSource,
    projected_column_names: Option<&[String]>,
) -> Result<ProjectedDeltaScan, delta_kernel::Error> {
    let (scan, kernel_schema) = kernel::build_projected_scan(
        source.loaded_snapshot().kernel_snapshot(),
        projected_column_names,
    )?;

    Ok(ProjectedDeltaScan {
        scan,
        kernel_schema,
    })
}

/// Loads one named Delta source.
///
/// Name validation runs before URI normalization, engine construction, or
/// snapshot loading.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::InvalidSourceName`] for an invalid source name,
/// [`DeltaFunnelError::InvalidSourceUri`] for an invalid table URI,
/// [`DeltaFunnelError::DeltaSourceEngine`] when engine construction fails, or
/// [`DeltaFunnelError::DeltaSnapshotLoad`] when the requested snapshot cannot
/// be loaded.
pub fn load_delta_source(
    config: DeltaSourceConfig,
) -> Result<PlannedDeltaSource, DeltaFunnelError> {
    validate_table_source_names([config.name.as_str()])?;

    load_delta_source_after_name_validation(config)
}

/// Loads configured Delta sources after validating all names.
///
/// Name validation and duplicate detection run before any URI normalization,
/// engine construction, or snapshot loading.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::InvalidSourceName`] or
/// [`DeltaFunnelError::DuplicateSourceName`] before loading any snapshot when
/// source names are invalid or ambiguous. Otherwise returns the same URI,
/// engine, and snapshot-loading errors as [`load_delta_source`].
pub fn load_delta_sources<I>(configs: I) -> Result<Vec<PlannedDeltaSource>, DeltaFunnelError>
where
    I: IntoIterator<Item = DeltaSourceConfig>,
{
    let configs: Vec<_> = configs.into_iter().collect();

    validate_table_source_names(configs.iter().map(|config| config.name.as_str()))?;

    configs
        .into_iter()
        .map(load_delta_source_after_name_validation)
        .collect()
}

fn load_delta_source_after_name_validation(
    config: DeltaSourceConfig,
) -> Result<PlannedDeltaSource, DeltaFunnelError> {
    let DeltaSourceConfig {
        name,
        table_uri,
        version,
    } = config;

    let snapshot = load_delta_table_snapshot(&table_uri, version)?;

    Ok(PlannedDeltaSource {
        name,
        requested_table_uri: table_uri,
        snapshot,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{DeltaSourceConfig, load_delta_source, load_delta_sources};
    use crate::DeltaFunnelError;

    struct DeltaLogTable {
        path: PathBuf,
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl DeltaLogTable {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-funnel-named-source-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{PROTOCOL_JSON}\n{METADATA_JSON}\n"),
            )?;
            fs::write(
                log_path.join("00000000000000000001.json"),
                format!("{}\n", add_json("part-00001.parquet")),
            )?;

            Ok(Self { path })
        }
    }

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;

    fn add_json(path: &str) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true}}}}"#
        )
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

        Ok(format!("{}-{}-{nanos}", std::process::id(), name))
    }

    #[test]
    fn loads_named_delta_source() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("success")?;
        let requested_table_uri = table.path.to_string_lossy().to_string();

        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: requested_table_uri.clone(),
            version: None,
        })?;

        assert_eq!(source.name(), "orders");
        assert_eq!(source.requested_table_uri(), requested_table_uri);
        assert!(source.table_uri().starts_with("file://"));
        assert_eq!(source.version(), 1);
        assert_eq!(source.loaded_snapshot().version(), 1);

        Ok(())
    }

    #[test]
    fn loads_named_delta_source_at_fixed_version() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("fixed")?;

        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: Some(0),
        })?;

        assert_eq!(source.version(), 0);

        Ok(())
    }

    #[test]
    fn validates_name_before_table_uri() {
        let result = load_delta_source(DeltaSourceConfig {
            name: "orders.latest".to_owned(),
            table_uri: "missing/path".to_owned(),
            version: None,
        });

        assert!(matches!(
            result,
            Err(DeltaFunnelError::InvalidSourceName { .. })
        ));
    }

    #[test]
    fn rejects_sql_keyword_name_before_table_uri() {
        let result = load_delta_source(DeltaSourceConfig {
            name: "select".to_owned(),
            table_uri: "missing/path".to_owned(),
            version: None,
        });

        assert!(matches!(
            result,
            Err(DeltaFunnelError::InvalidSourceName { name, reason })
                if name == "select" && reason == "source names must not be SQL keywords"
        ));
    }

    #[test]
    fn rejects_blank_table_uri_as_invalid_source_uri() {
        let result = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: " \t\n".to_owned(),
            version: None,
        });

        assert!(matches!(
            result,
            Err(DeltaFunnelError::InvalidSourceUri { reason })
                if reason == "table location must not be empty"
        ));
    }

    #[test]
    fn loads_multiple_named_delta_sources() -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("orders")?;
        let customers = DeltaLogTable::new("customers")?;

        let sources = load_delta_sources([
            DeltaSourceConfig {
                name: "orders".to_owned(),
                table_uri: orders.path.to_string_lossy().to_string(),
                version: None,
            },
            DeltaSourceConfig {
                name: "customers".to_owned(),
                table_uri: customers.path.to_string_lossy().to_string(),
                version: Some(0),
            },
        ])?;

        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].name(), "orders");
        assert_eq!(sources[0].version(), 1);
        assert_eq!(sources[1].name(), "customers");
        assert_eq!(sources[1].version(), 0);

        Ok(())
    }

    #[test]
    fn rejects_duplicate_names_before_table_uri_loading() {
        let result = load_delta_sources([
            DeltaSourceConfig {
                name: "orders".to_owned(),
                table_uri: "missing/orders".to_owned(),
                version: None,
            },
            DeltaSourceConfig {
                name: "Orders".to_owned(),
                table_uri: "missing/customers".to_owned(),
                version: None,
            },
        ]);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "Orders"
        ));
    }

    #[test]
    fn rejects_invalid_name_before_multi_source_table_uri_loading() {
        let result = load_delta_sources([
            DeltaSourceConfig {
                name: "orders".to_owned(),
                table_uri: "missing/orders".to_owned(),
                version: None,
            },
            DeltaSourceConfig {
                name: "customers.latest".to_owned(),
                table_uri: "missing/customers".to_owned(),
                version: None,
            },
        ]);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::InvalidSourceName { name, .. }) if name == "customers.latest"
        ));
    }

    #[test]
    fn source_load_errors_do_not_expose_secret_bearing_uri() {
        let result = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: "ftp://user:password@example.com/table".to_owned(),
            version: None,
        });

        let error = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();

        assert!(!error.contains("user"));
        assert!(!error.contains("password"));
        assert!(!error.contains("example.com"));
        assert!(!error.contains("ftp://"));
    }
}
