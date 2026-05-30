//! Named Delta source loading.

use crate::delta_kernel_adapter::Version;
use crate::{
    DeltaFunnelError, LoadedDeltaTableSnapshot, load_delta_table_snapshot,
    validate_delta_source_names,
};

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
    validate_delta_source_names([config.name.as_str()])?;

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

    validate_delta_source_names(configs.iter().map(|config| config.name.as_str()))?;

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
}
