//! Delta source snapshot loading.

use crate::DeltaFunnelError;

use super::kernel::{
    DefaultEngineBuilder, Snapshot, SnapshotRef, Version, store_from_url_opts, try_parse_uri,
};
use super::uri::normalize_delta_table_uri;

const ENGINE_CONSTRUCTION_FAILED: &str = "object store engine could not be constructed";
const SNAPSHOT_LOAD_FAILED: &str = "snapshot could not be loaded";

/// Loaded Delta table snapshot state.
///
/// This is intentionally narrower than a named source config. It proves and
/// owns the source-side state that later protocol and DataFusion provider
/// slices can consume without reloading the snapshot.
pub(crate) struct LoadedDeltaTableSnapshot {
    table_uri: String,
    snapshot: SnapshotRef,
}

impl LoadedDeltaTableSnapshot {
    /// Normalized Delta table URI used to load the snapshot.
    #[must_use]
    pub(crate) fn table_uri(&self) -> &str {
        &self.table_uri
    }

    /// Loaded Delta table version.
    #[must_use]
    pub(crate) fn version(&self) -> Version {
        self.kernel_snapshot().version()
    }

    pub(crate) fn kernel_snapshot(&self) -> &SnapshotRef {
        &self.snapshot
    }
}

struct DeltaKernelEngine {
    inner: Box<dyn delta_kernel::Engine + Send + Sync>,
}

impl DeltaKernelEngine {
    fn build(table_uri: &str) -> Result<Self, DeltaFunnelError> {
        let table_url =
            try_parse_uri(table_uri).map_err(|_| DeltaFunnelError::InvalidSourceUri {
                reason: "normalized table URI could not be parsed",
            })?;
        let store =
            store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>()).map_err(|_| {
                DeltaFunnelError::DeltaSourceEngine {
                    reason: ENGINE_CONSTRUCTION_FAILED,
                }
            })?;

        Ok(Self {
            inner: Box::new(DefaultEngineBuilder::new(store).build()),
        })
    }

    fn as_kernel_engine(&self) -> &dyn delta_kernel::Engine {
        self.inner.as_ref()
    }
}

/// Loads the latest or requested snapshot for a Delta table URI.
///
/// The table URI is normalized through [`normalize_delta_table_uri`] before
/// engine construction and snapshot loading.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::InvalidSourceUri`] when the table URI cannot be
/// normalized, [`DeltaFunnelError::DeltaSourceEngine`] when the object-store
/// backed default engine cannot be constructed, or
/// [`DeltaFunnelError::DeltaSnapshotLoad`] when `delta_kernel` cannot load the
/// requested snapshot.
pub(crate) fn load_delta_table_snapshot(
    table_uri: impl AsRef<str>,
    version: Option<Version>,
) -> Result<LoadedDeltaTableSnapshot, DeltaFunnelError> {
    let table_uri = normalize_delta_table_uri(table_uri)?;
    let engine = DeltaKernelEngine::build(&table_uri)?;

    let mut builder = Snapshot::builder_for(&table_uri);
    if let Some(version) = version {
        builder = builder.at_version(version);
    }

    let snapshot = builder.build(engine.as_kernel_engine()).map_err(|_| {
        DeltaFunnelError::DeltaSnapshotLoad {
            reason: SNAPSHOT_LOAD_FAILED,
        }
    })?;

    Ok(LoadedDeltaTableSnapshot {
        table_uri,
        snapshot,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::load_delta_table_snapshot;
    use crate::DeltaFunnelError;

    struct DeltaLogTable {
        path: PathBuf,
    }

    struct TestDir {
        path: PathBuf,
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl DeltaLogTable {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-funnel-snapshot-tests")
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

    impl TestDir {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-funnel-broken-snapshot-tests")
                .join(unique_name(name)?);
            fs::create_dir_all(&path)?;

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
    fn loads_latest_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("latest")?;
        let loaded = load_delta_table_snapshot(table.path.to_string_lossy(), None)?;

        assert_eq!(loaded.version(), 1);
        assert!(loaded.table_uri().starts_with("file://"));
        assert!(loaded.table_uri().ends_with('/'));

        Ok(())
    }

    #[test]
    fn loads_fixed_snapshot_version() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("fixed")?;
        let loaded = load_delta_table_snapshot(table.path.to_string_lossy(), Some(0))?;

        assert_eq!(loaded.version(), 0);

        Ok(())
    }

    #[test]
    fn rejects_missing_fixed_snapshot_version() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("missing-version")?;
        let result = load_delta_table_snapshot(table.path.to_string_lossy(), Some(2));

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSnapshotLoad { .. })
        ));

        Ok(())
    }

    #[test]
    fn rejects_unsupported_object_store_scheme() {
        let result = load_delta_table_snapshot("ftp://example.com/table", None);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSourceEngine { .. })
        ));
    }

    #[test]
    fn rejects_existing_empty_directory_as_snapshot_load_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::new("empty-table")?;
        let result = load_delta_table_snapshot(dir.path.to_string_lossy(), None);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSnapshotLoad { .. })
        ));

        Ok(())
    }

    #[test]
    fn rejects_malformed_commit_json_as_snapshot_load_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::new("malformed-json")?;
        let log_path = dir.path.join("_delta_log");
        fs::create_dir_all(&log_path)?;
        fs::write(log_path.join("00000000000000000000.json"), "{not json\n")?;

        let result = load_delta_table_snapshot(dir.path.to_string_lossy(), None);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSnapshotLoad { .. })
        ));

        Ok(())
    }

    #[test]
    fn rejects_commit_without_protocol_or_metadata_as_snapshot_load_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::new("missing-protocol-metadata")?;
        let log_path = dir.path.join("_delta_log");
        fs::create_dir_all(&log_path)?;
        fs::write(
            log_path.join("00000000000000000000.json"),
            format!("{}\n", add_json("part-00000.parquet")),
        )?;

        let result = load_delta_table_snapshot(dir.path.to_string_lossy(), None);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSnapshotLoad { .. })
        ));

        Ok(())
    }

    #[test]
    fn rejects_regular_file_as_invalid_source_uri() -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::new("regular-file-parent")?;
        let file_path = dir.path.join("not-a-directory");
        fs::write(&file_path, "not a table")?;

        let result = load_delta_table_snapshot(file_path.to_string_lossy(), None);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::InvalidSourceUri { .. })
        ));

        Ok(())
    }

    #[test]
    fn snapshot_errors_do_not_expose_secret_bearing_uri() {
        let result = load_delta_table_snapshot("ftp://user:password@example.com/table", None);
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
