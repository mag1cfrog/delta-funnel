//! Delta table-format source loading.

use crate::DeltaFunnelError;

mod kernel;
mod protocol;
mod snapshot;
mod uri;

use super::validate_table_source_names;
use kernel::{ArrowSchemaRef, Version, snapshot_arrow_schema};
pub(crate) use kernel::{DeltaKernelPredicate, datafusion_expr_to_kernel_predicate};
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

    #[cfg(test)]
    /// Returns scan file paths after kernel scan planning.
    pub(crate) fn scan_file_paths(
        &self,
        table_uri: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        fn collect_scan_file(files: &mut Vec<kernel::ScanFile>, file: kernel::ScanFile) {
            files.push(file);
        }

        let table_url = kernel::try_parse_uri(table_uri)?;
        let store = kernel::store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>())?;
        let engine = kernel::DefaultEngineBuilder::new(store).build();
        let mut files = Vec::new();

        for scan_metadata in self.scan.scan_metadata(&engine)? {
            files = scan_metadata?.visit_scan_files(files, collect_scan_file)?;
        }

        let mut paths = files.into_iter().map(|file| file.path).collect::<Vec<_>>();
        paths.sort();
        Ok(paths)
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
    build_projected_predicated_delta_scan(source, projected_column_names, None)
}

/// Builds kernel-backed scan state for a loaded Delta source projection and predicate.
///
/// `projected_column_names` is the schema passed to delta_kernel's scan
/// builder, not necessarily the final DataFusion output schema. Callers may
/// include hidden predicate columns here when the final projection omits them.
#[allow(dead_code)]
pub(crate) fn build_projected_predicated_delta_scan(
    source: &PlannedDeltaSource,
    projected_column_names: Option<&[String]>,
    predicate: Option<DeltaKernelPredicate>,
) -> Result<ProjectedDeltaScan, delta_kernel::Error> {
    let (scan, kernel_schema) = kernel::build_projected_predicated_scan(
        source.loaded_snapshot().kernel_snapshot(),
        projected_column_names,
        predicate,
    )?;

    Ok(ProjectedDeltaScan {
        scan,
        kernel_schema,
    })
}

/// Builds kernel-backed scan state with parsed file stats exposed in scan metadata.
#[allow(dead_code)]
pub(crate) fn build_projected_predicated_stats_delta_scan(
    source: &PlannedDeltaSource,
    projected_column_names: Option<&[String]>,
    predicate: Option<DeltaKernelPredicate>,
) -> Result<ProjectedDeltaScan, delta_kernel::Error> {
    let (scan, kernel_schema) = kernel::build_projected_predicated_stats_scan(
        source.loaded_snapshot().kernel_snapshot(),
        projected_column_names,
        predicate,
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

    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, col, lit};
    use delta_kernel::arrow::array::{Array, StructArray};
    use delta_kernel::arrow::record_batch::RecordBatch;

    use super::kernel::{ColumnName, Expression, Predicate, Scalar};
    use super::{
        DeltaKernelPredicate, DeltaSourceConfig, ProjectedDeltaScan, build_projected_delta_scan,
        build_projected_predicated_delta_scan, datafusion_expr_to_kernel_predicate,
        load_delta_source, load_delta_sources,
    };
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
            Self::new_with_metadata_and_adds(name, METADATA_JSON, &[add_json("part-00001.parquet")])
        }

        fn new_with_metadata_and_adds(
            name: &str,
            metadata_json: &str,
            add_jsons: &[String],
        ) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_protocol_metadata_and_adds(name, PROTOCOL_JSON, metadata_json, add_jsons)
        }

        fn new_with_protocol_metadata_and_adds(
            name: &str,
            protocol_json: &str,
            metadata_json: &str,
            add_jsons: &[String],
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-funnel-named-source-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{protocol_json}\n{metadata_json}\n"),
            )?;
            fs::write(log_path.join("00000000000000000001.json"), {
                let mut actions = add_jsons.join("\n");
                actions.push('\n');
                actions
            })?;

            Ok(Self { path })
        }
    }

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const BOOLEAN_DATA_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"is_current\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const BINARY_DATA_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"payload\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const DATE_DATA_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"event_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const DECIMAL_DATA_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const FLOATING_DATA_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"float_score\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"double_score\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const STRING_DATA_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const TIMESTAMP_DATA_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"event_ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const TIMESTAMP_NTZ_DATA_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"event_ts_ntz\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["region"],"configuration":{},"createdTime":1587968585495}}"#;
    const INTEGER_PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"byte_part\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"short_part\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"int_part\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"long_part\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["byte_part","short_part","int_part","long_part"],"configuration":{},"createdTime":1587968585495}}"#;
    const BOOLEAN_PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"is_current\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["is_current"],"configuration":{},"createdTime":1587968585495}}"#;
    const DATE_PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["event_date"],"configuration":{},"createdTime":1587968585495}}"#;
    const DECIMAL_PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["amount"],"configuration":{},"createdTime":1587968585495}}"#;
    const FLOATING_PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"float_part\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"double_part\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["float_part","double_part"],"configuration":{},"createdTime":1587968585495}}"#;
    const BINARY_PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"payload\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["payload"],"configuration":{},"createdTime":1587968585495}}"#;
    const TIMESTAMP_PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["event_ts"],"configuration":{},"createdTime":1587968585495}}"#;
    const TIMESTAMP_NTZ_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#;
    const TIMESTAMP_NTZ_PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_ts_ntz\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["event_ts_ntz"],"configuration":{},"createdTime":1587968585495}}"#;
    const DELETION_VECTOR_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#;

    fn add_json(path: &str) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true}}}}"#
        )
    }

    fn partitioned_add_json(path: &str, partition_values_json: &str) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{partition_values_json},"size":0,"modificationTime":1587968586000,"dataChange":true}}}}"#
        )
    }

    fn add_json_with_id_stats(
        path: &str,
        num_records: i64,
        min_value: i32,
        max_value: i32,
        null_count: i64,
    ) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"id\":{min_value}}},\"maxValues\":{{\"id\":{max_value}}},\"nullCount\":{{\"id\":{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_boolean_stats(
        path: &str,
        num_records: i64,
        min_value: bool,
        max_value: bool,
        null_count: i64,
    ) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"is_current\":{min_value}}},\"maxValues\":{{\"is_current\":{max_value}}},\"nullCount\":{{\"is_current\":{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_partial_boolean_stats(
        path: &str,
        num_records: i64,
        min_value: Option<bool>,
        max_value: Option<bool>,
        null_count: Option<i64>,
    ) -> String {
        let min_values = min_value
            .map(|value| format!(r#"\"is_current\":{value}"#))
            .unwrap_or_default();
        let max_values = max_value
            .map(|value| format!(r#"\"is_current\":{value}"#))
            .unwrap_or_default();
        let null_count = null_count
            .map(|value| format!(r#"\"is_current\":{value}"#))
            .unwrap_or_default();

        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{{min_values}}},\"maxValues\":{{{max_values}}},\"nullCount\":{{{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_date_stats(
        path: &str,
        num_records: i64,
        min_value: &str,
        max_value: &str,
        null_count: i64,
    ) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"event_date\":\"{min_value}\"}},\"maxValues\":{{\"event_date\":\"{max_value}\"}},\"nullCount\":{{\"event_date\":{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_partial_date_stats(
        path: &str,
        num_records: i64,
        min_value: Option<&str>,
        max_value: Option<&str>,
        null_count: Option<i64>,
    ) -> String {
        let min_values = min_value
            .map(|value| format!(r#"\"event_date\":\"{value}\""#))
            .unwrap_or_default();
        let max_values = max_value
            .map(|value| format!(r#"\"event_date\":\"{value}\""#))
            .unwrap_or_default();
        let null_count = null_count
            .map(|value| format!(r#"\"event_date\":{value}"#))
            .unwrap_or_default();

        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{{min_values}}},\"maxValues\":{{{max_values}}},\"nullCount\":{{{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_decimal_stats(
        path: &str,
        num_records: i64,
        min_value: &str,
        max_value: &str,
        null_count: i64,
    ) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"amount\":\"{min_value}\"}},\"maxValues\":{{\"amount\":\"{max_value}\"}},\"nullCount\":{{\"amount\":{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_partial_decimal_stats(
        path: &str,
        num_records: i64,
        min_value: Option<&str>,
        max_value: Option<&str>,
        null_count: Option<i64>,
    ) -> String {
        let min_values = min_value
            .map(|value| format!(r#"\"amount\":\"{value}\""#))
            .unwrap_or_default();
        let max_values = max_value
            .map(|value| format!(r#"\"amount\":\"{value}\""#))
            .unwrap_or_default();
        let null_count = null_count
            .map(|value| format!(r#"\"amount\":{value}"#))
            .unwrap_or_default();

        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{{min_values}}},\"maxValues\":{{{max_values}}},\"nullCount\":{{{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_binary_stats(
        path: &str,
        num_records: i64,
        min_value: &str,
        max_value: &str,
        null_count: i64,
    ) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"payload\":\"{min_value}\"}},\"maxValues\":{{\"payload\":\"{max_value}\"}},\"nullCount\":{{\"payload\":{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_partial_binary_stats(
        path: &str,
        num_records: i64,
        min_value: Option<&str>,
        max_value: Option<&str>,
        null_count: Option<i64>,
    ) -> String {
        let min_values = min_value
            .map(|value| format!(r#"\"payload\":\"{value}\""#))
            .unwrap_or_default();
        let max_values = max_value
            .map(|value| format!(r#"\"payload\":\"{value}\""#))
            .unwrap_or_default();
        let null_count = null_count
            .map(|value| format!(r#"\"payload\":{value}"#))
            .unwrap_or_default();

        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{{min_values}}},\"maxValues\":{{{max_values}}},\"nullCount\":{{{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_floating_stats(
        path: &str,
        num_records: i64,
        float_min_value: &str,
        float_max_value: &str,
        double_min_value: &str,
        double_max_value: &str,
        null_count: i64,
    ) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"float_score\":{float_min_value},\"double_score\":{double_min_value}}},\"maxValues\":{{\"float_score\":{float_max_value},\"double_score\":{double_max_value}}},\"nullCount\":{{\"float_score\":{null_count},\"double_score\":{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_partial_floating_stats(
        path: &str,
        num_records: i64,
        float_min_value: Option<&str>,
        float_max_value: Option<&str>,
        double_min_value: Option<&str>,
        double_max_value: Option<&str>,
        null_count: Option<i64>,
    ) -> String {
        let mut min_values = Vec::new();
        if let Some(value) = float_min_value {
            min_values.push(format!(r#"\"float_score\":{value}"#));
        }
        if let Some(value) = double_min_value {
            min_values.push(format!(r#"\"double_score\":{value}"#));
        }

        let mut max_values = Vec::new();
        if let Some(value) = float_max_value {
            max_values.push(format!(r#"\"float_score\":{value}"#));
        }
        if let Some(value) = double_max_value {
            max_values.push(format!(r#"\"double_score\":{value}"#));
        }

        let null_count = null_count
            .map(|value| format!(r#"\"float_score\":{value},\"double_score\":{value}"#))
            .unwrap_or_default();

        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{{}}},\"maxValues\":{{{}}},\"nullCount\":{{{null_count}}}}}"}}}}"#,
            min_values.join(","),
            max_values.join(",")
        )
    }

    fn add_json_with_string_stats(
        path: &str,
        num_records: i64,
        min_value: &str,
        max_value: &str,
        null_count: i64,
    ) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"customer_name\":\"{min_value}\"}},\"maxValues\":{{\"customer_name\":\"{max_value}\"}},\"nullCount\":{{\"customer_name\":{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_partial_string_stats(
        path: &str,
        num_records: i64,
        min_value: Option<&str>,
        max_value: Option<&str>,
        null_count: Option<i64>,
    ) -> String {
        let min_values = min_value
            .map(|value| format!(r#"\"customer_name\":\"{value}\""#))
            .unwrap_or_default();
        let max_values = max_value
            .map(|value| format!(r#"\"customer_name\":\"{value}\""#))
            .unwrap_or_default();
        let null_count = null_count
            .map(|value| format!(r#"\"customer_name\":{value}"#))
            .unwrap_or_default();

        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{{min_values}}},\"maxValues\":{{{max_values}}},\"nullCount\":{{{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_timestamp_stats(
        path: &str,
        column_name: &str,
        num_records: i64,
        min_value: &str,
        max_value: &str,
        null_count: i64,
    ) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"{column_name}\":\"{min_value}\"}},\"maxValues\":{{\"{column_name}\":\"{max_value}\"}},\"nullCount\":{{\"{column_name}\":{null_count}}}}}"}}}}"#
        )
    }

    fn add_json_with_partial_timestamp_stats(
        path: &str,
        column_name: &str,
        num_records: i64,
        min_value: Option<&str>,
        max_value: Option<&str>,
        null_count: Option<i64>,
    ) -> String {
        let min_values = min_value
            .map(|value| format!(r#"\"{column_name}\":\"{value}\""#))
            .unwrap_or_default();
        let max_values = max_value
            .map(|value| format!(r#"\"{column_name}\":\"{value}\""#))
            .unwrap_or_default();
        let null_count = null_count
            .map(|value| format!(r#"\"{column_name}\":{value}"#))
            .unwrap_or_default();

        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{{min_values}}},\"maxValues\":{{{max_values}}},\"nullCount\":{{{null_count}}}}}"}}}}"#
        )
    }

    fn dv_add_json_with_id_stats(
        path: &str,
        num_records: i64,
        min_value: i32,
        max_value: i32,
        null_count: i64,
    ) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"id\":{min_value}}},\"maxValues\":{{\"id\":{max_value}}},\"nullCount\":{{\"id\":{null_count}}}}}","deletionVector":{{"storageType":"u","pathOrInlineDv":"vBn[lx{{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}}}}"#
        )
    }

    fn partitioned_dv_add_json(path: &str, partition_values_json: &str) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{partition_values_json},"size":0,"modificationTime":1587968586000,"dataChange":true,"deletionVector":{{"storageType":"u","pathOrInlineDv":"vBn[lx{{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}}}}"#
        )
    }

    fn integer_partitioned_add_json(path: &str, value: &str) -> String {
        partitioned_add_json(
            path,
            &format!(
                r#"{{"byte_part":"{value}","short_part":"{value}","int_part":"{value}","long_part":"{value}"}}"#
            ),
        )
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

        Ok(format!("{}-{}-{nanos}", std::process::id(), name))
    }

    fn kernel_scan_file_paths(
        scan: &ProjectedDeltaScan,
        table_uri: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        fn collect_scan_file(
            files: &mut Vec<super::kernel::ScanFile>,
            file: super::kernel::ScanFile,
        ) {
            files.push(file);
        }

        let table_url = super::kernel::try_parse_uri(table_uri)?;
        let store =
            super::kernel::store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>())?;
        let engine = super::kernel::DefaultEngineBuilder::new(store).build();
        let mut files = Vec::new();

        for scan_metadata in scan.kernel_scan().scan_metadata(&engine)? {
            files = scan_metadata?.visit_scan_files(files, collect_scan_file)?;
        }

        let mut paths = files.into_iter().map(|file| file.path).collect::<Vec<_>>();
        paths.sort();
        Ok(paths)
    }

    #[derive(Debug, PartialEq, Eq)]
    struct KernelScanFileBoundary {
        path: String,
        has_deletion_vector: bool,
    }

    fn kernel_scan_file_boundaries(
        scan: &ProjectedDeltaScan,
        table_uri: &str,
    ) -> Result<Vec<KernelScanFileBoundary>, Box<dyn std::error::Error>> {
        fn collect_scan_file(
            files: &mut Vec<KernelScanFileBoundary>,
            file: super::kernel::ScanFile,
        ) {
            files.push(KernelScanFileBoundary {
                path: file.path,
                has_deletion_vector: file.dv_info.has_vector(),
            });
        }

        let table_url = super::kernel::try_parse_uri(table_uri)?;
        let store =
            super::kernel::store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>())?;
        let engine = super::kernel::DefaultEngineBuilder::new(store).build();
        let mut files = Vec::new();

        for scan_metadata in scan.kernel_scan().scan_metadata(&engine)? {
            files = scan_metadata?.visit_scan_files(files, collect_scan_file)?;
        }

        files.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(files)
    }

    fn kernel_partition_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-partition-characterization",
            PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json("region-us-west.parquet", r#"{"region":"us-west"}"#),
                partitioned_add_json("region-us-east.parquet", r#"{"region":"us-east"}"#),
                partitioned_add_json("region-null.parquet", r#"{"region":null}"#),
                partitioned_add_json("region-missing.parquet", r#"{}"#),
                partitioned_add_json("region-empty-string.parquet", r#"{"region":""}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_integer_partition_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-integer-partition-characterization",
            INTEGER_PARTITIONED_METADATA_JSON,
            &[
                integer_partitioned_add_json("integer--1.parquet", "-1"),
                integer_partitioned_add_json("integer-2.parquet", "2"),
                integer_partitioned_add_json("integer-10.parquet", "10"),
                partitioned_add_json(
                    "integer-null.parquet",
                    r#"{"byte_part":null,"short_part":null,"int_part":null,"long_part":null}"#,
                ),
                integer_partitioned_add_json("integer-empty.parquet", ""),
                partitioned_add_json("integer-missing.parquet", r#"{}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_boolean_partition_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-boolean-partition-characterization",
            BOOLEAN_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json("boolean-true.parquet", r#"{"is_current":"true"}"#),
                partitioned_add_json("boolean-false.parquet", r#"{"is_current":"false"}"#),
                partitioned_add_json("boolean-null.parquet", r#"{"is_current":null}"#),
                partitioned_add_json("boolean-empty.parquet", r#"{"is_current":""}"#),
                partitioned_add_json("boolean-missing.parquet", r#"{}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_date_partition_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-date-partition-characterization",
            DATE_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json("date-pre-epoch.parquet", r#"{"event_date":"1969-12-31"}"#),
                partitioned_add_json("date-epoch.parquet", r#"{"event_date":"1970-01-01"}"#),
                partitioned_add_json("date-leap-day.parquet", r#"{"event_date":"2024-02-29"}"#),
                partitioned_add_json("date-new-year.parquet", r#"{"event_date":"2026-01-01"}"#),
                partitioned_add_json("date-null.parquet", r#"{"event_date":null}"#),
                partitioned_add_json("date-empty.parquet", r#"{"event_date":""}"#),
                partitioned_add_json("date-missing.parquet", r#"{}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_decimal_partition_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-decimal-partition-characterization",
            DECIMAL_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json("decimal-negative.parquet", r#"{"amount":"-1.23"}"#),
                partitioned_add_json("decimal-zero.parquet", r#"{"amount":"0.00"}"#),
                partitioned_add_json("decimal-two.parquet", r#"{"amount":"2.00"}"#),
                partitioned_add_json("decimal-ten.parquet", r#"{"amount":"10.00"}"#),
                partitioned_add_json("decimal-large.parquet", r#"{"amount":"123.45"}"#),
                partitioned_add_json("decimal-null.parquet", r#"{"amount":null}"#),
                partitioned_add_json("decimal-empty.parquet", r#"{"amount":""}"#),
                partitioned_add_json("decimal-missing.parquet", r#"{}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_floating_partition_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-floating-partition-characterization",
            FLOATING_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json(
                    "floating-neg.parquet",
                    r#"{"float_part":"-1.5","double_part":"-2.25"}"#,
                ),
                partitioned_add_json(
                    "floating-neg-zero.parquet",
                    r#"{"float_part":"-0.0","double_part":"-0.0"}"#,
                ),
                partitioned_add_json(
                    "floating-pos-zero.parquet",
                    r#"{"float_part":"0.0","double_part":"0.0"}"#,
                ),
                partitioned_add_json(
                    "floating-one.parquet",
                    r#"{"float_part":"1.5","double_part":"2.25"}"#,
                ),
                partitioned_add_json(
                    "floating-ten.parquet",
                    r#"{"float_part":"10.0","double_part":"10.0"}"#,
                ),
                partitioned_add_json(
                    "floating-null.parquet",
                    r#"{"float_part":null,"double_part":null}"#,
                ),
                partitioned_add_json(
                    "floating-empty.parquet",
                    r#"{"float_part":"","double_part":""}"#,
                ),
                partitioned_add_json("floating-missing.parquet", r#"{}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_binary_partition_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-binary-partition-characterization",
            BINARY_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json("binary-HELLO.parquet", r#"{"payload":"HELLO"}"#),
                partitioned_add_json("binary-hello.parquet", r#"{"payload":"hello"}"#),
                partitioned_add_json("binary-special.parquet", r#"{"payload":"/=%"}"#),
                partitioned_add_json("binary-null.parquet", r#"{"payload":null}"#),
                partitioned_add_json("binary-empty.parquet", r#"{"payload":""}"#),
                partitioned_add_json("binary-missing.parquet", r#"{}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_timestamp_partition_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-timestamp-partition-characterization",
            TIMESTAMP_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json(
                    "timestamp-pre-epoch.parquet",
                    r#"{"event_ts":"1969-12-31T23:59:59.999999Z"}"#,
                ),
                partitioned_add_json(
                    "timestamp-low.parquet",
                    r#"{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
                ),
                partitioned_add_json(
                    "timestamp-target.parquet",
                    r#"{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
                ),
                partitioned_add_json(
                    "timestamp-high.parquet",
                    r#"{"event_ts":"2026-01-01T00:00:00.123457Z"}"#,
                ),
                partitioned_add_json("timestamp-null.parquet", r#"{"event_ts":null}"#),
                partitioned_add_json("timestamp-empty.parquet", r#"{"event_ts":""}"#),
                partitioned_add_json("timestamp-missing.parquet", r#"{}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_timestamp_ntz_partition_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "kernel-timestamp-ntz-partition-characterization",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json(
                    "timestamp-ntz-pre-epoch.parquet",
                    r#"{"event_ts_ntz":"1969-12-31 23:59:59.999999"}"#,
                ),
                partitioned_add_json(
                    "timestamp-ntz-low-space.parquet",
                    r#"{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
                ),
                partitioned_add_json(
                    "timestamp-ntz-target-space.parquet",
                    r#"{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
                ),
                partitioned_add_json(
                    "timestamp-ntz-high.parquet",
                    r#"{"event_ts_ntz":"2026-01-01 00:00:00.123457"}"#,
                ),
                partitioned_add_json("timestamp-ntz-null.parquet", r#"{"event_ts_ntz":null}"#),
                partitioned_add_json("timestamp-ntz-empty.parquet", r#"{"event_ts_ntz":""}"#),
                partitioned_add_json("timestamp-ntz-missing.parquet", r#"{}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-data-stats-characterization",
            METADATA_JSON,
            &[
                add_json_with_id_stats("id-impossible.parquet", 10, 1, 50, 0),
                add_json_with_id_stats("id-possible.parquet", 10, 101, 150, 0),
                add_json("id-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_boolean_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-boolean-data-stats-characterization",
            BOOLEAN_DATA_METADATA_JSON,
            &[
                add_json_with_boolean_stats("boolean-false-only.parquet", 10, false, false, 0),
                add_json_with_boolean_stats("boolean-true-only.parquet", 10, true, true, 0),
                add_json_with_boolean_stats("boolean-mixed.parquet", 10, false, true, 0),
                add_json_with_boolean_stats("boolean-false-with-null.parquet", 10, false, false, 2),
                add_json_with_boolean_stats("boolean-true-with-null.parquet", 10, true, true, 2),
                add_json_with_partial_boolean_stats(
                    "boolean-all-null.parquet",
                    10,
                    None,
                    None,
                    Some(10),
                ),
                add_json("boolean-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_partial_boolean_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-partial-boolean-data-stats-characterization",
            BOOLEAN_DATA_METADATA_JSON,
            &[
                add_json_with_partial_boolean_stats(
                    "boolean-min-only-false.parquet",
                    10,
                    Some(false),
                    None,
                    Some(0),
                ),
                add_json_with_partial_boolean_stats(
                    "boolean-max-only-true.parquet",
                    10,
                    None,
                    Some(true),
                    Some(0),
                ),
                add_json_with_partial_boolean_stats(
                    "boolean-counts-only.parquet",
                    10,
                    None,
                    None,
                    Some(0),
                ),
                add_json_with_partial_boolean_stats(
                    "boolean-missing-null-count.parquet",
                    10,
                    Some(false),
                    Some(true),
                    None,
                ),
                add_json("boolean-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn assert_boolean_stats_min_max_error(
        result: Result<Vec<String>, Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = match result {
            Ok(paths) => {
                return Err(format!(
                    "boolean min/max stats predicate should fail kernel scan: {paths:?}"
                )
                .into());
            }
            Err(error) => error,
        };
        let message = error.to_string();
        let debug_message = format!("{error:?}");
        assert!(
            message.contains("minValues") || debug_message.contains("minValues"),
            "{message}\n{debug_message}"
        );

        Ok(())
    }

    fn assert_binary_stats_min_max_error(
        result: Result<Vec<String>, Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = match result {
            Ok(paths) => {
                return Err(format!(
                    "binary min/max stats predicate should fail kernel scan: {paths:?}"
                )
                .into());
            }
            Err(error) => error,
        };
        let message = error.to_string();
        let debug_message = format!("{error:?}");
        assert!(
            message.contains("minValues")
                || message.contains("maxValues")
                || debug_message.contains("minValues")
                || debug_message.contains("maxValues"),
            "{message}\n{debug_message}"
        );

        Ok(())
    }

    fn assert_unsupported_literal_error(
        result: Result<Vec<String>, Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = match result {
            Ok(paths) => {
                return Err(format!(
                    "unsupported literal predicate should fail before kernel scan: {paths:?}"
                )
                .into());
            }
            Err(error) => error,
        };
        let message = error.to_string();
        let debug_message = format!("{error:?}");
        assert!(
            message.contains("unsupported DataFusion literal")
                || debug_message.contains("UnsupportedLiteral"),
            "{message}\n{debug_message}"
        );

        Ok(())
    }

    fn kernel_date_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-date-data-stats-characterization",
            DATE_DATA_METADATA_JSON,
            &[
                add_json_with_date_stats(
                    "date-pre-epoch-only.parquet",
                    10,
                    "1969-12-31",
                    "1969-12-31",
                    0,
                ),
                add_json_with_date_stats(
                    "date-leap-only.parquet",
                    10,
                    "2024-02-29",
                    "2024-02-29",
                    0,
                ),
                add_json_with_date_stats(
                    "date-new-year-only.parquet",
                    10,
                    "2026-01-01",
                    "2026-01-01",
                    0,
                ),
                add_json_with_date_stats("date-range.parquet", 10, "2024-02-29", "2026-01-01", 0),
                add_json_with_date_stats(
                    "date-new-year-with-null.parquet",
                    10,
                    "2026-01-01",
                    "2026-01-01",
                    2,
                ),
                add_json_with_partial_date_stats("date-all-null.parquet", 10, None, None, Some(10)),
                add_json("date-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_partial_date_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-partial-date-data-stats-characterization",
            DATE_DATA_METADATA_JSON,
            &[
                add_json_with_partial_date_stats(
                    "date-min-only-high.parquet",
                    10,
                    Some("2026-01-01"),
                    None,
                    Some(0),
                ),
                add_json_with_partial_date_stats(
                    "date-max-only-low.parquet",
                    10,
                    None,
                    Some("2024-02-29"),
                    Some(0),
                ),
                add_json_with_partial_date_stats(
                    "date-counts-only.parquet",
                    10,
                    None,
                    None,
                    Some(0),
                ),
                add_json_with_partial_date_stats(
                    "date-missing-null-count.parquet",
                    10,
                    Some("2024-02-29"),
                    Some("2026-01-01"),
                    None,
                ),
                add_json("date-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_decimal_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-decimal-data-stats-characterization",
            DECIMAL_DATA_METADATA_JSON,
            &[
                add_json_with_decimal_stats(
                    "decimal-negative-only.parquet",
                    10,
                    "-1.23",
                    "-1.23",
                    0,
                ),
                add_json_with_decimal_stats("decimal-zero-only.parquet", 10, "0.00", "0.00", 0),
                add_json_with_decimal_stats("decimal-two-only.parquet", 10, "2.00", "2.00", 0),
                add_json_with_decimal_stats("decimal-ten-only.parquet", 10, "10.00", "10.00", 0),
                add_json_with_decimal_stats(
                    "decimal-large-only.parquet",
                    10,
                    "123.45",
                    "123.45",
                    0,
                ),
                add_json_with_decimal_stats("decimal-range.parquet", 10, "0.00", "10.00", 0),
                add_json_with_decimal_stats("decimal-two-with-null.parquet", 10, "2.00", "2.00", 2),
                add_json_with_partial_decimal_stats(
                    "decimal-all-null.parquet",
                    10,
                    None,
                    None,
                    Some(10),
                ),
                add_json("decimal-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_partial_decimal_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-partial-decimal-data-stats-characterization",
            DECIMAL_DATA_METADATA_JSON,
            &[
                add_json_with_partial_decimal_stats(
                    "decimal-min-only-high.parquet",
                    10,
                    Some("10.00"),
                    None,
                    Some(0),
                ),
                add_json_with_partial_decimal_stats(
                    "decimal-max-only-low.parquet",
                    10,
                    None,
                    Some("0.00"),
                    Some(0),
                ),
                add_json_with_partial_decimal_stats(
                    "decimal-counts-only.parquet",
                    10,
                    None,
                    None,
                    Some(0),
                ),
                add_json_with_partial_decimal_stats(
                    "decimal-missing-null-count.parquet",
                    10,
                    Some("0.00"),
                    Some("10.00"),
                    None,
                ),
                add_json("decimal-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_binary_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-binary-data-stats-characterization",
            BINARY_DATA_METADATA_JSON,
            &[
                add_json_with_binary_stats("binary-HELLO.parquet", 10, "HELLO", "HELLO", 0),
                add_json_with_binary_stats("binary-empty.parquet", 10, "", "", 0),
                add_json_with_binary_stats("binary-hello.parquet", 10, "hello", "hello", 0),
                add_json_with_binary_stats("binary-range.parquet", 10, "a", "z", 0),
                add_json_with_binary_stats("binary-special.parquet", 10, "/=%", "/=%", 0),
                add_json_with_binary_stats("binary-with-null.parquet", 10, "hello", "hello", 2),
                add_json_with_partial_binary_stats(
                    "binary-all-null.parquet",
                    10,
                    None,
                    None,
                    Some(10),
                ),
                add_json("binary-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_partial_binary_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-partial-binary-data-stats-characterization",
            BINARY_DATA_METADATA_JSON,
            &[
                add_json_with_partial_binary_stats(
                    "binary-min-only-high.parquet",
                    10,
                    Some("m"),
                    None,
                    Some(0),
                ),
                add_json_with_partial_binary_stats(
                    "binary-max-only-low.parquet",
                    10,
                    None,
                    Some("a"),
                    Some(0),
                ),
                add_json_with_partial_binary_stats(
                    "binary-counts-only.parquet",
                    10,
                    None,
                    None,
                    Some(0),
                ),
                add_json_with_partial_binary_stats(
                    "binary-missing-null-count.parquet",
                    10,
                    Some("a"),
                    Some("z"),
                    None,
                ),
                add_json("binary-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_floating_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-floating-data-stats-characterization",
            FLOATING_DATA_METADATA_JSON,
            &[
                add_json_with_floating_stats(
                    "floating-neg.parquet",
                    10,
                    "-1.5",
                    "-1.5",
                    "-2.25",
                    "-2.25",
                    0,
                ),
                add_json_with_floating_stats(
                    "floating-neg-zero.parquet",
                    10,
                    "-0.0",
                    "-0.0",
                    "-0.0",
                    "-0.0",
                    0,
                ),
                add_json_with_floating_stats(
                    "floating-pos-zero.parquet",
                    10,
                    "0.0",
                    "0.0",
                    "0.0",
                    "0.0",
                    0,
                ),
                add_json_with_floating_stats(
                    "floating-one.parquet",
                    10,
                    "1.5",
                    "1.5",
                    "2.25",
                    "2.25",
                    0,
                ),
                add_json_with_floating_stats(
                    "floating-range.parquet",
                    10,
                    "-1.0",
                    "2.0",
                    "-2.0",
                    "3.0",
                    0,
                ),
                add_json_with_floating_stats(
                    "floating-ten.parquet",
                    10,
                    "10.0",
                    "10.0",
                    "10.0",
                    "10.0",
                    0,
                ),
                add_json_with_floating_stats(
                    "floating-one-with-null.parquet",
                    10,
                    "1.5",
                    "1.5",
                    "2.25",
                    "2.25",
                    2,
                ),
                add_json_with_partial_floating_stats(
                    "floating-all-null.parquet",
                    10,
                    None,
                    None,
                    None,
                    None,
                    Some(10),
                ),
                add_json("floating-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_partial_floating_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-partial-floating-data-stats-characterization",
            FLOATING_DATA_METADATA_JSON,
            &[
                add_json_with_partial_floating_stats(
                    "floating-min-only-high.parquet",
                    10,
                    Some("2.0"),
                    None,
                    Some("2.0"),
                    None,
                    Some(0),
                ),
                add_json_with_partial_floating_stats(
                    "floating-max-only-low.parquet",
                    10,
                    None,
                    Some("0.0"),
                    None,
                    Some("0.0"),
                    Some(0),
                ),
                add_json_with_partial_floating_stats(
                    "floating-counts-only.parquet",
                    10,
                    None,
                    None,
                    None,
                    None,
                    Some(0),
                ),
                add_json_with_partial_floating_stats(
                    "floating-missing-null-count.parquet",
                    10,
                    Some("-1.0"),
                    Some("2.0"),
                    Some("-1.0"),
                    Some("2.0"),
                    None,
                ),
                add_json("floating-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_nonfinite_floating_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-nonfinite-floating-data-stats-characterization",
            FLOATING_DATA_METADATA_JSON,
            &[
                add_json_with_floating_stats(
                    "floating-valid.parquet",
                    10,
                    "1.5",
                    "1.5",
                    "2.25",
                    "2.25",
                    0,
                ),
                add_json_with_floating_stats(
                    "floating-nan.parquet",
                    10,
                    "\\\"NaN\\\"",
                    "\\\"NaN\\\"",
                    "\\\"NaN\\\"",
                    "\\\"NaN\\\"",
                    0,
                ),
                add_json_with_floating_stats(
                    "floating-inf.parquet",
                    10,
                    "\\\"Infinity\\\"",
                    "\\\"Infinity\\\"",
                    "\\\"Infinity\\\"",
                    "\\\"Infinity\\\"",
                    0,
                ),
                add_json_with_floating_stats(
                    "floating-neg-inf.parquet",
                    10,
                    "\\\"-Infinity\\\"",
                    "\\\"-Infinity\\\"",
                    "\\\"-Infinity\\\"",
                    "\\\"-Infinity\\\"",
                    0,
                ),
                add_json("floating-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_string_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-string-data-stats-characterization",
            STRING_DATA_METADATA_JSON,
            &[
                add_json_with_string_stats("string-empty-only.parquet", 10, "", "", 0),
                add_json_with_string_stats(
                    "string-mixed-case-only.parquet",
                    10,
                    "Alice",
                    "Alice",
                    0,
                ),
                add_json_with_string_stats("string-alice-only.parquet", 10, "alice", "alice", 0),
                add_json_with_string_stats("string-bob-only.parquet", 10, "bob", "bob", 0),
                add_json_with_string_stats("string-range.parquet", 10, "alice", "morgan", 0),
                add_json_with_string_stats("string-zed-only.parquet", 10, "zed", "zed", 0),
                add_json_with_string_stats(
                    "string-alice-with-null.parquet",
                    10,
                    "alice",
                    "alice",
                    2,
                ),
                add_json_with_partial_string_stats(
                    "string-all-null.parquet",
                    10,
                    None,
                    None,
                    Some(10),
                ),
                add_json("string-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_partial_string_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-partial-string-data-stats-characterization",
            STRING_DATA_METADATA_JSON,
            &[
                add_json_with_partial_string_stats(
                    "string-min-only-morgan.parquet",
                    10,
                    Some("morgan"),
                    None,
                    Some(0),
                ),
                add_json_with_partial_string_stats(
                    "string-max-only-alice.parquet",
                    10,
                    None,
                    Some("alice"),
                    Some(0),
                ),
                add_json_with_partial_string_stats(
                    "string-counts-only.parquet",
                    10,
                    None,
                    None,
                    Some(0),
                ),
                add_json_with_partial_string_stats(
                    "string-missing-null-count.parquet",
                    10,
                    Some("alice"),
                    Some("morgan"),
                    None,
                ),
                add_json("string-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_non_ascii_string_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-non-ascii-string-data-stats-characterization",
            STRING_DATA_METADATA_JSON,
            &[
                add_json_with_string_stats("string-ascii-cafe.parquet", 10, "cafe", "cafe", 0),
                add_json_with_string_stats("string-ascii-zulu.parquet", 10, "zulu", "zulu", 0),
                add_json_with_string_stats(
                    "string-eclair.parquet",
                    10,
                    "\\u00e9clair",
                    "\\u00e9clair",
                    0,
                ),
                add_json_with_string_stats(
                    "string-emile.parquet",
                    10,
                    "\\u00e9mile",
                    "\\u00e9mile",
                    0,
                ),
                add_json("string-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_timestamp_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-timestamp-data-stats-characterization",
            TIMESTAMP_DATA_METADATA_JSON,
            &[
                add_json_with_timestamp_stats(
                    "timestamp-pre-epoch-only.parquet",
                    "event_ts",
                    10,
                    "1969-12-31T23:59:59.999999Z",
                    "1969-12-31T23:59:59.999999Z",
                    0,
                ),
                add_json_with_timestamp_stats(
                    "timestamp-low-only.parquet",
                    "event_ts",
                    10,
                    "2025-12-31T23:59:59.999999Z",
                    "2025-12-31T23:59:59.999999Z",
                    0,
                ),
                add_json_with_timestamp_stats(
                    "timestamp-target-only.parquet",
                    "event_ts",
                    10,
                    "2026-01-01T00:00:00.123456Z",
                    "2026-01-01T00:00:00.123456Z",
                    0,
                ),
                add_json_with_timestamp_stats(
                    "timestamp-high-only.parquet",
                    "event_ts",
                    10,
                    "2026-01-01T00:00:00.123457Z",
                    "2026-01-01T00:00:00.123457Z",
                    0,
                ),
                add_json_with_timestamp_stats(
                    "timestamp-range.parquet",
                    "event_ts",
                    10,
                    "2025-12-31T23:59:59.999999Z",
                    "2026-01-01T00:00:00.123456Z",
                    0,
                ),
                add_json_with_timestamp_stats(
                    "timestamp-target-with-null.parquet",
                    "event_ts",
                    10,
                    "2026-01-01T00:00:00.123456Z",
                    "2026-01-01T00:00:00.123456Z",
                    2,
                ),
                add_json_with_partial_timestamp_stats(
                    "timestamp-all-null.parquet",
                    "event_ts",
                    10,
                    None,
                    None,
                    Some(10),
                ),
                add_json("timestamp-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_timestamp_ntz_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "kernel-timestamp-ntz-data-stats-characterization",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_DATA_METADATA_JSON,
            &[
                add_json_with_timestamp_stats(
                    "timestamp-ntz-pre-epoch-only.parquet",
                    "event_ts_ntz",
                    10,
                    "1969-12-31 23:59:59.999999",
                    "1969-12-31 23:59:59.999999",
                    0,
                ),
                add_json_with_timestamp_stats(
                    "timestamp-ntz-low-only.parquet",
                    "event_ts_ntz",
                    10,
                    "2025-12-31 23:59:59.999999",
                    "2025-12-31 23:59:59.999999",
                    0,
                ),
                add_json_with_timestamp_stats(
                    "timestamp-ntz-target-only.parquet",
                    "event_ts_ntz",
                    10,
                    "2026-01-01 00:00:00.123456",
                    "2026-01-01 00:00:00.123456",
                    0,
                ),
                add_json_with_timestamp_stats(
                    "timestamp-ntz-high-only.parquet",
                    "event_ts_ntz",
                    10,
                    "2026-01-01 00:00:00.123457",
                    "2026-01-01 00:00:00.123457",
                    0,
                ),
                add_json_with_timestamp_stats(
                    "timestamp-ntz-range.parquet",
                    "event_ts_ntz",
                    10,
                    "2025-12-31 23:59:59.999999",
                    "2026-01-01 00:00:00.123456",
                    0,
                ),
                add_json_with_timestamp_stats(
                    "timestamp-ntz-target-with-null.parquet",
                    "event_ts_ntz",
                    10,
                    "2026-01-01 00:00:00.123456",
                    "2026-01-01 00:00:00.123456",
                    2,
                ),
                add_json_with_partial_timestamp_stats(
                    "timestamp-ntz-all-null.parquet",
                    "event_ts_ntz",
                    10,
                    None,
                    None,
                    Some(10),
                ),
                add_json("timestamp-ntz-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_partial_timestamp_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-partial-timestamp-data-stats-characterization",
            TIMESTAMP_DATA_METADATA_JSON,
            &[
                add_json_with_partial_timestamp_stats(
                    "timestamp-min-only-target.parquet",
                    "event_ts",
                    10,
                    Some("2026-01-01T00:00:00.123456Z"),
                    None,
                    Some(0),
                ),
                add_json_with_partial_timestamp_stats(
                    "timestamp-max-only-low.parquet",
                    "event_ts",
                    10,
                    None,
                    Some("2025-12-31T23:59:59.999999Z"),
                    Some(0),
                ),
                add_json_with_partial_timestamp_stats(
                    "timestamp-counts-only.parquet",
                    "event_ts",
                    10,
                    None,
                    None,
                    Some(0),
                ),
                add_json_with_partial_timestamp_stats(
                    "timestamp-missing-null-count.parquet",
                    "event_ts",
                    10,
                    Some("2025-12-31T23:59:59.999999Z"),
                    Some("2026-01-01T00:00:00.123456Z"),
                    None,
                ),
                add_json("timestamp-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_partial_timestamp_ntz_data_stats_characterization_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "kernel-partial-timestamp-ntz-data-stats-characterization",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_DATA_METADATA_JSON,
            &[
                add_json_with_partial_timestamp_stats(
                    "timestamp-ntz-min-only-target.parquet",
                    "event_ts_ntz",
                    10,
                    Some("2026-01-01 00:00:00.123456"),
                    None,
                    Some(0),
                ),
                add_json_with_partial_timestamp_stats(
                    "timestamp-ntz-max-only-low.parquet",
                    "event_ts_ntz",
                    10,
                    None,
                    Some("2025-12-31 23:59:59.999999"),
                    Some(0),
                ),
                add_json_with_partial_timestamp_stats(
                    "timestamp-ntz-counts-only.parquet",
                    "event_ts_ntz",
                    10,
                    None,
                    None,
                    Some(0),
                ),
                add_json_with_partial_timestamp_stats(
                    "timestamp-ntz-missing-null-count.parquet",
                    "event_ts_ntz",
                    10,
                    Some("2025-12-31 23:59:59.999999"),
                    Some("2026-01-01 00:00:00.123456"),
                    None,
                ),
                add_json("timestamp-ntz-missing-stats.parquet"),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_all_sufficient_data_stats_source()
    -> Result<(DeltaLogTable, super::PlannedDeltaSource), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-all-sufficient-data-stats",
            METADATA_JSON,
            &[
                add_json_with_id_stats("id-low-a.parquet", 10, 1, 50, 0),
                add_json_with_id_stats("id-low-b.parquet", 10, 51, 100, 0),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok((table, source))
    }

    fn kernel_predicated_file_paths(
        source: &super::PlannedDeltaSource,
        filter: &datafusion::logical_expr::Expr,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let predicate = datafusion_expr_to_kernel_predicate(filter)?;
        kernel_predicate_file_paths(source, predicate)
    }

    fn kernel_predicate_file_paths(
        source: &super::PlannedDeltaSource,
        predicate: DeltaKernelPredicate,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let scan = build_projected_predicated_delta_scan(source, None, Some(predicate))?;

        kernel_scan_file_paths(&scan, source.table_uri())
    }

    fn build_projected_predicated_stats_delta_scan(
        source: &super::PlannedDeltaSource,
        predicate: Option<DeltaKernelPredicate>,
    ) -> Result<ProjectedDeltaScan, delta_kernel::Error> {
        let (scan, kernel_schema) = super::kernel::build_projected_predicated_stats_scan(
            source.loaded_snapshot().kernel_snapshot(),
            None,
            predicate,
        )?;

        Ok(ProjectedDeltaScan {
            scan,
            kernel_schema,
        })
    }

    fn kernel_predicated_stats_file_paths(
        source: &super::PlannedDeltaSource,
        filter: &datafusion::logical_expr::Expr,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let predicate = datafusion_expr_to_kernel_predicate(filter)?;
        let scan = build_projected_predicated_stats_delta_scan(source, Some(predicate))?;

        kernel_scan_file_paths(&scan, source.table_uri())
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    struct KernelStatsMetadataBoundary {
        selected_rows: usize,
        selected_stats_rows: usize,
        selected_missing_stats_rows: usize,
        has_id_min_values: bool,
        has_id_max_values: bool,
        has_id_null_count: bool,
    }

    fn kernel_stats_metadata_boundary(
        scan: &ProjectedDeltaScan,
        table_uri: &str,
    ) -> Result<KernelStatsMetadataBoundary, Box<dyn std::error::Error>> {
        let table_url = super::kernel::try_parse_uri(table_uri)?;
        let store =
            super::kernel::store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>())?;
        let engine = super::kernel::DefaultEngineBuilder::new(store).build();
        let mut boundary = KernelStatsMetadataBoundary::default();

        for scan_metadata in scan.kernel_scan().scan_metadata(&engine)? {
            let (underlying_data, selection_vector) = scan_metadata?.scan_files.into_parts();
            let batch: RecordBatch =
                super::kernel::ArrowEngineData::try_from_engine_data(underlying_data)?.into();
            let stats_parsed = stats_parsed_column(&batch)?;
            let min_values = stats_struct_child(stats_parsed, "minValues")?;
            let max_values = stats_struct_child(stats_parsed, "maxValues")?;
            let null_count = stats_struct_child(stats_parsed, "nullCount")?;
            let min_id = stats_struct_child_column(min_values, "id")?;
            let max_id = stats_struct_child_column(max_values, "id")?;
            let null_count_id = stats_struct_child_column(null_count, "id")?;

            boundary.has_id_min_values |= min_values.column_by_name("id").is_some();
            boundary.has_id_max_values |= max_values.column_by_name("id").is_some();
            boundary.has_id_null_count |= null_count.column_by_name("id").is_some();

            for (row_index, selected) in selection_vector.iter().copied().enumerate() {
                if selected {
                    boundary.selected_rows += 1;
                    if !min_id.is_null(row_index)
                        && !max_id.is_null(row_index)
                        && !null_count_id.is_null(row_index)
                    {
                        boundary.selected_stats_rows += 1;
                    } else {
                        boundary.selected_missing_stats_rows += 1;
                    }
                }
            }
        }

        Ok(boundary)
    }

    fn stats_parsed_column(
        batch: &RecordBatch,
    ) -> Result<&StructArray, Box<dyn std::error::Error>> {
        let Some(column) = batch.column_by_name("stats_parsed") else {
            return Err("scan metadata did not expose stats_parsed".into());
        };
        let Some(stats_parsed) = column.as_any().downcast_ref::<StructArray>() else {
            return Err("scan metadata stats_parsed was not a struct array".into());
        };

        Ok(stats_parsed)
    }

    fn stats_struct_child<'a>(
        stats_parsed: &'a StructArray,
        name: &str,
    ) -> Result<&'a StructArray, Box<dyn std::error::Error>> {
        let Some(column) = stats_parsed.column_by_name(name) else {
            return Err(format!("scan metadata stats_parsed did not expose {name}").into());
        };
        let Some(stats_child) = column.as_any().downcast_ref::<StructArray>() else {
            return Err(format!("scan metadata stats_parsed.{name} was not a struct array").into());
        };

        Ok(stats_child)
    }

    fn stats_struct_child_column<'a>(
        stats_child: &'a StructArray,
        name: &str,
    ) -> Result<&'a dyn Array, Box<dyn std::error::Error>> {
        let Some(column) = stats_child.column_by_name(name) else {
            return Err(format!("scan metadata stats_parsed child did not expose {name}").into());
        };

        Ok(column.as_ref())
    }

    fn int8_lit(value: i8) -> Expr {
        Expr::Literal(ScalarValue::Int8(Some(value)), None)
    }

    fn int16_lit(value: i16) -> Expr {
        Expr::Literal(ScalarValue::Int16(Some(value)), None)
    }

    fn int32_lit(value: i32) -> Expr {
        Expr::Literal(ScalarValue::Int32(Some(value)), None)
    }

    fn int64_lit(value: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), None)
    }

    fn bool_lit(value: bool) -> Expr {
        Expr::Literal(ScalarValue::Boolean(Some(value)), None)
    }

    fn date_lit(value: i32) -> Expr {
        Expr::Literal(ScalarValue::Date32(Some(value)), None)
    }

    fn timestamp_lit(value: i64, timezone: &str) -> Expr {
        Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(value), Some(timezone.into())),
            None,
        )
    }

    fn timestamp_ntz_lit(value: i64) -> Expr {
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(value), None), None)
    }

    fn decimal_lit(value: i128) -> Expr {
        Expr::Literal(ScalarValue::Decimal128(Some(value), 10, 2), None)
    }

    fn decimal_lit_with_type(value: i128, precision: u8, scale: i8) -> Expr {
        Expr::Literal(ScalarValue::Decimal128(Some(value), precision, scale), None)
    }

    fn string_lit(value: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(value.to_owned())), None)
    }

    fn binary_lit(value: &[u8]) -> Expr {
        Expr::Literal(ScalarValue::Binary(Some(value.to_vec())), None)
    }

    fn large_string_lit(value: &str) -> Expr {
        Expr::Literal(ScalarValue::LargeUtf8(Some(value.to_owned())), None)
    }

    fn float32_lit(value: f32) -> Expr {
        Expr::Literal(ScalarValue::Float32(Some(value)), None)
    }

    fn float64_lit(value: f64) -> Expr {
        Expr::Literal(ScalarValue::Float64(Some(value)), None)
    }

    fn binary_partition_column() -> Expression {
        Expression::Column(ColumnName::new(["payload"]))
    }

    fn binary_partition_value(value: &[u8]) -> Expression {
        Expression::Literal(Scalar::Binary(value.to_vec()))
    }

    fn binary_partition_eq(value: &[u8]) -> DeltaKernelPredicate {
        DeltaKernelPredicate::new(Predicate::eq(
            binary_partition_column(),
            binary_partition_value(value),
        ))
    }

    fn binary_partition_ne(value: &[u8]) -> DeltaKernelPredicate {
        DeltaKernelPredicate::new(Predicate::ne(
            binary_partition_column(),
            binary_partition_value(value),
        ))
    }

    fn timestamp_partition_column() -> Expression {
        Expression::Column(ColumnName::new(["event_ts"]))
    }

    fn timestamp_partition_value(value: i64) -> Expression {
        Expression::Literal(Scalar::Timestamp(value))
    }

    fn timestamp_partition_eq(value: i64) -> DeltaKernelPredicate {
        DeltaKernelPredicate::new(Predicate::eq(
            timestamp_partition_column(),
            timestamp_partition_value(value),
        ))
    }

    fn timestamp_partition_ne(value: i64) -> DeltaKernelPredicate {
        DeltaKernelPredicate::new(Predicate::ne(
            timestamp_partition_column(),
            timestamp_partition_value(value),
        ))
    }

    fn timestamp_ntz_partition_column() -> Expression {
        Expression::Column(ColumnName::new(["event_ts_ntz"]))
    }

    fn timestamp_ntz_partition_value(value: i64) -> Expression {
        Expression::Literal(Scalar::TimestampNtz(value))
    }

    fn timestamp_ntz_partition_eq(value: i64) -> DeltaKernelPredicate {
        DeltaKernelPredicate::new(Predicate::eq(
            timestamp_ntz_partition_column(),
            timestamp_ntz_partition_value(value),
        ))
    }

    fn timestamp_ntz_partition_ne(value: i64) -> DeltaKernelPredicate {
        DeltaKernelPredicate::new(Predicate::ne(
            timestamp_ntz_partition_column(),
            timestamp_ntz_partition_value(value),
        ))
    }

    fn assert_invalid_integer_partition_error(
        result: Result<Vec<String>, Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = match result {
            Ok(paths) => {
                return Err(
                    format!("invalid integer metadata should fail kernel scan: {paths:?}").into(),
                );
            }
            Err(error) => error,
        };
        let message = error.to_string();
        let debug_message = format!("{error:?}");
        assert!(message.contains("not-an-integer"));
        assert!(debug_message.contains("Primitive(Long)"));

        Ok(())
    }

    fn assert_invalid_boolean_partition_error(
        result: Result<Vec<String>, Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = match result {
            Ok(paths) => {
                return Err(
                    format!("invalid boolean metadata should fail kernel scan: {paths:?}").into(),
                );
            }
            Err(error) => error,
        };
        let message = error.to_string();
        let debug_message = format!("{error:?}");
        assert!(message.contains("not-a-boolean"));
        assert!(debug_message.contains("Primitive(Boolean)"));

        Ok(())
    }

    fn assert_invalid_date_partition_error(
        result: Result<Vec<String>, Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = match result {
            Ok(paths) => {
                return Err(
                    format!("invalid date metadata should fail kernel scan: {paths:?}").into(),
                );
            }
            Err(error) => error,
        };
        let message = error.to_string();
        let debug_message = format!("{error:?}");
        assert!(message.contains("not-a-date"));
        assert!(debug_message.contains("Primitive(Date)"));

        Ok(())
    }

    fn assert_invalid_decimal_partition_error(
        result: Result<Vec<String>, Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = match result {
            Ok(paths) => {
                return Err(
                    format!("invalid decimal metadata should fail kernel scan: {paths:?}").into(),
                );
            }
            Err(error) => error,
        };
        let message = error.to_string();
        let debug_message = format!("{error:?}");
        assert!(
            message.contains("not-a-decimal")
                || message.contains("123.450")
                || message.contains("Could not parse int")
                || debug_message.contains("ParseIntError")
                || debug_message.contains("InvalidDecimal"),
            "{message}\n{debug_message}"
        );
        assert!(
            debug_message.contains("Decimal") || debug_message.contains("ParseIntError"),
            "{debug_message}"
        );

        Ok(())
    }

    fn assert_invalid_floating_partition_error(
        result: Result<Vec<String>, Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = match result {
            Ok(paths) => {
                return Err(format!(
                    "invalid floating metadata should fail kernel scan: {paths:?}"
                )
                .into());
            }
            Err(error) => error,
        };
        let message = error.to_string();
        let debug_message = format!("{error:?}");
        assert!(
            message.contains("not-a-float")
                || message.contains("not-a-double")
                || debug_message.contains("InvalidFloat")
                || debug_message.contains("ParseFloatError"),
            "{message}\n{debug_message}"
        );

        Ok(())
    }

    fn assert_invalid_timestamp_partition_error(
        result: Result<Vec<String>, Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = match result {
            Ok(paths) => {
                return Err(format!(
                    "invalid timestamp metadata should fail kernel scan: {paths:?}"
                )
                .into());
            }
            Err(error) => error,
        };
        let message = error.to_string();
        let debug_message = format!("{error:?}");
        assert!(
            message.contains("not-a-timestamp")
                || debug_message.contains("not-a-timestamp")
                || debug_message.contains("Timestamp"),
            "{message}\n{debug_message}"
        );

        Ok(())
    }

    fn assert_invalid_timestamp_ntz_partition_error(
        result: Result<Vec<String>, Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = match result {
            Ok(paths) => {
                return Err(format!(
                    "invalid timestamp_ntz metadata should fail kernel scan: {paths:?}"
                )
                .into());
            }
            Err(error) => error,
        };
        let message = error.to_string();
        let debug_message = format!("{error:?}");
        assert!(
            message.contains("not-a-timestamp")
                || debug_message.contains("not-a-timestamp")
                || debug_message.contains("TimestampNtz"),
            "{message}\n{debug_message}"
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_uses_official_delta_kernel_0_23_0() {
        let manifest = include_str!("../../Cargo.toml");
        let lockfile = include_str!("../../../../Cargo.lock");

        assert!(manifest.contains(r#"delta_kernel = { version = "0.23.0""#));
        assert!(lockfile.contains(
            "name = \"delta_kernel\"\nversion = \"0.23.0\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\""
        ));
        assert!(!manifest.contains("deltalake"));
        assert!(!manifest.contains("buoyant_kernel"));
    }

    #[test]
    fn kernel_predicated_scan_prunes_files() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-predicated-scan-files",
            PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json("region-us-west.parquet", r#"{"region":"us-west"}"#),
                partitioned_add_json("region-us-east.parquet", r#"{"region":"us-east"}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("region").eq(lit("us-west")))?;
        let predicated_scan =
            build_projected_predicated_delta_scan(&source, None, Some(predicate))?;

        assert_eq!(
            kernel_scan_file_paths(&unfiltered_scan, source.table_uri())?,
            vec!["region-us-east.parquet", "region-us-west.parquet"]
        );
        assert_eq!(
            kernel_scan_file_paths(&predicated_scan, source.table_uri())?,
            vec!["region-us-west.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_stats_scan_metadata_exposes_parsed_file_stats_without_data_file_reads()
    -> Result<(), Box<dyn std::error::Error>> {
        let (table, source) = kernel_data_stats_characterization_source()?;
        let stats_scan = build_projected_predicated_stats_delta_scan(&source, None)?;

        assert!(!table.path.join("id-impossible.parquet").exists());
        assert!(!table.path.join("id-possible.parquet").exists());
        assert!(!table.path.join("id-missing-stats.parquet").exists());
        assert_eq!(
            kernel_scan_file_paths(&stats_scan, source.table_uri())?,
            vec![
                "id-impossible.parquet",
                "id-missing-stats.parquet",
                "id-possible.parquet",
            ]
        );
        assert_eq!(
            kernel_stats_metadata_boundary(&stats_scan, source.table_uri())?,
            KernelStatsMetadataBoundary {
                selected_rows: 3,
                selected_stats_rows: 2,
                selected_missing_stats_rows: 1,
                has_id_min_values: true,
                has_id_max_values: true,
                has_id_null_count: true,
            }
        );

        Ok(())
    }

    #[test]
    fn kernel_data_column_stats_pruning_keeps_possible_and_missing_stats_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("id").gt(int32_lit(100)))?,
            vec!["id-missing-stats.parquet", "id-possible.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_data_column_stats_pruning_can_return_empty_when_all_stats_are_impossible()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_all_sufficient_data_stats_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("id").gt(int32_lit(100)))?,
            Vec::<String>::new()
        );

        Ok(())
    }

    #[test]
    fn kernel_boolean_data_column_stats_pruning_documents_min_max_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_boolean_data_stats_characterization_source()?;

        assert_boolean_stats_min_max_error(kernel_predicated_stats_file_paths(
            &source,
            &col("is_current").eq(bool_lit(true)),
        ))?;
        assert_boolean_stats_min_max_error(kernel_predicated_stats_file_paths(
            &source,
            &col("is_current").eq(bool_lit(false)),
        ))?;
        assert_boolean_stats_min_max_error(kernel_predicated_stats_file_paths(
            &source,
            &col("is_current").not_eq(bool_lit(true)),
        ))?;
        assert_boolean_stats_min_max_error(kernel_predicated_stats_file_paths(
            &source,
            &col("is_current").not_eq(bool_lit(false)),
        ))?;

        Ok(())
    }

    #[test]
    fn kernel_boolean_data_column_stats_pruning_documents_null_checks()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_boolean_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("is_current").is_null())?,
            vec![
                "boolean-all-null.parquet",
                "boolean-false-with-null.parquet",
                "boolean-missing-stats.parquet",
                "boolean-true-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("is_current").is_not_null())?,
            vec![
                "boolean-false-only.parquet",
                "boolean-false-with-null.parquet",
                "boolean-missing-stats.parquet",
                "boolean-mixed.parquet",
                "boolean-true-only.parquet",
                "boolean-true-with-null.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_boolean_data_column_stats_pruning_keeps_partial_null_counts_uncertain()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_partial_boolean_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("is_current").is_null())?,
            vec![
                "boolean-missing-null-count.parquet",
                "boolean-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("is_current").is_not_null())?,
            vec![
                "boolean-counts-only.parquet",
                "boolean-max-only-true.parquet",
                "boolean-min-only-false.parquet",
                "boolean-missing-null-count.parquet",
                "boolean-missing-stats.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_date_data_column_stats_pruning_documents_comparisons()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_date_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_date").gt(date_lit(19_782)))?,
            vec![
                "date-missing-stats.parquet",
                "date-new-year-only.parquet",
                "date-new-year-with-null.parquet",
                "date-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_date").gt_eq(date_lit(20_454)),
            )?,
            vec![
                "date-missing-stats.parquet",
                "date-new-year-only.parquet",
                "date-new-year-with-null.parquet",
                "date-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_date").lt_eq(date_lit(-1)),)?,
            vec!["date-missing-stats.parquet", "date-pre-epoch-only.parquet"]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &date_lit(20_454).gt(col("event_date")))?,
            vec![
                "date-leap-only.parquet",
                "date-missing-stats.parquet",
                "date-pre-epoch-only.parquet",
                "date-range.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_date_data_column_stats_pruning_documents_equality_and_not_equals()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_date_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_date").eq(date_lit(20_454)))?,
            vec![
                "date-missing-stats.parquet",
                "date-new-year-only.parquet",
                "date-new-year-with-null.parquet",
                "date-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_date").not_eq(date_lit(20_454)),
            )?,
            vec![
                "date-leap-only.parquet",
                "date-missing-stats.parquet",
                "date-pre-epoch-only.parquet",
                "date-range.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_date_data_column_stats_pruning_documents_null_checks()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_date_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_date").is_null())?,
            vec![
                "date-all-null.parquet",
                "date-missing-stats.parquet",
                "date-new-year-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_date").is_not_null())?,
            vec![
                "date-leap-only.parquet",
                "date-missing-stats.parquet",
                "date-new-year-only.parquet",
                "date-new-year-with-null.parquet",
                "date-pre-epoch-only.parquet",
                "date-range.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_date_data_column_stats_pruning_documents_partial_stats_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_partial_date_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_date").gt(date_lit(19_782)))?,
            vec![
                "date-counts-only.parquet",
                "date-min-only-high.parquet",
                "date-missing-null-count.parquet",
                "date-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_date").is_null())?,
            vec![
                "date-missing-null-count.parquet",
                "date-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_date").is_not_null())?,
            vec![
                "date-counts-only.parquet",
                "date-max-only-low.parquet",
                "date-min-only-high.parquet",
                "date-missing-null-count.parquet",
                "date-missing-stats.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_decimal_data_column_stats_pruning_documents_comparisons()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_decimal_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("amount").gt(decimal_lit(200)))?,
            vec![
                "decimal-large-only.parquet",
                "decimal-missing-stats.parquet",
                "decimal-range.parquet",
                "decimal-ten-only.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("amount").gt_eq(decimal_lit(1_000)))?,
            vec![
                "decimal-large-only.parquet",
                "decimal-missing-stats.parquet",
                "decimal-range.parquet",
                "decimal-ten-only.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("amount").lt_eq(decimal_lit(-123)))?,
            vec![
                "decimal-missing-stats.parquet",
                "decimal-negative-only.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &decimal_lit(1_000).gt(col("amount")))?,
            vec![
                "decimal-missing-stats.parquet",
                "decimal-negative-only.parquet",
                "decimal-range.parquet",
                "decimal-two-only.parquet",
                "decimal-two-with-null.parquet",
                "decimal-zero-only.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_decimal_data_column_stats_pruning_documents_equality_and_not_equals()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_decimal_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("amount").eq(decimal_lit(200)))?,
            vec![
                "decimal-missing-stats.parquet",
                "decimal-range.parquet",
                "decimal-two-only.parquet",
                "decimal-two-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("amount").not_eq(decimal_lit(200)))?,
            vec![
                "decimal-large-only.parquet",
                "decimal-missing-stats.parquet",
                "decimal-negative-only.parquet",
                "decimal-range.parquet",
                "decimal-ten-only.parquet",
                "decimal-zero-only.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_decimal_data_column_stats_pruning_documents_null_checks()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_decimal_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("amount").is_null())?,
            vec![
                "decimal-all-null.parquet",
                "decimal-missing-stats.parquet",
                "decimal-two-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("amount").is_not_null())?,
            vec![
                "decimal-large-only.parquet",
                "decimal-missing-stats.parquet",
                "decimal-negative-only.parquet",
                "decimal-range.parquet",
                "decimal-ten-only.parquet",
                "decimal-two-only.parquet",
                "decimal-two-with-null.parquet",
                "decimal-zero-only.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_decimal_data_column_stats_pruning_documents_partial_stats_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_partial_decimal_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("amount").gt(decimal_lit(200)))?,
            vec![
                "decimal-counts-only.parquet",
                "decimal-min-only-high.parquet",
                "decimal-missing-null-count.parquet",
                "decimal-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("amount").is_null())?,
            vec![
                "decimal-missing-null-count.parquet",
                "decimal-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("amount").is_not_null())?,
            vec![
                "decimal-counts-only.parquet",
                "decimal-max-only-low.parquet",
                "decimal-min-only-high.parquet",
                "decimal-missing-null-count.parquet",
                "decimal-missing-stats.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_decimal_data_column_stats_pruning_documents_precision_and_scale_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_decimal_data_stats_characterization_source()?;

        for (description, literal) in [
            ("scale mismatch", decimal_lit_with_type(2_000, 11, 3)),
            ("precision mismatch", decimal_lit_with_type(200, 11, 2)),
        ] {
            let error = kernel_predicated_stats_file_paths(&source, &col("amount").eq(literal))
                .err()
                .ok_or_else(|| {
                    format!("{description} decimal predicate should fail kernel scan")
                })?;
            let message = error.to_string();
            let debug_message = format!("{error:?}");
            assert!(
                message.contains("Invalid comparison operation")
                    || debug_message.contains("Invalid comparison operation"),
                "{message}\n{debug_message}"
            );
        }

        Ok(())
    }

    #[test]
    fn kernel_binary_data_column_stats_pruning_documents_equality_and_empty_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_binary_data_stats_characterization_source()?;

        assert_binary_stats_min_max_error(kernel_predicated_stats_file_paths(
            &source,
            &col("payload").eq(binary_lit(b"hello")),
        ))?;
        assert_binary_stats_min_max_error(kernel_predicated_stats_file_paths(
            &source,
            &col("payload").not_eq(binary_lit(b"hello")),
        ))?;
        assert_unsupported_literal_error(kernel_predicated_stats_file_paths(
            &source,
            &col("payload").eq(binary_lit(b"")),
        ))?;
        assert_unsupported_literal_error(kernel_predicated_stats_file_paths(
            &source,
            &col("payload").not_eq(binary_lit(b"")),
        ))?;

        Ok(())
    }

    #[test]
    fn kernel_binary_data_column_stats_pruning_documents_ordering()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_binary_data_stats_characterization_source()?;

        assert_binary_stats_min_max_error(kernel_predicated_stats_file_paths(
            &source,
            &col("payload").gt(binary_lit(b"hello")),
        ))?;
        assert_binary_stats_min_max_error(kernel_predicated_stats_file_paths(
            &source,
            &col("payload").lt(binary_lit(b"hello")),
        ))?;
        assert_binary_stats_min_max_error(kernel_predicated_stats_file_paths(
            &source,
            &binary_lit(b"hello").gt(col("payload")),
        ))?;

        Ok(())
    }

    #[test]
    fn kernel_binary_data_column_stats_pruning_documents_null_checks()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_binary_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("payload").is_null())?,
            vec![
                "binary-all-null.parquet",
                "binary-missing-stats.parquet",
                "binary-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("payload").is_not_null())?,
            vec![
                "binary-HELLO.parquet",
                "binary-empty.parquet",
                "binary-hello.parquet",
                "binary-missing-stats.parquet",
                "binary-range.parquet",
                "binary-special.parquet",
                "binary-with-null.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_binary_data_column_stats_pruning_documents_partial_stats_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_partial_binary_data_stats_characterization_source()?;

        assert_binary_stats_min_max_error(kernel_predicated_stats_file_paths(
            &source,
            &col("payload").gt(binary_lit(b"hello")),
        ))?;
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("payload").is_null())?,
            vec![
                "binary-missing-null-count.parquet",
                "binary-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("payload").is_not_null())?,
            vec![
                "binary-counts-only.parquet",
                "binary-max-only-low.parquet",
                "binary-min-only-high.parquet",
                "binary-missing-null-count.parquet",
                "binary-missing-stats.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_floating_data_column_stats_pruning_documents_comparisons()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_floating_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").gt(float32_lit(1.5)))?,
            vec![
                "floating-missing-stats.parquet",
                "floating-range.parquet",
                "floating-ten.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("double_score").lt(float64_lit(0.0)),)?,
            vec![
                "floating-missing-stats.parquet",
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &float64_lit(0.0).gt(col("double_score")))?,
            vec![
                "floating-missing-stats.parquet",
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").lt(float32_lit(10.0)))?,
            vec![
                "floating-missing-stats.parquet",
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-one-with-null.parquet",
                "floating-one.parquet",
                "floating-pos-zero.parquet",
                "floating-range.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_floating_data_column_stats_pruning_documents_equality_and_signed_zero()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_floating_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").eq(float32_lit(1.5)))?,
            vec![
                "floating-missing-stats.parquet",
                "floating-one-with-null.parquet",
                "floating-one.parquet",
                "floating-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("float_score").not_eq(float32_lit(1.5)),
            )?,
            vec![
                "floating-missing-stats.parquet",
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-pos-zero.parquet",
                "floating-range.parquet",
                "floating-ten.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").eq(float32_lit(-0.0)))?,
            vec![
                "floating-missing-stats.parquet",
                "floating-neg-zero.parquet",
                "floating-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").eq(float32_lit(0.0)))?,
            vec![
                "floating-missing-stats.parquet",
                "floating-pos-zero.parquet",
                "floating-range.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_floating_data_column_stats_pruning_documents_signed_zero_operator_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_floating_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").lt(float32_lit(0.0)))?,
            vec![
                "floating-missing-stats.parquet",
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("float_score").lt_eq(float32_lit(0.0)),
            )?,
            vec![
                "floating-missing-stats.parquet",
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-pos-zero.parquet",
                "floating-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").gt(float32_lit(0.0)))?,
            vec![
                "floating-missing-stats.parquet",
                "floating-one-with-null.parquet",
                "floating-one.parquet",
                "floating-range.parquet",
                "floating-ten.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("float_score").gt_eq(float32_lit(0.0)),
            )?,
            vec![
                "floating-missing-stats.parquet",
                "floating-one-with-null.parquet",
                "floating-one.parquet",
                "floating-pos-zero.parquet",
                "floating-range.parquet",
                "floating-ten.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("float_score").not_eq(float32_lit(0.0)),
            )?,
            vec![
                "floating-missing-stats.parquet",
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-one-with-null.parquet",
                "floating-one.parquet",
                "floating-range.parquet",
                "floating-ten.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_floating_data_column_stats_pruning_documents_null_checks()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_floating_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").is_null())?,
            vec![
                "floating-all-null.parquet",
                "floating-missing-stats.parquet",
                "floating-one-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("double_score").is_not_null())?,
            vec![
                "floating-missing-stats.parquet",
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-one-with-null.parquet",
                "floating-one.parquet",
                "floating-pos-zero.parquet",
                "floating-range.parquet",
                "floating-ten.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_floating_data_column_stats_pruning_documents_partial_stats_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_partial_floating_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").gt(float32_lit(1.0)))?,
            vec![
                "floating-counts-only.parquet",
                "floating-min-only-high.parquet",
                "floating-missing-null-count.parquet",
                "floating-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").is_null())?,
            vec![
                "floating-missing-null-count.parquet",
                "floating-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").is_not_null())?,
            vec![
                "floating-counts-only.parquet",
                "floating-max-only-low.parquet",
                "floating-min-only-high.parquet",
                "floating-missing-null-count.parquet",
                "floating-missing-stats.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_floating_data_column_stats_pruning_documents_nonfinite_stats_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_nonfinite_floating_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").gt(float32_lit(1.0)))?,
            vec![
                "floating-inf.parquet",
                "floating-missing-stats.parquet",
                "floating-nan.parquet",
                "floating-valid.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("double_score").lt(float64_lit(0.0)))?,
            vec!["floating-missing-stats.parquet", "floating-neg-inf.parquet"]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("float_score").eq(float32_lit(1.5)),)?,
            vec!["floating-missing-stats.parquet", "floating-valid.parquet"]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("float_score").not_eq(float32_lit(1.5)),
            )?,
            vec![
                "floating-inf.parquet",
                "floating-missing-stats.parquet",
                "floating-nan.parquet",
                "floating-neg-inf.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_string_data_column_stats_pruning_documents_comparisons()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_string_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("customer_name").gt(string_lit("m")),)?,
            vec![
                "string-missing-stats.parquet",
                "string-range.parquet",
                "string-zed-only.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("customer_name").gt_eq(string_lit("morgan")),
            )?,
            vec![
                "string-missing-stats.parquet",
                "string-range.parquet",
                "string-zed-only.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("customer_name").lt_eq(string_lit("Alice")),
            )?,
            vec![
                "string-empty-only.parquet",
                "string-missing-stats.parquet",
                "string-mixed-case-only.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &string_lit("m").gt(col("customer_name")),)?,
            vec![
                "string-alice-only.parquet",
                "string-alice-with-null.parquet",
                "string-bob-only.parquet",
                "string-empty-only.parquet",
                "string-missing-stats.parquet",
                "string-mixed-case-only.parquet",
                "string-range.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_string_data_column_stats_pruning_documents_equality_and_not_equals()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_string_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("customer_name").eq(string_lit("alice")),
            )?,
            vec![
                "string-alice-only.parquet",
                "string-alice-with-null.parquet",
                "string-missing-stats.parquet",
                "string-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("customer_name").not_eq(string_lit("alice")),
            )?,
            vec![
                "string-bob-only.parquet",
                "string-empty-only.parquet",
                "string-missing-stats.parquet",
                "string-mixed-case-only.parquet",
                "string-range.parquet",
                "string-zed-only.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_string_data_column_stats_pruning_documents_null_checks()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_string_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("customer_name").is_null())?,
            vec![
                "string-alice-with-null.parquet",
                "string-all-null.parquet",
                "string-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("customer_name").is_not_null())?,
            vec![
                "string-alice-only.parquet",
                "string-alice-with-null.parquet",
                "string-bob-only.parquet",
                "string-empty-only.parquet",
                "string-missing-stats.parquet",
                "string-mixed-case-only.parquet",
                "string-range.parquet",
                "string-zed-only.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_string_data_column_stats_pruning_documents_partial_stats_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_partial_string_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("customer_name").gt(string_lit("m")),)?,
            vec![
                "string-counts-only.parquet",
                "string-min-only-morgan.parquet",
                "string-missing-null-count.parquet",
                "string-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("customer_name").is_null())?,
            vec![
                "string-missing-null-count.parquet",
                "string-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("customer_name").is_not_null())?,
            vec![
                "string-counts-only.parquet",
                "string-max-only-alice.parquet",
                "string-min-only-morgan.parquet",
                "string-missing-null-count.parquet",
                "string-missing-stats.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_string_data_column_stats_pruning_documents_large_utf8_literal_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_string_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("customer_name").eq(large_string_lit("alice")),
            )?,
            vec![
                "string-alice-only.parquet",
                "string-alice-with-null.parquet",
                "string-missing-stats.parquet",
                "string-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("customer_name").gt(large_string_lit("m")),
            )?,
            vec![
                "string-missing-stats.parquet",
                "string-range.parquet",
                "string-zed-only.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_string_data_column_stats_pruning_documents_non_ascii_ordering()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_non_ascii_string_data_stats_characterization_source()?;
        let eclair = string_lit("\u{00e9}clair");

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("customer_name").eq(eclair.clone()),)?,
            vec!["string-eclair.parquet", "string-missing-stats.parquet"]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("customer_name").eq(large_string_lit("\u{00e9}clair")),
            )?,
            vec!["string-eclair.parquet", "string-missing-stats.parquet"]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("customer_name").gt_eq(eclair.clone()),
            )?,
            vec![
                "string-eclair.parquet",
                "string-emile.parquet",
                "string-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("customer_name").lt(eclair.clone()),)?,
            vec![
                "string-ascii-cafe.parquet",
                "string-ascii-zulu.parquet",
                "string-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("customer_name").gt(eclair),)?,
            vec!["string-emile.parquet", "string-missing-stats.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_timestamp_data_column_stats_pruning_documents_comparisons()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_timestamp_data_stats_characterization_source()?;
        let low = 1_767_225_599_999_999_i64;
        let target = 1_767_225_600_123_456_i64;
        let high = 1_767_225_600_123_457_i64;

        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_ts").lt(timestamp_lit(target, "UTC")),
            )?,
            vec![
                "timestamp-low-only.parquet",
                "timestamp-missing-stats.parquet",
                "timestamp-pre-epoch-only.parquet",
                "timestamp-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_ts").gt_eq(timestamp_lit(target, "UTC")),
            )?,
            vec![
                "timestamp-high-only.parquet",
                "timestamp-missing-stats.parquet",
                "timestamp-range.parquet",
                "timestamp-target-only.parquet",
                "timestamp-target-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &timestamp_lit(high, "UTC").gt(col("event_ts")),
            )?,
            vec![
                "timestamp-low-only.parquet",
                "timestamp-missing-stats.parquet",
                "timestamp-pre-epoch-only.parquet",
                "timestamp-range.parquet",
                "timestamp-target-only.parquet",
                "timestamp-target-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_ts").eq(timestamp_lit(low, "UTC")),
            )?,
            vec![
                "timestamp-low-only.parquet",
                "timestamp-missing-stats.parquet",
                "timestamp-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_ts").not_eq(timestamp_lit(target, "UTC")),
            )?,
            vec![
                "timestamp-high-only.parquet",
                "timestamp-low-only.parquet",
                "timestamp-missing-stats.parquet",
                "timestamp-pre-epoch-only.parquet",
                "timestamp-range.parquet",
                "timestamp-target-only.parquet",
                "timestamp-target-with-null.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_timestamp_data_column_stats_pruning_documents_null_checks()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_timestamp_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_ts").is_null())?,
            vec![
                "timestamp-all-null.parquet",
                "timestamp-missing-stats.parquet",
                "timestamp-target-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_ts").is_not_null())?,
            vec![
                "timestamp-high-only.parquet",
                "timestamp-low-only.parquet",
                "timestamp-missing-stats.parquet",
                "timestamp-pre-epoch-only.parquet",
                "timestamp-range.parquet",
                "timestamp-target-only.parquet",
                "timestamp-target-with-null.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_timestamp_data_column_stats_pruning_documents_partial_stats_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_partial_timestamp_data_stats_characterization_source()?;
        let low = 1_767_225_599_999_999_i64;

        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_ts").gt(timestamp_lit(low, "UTC")),
            )?,
            vec![
                "timestamp-counts-only.parquet",
                "timestamp-max-only-low.parquet",
                "timestamp-min-only-target.parquet",
                "timestamp-missing-null-count.parquet",
                "timestamp-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_ts").is_null())?,
            vec![
                "timestamp-missing-null-count.parquet",
                "timestamp-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_ts").is_not_null())?,
            vec![
                "timestamp-counts-only.parquet",
                "timestamp-max-only-low.parquet",
                "timestamp-min-only-target.parquet",
                "timestamp-missing-null-count.parquet",
                "timestamp-missing-stats.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_timestamp_ntz_data_column_stats_pruning_documents_comparisons()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_timestamp_ntz_data_stats_characterization_source()?;
        let low = 1_767_225_599_999_999_i64;
        let target = 1_767_225_600_123_456_i64;
        let high = 1_767_225_600_123_457_i64;

        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_ts_ntz").lt(timestamp_ntz_lit(target)),
            )?,
            vec![
                "timestamp-ntz-low-only.parquet",
                "timestamp-ntz-missing-stats.parquet",
                "timestamp-ntz-pre-epoch-only.parquet",
                "timestamp-ntz-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_ts_ntz").gt_eq(timestamp_ntz_lit(target)),
            )?,
            vec![
                "timestamp-ntz-high-only.parquet",
                "timestamp-ntz-missing-stats.parquet",
                "timestamp-ntz-range.parquet",
                "timestamp-ntz-target-only.parquet",
                "timestamp-ntz-target-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &timestamp_ntz_lit(high).gt(col("event_ts_ntz")),
            )?,
            vec![
                "timestamp-ntz-low-only.parquet",
                "timestamp-ntz-missing-stats.parquet",
                "timestamp-ntz-pre-epoch-only.parquet",
                "timestamp-ntz-range.parquet",
                "timestamp-ntz-target-only.parquet",
                "timestamp-ntz-target-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_ts_ntz").eq(timestamp_ntz_lit(low)),
            )?,
            vec![
                "timestamp-ntz-low-only.parquet",
                "timestamp-ntz-missing-stats.parquet",
                "timestamp-ntz-range.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_ts_ntz").not_eq(timestamp_ntz_lit(target)),
            )?,
            vec![
                "timestamp-ntz-high-only.parquet",
                "timestamp-ntz-low-only.parquet",
                "timestamp-ntz-missing-stats.parquet",
                "timestamp-ntz-pre-epoch-only.parquet",
                "timestamp-ntz-range.parquet",
                "timestamp-ntz-target-only.parquet",
                "timestamp-ntz-target-with-null.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_timestamp_ntz_data_column_stats_pruning_documents_null_checks()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_timestamp_ntz_data_stats_characterization_source()?;

        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_ts_ntz").is_null())?,
            vec![
                "timestamp-ntz-all-null.parquet",
                "timestamp-ntz-missing-stats.parquet",
                "timestamp-ntz-target-with-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_ts_ntz").is_not_null())?,
            vec![
                "timestamp-ntz-high-only.parquet",
                "timestamp-ntz-low-only.parquet",
                "timestamp-ntz-missing-stats.parquet",
                "timestamp-ntz-pre-epoch-only.parquet",
                "timestamp-ntz-range.parquet",
                "timestamp-ntz-target-only.parquet",
                "timestamp-ntz-target-with-null.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_timestamp_ntz_data_column_stats_pruning_documents_partial_stats_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_partial_timestamp_ntz_data_stats_characterization_source()?;
        let low = 1_767_225_599_999_999_i64;

        assert_eq!(
            kernel_predicated_stats_file_paths(
                &source,
                &col("event_ts_ntz").gt(timestamp_ntz_lit(low)),
            )?,
            vec![
                "timestamp-ntz-counts-only.parquet",
                "timestamp-ntz-max-only-low.parquet",
                "timestamp-ntz-min-only-target.parquet",
                "timestamp-ntz-missing-null-count.parquet",
                "timestamp-ntz-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_ts_ntz").is_null())?,
            vec![
                "timestamp-ntz-missing-null-count.parquet",
                "timestamp-ntz-missing-stats.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_stats_file_paths(&source, &col("event_ts_ntz").is_not_null())?,
            vec![
                "timestamp-ntz-counts-only.parquet",
                "timestamp-ntz-max-only-low.parquet",
                "timestamp-ntz-min-only-target.parquet",
                "timestamp-ntz-missing-null-count.parquet",
                "timestamp-ntz-missing-stats.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_predicated_stats_scan_exposes_stats_for_surviving_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_data_stats_characterization_source()?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("id").gt(int32_lit(100)))?;
        let stats_scan = build_projected_predicated_stats_delta_scan(&source, Some(predicate))?;

        assert_eq!(
            kernel_scan_file_paths(&stats_scan, source.table_uri())?,
            vec!["id-missing-stats.parquet", "id-possible.parquet"]
        );
        assert_eq!(
            kernel_stats_metadata_boundary(&stats_scan, source.table_uri())?,
            KernelStatsMetadataBoundary {
                selected_rows: 2,
                selected_stats_rows: 1,
                selected_missing_stats_rows: 1,
                has_id_min_values: true,
                has_id_max_values: true,
                has_id_null_count: true,
            }
        );

        Ok(())
    }

    #[test]
    fn kernel_stats_pruning_preserves_surviving_dv_metadata_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "kernel-dv-data-stats-boundary",
            DELETION_VECTOR_PROTOCOL_JSON,
            METADATA_JSON,
            &[
                dv_add_json_with_id_stats("id-dv-impossible.parquet", 10, 1, 50, 0),
                dv_add_json_with_id_stats("id-dv-possible.parquet", 10, 101, 150, 0),
                add_json_with_id_stats("id-plain-impossible.parquet", 10, 1, 50, 0),
                add_json("id-plain-missing-stats.parquet"),
                partitioned_dv_add_json("id-dv-missing-stats.parquet", r#"{}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("id").gt(int32_lit(100)))?;
        let stats_scan = build_projected_predicated_stats_delta_scan(&source, Some(predicate))?;

        assert_eq!(
            kernel_scan_file_boundaries(&stats_scan, source.table_uri())?,
            vec![
                KernelScanFileBoundary {
                    path: "id-dv-missing-stats.parquet".to_owned(),
                    has_deletion_vector: true,
                },
                KernelScanFileBoundary {
                    path: "id-dv-possible.parquet".to_owned(),
                    has_deletion_vector: true,
                },
                KernelScanFileBoundary {
                    path: "id-plain-missing-stats.parquet".to_owned(),
                    has_deletion_vector: false,
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn delta_table_format_production_paths_do_not_load_dv_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("table_formats");
        let production_source = [
            source_root.join("delta.rs"),
            source_root.join("delta").join("kernel.rs"),
        ]
        .iter()
        .map(fs::read_to_string)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|source| {
            source
                .split("\n#[cfg(test)]")
                .next()
                .unwrap_or(source.as_str())
                .to_owned()
        })
        .collect::<Vec<_>>()
        .join("\n");

        assert!(!production_source.contains("get_selection_vector"));
        assert!(!production_source.contains("get_row_indexes"));

        Ok(())
    }

    #[test]
    fn kernel_predicated_scan_preserves_dv_metadata_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "kernel-dv-partition-boundary",
            DELETION_VECTOR_PROTOCOL_JSON,
            PARTITIONED_METADATA_JSON,
            &[
                partitioned_dv_add_json("region-us-west-dv.parquet", r#"{"region":"us-west"}"#),
                partitioned_add_json("region-us-west-plain.parquet", r#"{"region":"us-west"}"#),
                partitioned_dv_add_json("region-us-east-dv.parquet", r#"{"region":"us-east"}"#),
                partitioned_add_json("region-us-east-plain.parquet", r#"{"region":"us-east"}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;
        let predicate = datafusion_expr_to_kernel_predicate(&col("region").eq(lit("us-west")))?;
        let predicated_scan =
            build_projected_predicated_delta_scan(&source, None, Some(predicate))?;

        assert_eq!(
            kernel_scan_file_boundaries(&unfiltered_scan, source.table_uri())?,
            vec![
                KernelScanFileBoundary {
                    path: "region-us-east-dv.parquet".to_owned(),
                    has_deletion_vector: true,
                },
                KernelScanFileBoundary {
                    path: "region-us-east-plain.parquet".to_owned(),
                    has_deletion_vector: false,
                },
                KernelScanFileBoundary {
                    path: "region-us-west-dv.parquet".to_owned(),
                    has_deletion_vector: true,
                },
                KernelScanFileBoundary {
                    path: "region-us-west-plain.parquet".to_owned(),
                    has_deletion_vector: false,
                },
            ]
        );
        assert_eq!(
            kernel_scan_file_boundaries(&predicated_scan, source.table_uri())?,
            vec![
                KernelScanFileBoundary {
                    path: "region-us-west-dv.parquet".to_owned(),
                    has_deletion_vector: true,
                },
                KernelScanFileBoundary {
                    path: "region-us-west-plain.parquet".to_owned(),
                    has_deletion_vector: false,
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_string_null_and_empty_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_partition_characterization_source()?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_eq!(
            kernel_scan_file_paths(&unfiltered_scan, source.table_uri())?,
            vec![
                "region-empty-string.parquet",
                "region-missing.parquet",
                "region-null.parquet",
                "region-us-east.parquet",
                "region-us-west.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("region").is_null())?,
            vec![
                "region-empty-string.parquet",
                "region-missing.parquet",
                "region-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("region").is_not_null())?,
            vec!["region-us-east.parquet", "region-us-west.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("region").eq(lit("us-west")))?,
            vec!["region-us-west.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("region").not_eq(lit("us-west")))?,
            vec!["region-us-east.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("region").eq(lit("")))?,
            Vec::<String>::new()
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("region").not_eq(lit("")))?,
            vec!["region-us-east.parquet", "region-us-west.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("region").in_list(vec![lit("us-west"), lit("")], false),
            )?,
            vec!["region-us-west.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("region").in_list(vec![lit("us-west"), lit("")], true),
            )?,
            vec!["region-us-east.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_boolean_null_and_empty_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_boolean_partition_characterization_source()?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_eq!(
            kernel_scan_file_paths(&unfiltered_scan, source.table_uri())?,
            vec![
                "boolean-empty.parquet",
                "boolean-false.parquet",
                "boolean-missing.parquet",
                "boolean-null.parquet",
                "boolean-true.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("is_current").is_null())?,
            vec![
                "boolean-empty.parquet",
                "boolean-missing.parquet",
                "boolean-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("is_current").is_not_null())?,
            vec!["boolean-false.parquet", "boolean-true.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("is_current").eq(bool_lit(true)))?,
            vec!["boolean-true.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("is_current").eq(bool_lit(false)))?,
            vec!["boolean-false.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("is_current").not_eq(bool_lit(true)))?,
            vec!["boolean-false.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_boolean_membership_and_composition()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_boolean_partition_characterization_source()?;

        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("is_current").in_list(vec![bool_lit(true), bool_lit(false)], false),
            )?,
            vec!["boolean-false.parquet", "boolean-true.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("is_current").in_list(vec![bool_lit(true)], true),
            )?,
            vec!["boolean-false.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("is_current")
                    .eq(bool_lit(true))
                    .or(col("is_current").is_null()),
            )?,
            vec![
                "boolean-empty.parquet",
                "boolean-missing.parquet",
                "boolean-null.parquet",
                "boolean-true.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("is_current")
                    .eq(bool_lit(true))
                    .and(col("is_current").is_not_null()),
            )?,
            vec!["boolean-true.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &Expr::Not(Box::new(col("is_current").eq(bool_lit(true)))),
            )?,
            vec!["boolean-false.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_date_ordering()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_date_partition_characterization_source()?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_eq!(
            kernel_scan_file_paths(&unfiltered_scan, source.table_uri())?,
            vec![
                "date-empty.parquet",
                "date-epoch.parquet",
                "date-leap-day.parquet",
                "date-missing.parquet",
                "date-new-year.parquet",
                "date-null.parquet",
                "date-pre-epoch.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("event_date").gt(date_lit(19_782)))?,
            vec!["date-new-year.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("event_date").lt_eq(date_lit(0)))?,
            vec!["date-epoch.parquet", "date-pre-epoch.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &date_lit(20_454).gt(col("event_date")))?,
            vec![
                "date-epoch.parquet",
                "date-leap-day.parquet",
                "date-pre-epoch.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_date_null_and_empty_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_date_partition_characterization_source()?;

        assert_eq!(
            kernel_predicated_file_paths(&source, &col("event_date").is_null())?,
            vec![
                "date-empty.parquet",
                "date-missing.parquet",
                "date-null.parquet"
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("event_date").is_not_null())?,
            vec![
                "date-epoch.parquet",
                "date-leap-day.parquet",
                "date-new-year.parquet",
                "date-pre-epoch.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("event_date").eq(date_lit(20_454)))?,
            vec!["date-new-year.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("event_date").not_eq(date_lit(20_454)))?,
            vec![
                "date-epoch.parquet",
                "date-leap-day.parquet",
                "date-pre-epoch.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_date_membership_between_and_composition()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_date_partition_characterization_source()?;

        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("event_date").in_list(vec![date_lit(-1), date_lit(20_454)], false),
            )?,
            vec!["date-new-year.parquet", "date-pre-epoch.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("event_date").in_list(vec![date_lit(19_782)], true),
            )?,
            vec![
                "date-epoch.parquet",
                "date-new-year.parquet",
                "date-pre-epoch.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("event_date").between(date_lit(-1), date_lit(19_782)),
            )?,
            vec![
                "date-epoch.parquet",
                "date-leap-day.parquet",
                "date-pre-epoch.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("event_date").not_between(date_lit(-1), date_lit(19_782)),
            )?,
            vec!["date-new-year.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("event_date")
                    .gt_eq(date_lit(0))
                    .and(col("event_date").lt(date_lit(20_454))),
            )?,
            vec!["date-epoch.parquet", "date-leap-day.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("event_date")
                    .eq(date_lit(-1))
                    .or(col("event_date").eq(date_lit(20_454))),
            )?,
            vec!["date-new-year.parquet", "date-pre-epoch.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &Expr::Not(Box::new(col("event_date").eq(date_lit(19_782)))),
            )?,
            vec![
                "date-epoch.parquet",
                "date-new-year.parquet",
                "date-pre-epoch.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_decimal_numeric_ordering()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_decimal_partition_characterization_source()?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_eq!(
            kernel_scan_file_paths(&unfiltered_scan, source.table_uri())?,
            vec![
                "decimal-empty.parquet",
                "decimal-large.parquet",
                "decimal-missing.parquet",
                "decimal-negative.parquet",
                "decimal-null.parquet",
                "decimal-ten.parquet",
                "decimal-two.parquet",
                "decimal-zero.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("amount").gt(decimal_lit(200)))?,
            vec!["decimal-large.parquet", "decimal-ten.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("amount").lt(decimal_lit(1000)))?,
            vec![
                "decimal-negative.parquet",
                "decimal-two.parquet",
                "decimal-zero.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &decimal_lit(1000).gt(col("amount")))?,
            vec![
                "decimal-negative.parquet",
                "decimal-two.parquet",
                "decimal-zero.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_decimal_null_and_empty_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_decimal_partition_characterization_source()?;

        assert_eq!(
            kernel_predicated_file_paths(&source, &col("amount").is_null())?,
            vec![
                "decimal-empty.parquet",
                "decimal-missing.parquet",
                "decimal-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("amount").is_not_null())?,
            vec![
                "decimal-large.parquet",
                "decimal-negative.parquet",
                "decimal-ten.parquet",
                "decimal-two.parquet",
                "decimal-zero.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("amount").eq(decimal_lit(12_345)))?,
            vec!["decimal-large.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("amount").not_eq(decimal_lit(12_345)))?,
            vec![
                "decimal-negative.parquet",
                "decimal-ten.parquet",
                "decimal-two.parquet",
                "decimal-zero.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_decimal_membership_between_and_composition()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_decimal_partition_characterization_source()?;

        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("amount").in_list(vec![decimal_lit(-123), decimal_lit(12_345)], false),
            )?,
            vec!["decimal-large.parquet", "decimal-negative.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("amount").in_list(vec![decimal_lit(200)], true),
            )?,
            vec![
                "decimal-large.parquet",
                "decimal-negative.parquet",
                "decimal-ten.parquet",
                "decimal-zero.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("amount").between(decimal_lit(-123), decimal_lit(200)),
            )?,
            vec![
                "decimal-negative.parquet",
                "decimal-two.parquet",
                "decimal-zero.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("amount").not_between(decimal_lit(-123), decimal_lit(200)),
            )?,
            vec!["decimal-large.parquet", "decimal-ten.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("amount")
                    .gt_eq(decimal_lit(0))
                    .and(col("amount").lt(decimal_lit(12345))),
            )?,
            vec![
                "decimal-ten.parquet",
                "decimal-two.parquet",
                "decimal-zero.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("amount")
                    .eq(decimal_lit(-123))
                    .or(col("amount").eq(decimal_lit(12_345))),
            )?,
            vec!["decimal-large.parquet", "decimal-negative.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &Expr::Not(Box::new(col("amount").eq(decimal_lit(200)))),
            )?,
            vec![
                "decimal-large.parquet",
                "decimal-negative.parquet",
                "decimal-ten.parquet",
                "decimal-zero.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_invalid_decimal_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let invalid_text_table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-invalid-decimal-characterization",
            DECIMAL_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json("decimal-valid.parquet", r#"{"amount":"123.45"}"#),
                partitioned_add_json("decimal-invalid.parquet", r#"{"amount":"not-a-decimal"}"#),
            ],
        )?;
        let invalid_text_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: invalid_text_table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&invalid_text_source, None)?;

        assert_invalid_decimal_partition_error(kernel_scan_file_paths(
            &unfiltered_scan,
            invalid_text_source.table_uri(),
        ))?;
        assert_invalid_decimal_partition_error(kernel_predicated_file_paths(
            &invalid_text_source,
            &col("amount").eq(decimal_lit(12_345)),
        ))?;

        let scale_mismatch_table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-invalid-decimal-scale-characterization",
            DECIMAL_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json("decimal-valid.parquet", r#"{"amount":"123.45"}"#),
                partitioned_add_json("decimal-scale.parquet", r#"{"amount":"123.450"}"#),
            ],
        )?;
        let scale_mismatch_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: scale_mismatch_table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        assert_invalid_decimal_partition_error(kernel_predicated_file_paths(
            &scale_mismatch_source,
            &col("amount").eq(decimal_lit(12_345)),
        ))?;

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_floating_numeric_ordering()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_floating_partition_characterization_source()?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_eq!(
            kernel_scan_file_paths(&unfiltered_scan, source.table_uri())?,
            vec![
                "floating-empty.parquet",
                "floating-missing.parquet",
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-null.parquet",
                "floating-one.parquet",
                "floating-pos-zero.parquet",
                "floating-ten.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("float_part").gt(float32_lit(1.5)))?,
            vec!["floating-ten.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("double_part").lt(float64_lit(0.0)))?,
            vec!["floating-neg-zero.parquet", "floating-neg.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &float64_lit(0.0).gt(col("double_part")))?,
            vec!["floating-neg-zero.parquet", "floating-neg.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("float_part").lt(float32_lit(10.0)))?,
            vec![
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-one.parquet",
                "floating-pos-zero.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_floating_null_and_zero_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_floating_partition_characterization_source()?;

        assert_eq!(
            kernel_predicated_file_paths(&source, &col("float_part").is_null())?,
            vec![
                "floating-empty.parquet",
                "floating-missing.parquet",
                "floating-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("double_part").is_not_null())?,
            vec![
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-one.parquet",
                "floating-pos-zero.parquet",
                "floating-ten.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("float_part").eq(float32_lit(-0.0)))?,
            vec!["floating-neg-zero.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("float_part").eq(float32_lit(0.0)))?,
            vec!["floating-pos-zero.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("double_part").not_eq(float64_lit(0.0)))?,
            vec![
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-one.parquet",
                "floating-ten.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_floating_membership_between_and_composition()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_floating_partition_characterization_source()?;

        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("float_part").in_list(vec![float32_lit(-1.5), float32_lit(1.5)], false),
            )?,
            vec!["floating-neg.parquet", "floating-one.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("double_part").in_list(vec![float64_lit(2.25)], true),
            )?,
            vec![
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-pos-zero.parquet",
                "floating-ten.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("float_part").between(float32_lit(-0.0), float32_lit(1.5)),
            )?,
            vec![
                "floating-neg-zero.parquet",
                "floating-one.parquet",
                "floating-pos-zero.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("double_part").not_between(float64_lit(0.0), float64_lit(2.25)),
            )?,
            vec![
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-ten.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("float_part")
                    .gt_eq(float32_lit(0.0))
                    .and(col("float_part").lt(float32_lit(10.0))),
            )?,
            vec!["floating-one.parquet", "floating-pos-zero.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("double_part")
                    .eq(float64_lit(-2.25))
                    .or(col("double_part").eq(float64_lit(10.0))),
            )?,
            vec!["floating-neg.parquet", "floating-ten.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &Expr::Not(Box::new(col("float_part").eq(float32_lit(1.5)))),
            )?,
            vec![
                "floating-neg-zero.parquet",
                "floating-neg.parquet",
                "floating-pos-zero.parquet",
                "floating-ten.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_binary_null_empty_and_equality_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_binary_partition_characterization_source()?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_eq!(
            kernel_scan_file_paths(&unfiltered_scan, source.table_uri())?,
            vec![
                "binary-HELLO.parquet",
                "binary-empty.parquet",
                "binary-hello.parquet",
                "binary-missing.parquet",
                "binary-null.parquet",
                "binary-special.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::is_null(binary_partition_column())),
            )?,
            vec![
                "binary-empty.parquet",
                "binary-missing.parquet",
                "binary-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::is_not_null(binary_partition_column())),
            )?,
            vec![
                "binary-HELLO.parquet",
                "binary-hello.parquet",
                "binary-special.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, binary_partition_eq(b"HELLO"))?,
            vec!["binary-HELLO.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, binary_partition_eq(b"hello"))?,
            vec!["binary-hello.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, binary_partition_ne(b"hello"))?,
            vec!["binary-HELLO.parquet", "binary-special.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, binary_partition_eq(&[]))?,
            Vec::<String>::new()
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, binary_partition_ne(&[]))?,
            vec![
                "binary-HELLO.parquet",
                "binary-hello.parquet",
                "binary-special.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_binary_membership_and_composition()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_binary_partition_characterization_source()?;

        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::or(
                    Predicate::eq(binary_partition_column(), binary_partition_value(b"HELLO")),
                    Predicate::eq(binary_partition_column(), binary_partition_value(b"/=%")),
                )),
            )?,
            vec!["binary-HELLO.parquet", "binary-special.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::and(
                    Predicate::ne(binary_partition_column(), binary_partition_value(b"hello")),
                    Predicate::ne(binary_partition_column(), binary_partition_value(b"/=%")),
                )),
            )?,
            vec!["binary-HELLO.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::and(
                    Predicate::is_not_null(binary_partition_column()),
                    Predicate::ne(binary_partition_column(), binary_partition_value(b"HELLO")),
                )),
            )?,
            vec!["binary-hello.parquet", "binary-special.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::or(
                    Predicate::eq(binary_partition_column(), binary_partition_value(b"HELLO")),
                    Predicate::is_null(binary_partition_column()),
                )),
            )?,
            vec![
                "binary-HELLO.parquet",
                "binary-empty.parquet",
                "binary-missing.parquet",
                "binary-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::not(Predicate::eq(
                    binary_partition_column(),
                    binary_partition_value(b"hello"),
                ))),
            )?,
            vec!["binary-HELLO.parquet", "binary-special.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_non_utf8_binary_literals_do_not_match()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_binary_partition_characterization_source()?;

        assert_eq!(
            kernel_predicate_file_paths(&source, binary_partition_eq(&[0xDE, 0xAD, 0xBE, 0xEF]))?,
            Vec::<String>::new()
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, binary_partition_ne(&[0xDE, 0xAD, 0xBE, 0xEF]))?,
            vec![
                "binary-HELLO.parquet",
                "binary-hello.parquet",
                "binary-special.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_timestamp_ordering_and_precision()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_timestamp_partition_characterization_source()?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;
        let pre_epoch = -1_i64;
        let low = 1_767_225_599_999_999_i64;
        let target = 1_767_225_600_123_456_i64;
        let high = 1_767_225_600_123_457_i64;

        assert_eq!(
            kernel_scan_file_paths(&unfiltered_scan, source.table_uri())?,
            vec![
                "timestamp-empty.parquet",
                "timestamp-high.parquet",
                "timestamp-low.parquet",
                "timestamp-missing.parquet",
                "timestamp-null.parquet",
                "timestamp-pre-epoch.parquet",
                "timestamp-target.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::lt(
                    timestamp_partition_column(),
                    timestamp_partition_value(target),
                )),
            )?,
            vec!["timestamp-low.parquet", "timestamp-pre-epoch.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::ge(
                    timestamp_partition_column(),
                    timestamp_partition_value(target),
                )),
            )?,
            vec!["timestamp-high.parquet", "timestamp-target.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::gt(
                    timestamp_partition_value(high),
                    timestamp_partition_column(),
                )),
            )?,
            vec![
                "timestamp-low.parquet",
                "timestamp-pre-epoch.parquet",
                "timestamp-target.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, timestamp_partition_eq(pre_epoch))?,
            vec!["timestamp-pre-epoch.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, timestamp_partition_eq(target))?,
            vec!["timestamp-target.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, timestamp_partition_ne(target))?,
            vec![
                "timestamp-high.parquet",
                "timestamp-low.parquet",
                "timestamp-pre-epoch.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, timestamp_partition_eq(target + 1))?,
            vec!["timestamp-high.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, timestamp_partition_eq(low))?,
            vec!["timestamp-low.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_timestamp_null_empty_and_membership()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_timestamp_partition_characterization_source()?;
        let low = 1_767_225_599_999_999_i64;
        let target = 1_767_225_600_123_456_i64;

        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::is_null(timestamp_partition_column())),
            )?,
            vec![
                "timestamp-empty.parquet",
                "timestamp-missing.parquet",
                "timestamp-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::is_not_null(timestamp_partition_column())),
            )?,
            vec![
                "timestamp-high.parquet",
                "timestamp-low.parquet",
                "timestamp-pre-epoch.parquet",
                "timestamp-target.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::or(
                    Predicate::eq(timestamp_partition_column(), timestamp_partition_value(low)),
                    Predicate::eq(
                        timestamp_partition_column(),
                        timestamp_partition_value(target)
                    ),
                )),
            )?,
            vec!["timestamp-low.parquet", "timestamp-target.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::and(
                    Predicate::ne(timestamp_partition_column(), timestamp_partition_value(low)),
                    Predicate::ne(
                        timestamp_partition_column(),
                        timestamp_partition_value(target)
                    ),
                )),
            )?,
            vec!["timestamp-high.parquet", "timestamp-pre-epoch.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_timestamp_between_and_composition()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_timestamp_partition_characterization_source()?;
        let low = 1_767_225_599_999_999_i64;
        let target = 1_767_225_600_123_456_i64;
        let high = 1_767_225_600_123_457_i64;

        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::and(
                    Predicate::ge(timestamp_partition_column(), timestamp_partition_value(low)),
                    Predicate::le(
                        timestamp_partition_column(),
                        timestamp_partition_value(target)
                    ),
                )),
            )?,
            vec!["timestamp-low.parquet", "timestamp-target.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::or(
                    Predicate::lt(timestamp_partition_column(), timestamp_partition_value(low)),
                    Predicate::gt(
                        timestamp_partition_column(),
                        timestamp_partition_value(target)
                    ),
                )),
            )?,
            vec!["timestamp-high.parquet", "timestamp-pre-epoch.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::and(
                    Predicate::gt(timestamp_partition_column(), timestamp_partition_value(low)),
                    Predicate::le(
                        timestamp_partition_column(),
                        timestamp_partition_value(high)
                    ),
                )),
            )?,
            vec!["timestamp-high.parquet", "timestamp-target.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::or(
                    Predicate::eq(timestamp_partition_column(), timestamp_partition_value(low)),
                    Predicate::is_null(timestamp_partition_column()),
                )),
            )?,
            vec![
                "timestamp-empty.parquet",
                "timestamp-low.parquet",
                "timestamp-missing.parquet",
                "timestamp-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::not(Predicate::eq(
                    timestamp_partition_column(),
                    timestamp_partition_value(target),
                ))),
            )?,
            vec![
                "timestamp-high.parquet",
                "timestamp-low.parquet",
                "timestamp-pre-epoch.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_timestamp_ntz_ordering_and_formatting()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_timestamp_ntz_partition_characterization_source()?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;
        let pre_epoch = -1_i64;
        let low = 1_767_225_599_999_999_i64;
        let target = 1_767_225_600_123_456_i64;
        let high = 1_767_225_600_123_457_i64;

        assert_eq!(
            kernel_scan_file_paths(&unfiltered_scan, source.table_uri())?,
            vec![
                "timestamp-ntz-empty.parquet",
                "timestamp-ntz-high.parquet",
                "timestamp-ntz-low-space.parquet",
                "timestamp-ntz-missing.parquet",
                "timestamp-ntz-null.parquet",
                "timestamp-ntz-pre-epoch.parquet",
                "timestamp-ntz-target-space.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::lt(
                    timestamp_ntz_partition_column(),
                    timestamp_ntz_partition_value(target),
                )),
            )?,
            vec![
                "timestamp-ntz-low-space.parquet",
                "timestamp-ntz-pre-epoch.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::ge(
                    timestamp_ntz_partition_column(),
                    timestamp_ntz_partition_value(target),
                )),
            )?,
            vec![
                "timestamp-ntz-high.parquet",
                "timestamp-ntz-target-space.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::gt(
                    timestamp_ntz_partition_value(high),
                    timestamp_ntz_partition_column(),
                )),
            )?,
            vec![
                "timestamp-ntz-low-space.parquet",
                "timestamp-ntz-pre-epoch.parquet",
                "timestamp-ntz-target-space.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, timestamp_ntz_partition_eq(pre_epoch))?,
            vec!["timestamp-ntz-pre-epoch.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, timestamp_ntz_partition_eq(target))?,
            vec!["timestamp-ntz-target-space.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, timestamp_ntz_partition_ne(target))?,
            vec![
                "timestamp-ntz-high.parquet",
                "timestamp-ntz-low-space.parquet",
                "timestamp-ntz-pre-epoch.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, timestamp_ntz_partition_eq(high))?,
            vec!["timestamp-ntz-high.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(&source, timestamp_ntz_partition_eq(low))?,
            vec!["timestamp-ntz-low-space.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_timestamp_ntz_null_empty_and_membership()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_timestamp_ntz_partition_characterization_source()?;
        let low = 1_767_225_599_999_999_i64;
        let target = 1_767_225_600_123_456_i64;

        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::is_null(timestamp_ntz_partition_column())),
            )?,
            vec![
                "timestamp-ntz-empty.parquet",
                "timestamp-ntz-missing.parquet",
                "timestamp-ntz-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::is_not_null(timestamp_ntz_partition_column())),
            )?,
            vec![
                "timestamp-ntz-high.parquet",
                "timestamp-ntz-low-space.parquet",
                "timestamp-ntz-pre-epoch.parquet",
                "timestamp-ntz-target-space.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::or(
                    Predicate::eq(
                        timestamp_ntz_partition_column(),
                        timestamp_ntz_partition_value(low)
                    ),
                    Predicate::eq(
                        timestamp_ntz_partition_column(),
                        timestamp_ntz_partition_value(target)
                    ),
                )),
            )?,
            vec![
                "timestamp-ntz-low-space.parquet",
                "timestamp-ntz-target-space.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::and(
                    Predicate::ne(
                        timestamp_ntz_partition_column(),
                        timestamp_ntz_partition_value(low)
                    ),
                    Predicate::ne(
                        timestamp_ntz_partition_column(),
                        timestamp_ntz_partition_value(target)
                    ),
                )),
            )?,
            vec![
                "timestamp-ntz-high.parquet",
                "timestamp-ntz-pre-epoch.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_timestamp_ntz_between_and_composition()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_timestamp_ntz_partition_characterization_source()?;
        let low = 1_767_225_599_999_999_i64;
        let target = 1_767_225_600_123_456_i64;
        let high = 1_767_225_600_123_457_i64;

        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::and(
                    Predicate::ge(
                        timestamp_ntz_partition_column(),
                        timestamp_ntz_partition_value(low)
                    ),
                    Predicate::le(
                        timestamp_ntz_partition_column(),
                        timestamp_ntz_partition_value(target)
                    ),
                )),
            )?,
            vec![
                "timestamp-ntz-low-space.parquet",
                "timestamp-ntz-target-space.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::or(
                    Predicate::lt(
                        timestamp_ntz_partition_column(),
                        timestamp_ntz_partition_value(low)
                    ),
                    Predicate::gt(
                        timestamp_ntz_partition_column(),
                        timestamp_ntz_partition_value(target)
                    ),
                )),
            )?,
            vec![
                "timestamp-ntz-high.parquet",
                "timestamp-ntz-pre-epoch.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::and(
                    Predicate::gt(
                        timestamp_ntz_partition_column(),
                        timestamp_ntz_partition_value(low)
                    ),
                    Predicate::le(
                        timestamp_ntz_partition_column(),
                        timestamp_ntz_partition_value(high)
                    ),
                )),
            )?,
            vec![
                "timestamp-ntz-high.parquet",
                "timestamp-ntz-target-space.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::or(
                    Predicate::eq(
                        timestamp_ntz_partition_column(),
                        timestamp_ntz_partition_value(low)
                    ),
                    Predicate::is_null(timestamp_ntz_partition_column()),
                )),
            )?,
            vec![
                "timestamp-ntz-empty.parquet",
                "timestamp-ntz-low-space.parquet",
                "timestamp-ntz-missing.parquet",
                "timestamp-ntz-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::not(Predicate::eq(
                    timestamp_ntz_partition_column(),
                    timestamp_ntz_partition_value(target),
                ))),
            )?,
            vec![
                "timestamp-ntz-high.parquet",
                "timestamp-ntz-low-space.parquet",
                "timestamp-ntz-pre-epoch.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_invalid_timestamp_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-invalid-timestamp-characterization",
            TIMESTAMP_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json(
                    "timestamp-valid.parquet",
                    r#"{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
                ),
                partitioned_add_json(
                    "timestamp-invalid.parquet",
                    r#"{"event_ts":"not-a-timestamp"}"#,
                ),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_invalid_timestamp_partition_error(kernel_scan_file_paths(
            &unfiltered_scan,
            source.table_uri(),
        ))?;
        assert_invalid_timestamp_partition_error(kernel_predicate_file_paths(
            &source,
            timestamp_partition_eq(1_767_225_600_123_456),
        ))?;

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_invalid_timestamp_ntz_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let invalid_text_table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "kernel-invalid-timestamp-ntz-characterization",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json(
                    "timestamp-ntz-valid.parquet",
                    r#"{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
                ),
                partitioned_add_json(
                    "timestamp-ntz-invalid.parquet",
                    r#"{"event_ts_ntz":"not-a-timestamp"}"#,
                ),
            ],
        )?;
        let invalid_text_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: invalid_text_table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&invalid_text_source, None)?;

        assert_invalid_timestamp_ntz_partition_error(kernel_scan_file_paths(
            &unfiltered_scan,
            invalid_text_source.table_uri(),
        ))?;
        assert_invalid_timestamp_ntz_partition_error(kernel_predicate_file_paths(
            &invalid_text_source,
            timestamp_ntz_partition_eq(1_767_225_600_123_456),
        ))?;

        let t_separator_table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "kernel-t-separator-timestamp-ntz-characterization",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json(
                    "timestamp-ntz-valid.parquet",
                    r#"{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
                ),
                partitioned_add_json(
                    "timestamp-ntz-t-separator.parquet",
                    r#"{"event_ts_ntz":"2026-01-01T00:00:00.123456"}"#,
                ),
            ],
        )?;
        let t_separator_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: t_separator_table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&t_separator_source, None)?;

        assert_invalid_timestamp_ntz_partition_error(kernel_scan_file_paths(
            &unfiltered_scan,
            t_separator_source.table_uri(),
        ))?;
        assert_invalid_timestamp_ntz_partition_error(kernel_predicate_file_paths(
            &t_separator_source,
            timestamp_ntz_partition_eq(1_767_225_600_123_456),
        ))?;

        let zone_table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "kernel-zone-timestamp-ntz-characterization",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TIMESTAMP_NTZ_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json(
                    "timestamp-ntz-valid.parquet",
                    r#"{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
                ),
                partitioned_add_json(
                    "timestamp-ntz-zone.parquet",
                    r#"{"event_ts_ntz":"2026-01-01T00:00:00.123456Z"}"#,
                ),
            ],
        )?;
        let zone_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: zone_table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&zone_source, None)?;

        assert_invalid_timestamp_ntz_partition_error(kernel_scan_file_paths(
            &unfiltered_scan,
            zone_source.table_uri(),
        ))?;
        assert_invalid_timestamp_ntz_partition_error(kernel_predicate_file_paths(
            &zone_source,
            timestamp_ntz_partition_eq(1_767_225_600_123_456),
        ))?;

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_invalid_floating_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let invalid_text_table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-invalid-floating-characterization",
            FLOATING_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json(
                    "floating-valid.parquet",
                    r#"{"float_part":"1.5","double_part":"2.25"}"#,
                ),
                partitioned_add_json(
                    "floating-invalid.parquet",
                    r#"{"float_part":"not-a-float","double_part":"not-a-double"}"#,
                ),
            ],
        )?;
        let invalid_text_source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: invalid_text_table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&invalid_text_source, None)?;

        assert_invalid_floating_partition_error(kernel_scan_file_paths(
            &unfiltered_scan,
            invalid_text_source.table_uri(),
        ))?;
        assert_invalid_floating_partition_error(kernel_predicated_file_paths(
            &invalid_text_source,
            &col("float_part").eq(float32_lit(1.5)),
        ))?;

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_nonfinite_floating_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-nonfinite-floating-characterization",
            FLOATING_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json(
                    "floating-valid.parquet",
                    r#"{"float_part":"1.5","double_part":"2.25"}"#,
                ),
                partitioned_add_json(
                    "floating-nan.parquet",
                    r#"{"float_part":"NaN","double_part":"NaN"}"#,
                ),
                partitioned_add_json(
                    "floating-inf.parquet",
                    r#"{"float_part":"Infinity","double_part":"Infinity"}"#,
                ),
                partitioned_add_json(
                    "floating-neg-inf.parquet",
                    r#"{"float_part":"-Infinity","double_part":"-Infinity"}"#,
                ),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_eq!(
            kernel_scan_file_paths(&unfiltered_scan, source.table_uri())?,
            vec![
                "floating-inf.parquet",
                "floating-nan.parquet",
                "floating-neg-inf.parquet",
                "floating-valid.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("float_part").gt(float32_lit(0.0)))?,
            vec![
                "floating-inf.parquet",
                "floating-nan.parquet",
                "floating-valid.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("double_part").lt(float64_lit(0.0)))?,
            vec!["floating-neg-inf.parquet"]
        );
        assert_eq!(
            kernel_predicate_file_paths(
                &source,
                DeltaKernelPredicate::new(Predicate::eq(
                    Expression::Column(ColumnName::new(["float_part"])),
                    Expression::Literal(Scalar::Float(f32::NAN)),
                )),
            )?,
            vec!["floating-nan.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_invalid_date_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-invalid-date-characterization",
            DATE_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json("date-valid.parquet", r#"{"event_date":"2026-01-01"}"#),
                partitioned_add_json("date-invalid.parquet", r#"{"event_date":"not-a-date"}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_invalid_date_partition_error(kernel_scan_file_paths(
            &unfiltered_scan,
            source.table_uri(),
        ))?;
        assert_invalid_date_partition_error(kernel_predicated_file_paths(
            &source,
            &col("event_date").eq(date_lit(20_454)),
        ))?;

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_invalid_boolean_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-invalid-boolean-characterization",
            BOOLEAN_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json("boolean-valid.parquet", r#"{"is_current":"true"}"#),
                partitioned_add_json(
                    "boolean-invalid.parquet",
                    r#"{"is_current":"not-a-boolean"}"#,
                ),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_invalid_boolean_partition_error(kernel_scan_file_paths(
            &unfiltered_scan,
            source.table_uri(),
        ))?;
        assert_invalid_boolean_partition_error(kernel_predicated_file_paths(
            &source,
            &col("is_current").eq(bool_lit(true)),
        ))?;

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_integer_numeric_ordering()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_integer_partition_characterization_source()?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_eq!(
            kernel_scan_file_paths(&unfiltered_scan, source.table_uri())?,
            vec![
                "integer--1.parquet",
                "integer-10.parquet",
                "integer-2.parquet",
                "integer-empty.parquet",
                "integer-missing.parquet",
                "integer-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("long_part").gt(int64_lit(2)))?,
            vec!["integer-10.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("long_part").lt(int64_lit(10)))?,
            vec!["integer--1.parquet", "integer-2.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &int64_lit(10).gt(col("long_part")))?,
            vec!["integer--1.parquet", "integer-2.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_integer_null_and_empty_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_integer_partition_characterization_source()?;

        assert_eq!(
            kernel_predicated_file_paths(&source, &col("long_part").is_null())?,
            vec![
                "integer-empty.parquet",
                "integer-missing.parquet",
                "integer-null.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("long_part").is_not_null())?,
            vec![
                "integer--1.parquet",
                "integer-10.parquet",
                "integer-2.parquet",
            ]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("long_part").eq(int64_lit(2)))?,
            vec!["integer-2.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("long_part").not_eq(int64_lit(2)))?,
            vec!["integer--1.parquet", "integer-10.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_integer_membership_between_and_composition()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, source) = kernel_integer_partition_characterization_source()?;

        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("long_part").in_list(vec![int64_lit(-1), int64_lit(10)], false),
            )?,
            vec!["integer--1.parquet", "integer-10.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("long_part").in_list(vec![int64_lit(2)], true),
            )?,
            vec!["integer--1.parquet", "integer-10.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("long_part").between(int64_lit(-1), int64_lit(2)),
            )?,
            vec!["integer--1.parquet", "integer-2.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("long_part").not_between(int64_lit(-1), int64_lit(2)),
            )?,
            vec!["integer-10.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("long_part")
                    .gt_eq(int64_lit(-1))
                    .and(col("long_part").lt(int64_lit(10))),
            )?,
            vec!["integer--1.parquet", "integer-2.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &col("long_part")
                    .eq(int64_lit(-1))
                    .or(col("long_part").eq(int64_lit(10))),
            )?,
            vec!["integer--1.parquet", "integer-10.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(
                &source,
                &Expr::Not(Box::new(col("long_part").eq(int64_lit(2)))),
            )?,
            vec!["integer--1.parquet", "integer-10.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_integer_width_scalars()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-integer-width-characterization",
            INTEGER_PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json(
                    "width-byte-min.parquet",
                    r#"{"byte_part":"-128","short_part":"0","int_part":"0","long_part":"0"}"#,
                ),
                partitioned_add_json(
                    "width-short-max.parquet",
                    r#"{"byte_part":"0","short_part":"32767","int_part":"0","long_part":"0"}"#,
                ),
                partitioned_add_json(
                    "width-int-max.parquet",
                    r#"{"byte_part":"0","short_part":"0","int_part":"2147483647","long_part":"0"}"#,
                ),
                partitioned_add_json(
                    "width-long-max.parquet",
                    r#"{"byte_part":"0","short_part":"0","int_part":"0","long_part":"9223372036854775807"}"#,
                ),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        assert_eq!(
            kernel_predicated_file_paths(&source, &col("byte_part").eq(int8_lit(i8::MIN)))?,
            vec!["width-byte-min.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("short_part").eq(int16_lit(i16::MAX)))?,
            vec!["width-short-max.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("int_part").eq(int32_lit(i32::MAX)))?,
            vec!["width-int-max.parquet"]
        );
        assert_eq!(
            kernel_predicated_file_paths(&source, &col("long_part").eq(int64_lit(i64::MAX)))?,
            vec!["width-long-max.parquet"]
        );

        Ok(())
    }

    #[test]
    fn kernel_partition_characterization_documents_invalid_integer_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "kernel-invalid-integer-characterization",
            INTEGER_PARTITIONED_METADATA_JSON,
            &[
                integer_partitioned_add_json("integer-valid.parquet", "7"),
                partitioned_add_json(
                    "integer-invalid.parquet",
                    r#"{"byte_part":"0","short_part":"0","int_part":"0","long_part":"not-an-integer"}"#,
                ),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let unfiltered_scan = build_projected_delta_scan(&source, None)?;

        assert_invalid_integer_partition_error(kernel_scan_file_paths(
            &unfiltered_scan,
            source.table_uri(),
        ))?;
        assert_invalid_integer_partition_error(kernel_predicated_file_paths(
            &source,
            &col("long_part").eq(int64_lit(7)),
        ))?;

        Ok(())
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
