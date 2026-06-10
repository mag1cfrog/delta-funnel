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

    fn decimal_lit(value: i128) -> Expr {
        Expr::Literal(ScalarValue::Decimal128(Some(value), 10, 2), None)
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
