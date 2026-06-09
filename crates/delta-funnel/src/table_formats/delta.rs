//! Delta table-format source loading.

use crate::DeltaFunnelError;

mod kernel;
mod partition_metadata_predicate;
mod protocol;
mod snapshot;
mod uri;

use super::validate_table_source_names;
use kernel::{ArrowSchemaRef, Version, snapshot_arrow_schema};
pub(crate) use kernel::{
    DeltaKernelPredicate, DeltaKernelPredicateAdapterError, datafusion_expr_to_kernel_predicate,
};
pub(crate) use partition_metadata_predicate::DeltaPartitionMetadataPredicate;
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
    /// Returns scan file paths after kernel scan planning and optional metadata filtering.
    ///
    /// The kernel scan may already carry a delta_kernel predicate. Tests may
    /// also pass a provider-owned metadata predicate to mirror legacy pruning:
    /// expand kernel scan metadata first, then optionally evaluate the provider
    /// predicate against each `ScanFile`'s partition values.
    pub(crate) fn scan_file_paths(
        &self,
        table_uri: &str,
        partition_metadata_filter: Option<&DeltaPartitionMetadataPredicate>,
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

        let mut paths = files
            .into_iter()
            .filter(|file| {
                // This is the provider-owned partition pruning step. A file is
                // kept only when there is no pushed partition predicate or when
                // that predicate evaluates to SQL TRUE for the file metadata.
                partition_metadata_filter
                    .is_none_or(|predicate| predicate.matches_scan_file(&file.partition_values))
            })
            .map(|file| file.path)
            .collect::<Vec<_>>();
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
    use std::collections::HashSet;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, col, lit};

    use super::partition_metadata_predicate::DeltaPartitionNameMap;
    use super::{
        DeltaPartitionMetadataPredicate, DeltaSourceConfig, ProjectedDeltaScan,
        build_projected_delta_scan, build_projected_predicated_delta_scan,
        datafusion_expr_to_kernel_predicate, load_delta_source, load_delta_sources,
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
            let path = Path::new("target")
                .join("delta-funnel-named-source-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{PROTOCOL_JSON}\n{metadata_json}\n"),
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

    fn kernel_predicated_file_paths(
        source: &super::PlannedDeltaSource,
        filter: &datafusion::logical_expr::Expr,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let predicate = datafusion_expr_to_kernel_predicate(filter)?;
        let scan = build_projected_predicated_delta_scan(source, None, Some(predicate))?;

        kernel_scan_file_paths(&scan, source.table_uri())
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
    fn kernel_predicated_scan_prunes_files_without_provider_metadata_filter()
    -> Result<(), Box<dyn std::error::Error>> {
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
    fn scan_file_paths_can_apply_partition_metadata_filter()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_metadata_and_adds(
            "partition-metadata-filtered-scan-files",
            PARTITIONED_METADATA_JSON,
            &[
                partitioned_add_json("part-00000.parquet", r#"{"region":"us-west"}"#),
                partitioned_add_json("part-00001.parquet", r#"{"region":""}"#),
                partitioned_add_json("part-00002.parquet", r#"{}"#),
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;
        let partition_columns = HashSet::from(["region".to_owned()]);
        let physical_name_lookup = DeltaPartitionNameMap::identity(&partition_columns);
        let metadata_filter = DeltaPartitionMetadataPredicate::from_datafusion_expr(
            &col("region").eq(lit("")),
            &super::delta_source_arrow_schema(&source)?,
            &partition_columns,
            &physical_name_lookup,
        )?;
        let scan = build_projected_delta_scan(&source, None)?;

        assert_eq!(
            scan.scan_file_paths(source.table_uri(), None)?,
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet",
            ]
        );
        assert_eq!(
            scan.scan_file_paths(source.table_uri(), Some(&metadata_filter))?,
            vec!["part-00001.parquet"]
        );

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
