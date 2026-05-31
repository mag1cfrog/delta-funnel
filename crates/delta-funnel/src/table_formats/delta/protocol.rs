//! Delta protocol preflight.

use crate::DeltaFunnelError;
use crate::redaction::sanitize_uri_for_display;

use super::PlannedDeltaSource;
use super::kernel::{
    DeltaKernelProtocol, TABLE_FEATURES_MIN_READER_VERSION, Version, snapshot_protocol_report,
};

// Reader features are Delta correctness requirements. Keep this allowlist
// empty until the provider execution path proves support for each feature.
// For example, `deletionVectors` must remain rejected until rows are masked
// before reaching DataFusion.
const SUPPORTED_READER_FEATURES: &[&str] = &[];

/// Protocol details for one named Delta source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaProtocolReport {
    /// DataFusion table name for this source.
    pub source_name: String,
    /// Sanitized normalized Delta table URI context.
    pub table_uri: String,
    /// Resolved Delta snapshot version.
    pub snapshot_version: Version,
    /// Delta minimum reader protocol version.
    pub min_reader_version: i32,
    /// Delta minimum writer protocol version.
    pub min_writer_version: i32,
    /// Delta reader table features required by this source.
    pub reader_features: Vec<String>,
    /// Delta writer table features advertised by this source.
    pub writer_features: Vec<String>,
}

/// Successful protocol preflight for one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolPreflight {
    /// Protocol report captured during preflight.
    pub protocol: DeltaProtocolReport,
}

/// Runs conservative Delta protocol preflight for one loaded source.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::DeltaProtocolCompatibility`] when the source
/// requires a reader protocol version or reader feature that DeltaFunnel does
/// not support yet.
pub fn preflight_delta_protocol(
    source: &PlannedDeltaSource,
) -> Result<ProtocolPreflight, DeltaFunnelError> {
    let protocol = delta_protocol_report(source);

    ensure_protocol_supported(&protocol)?;

    Ok(ProtocolPreflight { protocol })
}

/// Runs conservative Delta protocol preflight for loaded sources.
///
/// # Errors
///
/// Returns the first source-specific protocol compatibility error.
pub fn preflight_delta_sources(
    sources: &[PlannedDeltaSource],
) -> Result<Vec<ProtocolPreflight>, DeltaFunnelError> {
    sources.iter().map(preflight_delta_protocol).collect()
}

/// Extracts protocol details for one loaded source without applying policy.
#[must_use]
pub fn delta_protocol_report(source: &PlannedDeltaSource) -> DeltaProtocolReport {
    let kernel = snapshot_protocol_report(source.loaded_snapshot().kernel_snapshot());

    report_from_kernel(source, kernel)
}

fn report_from_kernel(
    source: &PlannedDeltaSource,
    kernel: DeltaKernelProtocol,
) -> DeltaProtocolReport {
    build_protocol_report(source.name(), source.table_uri(), source.version(), kernel)
}

fn build_protocol_report(
    source_name: &str,
    table_uri: &str,
    snapshot_version: Version,
    kernel: DeltaKernelProtocol,
) -> DeltaProtocolReport {
    DeltaProtocolReport {
        source_name: source_name.to_owned(),
        table_uri: sanitize_uri_for_display(table_uri),
        snapshot_version,
        min_reader_version: kernel.min_reader_version,
        min_writer_version: kernel.min_writer_version,
        reader_features: kernel.reader_features,
        writer_features: kernel.writer_features,
    }
}

fn ensure_protocol_supported(protocol: &DeltaProtocolReport) -> Result<(), DeltaFunnelError> {
    if !is_supported_reader_version(protocol.min_reader_version) {
        return Err(compatibility_error(
            protocol,
            format!(
                "unsupported Delta minReaderVersion {}",
                protocol.min_reader_version
            ),
        ));
    }

    if let Some(feature) = unsupported_reader_feature(&protocol.reader_features) {
        return Err(compatibility_error(
            protocol,
            format!(
                "unsupported Delta reader feature `{}`",
                sanitize_value_for_display(feature)
            ),
        ));
    }

    Ok(())
}

fn unsupported_reader_feature(features: &[String]) -> Option<&str> {
    features
        .iter()
        .map(String::as_str)
        .find(|feature| !SUPPORTED_READER_FEATURES.contains(feature))
}

fn is_supported_reader_version(version: i32) -> bool {
    // Version 1 is the basic legacy read protocol. Version 3 is Delta's
    // table-feature protocol; the concrete reader requirements are then
    // expressed by `readerFeatures` and checked separately above.
    // Legacy reader version 2 implies column mapping support, so keep it
    // rejected until physical-to-logical column handling is implemented.
    matches!(version, 1) || version == TABLE_FEATURES_MIN_READER_VERSION
}

fn compatibility_error(protocol: &DeltaProtocolReport, reason: String) -> DeltaFunnelError {
    DeltaFunnelError::DeltaProtocolCompatibility {
        source_name: protocol.source_name.clone(),
        table_uri: protocol.table_uri.clone(),
        snapshot_version: protocol.snapshot_version,
        reason,
    }
}

fn sanitize_value_for_display(value: &str) -> String {
    value.chars().flat_map(char::escape_default).collect()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        DeltaProtocolReport, delta_protocol_report, preflight_delta_protocol,
        preflight_delta_sources,
    };
    use crate::{DeltaFunnelError, DeltaSourceConfig, load_delta_source, load_delta_sources};

    struct DeltaLogTable {
        path: PathBuf,
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl DeltaLogTable {
        fn new(name: &str, protocol_json: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-funnel-protocol-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{protocol_json}\n{METADATA_JSON}\n"),
            )?;
            fs::write(
                log_path.join("00000000000000000001.json"),
                format!("{}\n", add_json("part-00001.parquet")),
            )?;

            Ok(Self { path })
        }
    }

    const LEGACY_PROTOCOL_JSON: &str =
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const WRITER_ONLY_FEATURE_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["inCommitTimestamp"]}}"#;
    const DELETION_VECTOR_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#;
    const UNKNOWN_READER_FEATURE_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["madeUpFeature"],"writerFeatures":["madeUpFeature"]}}"#;
    const LEGACY_COLUMN_MAPPING_PROTOCOL_JSON: &str =
        r#"{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}"#;
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

    fn load_source(
        name: &str,
        protocol_json: &str,
    ) -> Result<crate::PlannedDeltaSource, Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new(name, protocol_json)?;
        let source = load_delta_source(DeltaSourceConfig {
            name: name.to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        })?;

        Ok(source)
    }

    fn source_config(name: &str, table: &DeltaLogTable) -> DeltaSourceConfig {
        DeltaSourceConfig {
            name: name.to_owned(),
            table_uri: table.path.to_string_lossy().to_string(),
            version: None,
        }
    }

    #[test]
    fn reports_legacy_protocol_details() -> Result<(), Box<dyn std::error::Error>> {
        let source = load_source("orders", LEGACY_PROTOCOL_JSON)?;

        let report = delta_protocol_report(&source);

        assert_eq!(report.source_name, "orders");
        assert!(report.table_uri.starts_with("file://"));
        assert_eq!(report.snapshot_version, 1);
        assert_eq!(report.min_reader_version, 1);
        assert_eq!(report.min_writer_version, 2);
        assert!(report.reader_features.is_empty());
        assert!(report.writer_features.is_empty());

        Ok(())
    }

    #[test]
    fn protocol_report_sanitizes_uri_context() {
        let report = super::build_protocol_report(
            "orders",
            "s3://user:password@example.com/table?token=secret#debug",
            42,
            super::DeltaKernelProtocol {
                min_reader_version: 1,
                min_writer_version: 2,
                reader_features: Vec::new(),
                writer_features: Vec::new(),
            },
        );

        assert_eq!(report.table_uri, "s3://example.com/table");
    }

    #[test]
    fn preflight_allows_writer_only_features() -> Result<(), Box<dyn std::error::Error>> {
        let source = load_source("orders", WRITER_ONLY_FEATURE_PROTOCOL_JSON)?;

        let preflight = preflight_delta_protocol(&source)?;

        assert_eq!(preflight.protocol.min_reader_version, 3);
        assert!(preflight.protocol.reader_features.is_empty());
        assert_eq!(
            preflight.protocol.writer_features,
            vec!["inCommitTimestamp"]
        );

        Ok(())
    }

    #[test]
    fn preflight_rejects_deletion_vectors_before_execution_support()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = load_source("orders", DELETION_VECTOR_PROTOCOL_JSON)?;

        let result = preflight_delta_protocol(&source);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaProtocolCompatibility {
                source_name,
                reason,
                ..
            }) if source_name == "orders" && reason.contains("deletionVectors")
        ));

        Ok(())
    }

    #[test]
    fn preflight_rejects_unknown_reader_features() -> Result<(), Box<dyn std::error::Error>> {
        let source = load_source("orders", UNKNOWN_READER_FEATURE_PROTOCOL_JSON)?;

        let result = preflight_delta_protocol(&source);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaProtocolCompatibility {
                reason,
                ..
            }) if reason.contains("madeUpFeature")
        ));

        Ok(())
    }

    #[test]
    fn preflight_rejects_legacy_column_mapping_version() -> Result<(), Box<dyn std::error::Error>> {
        let source = load_source("orders", LEGACY_COLUMN_MAPPING_PROTOCOL_JSON)?;

        let result = preflight_delta_protocol(&source);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaProtocolCompatibility {
                reason,
                ..
            }) if reason.contains("minReaderVersion 2")
        ));

        Ok(())
    }

    #[test]
    fn preflights_multiple_sources() -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("orders", LEGACY_PROTOCOL_JSON)?;
        let customers = DeltaLogTable::new("customers", WRITER_ONLY_FEATURE_PROTOCOL_JSON)?;
        let sources = load_delta_sources([
            source_config("orders", &orders),
            source_config("customers", &customers),
        ])?;

        let preflights = preflight_delta_sources(&sources)?;

        assert_eq!(preflights.len(), 2);
        assert_eq!(preflights[0].protocol.source_name, "orders");
        assert_eq!(preflights[0].protocol.min_reader_version, 1);
        assert_eq!(preflights[1].protocol.source_name, "customers");
        assert_eq!(
            preflights[1].protocol.writer_features,
            vec!["inCommitTimestamp"]
        );

        Ok(())
    }

    #[test]
    fn multi_source_preflight_reports_failing_source() -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("orders", LEGACY_PROTOCOL_JSON)?;
        let customers = DeltaLogTable::new("customers", DELETION_VECTOR_PROTOCOL_JSON)?;
        let sources = load_delta_sources([
            source_config("orders", &orders),
            source_config("customers", &customers),
        ])?;

        let result = preflight_delta_sources(&sources);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaProtocolCompatibility {
                source_name,
                reason,
                ..
            }) if source_name == "customers" && reason.contains("deletionVectors")
        ));

        Ok(())
    }

    #[test]
    fn protocol_policy_rejects_future_reader_versions() {
        let report = DeltaProtocolReport {
            source_name: "orders".to_owned(),
            table_uri: "s3://bucket/table/".to_owned(),
            snapshot_version: 42,
            min_reader_version: 4,
            min_writer_version: 7,
            reader_features: Vec::new(),
            writer_features: Vec::new(),
        };

        let result = super::ensure_protocol_supported(&report);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaProtocolCompatibility {
                reason,
                ..
            }) if reason.contains("minReaderVersion 4")
        ));
    }

    #[test]
    fn protocol_policy_allows_reader_version_three_without_reader_features() {
        let report = DeltaProtocolReport {
            source_name: "orders".to_owned(),
            table_uri: "s3://bucket/table/".to_owned(),
            snapshot_version: 42,
            min_reader_version: super::TABLE_FEATURES_MIN_READER_VERSION,
            min_writer_version: 7,
            reader_features: Vec::new(),
            writer_features: vec!["inCommitTimestamp".to_owned()],
        };

        assert!(super::ensure_protocol_supported(&report).is_ok());
    }

    #[test]
    fn protocol_policy_reports_first_unsupported_reader_feature() {
        let report = DeltaProtocolReport {
            source_name: "orders".to_owned(),
            table_uri: "s3://bucket/table/".to_owned(),
            snapshot_version: 42,
            min_reader_version: super::TABLE_FEATURES_MIN_READER_VERSION,
            min_writer_version: 7,
            reader_features: vec!["deletionVectors".to_owned(), "madeUpFeature".to_owned()],
            writer_features: vec!["deletionVectors".to_owned(), "madeUpFeature".to_owned()],
        };

        let result = super::ensure_protocol_supported(&report);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaProtocolCompatibility {
                reason,
                ..
            }) if reason.contains("deletionVectors") && !reason.contains("madeUpFeature")
        ));
    }

    #[test]
    fn compatibility_error_display_redacts_uri_credentials() {
        let error = DeltaFunnelError::DeltaProtocolCompatibility {
            source_name: "orders".to_owned(),
            table_uri: "s3://user:password@example.com/table?token=secret".to_owned(),
            snapshot_version: 9,
            reason: "unsupported Delta reader feature `madeUpFeature`".to_owned(),
        };

        let display = error.to_string();

        assert!(display.contains("orders"));
        assert!(display.contains("madeUpFeature"));
        assert!(!display.contains("user"));
        assert!(!display.contains("password"));
        assert!(!display.contains("token"));
        assert!(!display.contains("secret"));
    }
}
