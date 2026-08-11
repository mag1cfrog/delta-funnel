//! Delta protocol metadata and reader compatibility policy.

use crate::{
    DeltaReaderError,
    error::UnsupportedProtocolSnafu,
    kernel::{
        DeltaKernelProtocol, KernelSnapshot, TABLE_FEATURES_READER_VERSION,
        snapshot_protocol_report,
    },
};

#[allow(dead_code)]
const SUPPORTED_READER_FEATURES: &[&str] = &[
    "timestampNtz",
    "deletionVectors",
    "columnMapping",
    "v2Checkpoint",
    "vacuumProtocolCheck",
    "typeWidening",
    "typeWidening-preview",
];

/// Protocol metadata captured from one immutable Delta snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaProtocolInfo {
    snapshot_version: u64,
    min_reader_version: i32,
    min_writer_version: i32,
    reader_features: Vec<String>,
    writer_features: Vec<String>,
}

impl DeltaProtocolInfo {
    pub(crate) fn from_snapshot(snapshot: &KernelSnapshot) -> Self {
        let version = snapshot.version();
        let DeltaKernelProtocol {
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        } = snapshot_protocol_report(snapshot);

        Self {
            snapshot_version: version,
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        }
    }

    /// Returns the loaded snapshot version.
    pub const fn snapshot_version(&self) -> u64 {
        self.snapshot_version
    }

    /// Returns the minimum Delta reader protocol version.
    pub const fn min_reader_version(&self) -> i32 {
        self.min_reader_version
    }

    /// Returns the minimum Delta writer protocol version.
    pub const fn min_writer_version(&self) -> i32 {
        self.min_writer_version
    }

    /// Returns the required reader feature names in deterministic Kernel order.
    pub fn reader_features(&self) -> &[String] {
        &self.reader_features
    }

    /// Returns the writer feature names in deterministic Kernel order.
    pub fn writer_features(&self) -> &[String] {
        &self.writer_features
    }

    /// Returns the first required reader feature unsupported by this crate.
    pub fn first_unsupported_reader_feature(&self) -> Option<&str> {
        self.reader_features
            .iter()
            .map(String::as_str)
            .find(|feature| !SUPPORTED_READER_FEATURES.contains(feature))
    }
}

#[allow(dead_code)]
pub(crate) fn validate_protocol(protocol: &DeltaProtocolInfo) -> Result<(), DeltaReaderError> {
    if !matches!(protocol.min_reader_version, 1 | 2)
        && protocol.min_reader_version != TABLE_FEATURES_READER_VERSION
    {
        return UnsupportedProtocolSnafu {
            reason: "unsupported_reader_version",
        }
        .fail();
    }

    if protocol.first_unsupported_reader_feature().is_some() {
        return UnsupportedProtocolSnafu {
            reason: "unsupported_reader_feature",
        }
        .fail();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{DeltaProtocolInfo, validate_protocol};
    use crate::{
        DeltaReaderError, DeltaReaderPhase, DeltaSnapshotSelection, DeltaStorageOptions,
        snapshot::load_delta_table_snapshot_blocking,
    };

    const METADATA_JSON: &str = r#"{"metaData":{"id":"delta-arrow-reader-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;

    struct DeltaLogTable(PathBuf);

    impl DeltaLogTable {
        fn new(name: &str, protocol_json: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let path = Path::new("target")
                .join("delta-arrow-reader-protocol-tests")
                .join(format!("{}-{name}-{nanos}", std::process::id()));
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{protocol_json}\n{METADATA_JSON}\n"),
            )?;
            Ok(Self(path))
        }

        fn load(&self) -> Result<crate::snapshot::LoadedDeltaTableSnapshot, DeltaReaderError> {
            load_delta_table_snapshot_blocking(
                &self.0.to_string_lossy(),
                &DeltaStorageOptions::new(),
                DeltaSnapshotSelection::Latest,
            )
        }
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn extracts_type_widening_parity_features_in_kernel_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new(
            "type-widening",
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz","typeWidening-preview"],"writerFeatures":["timestampNtz","typeWidening-preview"]}}"#,
        )?;
        let loaded = table.load()?;
        let protocol = loaded.protocol_info();

        assert_eq!(protocol.snapshot_version(), 0);
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert_eq!(
            protocol.reader_features(),
            ["timestampNtz", "typeWidening-preview"]
        );
        assert_eq!(
            protocol.writer_features(),
            ["timestampNtz", "typeWidening-preview"]
        );
        validate_protocol(protocol)?;
        Ok(())
    }

    #[test]
    fn loads_and_accepts_the_frozen_reader_feature_set() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new(
            "all-supported-features",
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz","deletionVectors","columnMapping","v2Checkpoint","vacuumProtocolCheck","typeWidening","typeWidening-preview"],"writerFeatures":["timestampNtz","deletionVectors","columnMapping","v2Checkpoint","vacuumProtocolCheck","typeWidening","typeWidening-preview"]}}"#,
        )?;
        let loaded = table.load()?;
        let protocol = loaded.protocol_info();

        assert_eq!(
            protocol.reader_features(),
            [
                "timestampNtz",
                "deletionVectors",
                "columnMapping",
                "v2Checkpoint",
                "vacuumProtocolCheck",
                "typeWidening",
                "typeWidening-preview",
            ]
        );
        validate_protocol(protocol)?;
        Ok(())
    }

    #[test]
    fn preserves_legacy_versions_and_treats_writer_only_features_as_diagnostic()
    -> Result<(), Box<dyn std::error::Error>> {
        for (name, protocol_json, reader_version, writer_features) in [
            (
                "legacy-column-mapping",
                r#"{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}"#,
                2,
                &[][..],
            ),
            (
                "writer-only",
                r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["inCommitTimestamp"]}}"#,
                3,
                &["inCommitTimestamp"][..],
            ),
        ] {
            let table = DeltaLogTable::new(name, protocol_json)?;
            let loaded = table.load()?;
            let protocol = loaded.protocol_info();

            assert_eq!(protocol.min_reader_version(), reader_version);
            assert!(protocol.reader_features().is_empty());
            assert_eq!(protocol.writer_features(), writer_features);
            validate_protocol(protocol)?;
        }
        Ok(())
    }

    #[test]
    fn unsupported_feature_remains_inspectable_until_validation()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new(
            "unknown-feature",
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["madeUpFeature"],"writerFeatures":["madeUpFeature"]}}"#,
        )?;
        let loaded = table.load()?;
        let protocol = loaded.protocol_info();

        assert_eq!(protocol.reader_features(), ["madeUpFeature"]);
        assert_eq!(
            protocol.first_unsupported_reader_feature(),
            Some("madeUpFeature")
        );
        let error = validate_protocol(protocol).expect_err("unknown feature must fail");
        assert!(matches!(
            error,
            DeltaReaderError::UnsupportedProtocol { .. }
        ));
        assert_eq!(error.phase(), DeltaReaderPhase::Protocol);
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=protocol error=unsupported_protocol reason=unsupported_reader_feature"
        );
        Ok(())
    }

    #[test]
    fn unsupported_version_fails_at_the_kernel_boundary_and_policy_backstop()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new(
            "future-version",
            r#"{"protocol":{"minReaderVersion":4,"minWriterVersion":7}}"#,
        )?;
        let load_error = match table.load() {
            Ok(_) => panic!("Kernel must reject the future reader version"),
            Err(error) => error,
        };
        assert!(matches!(load_error, DeltaReaderError::SnapshotLoad { .. }));
        assert_eq!(load_error.phase(), DeltaReaderPhase::Snapshot);

        let protocol = DeltaProtocolInfo {
            snapshot_version: 0,
            min_reader_version: 4,
            min_writer_version: 7,
            reader_features: Vec::new(),
            writer_features: Vec::new(),
        };
        let error = validate_protocol(&protocol).expect_err("future version must fail");
        assert_eq!(error.phase(), DeltaReaderPhase::Protocol);
        assert_eq!(error.as_str(), "unsupported_protocol");
        Ok(())
    }
}
