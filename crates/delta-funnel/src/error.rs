//! Shared error pattern for DeltaFunnel.

use crate::redaction::sanitize_uri_for_display;

use snafu::Snafu;

/// Phase associated with a Delta scan file read failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaScanFileReadPhase {
    /// Parsing the table URI failed.
    TableUriParsing,
    /// Converting provider file metadata into kernel file metadata failed.
    FileMetadataConversion,
    /// Resolving the Delta add-action path against the table root failed.
    FilePathResolution,
    /// Constructing the kernel object-store engine failed.
    ObjectStoreEngineConstruction,
    /// Starting a Parquet read failed.
    ParquetReadSetup,
    /// Reading a Parquet batch failed.
    ParquetBatchRead,
    /// Generating or decoding original row indexes failed.
    RowIndexGeneration,
    /// Converting kernel engine data into Arrow failed.
    ArrowConversion,
    /// Applying a physical-to-logical transform failed.
    TransformApplication,
    /// The selected backend cannot yet read this file task equivalently.
    UnsupportedReadMode,
    /// A deletion-vector read was rejected because the requested read mode is not safe yet.
    DeletionVectorPredicateRejection,
    /// Applying a deletion-vector mask failed.
    DeletionVectorMasking,
}

impl std::fmt::Display for DeltaScanFileReadPhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::TableUriParsing => "table URI parsing",
            Self::FileMetadataConversion => "file metadata conversion",
            Self::FilePathResolution => "file path resolution",
            Self::ObjectStoreEngineConstruction => "object store engine construction",
            Self::ParquetReadSetup => "Parquet read setup",
            Self::ParquetBatchRead => "Parquet batch read",
            Self::RowIndexGeneration => "row-index generation",
            Self::ArrowConversion => "Arrow conversion",
            Self::TransformApplication => "physical-to-logical transform application",
            Self::UnsupportedReadMode => "unsupported read mode",
            Self::DeletionVectorPredicateRejection => "deletion-vector predicate read rejection",
            Self::DeletionVectorMasking => "deletion-vector masking",
        })
    }
}

/// Phase associated with a Delta scan deletion-vector failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaScanDeletionVectorPhase {
    /// Parsing the table URI failed.
    TableUriParsing,
    /// Constructing the kernel object-store engine failed.
    ObjectStoreEngineConstruction,
    /// Accessing the preserved deletion-vector descriptor failed.
    DescriptorAccess,
    /// Reading or decoding the deletion-vector payload failed.
    PayloadRead,
    /// The selection vector did not match the physical file row count.
    SelectionVectorLengthMismatch,
    /// The selection vector was consumed after it was closed.
    SelectionVectorExhaustion,
}

impl std::fmt::Display for DeltaScanDeletionVectorPhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::TableUriParsing => "table URI parsing",
            Self::ObjectStoreEngineConstruction => "object store engine construction",
            Self::DescriptorAccess => "deletion-vector descriptor access",
            Self::PayloadRead => "deletion-vector payload read",
            Self::SelectionVectorLengthMismatch => "selection-vector length mismatch",
            Self::SelectionVectorExhaustion => "selection-vector exhaustion",
        })
    }
}

/// Error type used by DeltaFunnel APIs.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum DeltaFunnelError {
    /// Caller configuration is invalid.
    #[snafu(display("configuration error: {message}"))]
    Config {
        /// Sanitized message suitable for logs and Python-facing errors.
        message: String,
    },

    /// A Delta source name is not valid for registration.
    #[snafu(display(
        "invalid Delta source name `{}`: {reason}",
        sanitize_source_name_for_display(name)
    ))]
    InvalidSourceName {
        /// Caller-provided source name.
        name: String,
        /// Sanitized reason for the validation failure.
        reason: &'static str,
    },

    /// Two configured Delta sources use the same registration name.
    #[snafu(display(
        "duplicate Delta source name `{}`",
        sanitize_source_name_for_display(name)
    ))]
    DuplicateSourceName {
        /// Caller-provided duplicate source name.
        name: String,
    },

    /// A Delta source URI is not valid for snapshot loading.
    #[snafu(display("invalid Delta source URI: {reason}"))]
    InvalidSourceUri {
        /// Sanitized reason for the validation failure.
        reason: &'static str,
    },

    /// A Delta source engine could not be constructed.
    #[snafu(display("Delta source engine error: {reason}"))]
    DeltaSourceEngine {
        /// Sanitized reason for the engine construction failure.
        reason: &'static str,
    },

    /// A Delta snapshot could not be loaded.
    #[snafu(display("Delta snapshot load error: {reason}"))]
    DeltaSnapshotLoad {
        /// Sanitized reason for the snapshot load failure.
        reason: &'static str,
    },

    /// A Delta source requires an unsupported reader protocol.
    #[snafu(display(
        "Delta protocol compatibility error for source `{}` at snapshot version {snapshot_version} ({}): {reason}",
        sanitize_source_name_for_display(source_name),
        sanitize_uri_for_display(table_uri)
    ))]
    DeltaProtocolCompatibility {
        /// Caller-provided source name.
        source_name: String,
        /// Sanitized or sanitizable Delta table URI context.
        table_uri: String,
        /// Resolved Delta snapshot version.
        snapshot_version: u64,
        /// Sanitized reason for the compatibility failure.
        reason: String,
    },

    /// A Delta source schema could not be exposed to the query engine.
    #[snafu(display(
        "Delta source schema error for source `{}` ({}): {}",
        sanitize_source_name_for_display(source_name),
        sanitize_uri_for_display(table_uri),
        sanitize_reason_for_display(reason)
    ))]
    DeltaSourceSchema {
        /// Caller-provided source name.
        source_name: String,
        /// Sanitized or sanitizable Delta table URI context.
        table_uri: String,
        /// Sanitized reason for the schema failure.
        reason: String,
    },

    /// A Delta source could not be registered with DataFusion.
    #[snafu(display(
        "DataFusion registration error for source `{}` ({}): {}",
        sanitize_source_name_for_display(source_name),
        sanitize_uri_for_display(table_uri),
        sanitize_reason_for_display(reason)
    ))]
    DataFusionRegistration {
        /// Caller-provided source name.
        source_name: String,
        /// Sanitized or sanitizable Delta table URI context.
        table_uri: String,
        /// Sanitized reason for the registration failure.
        reason: String,
    },

    /// A Delta provider scan projection could not be planned.
    #[snafu(display(
        "Delta scan projection error for source `{}` ({}): {}",
        sanitize_source_name_for_display(source_name),
        sanitize_uri_for_display(table_uri),
        sanitize_reason_for_display(reason)
    ))]
    DeltaScanProjection {
        /// Caller-provided source name.
        source_name: String,
        /// Sanitized or sanitizable Delta table URI context.
        table_uri: String,
        /// Sanitized reason for the projection failure.
        reason: String,
    },

    /// A pushed Delta provider scan filter could not be planned safely.
    #[snafu(display(
        "Delta scan filter error for source `{}` ({}): {}",
        sanitize_source_name_for_display(source_name),
        sanitize_uri_for_display(table_uri),
        sanitize_reason_for_display(reason)
    ))]
    DeltaScanFilter {
        /// Caller-provided source name.
        source_name: String,
        /// Sanitized or sanitizable Delta table URI context.
        table_uri: String,
        /// Sanitized reason for the filter failure.
        reason: String,
    },

    /// A Delta kernel scan could not be constructed.
    #[snafu(display(
        "Delta scan construction error for source `{}` ({}): {}",
        sanitize_source_name_for_display(source_name),
        sanitize_uri_for_display(table_uri),
        sanitize_reason_for_display(&source.to_string())
    ))]
    DeltaScanConstruction {
        /// Caller-provided source name.
        source_name: String,
        /// Sanitized or sanitizable Delta table URI context.
        table_uri: String,
        /// Kernel scan construction failure.
        #[snafu(source(from(delta_kernel::Error, Box::new)))]
        source: Box<delta_kernel::Error>,
    },

    /// Delta scan metadata could not be expanded from kernel scan planning.
    #[snafu(display(
        "Delta scan metadata expansion error for source `{}` at snapshot version {snapshot_version} ({}): {}",
        sanitize_source_name_for_display(source_name),
        sanitize_uri_for_display(table_uri),
        sanitize_reason_for_display(&source.to_string())
    ))]
    DeltaScanMetadataExpansion {
        /// Caller-provided source name.
        source_name: String,
        /// Sanitized or sanitizable Delta table URI context.
        table_uri: String,
        /// Resolved Delta snapshot version.
        snapshot_version: u64,
        /// Kernel scan metadata expansion failure.
        #[snafu(source(from(delta_kernel::Error, Box::new)))]
        source: Box<delta_kernel::Error>,
    },

    /// Delta scan metadata could not be converted into provider file tasks.
    #[snafu(display(
        "Delta scan file task planning error for source `{}` at snapshot version {snapshot_version} ({}), file `{}`: {}",
        sanitize_source_name_for_display(source_name),
        sanitize_uri_for_display(table_uri),
        sanitize_reason_for_display(path),
        sanitize_reason_for_display(reason)
    ))]
    DeltaScanFileTaskPlanning {
        /// Caller-provided source name.
        source_name: String,
        /// Sanitized or sanitizable Delta table URI context.
        table_uri: String,
        /// Resolved Delta snapshot version.
        snapshot_version: u64,
        /// Delta add-action path associated with the task planning failure.
        path: String,
        /// Sanitized reason for the task planning failure.
        reason: String,
    },

    /// Delta scan file tasks could not be grouped into provider scan partitions.
    #[snafu(display(
        "Delta scan file task partition planning error for source `{}` at snapshot version {snapshot_version} ({}): {}",
        sanitize_source_name_for_display(source_name),
        sanitize_uri_for_display(table_uri),
        sanitize_reason_for_display(reason)
    ))]
    DeltaScanFileTaskPartitionPlanning {
        /// Caller-provided source name.
        source_name: String,
        /// Sanitized or sanitizable Delta table URI context.
        table_uri: String,
        /// Resolved Delta snapshot version.
        snapshot_version: u64,
        /// Sanitized reason for the partition planning failure.
        reason: String,
    },

    /// A Delta scan data file could not be read through the kernel adapter.
    #[snafu(display(
        "Delta scan file read error for source `{}` at snapshot version {snapshot_version} ({}), file `{}` during {phase}: {}",
        sanitize_source_name_for_display(source_name),
        sanitize_uri_for_display(table_uri),
        sanitize_reason_for_display(path),
        sanitize_reason_for_display(&source.to_string())
    ))]
    DeltaScanFileRead {
        /// Caller-provided source name.
        source_name: String,
        /// Sanitized or sanitizable Delta table URI context.
        table_uri: String,
        /// Resolved Delta snapshot version.
        snapshot_version: u64,
        /// Delta add-action path associated with the read failure.
        path: String,
        /// Read phase associated with the failure.
        phase: DeltaScanFileReadPhase,
        /// Underlying kernel read failure.
        #[snafu(source(from(delta_kernel::Error, Box::new)))]
        source: Box<delta_kernel::Error>,
    },

    /// A Delta scan deletion vector could not be loaded or consumed safely.
    #[snafu(display(
        "Delta scan deletion-vector error for source `{}` at snapshot version {snapshot_version} ({}), file `{}` during {phase}: {}",
        sanitize_source_name_for_display(source_name),
        sanitize_uri_for_display(table_uri),
        sanitize_reason_for_display(path),
        sanitize_reason_for_display(&source.to_string())
    ))]
    DeltaScanDeletionVector {
        /// Caller-provided source name.
        source_name: String,
        /// Sanitized or sanitizable Delta table URI context.
        table_uri: String,
        /// Resolved Delta snapshot version.
        snapshot_version: u64,
        /// Delta add-action path associated with the deletion-vector failure.
        path: String,
        /// Deletion-vector phase associated with the failure.
        phase: DeltaScanDeletionVectorPhase,
        /// Underlying kernel deletion-vector failure.
        #[snafu(source(from(delta_kernel::Error, Box::new)))]
        source: Box<delta_kernel::Error>,
    },

    /// A required dependency contract is unavailable or incompatible.
    #[snafu(display("dependency compatibility error: {message}"))]
    DependencyCompatibility {
        /// Sanitized message suitable for logs and Python-facing errors.
        message: String,
    },
}

fn sanitize_source_name_for_display(name: &str) -> String {
    name.chars().flat_map(char::escape_default).collect()
}

fn sanitize_reason_for_display(reason: &str) -> String {
    reason.chars().flat_map(char::escape_default).collect()
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::DeltaFunnelError;

    #[test]
    fn config_error_has_sanitized_display() {
        let error = DeltaFunnelError::Config {
            message: "max_concurrent_file_reads_per_scan must be greater than zero".to_owned(),
        };

        assert_eq!(
            error.to_string(),
            "configuration error: max_concurrent_file_reads_per_scan must be greater than zero"
        );
    }

    #[test]
    fn dependency_error_has_sanitized_display() {
        let error = DeltaFunnelError::DependencyCompatibility {
            message: "delta_kernel API smoke test failed".to_owned(),
        };

        assert_eq!(
            error.to_string(),
            "dependency compatibility error: delta_kernel API smoke test failed"
        );
    }

    #[test]
    fn invalid_source_name_error_has_sanitized_display() {
        let error = DeltaFunnelError::InvalidSourceName {
            name: "orders.latest".to_owned(),
            reason: "source names may contain only ASCII letters, digits, and underscores",
        };

        assert_eq!(
            error.to_string(),
            "invalid Delta source name `orders.latest`: source names may contain only ASCII letters, digits, and underscores"
        );
    }

    #[test]
    fn invalid_source_name_display_escapes_control_characters() {
        let error = DeltaFunnelError::InvalidSourceName {
            name: "orders\nlatest\tname".to_owned(),
            reason: "source names may contain only ASCII letters, digits, and underscores",
        };

        let display = error.to_string();

        assert!(!display.contains('\n'));
        assert!(!display.contains('\t'));
        assert!(display.contains(r"orders\nlatest\tname"));
    }

    #[test]
    fn duplicate_source_name_error_has_sanitized_display() {
        let error = DeltaFunnelError::DuplicateSourceName {
            name: "Orders".to_owned(),
        };

        assert_eq!(error.to_string(), "duplicate Delta source name `Orders`");
    }

    #[test]
    fn invalid_source_uri_error_has_sanitized_display() {
        let error = DeltaFunnelError::InvalidSourceUri {
            reason: "table location could not be parsed or normalized",
        };

        assert_eq!(
            error.to_string(),
            "invalid Delta source URI: table location could not be parsed or normalized"
        );
    }

    #[test]
    fn source_engine_error_has_sanitized_display() {
        let error = DeltaFunnelError::DeltaSourceEngine {
            reason: "object store engine could not be constructed",
        };

        assert_eq!(
            error.to_string(),
            "Delta source engine error: object store engine could not be constructed"
        );
    }

    #[test]
    fn snapshot_load_error_has_sanitized_display() {
        let error = DeltaFunnelError::DeltaSnapshotLoad {
            reason: "snapshot could not be loaded",
        };

        assert_eq!(
            error.to_string(),
            "Delta snapshot load error: snapshot could not be loaded"
        );
    }

    #[test]
    fn protocol_compatibility_error_has_sanitized_display() {
        let error = DeltaFunnelError::DeltaProtocolCompatibility {
            source_name: "orders\nlatest".to_owned(),
            table_uri: "s3://user:password@example.com/table?token=secret".to_owned(),
            snapshot_version: 7,
            reason: "unsupported Delta reader feature `deletionVectors`".to_owned(),
        };

        let display = error.to_string();

        assert!(display.contains(r"orders\nlatest"));
        assert!(display.contains("snapshot version 7"));
        assert!(display.contains("s3://example.com/table"));
        assert!(display.contains("deletionVectors"));
        assert!(!display.contains('\n'));
        assert!(!display.contains("user"));
        assert!(!display.contains("password"));
        assert!(!display.contains("token"));
        assert!(!display.contains("secret"));
    }

    #[test]
    fn source_schema_error_has_sanitized_display() {
        let error = DeltaFunnelError::DeltaSourceSchema {
            source_name: "orders\nlatest".to_owned(),
            table_uri: "s3://user:password@example.com/table?token=secret".to_owned(),
            reason: "field\nname could not be converted".to_owned(),
        };

        let display = error.to_string();

        assert!(display.contains(r"orders\nlatest"));
        assert!(display.contains("s3://example.com/table"));
        assert!(display.contains(r"field\nname"));
        assert!(!display.contains('\n'));
        assert!(!display.contains("user"));
        assert!(!display.contains("password"));
        assert!(!display.contains("token"));
        assert!(!display.contains("secret"));
    }

    #[test]
    fn datafusion_registration_error_has_sanitized_display() {
        let error = DeltaFunnelError::DataFusionRegistration {
            source_name: "orders\nlatest".to_owned(),
            table_uri: "s3://user:password@example.com/table?token=secret".to_owned(),
            reason: "table\nalready exists".to_owned(),
        };

        let display = error.to_string();

        assert!(display.contains(r"orders\nlatest"));
        assert!(display.contains("s3://example.com/table"));
        assert!(display.contains(r"table\nalready exists"));
        assert!(!display.contains('\n'));
        assert!(!display.contains("user"));
        assert!(!display.contains("password"));
        assert!(!display.contains("token"));
        assert!(!display.contains("secret"));
    }

    #[test]
    fn scan_metadata_expansion_error_has_sanitized_display()
    -> Result<(), Box<dyn std::error::Error>> {
        let error = DeltaFunnelError::DeltaScanMetadataExpansion {
            source_name: "orders\nlatest".to_owned(),
            table_uri: "s3://user:password@example.com/table?token=secret".to_owned(),
            snapshot_version: 7,
            source: Box::new(delta_kernel::Error::generic(
                "scan\nmetadata expansion failed",
            )),
        };

        let display = error.to_string();

        assert!(display.contains(r"orders\nlatest"));
        assert!(display.contains("snapshot version 7"));
        assert!(display.contains("s3://example.com/table"));
        assert!(display.contains(r"scan\nmetadata expansion failed"));
        assert!(!display.contains('\n'));
        assert!(!display.contains("user"));
        assert!(!display.contains("password"));
        assert!(!display.contains("token"));
        assert!(!display.contains("secret"));

        let source = Error::source(&error)
            .ok_or("metadata expansion error must preserve its kernel source")?;
        assert!(
            source
                .to_string()
                .contains("scan\nmetadata expansion failed")
        );

        Ok(())
    }

    #[test]
    fn scan_file_task_planning_error_has_sanitized_display() {
        let error = DeltaFunnelError::DeltaScanFileTaskPlanning {
            source_name: "orders\nlatest".to_owned(),
            table_uri: "s3://user:password@example.com/table?token=secret".to_owned(),
            snapshot_version: 7,
            path: "part\n00000.parquet".to_owned(),
            reason: "kernel\nsize was negative".to_owned(),
        };

        let display = error.to_string();

        assert!(display.contains(r"orders\nlatest"));
        assert!(display.contains("snapshot version 7"));
        assert!(display.contains("s3://example.com/table"));
        assert!(display.contains(r"part\n00000.parquet"));
        assert!(display.contains(r"kernel\nsize was negative"));
        assert!(!display.contains('\n'));
        assert!(!display.contains("user"));
        assert!(!display.contains("password"));
        assert!(!display.contains("token"));
        assert!(!display.contains("secret"));
    }

    #[test]
    fn scan_file_task_partition_planning_error_has_sanitized_display() {
        let error = DeltaFunnelError::DeltaScanFileTaskPartitionPlanning {
            source_name: "orders\nlatest".to_owned(),
            table_uri: "s3://user:password@example.com/table?token=secret".to_owned(),
            snapshot_version: 7,
            reason: "target\npartitions was zero".to_owned(),
        };

        let display = error.to_string();

        assert!(display.contains(r"orders\nlatest"));
        assert!(display.contains("snapshot version 7"));
        assert!(display.contains("s3://example.com/table"));
        assert!(display.contains(r"target\npartitions was zero"));
        assert!(!display.contains('\n'));
        assert!(!display.contains("user"));
        assert!(!display.contains("password"));
        assert!(!display.contains("token"));
        assert!(!display.contains("secret"));
    }
}
