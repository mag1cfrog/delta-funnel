//! Shared error pattern for DeltaFunnel.

use crate::redaction::sanitize_uri_for_display;

use snafu::Snafu;

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
    use super::DeltaFunnelError;

    #[test]
    fn config_error_has_sanitized_display() {
        let error = DeltaFunnelError::Config {
            message: "read_parallelism must be greater than zero".to_owned(),
        };

        assert_eq!(
            error.to_string(),
            "configuration error: read_parallelism must be greater than zero"
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
    fn scan_metadata_expansion_error_has_sanitized_display() {
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
    }
}
