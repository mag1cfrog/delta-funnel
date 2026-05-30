//! Shared error pattern for DeltaFunnel.

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
}
