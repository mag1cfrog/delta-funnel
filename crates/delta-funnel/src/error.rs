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
    #[snafu(display("invalid Delta source name `{name}`: {reason}"))]
    InvalidSourceName {
        /// Caller-provided source name.
        name: String,
        /// Sanitized reason for the validation failure.
        reason: &'static str,
    },

    /// Two configured Delta sources use the same registration name.
    #[snafu(display("duplicate Delta source name `{name}`"))]
    DuplicateSourceName {
        /// Caller-provided duplicate source name.
        name: String,
    },

    /// A required dependency contract is unavailable or incompatible.
    #[snafu(display("dependency compatibility error: {message}"))]
    DependencyCompatibility {
        /// Sanitized message suitable for logs and Python-facing errors.
        message: String,
    },
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
    fn duplicate_source_name_error_has_sanitized_display() {
        let error = DeltaFunnelError::DuplicateSourceName {
            name: "Orders".to_owned(),
        };

        assert_eq!(error.to_string(), "duplicate Delta source name `Orders`");
    }
}
