//! Shared error pattern for DeltaFunnel.

use snafu::Snafu;

/// Result type used by DeltaFunnel APIs.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Stable category for the currently defined error variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCategory {
    /// Invalid caller configuration or unsupported options.
    Config,
    /// Compile-time or runtime dependency compatibility failure.
    DependencyCompatibility,
}

/// Error type used by the current foundation layer.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// Caller configuration is invalid.
    #[snafu(display("configuration error: {message}"))]
    Config {
        /// Sanitized message suitable for logs and Python-facing errors.
        message: String,
    },

    /// A required dependency contract is unavailable or incompatible.
    #[snafu(display("dependency compatibility error: {message}"))]
    DependencyCompatibility {
        /// Sanitized message suitable for logs and Python-facing errors.
        message: String,
    },
}

impl Error {
    /// Returns the stable error category for this error.
    #[must_use]
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Config { .. } => ErrorCategory::Config,
            Self::DependencyCompatibility { .. } => ErrorCategory::DependencyCompatibility,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, ErrorCategory, Result};

    #[test]
    fn config_error_has_sanitized_display_and_category() {
        let error = Error::Config {
            message: "read_parallelism must be greater than zero".to_owned(),
        };

        assert_eq!(
            error.to_string(),
            "configuration error: read_parallelism must be greater than zero"
        );
        assert_eq!(error.category(), ErrorCategory::Config);
    }

    #[test]
    fn dependency_error_has_sanitized_display_and_category() {
        let error = Error::DependencyCompatibility {
            message: "delta_kernel API smoke test failed".to_owned(),
        };

        assert_eq!(
            error.to_string(),
            "dependency compatibility error: delta_kernel API smoke test failed"
        );
        assert_eq!(error.category(), ErrorCategory::DependencyCompatibility);
    }

    #[test]
    fn result_alias_defaults_to_foundation_error() {
        fn validate() -> Result<()> {
            Ok(())
        }

        assert!(validate().is_ok());
    }
}
