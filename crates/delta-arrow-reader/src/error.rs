use std::fmt;

use snafu::Snafu;

/// Reader operation phase associated with an error.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaReaderPhase {
    /// Reader configuration validation.
    Configuration,
    /// Delta table URI parsing and normalization.
    TableUri,
    /// Object-store initialization.
    Storage,
    /// Delta snapshot loading.
    Snapshot,
    /// Delta protocol validation.
    Protocol,
    /// Delta-to-Arrow schema conversion.
    Schema,
    /// Delta scan planning.
    ScanPlanning,
    /// Delta data-file reading.
    DataFileRead,
    /// Delta deletion-vector handling.
    DeletionVector,
    /// Physical-to-logical data transformation.
    Transform,
    /// Reader execution.
    Execution,
    /// Optional DataFusion integration.
    DataFusion,
}

impl DeltaReaderPhase {
    /// Returns the stable snake_case phase name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Configuration => "configuration",
            Self::TableUri => "table_uri",
            Self::Storage => "storage",
            Self::Snapshot => "snapshot",
            Self::Protocol => "protocol",
            Self::Schema => "schema",
            Self::ScanPlanning => "scan_planning",
            Self::DataFileRead => "data_file_read",
            Self::DeletionVector => "deletion_vector",
            Self::Transform => "transform",
            Self::Execution => "execution",
            Self::DataFusion => "data_fusion",
        }
    }
}

/// Redacted failure returned by reader APIs.
#[non_exhaustive]
#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum DeltaReaderError {
    /// Reader configuration is invalid.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=configuration error=invalid_configuration reason={reason}"
    ))]
    InvalidConfiguration {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// The table URI is invalid.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=table_uri error=invalid_table_uri reason={reason}"
    ))]
    InvalidTableUri {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// Object-store initialization failed.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=storage error=storage_initialization reason={reason}"
    ))]
    StorageInitialization {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// Snapshot loading failed.
    #[non_exhaustive]
    #[snafu(display("delta reader error: phase=snapshot error=snapshot_load reason={reason}"))]
    SnapshotLoad {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// The table protocol is unsupported.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=protocol error=unsupported_protocol reason={reason}"
    ))]
    UnsupportedProtocol {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// Delta-to-Arrow schema conversion failed.
    #[non_exhaustive]
    #[snafu(display("delta reader error: phase=schema error=schema_conversion reason={reason}"))]
    SchemaConversion {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// A requested projection is invalid.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=scan_planning error=invalid_projection reason={reason}"
    ))]
    InvalidProjection {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// A requested predicate is unsupported.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=scan_planning error=unsupported_predicate reason={reason}"
    ))]
    UnsupportedPredicate {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// Delta scan planning failed.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=scan_planning error=scan_planning reason={reason}"
    ))]
    ScanPlanning {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// The requested reader backend is unavailable.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=execution error=unsupported_backend reason={reason}"
    ))]
    UnsupportedBackend {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// A Delta data file could not be read.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=data_file_read error=data_file_read reason={reason}"
    ))]
    DataFileRead {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// A deletion vector could not be read.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=deletion_vector error=deletion_vector_read reason={reason}"
    ))]
    DeletionVectorRead {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// A physical-to-logical transform failed.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=transform error=physical_to_logical_transform reason={reason}"
    ))]
    PhysicalToLogicalTransform {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// Reader execution was cancelled.
    #[non_exhaustive]
    #[snafu(display("delta reader error: phase=execution error=cancelled reason={reason}"))]
    Cancelled {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// Optional DataFusion integration failed.
    #[cfg(feature = "datafusion")]
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=data_fusion error=data_fusion_adapter reason={reason}"
    ))]
    DataFusionAdapter {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying DataFusion failure.
        #[snafu(source(from(datafusion::common::DataFusionError, Box::new)))]
        source: Box<datafusion::common::DataFusionError>,
    },
}

impl DeltaReaderError {
    /// Returns the stable snake_case error variant name.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidConfiguration { .. } => "invalid_configuration",
            Self::InvalidTableUri { .. } => "invalid_table_uri",
            Self::StorageInitialization { .. } => "storage_initialization",
            Self::SnapshotLoad { .. } => "snapshot_load",
            Self::UnsupportedProtocol { .. } => "unsupported_protocol",
            Self::SchemaConversion { .. } => "schema_conversion",
            Self::InvalidProjection { .. } => "invalid_projection",
            Self::UnsupportedPredicate { .. } => "unsupported_predicate",
            Self::ScanPlanning { .. } => "scan_planning",
            Self::UnsupportedBackend { .. } => "unsupported_backend",
            Self::DataFileRead { .. } => "data_file_read",
            Self::DeletionVectorRead { .. } => "deletion_vector_read",
            Self::PhysicalToLogicalTransform { .. } => "physical_to_logical_transform",
            Self::Cancelled { .. } => "cancelled",
            #[cfg(feature = "datafusion")]
            Self::DataFusionAdapter { .. } => "data_fusion_adapter",
        }
    }

    /// Returns the reader phase that failed.
    pub const fn phase(&self) -> DeltaReaderPhase {
        match self {
            Self::InvalidConfiguration { .. } => DeltaReaderPhase::Configuration,
            Self::InvalidTableUri { .. } => DeltaReaderPhase::TableUri,
            Self::StorageInitialization { .. } => DeltaReaderPhase::Storage,
            Self::SnapshotLoad { .. } => DeltaReaderPhase::Snapshot,
            Self::UnsupportedProtocol { .. } => DeltaReaderPhase::Protocol,
            Self::SchemaConversion { .. } => DeltaReaderPhase::Schema,
            Self::InvalidProjection { .. }
            | Self::UnsupportedPredicate { .. }
            | Self::ScanPlanning { .. } => DeltaReaderPhase::ScanPlanning,
            Self::UnsupportedBackend { .. } | Self::Cancelled { .. } => DeltaReaderPhase::Execution,
            Self::DataFileRead { .. } => DeltaReaderPhase::DataFileRead,
            Self::DeletionVectorRead { .. } => DeltaReaderPhase::DeletionVector,
            Self::PhysicalToLogicalTransform { .. } => DeltaReaderPhase::Transform,
            #[cfg(feature = "datafusion")]
            Self::DataFusionAdapter { .. } => DeltaReaderPhase::DataFusion,
        }
    }
}

impl fmt::Debug for DeltaReaderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error as _, io};

    use super::{DeltaReaderError, DeltaReaderPhase};

    #[test]
    fn phase_names_are_stable() {
        let cases = [
            (DeltaReaderPhase::Configuration, "configuration"),
            (DeltaReaderPhase::TableUri, "table_uri"),
            (DeltaReaderPhase::Storage, "storage"),
            (DeltaReaderPhase::Snapshot, "snapshot"),
            (DeltaReaderPhase::Protocol, "protocol"),
            (DeltaReaderPhase::Schema, "schema"),
            (DeltaReaderPhase::ScanPlanning, "scan_planning"),
            (DeltaReaderPhase::DataFileRead, "data_file_read"),
            (DeltaReaderPhase::DeletionVector, "deletion_vector"),
            (DeltaReaderPhase::Transform, "transform"),
            (DeltaReaderPhase::Execution, "execution"),
            (DeltaReaderPhase::DataFusion, "data_fusion"),
        ];

        for (phase, expected) in cases {
            assert_eq!(phase.as_str(), expected);
        }
    }

    #[test]
    fn variants_map_to_stable_accessors_and_sources() {
        let errors = [
            (
                DeltaReaderError::InvalidConfiguration {
                    reason: "invalid_configuration",
                },
                "invalid_configuration",
                DeltaReaderPhase::Configuration,
                false,
            ),
            (
                DeltaReaderError::InvalidTableUri {
                    reason: "invalid_table_uri",
                },
                "invalid_table_uri",
                DeltaReaderPhase::TableUri,
                false,
            ),
            (
                DeltaReaderError::StorageInitialization {
                    reason: "storage_initialization",
                    source: dependency_source(),
                },
                "storage_initialization",
                DeltaReaderPhase::Storage,
                true,
            ),
            (
                DeltaReaderError::SnapshotLoad {
                    reason: "snapshot_load",
                    source: dependency_source(),
                },
                "snapshot_load",
                DeltaReaderPhase::Snapshot,
                true,
            ),
            (
                DeltaReaderError::UnsupportedProtocol {
                    reason: "unsupported_protocol",
                },
                "unsupported_protocol",
                DeltaReaderPhase::Protocol,
                false,
            ),
            (
                DeltaReaderError::SchemaConversion {
                    reason: "schema_conversion",
                    source: dependency_source(),
                },
                "schema_conversion",
                DeltaReaderPhase::Schema,
                true,
            ),
            (
                DeltaReaderError::InvalidProjection {
                    reason: "invalid_projection",
                },
                "invalid_projection",
                DeltaReaderPhase::ScanPlanning,
                false,
            ),
            (
                DeltaReaderError::UnsupportedPredicate {
                    reason: "unsupported_predicate",
                },
                "unsupported_predicate",
                DeltaReaderPhase::ScanPlanning,
                false,
            ),
            (
                DeltaReaderError::ScanPlanning {
                    reason: "scan_planning",
                    source: dependency_source(),
                },
                "scan_planning",
                DeltaReaderPhase::ScanPlanning,
                true,
            ),
            (
                DeltaReaderError::UnsupportedBackend {
                    reason: "unsupported_backend",
                },
                "unsupported_backend",
                DeltaReaderPhase::Execution,
                false,
            ),
            (
                DeltaReaderError::DataFileRead {
                    reason: "data_file_read",
                    source: dependency_source(),
                },
                "data_file_read",
                DeltaReaderPhase::DataFileRead,
                true,
            ),
            (
                DeltaReaderError::DeletionVectorRead {
                    reason: "deletion_vector_read",
                    source: dependency_source(),
                },
                "deletion_vector_read",
                DeltaReaderPhase::DeletionVector,
                true,
            ),
            (
                DeltaReaderError::PhysicalToLogicalTransform {
                    reason: "physical_to_logical_transform",
                    source: dependency_source(),
                },
                "physical_to_logical_transform",
                DeltaReaderPhase::Transform,
                true,
            ),
            (
                DeltaReaderError::Cancelled {
                    reason: "cancelled",
                },
                "cancelled",
                DeltaReaderPhase::Execution,
                false,
            ),
            #[cfg(feature = "datafusion")]
            (
                DeltaReaderError::DataFusionAdapter {
                    reason: "data_fusion_adapter",
                    source: Box::new(datafusion::common::DataFusionError::Execution(
                        "sensitive dependency detail".into(),
                    )),
                },
                "data_fusion_adapter",
                DeltaReaderPhase::DataFusion,
                true,
            ),
        ];

        for (error, name, phase, has_source) in errors {
            assert_eq!(error.source().is_some(), has_source);
            assert_eq!(error.as_str(), name);
            assert_eq!(error.phase(), phase);
            let display = error.to_string();
            let debug = format!("{error:?}");
            assert!(display.contains(&format!("phase={}", phase.as_str())));
            assert!(display.contains(&format!("error={name}")));
            assert!(!display.contains("sensitive dependency detail"));
            assert!(!debug.contains("sensitive dependency detail"));
        }
    }

    fn dependency_source() -> Box<dyn std::error::Error + Send + Sync + 'static> {
        Box::new(io::Error::other("sensitive dependency detail"))
    }

    #[test]
    fn boxed_source_preserves_its_concrete_type() {
        let error = DeltaReaderError::DataFileRead {
            reason: "data_file_read",
            source: dependency_source(),
        };

        assert!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .is_some()
        );
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn datafusion_source_preserves_its_boxed_type() {
        let error = DeltaReaderError::DataFusionAdapter {
            reason: "data_fusion_adapter",
            source: Box::new(datafusion::common::DataFusionError::Execution(
                "failure".into(),
            )),
        };

        assert!(
            error
                .source()
                .and_then(|source| {
                    source.downcast_ref::<Box<datafusion::common::DataFusionError>>()
                })
                .is_some()
        );
    }
}
