//! DataFusion session options for query execution.
//!
//! These options are query-engine settings. They are intentionally separate
//! from provider file-read scheduling and from future MSSQL/TDS writer options.

use datafusion::prelude::{SessionConfig, SessionContext};

use crate::DeltaFunnelError;
use crate::pipeline::{BatchPipelinePhase, validate_nonzero_usize_option};

/// Query execution options applied before DataFusion planning and execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryOptions {
    /// Optional DataFusion execution partition target.
    ///
    /// This controls query and scan parallelism. It does not set per-batch row
    /// counts and should be reported separately from `output_batch_size`.
    pub target_partitions: Option<usize>,

    /// Optional DataFusion execution batch size.
    ///
    /// This is an upstream query-engine batch-size request. Arbitrary
    /// DataFusion operators may still produce smaller or differently shaped
    /// final output batches.
    pub output_batch_size: Option<usize>,
}

impl QueryOptions {
    /// Validates query options before any source reads or target side effects.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaFunnelError::BatchPipeline`] when a configured numeric
    /// option is zero.
    pub fn validate(&self) -> Result<(), DeltaFunnelError> {
        if let Some(target_partitions) = self.target_partitions {
            validate_nonzero_usize_option(
                BatchPipelinePhase::Configuration,
                "target_partitions",
                target_partitions,
            )?;
        }

        if let Some(output_batch_size) = self.output_batch_size {
            validate_nonzero_usize_option(
                BatchPipelinePhase::Configuration,
                "output_batch_size",
                output_batch_size,
            )?;
        }

        Ok(())
    }

    /// Applies validated query options to a DataFusion session config.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaFunnelError::BatchPipeline`] when validation fails.
    pub fn apply_to_session_config(
        &self,
        mut config: SessionConfig,
    ) -> Result<SessionConfig, DeltaFunnelError> {
        self.validate()?;

        if let Some(target_partitions) = self.target_partitions {
            config = config.with_target_partitions(target_partitions);
        }
        if let Some(output_batch_size) = self.output_batch_size {
            config = config.with_batch_size(output_batch_size);
        }

        Ok(config)
    }
}

/// Builds a DataFusion session config from DeltaFunnel query options.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::BatchPipeline`] when validation fails.
pub fn datafusion_session_config(options: QueryOptions) -> Result<SessionConfig, DeltaFunnelError> {
    options.apply_to_session_config(SessionConfig::new())
}

/// Builds a DataFusion session context from DeltaFunnel query options.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::BatchPipeline`] when validation fails.
pub fn datafusion_session_context(
    options: QueryOptions,
) -> Result<SessionContext, DeltaFunnelError> {
    Ok(SessionContext::new_with_config(datafusion_session_config(
        options,
    )?))
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionConfig;

    use super::{QueryOptions, datafusion_session_config, datafusion_session_context};
    use crate::{BatchPipelinePhase, DeltaFunnelError};

    #[test]
    fn omitted_query_options_preserve_datafusion_defaults() -> Result<(), Box<dyn std::error::Error>>
    {
        let default_config = SessionConfig::new();
        let configured = datafusion_session_config(QueryOptions::default())?;

        assert_eq!(configured.batch_size(), default_config.batch_size());
        assert_eq!(
            configured.target_partitions(),
            default_config.target_partitions()
        );

        Ok(())
    }

    #[test]
    fn output_batch_size_is_applied_to_session_config() -> Result<(), Box<dyn std::error::Error>> {
        let configured = datafusion_session_config(QueryOptions {
            target_partitions: None,
            output_batch_size: Some(17),
        })?;

        assert_eq!(configured.batch_size(), 17);
        assert_eq!(
            configured.target_partitions(),
            SessionConfig::new().target_partitions()
        );

        Ok(())
    }

    #[test]
    fn target_partitions_is_applied_to_session_config() -> Result<(), Box<dyn std::error::Error>> {
        let configured = datafusion_session_config(QueryOptions {
            target_partitions: Some(3),
            output_batch_size: None,
        })?;

        assert_eq!(configured.target_partitions(), 3);
        assert_eq!(configured.batch_size(), SessionConfig::new().batch_size());

        Ok(())
    }

    #[test]
    fn target_partitions_and_output_batch_size_are_independent()
    -> Result<(), Box<dyn std::error::Error>> {
        let configured = datafusion_session_config(QueryOptions {
            target_partitions: Some(4),
            output_batch_size: Some(11),
        })?;

        assert_eq!(configured.target_partitions(), 4);
        assert_eq!(configured.batch_size(), 11);

        Ok(())
    }

    #[test]
    fn session_context_uses_configured_query_options() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = datafusion_session_context(QueryOptions {
            target_partitions: Some(2),
            output_batch_size: Some(9),
        })?;

        assert_eq!(ctx.state().config().target_partitions(), 2);
        assert_eq!(ctx.state().config().batch_size(), 9);

        Ok(())
    }

    #[test]
    fn validation_rejects_zero_output_batch_size() {
        let error = QueryOptions {
            target_partitions: None,
            output_batch_size: Some(0),
        }
        .validate();

        assert!(matches!(
            error,
            Err(DeltaFunnelError::BatchPipeline {
                phase: BatchPipelinePhase::Configuration,
                option: "output_batch_size",
                ..
            })
        ));
    }

    #[test]
    fn validation_rejects_zero_target_partitions() {
        let error = QueryOptions {
            target_partitions: Some(0),
            output_batch_size: None,
        }
        .validate();

        assert!(matches!(
            error,
            Err(DeltaFunnelError::BatchPipeline {
                phase: BatchPipelinePhase::Configuration,
                option: "target_partitions",
                ..
            })
        ));
    }
}
