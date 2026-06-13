//! Bounded scheduling options for Delta scan execution.

use crate::DeltaFunnelError;

/// Provider-local limits for Delta scan read scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeltaProviderExecutionOptions {
    /// Maximum provider file reads admitted across one physical scan.
    pub(crate) read_parallelism: usize,
    /// Maximum provider file reads admitted by one execution partition.
    pub(crate) max_partition_read_parallelism: usize,
    /// Maximum provider files that may be active or queued for one scan.
    pub(crate) max_in_flight_files: usize,
}

impl Default for DeltaProviderExecutionOptions {
    fn default() -> Self {
        Self {
            read_parallelism: 1,
            max_partition_read_parallelism: 1,
            max_in_flight_files: 1,
        }
    }
}

impl DeltaProviderExecutionOptions {
    #[allow(dead_code)]
    pub(crate) fn try_new(
        read_parallelism: usize,
        max_partition_read_parallelism: usize,
        max_in_flight_files: usize,
    ) -> Result<Self, DeltaFunnelError> {
        let options = Self {
            read_parallelism,
            max_partition_read_parallelism,
            max_in_flight_files,
        };
        options.validate()?;
        Ok(options)
    }

    pub(crate) fn validate(&self) -> Result<(), DeltaFunnelError> {
        validate_positive("read_parallelism", self.read_parallelism)?;
        validate_positive(
            "max_partition_read_parallelism",
            self.max_partition_read_parallelism,
        )?;
        validate_positive("max_in_flight_files", self.max_in_flight_files)?;
        Ok(())
    }
}

fn validate_positive(name: &'static str, value: usize) -> Result<(), DeltaFunnelError> {
    if value == 0 {
        return Err(DeltaFunnelError::Config {
            message: format!("{name} must be greater than zero"),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::DeltaProviderExecutionOptions;

    #[test]
    fn default_execution_options_are_valid() -> Result<(), Box<dyn std::error::Error>> {
        DeltaProviderExecutionOptions::default().validate()?;

        Ok(())
    }

    #[test]
    fn execution_options_reject_zero_read_parallelism() {
        let error = DeltaProviderExecutionOptions::try_new(0, 1, 1)
            .expect_err("zero read_parallelism must fail");

        assert_eq!(
            error.to_string(),
            "configuration error: read_parallelism must be greater than zero"
        );
    }

    #[test]
    fn execution_options_reject_zero_partition_read_parallelism() {
        let error = DeltaProviderExecutionOptions::try_new(1, 0, 1)
            .expect_err("zero max_partition_read_parallelism must fail");

        assert_eq!(
            error.to_string(),
            "configuration error: max_partition_read_parallelism must be greater than zero"
        );
    }

    #[test]
    fn execution_options_reject_zero_in_flight_files() {
        let error = DeltaProviderExecutionOptions::try_new(1, 1, 0)
            .expect_err("zero max_in_flight_files must fail");

        assert_eq!(
            error.to_string(),
            "configuration error: max_in_flight_files must be greater than zero"
        );
    }
}
