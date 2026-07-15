mod cache_alias;
mod cache_plan;
mod cache_report;
mod cached_stream;
mod request;
mod workflow;
pub(crate) use cache_plan::{
    MssqlCacheCandidateSkip, MssqlCacheCandidateSkipReason, MssqlDerivedCacheAliasPlan,
    MssqlNoCacheReason, MssqlOutputCacheDecision, MssqlOutputCachePlan,
};
pub(crate) use request::ensure_unique_write_all_output_names;

use crate::ExecutionProfileMode;

/// Cache policy for one multi-output `write_all` call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WriteAllCacheMode {
    /// Select and materialize conservative shared derived aliases when safe.
    #[default]
    Auto,
    /// Use the baseline sequential workflow without cache planning or materialization.
    Disabled,
}

/// Execution options for one multi-output `write_all` call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteAllOptions {
    cache_mode: WriteAllCacheMode,
    execution_profile_mode: ExecutionProfileMode,
}

impl WriteAllOptions {
    /// Creates default `write_all` options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cache_mode: WriteAllCacheMode::Auto,
            execution_profile_mode: ExecutionProfileMode::Disabled,
        }
    }

    /// Sets the cache policy for this `write_all` call.
    #[must_use]
    pub const fn with_cache_mode(mut self, cache_mode: WriteAllCacheMode) -> Self {
        self.cache_mode = cache_mode;
        self
    }

    /// Returns the cache policy for this `write_all` call.
    #[must_use]
    pub const fn cache_mode(&self) -> WriteAllCacheMode {
        self.cache_mode
    }

    /// Sets detailed execution profiling for each attempted output.
    #[must_use]
    pub const fn with_execution_profile_mode(mut self, mode: ExecutionProfileMode) -> Self {
        self.execution_profile_mode = mode;
        self
    }

    /// Returns the per-output execution profile mode.
    #[must_use]
    pub const fn execution_profile_mode(&self) -> ExecutionProfileMode {
        self.execution_profile_mode
    }
}

#[cfg(test)]
mod tests {
    use super::{WriteAllCacheMode, WriteAllOptions};
    use crate::ExecutionProfileMode;

    #[test]
    fn write_all_options_default_and_builders_compose() {
        let options = WriteAllOptions::default();

        assert_eq!(options.cache_mode(), WriteAllCacheMode::Auto);
        assert_eq!(
            options.execution_profile_mode(),
            ExecutionProfileMode::Disabled
        );

        for cache_mode in [WriteAllCacheMode::Auto, WriteAllCacheMode::Disabled] {
            for options in [
                WriteAllOptions::new()
                    .with_cache_mode(cache_mode)
                    .with_execution_profile_mode(ExecutionProfileMode::Detailed),
                WriteAllOptions::new()
                    .with_execution_profile_mode(ExecutionProfileMode::Detailed)
                    .with_cache_mode(cache_mode),
            ] {
                assert_eq!(options.cache_mode(), cache_mode);
                assert_eq!(
                    options.execution_profile_mode(),
                    ExecutionProfileMode::Detailed
                );
            }
        }
    }
}
