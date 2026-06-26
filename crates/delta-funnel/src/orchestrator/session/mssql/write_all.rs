mod cache_alias;
mod cache_plan;
mod report;
mod request;
mod workflow;
pub(crate) use cache_plan::{
    MssqlCacheCandidateSkip, MssqlCacheCandidateSkipReason, MssqlCachedOutputStreamRoute,
    MssqlDerivedCacheAliasPlan, MssqlNoCacheReason, MssqlOutputCacheDecision, MssqlOutputCachePlan,
};
pub use report::{
    WriteAllCacheAliasReport, WriteAllCacheAliasStatus, WriteAllCacheCandidateSkip,
    WriteAllCacheCandidateSkipReason, WriteAllCacheReport, WriteAllNoCacheReason, WriteAllReport,
};
pub(crate) use request::ensure_unique_write_all_output_names;

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
}

impl WriteAllOptions {
    /// Creates default `write_all` options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cache_mode: WriteAllCacheMode::Auto,
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
}

#[cfg(test)]
mod tests {
    use super::{WriteAllCacheMode, WriteAllOptions};

    #[test]
    fn write_all_options_default_to_auto_cache_mode() {
        let options = WriteAllOptions::default();

        assert_eq!(options.cache_mode(), WriteAllCacheMode::Auto);
        assert_eq!(
            WriteAllOptions::new()
                .with_cache_mode(WriteAllCacheMode::Disabled)
                .cache_mode(),
            WriteAllCacheMode::Disabled
        );
    }
}
