use crate::{MssqlOutputWriteStatus, MssqlWorkflowWriteReport};

use super::{DeltaSourceReport, WriteAllCacheReport};

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

/// Report for one `write_all` call that reached the sequential workflow.
///
/// Planning and cache setup failures are returned as errors before this report
/// exists. Once the workflow starts, output write failures and dependent-output
/// stream setup failures are represented in the wrapped workflow report while
/// cache metadata remains available through [`WriteAllReport::cache`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAllReport {
    workflow: MssqlWorkflowWriteReport,
    cache: WriteAllCacheReport,
    sources: Vec<DeltaSourceReport>,
}

impl WriteAllReport {
    pub(super) fn new(
        workflow: MssqlWorkflowWriteReport,
        cache: WriteAllCacheReport,
        sources: Vec<DeltaSourceReport>,
    ) -> Self {
        Self {
            workflow,
            cache,
            sources,
        }
    }

    /// Returns the lower-level SQL Server workflow report.
    #[must_use]
    pub const fn workflow(&self) -> &MssqlWorkflowWriteReport {
        &self.workflow
    }

    /// Returns cache planning, selection, and lifecycle metadata for this call.
    #[must_use]
    pub const fn cache(&self) -> &WriteAllCacheReport {
        &self.cache
    }

    /// Returns Delta source reports in session registration order.
    #[must_use]
    pub fn sources(&self) -> &[DeltaSourceReport] {
        &self.sources
    }

    /// Returns the number of selected outputs represented by this report.
    #[must_use]
    pub fn len(&self) -> usize {
        self.workflow.len()
    }

    /// Returns whether this report contains no selected outputs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.workflow.is_empty()
    }

    /// Returns per-output SQL Server workflow statuses in caller-provided order.
    #[must_use]
    pub fn outputs(&self) -> &[MssqlOutputWriteStatus] {
        self.workflow.outputs()
    }

    /// Returns whether every selected output completed successfully.
    #[must_use]
    pub fn all_succeeded(&self) -> bool {
        self.workflow.all_succeeded()
    }

    /// Returns the number of outputs that completed successfully.
    #[must_use]
    pub fn succeeded_count(&self) -> usize {
        self.workflow.succeeded_count()
    }

    /// Returns the number of outputs that failed.
    #[must_use]
    pub fn failed_count(&self) -> usize {
        self.workflow.failed_count()
    }

    /// Returns the number of outputs skipped after a previous output failed.
    #[must_use]
    pub fn skipped_count(&self) -> usize {
        self.workflow.skipped_count()
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
