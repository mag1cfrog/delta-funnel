//! Rust backing model for lazy query-load orchestration.
//!
//! This module owns the high-level session and request data shapes that will
//! later back the Python `Session` and `Table` API. Metadata report helpers do
//! not contact SQL Server or execute rows unless a write path explicitly does so.

mod dry_run_report;
mod errors;
mod handles;
mod mssql_output;
mod options;
mod registry;
mod source_report;
mod streams;
#[cfg(test)]
mod test_support;
mod write_all;

use std::fmt;

use datafusion::prelude::SessionContext;

use crate::{DeltaFunnelError, datafusion_session_context};

pub use handles::{
    LazyTable, LazyTableKind, MssqlOutputTarget, OutputWritePlan, PlannedMssqlOutput, RunMode,
};
pub use options::SessionOptions;
pub use registry::{RegisteredDerivedTable, RegisteredSessionSource};
pub use source_report::{DeltaProviderSchedulingReport, DeltaSourceReport, SourceUsageStatus};
pub use write_all::{
    WriteAllCacheAliasReport, WriteAllCacheAliasStatus, WriteAllCacheCandidateSkip,
    WriteAllCacheCandidateSkipReason, WriteAllCacheMode, WriteAllCacheReport,
    WriteAllNoCacheReason, WriteAllOptions, WriteAllReport,
};

pub use dry_run_report::{
    MssqlDryRunOutputFieldReport, MssqlDryRunOutputReport, MssqlDryRunSqlIdentityReport,
    MssqlDryRunSqlIdentityState, MssqlDryRunWorkflowReport,
};
#[cfg(test)]
pub(crate) use mssql_output::OrchestratorMssqlOutputWriter;
use registry::PendingDerivedTable;

/// Rust backing session for lazy query-load workflows.
pub struct DeltaFunnelSession {
    options: SessionOptions,
    context: SessionContext,
    next_table_id: u64,
    sources: Vec<RegisteredSessionSource>,
    derived_tables: Vec<RegisteredDerivedTable>,
    pending_derived_tables: Vec<PendingDerivedTable>,
}

impl DeltaFunnelSession {
    /// Builds a new session with validated local options.
    ///
    /// # Errors
    ///
    /// Returns the first local option validation failure before any source
    /// loading, SQL planning, SQL Server connection, or row execution.
    pub fn new(options: SessionOptions) -> Result<Self, DeltaFunnelError> {
        options.validate()?;
        let context = datafusion_session_context(options.query_options())?;
        Ok(Self {
            options,
            context,
            next_table_id: 0,
            sources: Vec::new(),
            derived_tables: Vec::new(),
            pending_derived_tables: Vec::new(),
        })
    }

    /// Returns the validated session options.
    #[must_use]
    pub const fn options(&self) -> &SessionOptions {
        &self.options
    }

    /// Returns the DataFusion session context owned by this orchestrator.
    ///
    /// The session context is exposed so later planning steps can analyze SQL
    /// against registered session aliases. Delta source registration should
    /// still go through [`DeltaFunnelSession::delta_lake`] so the orchestrator's
    /// source reports stay aligned with the DataFusion catalog.
    #[must_use]
    pub const fn context(&self) -> &SessionContext {
        &self.context
    }

    /// Returns the next unassigned session-local lazy table id.
    #[must_use]
    pub const fn next_table_id(&self) -> u64 {
        self.next_table_id
    }
}

impl fmt::Debug for DeltaFunnelSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaFunnelSession")
            .field("options", &self.options)
            .field("sources", &self.sources)
            .field("derived_tables", &self.derived_tables)
            .field("next_table_id", &self.next_table_id)
            .finish_non_exhaustive()
    }
}
