use std::fmt;

use crate::{
    ExecutionProfileMode, MssqlTargetConfig, MssqlTargetOutputPlan, PhaseTimingReport,
    QueryExecutionProfile, ReportReasonCode, ResolvedMssqlTarget,
    support::sanitize_text_for_display,
};

pub(crate) const PREVIEW_DATAFRAME_PLANNING_PHASE: &str = "preview_dataframe_planning";
pub(crate) const PREVIEW_PHYSICAL_PLANNING_PHASE: &str = "preview_physical_planning";
pub(crate) const PREVIEW_STREAM_SETUP_PHASE: &str = "preview_stream_setup";
pub(crate) const PREVIEW_EXECUTE_COLLECT_PHASE: &str = "preview_execute_collect";
pub(crate) const PREVIEW_FORMAT_TEXT_PHASE: &str = "preview_format_text";
pub(crate) const PREVIEW_FORMAT_HTML_PHASE: &str = "preview_format_html";
pub(crate) const PREVIEW_TOTAL_PHASE: &str = "preview_total";

const PREVIEW_PHASE_NAMES: [&str; 7] = [
    PREVIEW_DATAFRAME_PLANNING_PHASE,
    PREVIEW_PHYSICAL_PLANNING_PHASE,
    PREVIEW_STREAM_SETUP_PHASE,
    PREVIEW_EXECUTE_COLLECT_PHASE,
    PREVIEW_FORMAT_TEXT_PHASE,
    PREVIEW_FORMAT_HTML_PHASE,
    PREVIEW_TOTAL_PHASE,
];

/// Query-load action mode requested by a caller.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RunMode {
    /// Plan and execute the selected output workflow.
    #[default]
    Execute,
    /// Reuse planning paths without row production or SQL Server write effects.
    DryRun,
}

/// Lazy table identity owned by a query-load session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LazyTable {
    id: LazyTableId,
    kind: LazyTableKind,
    name: String,
}

impl LazyTable {
    /// Creates a placeholder lazy table handle for future registration slices.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn placeholder(id: u64, kind: LazyTableKind) -> Self {
        Self {
            id: LazyTableId(id),
            kind,
            name: format!("table_{id}"),
        }
    }

    pub(super) fn delta_source(id: u64, name: String) -> Self {
        Self {
            id: LazyTableId(id),
            kind: LazyTableKind::DeltaSource,
            name,
        }
    }

    pub(super) fn derived_sql(id: u64) -> Self {
        Self {
            id: LazyTableId(id),
            kind: LazyTableKind::DerivedSql,
            name: format!("table_{id}"),
        }
    }

    pub(super) fn with_name(&self, name: String) -> Self {
        Self {
            id: self.id,
            kind: self.kind,
            name,
        }
    }

    /// Returns the stable session-local table id.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id.0
    }

    /// Returns the lazy table kind.
    #[must_use]
    pub const fn kind(&self) -> LazyTableKind {
        self.kind
    }

    /// Returns the session-owned table name for this lazy table.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LazyTableId(u64);

/// Kind of lazy table represented by a session handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LazyTableKind {
    /// Registered Delta source table.
    DeltaSource,
    /// SQL-derived table.
    DerivedSql,
}

/// Options for a bounded lazy-table preview.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreviewOptions {
    limit: usize,
    execution_profile_mode: ExecutionProfileMode,
}

impl PreviewOptions {
    /// Creates preview options with detailed execution profiling disabled.
    #[must_use]
    pub const fn new(limit: usize) -> Self {
        Self {
            limit,
            execution_profile_mode: ExecutionProfileMode::Disabled,
        }
    }

    /// Selects whether the preview collects a detailed execution profile.
    #[must_use]
    pub const fn with_execution_profile_mode(mut self, mode: ExecutionProfileMode) -> Self {
        self.execution_profile_mode = mode;
        self
    }

    /// Returns the requested preview row limit.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }

    /// Returns the selected execution-profile mode.
    #[must_use]
    pub const fn execution_profile_mode(&self) -> ExecutionProfileMode {
        self.execution_profile_mode
    }
}

/// Rendered bounded preview of a lazy table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePreview {
    text: String,
    html: String,
    phase_timings: Vec<PhaseTimingReport>,
    execution_profile: Option<QueryExecutionProfile>,
}

impl TablePreview {
    /// Creates a rendered lazy table preview.
    #[must_use]
    pub fn new(text: String, html: String) -> Self {
        Self::from_execution(
            text,
            html,
            PREVIEW_PHASE_NAMES
                .into_iter()
                .map(|phase_name| {
                    PhaseTimingReport::unavailable(phase_name, ReportReasonCode::NotExecuted)
                })
                .collect(),
            None,
        )
    }

    pub(crate) fn from_execution(
        text: String,
        html: String,
        phase_timings: Vec<PhaseTimingReport>,
        execution_profile: Option<QueryExecutionProfile>,
    ) -> Self {
        Self {
            text,
            html,
            phase_timings,
            execution_profile,
        }
    }

    /// Returns the plain text table preview.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Returns the HTML table preview.
    #[must_use]
    pub fn html(&self) -> &str {
        &self.html
    }

    /// Returns phase timings captured for this preview operation.
    #[must_use]
    pub fn phase_timings(&self) -> &[PhaseTimingReport] {
        &self.phase_timings
    }

    /// Returns the detailed execution profile when collection was enabled.
    #[must_use]
    pub const fn execution_profile(&self) -> Option<&QueryExecutionProfile> {
        self.execution_profile.as_ref()
    }
}

/// MSSQL output target selected from a lazy table.
#[derive(Clone, PartialEq, Eq)]
pub struct MssqlOutputTarget {
    output_name: String,
    target: MssqlTargetConfig,
    run_mode: RunMode,
}

impl MssqlOutputTarget {
    /// Creates an MSSQL output target request.
    #[must_use]
    pub fn new(
        output_name: impl Into<String>,
        target: MssqlTargetConfig,
        run_mode: RunMode,
    ) -> Self {
        Self {
            output_name: output_name.into(),
            target,
            run_mode,
        }
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the SQL Server target config.
    #[must_use]
    pub const fn target(&self) -> &MssqlTargetConfig {
        &self.target
    }

    /// Returns the requested run mode.
    #[must_use]
    pub const fn run_mode(&self) -> RunMode {
        self.run_mode
    }
}

impl fmt::Debug for MssqlOutputTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlOutputTarget")
            .field("output_name", &sanitize_text_for_display(&self.output_name))
            .field("target", &self.target)
            .field("run_mode", &self.run_mode)
            .finish()
    }
}

/// Planned output write request before schema planning or execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputWritePlan {
    table: LazyTable,
    target: MssqlOutputTarget,
}

impl OutputWritePlan {
    /// Creates an output write request for a lazy table.
    #[must_use]
    pub const fn new(table: LazyTable, target: MssqlOutputTarget) -> Self {
        Self { table, target }
    }

    /// Returns the selected lazy table.
    #[must_use]
    pub const fn table(&self) -> &LazyTable {
        &self.table
    }

    /// Returns the selected MSSQL output target.
    #[must_use]
    pub const fn target(&self) -> &MssqlOutputTarget {
        &self.target
    }
}

/// Planned MSSQL output request for one selected lazy table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedMssqlOutput {
    request: OutputWritePlan,
    resolved_target: ResolvedMssqlTarget,
    output_plan: MssqlTargetOutputPlan,
    phase_timings: Vec<PhaseTimingReport>,
}

impl PlannedMssqlOutput {
    pub(super) fn new(
        request: OutputWritePlan,
        resolved_target: ResolvedMssqlTarget,
        output_plan: MssqlTargetOutputPlan,
        phase_timings: Vec<PhaseTimingReport>,
    ) -> Self {
        Self {
            request,
            resolved_target,
            output_plan,
            phase_timings,
        }
    }

    /// Returns the original lazy-table output request.
    #[must_use]
    pub const fn request(&self) -> &OutputWritePlan {
        &self.request
    }

    /// Returns the selected lazy table.
    #[must_use]
    pub const fn table(&self) -> &LazyTable {
        self.request.table()
    }

    /// Returns the selected MSSQL target request.
    #[must_use]
    pub const fn target(&self) -> &MssqlOutputTarget {
        self.request.target()
    }

    /// Returns the resolved SQL Server target, including the private connection config.
    #[must_use]
    pub const fn resolved_target(&self) -> &ResolvedMssqlTarget {
        &self.resolved_target
    }

    /// Returns the complete SQL Server target output plan.
    #[must_use]
    pub const fn output_plan(&self) -> &MssqlTargetOutputPlan {
        &self.output_plan
    }

    /// Returns durable phase timings captured while planning this output.
    #[must_use]
    pub fn phase_timings(&self) -> &[PhaseTimingReport] {
        &self.phase_timings
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DeltaFunnelError, ExecutionProfileMode, LoadMode, MssqlConnectionConfig, MssqlTargetConfig,
        MssqlTargetTable, PhaseStatus, ReportReasonCode,
    };

    use super::{
        LazyTable, LazyTableKind, MssqlOutputTarget, OutputWritePlan, PREVIEW_PHASE_NAMES,
        PreviewOptions, RunMode, TablePreview,
    };

    #[test]
    fn preview_options_default_to_disabled_profiling() {
        let default = PreviewOptions::new(20);
        let detailed = default.with_execution_profile_mode(ExecutionProfileMode::Detailed);

        assert_eq!(default.limit(), 20);
        assert_eq!(
            default.execution_profile_mode(),
            ExecutionProfileMode::Disabled
        );
        assert_eq!(detailed.limit(), 20);
        assert_eq!(
            detailed.execution_profile_mode(),
            ExecutionProfileMode::Detailed
        );
    }

    #[test]
    fn legacy_table_preview_has_unavailable_timings_and_no_profile() {
        let preview = TablePreview::new("text".to_owned(), "html".to_owned());

        assert_eq!(preview.text(), "text");
        assert_eq!(preview.html(), "html");
        assert_eq!(preview.phase_timings().len(), PREVIEW_PHASE_NAMES.len());
        for (timing, phase_name) in preview.phase_timings().iter().zip(PREVIEW_PHASE_NAMES) {
            assert_eq!(timing.phase_name(), phase_name);
            assert_eq!(
                timing.status(),
                PhaseStatus::unavailable(ReportReasonCode::NotExecuted)
            );
            assert_eq!(timing.elapsed_micros(), None);
        }
        assert_eq!(preview.execution_profile(), None);
    }

    #[test]
    fn output_request_shapes_preserve_table_target_and_run_mode() -> Result<(), DeltaFunnelError> {
        let table = LazyTable::placeholder(7, LazyTableKind::DerivedSql);
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_load_mode(LoadMode::CreateAndLoad)
            .with_connection(
                MssqlConnectionConfig::new(
                    "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
                )?
                .with_display_label("warehouse-primary"),
            );
        let target = MssqlOutputTarget::new("orders_output", target_config, RunMode::DryRun);
        let plan = OutputWritePlan::new(table.clone(), target.clone());

        assert_eq!(table.id(), 7);
        assert_eq!(table.kind(), LazyTableKind::DerivedSql);
        assert_eq!(target.output_name(), "orders_output");
        assert_eq!(target.run_mode(), RunMode::DryRun);
        assert_eq!(target.target().load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(plan.table(), &table);
        assert_eq!(plan.target(), &target);

        let debug = format!("{target:?}");
        assert!(debug.contains("orders_output"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }
}
