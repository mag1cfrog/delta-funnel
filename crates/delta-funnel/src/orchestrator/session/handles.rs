use std::fmt;

use crate::{
    MssqlTargetConfig, MssqlTargetOutputPlan, PhaseTimingReport, ResolvedMssqlTarget,
    support::sanitize_text_for_display,
};

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
        DeltaFunnelError, LoadMode, MssqlConnectionConfig, MssqlTargetConfig, MssqlTargetTable,
    };

    use super::{LazyTable, LazyTableKind, MssqlOutputTarget, OutputWritePlan, RunMode};

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
