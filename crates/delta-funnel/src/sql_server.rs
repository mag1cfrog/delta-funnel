//! SQL Server target planning primitives.
//!
//! This module owns DeltaFunnel-side configuration and reporting shapes around
//! SQL Server targets. Schema mapping, identifier quoting, DDL rendering, and
//! Arrow-to-TDS writing remain owned by `arrow-tiberius`.

mod target;

pub use target::{
    LoadMode, MssqlConnectionConfig, MssqlConnectionSource, MssqlConnectionSummary,
    MssqlTargetConfig, MssqlTargetResolutionContext, MssqlTargetSummary, MssqlTargetTable,
    ResolvedMssqlTarget,
};
