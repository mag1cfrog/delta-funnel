//! Delta DataFusion scan execution.

pub(crate) mod environment;
pub(crate) mod file_reader;
mod planning_exec;
pub(crate) mod read_stats;
pub(crate) mod scheduling;

pub(crate) use planning_exec::DeltaScanPlanningExec;
pub use scheduling::{DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions};
