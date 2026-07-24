//! Delta DataFusion scan execution.

pub(crate) mod async_scheduler;
pub(crate) mod environment;
pub(crate) mod file_reader;
pub(crate) mod metered_object_store;
pub(crate) mod native_async_reader;
pub(crate) mod native_async_row_group_pruning;
mod planning_exec;
pub(crate) mod read_stats;
pub(crate) mod reader_backend;
pub(crate) mod scheduling;

pub(crate) use planning_exec::DeltaScanPlanningExec;
pub use read_stats::DeltaProviderReadStatsSnapshot;
pub use scheduling::{DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions};
