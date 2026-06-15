//! Delta DataFusion scan execution.

pub(crate) mod async_scheduler;
pub(crate) mod environment;
pub(crate) mod file_reader;
pub(crate) mod native_async_reader;
pub(crate) mod native_async_row_group_pruning;
mod planning_exec;
pub(crate) mod read_stats;
pub(crate) mod reader_backend;
pub(crate) mod scheduling;

pub(crate) use native_async_reader::{
    DeltaNativeAsyncFileReaderConfig, validate_native_async_reader_config,
};
pub(crate) use planning_exec::DeltaScanPlanningExec;
pub use scheduling::{DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions};
