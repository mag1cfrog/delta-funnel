//! Delta DataFusion scan execution.

pub(crate) mod environment;
pub(crate) mod file_reader;
mod planning_exec;

pub(crate) use planning_exec::DeltaScanPlanningExec;
