//! Table-format integrations.

mod delta;

pub use delta::{DeltaSourceConfig, PlannedDeltaSource, load_delta_source, load_delta_sources};
