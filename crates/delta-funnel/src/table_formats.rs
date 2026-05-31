//! Table-format integrations.

mod delta;
mod name;

pub use delta::{
    DeltaProtocolReport, DeltaSourceConfig, PlannedDeltaSource, ProtocolPreflight,
    load_delta_source, load_delta_sources, preflight_delta_protocol, preflight_delta_sources,
};
pub(crate) use delta::{ProjectedDeltaScan, build_projected_delta_scan, delta_source_arrow_schema};
pub(crate) use name::validate_table_source_names;
