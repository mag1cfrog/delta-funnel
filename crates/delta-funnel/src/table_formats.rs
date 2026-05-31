//! Table-format integrations.

mod delta;
mod name;

pub(crate) use delta::delta_source_arrow_schema;
pub use delta::{
    DeltaProtocolReport, DeltaSourceConfig, PlannedDeltaSource, ProtocolPreflight,
    load_delta_source, load_delta_sources, preflight_delta_protocol, preflight_delta_sources,
};
pub(crate) use name::validate_table_source_names;
