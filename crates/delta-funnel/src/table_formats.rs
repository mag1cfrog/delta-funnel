//! Table-format integrations.

mod delta;
mod name;

pub(crate) use delta::{
    DeltaKernelPredicate, KernelScanFileMetadata, KernelScanMetadataExpansion, ProjectedDeltaScan,
    build_projected_predicated_delta_scan, build_projected_predicated_stats_delta_scan,
    datafusion_expr_to_kernel_predicate, delta_source_arrow_schema,
};
pub use delta::{
    DeltaProtocolReport, DeltaSourceConfig, PlannedDeltaSource, ProtocolPreflight,
    load_delta_source, load_delta_sources, preflight_delta_protocol, preflight_delta_sources,
};
pub(crate) use name::validate_table_source_names;
