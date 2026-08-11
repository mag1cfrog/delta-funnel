//! Query engine integration.

pub(crate) mod datafusion;

pub use datafusion::{
    QueryOptions, RegisteredDeltaSource, RegisteredDeltaSources, datafusion_query_output_stream,
    datafusion_session_config, datafusion_session_context, register_delta_sources,
    register_delta_sources_with_scan_execution_options,
};
