//! Query engine integration.

pub(crate) mod datafusion;

pub use datafusion::{
    DeltaTableProviderConfig, RegisteredDeltaSource, RegisteredDeltaSources, register_delta_sources,
};
