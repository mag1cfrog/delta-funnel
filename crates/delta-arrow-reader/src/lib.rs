//! Read-only Delta Lake to Arrow support.

/// The crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the crate version.
pub const fn version() -> &'static str {
    VERSION
}
