//! Minimal PyO3 module for the `deltafunnel` Python package.

use pyo3::prelude::*;

mod exception;
mod json;
mod session;

#[pymodule]
fn deltafunnel(module: &Bound<'_, PyModule>) -> PyResult<()> {
    exception::add_exception(module)?;
    session::add_session(module)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
