//! Minimal PyO3 module for the `deltafunnel` Python package.

use pyo3::prelude::*;

mod exception;
mod json;
mod output;
mod session;
mod table;

#[pymodule]
fn deltafunnel(module: &Bound<'_, PyModule>) -> PyResult<()> {
    exception::add_exception(module)?;
    output::add_output(module)?;
    session::add_session(module)?;
    table::add_table(module)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
