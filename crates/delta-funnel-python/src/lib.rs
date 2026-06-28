//! Minimal PyO3 module for the `deltafunnel` Python package.

use pyo3::prelude::*;

#[pymodule]
fn deltafunnel(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
