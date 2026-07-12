//! Minimal PyO3 module for the `deltafunnel` Python package.

use pyo3::prelude::*;

mod exception;
mod json;
mod logging;
mod output;
mod progress;
mod session;
mod table;

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Mutex, MutexGuard};

    static PYTHON_STATE: Mutex<()> = Mutex::new(());

    /// Prevents tests from observing another test's temporary Python globals.
    pub(crate) fn python_state() -> MutexGuard<'static, ()> {
        match PYTHON_STATE.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[pymodule]
fn deltafunnel(module: &Bound<'_, PyModule>) -> PyResult<()> {
    exception::add_exception(module)?;
    logging::add_logging(module)?;
    output::add_output(module)?;
    session::add_session(module)?;
    table::add_table(module)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
