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

#[cfg(test)]
mod tests {
    const README: &str = include_str!("../../../README.md");

    #[test]
    fn root_readme_documents_python_api_without_unsafe_examples() {
        assert!(README.contains("PyO3 native extension module"));
        assert!(README.contains("write_to_mssql("));
        assert!(README.contains("write_all(outputs, dry_run=True)"));
        assert!(README.contains("options={\"cache_mode\": \"disabled\"}"));
        assert!(README.contains("cargo xtask python-package-check"));
        assert!(README.contains("docs/failure-reports-and-tracing.md"));
        assert!(README.contains("does not include persistent `cache`, `persist`,"));
        assert!(README.contains("native TDS driver"));
        assert!(!README.contains("dry_run_to_mssql"));
        assert!(!README.contains("dry_run_all"));
        assert!(!README.contains("ODBC Driver"));
        assert!(!README.contains("password=secret"));
        assert!(!README.contains("token="));
        assert!(!README.contains("AWS_SECRET_ACCESS_KEY"));
    }
}
