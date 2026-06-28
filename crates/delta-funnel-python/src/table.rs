//! Python lazy table wrapper.

use pyo3::prelude::*;

use crate::exception::delta_funnel_py_error;
use crate::output::PyMssqlOutputSpec;
use crate::session::PySession;

#[pyclass(name = "Table", module = "deltafunnel")]
pub(crate) struct PyTable {
    session: Py<PySession>,
    pub(crate) inner: delta_funnel::LazyTable,
}

impl PyTable {
    pub(crate) const fn from_inner(session: Py<PySession>, inner: delta_funnel::LazyTable) -> Self {
        Self { session, inner }
    }
}

#[pymethods]
impl PyTable {
    /// Registers this pending SQL-derived table under `name` and returns a `Table`.
    fn alias(&self, py: Python<'_>, name: String) -> PyResult<Self> {
        let table = self
            .session
            .borrow_mut(py)
            .register_table_alias(py, name, &self.inner)?;
        Ok(Self::from_inner(self.session.clone_ref(py), table))
    }

    /// Builds a SQL Server output spec without executing rows.
    #[pyo3(signature = (*, schema, table, load_mode, name=None, connection_string=None))]
    fn to_mssql(
        &self,
        py: Python<'_>,
        schema: String,
        table: String,
        load_mode: String,
        name: Option<String>,
        connection_string: Option<String>,
    ) -> PyResult<PyMssqlOutputSpec> {
        PyMssqlOutputSpec::new(
            py,
            self.session.clone_ref(py),
            self.inner.clone(),
            schema,
            table,
            load_mode,
            name,
            connection_string,
        )
    }

    /// Runs a SQL Server dry-run plan without executing rows.
    #[pyo3(signature = (*, schema, table, load_mode, dry_run=None, name=None, connection_string=None))]
    #[allow(clippy::too_many_arguments)]
    fn write_to_mssql(
        &self,
        py: Python<'_>,
        schema: String,
        table: String,
        load_mode: String,
        dry_run: Option<bool>,
        name: Option<String>,
        connection_string: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        ensure_dry_run_enabled(py, dry_run)?;
        let spec = PyMssqlOutputSpec::new(
            py,
            self.session.clone_ref(py),
            self.inner.clone(),
            schema,
            table,
            load_mode,
            name,
            connection_string,
        )?;
        self.session
            .borrow(py)
            .dry_run_to_mssql(py, &spec.write_plan(delta_funnel::RunMode::DryRun))
    }

    fn __repr__(&self) -> String {
        format!("deltafunnel.Table(name={:?})", self.inner.name())
    }
}

pub(crate) fn add_table(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyTable>()
}

fn ensure_dry_run_enabled(py: Python<'_>, dry_run: Option<bool>) -> PyResult<()> {
    if dry_run == Some(true) {
        return Ok(());
    }

    Err(config_py_error(
        py,
        "execute_mode_not_enabled",
        "write_to_mssql execute mode is not enabled yet; pass `dry_run=True`".to_owned(),
    ))
}

fn config_py_error(py: Python<'_>, kind: &'static str, message: String) -> PyErr {
    match delta_funnel_py_error(py, "config", kind, message, None) {
        Ok(error) => error,
        Err(error) => error,
    }
}

#[cfg(test)]
mod tests {
    use crate::deltafunnel;
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyModule};

    #[test]
    fn module_exports_table_type() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            let table_type = module.getattr("Table")?;
            assert_eq!(
                table_type.getattr("__name__")?.extract::<String>()?,
                "Table"
            );

            Ok(())
        })
    }
}
