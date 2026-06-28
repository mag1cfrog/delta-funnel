//! Python lazy table wrapper.

use pyo3::prelude::*;

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

    fn __repr__(&self) -> String {
        format!("deltafunnel.Table(name={:?})", self.inner.name())
    }
}

pub(crate) fn add_table(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyTable>()
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
