//! Python lazy table wrapper.

use pyo3::prelude::*;

#[pyclass(name = "Table", module = "deltafunnel")]
pub(crate) struct PyTable {
    pub(crate) inner: delta_funnel::LazyTable,
}

impl PyTable {
    pub(crate) const fn from_inner(inner: delta_funnel::LazyTable) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyTable {
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
