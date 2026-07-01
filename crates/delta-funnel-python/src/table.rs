//! Python lazy table wrapper.

use pyo3::prelude::*;

use crate::output::PyMssqlOutputSpec;
use crate::session::PySession;

/// Lazy Delta Funnel table.
///
/// A `Table` can be aliased for later SQL, converted to a `MssqlOutputSpec`,
/// or written directly to SQL Server.
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
    ///
    /// The default output name is the target `table`; pass `name` to override
    /// the report/output identity.
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

    /// Writes this table to SQL Server, or runs a dry-run plan when requested.
    ///
    /// Pass `dry_run=True` to plan without writing. Returns a plain Python
    /// `dict` report.
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
        if dry_run == Some(true) {
            return self
                .session
                .borrow(py)
                .dry_run_to_mssql(py, &spec.write_plan(delta_funnel::RunMode::DryRun));
        }

        self.session
            .borrow(py)
            .write_to_mssql(py, &spec.write_plan(delta_funnel::RunMode::Execute))
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let kind = match self.inner.kind() {
            delta_funnel::LazyTableKind::DeltaSource => "delta_source",
            delta_funnel::LazyTableKind::DerivedSql => "derived_sql",
        };
        if let Some((source_uri, snapshot_version)) =
            self.session.borrow(py).source_repr_details(&self.inner)
        {
            return format!(
                "deltafunnel.Table(id={}, kind={kind:?}, name={:?}, source_uri={source_uri:?}, snapshot_version={snapshot_version})",
                self.inner.id(),
                self.inner.name()
            );
        }
        format!(
            "deltafunnel.Table(id={}, kind={kind:?}, name={:?})",
            self.inner.id(),
            self.inner.name()
        )
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
