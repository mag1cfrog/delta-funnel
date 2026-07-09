//! Python lazy table wrapper.

use pyo3::prelude::*;

use crate::output::PyMssqlOutputSpec;
use crate::session::PySession;

/// Rendered preview of a Delta Funnel table.
#[pyclass(name = "Preview", module = "deltafunnel")]
pub(crate) struct PyPreview {
    text: String,
    html: String,
}

impl PyPreview {
    fn new(preview: delta_funnel::TablePreview) -> Self {
        Self {
            text: preview.text().to_owned(),
            html: preview.html().to_owned(),
        }
    }
}

#[pymethods]
impl PyPreview {
    #[getter]
    fn text(&self) -> &str {
        &self.text
    }

    #[getter]
    fn html(&self) -> &str {
        &self.html
    }

    fn __str__(&self) -> &str {
        &self.text
    }

    fn __repr__(&self) -> &str {
        &self.text
    }

    fn _repr_html_(&self) -> &str {
        &self.html
    }
}

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

    /// Returns a bounded rendered preview of this lazy table.
    #[pyo3(signature = (limit=20))]
    fn preview(&self, py: Python<'_>, limit: usize) -> PyResult<PyPreview> {
        let text = self
            .session
            .borrow(py)
            .preview_table(py, &self.inner, limit)?;
        Ok(PyPreview::new(text))
    }

    /// Prints a bounded preview of this lazy table to Python stdout.
    #[pyo3(signature = (limit=20))]
    fn show(&self, py: Python<'_>, limit: usize) -> PyResult<()> {
        let preview = self.preview(py, limit)?;
        py.import("builtins")?
            .getattr("print")?
            .call1((preview.text.as_str(),))?;
        Ok(())
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
    module.add_class::<PyPreview>()?;
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
            let preview_type = module.getattr("Preview")?;
            assert_eq!(
                preview_type.getattr("__name__")?.extract::<String>()?,
                "Preview"
            );

            Ok(())
        })
    }

    #[test]
    fn table_preview_returns_limited_preview_object() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1(
                "table_from_sql",
                ("select 1 as id union all select 2 as id",),
            )?;

            let preview = table.call_method1("preview", (1,))?;
            let text = preview.getattr("text")?.extract::<String>()?;
            let html = preview.getattr("html")?.extract::<String>()?;

            assert_eq!(
                preview
                    .get_type()
                    .getattr("__name__")?
                    .extract::<String>()?,
                "Preview"
            );
            assert_eq!(preview.str()?.extract::<String>()?, text);
            assert_eq!(
                preview.call_method0("_repr_html_")?.extract::<String>()?,
                html
            );
            assert!(html.contains("class=\"deltafunnel-preview\""));
            assert!(html.contains("<td class=\"df-num\">1</td>"));
            assert!(!html.contains("<td class=\"df-num\">2</td>"));
            assert!(
                html.contains("<th class=\"df-num\"><span>id</span><br><span class=\"df-type\">")
            );
            assert!(text.contains("| id |"));
            assert!(text.lines().any(|line| line.contains("| 1  |")));
            assert!(!text.lines().any(|line| line.contains("| 2  |")));
            Ok(())
        })
    }

    #[test]
    fn table_show_prints_preview_to_python_stdout() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1("table_from_sql", ("select 'west' as region",))?;
            let sys = py.import("sys")?;
            let io = py.import("io")?;
            let capture = io.call_method0("StringIO")?;
            let old_stdout = sys.getattr("stdout")?;

            sys.setattr("stdout", &capture)?;
            let show_result = table.call_method1("show", (20,));
            sys.setattr("stdout", old_stdout)?;
            show_result?;

            let output = capture.call_method0("getvalue")?.extract::<String>()?;
            assert!(output.contains("| region |"));
            assert!(output.lines().any(|line| line.contains("| west   |")));
            Ok(())
        })
    }
}
