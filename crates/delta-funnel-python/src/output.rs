//! Python MSSQL output specification wrapper.

use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

use crate::exception::{delta_funnel_error_to_py, delta_funnel_py_error};
use crate::session::PySession;

#[pyclass(name = "MssqlOutputSpec", module = "deltafunnel")]
pub(crate) struct PyMssqlOutputSpec {
    session: Py<PySession>,
    table: delta_funnel::LazyTable,
    output_name: String,
    target: delta_funnel::MssqlTargetConfig,
}

impl PyMssqlOutputSpec {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        py: Python<'_>,
        session: Py<PySession>,
        table: delta_funnel::LazyTable,
        schema: String,
        target_table: String,
        load_mode: String,
        name: Option<String>,
        connection_string: Option<String>,
    ) -> PyResult<Self> {
        validate_undotted_target_name(py, "schema", &schema)?;
        validate_undotted_target_name(py, "table", &target_table)?;

        let table_identity = delta_funnel::MssqlTargetTable::new(schema, target_table.clone())
            .map_err(|error| rust_error_to_py(py, error))?;
        let mut target = delta_funnel::MssqlTargetConfig::new(table_identity)
            .with_load_mode(parse_load_mode(py, &load_mode)?);
        if let Some(connection_string) = connection_string {
            target = target.with_connection(
                delta_funnel::MssqlConnectionConfig::new(connection_string)
                    .map_err(|error| rust_error_to_py(py, error))?,
            );
        }

        Ok(Self {
            session,
            table,
            output_name: name.unwrap_or(target_table),
            target,
        })
    }

    pub(crate) fn write_plan(
        &self,
        run_mode: delta_funnel::RunMode,
    ) -> delta_funnel::OutputWritePlan {
        delta_funnel::OutputWritePlan::new(
            self.table.clone(),
            delta_funnel::MssqlOutputTarget::new(
                self.output_name.clone(),
                self.target.clone(),
                run_mode,
            ),
        )
    }

    pub(crate) fn belongs_to_session(&self, py: Python<'_>, session: &Py<PySession>) -> bool {
        self.session.bind(py).is(session.bind(py))
    }
}

#[pymethods]
impl PyMssqlOutputSpec {
    fn __repr__(&self) -> String {
        let table = self.target.table();
        let schema = table.schema().unwrap_or("");
        format!(
            "deltafunnel.MssqlOutputSpec(name={:?}, source_table={:?}, schema={:?}, table={:?})",
            self.output_name,
            self.table.name(),
            schema,
            table.table()
        )
    }
}

pub(crate) fn add_output(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyMssqlOutputSpec>()
}

fn parse_load_mode(py: Python<'_>, value: &str) -> PyResult<delta_funnel::LoadMode> {
    match value {
        "append_existing" => Ok(delta_funnel::LoadMode::AppendExisting),
        "create_and_load" => Ok(delta_funnel::LoadMode::CreateAndLoad),
        "replace" => Ok(delta_funnel::LoadMode::Replace),
        _ => Err(config_py_error(
            py,
            "invalid_load_mode",
            format!("invalid `load_mode` value `{value}`"),
        )),
    }
}

fn validate_undotted_target_name(
    py: Python<'_>,
    option_name: &'static str,
    value: &str,
) -> PyResult<()> {
    if value.contains('.') {
        return Err(config_py_error(
            py,
            "dotted_target_name",
            format!("`{option_name}` must be passed separately and must not contain `.`"),
        ));
    }
    Ok(())
}

fn rust_error_to_py(py: Python<'_>, error: delta_funnel::DeltaFunnelError) -> PyErr {
    match delta_funnel_error_to_py(py, error) {
        Ok(error) => error,
        Err(error) => error,
    }
}

fn config_py_error(py: Python<'_>, kind: &'static str, message: String) -> PyErr {
    match delta_funnel_py_error(py, "config", kind, message, None) {
        Ok(error) => error,
        Err(error) => error,
    }
}

#[cfg(test)]
mod tests {
    use super::PyMssqlOutputSpec;
    use crate::deltafunnel;
    use delta_funnel::LoadMode;
    use pyo3::exceptions::PyKeyError;
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyModule};

    #[test]
    fn module_exports_mssql_output_spec_type() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            let output_type = module.getattr("MssqlOutputSpec")?;
            assert_eq!(
                output_type.getattr("__name__")?.extract::<String>()?,
                "MssqlOutputSpec"
            );

            Ok(())
        })
    }

    #[test]
    fn table_to_mssql_builds_output_spec_with_default_name() -> PyResult<()> {
        Python::attach(|py| {
            let table = sql_table(py)?;
            let kwargs = mssql_kwargs(py, "dbo", "orders", "create_and_load")?;

            let spec = table.call_method("to_mssql", (), Some(&kwargs))?;
            assert_eq!(
                spec.repr()?.extract::<String>()?,
                "deltafunnel.MssqlOutputSpec(name=\"orders\", source_table=\"table_0\", schema=\"dbo\", table=\"orders\")"
            );
            let spec = spec.extract::<PyRef<'_, PyMssqlOutputSpec>>()?;

            assert_eq!(spec.output_name, "orders");
            assert_eq!(spec.target.table().schema(), Some("dbo"));
            assert_eq!(spec.target.table().table(), "orders");
            assert_eq!(spec.target.load_mode(), LoadMode::CreateAndLoad);
            assert_eq!(spec.table.name(), "table_0");

            Ok(())
        })
    }

    #[test]
    fn table_to_mssql_preserves_explicit_output_name() -> PyResult<()> {
        Python::attach(|py| {
            let table = sql_table(py)?;
            let kwargs = mssql_kwargs(py, "reporting", "orders", "append_existing")?;
            kwargs.set_item("name", "orders_output")?;

            let spec = table.call_method("to_mssql", (), Some(&kwargs))?;
            let spec = spec.extract::<PyRef<'_, PyMssqlOutputSpec>>()?;

            assert_eq!(spec.output_name, "orders_output");
            assert_eq!(spec.target.table().schema(), Some("reporting"));
            assert_eq!(spec.target.load_mode(), LoadMode::AppendExisting);

            Ok(())
        })
    }

    #[test]
    fn table_to_mssql_accepts_and_redacts_connection_override() -> PyResult<()> {
        Python::attach(|py| {
            let table = sql_table(py)?;
            let kwargs = mssql_kwargs(py, "dbo", "orders", "append_existing")?;
            kwargs.set_item(
                "connection_string",
                "server=tcp:sql.example.com;password=secret-token",
            )?;

            let spec = table.call_method("to_mssql", (), Some(&kwargs))?;
            let spec = spec.extract::<PyRef<'_, PyMssqlOutputSpec>>()?;
            let debug = format!("{:?}", spec.target);

            assert!(spec.target.connection().is_some());
            assert!(!debug.contains("secret-token"));
            assert!(!debug.contains("password=secret-token"));

            Ok(())
        })
    }

    #[test]
    fn table_to_mssql_rejects_dotted_target_parts() -> PyResult<()> {
        Python::attach(|py| {
            let table = sql_table(py)?;
            let kwargs = mssql_kwargs(py, "dbo", "dbo.orders", "append_existing")?;

            let error = table
                .call_method("to_mssql", (), Some(&kwargs))
                .unwrap_err();

            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "dotted_target_name"
            );

            Ok(())
        })
    }

    #[test]
    fn table_to_mssql_rejects_invalid_load_mode() -> PyResult<()> {
        Python::attach(|py| {
            let table = sql_table(py)?;
            let kwargs = mssql_kwargs(py, "dbo", "orders", "truncate_and_load")?;

            let error = table
                .call_method("to_mssql", (), Some(&kwargs))
                .unwrap_err();

            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_load_mode"
            );

            Ok(())
        })
    }

    #[test]
    fn table_write_to_mssql_dry_run_returns_report_dict() -> PyResult<()> {
        Python::attach(|py| {
            let table = sql_table(py)?;
            let kwargs = mssql_kwargs(py, "dbo", "orders", "create_and_load")?;
            kwargs.set_item(
                "connection_string",
                "server=tcp:sql.example.com;password=secret-token",
            )?;
            kwargs.set_item("dry_run", true)?;

            let report = table.call_method("write_to_mssql", (), Some(&kwargs))?;
            let report_repr = report.repr()?.extract::<String>()?;
            assert!(!report_repr.contains("secret-token"));
            assert!(!report_repr.contains("password=secret-token"));

            let report = report.cast::<PyDict>()?;
            let dry_run = required_item(report, "dry_run")?;
            let dry_run = dry_run.cast::<PyDict>()?;
            let target_table = required_item(report, "target_table")?;
            let target_table = target_table.cast::<PyDict>()?;

            assert_eq!(
                required_item(report, "output_name")?.extract::<String>()?,
                "orders"
            );
            assert_eq!(
                required_item(report, "run_mode")?.extract::<String>()?,
                "dry_run"
            );
            assert_eq!(
                required_item(report, "load_mode")?.extract::<String>()?,
                "create_and_load"
            );
            assert_eq!(
                required_item(target_table, "schema")?.extract::<String>()?,
                "dbo"
            );
            assert_eq!(
                required_item(target_table, "table")?.extract::<String>()?,
                "orders"
            );
            assert!(!required_item(dry_run, "sql_server_contacted")?.extract::<bool>()?);
            assert!(!required_item(dry_run, "row_production_started")?.extract::<bool>()?);
            assert!(!required_item(dry_run, "table_lifecycle_started")?.extract::<bool>()?);
            assert!(!required_item(dry_run, "bulk_writer_started")?.extract::<bool>()?);

            Ok(())
        })
    }

    #[test]
    fn table_write_to_mssql_dry_run_rejects_missing_connection() -> PyResult<()> {
        Python::attach(|py| {
            let table = sql_table(py)?;
            let kwargs = mssql_kwargs(py, "dbo", "orders", "append_existing")?;
            kwargs.set_item("dry_run", true)?;

            let error = table
                .call_method("write_to_mssql", (), Some(&kwargs))
                .unwrap_err();

            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "mssql_target_config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "missing_mssql_connection"
            );

            Ok(())
        })
    }

    #[test]
    fn table_write_to_mssql_rejects_execute_mode_until_write_slice() -> PyResult<()> {
        Python::attach(|py| {
            let table = sql_table(py)?;
            let kwargs = mssql_kwargs(py, "dbo", "orders", "append_existing")?;

            let error = table
                .call_method("write_to_mssql", (), Some(&kwargs))
                .unwrap_err();
            assert_execute_mode_error(py, &error)?;

            kwargs.set_item("dry_run", false)?;
            let error = table
                .call_method("write_to_mssql", (), Some(&kwargs))
                .unwrap_err();
            assert_execute_mode_error(py, &error)?;

            Ok(())
        })
    }

    fn sql_table(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        let module = PyModule::new(py, "deltafunnel")?;
        deltafunnel(&module)?;
        let session = module.getattr("Session")?.call0()?;
        session.call_method("table_from_sql", ("select 1 as id",), None)
    }

    fn mssql_kwargs<'py>(
        py: Python<'py>,
        schema: &str,
        table: &str,
        load_mode: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("schema", schema)?;
        kwargs.set_item("table", table)?;
        kwargs.set_item("load_mode", load_mode)?;
        Ok(kwargs)
    }

    fn required_item<'py>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Bound<'py, PyAny>> {
        dict.get_item(key)?
            .ok_or_else(|| PyKeyError::new_err(key.to_owned()))
    }

    fn assert_execute_mode_error(py: Python<'_>, error: &PyErr) -> PyResult<()> {
        assert_eq!(
            error.value(py).getattr("phase")?.extract::<String>()?,
            "config"
        );
        assert_eq!(
            error.value(py).getattr("kind")?.extract::<String>()?,
            "execute_mode_not_enabled"
        );
        Ok(())
    }
}
