//! Python MSSQL output specification wrapper.

use pyo3::prelude::*;

use crate::exception::{delta_funnel_error_to_py, delta_funnel_py_error};

#[pyclass(name = "MssqlOutputSpec", module = "deltafunnel")]
pub(crate) struct PyMssqlOutputSpec {
    table: delta_funnel::LazyTable,
    output_name: String,
    target: delta_funnel::MssqlTargetConfig,
}

impl PyMssqlOutputSpec {
    pub(crate) fn new(
        py: Python<'_>,
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
            table,
            output_name: name.unwrap_or(target_table),
            target,
        })
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
}
