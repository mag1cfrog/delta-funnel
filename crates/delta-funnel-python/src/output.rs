//! Python MSSQL output specification wrapper.

use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

use crate::exception::{delta_funnel_error_to_py, delta_funnel_py_error};
use crate::session::PySession;

/// Opaque SQL Server output spec for `Session.write_all(...)`.
///
/// Build values with `Table.to_mssql(...)`. The spec carries its owning
/// session, lazy source table, output name, target table, load mode, and
/// optional connection override.
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
            "deltafunnel.MssqlOutputSpec(name={:?}, source_table={:?}, schema={:?}, table={:?}, load_mode={:?})",
            self.output_name,
            self.table.name(),
            schema,
            table.table(),
            load_mode_name(self.target.load_mode())
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

const fn load_mode_name(load_mode: delta_funnel::LoadMode) -> &'static str {
    match load_mode {
        delta_funnel::LoadMode::AppendExisting => "append_existing",
        delta_funnel::LoadMode::CreateAndLoad => "create_and_load",
        delta_funnel::LoadMode::Replace => "replace",
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
    use delta_funnel::{LoadMode, MssqlTableName, connect_mssql_client_from_ado_string};
    use pyo3::exceptions::PyKeyError;
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyModule};
    use std::{
        env,
        error::Error,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    const MSSQL_CONNECTION_STRING_ENV: &str = "DELTA_FUNNEL_MSSQL_TEST_CONNECTION_STRING";
    const MSSQL_SCHEMA_ENV: &str = "DELTA_FUNNEL_MSSQL_TEST_SCHEMA";
    static NEXT_TABLE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    type TestResult<T> = Result<T, Box<dyn Error + Send + Sync + 'static>>;

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
                "deltafunnel.MssqlOutputSpec(name=\"orders\", source_table=\"table_0\", schema=\"dbo\", table=\"orders\", load_mode=\"create_and_load\")"
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
    fn table_to_mssql_accepts_replace_load_mode() -> PyResult<()> {
        Python::attach(|py| {
            let table = sql_table(py)?;
            let kwargs = mssql_kwargs(py, "dbo", "orders", "replace")?;

            let spec = table.call_method("to_mssql", (), Some(&kwargs))?;
            let spec = spec.extract::<PyRef<'_, PyMssqlOutputSpec>>()?;

            assert_eq!(spec.target.load_mode(), LoadMode::Replace);

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
    fn table_write_to_mssql_execute_mode_uses_rust_missing_connection_guard() -> PyResult<()> {
        Python::attach(|py| {
            let table = sql_table(py)?;
            let kwargs = mssql_kwargs(py, "dbo", "orders", "append_existing")?;

            let error = table
                .call_method("write_to_mssql", (), Some(&kwargs))
                .unwrap_err();
            assert_missing_connection_error(py, &error)?;

            kwargs.set_item("dry_run", false)?;
            let error = table
                .call_method("write_to_mssql", (), Some(&kwargs))
                .unwrap_err();
            assert_missing_connection_error(py, &error)?;

            Ok(())
        })
    }

    #[test]
    #[ignore = "runs through cargo xtask sqlserver-test"]
    fn table_write_to_mssql_execute_writes_with_default_connection_when_configured()
    -> TestResult<()> {
        let Some(config) = MssqlIntegrationConfig::from_env() else {
            return Ok(());
        };
        let table = unique_table_name(&config.schema)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        runtime.block_on(drop_table(&config, &table))?;
        let write_result = Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session_kwargs = PyDict::new(py);
            session_kwargs.set_item(
                "default_mssql_connection_string",
                config.connection_string.as_str(),
            )?;
            let session = module.getattr("Session")?.call((), Some(&session_kwargs))?;
            let source = session.call_method(
                "table_from_sql",
                ("\
select cast(101 as bigint) as order_id
union all select cast(102 as bigint) as order_id
union all select cast(103 as bigint) as order_id",),
                None,
            )?;
            let kwargs = mssql_kwargs(
                py,
                &config.schema,
                table.table().as_str(),
                "create_and_load",
            )?;

            let report = source.call_method("write_to_mssql", (), Some(&kwargs))?;
            let report_repr = report.repr()?.extract::<String>()?;
            assert!(!report_repr.contains(config.connection_string.as_str()));
            let report = report.cast::<PyDict>()?;
            assert_eq!(
                required_item(report, "output_name")?.extract::<String>()?,
                table.table().as_str()
            );
            assert_eq!(
                required_item(report, "run_mode")?.extract::<String>()?,
                "execute"
            );
            assert_eq!(
                required_item(report, "connection_source")?.extract::<String>()?,
                "context_default"
            );
            assert_eq!(
                required_item(report, "cleanup")?.extract::<String>()?,
                "not_attempted"
            );
            assert!(!required_item(report, "partial_write_possible")?.extract::<bool>()?);

            let output_row_count = required_item(report, "output_row_count")?;
            let output_row_count = output_row_count.cast::<PyDict>()?;
            assert_eq!(
                required_item(output_row_count, "kind")?.extract::<String>()?,
                "exact"
            );
            assert_eq!(
                required_item(output_row_count, "value")?.extract::<u64>()?,
                3
            );
            let target_row_count = required_item(report, "target_row_count")?;
            let target_row_count = target_row_count.cast::<PyDict>()?;
            assert_eq!(
                required_item(target_row_count, "kind")?.extract::<String>()?,
                "exact"
            );
            assert_eq!(
                required_item(target_row_count, "value")?.extract::<u64>()?,
                3
            );

            let validation = required_item(report, "validation_status")?;
            let validation = validation.cast::<PyDict>()?;
            assert_eq!(
                required_item(validation, "kind")?.extract::<String>()?,
                "passed"
            );
            let write_stats = required_item(report, "write_stats")?;
            let write_stats = write_stats.cast::<PyDict>()?;
            assert_eq!(
                required_item(write_stats, "rows_written")?.extract::<u64>()?,
                3
            );
            assert!(required_item(write_stats, "batches_written")?.extract::<u64>()? >= 1);

            Ok::<(), PyErr>(())
        });
        let cleanup_result = runtime.block_on(drop_table(&config, &table));

        match (write_result, cleanup_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(write_error), Ok(())) => Err(Box::new(write_error)),
            (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
            (Err(write_error), Err(cleanup_error)) => {
                Err(format!("write failed: {write_error}; cleanup failed: {cleanup_error}").into())
            }
        }
    }

    #[test]
    #[ignore = "runs through cargo xtask sqlserver-test"]
    fn table_write_to_mssql_execute_writes_with_connection_override_when_configured()
    -> TestResult<()> {
        let Some(config) = MssqlIntegrationConfig::from_env() else {
            return Ok(());
        };
        let table = unique_table_name(&config.schema)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        runtime.block_on(drop_table(&config, &table))?;
        let write_result = Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let source = session.call_method(
                "table_from_sql",
                ("\
select cast(201 as bigint) as order_id
union all select cast(202 as bigint) as order_id",),
                None,
            )?;
            let kwargs = mssql_kwargs(
                py,
                &config.schema,
                table.table().as_str(),
                "create_and_load",
            )?;
            kwargs.set_item("connection_string", config.connection_string.as_str())?;
            kwargs.set_item("name", "orders_override")?;

            let report = source.call_method("write_to_mssql", (), Some(&kwargs))?;
            let report_repr = report.repr()?.extract::<String>()?;
            assert!(!report_repr.contains(config.connection_string.as_str()));
            let report = report.cast::<PyDict>()?;
            assert_eq!(
                required_item(report, "output_name")?.extract::<String>()?,
                "orders_override"
            );
            assert_eq!(
                required_item(report, "connection_source")?.extract::<String>()?,
                "target_override"
            );
            let output_row_count = required_item(report, "output_row_count")?;
            let output_row_count = output_row_count.cast::<PyDict>()?;
            assert_eq!(
                required_item(output_row_count, "value")?.extract::<u64>()?,
                2
            );
            let write_stats = required_item(report, "write_stats")?;
            let write_stats = write_stats.cast::<PyDict>()?;
            assert_eq!(
                required_item(write_stats, "output_name")?.extract::<String>()?,
                "orders_override"
            );
            assert_eq!(
                required_item(write_stats, "rows_written")?.extract::<u64>()?,
                2
            );

            Ok::<(), PyErr>(())
        });
        let cleanup_result = runtime.block_on(drop_table(&config, &table));

        match (write_result, cleanup_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(write_error), Ok(())) => Err(Box::new(write_error)),
            (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
            (Err(write_error), Err(cleanup_error)) => {
                Err(format!("write failed: {write_error}; cleanup failed: {cleanup_error}").into())
            }
        }
    }

    #[test]
    #[ignore = "runs through cargo xtask sqlserver-test"]
    fn table_write_to_mssql_replace_swaps_existing_target_when_configured() -> TestResult<()> {
        let Some(config) = MssqlIntegrationConfig::from_env() else {
            return Ok(());
        };
        let table = unique_table_name(&config.schema)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        runtime.block_on(seed_replace_target(&config, &table))?;
        let write_result = Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session_kwargs = PyDict::new(py);
            session_kwargs.set_item(
                "default_mssql_connection_string",
                config.connection_string.as_str(),
            )?;
            let session = module.getattr("Session")?.call((), Some(&session_kwargs))?;
            let source = session.call_method(
                "table_from_sql",
                ("\
select cast(801 as bigint) as order_id
union all select cast(802 as bigint) as order_id",),
                None,
            )?;
            let kwargs = mssql_kwargs(py, &config.schema, table.table().as_str(), "replace")?;

            let report = source.call_method("write_to_mssql", (), Some(&kwargs))?;
            let report = report.cast::<PyDict>()?;
            assert_eq!(
                required_item(report, "load_mode")?.extract::<String>()?,
                "replace"
            );
            assert!(!required_item(report, "partial_write_possible")?.extract::<bool>()?);
            let validation = required_item(report, "validation_status")?;
            let validation = validation.cast::<PyDict>()?;
            assert_eq!(
                required_item(validation, "kind")?.extract::<String>()?,
                "passed"
            );
            let target_row_count = required_item(report, "target_row_count")?;
            let target_row_count = target_row_count.cast::<PyDict>()?;
            assert_eq!(
                required_item(target_row_count, "value")?.extract::<u64>()?,
                2
            );

            Ok::<(), PyErr>(())
        });
        let result = match write_result {
            Ok(()) => runtime.block_on(assert_replace_target_rows(&config, &table, 801, 802)),
            Err(write_error) => Err(Box::new(write_error) as Box<dyn Error + Send + Sync>),
        };
        let cleanup_result = runtime.block_on(drop_table(&config, &table));

        match (result, cleanup_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(write_error), Ok(())) => Err(write_error),
            (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
            (Err(write_error), Err(cleanup_error)) => {
                Err(format!("write failed: {write_error}; cleanup failed: {cleanup_error}").into())
            }
        }
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

    fn assert_missing_connection_error(py: Python<'_>, error: &PyErr) -> PyResult<()> {
        assert_eq!(
            error.value(py).getattr("phase")?.extract::<String>()?,
            "mssql_target_config"
        );
        assert_eq!(
            error.value(py).getattr("kind")?.extract::<String>()?,
            "missing_mssql_connection"
        );
        Ok(())
    }

    struct MssqlIntegrationConfig {
        connection_string: String,
        schema: String,
    }

    impl MssqlIntegrationConfig {
        fn from_env() -> Option<Self> {
            let connection_string = env::var(MSSQL_CONNECTION_STRING_ENV)
                .ok()
                .and_then(non_empty_value);
            let schema = env::var(MSSQL_SCHEMA_ENV).ok().and_then(non_empty_value);

            match (connection_string, schema) {
                (Some(connection_string), Some(schema)) => Some(Self {
                    connection_string,
                    schema,
                }),
                _ => {
                    eprintln!(
                        "skipping Python MSSQL integration test; missing {MSSQL_CONNECTION_STRING_ENV} or {MSSQL_SCHEMA_ENV}"
                    );
                    None
                }
            }
        }
    }

    async fn drop_table(config: &MssqlIntegrationConfig, table: &MssqlTableName) -> TestResult<()> {
        let mut client = connect_mssql_client_from_ado_string(&config.connection_string).await?;
        client
            .execute_statement(&format!("DROP TABLE IF EXISTS {};", table.quoted_sql()))
            .await?;

        Ok(())
    }

    async fn seed_replace_target(
        config: &MssqlIntegrationConfig,
        table: &MssqlTableName,
    ) -> TestResult<()> {
        let mut client = connect_mssql_client_from_ado_string(&config.connection_string).await?;
        client
            .execute_statement(&format!(
                "\
DROP TABLE IF EXISTS {table};
CREATE TABLE {table} ([order_id] BIGINT NOT NULL);
INSERT INTO {table} ([order_id]) VALUES (700), (701);",
                table = table.quoted_sql(),
            ))
            .await?;

        Ok(())
    }

    async fn assert_replace_target_rows(
        config: &MssqlIntegrationConfig,
        table: &MssqlTableName,
        first_id: i64,
        second_id: i64,
    ) -> TestResult<()> {
        let mut client = connect_mssql_client_from_ado_string(&config.connection_string).await?;
        client
            .execute_statement(&format!(
                "\
IF (SELECT COUNT(*) FROM {table}) <> 2
    THROW 51010, 'replace target row count mismatch', 1;
IF NOT EXISTS (SELECT 1 FROM {table} WHERE [order_id] = {first_id})
    THROW 51011, 'replace target first row missing', 1;
IF NOT EXISTS (SELECT 1 FROM {table} WHERE [order_id] = {second_id})
    THROW 51012, 'replace target second row missing', 1;
IF EXISTS (SELECT 1 FROM {table} WHERE [order_id] NOT IN ({first_id}, {second_id}))
    THROW 51013, 'replace target kept unexpected rows', 1;",
                table = table.quoted_sql(),
            ))
            .await?;

        Ok(())
    }

    fn unique_table_name(schema: &str) -> TestResult<MssqlTableName> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let sequence = NEXT_TABLE_SEQUENCE.fetch_add(1, Ordering::Relaxed);

        Ok(MssqlTableName::new(
            schema.to_owned(),
            format!(
                "df_python_mssql_it_{}_{}_{}",
                std::process::id(),
                timestamp,
                sequence
            ),
        )?)
    }

    fn non_empty_value(value: String) -> Option<String> {
        let value = value.trim().to_owned();
        (!value.is_empty()).then_some(value)
    }
}
