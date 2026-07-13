//! Python session wrapper.

use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBool, PyDict, PyDictMethods};

use crate::exception::{delta_funnel_error_to_py, delta_funnel_py_error};
use crate::json::json_value_to_py;
use crate::output::PyMssqlOutputSpec;
use crate::progress::PythonProgress;
use crate::table::PyTable;

/// Delta Funnel workflow session.
///
/// `Session()` uses Rust defaults unless options are supplied. Register Delta
/// sources, build lazy SQL tables, and execute or dry-run SQL Server outputs
/// from this object.
#[pyclass(name = "Session", module = "deltafunnel")]
pub(crate) struct PySession {
    inner: delta_funnel::DeltaFunnelSession,
    runtime: delta_funnel::DeltaFunnelRuntime,
}

#[pymethods]
impl PySession {
    #[new]
    #[pyo3(signature = (*, default_mssql_connection_string=None, target_partitions=None, output_batch_size=None, provider_scan_options=None, validation_options=None, schema_options=None))]
    fn new(
        py: Python<'_>,
        default_mssql_connection_string: Option<String>,
        #[pyo3(from_py_with = parse_target_partitions_arg)] target_partitions: Option<usize>,
        #[pyo3(from_py_with = parse_output_batch_size_arg)] output_batch_size: Option<usize>,
        provider_scan_options: Option<&Bound<'_, PyDict>>,
        validation_options: Option<&Bound<'_, PyDict>>,
        schema_options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let mut options = delta_funnel::SessionOptions::default();
        if let Some(connection_string) = default_mssql_connection_string {
            let connection = delta_funnel::MssqlConnectionConfig::new(connection_string)
                .map_err(|error| rust_error_to_py(py, error))?;
            options = options.with_default_mssql_connection(connection);
        }
        options = options.with_query_options(delta_funnel::QueryOptions {
            target_partitions,
            output_batch_size,
        });
        if let Some(provider_scan_options) = parse_provider_scan_options(py, provider_scan_options)?
        {
            options = options.with_provider_scan_options(provider_scan_options);
        }
        options =
            options.with_validation_options(parse_validation_options(py, validation_options)?);
        if let Some(schema_options) = parse_schema_options(py, schema_options)? {
            options = options.with_mssql_schema_options(schema_options);
        }

        let inner = delta_funnel::DeltaFunnelSession::new(options)
            .map_err(|error| rust_error_to_py(py, error))?;
        let runtime =
            delta_funnel::DeltaFunnelRuntime::new().map_err(|error| rust_error_to_py(py, error))?;

        Ok(Self { inner, runtime })
    }

    fn __repr__(&self) -> String {
        let sources = self
            .inner
            .sources()
            .iter()
            .map(delta_funnel::RegisteredSessionSource::name)
            .collect::<Vec<_>>();
        let derived_tables = self
            .inner
            .derived_tables()
            .iter()
            .map(delta_funnel::RegisteredDerivedTable::name)
            .collect::<Vec<_>>();
        format!("deltafunnel.Session(sources={sources:?}, derived_tables={derived_tables:?})")
    }

    /// Registers a named Delta source, or returns a pending source that cannot be referenced by SQL.
    ///
    /// A pending source is not registered in the session SQL catalog and cannot
    /// be referenced by SQL until `alias(name)` is called. Progress applies only
    /// when this call registers a named source. An unnamed pending source stays
    /// lazy and ignores this call's progress setting.
    #[pyo3(signature = (source_uri, *, version=None, storage_options=None, name=None, progress=None))]
    fn delta_lake(
        slf: Py<Self>,
        py: Python<'_>,
        source_uri: String,
        version: Option<&Bound<'_, PyAny>>,
        storage_options: Option<&Bound<'_, PyDict>>,
        name: Option<String>,
        progress: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let source =
            PendingDeltaSource::new(py, slf.clone_ref(py), source_uri, version, storage_options)?;
        if let Some(name) = name {
            let progress = PythonProgress::new(progress);
            return Py::new(py, source.register_alias(py, name, progress.as_ref())?)
                .map(Py::into_any);
        };

        Py::new(py, source).map(Py::into_any)
    }

    /// Builds a lazy SQL-derived table without executing rows.
    fn table_from_sql(slf: Py<Self>, py: Python<'_>, sql: String) -> PyResult<PyTable> {
        let table = {
            let mut session = slf.borrow_mut(py);
            let PySession { inner, runtime } = &mut *session;
            runtime
                .table_from_sql(inner, sql.as_str())
                .map_err(|error| rust_error_to_py(py, error))?
        };
        Ok(PyTable::from_inner(slf, table))
    }

    /// Writes multiple SQL Server outputs, or runs a dry-run plan when requested.
    ///
    /// Pass `dry_run=True` to plan without writing. Execute calls accept
    /// `options={"cache_mode": "auto"}` or `options={"cache_mode": "disabled"}`.
    /// Returns a plain Python `dict` report. One consolidated progress display
    /// follows output planning, shared cache work, and sequential writes. Pass
    /// `progress=False` to disable it for this call.
    #[pyo3(signature = (outputs, *, options=None, dry_run=None, progress=None))]
    fn write_all(
        slf: Py<Self>,
        py: Python<'_>,
        outputs: Vec<PyRef<'_, PyMssqlOutputSpec>>,
        options: Option<&Bound<'_, PyDict>>,
        dry_run: Option<bool>,
        progress: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        for output in &outputs {
            if !output.belongs_to_session(py, &slf) {
                return Err(config_py_error(
                    py,
                    "output_session_mismatch",
                    "all output specs must belong to this Session".to_owned(),
                ));
            }
        }
        let requests = outputs
            .iter()
            .map(|output| {
                output.write_plan(if dry_run == Some(true) {
                    delta_funnel::RunMode::DryRun
                } else {
                    delta_funnel::RunMode::Execute
                })
            })
            .collect::<Vec<_>>();

        if dry_run == Some(true) {
            if options.is_some() {
                return Err(config_py_error(
                    py,
                    "invalid_option_value",
                    "`options` is only supported for execute `write_all` calls".to_owned(),
                ));
            }
            if requests.is_empty() {
                return slf.borrow(py).dry_run_all_to_mssql(py, &requests, None);
            }
            let progress = PythonProgress::new(progress);
            return slf
                .borrow(py)
                .dry_run_all_to_mssql(py, &requests, progress.as_ref());
        }

        let options = parse_write_all_options(py, options)?;
        if requests.is_empty() {
            return slf
                .borrow(py)
                .execute_write_all(py, &requests, options, None);
        }
        let progress = PythonProgress::new(progress);
        slf.borrow(py)
            .execute_write_all(py, &requests, options, progress.as_ref())
    }
}

impl PySession {
    pub(crate) fn source_repr_details(
        &self,
        table: &delta_funnel::LazyTable,
    ) -> Option<(&str, u64)> {
        if table.kind() != delta_funnel::LazyTableKind::DeltaSource {
            return None;
        }
        self.inner
            .registered_source(table.name())
            .map(|source| (source.source_uri(), source.snapshot_version()))
    }

    fn register_delta_source(
        &mut self,
        py: Python<'_>,
        name: String,
        source_uri: String,
        version: Option<u64>,
        storage_options: delta_funnel::DeltaStorageOptions,
        progress: Option<&PythonProgress>,
    ) -> PyResult<delta_funnel::LazyTable> {
        let source = delta_source_config(name, source_uri, version, storage_options);
        let result = match progress {
            Some(progress) => {
                self.runtime
                    .delta_lake_with_progress(&mut self.inner, source, progress.reporter())
            }
            None => self.inner.delta_lake(source),
        }
        .map_err(|error| rust_error_to_py(py, error));
        if let Some(progress) = progress {
            progress.finish(py, result.as_ref().err(), None)?;
        }
        result
    }

    pub(crate) fn register_table_alias(
        &mut self,
        py: Python<'_>,
        name: String,
        table: &delta_funnel::LazyTable,
    ) -> PyResult<delta_funnel::LazyTable> {
        self.inner
            .register_alias(name, table)
            .map_err(|error| rust_error_to_py(py, error))
    }

    pub(crate) fn dry_run_to_mssql(
        &self,
        py: Python<'_>,
        request: &delta_funnel::OutputWritePlan,
        progress: Option<&PythonProgress>,
    ) -> PyResult<Py<PyAny>> {
        let report = match progress {
            Some(progress) => self.runtime.dry_run_to_mssql_with_progress(
                &self.inner,
                request,
                progress.reporter(),
            ),
            None => self.runtime.dry_run_to_mssql(&self.inner, request),
        };
        let report = report.map_err(|error| rust_error_to_py(py, error));
        if let Some(progress) = progress {
            progress.finish(py, report.as_ref().err(), None)?;
        }
        let report = report?;
        json_value_to_py(py, &report.to_json_value())
    }

    #[allow(
        clippy::result_large_err,
        reason = "the GIL-detached call carries the core error until Python conversion resumes"
    )]
    pub(crate) fn write_to_mssql(
        &self,
        py: Python<'_>,
        request: &delta_funnel::OutputWritePlan,
        progress: Option<&PythonProgress>,
    ) -> PyResult<Py<PyAny>> {
        let report = py.detach(|| match progress {
            Some(progress) => {
                self.runtime
                    .write_to_mssql_with_progress(&self.inner, request, progress.reporter())
            }
            None => self.runtime.write_to_mssql(&self.inner, request),
        });
        let report = report.map_err(|error| rust_error_to_py(py, error));
        if let Some(progress) = progress {
            progress.finish(py, report.as_ref().err(), None)?;
        }
        let report = report?;
        json_value_to_py(py, &report.to_json_value())
    }

    #[allow(
        clippy::result_large_err,
        reason = "the GIL-detached call carries the core error until Python conversion resumes"
    )]
    pub(crate) fn preview_table(
        &self,
        py: Python<'_>,
        table: &delta_funnel::LazyTable,
        limit: usize,
        progress: Option<&PythonProgress>,
    ) -> PyResult<delta_funnel::TablePreview> {
        let preview = py.detach(|| match progress {
            Some(progress) => self.runtime.preview_table_with_progress(
                &self.inner,
                table,
                limit,
                progress.reporter(),
            ),
            None => self.runtime.preview_table(&self.inner, table, limit),
        });
        let preview = preview.map_err(|error| rust_error_to_py(py, error));
        if let Some(progress) = progress {
            progress.finish(py, preview.as_ref().err(), None)?;
        }
        preview
    }

    fn dry_run_all_to_mssql(
        &self,
        py: Python<'_>,
        requests: &[delta_funnel::OutputWritePlan],
        progress: Option<&PythonProgress>,
    ) -> PyResult<Py<PyAny>> {
        let report = match progress {
            Some(progress) => self.runtime.dry_run_all_to_mssql_with_progress(
                &self.inner,
                requests,
                progress.reporter(),
            ),
            None => self.runtime.dry_run_all_to_mssql(&self.inner, requests),
        };
        let report = report.map_err(|error| rust_error_to_py(py, error));
        if let Some(progress) = progress {
            progress.finish(py, report.as_ref().err(), None)?;
        }
        let report = report?;
        json_value_to_py(py, &report.to_json_value())
    }

    #[allow(
        clippy::result_large_err,
        reason = "the GIL-detached call carries the core error until Python conversion resumes"
    )]
    fn execute_write_all(
        &self,
        py: Python<'_>,
        requests: &[delta_funnel::OutputWritePlan],
        options: delta_funnel::WriteAllOptions,
        progress: Option<&PythonProgress>,
    ) -> PyResult<Py<PyAny>> {
        let report = py.detach(|| match progress {
            Some(progress) => self.runtime.write_all_with_progress(
                &self.inner,
                requests,
                options,
                progress.reporter(),
            ),
            None => self
                .runtime
                .write_all_with_options(&self.inner, requests, options),
        });
        let report = report.map_err(|error| rust_error_to_py(py, error));
        if let Some(progress) = progress {
            let operation_report = report
                .as_ref()
                .ok()
                .filter(|report| !report.all_succeeded())
                .map(delta_funnel::WriteAllReport::to_json_value);
            progress.finish(py, report.as_ref().err(), operation_report.as_ref())?;
        }
        let report = report?;
        json_value_to_py(py, &report.to_json_value())
    }
}

/// Unregistered Delta source returned by `Session.delta_lake(...)` without `name`.
///
/// Call `alias(name)` to register it and receive a `Table`. A pending source
/// cannot be referenced by SQL.
#[pyclass(name = "PendingDeltaSource", module = "deltafunnel")]
struct PendingDeltaSource {
    session: Py<PySession>,
    source_uri: String,
    version: Option<u64>,
    storage_options: delta_funnel::DeltaStorageOptions,
}

impl PendingDeltaSource {
    fn new(
        py: Python<'_>,
        session: Py<PySession>,
        source_uri: String,
        version: Option<&Bound<'_, PyAny>>,
        storage_options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        validate_source_uri(py, &source_uri)?;
        Ok(Self {
            session,
            source_uri,
            version: parse_delta_version(py, version)?,
            storage_options: parse_storage_options(py, storage_options)?,
        })
    }

    fn register_alias(
        &self,
        py: Python<'_>,
        name: String,
        progress: Option<&PythonProgress>,
    ) -> PyResult<PyTable> {
        let table = self.session.borrow_mut(py).register_delta_source(
            py,
            name,
            self.source_uri.clone(),
            self.version,
            self.storage_options.clone(),
            progress,
        )?;
        Ok(PyTable::from_inner(self.session.clone_ref(py), table))
    }
}

#[pymethods]
impl PendingDeltaSource {
    /// Registers this pending Delta source under `name` and returns a `Table`.
    ///
    /// Progress is selected for this registration call only. The earlier
    /// `Session.delta_lake(...)` call does not preserve a progress setting.
    #[pyo3(signature = (name, *, progress=None))]
    fn alias(&self, py: Python<'_>, name: String, progress: Option<bool>) -> PyResult<PyTable> {
        let progress = PythonProgress::new(progress);
        self.register_alias(py, name, progress.as_ref())
    }

    fn __repr__(&self) -> String {
        match self.version {
            Some(version) => format!(
                "deltafunnel.PendingDeltaSource(source_uri={:?}, snapshot_version={version})",
                delta_funnel::sanitize_uri_for_display(&self.source_uri),
            ),
            None => format!(
                "deltafunnel.PendingDeltaSource(source_uri={:?}, snapshot_version=None)",
                delta_funnel::sanitize_uri_for_display(&self.source_uri),
            ),
        }
    }
}

pub(crate) fn add_session(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySession>()?;
    module.add_class::<PendingDeltaSource>()
}

fn rust_error_to_py(py: Python<'_>, error: delta_funnel::DeltaFunnelError) -> PyErr {
    match delta_funnel_error_to_py(py, error) {
        Ok(error) => error,
        Err(error) => error,
    }
}

fn delta_source_config(
    name: String,
    source_uri: String,
    version: Option<u64>,
    storage_options: delta_funnel::DeltaStorageOptions,
) -> delta_funnel::DeltaSourceConfig {
    delta_funnel::DeltaSourceConfig::new(name, source_uri)
        .with_version(version)
        .with_storage_options(storage_options)
}

fn validate_source_uri(py: Python<'_>, source_uri: &str) -> PyResult<()> {
    if source_uri.is_empty() {
        return Err(source_config_py_error(
            py,
            "invalid_source_uri",
            "`source_uri` must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn parse_delta_version(
    py: Python<'_>,
    version: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<u64>> {
    let Some(version) = version else {
        return Ok(None);
    };
    if version.is_instance_of::<PyBool>() {
        return Err(source_config_py_error(
            py,
            "invalid_version",
            "`version` must be a non-negative integer".to_owned(),
        ));
    }

    let version = version.extract::<u64>().map_err(|_| {
        source_config_py_error(
            py,
            "invalid_version",
            "`version` must be a non-negative integer".to_owned(),
        )
    })?;

    Ok(Some(version))
}

fn parse_storage_options(
    py: Python<'_>,
    storage_options: Option<&Bound<'_, PyDict>>,
) -> PyResult<delta_funnel::DeltaStorageOptions> {
    let mut parsed = delta_funnel::DeltaStorageOptions::default();
    let Some(storage_options) = storage_options else {
        return Ok(parsed);
    };

    for (key, value) in storage_options.iter() {
        let key = key.extract::<String>().map_err(|_| {
            source_config_py_error(
                py,
                "invalid_storage_options",
                "`storage_options` keys must be strings".to_owned(),
            )
        })?;
        let value = value.extract::<String>().map_err(|_| {
            source_config_py_error(
                py,
                "invalid_storage_options",
                "`storage_options` values must be strings".to_owned(),
            )
        })?;
        parsed.insert(key, value);
    }

    Ok(parsed)
}

fn parse_target_partitions_arg(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    optional_usize_arg(value, "target_partitions")
}

fn parse_output_batch_size_arg(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    optional_usize_arg(value, "output_batch_size")
}

fn optional_usize_arg(value: &Bound<'_, PyAny>, option_name: &str) -> PyResult<Option<usize>> {
    if value.is_none() {
        Ok(None)
    } else {
        usize_option(value.py(), value, option_name).map(Some)
    }
}

fn parse_provider_scan_options(
    py: Python<'_>,
    provider_scan_options: Option<&Bound<'_, PyDict>>,
) -> PyResult<Option<delta_funnel::DeltaProviderScanExecutionOptions>> {
    let mut options = delta_funnel::DeltaProviderScanExecutionOptions::default();
    let Some(provider_scan_options) = provider_scan_options else {
        return Ok(None);
    };

    for (key, value) in option_entries(py, provider_scan_options)? {
        let value = usize_option(py, &value, key.as_str())?;
        match key.as_str() {
            "max_concurrent_file_reads_per_scan" => {
                options.max_concurrent_file_reads_per_scan = Some(value);
            }
            "max_concurrent_file_reads_per_partition" => {
                options.max_concurrent_file_reads_per_partition = value;
            }
            "output_buffer_capacity_per_partition" => {
                options.output_buffer_capacity_per_partition = value;
            }
            "native_async_prefetch_file_count_per_partition" => {
                options.native_async_prefetch_file_count_per_partition = value;
            }
            _ => {
                return Err(unknown_option_error(py, "provider scan", key.as_str()));
            }
        }
    }

    options
        .validate()
        .map_err(|error| rust_error_to_py(py, error))?;
    Ok(Some(options))
}

fn parse_validation_options(
    py: Python<'_>,
    validation_options: Option<&Bound<'_, PyDict>>,
) -> PyResult<delta_funnel::ValidationOptions> {
    let mut options = delta_funnel::ValidationOptions::default();
    let Some(validation_options) = validation_options else {
        return Ok(options);
    };

    for (key, value) in option_entries(py, validation_options)? {
        match key.as_str() {
            "target_validation_mode" => {
                options = options.with_target_validation_mode(parse_target_validation_mode(
                    py,
                    &value,
                    key.as_str(),
                )?);
            }
            "dry_run_scan_summary_mode" => {
                options = options.with_dry_run_scan_summary_mode(parse_dry_run_scan_summary_mode(
                    py,
                    &value,
                    key.as_str(),
                )?);
            }
            "require_successful_planning" => {
                let value = bool_option(py, &value, key.as_str())?;
                options = options.with_require_successful_planning(value);
            }
            _ => {
                return Err(unknown_option_error(py, "validation", key.as_str()));
            }
        }
    }

    Ok(options)
}

fn parse_write_all_options(
    py: Python<'_>,
    write_all_options: Option<&Bound<'_, PyDict>>,
) -> PyResult<delta_funnel::WriteAllOptions> {
    let mut options = delta_funnel::WriteAllOptions::default();
    let Some(write_all_options) = write_all_options else {
        return Ok(options);
    };

    for (key, value) in option_entries(py, write_all_options)? {
        match key.as_str() {
            "cache_mode" => {
                options =
                    options.with_cache_mode(parse_write_all_cache_mode(py, &value, key.as_str())?);
            }
            _ => {
                return Err(unknown_option_error(py, "write_all", key.as_str()));
            }
        }
    }

    Ok(options)
}

fn parse_schema_options(
    py: Python<'_>,
    schema_options: Option<&Bound<'_, PyDict>>,
) -> PyResult<Option<delta_funnel::MssqlSchemaPlanOptions>> {
    let mut options = delta_funnel::MssqlSchemaPlanOptions::default();
    let Some(schema_options) = schema_options else {
        return Ok(None);
    };

    for (key, value) in option_entries(py, schema_options)? {
        match key.as_str() {
            "string_policy" => {
                options.string_policy = parse_string_policy(py, &value, key.as_str())?;
            }
            "binary_policy" => {
                options.binary_policy = parse_binary_policy(py, &value, key.as_str())?;
            }
            "timezone_policy" => {
                options.timezone_policy = parse_timezone_policy(py, &value, key.as_str())?;
            }
            "timestamp_policy" => {
                options.timestamp_policy = parse_timestamp_policy(py, &value, key.as_str())?;
            }
            "nanosecond_policy" => {
                options.nanosecond_policy = parse_nanosecond_policy(py, &value, key.as_str())?;
            }
            "uint64_policy" => {
                options.uint64_policy = parse_uint64_policy(py, &value, key.as_str())?;
            }
            "decimal_policy" => {
                options.decimal_policy = parse_decimal_policy(py, &value, key.as_str())?;
            }
            "decimal256_policy" => {
                options.decimal256_policy = parse_decimal256_policy(py, &value, key.as_str())?;
            }
            "float_policy" => {
                options.float_policy = parse_float_policy(py, &value, key.as_str())?;
            }
            "date64_policy" => {
                options.date64_policy = parse_date64_policy(py, &value, key.as_str())?;
            }
            _ => {
                return Err(unknown_option_error(py, "schema", key.as_str()));
            }
        }
    }

    Ok(Some(options))
}

fn parse_target_validation_mode(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::TargetValidationMode> {
    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "disabled" => Ok(delta_funnel::TargetValidationMode::Disabled),
        "validate_if_possible" => Ok(delta_funnel::TargetValidationMode::ValidateIfPossible),
        "require" => Ok(delta_funnel::TargetValidationMode::Require),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn parse_write_all_cache_mode(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::WriteAllCacheMode> {
    match option_string(py, value, option_name)?.as_str() {
        "auto" => Ok(delta_funnel::WriteAllCacheMode::Auto),
        "disabled" => Ok(delta_funnel::WriteAllCacheMode::Disabled),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value"),
        )),
    }
}

fn parse_dry_run_scan_summary_mode(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::DryRunScanSummaryMode> {
    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "metadata_only" => Ok(delta_funnel::DryRunScanSummaryMode::MetadataOnly),
        "exhaust_scan_metadata" => Ok(delta_funnel::DryRunScanSummaryMode::ExhaustScanMetadata),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn parse_string_policy(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::MssqlStringPolicy> {
    if let Ok(value) = value.cast::<PyDict>() {
        return Ok(delta_funnel::MssqlStringPolicy::NVarChar(
            single_positive_usize_entry(py, value, option_name, "nvarchar")?,
        ));
    }

    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "nvarchar_max" => Ok(delta_funnel::MssqlStringPolicy::NVarCharMax),
        "observed_nvarchar" => Ok(delta_funnel::MssqlStringPolicy::ObservedNVarChar),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn parse_binary_policy(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::MssqlBinaryPolicy> {
    if let Ok(value) = value.cast::<PyDict>() {
        return Ok(delta_funnel::MssqlBinaryPolicy::VarBinary(
            single_positive_usize_entry(py, value, option_name, "varbinary")?,
        ));
    }

    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "varbinary_max" => Ok(delta_funnel::MssqlBinaryPolicy::VarBinaryMax),
        "observed_varbinary" => Ok(delta_funnel::MssqlBinaryPolicy::ObservedVarBinary),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn parse_timezone_policy(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::MssqlTimezonePolicy> {
    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "reject" => Ok(delta_funnel::MssqlTimezonePolicy::Reject),
        "datetimeoffset" => Ok(delta_funnel::MssqlTimezonePolicy::DateTimeOffset),
        "normalize_utc_datetime2" => Ok(delta_funnel::MssqlTimezonePolicy::NormalizeUtcDateTime2),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn parse_nanosecond_policy(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::MssqlNanosecondPolicy> {
    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "reject_non_100ns" => Ok(delta_funnel::MssqlNanosecondPolicy::RejectNon100ns),
        "round_to_100ns" => Ok(delta_funnel::MssqlNanosecondPolicy::RoundTo100ns),
        "truncate_to_100ns" => Ok(delta_funnel::MssqlNanosecondPolicy::TruncateTo100ns),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn parse_timestamp_policy(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::MssqlTimestampPolicy> {
    if let Ok(value) = value.cast::<PyDict>() {
        let precision = single_usize_entry(py, value, option_name, "datetime2")?;
        let precision = u8::try_from(precision).map_err(|_| {
            config_py_error(
                py,
                "invalid_option_value",
                format!("`{option_name}` datetime2 precision must be in 0..=7"),
            )
        })?;
        if precision > 7 {
            return Err(config_py_error(
                py,
                "invalid_option_value",
                format!("`{option_name}` datetime2 precision must be in 0..=7"),
            ));
        }
        return Ok(delta_funnel::MssqlTimestampPolicy::DateTime2 { precision });
    }

    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "datetime" => Ok(delta_funnel::MssqlTimestampPolicy::DateTime),
        "datetime2" => Ok(delta_funnel::MssqlTimestampPolicy::DateTime2 { precision: 7 }),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn parse_uint64_policy(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::MssqlUInt64Policy> {
    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "reject" => Ok(delta_funnel::MssqlUInt64Policy::Reject),
        "decimal20_0" => Ok(delta_funnel::MssqlUInt64Policy::Decimal20_0),
        "checked_bigint" => Ok(delta_funnel::MssqlUInt64Policy::CheckedBigInt),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn parse_decimal_policy(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::MssqlDecimalPolicy> {
    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "reject_negative_scale" => Ok(delta_funnel::MssqlDecimalPolicy::RejectNegativeScale),
        "normalize_negative_scale" => Ok(delta_funnel::MssqlDecimalPolicy::NormalizeNegativeScale),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn parse_decimal256_policy(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::MssqlDecimal256Policy> {
    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "checked_downcast" => Ok(delta_funnel::MssqlDecimal256Policy::CheckedDowncast),
        "reject" => Ok(delta_funnel::MssqlDecimal256Policy::Reject),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn parse_float_policy(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::MssqlFloatPolicy> {
    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "reject_non_finite" => Ok(delta_funnel::MssqlFloatPolicy::RejectNonFinite),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn parse_date64_policy(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    option_name: &str,
) -> PyResult<delta_funnel::MssqlDate64Policy> {
    let value = option_string(py, value, option_name)?;
    match value.as_str() {
        "reject_non_midnight" => Ok(delta_funnel::MssqlDate64Policy::RejectNonMidnight),
        "timestamp_datetime2" => Ok(delta_funnel::MssqlDate64Policy::TimestampDateTime2),
        _ => Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` value `{value}`"),
        )),
    }
}

fn single_positive_usize_entry(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
    option_name: &str,
    variant_name: &str,
) -> PyResult<usize> {
    let value = single_usize_entry(py, value, option_name, variant_name)?;
    if value == 0 {
        return Err(config_py_error(
            py,
            "invalid_option_value",
            format!("`{option_name}` bounded length must be at least 1"),
        ));
    }

    Ok(value)
}

fn single_usize_entry(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
    option_name: &str,
    variant_name: &str,
) -> PyResult<usize> {
    if value.len() != 1 {
        return Err(config_py_error(
            py,
            "invalid_option_value",
            format!("`{option_name}` must contain exactly one policy entry"),
        ));
    }

    let Some(raw_value) = value.get_item(variant_name)? else {
        return Err(config_py_error(
            py,
            "invalid_option_value",
            format!("invalid `{option_name}` variant"),
        ));
    };
    usize_option(py, &raw_value, option_name)
}

fn option_name(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<String> {
    value.extract::<String>().map_err(|_| {
        config_py_error(
            py,
            "invalid_option_name",
            "option names must be strings".to_owned(),
        )
    })
}

fn option_entries<'py>(
    py: Python<'py>,
    options: &Bound<'py, PyDict>,
) -> PyResult<Vec<(String, Bound<'py, PyAny>)>> {
    let mut entries = Vec::with_capacity(options.len());
    for (key, value) in options.iter() {
        entries.push((option_name(py, &key)?, value));
    }
    Ok(entries)
}

fn bool_option(py: Python<'_>, value: &Bound<'_, PyAny>, option_name: &str) -> PyResult<bool> {
    value.extract::<bool>().map_err(|_| {
        config_py_error(
            py,
            "invalid_option_value",
            format!("`{option_name}` must be a bool"),
        )
    })
}

fn usize_option(py: Python<'_>, value: &Bound<'_, PyAny>, option_name: &str) -> PyResult<usize> {
    if value.is_instance_of::<PyBool>() {
        return Err(config_py_error(
            py,
            "invalid_option_value",
            format!("`{option_name}` must be a non-negative integer"),
        ));
    }

    value.extract::<usize>().map_err(|_| {
        config_py_error(
            py,
            "invalid_option_value",
            format!("`{option_name}` must be a non-negative integer"),
        )
    })
}

fn option_string(py: Python<'_>, value: &Bound<'_, PyAny>, option_name: &str) -> PyResult<String> {
    value.extract::<String>().map_err(|_| {
        config_py_error(
            py,
            "invalid_option_value",
            format!("`{option_name}` must be a string"),
        )
    })
}

fn unknown_option_error(py: Python<'_>, group: &str, key: &str) -> PyErr {
    config_py_error(
        py,
        "unknown_option",
        format!("unknown {group} option `{key}`"),
    )
}

fn config_py_error(py: Python<'_>, kind: &'static str, message: String) -> PyErr {
    match delta_funnel_py_error(py, "config", kind, message, None) {
        Ok(error) => error,
        Err(error) => error,
    }
}

fn source_config_py_error(py: Python<'_>, kind: &'static str, message: String) -> PyErr {
    match delta_funnel_py_error(py, "source_config", kind, message, None) {
        Ok(error) => error,
        Err(error) => error,
    }
}

#[cfg(test)]
mod tests {
    use super::{PySession, parse_storage_options};
    use crate::{
        deltafunnel,
        progress::{
            adapter_creation_count,
            tests::{ModuleGuard, record_strings},
        },
        test_support::python_state,
    };
    use delta_funnel::{
        DeltaProviderScanExecutionOptions, DryRunScanSummaryMode, MssqlBinaryPolicy,
        MssqlDate64Policy, MssqlDecimal256Policy, MssqlDecimalPolicy, MssqlFloatPolicy,
        MssqlNanosecondPolicy, MssqlSchemaPlanOptions, MssqlStringPolicy, MssqlTableName,
        MssqlTimestampPolicy, MssqlTimezonePolicy, MssqlUInt64Policy, QueryOptions,
        TargetValidationMode, connect_mssql_client_from_ado_string,
    };
    use pyo3::exceptions::{PyAssertionError, PyKeyError, PyTypeError};
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods, PyModule};
    use std::{
        env,
        error::Error,
        fs,
        path::PathBuf,
        sync::{
            Arc, Barrier,
            atomic::{AtomicU64, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
        time::{SystemTime, UNIX_EPOCH},
    };

    const MSSQL_CONNECTION_STRING_ENV: &str = "DELTA_FUNNEL_MSSQL_TEST_CONNECTION_STRING";
    const MSSQL_SCHEMA_ENV: &str = "DELTA_FUNNEL_MSSQL_TEST_SCHEMA";
    static NEXT_MSSQL_TABLE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    type TestResult<T> = Result<T, Box<dyn Error + Send + Sync + 'static>>;

    #[test]
    fn parse_storage_options_preserves_string_keys_and_values() -> PyResult<()> {
        Python::attach(|py| {
            let cases = [
                ("AWS_ACCESS_KEY_ID", "upper-access"),
                ("aws_access_key_id", "lower-access"),
                ("AWS_SECRET_ACCESS_KEY", "upper-secret"),
                ("aws_secret_access_key", "lower-secret"),
                ("AWS_SESSION_TOKEN", "upper-token"),
                ("aws_session_token", "lower-token"),
                ("AWS_REGION", "upper-region"),
                ("aws_region", "lower-region"),
                ("region", "short-region"),
                ("AWS_DEFAULT_REGION", "upper-default-region"),
                ("aws_default_region", "lower-default-region"),
            ];
            let storage_options = PyDict::new(py);
            for (key, value) in cases {
                storage_options.set_item(key, value)?;
            }

            let parsed = parse_storage_options(py, Some(&storage_options))?;

            assert_eq!(parsed.len(), cases.len());
            for (key, value) in cases {
                assert_eq!(parsed.get(key).map(String::as_str), Some(value), "{key}");
            }

            Ok(())
        })
    }

    #[test]
    fn module_exports_session_type() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            let session_type = module.getattr("Session")?;
            assert_eq!(
                session_type.getattr("__name__")?.extract::<String>()?,
                "Session"
            );

            Ok(())
        })
    }

    #[test]
    fn module_does_not_expose_duplicate_dry_run_methods() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            let session_type = module.getattr("Session")?;
            let table_type = module.getattr("Table")?;

            assert!(session_type.getattr("dry_run_all").is_err());
            assert!(session_type.getattr("dry_run_all_to_mssql").is_err());
            assert!(table_type.getattr("dry_run_to_mssql").is_err());

            Ok(())
        })
    }

    #[test]
    fn pyo3_detach_allows_another_thread_to_run_python() -> PyResult<()> {
        Python::attach(|py| {
            let barrier = Arc::new(Barrier::new(2));
            let worker_barrier = Arc::clone(&barrier);
            let (sender, receiver) = mpsc::channel();
            let worker = thread::spawn(move || {
                worker_barrier.wait();
                Python::attach(|py| -> PyResult<()> {
                    let result = py.eval(c"40 + 2", None, None)?.extract::<i32>()?;
                    sender.send(result).expect("send Python worker result");
                    Ok(())
                })
            });

            let result = py.detach(move || {
                barrier.wait();
                receiver
                    .recv_timeout(Duration::from_secs(2))
                    .expect("Python worker should run while GIL is detached")
            });
            assert_eq!(result, 42);
            worker.join().expect("join Python worker")?;

            Ok(())
        })
    }

    #[test]
    fn delta_lake_docstrings_describe_pending_alias_semantics() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            let session_type = module.getattr("Session")?;
            let session_doc = session_type.getattr("__doc__")?.extract::<String>()?;
            assert!(session_doc.contains("uses Rust defaults"));
            assert!(session_doc.contains("execute or dry-run SQL Server outputs"));

            let delta_lake_doc = session_type
                .getattr("delta_lake")?
                .getattr("__doc__")?
                .extract::<String>()?;
            assert!(delta_lake_doc.contains("pending source"));
            assert!(delta_lake_doc.contains("cannot be referenced by SQL"));
            assert!(delta_lake_doc.contains("ignores this call's progress setting"));
            assert!(
                session_type
                    .getattr("delta_lake")?
                    .getattr("__text_signature__")?
                    .extract::<String>()?
                    .contains("progress=None")
            );

            let pending_type = module.getattr("PendingDeltaSource")?;
            let pending_doc = pending_type.getattr("__doc__")?.extract::<String>()?;
            assert!(pending_doc.contains("Unregistered Delta source"));

            let alias_doc = pending_type
                .getattr("alias")?
                .getattr("__doc__")?
                .extract::<String>()?;
            assert!(alias_doc.contains("returns a `Table`"));
            assert!(alias_doc.contains("registration call only"));
            assert!(
                pending_type
                    .getattr("alias")?
                    .getattr("__text_signature__")?
                    .extract::<String>()?
                    .contains("*, progress=None")
            );

            let write_all_doc = session_type
                .getattr("write_all")?
                .getattr("__doc__")?
                .extract::<String>()?;
            assert!(write_all_doc.contains("dry_run=True"));
            assert!(write_all_doc.contains("cache_mode"));
            assert!(write_all_doc.contains("plain Python `dict` report"));

            let table_type = module.getattr("Table")?;
            let table_doc = table_type.getattr("__doc__")?.extract::<String>()?;
            assert!(table_doc.contains("Lazy Delta Funnel table"));

            let to_mssql_doc = table_type
                .getattr("to_mssql")?
                .getattr("__doc__")?
                .extract::<String>()?;
            assert!(to_mssql_doc.contains("default output name"));

            let write_to_mssql_doc = table_type
                .getattr("write_to_mssql")?
                .getattr("__doc__")?
                .extract::<String>()?;
            assert!(write_to_mssql_doc.contains("dry_run=True"));
            assert!(write_to_mssql_doc.contains("plain Python"));

            let output_doc = module
                .getattr("MssqlOutputSpec")?
                .getattr("__doc__")?
                .extract::<String>()?;
            assert!(output_doc.contains("Opaque SQL Server output spec"));

            let error_doc = module
                .getattr("DeltaFunnelError")?
                .getattr("__doc__")?
                .extract::<String>()?;
            assert!(error_doc.contains("phase"));
            assert!(error_doc.contains("kind"));

            Ok(())
        })
    }

    #[test]
    fn default_session_constructs_with_safe_repr() -> PyResult<()> {
        Python::attach(|py| {
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let repr = session.bind(py).repr()?.extract::<String>()?;

            assert_eq!(repr, "deltafunnel.Session(sources=[], derived_tables=[])");
            assert!(!repr.contains("server=tcp"));
            assert!(!repr.contains("password"));
            assert!(!repr.contains("token"));
            assert!(!repr.contains("secret"));

            Ok(())
        })
    }

    #[test]
    fn delta_lake_registers_named_source_and_returns_table() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("orders")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;

            let lazy = session
                .bind(py)
                .call_method("delta_lake", (table.uri(),), Some(&kwargs))?;

            let session = session.bind(py).borrow();
            assert_eq!(session.inner.source_reports().len(), 1);
            assert_eq!(session.inner.source_reports()[0].source_name(), "orders");
            let source_uri = session.inner.source_reports()[0].source_uri().to_owned();
            let snapshot_version = session.inner.source_reports()[0].snapshot_version();
            assert_eq!(
                lazy.repr()?.extract::<String>()?,
                format!(
                    "deltafunnel.Table(id=0, kind=\"delta_source\", name=\"orders\", source_uri={source_uri:?}, snapshot_version={snapshot_version})"
                )
            );
            Ok(())
        })
    }

    #[test]
    fn delta_lake_pending_source_alias_registers_source() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("orders")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("version", 0)?;

            let pending =
                session
                    .bind(py)
                    .call_method("delta_lake", (table.uri(),), Some(&kwargs))?;

            assert_eq!(
                pending.repr()?.extract::<String>()?,
                format!(
                    "deltafunnel.PendingDeltaSource(source_uri={:?}, snapshot_version=0)",
                    table.uri()
                )
            );
            assert!(session.bind(py).borrow().inner.source_reports().is_empty());

            let lazy = pending.call_method("alias", ("orders",), None)?;

            let session = session.bind(py).borrow();
            assert_eq!(session.inner.source_reports().len(), 1);
            assert_eq!(session.inner.source_reports()[0].snapshot_version(), 0);
            let source_uri = session.inner.source_reports()[0].source_uri().to_owned();
            assert_eq!(
                lazy.repr()?.extract::<String>()?,
                format!(
                    "deltafunnel.Table(id=0, kind=\"delta_source\", name=\"orders\", source_uri={source_uri:?}, snapshot_version=0)"
                )
            );
            Ok(())
        })
    }

    #[test]
    fn pending_delta_source_defers_progress_selection_until_alias() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("pending-progress")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let initial_count = adapter_creation_count();
            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", true)?;

            let pending =
                session
                    .bind(py)
                    .call_method("delta_lake", (table.uri(),), Some(&kwargs))?;

            assert_eq!(adapter_creation_count(), initial_count);
            assert!(session.bind(py).borrow().inner.source_reports().is_empty());

            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", false)?;
            pending.call_method("alias", ("orders",), Some(&kwargs))?;

            assert_eq!(adapter_creation_count(), initial_count);
            assert_eq!(session.bind(py).borrow().inner.source_reports().len(), 1);
            Ok(())
        })
    }

    #[test]
    fn named_and_pending_alias_registration_create_at_most_one_adapter() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("registration-adapters")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let initial_count = adapter_creation_count();
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;
            kwargs.set_item("progress", true)?;

            session
                .bind(py)
                .call_method("delta_lake", (table.uri(),), Some(&kwargs))?;
            assert_eq!(adapter_creation_count(), initial_count + 1);

            let pending = session
                .bind(py)
                .call_method("delta_lake", (table.uri(),), None)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", true)?;
            pending.call_method("alias", ("customers",), Some(&kwargs))?;

            assert_eq!(adapter_creation_count(), initial_count + 2);
            assert_eq!(session.bind(py).borrow().inner.source_reports().len(), 2);
            Ok(())
        })
    }

    #[test]
    fn delta_registration_entry_points_share_one_rich_lifecycle() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let table = DeltaLogFixture::new("registration-lifecycle")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let (_guard, records) = ModuleGuard::install(py, true, false)?;
            let progress_kwargs = PyDict::new(py);
            progress_kwargs.set_item("progress", true)?;
            let named_kwargs = PyDict::new(py);
            named_kwargs.set_item("name", "orders")?;
            named_kwargs.set_item("progress", true)?;

            session
                .bind(py)
                .call_method("delta_lake", (table.uri(),), Some(&named_kwargs))?;
            let immediate_calls = record_strings(records.bind(py), "call")?;
            let immediate_descriptions = registration_update_descriptions(records.bind(py))?;
            records.bind(py).call_method0("clear")?;

            let pending = session.bind(py).call_method(
                "delta_lake",
                (table.uri(),),
                Some(&progress_kwargs),
            )?;
            assert!(records.bind(py).is_empty());
            pending.call_method("alias", ("customers",), Some(&progress_kwargs))?;

            assert_eq!(record_strings(records.bind(py), "call")?, immediate_calls);
            assert_eq!(
                registration_update_descriptions(records.bind(py))?,
                immediate_descriptions
            );
            assert_eq!(
                immediate_descriptions,
                [
                    "Loading Delta metadata",
                    "Validating Delta protocol",
                    "Preparing Delta provider",
                    "Registering Delta source",
                    "Completed",
                ]
            );
            let add_task = records
                .bind(py)
                .iter()
                .find(|record| {
                    record
                        .get_item("call")
                        .and_then(|call| call.extract::<String>())
                        .is_ok_and(|call| call == "add_task")
                })
                .ok_or_else(|| PyAssertionError::new_err("missing Rich add_task call"))?;
            assert_eq!(
                add_task.get_item("description")?.extract::<String>()?,
                "Loading Delta source"
            );
            let rendered = records.bind(py).repr()?.extract::<String>()?;
            assert!(!rendered.contains(table.uri().as_str()));
            assert!(!rendered.contains("orders"));
            assert!(!rendered.contains("customers"));
            Ok(())
        })
    }

    #[test]
    fn delta_registration_entry_points_use_shared_progress_modes() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let table = DeltaLogFixture::new("registration-modes")?;
            let cases = [
                (None, true, true),
                (Some(None), true, true),
                (None, false, false),
                (Some(Some(true)), false, true),
                (Some(Some(false)), true, false),
            ];

            for use_pending_alias in [false, true] {
                for (progress, interactive, should_render) in cases {
                    let session =
                        Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
                    let (guard, records) = ModuleGuard::install(py, interactive, false)?;

                    if use_pending_alias {
                        let pending =
                            session
                                .bind(py)
                                .call_method("delta_lake", (table.uri(),), None)?;
                        let kwargs = PyDict::new(py);
                        if let Some(progress) = progress {
                            kwargs.set_item("progress", progress)?;
                        }
                        pending.call_method("alias", ("orders",), Some(&kwargs))?;
                    } else {
                        let kwargs = PyDict::new(py);
                        kwargs.set_item("name", "orders")?;
                        if let Some(progress) = progress {
                            kwargs.set_item("progress", progress)?;
                        }
                        session.bind(py).call_method(
                            "delta_lake",
                            (table.uri(),),
                            Some(&kwargs),
                        )?;
                    }

                    let calls = record_strings(records.bind(py), "call")?;
                    assert_eq!(calls.iter().any(|call| call == "progress"), should_render);
                    assert_eq!(session.bind(py).borrow().inner.source_reports().len(), 1);
                    drop(guard);
                }
            }
            Ok(())
        })
    }

    fn registration_update_descriptions(records: &Bound<'_, PyList>) -> PyResult<Vec<String>> {
        let mut descriptions = Vec::new();
        for record in records.iter() {
            if record.get_item("call")?.extract::<String>()? == "update"
                && let Ok(description) = record.get_item("description")
            {
                descriptions.push(description.extract::<String>()?);
            }
        }
        Ok(descriptions)
    }

    #[test]
    fn delta_lake_maps_fixed_version() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("orders")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;
            kwargs.set_item("version", 0)?;

            session
                .bind(py)
                .call_method("delta_lake", (table.uri(),), Some(&kwargs))?;

            assert_eq!(
                session.bind(py).borrow().inner.source_reports()[0].snapshot_version(),
                0
            );
            Ok(())
        })
    }

    #[test]
    fn delta_lake_registers_multiple_distinct_sources() -> PyResult<()> {
        Python::attach(|py| {
            let orders = DeltaLogFixture::new("orders")?;
            let customers = DeltaLogFixture::new("customers")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;

            session
                .bind(py)
                .call_method("delta_lake", (orders.uri(),), Some(&kwargs))?;
            session
                .bind(py)
                .call_method("delta_lake", (customers.uri(),), None)?
                .call_method("alias", ("customers",), None)?;

            let session = session.bind(py).borrow();
            let source_names = session
                .inner
                .source_reports()
                .into_iter()
                .map(|report| report.source_name().to_owned())
                .collect::<Vec<_>>();
            assert_eq!(
                source_names,
                vec!["orders".to_owned(), "customers".to_owned()]
            );
            Ok(())
        })
    }

    #[test]
    fn delta_lake_preserves_duplicate_alias_error() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("orders")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;
            session
                .bind(py)
                .call_method("delta_lake", (table.uri(),), Some(&kwargs))?;
            kwargs.set_item("name", "ORDERS")?;

            let error =
                match session
                    .bind(py)
                    .call_method("delta_lake", (table.uri(),), Some(&kwargs))
                {
                    Ok(_) => {
                        return Err(PyAssertionError::new_err("expected duplicate alias error"));
                    }
                    Err(error) => error,
                };

            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "source_config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "duplicate_source_name"
            );
            Ok(())
        })
    }

    #[test]
    fn delta_lake_rejects_invalid_alias_and_missing_uri() -> PyResult<()> {
        Python::attach(|py| {
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "select")?;

            let error =
                match session
                    .bind(py)
                    .call_method("delta_lake", ("somewhere",), Some(&kwargs))
                {
                    Ok(_) => return Err(PyAssertionError::new_err("expected invalid alias error")),
                    Err(error) => error,
                };
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "source_config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_source_name"
            );

            let error = match session.bind(py).call_method("delta_lake", (), None) {
                Ok(_) => return Err(PyAssertionError::new_err("expected missing uri error")),
                Err(error) => error,
            };
            assert!(error.value(py).is_instance_of::<PyTypeError>());
            assert!(session.bind(py).borrow().inner.source_reports().is_empty());
            Ok(())
        })
    }

    #[test]
    fn delta_lake_rejects_invalid_source_args_before_loading() -> PyResult<()> {
        Python::attach(|py| {
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let initial_adapter_count = adapter_creation_count();

            let cases = vec![
                ("", None, "invalid_source_uri"),
                (
                    "somewhere",
                    Some((-1i64).into_pyobject(py)?.into_any()),
                    "invalid_version",
                ),
                (
                    "somewhere",
                    Some(true.into_pyobject(py)?.to_owned().into_any()),
                    "invalid_version",
                ),
            ];
            for (uri, version, expected_kind) in cases {
                let kwargs = PyDict::new(py);
                kwargs.set_item("name", "orders")?;
                kwargs.set_item("progress", true)?;
                if let Some(version) = version {
                    kwargs.set_item("version", version)?;
                }
                let error = match session.bind(py).call_method(
                    "delta_lake",
                    (uri.to_owned(),),
                    Some(&kwargs),
                ) {
                    Ok(_) => return Err(PyAssertionError::new_err("expected source arg error")),
                    Err(error) => error,
                };

                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "source_config"
                );
                assert_eq!(
                    error.value(py).getattr("kind")?.extract::<String>()?,
                    expected_kind
                );
                assert!(session.bind(py).borrow().inner.source_reports().is_empty());
                assert_eq!(adapter_creation_count(), initial_adapter_count);
            }

            let storage_options = PyDict::new(py);
            storage_options.set_item("token", 7)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;
            kwargs.set_item("progress", true)?;
            kwargs.set_item("storage_options", storage_options)?;
            let error =
                match session
                    .bind(py)
                    .call_method("delta_lake", ("somewhere",), Some(&kwargs))
                {
                    Ok(_) => {
                        return Err(PyAssertionError::new_err("expected storage option error"));
                    }
                    Err(error) => error,
                };
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "source_config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_storage_options"
            );
            assert!(session.bind(py).borrow().inner.source_reports().is_empty());
            assert_eq!(adapter_creation_count(), initial_adapter_count);

            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", "always")?;
            let error = session
                .bind(py)
                .call_method("delta_lake", ("somewhere",), Some(&kwargs))
                .unwrap_err();
            assert!(error.is_instance_of::<PyTypeError>(py));
            assert_eq!(adapter_creation_count(), initial_adapter_count);

            Ok(())
        })
    }

    #[test]
    fn delta_lake_snapshot_load_error_exposes_cause_context() -> PyResult<()> {
        Python::attach(|py| {
            let dir = env_unique_path("empty-delta-table")?;
            fs::create_dir_all(&dir).map_err(io_py_error)?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;

            let error = match session.bind(py).call_method(
                "delta_lake",
                (dir.to_string_lossy().to_string(),),
                Some(&kwargs),
            ) {
                Ok(_) => return Err(PyAssertionError::new_err("expected snapshot load error")),
                Err(error) => error,
            };
            let _ = fs::remove_dir_all(&dir);

            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "delta_source"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "delta_snapshot_load"
            );
            let message = error.value(py).getattr("message")?.extract::<String>()?;
            assert!(message.contains("snapshot could not be loaded"));
            let context = error.value(py).getattr("context")?;
            let cause = context.get_item("cause")?.extract::<String>()?;
            assert!(cause.contains("snapshot could not be loaded"));

            Ok(())
        })
    }

    #[test]
    fn delta_lake_repr_and_source_config_errors_do_not_expose_source_secrets() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("orders")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let storage_options = PyDict::new(py);
            storage_options.set_item("authorization", "super-secret-token")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("storage_options", storage_options)?;

            let pending = session.bind(py).call_method(
                "delta_lake",
                (table.uri_with_secret_parts(),),
                Some(&kwargs),
            )?;
            let pending_repr = pending.repr()?.extract::<String>()?;
            assert!(pending_repr.contains(table.uri().as_str()));
            assert!(!pending_repr.contains("super-secret"));
            assert!(!pending_repr.contains("debug-secret"));
            assert!(!pending_repr.contains("?token="));

            kwargs.set_item("name", "select")?;
            let error = match session.bind(py).call_method(
                "delta_lake",
                (table.uri_with_secret_parts(),),
                Some(&kwargs),
            ) {
                Ok(_) => return Err(PyAssertionError::new_err("expected invalid alias error")),
                Err(error) => error,
            };
            let message = error.value(py).getattr("message")?.extract::<String>()?;
            assert!(!message.contains("super-secret"));
            assert!(!message.contains("debug-secret"));
            assert!(session.bind(py).borrow().inner.source_reports().is_empty());

            Ok(())
        })
    }

    #[test]
    fn table_from_sql_returns_lazy_table_over_registered_source() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("orders")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;
            session
                .bind(py)
                .call_method("delta_lake", (table.uri(),), Some(&kwargs))?;

            let derived =
                session
                    .bind(py)
                    .call_method("table_from_sql", ("select id from orders",), None)?;

            assert_eq!(
                derived.repr()?.extract::<String>()?,
                "deltafunnel.Table(id=1, kind=\"derived_sql\", name=\"table_1\")"
            );
            assert!(!derived.repr()?.extract::<String>()?.contains("select id"));
            Ok(())
        })
    }

    #[test]
    fn table_alias_registers_derived_table_for_later_sql() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("orders")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;
            session
                .bind(py)
                .call_method("delta_lake", (table.uri(),), Some(&kwargs))?;

            let derived =
                session
                    .bind(py)
                    .call_method("table_from_sql", ("select id from orders",), None)?;
            let aliased = derived.call_method("alias", ("recent_orders",), None)?;
            let downstream = session.bind(py).call_method(
                "table_from_sql",
                ("select id from recent_orders",),
                None,
            )?;

            assert_eq!(
                aliased.repr()?.extract::<String>()?,
                "deltafunnel.Table(id=1, kind=\"derived_sql\", name=\"recent_orders\")"
            );
            assert_eq!(
                downstream.repr()?.extract::<String>()?,
                "deltafunnel.Table(id=2, kind=\"derived_sql\", name=\"table_2\")"
            );
            assert_eq!(
                session.bind(py).repr()?.extract::<String>()?,
                "deltafunnel.Session(sources=[\"orders\"], derived_tables=[\"recent_orders\"])"
            );
            Ok(())
        })
    }

    #[test]
    fn empty_write_all_validates_progress_without_creating_an_adapter() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let outputs = PyList::empty(py);
            let initial_count = adapter_creation_count();

            for dry_run in [false, true] {
                let kwargs = PyDict::new(py);
                kwargs.set_item("dry_run", dry_run)?;
                kwargs.set_item("progress", true)?;
                session.call_method("write_all", (&outputs,), Some(&kwargs))?;
            }
            assert_eq!(adapter_creation_count(), initial_count);

            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", "always")?;
            let error = session
                .call_method("write_all", (&outputs,), Some(&kwargs))
                .unwrap_err();
            assert!(error.is_instance_of::<PyTypeError>(py));
            assert_eq!(adapter_creation_count(), initial_count);

            let kwargs = PyDict::new(py);
            kwargs.set_item("dry_run", true)?;
            kwargs.set_item("progress", true)?;
            kwargs.set_item("options", PyDict::new(py))?;
            let error = session
                .call_method("write_all", (&outputs,), Some(&kwargs))
                .unwrap_err();
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_option_value"
            );
            assert_eq!(adapter_creation_count(), initial_count);
            Ok(())
        })
    }

    #[test]
    fn table_alias_preserves_alias_validation_errors() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("orders")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;
            session
                .bind(py)
                .call_method("delta_lake", (table.uri(),), Some(&kwargs))?;

            let derived =
                session
                    .bind(py)
                    .call_method("table_from_sql", ("select id from orders",), None)?;
            let invalid = match derived.call_method("alias", ("select",), None) {
                Ok(_) => return Err(PyAssertionError::new_err("expected invalid alias error")),
                Err(error) => error,
            };
            assert_eq!(
                invalid.value(py).getattr("phase")?.extract::<String>()?,
                "source_config"
            );
            assert_eq!(
                invalid.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_source_name"
            );

            let duplicate = match derived.call_method("alias", ("orders",), None) {
                Ok(_) => return Err(PyAssertionError::new_err("expected duplicate alias error")),
                Err(error) => error,
            };
            assert_eq!(
                duplicate.value(py).getattr("phase")?.extract::<String>()?,
                "source_config"
            );
            assert_eq!(
                duplicate.value(py).getattr("kind")?.extract::<String>()?,
                "duplicate_source_name"
            );
            Ok(())
        })
    }

    #[test]
    fn write_all_dry_run_returns_workflow_report_dict() -> PyResult<()> {
        Python::attach(|py| {
            let session = Py::new(
                py,
                PySession::new(
                    py,
                    Some("server=tcp:sql.example.com;password=secret-token".to_owned()),
                    None,
                    None,
                    None,
                    None,
                    None,
                )?,
            )?;
            let west = session
                .bind(py)
                .call_method("table_from_sql", ("select 1 as id",), None)?;
            let east = session
                .bind(py)
                .call_method("table_from_sql", ("select 2 as id",), None)?;
            let west_spec = west.call_method("to_mssql", (), Some(&mssql_kwargs(py, "west")?))?;
            let east_spec = east.call_method("to_mssql", (), Some(&mssql_kwargs(py, "east")?))?;
            let outputs = PyList::new(py, [&west_spec, &east_spec])?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("dry_run", true)?;

            let report = session
                .bind(py)
                .call_method("write_all", (outputs,), Some(&kwargs))?;
            let report_repr = report.repr()?.extract::<String>()?;
            assert!(!report_repr.contains("secret-token"));
            assert!(!report_repr.contains("password=secret-token"));

            let report = report.cast::<PyDict>()?;
            let outputs = required_item(report, "outputs")?;
            let outputs = outputs.cast::<PyList>()?;
            let dry_run = required_item(report, "dry_run")?;
            let dry_run = dry_run.cast::<PyDict>()?;

            assert_eq!(
                required_item(report, "run_mode")?.extract::<String>()?,
                "dry_run"
            );
            assert_eq!(required_item(report, "output_count")?.extract::<u64>()?, 2);
            assert_eq!(outputs.len(), 2);
            assert_eq!(
                required_item(outputs.get_item(0)?.cast::<PyDict>()?, "output_name")?
                    .extract::<String>()?,
                "west"
            );
            assert_eq!(
                required_item(outputs.get_item(1)?.cast::<PyDict>()?, "output_name")?
                    .extract::<String>()?,
                "east"
            );
            assert!(!required_item(dry_run, "sql_server_contacted")?.extract::<bool>()?);
            assert!(!required_item(dry_run, "row_production_started")?.extract::<bool>()?);
            assert!(!required_item(dry_run, "table_lifecycle_started")?.extract::<bool>()?);
            assert!(!required_item(dry_run, "bulk_writer_started")?.extract::<bool>()?);

            Ok(())
        })
    }

    #[test]
    fn write_all_dry_run_report_includes_source_phase_and_validation_facts() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("orders")?;
            let session = Py::new(
                py,
                PySession::new(
                    py,
                    Some("server=tcp:sql.example.com;password=secret-token".to_owned()),
                    None,
                    None,
                    None,
                    None,
                    None,
                )?,
            )?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;
            session
                .bind(py)
                .call_method("delta_lake", (table.uri(),), Some(&kwargs))?;
            let derived =
                session
                    .bind(py)
                    .call_method("table_from_sql", ("select id from orders",), None)?;
            let spec =
                derived.call_method("to_mssql", (), Some(&mssql_kwargs(py, "orders_sink")?))?;
            let outputs = PyList::new(py, [&spec])?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("dry_run", true)?;

            let report = session
                .bind(py)
                .call_method("write_all", (outputs,), Some(&kwargs))?;
            let report = report.cast::<PyDict>()?;
            let sources = required_item(report, "sources")?;
            let sources = sources.cast::<PyList>()?;
            let outputs = required_item(report, "outputs")?;
            let outputs = outputs.cast::<PyList>()?;
            let output = outputs.get_item(0)?;
            let output = output.cast::<PyDict>()?;
            let validation_status = required_item(output, "validation_status")?;
            let validation_status = validation_status.cast::<PyDict>()?;
            let phase_timings = required_item(output, "phase_timings")?;
            let phase_timings = phase_timings.cast::<PyList>()?;
            let target_table = required_item(output, "target_table")?;
            let target_table = target_table.cast::<PyDict>()?;

            assert_eq!(sources.len(), 1);
            assert_eq!(
                required_item(sources.get_item(0)?.cast::<PyDict>()?, "source_name")?
                    .extract::<String>()?,
                "orders"
            );
            assert_eq!(
                required_item(output, "source_usage_status")?.extract::<String>()?,
                "used"
            );
            assert_eq!(
                required_item(target_table, "table")?.extract::<String>()?,
                "orders_sink"
            );
            assert_eq!(
                required_item(validation_status, "kind")?.extract::<String>()?,
                "skipped"
            );
            assert_eq!(
                required_item(validation_status, "reason")?.extract::<String>()?,
                "dry_run"
            );
            assert!(!phase_timings.is_empty());

            Ok(())
        })
    }

    #[test]
    fn write_all_execute_mode_uses_rust_missing_connection_guard() -> PyResult<()> {
        Python::attach(|py| {
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let table =
                session
                    .bind(py)
                    .call_method("table_from_sql", ("select 1 as id",), None)?;
            let spec = table.call_method("to_mssql", (), Some(&mssql_kwargs(py, "orders")?))?;
            let outputs = PyList::new(py, [&spec])?;
            let kwargs = PyDict::new(py);

            let error = session
                .bind(py)
                .call_method("write_all", (&outputs,), Some(&kwargs))
                .unwrap_err();
            assert_missing_connection_error(py, &error)?;

            kwargs.set_item("dry_run", false)?;
            let error = session
                .bind(py)
                .call_method("write_all", (&outputs,), Some(&kwargs))
                .unwrap_err();
            assert_missing_connection_error(py, &error)?;

            Ok(())
        })
    }

    #[test]
    #[ignore = "runs through cargo xtask sqlserver-test"]
    fn write_all_execute_writes_default_and_override_connections_when_configured() -> TestResult<()>
    {
        let Some(config) = MssqlIntegrationConfig::from_env() else {
            return Ok(());
        };
        let west_table = unique_mssql_table_name(&config.schema)?;
        let east_table = unique_mssql_table_name(&config.schema)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let tables = [&west_table, &east_table];

        runtime.block_on(drop_tables(&config, &tables))?;
        let write_result = Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session_kwargs = PyDict::new(py);
            session_kwargs.set_item(
                "default_mssql_connection_string",
                config.connection_string.as_str(),
            )?;
            let session = module.getattr("Session")?.call((), Some(&session_kwargs))?;
            let big = session.call_method(
                "table_from_sql",
                ("\
select cast(301 as bigint) as order_id
union all select cast(302 as bigint) as order_id",),
                None,
            )?;
            let _big = big.call_method("alias", ("big_orders",), None)?;
            let west = session.call_method(
                "table_from_sql",
                ("select order_id from big_orders where order_id = 301",),
                None,
            )?;
            let east = session.call_method(
                "table_from_sql",
                ("select order_id from big_orders where order_id = 302",),
                None,
            )?;

            let west_kwargs = PyDict::new(py);
            west_kwargs.set_item("schema", config.schema.as_str())?;
            west_kwargs.set_item("table", west_table.table().as_str())?;
            west_kwargs.set_item("load_mode", "create_and_load")?;
            let west_spec = west.call_method("to_mssql", (), Some(&west_kwargs))?;
            let east_kwargs = PyDict::new(py);
            east_kwargs.set_item("schema", config.schema.as_str())?;
            east_kwargs.set_item("table", east_table.table().as_str())?;
            east_kwargs.set_item("load_mode", "create_and_load")?;
            east_kwargs.set_item("name", "east_orders")?;
            east_kwargs.set_item("connection_string", config.connection_string.as_str())?;
            let east_spec = east.call_method("to_mssql", (), Some(&east_kwargs))?;
            let outputs = PyList::new(py, [&west_spec, &east_spec])?;

            let report = session.call_method("write_all", (outputs,), None)?;
            let report_repr = report.repr()?.extract::<String>()?;
            assert!(!report_repr.contains(config.connection_string.as_str()));
            let report = report.cast::<PyDict>()?;
            assert_eq!(required_item(report, "output_count")?.extract::<u64>()?, 2);
            assert!(required_item(report, "all_succeeded")?.extract::<bool>()?);
            assert_eq!(
                required_item(report, "succeeded_count")?.extract::<u64>()?,
                2
            );
            assert_eq!(required_item(report, "failed_count")?.extract::<u64>()?, 0);
            assert_eq!(required_item(report, "skipped_count")?.extract::<u64>()?, 0);
            assert!(
                !required_item(report, "phase_timings")?
                    .cast::<PyList>()?
                    .is_empty()
            );
            assert!(
                required_item(report, "sources")?
                    .cast::<PyList>()?
                    .is_empty()
            );
            let cache = required_item(report, "cache")?;
            let cache = cache.cast::<PyDict>()?;
            assert_eq!(
                required_item(cache, "kind")?.extract::<String>()?,
                "cache_aliases"
            );
            let aliases = required_item(cache, "aliases")?;
            let aliases = aliases.cast::<PyList>()?;
            assert_eq!(aliases.len(), 1);
            let alias = aliases.get_item(0)?;
            let alias = alias.cast::<PyDict>()?;
            assert_eq!(
                required_item(alias, "alias")?.extract::<String>()?,
                "big_orders"
            );
            let output_indexes = required_item(alias, "output_indexes")?;
            let output_indexes = output_indexes.cast::<PyList>()?;
            assert_eq!(output_indexes.len(), 2);
            assert_eq!(output_indexes.get_item(0)?.extract::<u64>()?, 0);
            assert_eq!(output_indexes.get_item(1)?.extract::<u64>()?, 1);

            let workflow = required_item(report, "workflow")?;
            let workflow = workflow.cast::<PyDict>()?;
            assert_eq!(
                required_item(workflow, "output_count")?.extract::<u64>()?,
                2
            );
            assert!(required_item(workflow, "all_succeeded")?.extract::<bool>()?);
            let outputs = required_item(workflow, "outputs")?;
            let outputs = outputs.cast::<PyList>()?;
            assert_eq!(outputs.len(), 2);

            let west_output = outputs.get_item(0)?;
            let west_output = west_output.cast::<PyDict>()?;
            assert_succeeded_output(
                west_output,
                west_table.table().as_str(),
                "context_default",
                1,
            )?;
            let east_output = outputs.get_item(1)?;
            let east_output = east_output.cast::<PyDict>()?;
            assert_succeeded_output(east_output, "east_orders", "target_override", 1)?;

            Ok::<(), PyErr>(())
        });
        let cleanup_result = runtime.block_on(drop_tables(&config, &tables));

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
    fn write_all_execute_writes_reports_failed_and_skipped_outputs_when_configured()
    -> TestResult<()> {
        let Some(config) = MssqlIntegrationConfig::from_env() else {
            return Ok(());
        };
        let first_table = unique_mssql_table_name(&config.schema)?;
        let failing_table = unique_mssql_table_name(&config.schema)?;
        let skipped_table = unique_mssql_table_name(&config.schema)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let tables = [&first_table, &failing_table, &skipped_table];

        runtime.block_on(drop_tables(&config, &tables))?;
        let write_result = Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session_kwargs = PyDict::new(py);
            session_kwargs.set_item(
                "default_mssql_connection_string",
                config.connection_string.as_str(),
            )?;
            let session = module.getattr("Session")?.call((), Some(&session_kwargs))?;
            let first = session.call_method(
                "table_from_sql",
                ("select cast(501 as bigint) as id",),
                None,
            )?;
            let failing = session.call_method(
                "table_from_sql",
                ("select cast(601 as bigint) as id",),
                None,
            )?;
            let skipped = session.call_method(
                "table_from_sql",
                ("select cast(701 as bigint) as id",),
                None,
            )?;

            let first_kwargs = PyDict::new(py);
            first_kwargs.set_item("schema", config.schema.as_str())?;
            first_kwargs.set_item("table", first_table.table().as_str())?;
            first_kwargs.set_item("load_mode", "create_and_load")?;
            first_kwargs.set_item("name", "first_output")?;
            let first_spec = first.call_method("to_mssql", (), Some(&first_kwargs))?;
            let failing_kwargs = PyDict::new(py);
            failing_kwargs.set_item("schema", config.schema.as_str())?;
            failing_kwargs.set_item("table", failing_table.table().as_str())?;
            failing_kwargs.set_item("load_mode", "append_existing")?;
            failing_kwargs.set_item("name", "failing_output")?;
            let failing_spec = failing.call_method("to_mssql", (), Some(&failing_kwargs))?;
            let skipped_kwargs = PyDict::new(py);
            skipped_kwargs.set_item("schema", config.schema.as_str())?;
            skipped_kwargs.set_item("table", skipped_table.table().as_str())?;
            skipped_kwargs.set_item("load_mode", "create_and_load")?;
            skipped_kwargs.set_item("name", "skipped_output")?;
            let skipped_spec = skipped.call_method("to_mssql", (), Some(&skipped_kwargs))?;
            let outputs = PyList::new(py, [&first_spec, &failing_spec, &skipped_spec])?;
            let options = PyDict::new(py);
            options.set_item("cache_mode", "disabled")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("options", options)?;
            kwargs.set_item("progress", true)?;
            kwargs.set_item("dry_run", false)?;

            let report = session.call_method("write_all", (outputs,), Some(&kwargs))?;
            let report = report.cast::<PyDict>()?;
            assert_eq!(required_item(report, "output_count")?.extract::<u64>()?, 3);
            assert!(!required_item(report, "all_succeeded")?.extract::<bool>()?);
            assert_eq!(
                required_item(report, "succeeded_count")?.extract::<u64>()?,
                1
            );
            assert_eq!(required_item(report, "failed_count")?.extract::<u64>()?, 1);
            assert_eq!(required_item(report, "skipped_count")?.extract::<u64>()?, 1);
            let cache = required_item(report, "cache")?;
            let cache = cache.cast::<PyDict>()?;
            assert_eq!(
                required_item(cache, "kind")?.extract::<String>()?,
                "disabled"
            );

            let workflow = required_item(report, "workflow")?;
            let workflow = workflow.cast::<PyDict>()?;
            let outputs = required_item(workflow, "outputs")?;
            let outputs = outputs.cast::<PyList>()?;
            assert_eq!(outputs.len(), 3);
            assert_succeeded_output(
                outputs.get_item(0)?.cast::<PyDict>()?,
                "first_output",
                "context_default",
                1,
            )?;

            let failed = outputs.get_item(1)?;
            let failed = failed.cast::<PyDict>()?;
            assert_eq!(
                required_item(failed, "kind")?.extract::<String>()?,
                "failed"
            );
            assert_eq!(
                required_item(failed, "output_name")?.extract::<String>()?,
                "failing_output"
            );
            required_item(failed, "failure")?;

            let skipped = outputs.get_item(2)?;
            let skipped = skipped.cast::<PyDict>()?;
            assert_eq!(
                required_item(skipped, "kind")?.extract::<String>()?,
                "skipped"
            );
            assert_eq!(
                required_item(skipped, "output_name")?.extract::<String>()?,
                "skipped_output"
            );
            let skipped_report = required_item(skipped, "skipped")?;
            let skipped_report = skipped_report.cast::<PyDict>()?;
            let reason = required_item(skipped_report, "reason")?;
            let reason = reason.cast::<PyDict>()?;
            assert_eq!(
                required_item(reason, "kind")?.extract::<String>()?,
                "previous_output_failed"
            );
            assert_eq!(
                required_item(reason, "failed_output_name")?.extract::<String>()?,
                "failing_output"
            );

            Ok::<(), PyErr>(())
        });
        let cleanup_result = runtime.block_on(drop_tables(&config, &tables));

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
    fn write_all_execute_writes_replace_output_when_configured() -> TestResult<()> {
        let Some(config) = MssqlIntegrationConfig::from_env() else {
            return Ok(());
        };
        let table = unique_mssql_table_name(&config.schema)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let tables = [&table];

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
select cast(901 as bigint) as order_id
union all select cast(902 as bigint) as order_id",),
                None,
            )?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("schema", config.schema.as_str())?;
            kwargs.set_item("table", table.table().as_str())?;
            kwargs.set_item("load_mode", "replace")?;
            kwargs.set_item("name", "replace_output")?;
            let spec = source.call_method("to_mssql", (), Some(&kwargs))?;
            let outputs = PyList::new(py, [&spec])?;

            let report = session.call_method("write_all", (outputs,), None)?;
            let report = report.cast::<PyDict>()?;
            assert!(required_item(report, "all_succeeded")?.extract::<bool>()?);

            let workflow = required_item(report, "workflow")?;
            let workflow = workflow.cast::<PyDict>()?;
            let outputs = required_item(workflow, "outputs")?;
            let outputs = outputs.cast::<PyList>()?;
            assert_eq!(outputs.len(), 1);
            let output = outputs.get_item(0)?;
            let output = output.cast::<PyDict>()?;
            assert_succeeded_output(output, "replace_output", "context_default", 2)?;
            assert_eq!(
                required_item(output, "load_mode")?.extract::<String>()?,
                "replace"
            );
            let output_report = required_item(output, "report")?;
            let output_report = output_report.cast::<PyDict>()?;
            assert!(!required_item(output_report, "partial_write_possible")?.extract::<bool>()?);

            Ok::<(), PyErr>(())
        });
        let result = match write_result {
            Ok(()) => runtime.block_on(assert_replace_target_rows(&config, &table, 901, 902)),
            Err(write_error) => Err(Box::new(write_error) as Box<dyn Error + Send + Sync>),
        };
        let cleanup_result = runtime.block_on(drop_tables(&config, &tables));

        match (result, cleanup_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(write_error), Ok(())) => Err(write_error),
            (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
            (Err(write_error), Err(cleanup_error)) => {
                Err(format!("write failed: {write_error}; cleanup failed: {cleanup_error}").into())
            }
        }
    }

    #[test]
    #[ignore = "runs through cargo xtask sqlserver-test"]
    fn write_all_execute_writes_replace_empty_output_to_missing_target() -> TestResult<()> {
        let Some(config) = MssqlIntegrationConfig::from_env() else {
            return Ok(());
        };
        let table = unique_mssql_table_name(&config.schema)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let tables = [&table];

        runtime.block_on(drop_tables(&config, &tables))?;
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
                ("select cast(901 as bigint) as order_id where 1 = 0",),
                None,
            )?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("schema", config.schema.as_str())?;
            kwargs.set_item("table", table.table().as_str())?;
            kwargs.set_item("load_mode", "replace")?;
            kwargs.set_item("name", "empty_replace_output")?;
            let spec = source.call_method("to_mssql", (), Some(&kwargs))?;
            let outputs = PyList::new(py, [&spec])?;

            let report = session.call_method("write_all", (outputs,), None)?;
            let report = report.cast::<PyDict>()?;
            assert!(required_item(report, "all_succeeded")?.extract::<bool>()?);

            let workflow = required_item(report, "workflow")?;
            let workflow = workflow.cast::<PyDict>()?;
            let outputs = required_item(workflow, "outputs")?;
            let outputs = outputs.cast::<PyList>()?;
            assert_eq!(outputs.len(), 1);
            let output = outputs.get_item(0)?;
            let output = output.cast::<PyDict>()?;
            assert_succeeded_output(output, "empty_replace_output", "context_default", 0)?;
            assert_eq!(
                required_item(output, "load_mode")?.extract::<String>()?,
                "replace"
            );

            Ok::<(), PyErr>(())
        });
        let result = match write_result {
            Ok(()) => runtime.block_on(assert_target_exists_with_zero_rows(&config, &table)),
            Err(write_error) => Err(Box::new(write_error) as Box<dyn Error + Send + Sync>),
        };
        let cleanup_result = runtime.block_on(drop_tables(&config, &tables));

        match (result, cleanup_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(write_error), Ok(())) => Err(write_error),
            (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
            (Err(write_error), Err(cleanup_error)) => {
                Err(format!("write failed: {write_error}; cleanup failed: {cleanup_error}").into())
            }
        }
    }

    #[test]
    fn write_all_dry_run_rejects_missing_connection() -> PyResult<()> {
        Python::attach(|py| {
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let table =
                session
                    .bind(py)
                    .call_method("table_from_sql", ("select 1 as id",), None)?;
            let spec = table.call_method("to_mssql", (), Some(&mssql_kwargs(py, "orders")?))?;
            let outputs = PyList::new(py, [&spec])?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("dry_run", true)?;

            let error = session
                .bind(py)
                .call_method("write_all", (outputs,), Some(&kwargs))
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
    fn write_all_rejects_duplicate_output_names_before_stream_setup() -> PyResult<()> {
        Python::attach(|py| {
            let session = Py::new(
                py,
                PySession::new(
                    py,
                    Some("server=tcp:sql.example.com;password=secret-token".to_owned()),
                    None,
                    None,
                    None,
                    None,
                    None,
                )?,
            )?;
            let first =
                session
                    .bind(py)
                    .call_method("table_from_sql", ("select 1 as id",), None)?;
            let second =
                session
                    .bind(py)
                    .call_method("table_from_sql", ("select 2 as id",), None)?;
            let first_spec =
                first.call_method("to_mssql", (), Some(&mssql_kwargs(py, "orders")?))?;
            let second_spec =
                second.call_method("to_mssql", (), Some(&mssql_kwargs(py, "orders")?))?;

            for dry_run in [true, false] {
                let outputs = PyList::new(py, [&first_spec, &second_spec])?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("dry_run", dry_run)?;

                let error = session
                    .bind(py)
                    .call_method("write_all", (outputs,), Some(&kwargs))
                    .unwrap_err();

                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "mssql_workflow_planning"
                );
                assert_eq!(
                    error.value(py).getattr("kind")?.extract::<String>()?,
                    "mssql_workflow_planning"
                );
                assert!(
                    error
                        .value(py)
                        .getattr("message")?
                        .extract::<String>()?
                        .contains("duplicate output name")
                );
            }

            Ok(())
        })
    }

    #[test]
    fn write_all_rejects_bad_options_with_config_phase() -> PyResult<()> {
        Python::attach(|py| {
            let initial_adapter_count = adapter_creation_count();
            let session = Py::new(
                py,
                PySession::new(
                    py,
                    Some("server=tcp:sql.example.com;password=secret-token".to_owned()),
                    None,
                    None,
                    None,
                    None,
                    None,
                )?,
            )?;
            let table =
                session
                    .bind(py)
                    .call_method("table_from_sql", ("select 1 as id",), None)?;
            let spec = table.call_method("to_mssql", (), Some(&mssql_kwargs(py, "orders")?))?;

            let options = PyDict::new(py);
            options.set_item("bogus", "disabled")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("options", options)?;
            kwargs.set_item("progress", true)?;
            let outputs = PyList::new(py, [&spec])?;
            let error = session
                .bind(py)
                .call_method("write_all", (outputs,), Some(&kwargs))
                .unwrap_err();
            assert_config_error(py, &error, "unknown_option")?;

            let options = PyDict::new(py);
            options.set_item("cache_mode", "sometimes")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("options", options)?;
            let outputs = PyList::new(py, [&spec])?;
            let error = session
                .bind(py)
                .call_method("write_all", (outputs,), Some(&kwargs))
                .unwrap_err();
            assert_config_error(py, &error, "invalid_option_value")?;

            let options = PyDict::new(py);
            options.set_item("cache_mode", "disabled")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("options", options)?;
            kwargs.set_item("dry_run", true)?;
            kwargs.set_item("progress", true)?;
            let outputs = PyList::new(py, [&spec])?;
            let error = session
                .bind(py)
                .call_method("write_all", (outputs,), Some(&kwargs))
                .unwrap_err();
            assert_config_error(py, &error, "invalid_option_value")?;

            assert_eq!(adapter_creation_count(), initial_adapter_count);

            Ok(())
        })
    }

    #[test]
    fn write_all_rejects_output_specs_from_another_session() -> PyResult<()> {
        Python::attach(|py| {
            let initial_adapter_count = adapter_creation_count();
            let first_session =
                Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let second_session =
                Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let table =
                first_session
                    .bind(py)
                    .call_method("table_from_sql", ("select 1 as id",), None)?;
            let spec = table.call_method("to_mssql", (), Some(&mssql_kwargs(py, "orders")?))?;
            let outputs = PyList::new(py, [&spec])?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("dry_run", true)?;
            kwargs.set_item("progress", true)?;

            let error = second_session
                .bind(py)
                .call_method("write_all", (outputs,), Some(&kwargs))
                .unwrap_err();

            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "output_session_mismatch"
            );
            assert_eq!(adapter_creation_count(), initial_adapter_count);

            Ok(())
        })
    }

    #[test]
    fn table_from_sql_rejects_empty_sql() -> PyResult<()> {
        Python::attach(|py| {
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;

            let error = match session
                .bind(py)
                .call_method("table_from_sql", ("   ",), None)
            {
                Ok(_) => return Err(PyAssertionError::new_err("expected empty SQL error")),
                Err(error) => error,
            };

            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "sql_table"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "sql_table"
            );
            assert!(!error.value(py).str()?.to_string().contains("select"));
            Ok(())
        })
    }

    #[test]
    fn table_from_sql_rejects_invalid_sql_without_exposing_raw_sql() -> PyResult<()> {
        Python::attach(|py| {
            let table = DeltaLogFixture::new("orders")?;
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;
            session
                .bind(py)
                .call_method("delta_lake", (table.uri(),), Some(&kwargs))?;

            for sql in [
                "select id from orders; select id from orders",
                "create table raw_sql_secret as select id from orders",
                "insert into orders select id from orders",
                "select raw_sql_secret from missing_orders",
            ] {
                let error = match session.bind(py).call_method("table_from_sql", (sql,), None) {
                    Ok(_) => return Err(PyAssertionError::new_err("expected SQL error")),
                    Err(error) => error,
                };

                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "sql_table"
                );
                assert_eq!(
                    error.value(py).getattr("kind")?.extract::<String>()?,
                    "sql_table"
                );
                assert!(
                    !error
                        .value(py)
                        .str()?
                        .to_string()
                        .contains("raw_sql_secret")
                );
            }

            Ok(())
        })
    }

    #[test]
    fn session_accepts_default_mssql_connection_string() -> PyResult<()> {
        Python::attach(|py| {
            let session = Py::new(
                py,
                PySession::new(
                    py,
                    Some(
                        "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token"
                            .to_owned(),
                    ),
                    None,
                    None,
                    None,
                    None,
                    None,
                )?,
            )?;
            let repr = session.bind(py).repr()?.extract::<String>()?;

            assert_eq!(repr, "deltafunnel.Session(sources=[], derived_tables=[])");
            assert!(!repr.contains("server=tcp"));
            assert!(!repr.contains("admin"));
            assert!(!repr.contains("password"));
            assert!(!repr.contains("secret-token"));

            Ok(())
        })
    }

    #[test]
    fn session_accepts_query_options() -> PyResult<()> {
        Python::attach(|py| {
            let session = PySession::new(py, None, Some(3), Some(17), None, None, None)?;

            assert_eq!(
                session.inner.options().query_options(),
                QueryOptions {
                    target_partitions: Some(3),
                    output_batch_size: Some(17),
                }
            );

            Ok(())
        })
    }

    #[test]
    fn omitted_schema_options_preserve_rust_defaults() -> PyResult<()> {
        Python::attach(|py| {
            let session = PySession::new(py, None, None, None, None, None, None)?;

            assert_eq!(
                session.inner.options().mssql_schema_options(),
                MssqlSchemaPlanOptions::default()
            );

            Ok(())
        })
    }

    #[test]
    fn session_rejects_zero_query_options_with_config_phase() -> PyResult<()> {
        Python::attach(|py| {
            let cases = [
                (Some(0), None, "target_partitions"),
                (None, Some(0), "output_batch_size"),
            ];

            for (target_partitions, output_batch_size, option_name) in cases {
                let error = match PySession::new(
                    py,
                    None,
                    target_partitions,
                    output_batch_size,
                    None,
                    None,
                    None,
                ) {
                    Ok(_) => {
                        return Err(PyAssertionError::new_err("expected zero value error"));
                    }
                    Err(error) => error,
                };

                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "config"
                );
                assert!(
                    error
                        .value(py)
                        .getattr("message")?
                        .extract::<String>()?
                        .contains(option_name)
                );
            }

            Ok(())
        })
    }

    #[test]
    fn session_rejects_bool_numeric_options_with_config_phase() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            for option_name in ["target_partitions", "output_batch_size"] {
                let kwargs = PyDict::new(py);
                kwargs.set_item(option_name, true)?;

                let error = match module.getattr("Session")?.call((), Some(&kwargs)) {
                    Ok(_) => return Err(PyAssertionError::new_err("expected bool option error")),
                    Err(error) => error,
                };

                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "config"
                );
                assert!(
                    error
                        .value(py)
                        .getattr("message")?
                        .extract::<String>()?
                        .contains(option_name)
                );
            }

            let provider_scan_options = PyDict::new(py);
            provider_scan_options.set_item("max_concurrent_file_reads_per_partition", true)?;
            let error = match PySession::new(
                py,
                None,
                None,
                None,
                Some(&provider_scan_options),
                None,
                None,
            ) {
                Ok(_) => return Err(PyAssertionError::new_err("expected bool option error")),
                Err(error) => error,
            };
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );

            let string_policy = PyDict::new(py);
            string_policy.set_item("nvarchar", true)?;
            let schema_options = PyDict::new(py);
            schema_options.set_item("string_policy", string_policy)?;
            let error =
                match PySession::new(py, None, None, None, None, None, Some(&schema_options)) {
                    Ok(_) => return Err(PyAssertionError::new_err("expected bool option error")),
                    Err(error) => error,
                };
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );

            Ok(())
        })
    }

    #[test]
    fn session_accepts_provider_scan_options() -> PyResult<()> {
        Python::attach(|py| {
            let provider_scan_options = PyDict::new(py);
            provider_scan_options.set_item("max_concurrent_file_reads_per_scan", 8)?;
            provider_scan_options.set_item("max_concurrent_file_reads_per_partition", 2)?;
            provider_scan_options.set_item("output_buffer_capacity_per_partition", 4)?;
            provider_scan_options.set_item("native_async_prefetch_file_count_per_partition", 1)?;

            let session = PySession::new(
                py,
                None,
                None,
                None,
                Some(&provider_scan_options),
                None,
                None,
            )?;

            assert_eq!(
                session.inner.options().provider_scan_options(),
                DeltaProviderScanExecutionOptions {
                    max_concurrent_file_reads_per_scan: Some(8),
                    max_concurrent_file_reads_per_partition: 2,
                    output_buffer_capacity_per_partition: 4,
                    native_async_prefetch_file_count_per_partition: 1,
                    ..DeltaProviderScanExecutionOptions::default()
                }
            );

            Ok(())
        })
    }

    #[test]
    fn partial_provider_scan_options_keep_auto_scan_wide_capacity() -> PyResult<()> {
        Python::attach(|py| {
            let provider_scan_options = PyDict::new(py);
            provider_scan_options.set_item("max_concurrent_file_reads_per_partition", 2)?;

            let session = PySession::new(
                py,
                None,
                None,
                None,
                Some(&provider_scan_options),
                None,
                None,
            )?;

            assert_eq!(
                session
                    .inner
                    .options()
                    .provider_scan_options()
                    .max_concurrent_file_reads_per_partition,
                2
            );
            assert!(
                session
                    .inner
                    .options()
                    .provider_scan_options()
                    .max_concurrent_file_reads_per_scan
                    .is_none()
            );

            Ok(())
        })
    }

    #[test]
    fn session_rejects_bad_provider_scan_options_with_config_phase() -> PyResult<()> {
        Python::attach(|py| {
            let cases = [
                ("unknown_option", 1),
                ("max_concurrent_file_reads_per_scan", 0),
                ("max_concurrent_file_reads_per_partition", 0),
                ("output_buffer_capacity_per_partition", 0),
            ];

            for (key, value) in cases {
                let provider_scan_options = PyDict::new(py);
                provider_scan_options.set_item(key, value)?;
                let error = match PySession::new(
                    py,
                    None,
                    None,
                    None,
                    Some(&provider_scan_options),
                    None,
                    None,
                ) {
                    Ok(_) => {
                        return Err(PyAssertionError::new_err(
                            "expected provider scan option error",
                        ));
                    }
                    Err(error) => error,
                };

                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "config"
                );
            }

            Ok(())
        })
    }

    #[test]
    fn session_accepts_validation_options() -> PyResult<()> {
        Python::attach(|py| {
            let target_validation_modes = [
                ("disabled", TargetValidationMode::Disabled),
                (
                    "validate_if_possible",
                    TargetValidationMode::ValidateIfPossible,
                ),
                ("require", TargetValidationMode::Require),
            ];
            for (value, expected) in target_validation_modes {
                let validation_options = PyDict::new(py);
                validation_options.set_item("target_validation_mode", value)?;

                let session =
                    PySession::new(py, None, None, None, None, Some(&validation_options), None)?;

                assert_eq!(
                    session
                        .inner
                        .options()
                        .validation_options()
                        .target_validation_mode(),
                    expected
                );
            }

            let dry_run_scan_summary_modes = [
                ("metadata_only", DryRunScanSummaryMode::MetadataOnly),
                (
                    "exhaust_scan_metadata",
                    DryRunScanSummaryMode::ExhaustScanMetadata,
                ),
            ];
            for (value, expected) in dry_run_scan_summary_modes {
                let validation_options = PyDict::new(py);
                validation_options.set_item("dry_run_scan_summary_mode", value)?;

                let session =
                    PySession::new(py, None, None, None, None, Some(&validation_options), None)?;

                assert_eq!(
                    session
                        .inner
                        .options()
                        .validation_options()
                        .dry_run_scan_summary_mode(),
                    expected
                );
            }

            let validation_options = PyDict::new(py);
            validation_options.set_item("require_successful_planning", false)?;
            let session =
                PySession::new(py, None, None, None, None, Some(&validation_options), None)?;

            assert!(
                !session
                    .inner
                    .options()
                    .validation_options()
                    .require_successful_planning()
            );

            Ok(())
        })
    }

    #[test]
    fn session_rejects_bad_validation_options_with_config_phase() -> PyResult<()> {
        Python::attach(|py| {
            let cases = [
                ("unknown_option", "value"),
                ("target_validation_mode", "sometimes"),
                ("dry_run_scan_summary_mode", "full"),
            ];

            for (key, value) in cases {
                let validation_options = PyDict::new(py);
                validation_options.set_item(key, value)?;
                let error = match PySession::new(
                    py,
                    None,
                    None,
                    None,
                    None,
                    Some(&validation_options),
                    None,
                ) {
                    Ok(_) => {
                        return Err(PyAssertionError::new_err(
                            "expected validation option error",
                        ));
                    }
                    Err(error) => error,
                };

                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "config"
                );
            }

            Ok(())
        })
    }

    #[test]
    fn session_accepts_schema_options() -> PyResult<()> {
        Python::attach(|py| {
            let string_policies = [
                ("nvarchar_max", MssqlStringPolicy::NVarCharMax),
                ("observed_nvarchar", MssqlStringPolicy::ObservedNVarChar),
            ];
            for (value, expected) in string_policies {
                let schema_options = PyDict::new(py);
                schema_options.set_item("string_policy", value)?;

                let session =
                    PySession::new(py, None, None, None, None, None, Some(&schema_options))?;

                assert_eq!(
                    session.inner.options().mssql_schema_options().string_policy,
                    expected
                );
            }

            let string_policy = PyDict::new(py);
            string_policy.set_item("nvarchar", 128)?;
            let schema_options = PyDict::new(py);
            schema_options.set_item("string_policy", string_policy)?;
            let session = PySession::new(py, None, None, None, None, None, Some(&schema_options))?;
            assert_eq!(
                session.inner.options().mssql_schema_options().string_policy,
                MssqlStringPolicy::NVarChar(128)
            );

            let binary_policies = [
                ("varbinary_max", MssqlBinaryPolicy::VarBinaryMax),
                ("observed_varbinary", MssqlBinaryPolicy::ObservedVarBinary),
            ];
            for (value, expected) in binary_policies {
                let schema_options = PyDict::new(py);
                schema_options.set_item("binary_policy", value)?;

                let session =
                    PySession::new(py, None, None, None, None, None, Some(&schema_options))?;

                assert_eq!(
                    session.inner.options().mssql_schema_options().binary_policy,
                    expected
                );
            }

            let binary_policy = PyDict::new(py);
            binary_policy.set_item("varbinary", 256)?;
            let schema_options = PyDict::new(py);
            schema_options.set_item("binary_policy", binary_policy)?;
            let session = PySession::new(py, None, None, None, None, None, Some(&schema_options))?;
            assert_eq!(
                session.inner.options().mssql_schema_options().binary_policy,
                MssqlBinaryPolicy::VarBinary(256)
            );

            let timezone_policies = [
                ("reject", MssqlTimezonePolicy::Reject),
                ("datetimeoffset", MssqlTimezonePolicy::DateTimeOffset),
                (
                    "normalize_utc_datetime2",
                    MssqlTimezonePolicy::NormalizeUtcDateTime2,
                ),
            ];
            for (value, expected) in timezone_policies {
                let schema_options = PyDict::new(py);
                schema_options.set_item("timezone_policy", value)?;

                let session =
                    PySession::new(py, None, None, None, None, None, Some(&schema_options))?;

                assert_eq!(
                    session
                        .inner
                        .options()
                        .mssql_schema_options()
                        .timezone_policy,
                    expected
                );
            }

            let timestamp_policies = [
                ("datetime", MssqlTimestampPolicy::DateTime),
                (
                    "datetime2",
                    MssqlTimestampPolicy::DateTime2 { precision: 7 },
                ),
            ];
            for (value, expected) in timestamp_policies {
                let schema_options = PyDict::new(py);
                schema_options.set_item("timestamp_policy", value)?;

                let session =
                    PySession::new(py, None, None, None, None, None, Some(&schema_options))?;

                assert_eq!(
                    session
                        .inner
                        .options()
                        .mssql_schema_options()
                        .timestamp_policy,
                    expected
                );
            }

            let timestamp_policy = PyDict::new(py);
            timestamp_policy.set_item("datetime2", 3)?;
            let schema_options = PyDict::new(py);
            schema_options.set_item("timestamp_policy", timestamp_policy)?;
            let session = PySession::new(py, None, None, None, None, None, Some(&schema_options))?;
            assert_eq!(
                session
                    .inner
                    .options()
                    .mssql_schema_options()
                    .timestamp_policy,
                MssqlTimestampPolicy::DateTime2 { precision: 3 }
            );

            let nanosecond_policies = [
                ("reject_non_100ns", MssqlNanosecondPolicy::RejectNon100ns),
                ("round_to_100ns", MssqlNanosecondPolicy::RoundTo100ns),
                ("truncate_to_100ns", MssqlNanosecondPolicy::TruncateTo100ns),
            ];
            for (value, expected) in nanosecond_policies {
                let schema_options = PyDict::new(py);
                schema_options.set_item("nanosecond_policy", value)?;

                let session =
                    PySession::new(py, None, None, None, None, None, Some(&schema_options))?;

                assert_eq!(
                    session
                        .inner
                        .options()
                        .mssql_schema_options()
                        .nanosecond_policy,
                    expected
                );
            }

            let uint64_policies = [
                ("reject", MssqlUInt64Policy::Reject),
                ("decimal20_0", MssqlUInt64Policy::Decimal20_0),
                ("checked_bigint", MssqlUInt64Policy::CheckedBigInt),
            ];
            for (value, expected) in uint64_policies {
                let schema_options = PyDict::new(py);
                schema_options.set_item("uint64_policy", value)?;

                let session =
                    PySession::new(py, None, None, None, None, None, Some(&schema_options))?;

                assert_eq!(
                    session.inner.options().mssql_schema_options().uint64_policy,
                    expected
                );
            }

            let decimal_policies = [
                (
                    "reject_negative_scale",
                    MssqlDecimalPolicy::RejectNegativeScale,
                ),
                (
                    "normalize_negative_scale",
                    MssqlDecimalPolicy::NormalizeNegativeScale,
                ),
            ];
            for (value, expected) in decimal_policies {
                let schema_options = PyDict::new(py);
                schema_options.set_item("decimal_policy", value)?;

                let session =
                    PySession::new(py, None, None, None, None, None, Some(&schema_options))?;

                assert_eq!(
                    session
                        .inner
                        .options()
                        .mssql_schema_options()
                        .decimal_policy,
                    expected
                );
            }

            let decimal256_policies = [
                ("checked_downcast", MssqlDecimal256Policy::CheckedDowncast),
                ("reject", MssqlDecimal256Policy::Reject),
            ];
            for (value, expected) in decimal256_policies {
                let schema_options = PyDict::new(py);
                schema_options.set_item("decimal256_policy", value)?;

                let session =
                    PySession::new(py, None, None, None, None, None, Some(&schema_options))?;

                assert_eq!(
                    session
                        .inner
                        .options()
                        .mssql_schema_options()
                        .decimal256_policy,
                    expected
                );
            }

            let schema_options = PyDict::new(py);
            schema_options.set_item("float_policy", "reject_non_finite")?;
            let session = PySession::new(py, None, None, None, None, None, Some(&schema_options))?;
            assert_eq!(
                session.inner.options().mssql_schema_options().float_policy,
                MssqlFloatPolicy::RejectNonFinite
            );

            let date64_policies = [
                ("reject_non_midnight", MssqlDate64Policy::RejectNonMidnight),
                ("timestamp_datetime2", MssqlDate64Policy::TimestampDateTime2),
            ];
            for (value, expected) in date64_policies {
                let schema_options = PyDict::new(py);
                schema_options.set_item("date64_policy", value)?;

                let session =
                    PySession::new(py, None, None, None, None, None, Some(&schema_options))?;

                assert_eq!(
                    session.inner.options().mssql_schema_options().date64_policy,
                    expected
                );
            }

            Ok(())
        })
    }

    #[test]
    fn session_rejects_bad_schema_options_with_config_phase() -> PyResult<()> {
        Python::attach(|py| {
            let cases = [
                ("unknown_option", "value"),
                ("string_policy", "nvarchar"),
                ("binary_policy", "varbinary"),
                ("timezone_policy", "sometimes"),
                ("timestamp_policy", "sometimes"),
                ("nanosecond_policy", "round"),
                ("uint64_policy", "decimal"),
                ("decimal_policy", "normalize"),
                ("decimal256_policy", "checked"),
                ("float_policy", "allow_non_finite"),
                ("date64_policy", "datetime2"),
            ];

            for (key, value) in cases {
                let schema_options = PyDict::new(py);
                schema_options.set_item(key, value)?;
                let error =
                    match PySession::new(py, None, None, None, None, None, Some(&schema_options)) {
                        Ok(_) => {
                            return Err(PyAssertionError::new_err("expected schema option error"));
                        }
                        Err(error) => error,
                    };

                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "config"
                );
            }

            let bad_string_policy = PyDict::new(py);
            bad_string_policy.set_item("nvarchar", 0)?;
            let schema_options = PyDict::new(py);
            schema_options.set_item("string_policy", bad_string_policy)?;
            let error =
                match PySession::new(py, None, None, None, None, None, Some(&schema_options)) {
                    Ok(_) => return Err(PyAssertionError::new_err("expected schema option error")),
                    Err(error) => error,
                };
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );

            let bad_timestamp_policy = PyDict::new(py);
            bad_timestamp_policy.set_item("datetime2", 8)?;
            let schema_options = PyDict::new(py);
            schema_options.set_item("timestamp_policy", bad_timestamp_policy)?;
            let error =
                match PySession::new(py, None, None, None, None, None, Some(&schema_options)) {
                    Ok(_) => return Err(PyAssertionError::new_err("expected schema option error")),
                    Err(error) => error,
                };
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );

            let bad_binary_policy = PyDict::new(py);
            bad_binary_policy.set_item("varbinary", 0)?;
            let schema_options = PyDict::new(py);
            schema_options.set_item("binary_policy", bad_binary_policy)?;
            let error =
                match PySession::new(py, None, None, None, None, None, Some(&schema_options)) {
                    Ok(_) => return Err(PyAssertionError::new_err("expected schema option error")),
                    Err(error) => error,
                };
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );

            Ok(())
        })
    }

    #[test]
    fn session_constructor_accepts_connection_keyword() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item(
                "default_mssql_connection_string",
                "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
            )?;
            kwargs.set_item("target_partitions", 3)?;
            kwargs.set_item("output_batch_size", 17)?;
            let provider_scan_options = PyDict::new(py);
            provider_scan_options.set_item("max_concurrent_file_reads_per_scan", 8)?;
            kwargs.set_item("provider_scan_options", provider_scan_options)?;
            let validation_options = PyDict::new(py);
            validation_options.set_item("target_validation_mode", "disabled")?;
            kwargs.set_item("validation_options", validation_options)?;
            let schema_options = PyDict::new(py);
            schema_options.set_item("uint64_policy", "decimal20_0")?;
            kwargs.set_item("schema_options", schema_options)?;

            let session = module.getattr("Session")?.call((), Some(&kwargs))?;
            let repr = session.repr()?.extract::<String>()?;

            assert_eq!(repr, "deltafunnel.Session(sources=[], derived_tables=[])");
            assert!(!repr.contains("server=tcp"));
            assert!(!repr.contains("admin"));
            assert!(!repr.contains("password"));
            assert!(!repr.contains("secret-token"));

            Ok(())
        })
    }

    fn mssql_kwargs<'py>(py: Python<'py>, table: &str) -> PyResult<Bound<'py, PyDict>> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("schema", "dbo")?;
        kwargs.set_item("table", table)?;
        kwargs.set_item("load_mode", "append_existing")?;
        Ok(kwargs)
    }

    fn required_item<'py>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Bound<'py, PyAny>> {
        dict.get_item(key)?
            .ok_or_else(|| PyKeyError::new_err(key.to_owned()))
    }

    fn assert_succeeded_output(
        output: &Bound<'_, PyDict>,
        output_name: &str,
        connection_source: &str,
        row_count: u64,
    ) -> PyResult<()> {
        assert_eq!(
            required_item(output, "kind")?.extract::<String>()?,
            "succeeded"
        );
        assert_eq!(
            required_item(output, "output_name")?.extract::<String>()?,
            output_name
        );
        assert_eq!(
            required_item(output, "connection_source")?.extract::<String>()?,
            connection_source
        );
        let output_row_count = required_item(output, "output_row_count")?;
        let output_row_count = output_row_count.cast::<PyDict>()?;
        assert_eq!(
            required_item(output_row_count, "value")?.extract::<u64>()?,
            row_count
        );
        let validation = required_item(output, "validation_status")?;
        let validation = validation.cast::<PyDict>()?;
        assert_eq!(
            required_item(validation, "kind")?.extract::<String>()?,
            "passed"
        );

        Ok(())
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

    fn assert_config_error(py: Python<'_>, error: &PyErr, kind: &str) -> PyResult<()> {
        assert_eq!(
            error.value(py).getattr("phase")?.extract::<String>()?,
            "config"
        );
        assert_eq!(error.value(py).getattr("kind")?.extract::<String>()?, kind);
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

    async fn drop_tables(
        config: &MssqlIntegrationConfig,
        tables: &[&MssqlTableName],
    ) -> TestResult<()> {
        let mut client = connect_mssql_client_from_ado_string(&config.connection_string).await?;
        for table in tables {
            client
                .execute_statement(&format!("DROP TABLE IF EXISTS {};", table.quoted_sql()))
                .await?;
        }

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
INSERT INTO {table} ([order_id]) VALUES (800), (801);",
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

    async fn assert_target_exists_with_zero_rows(
        config: &MssqlIntegrationConfig,
        table: &MssqlTableName,
    ) -> TestResult<()> {
        let mut client = connect_mssql_client_from_ado_string(&config.connection_string).await?;
        client
            .execute_statement(&format!(
                "\
IF OBJECT_ID(N'{table}', N'U') IS NULL
    THROW 51014, 'replace target table was not created', 1;
IF (SELECT COUNT(*) FROM {table}) <> 0
    THROW 51015, 'replace target row count mismatch', 1;",
                table = table.quoted_sql(),
            ))
            .await?;

        Ok(())
    }

    fn unique_mssql_table_name(schema: &str) -> TestResult<MssqlTableName> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let sequence = NEXT_MSSQL_TABLE_SEQUENCE.fetch_add(1, Ordering::Relaxed);

        Ok(MssqlTableName::new(
            schema.to_owned(),
            format!(
                "df_python_write_all_it_{}_{}_{}",
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

    struct DeltaLogFixture {
        path: PathBuf,
    }

    impl DeltaLogFixture {
        fn new(name: &str) -> PyResult<Self> {
            let path = env_unique_path(name)?;
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path).map_err(io_py_error)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{}\n{}\n", PROTOCOL_JSON, metadata_json()),
            )
            .map_err(io_py_error)?;
            fs::write(
                log_path.join("00000000000000000001.json"),
                format!("{}\n", add_json("part-00000.parquet")),
            )
            .map_err(io_py_error)?;

            Ok(Self { path })
        }

        fn uri(&self) -> String {
            self.path.to_string_lossy().to_string()
        }

        fn uri_with_secret_parts(&self) -> String {
            format!(
                "{}?token=super-secret-token#debug-secret",
                self.path.to_string_lossy()
            )
        }
    }

    impl Drop for DeltaLogFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn env_unique_path(name: &str) -> PyResult<PathBuf> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| PyAssertionError::new_err(error.to_string()))?
            .as_nanos();
        Ok(std::env::temp_dir().join(format!(
            "delta-funnel-python-{name}-{}-{nanos}",
            std::process::id()
        )))
    }

    fn io_py_error(error: std::io::Error) -> PyErr {
        PyAssertionError::new_err(error.to_string())
    }

    fn metadata_json() -> String {
        format!(
            r#"{{"metaData":{{"id":"delta-funnel-python-test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":{SCHEMA_FIELDS_JSON}}}","partitionColumns":[],"configuration":{{}},"createdTime":1587968585495}}}}"#
        )
    }

    fn add_json(path: &str) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true}}}}"#
        )
    }

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const SCHEMA_FIELDS_JSON: &str =
        r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]"#;
}
