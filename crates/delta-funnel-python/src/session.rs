//! Python session wrapper.

use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBool, PyDict, PyDictMethods};

use crate::exception::{delta_funnel_error_to_py, delta_funnel_py_error};
use crate::json::json_value_to_py;
use crate::output::PyMssqlOutputSpec;
use crate::table::PyTable;

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
        "deltafunnel.Session()".to_owned()
    }

    /// Registers a named Delta source, or returns a pending source that cannot be referenced by SQL.
    ///
    /// A pending source is not registered in the session SQL catalog and cannot
    /// be referenced by SQL until `alias(name)` is called.
    #[pyo3(signature = (source_uri, *, version=None, storage_options=None, name=None))]
    fn delta_lake(
        slf: Py<Self>,
        py: Python<'_>,
        source_uri: String,
        version: Option<&Bound<'_, PyAny>>,
        storage_options: Option<&Bound<'_, PyDict>>,
        name: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let source =
            PendingDeltaSource::new(py, slf.clone_ref(py), source_uri, version, storage_options)?;
        if let Some(name) = name {
            return Py::new(py, source.register_alias(py, name)?).map(Py::into_any);
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

    /// Runs a multi-output SQL Server dry-run plan without executing rows.
    #[pyo3(signature = (outputs, *, dry_run=None))]
    fn write_all(
        slf: Py<Self>,
        py: Python<'_>,
        outputs: Vec<PyRef<'_, PyMssqlOutputSpec>>,
        dry_run: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        ensure_write_all_dry_run_enabled(py, dry_run)?;
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
            .map(|output| output.write_plan(delta_funnel::RunMode::DryRun))
            .collect::<Vec<_>>();

        slf.borrow(py).dry_run_all_to_mssql(py, &requests)
    }
}

impl PySession {
    fn register_delta_source(
        &mut self,
        py: Python<'_>,
        name: String,
        source_uri: String,
        version: Option<u64>,
        storage_options: delta_funnel::DeltaStorageOptions,
    ) -> PyResult<delta_funnel::LazyTable> {
        let source = delta_source_config(name, source_uri, version, storage_options);

        self.inner
            .delta_lake(source)
            .map_err(|error| rust_error_to_py(py, error))
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
    ) -> PyResult<Py<PyAny>> {
        let report = self
            .runtime
            .dry_run_to_mssql(&self.inner, request)
            .map_err(|error| rust_error_to_py(py, error))?;
        json_value_to_py(py, &report.to_json_value())
    }

    fn dry_run_all_to_mssql(
        &self,
        py: Python<'_>,
        requests: &[delta_funnel::OutputWritePlan],
    ) -> PyResult<Py<PyAny>> {
        let report = self
            .runtime
            .dry_run_all_to_mssql(&self.inner, requests)
            .map_err(|error| rust_error_to_py(py, error))?;
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

    fn register_alias(&self, py: Python<'_>, name: String) -> PyResult<PyTable> {
        let table = self.session.borrow_mut(py).register_delta_source(
            py,
            name,
            self.source_uri.clone(),
            self.version,
            self.storage_options.clone(),
        )?;
        Ok(PyTable::from_inner(self.session.clone_ref(py), table))
    }
}

#[pymethods]
impl PendingDeltaSource {
    /// Registers this pending Delta source under `name` and returns a `Table`.
    fn alias(&self, py: Python<'_>, name: String) -> PyResult<PyTable> {
        self.register_alias(py, name)
    }

    fn __repr__(&self) -> String {
        "deltafunnel.PendingDeltaSource()".to_owned()
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

    for (key, value) in provider_scan_options.iter() {
        let key = option_name(py, &key)?;
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
                return Err(config_py_error(
                    py,
                    "unknown_option",
                    format!("unknown provider scan option `{key}`"),
                ));
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

    for (key, value) in validation_options.iter() {
        let key = option_name(py, &key)?;
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
                let value = value.extract::<bool>().map_err(|_| {
                    config_py_error(
                        py,
                        "invalid_option_value",
                        "`require_successful_planning` must be a bool".to_owned(),
                    )
                })?;
                options = options.with_require_successful_planning(value);
            }
            _ => {
                return Err(config_py_error(
                    py,
                    "unknown_option",
                    format!("unknown validation option `{key}`"),
                ));
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

    for (key, value) in schema_options.iter() {
        let key = option_name(py, &key)?;
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
                return Err(config_py_error(
                    py,
                    "unknown_option",
                    format!("unknown schema option `{key}`"),
                ));
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
    let value = usize_option(py, &raw_value, option_name)?;
    if value == 0 {
        return Err(config_py_error(
            py,
            "invalid_option_value",
            format!("`{option_name}` bounded length must be at least 1"),
        ));
    }

    Ok(value)
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

fn ensure_write_all_dry_run_enabled(py: Python<'_>, dry_run: Option<bool>) -> PyResult<()> {
    if dry_run == Some(true) {
        return Ok(());
    }

    Err(config_py_error(
        py,
        "execute_mode_not_enabled",
        "write_all execute mode is not enabled yet; pass `dry_run=True`".to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::PySession;
    use crate::deltafunnel;
    use delta_funnel::{
        DeltaProviderScanExecutionOptions, DryRunScanSummaryMode, MssqlBinaryPolicy,
        MssqlDate64Policy, MssqlDecimal256Policy, MssqlDecimalPolicy, MssqlFloatPolicy,
        MssqlNanosecondPolicy, MssqlSchemaPlanOptions, MssqlStringPolicy, MssqlTimezonePolicy,
        MssqlUInt64Policy, QueryOptions, TargetValidationMode,
    };
    use pyo3::exceptions::{PyAssertionError, PyKeyError, PyTypeError};
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods, PyModule};
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

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
    fn delta_lake_docstrings_describe_pending_alias_semantics() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            let session_type = module.getattr("Session")?;
            let delta_lake_doc = session_type
                .getattr("delta_lake")?
                .getattr("__doc__")?
                .extract::<String>()?;
            assert!(delta_lake_doc.contains("pending source"));
            assert!(delta_lake_doc.contains("cannot be referenced by SQL"));

            let pending_type = module.getattr("PendingDeltaSource")?;
            let pending_doc = pending_type.getattr("__doc__")?.extract::<String>()?;
            assert!(pending_doc.contains("Unregistered Delta source"));

            let alias_doc = pending_type
                .getattr("alias")?
                .getattr("__doc__")?
                .extract::<String>()?;
            assert!(alias_doc.contains("returns a `Table`"));

            Ok(())
        })
    }

    #[test]
    fn default_session_constructs_with_safe_repr() -> PyResult<()> {
        Python::attach(|py| {
            let session = Py::new(py, PySession::new(py, None, None, None, None, None, None)?)?;
            let repr = session.bind(py).repr()?.extract::<String>()?;

            assert_eq!(repr, "deltafunnel.Session()");
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

            assert_eq!(
                lazy.repr()?.extract::<String>()?,
                "deltafunnel.Table(name=\"orders\")"
            );
            let session = session.bind(py).borrow();
            assert_eq!(session.inner.source_reports().len(), 1);
            assert_eq!(session.inner.source_reports()[0].source_name(), "orders");
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
                "deltafunnel.PendingDeltaSource()"
            );
            assert!(session.bind(py).borrow().inner.source_reports().is_empty());

            let lazy = pending.call_method("alias", ("orders",), None)?;

            assert_eq!(
                lazy.repr()?.extract::<String>()?,
                "deltafunnel.Table(name=\"orders\")"
            );
            let session = session.bind(py).borrow();
            assert_eq!(session.inner.source_reports().len(), 1);
            assert_eq!(session.inner.source_reports()[0].snapshot_version(), 0);
            Ok(())
        })
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
            }

            let storage_options = PyDict::new(py);
            storage_options.set_item("token", 7)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", "orders")?;
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
            assert!(!pending_repr.contains("super-secret"));
            assert!(!pending_repr.contains("debug-secret"));

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
                "deltafunnel.Table(name=\"table_1\")"
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
                "deltafunnel.Table(name=\"recent_orders\")"
            );
            assert_eq!(
                downstream.repr()?.extract::<String>()?,
                "deltafunnel.Table(name=\"table_2\")"
            );
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
    fn write_all_rejects_execute_mode_until_write_all_slice() -> PyResult<()> {
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
            assert_execute_mode_error(py, &error)?;

            kwargs.set_item("dry_run", false)?;
            let error = session
                .bind(py)
                .call_method("write_all", (&outputs,), Some(&kwargs))
                .unwrap_err();
            assert_execute_mode_error(py, &error)?;

            Ok(())
        })
    }

    #[test]
    fn write_all_dry_run_rejects_duplicate_output_names() -> PyResult<()> {
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
            let outputs = PyList::new(py, [&first_spec, &second_spec])?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("dry_run", true)?;

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

            Ok(())
        })
    }

    #[test]
    fn write_all_rejects_output_specs_from_another_session() -> PyResult<()> {
        Python::attach(|py| {
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

            assert_eq!(repr, "deltafunnel.Session()");
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

            assert_eq!(repr, "deltafunnel.Session()");
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
