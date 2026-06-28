//! Python session wrapper.

use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};

use crate::exception::{delta_funnel_error_to_py, delta_funnel_py_error};

#[pyclass(name = "Session", module = "deltafunnel")]
pub(crate) struct PySession {
    #[allow(dead_code)]
    inner: delta_funnel::DeltaFunnelSession,
}

#[pymethods]
impl PySession {
    #[new]
    #[pyo3(signature = (*, default_mssql_connection_string=None, target_partitions=None, output_batch_size=None, provider_scan_options=None, validation_options=None, schema_options=None))]
    fn new(
        py: Python<'_>,
        default_mssql_connection_string: Option<String>,
        target_partitions: Option<usize>,
        output_batch_size: Option<usize>,
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

        Ok(Self { inner })
    }

    fn __repr__(&self) -> String {
        "deltafunnel.Session()".to_owned()
    }
}

pub(crate) fn add_session(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySession>()
}

fn rust_error_to_py(py: Python<'_>, error: delta_funnel::DeltaFunnelError) -> PyErr {
    match delta_funnel_error_to_py(py, error) {
        Ok(error) => error,
        Err(error) => error,
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
    use pyo3::exceptions::PyAssertionError;
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyModule};

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
}
