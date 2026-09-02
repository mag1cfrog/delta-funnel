//! Typed Python options for Delta provider scans.

use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBool};

use crate::{exception::delta_funnel_error_to_py, session::config_py_error};

/// Controls when Delta files may be split into ranged scan tasks.
#[pyclass(
    frozen,
    eq,
    hash,
    from_py_object,
    rename_all = "SCREAMING_SNAKE_CASE",
    name = "FileRepartitioning",
    module = "deltafunnel"
)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum PyFileRepartitioning {
    /// Split files only when whole-file planning produces too few partitions.
    #[default]
    FillMissingParallelism,
    /// Let DataFusion rebalance file groups that already fill the target.
    Rebalance,
}

impl From<PyFileRepartitioning> for delta_arrow_reader::datafusion::IntraFileRepartitioning {
    fn from(value: PyFileRepartitioning) -> Self {
        match value {
            PyFileRepartitioning::FillMissingParallelism => Self::WhenBelowTarget,
            PyFileRepartitioning::Rebalance => Self::Always,
        }
    }
}

/// Immutable Delta provider scan configuration.
#[pyclass(frozen, name = "ProviderScanOptions", module = "deltafunnel")]
pub(crate) struct PyProviderScanOptions {
    execution_options: delta_arrow_reader::DeltaScanExecutionOptions,
    file_repartitioning: PyFileRepartitioning,
    use_view_types: bool,
}

impl PyProviderScanOptions {
    pub(crate) fn apply_to(
        &self,
        options: delta_funnel::SessionOptions,
    ) -> delta_funnel::SessionOptions {
        options
            .with_provider_scan_options(self.execution_options)
            .with_provider_file_repartitioning(self.file_repartitioning.into())
            .with_provider_use_view_types(self.use_view_types)
    }
}

#[pymethods]
impl PyProviderScanOptions {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        max_concurrent_file_reads_per_scan=None,
        max_concurrent_file_reads_per_partition=3,
        output_buffer_capacity_per_partition=1,
        native_async_prefetch_file_count_per_partition=2,
        parquet_metadata_size_hint=65_536,
        parquet_full_file_read_threshold=None,
        intra_file_repartitioning=None,
        use_view_types=false,
    ))]
    fn new(
        py: Python<'_>,
        #[pyo3(from_py_with = parse_max_concurrent_file_reads_per_scan)]
        max_concurrent_file_reads_per_scan: Option<usize>,
        #[pyo3(from_py_with = parse_max_concurrent_file_reads_per_partition)]
        max_concurrent_file_reads_per_partition: usize,
        #[pyo3(from_py_with = parse_output_buffer_capacity_per_partition)]
        output_buffer_capacity_per_partition: usize,
        #[pyo3(from_py_with = parse_native_async_prefetch_file_count_per_partition)]
        native_async_prefetch_file_count_per_partition: usize,
        #[pyo3(from_py_with = parse_parquet_metadata_size_hint)] parquet_metadata_size_hint: Option<
            usize,
        >,
        #[pyo3(from_py_with = parse_parquet_full_file_read_threshold)]
        parquet_full_file_read_threshold: Option<usize>,
        #[pyo3(from_py_with = parse_file_repartitioning)] intra_file_repartitioning: Option<
            PyFileRepartitioning,
        >,
        #[pyo3(from_py_with = parse_use_view_types)] use_view_types: bool,
    ) -> PyResult<Self> {
        let execution_options = delta_arrow_reader::DeltaScanExecutionOptions::default()
            .with_max_concurrent_file_reads_per_scan(max_concurrent_file_reads_per_scan)
            .map_err(|_| provider_scan_bound_error_to_py(py, "max_concurrent_file_reads_per_scan"))?
            .with_max_concurrent_file_reads_per_partition(max_concurrent_file_reads_per_partition)
            .map_err(|_| {
                provider_scan_bound_error_to_py(py, "max_concurrent_file_reads_per_partition")
            })?
            .with_output_buffer_batches_per_partition(output_buffer_capacity_per_partition)
            .map_err(|_| {
                provider_scan_bound_error_to_py(py, "output_buffer_capacity_per_partition")
            })?
            .with_parquet_metadata_size_hint_bytes(parquet_metadata_size_hint)
            .map_err(|_| provider_scan_bound_error_to_py(py, "parquet_metadata_size_hint"))?
            .with_parquet_full_file_read_threshold_bytes(parquet_full_file_read_threshold)
            .map_err(|_| provider_scan_bound_error_to_py(py, "parquet_full_file_read_threshold"))?
            .with_prefetch_files_per_partition(native_async_prefetch_file_count_per_partition);
        Ok(Self {
            execution_options,
            file_repartitioning: intra_file_repartitioning.unwrap_or_default(),
            use_view_types,
        })
    }

    #[getter]
    fn max_concurrent_file_reads_per_scan(&self) -> Option<usize> {
        self.execution_options.max_concurrent_file_reads_per_scan()
    }

    #[getter]
    fn max_concurrent_file_reads_per_partition(&self) -> usize {
        self.execution_options
            .max_concurrent_file_reads_per_partition()
    }

    #[getter]
    fn output_buffer_capacity_per_partition(&self) -> usize {
        self.execution_options.output_buffer_batches_per_partition()
    }

    #[getter]
    fn native_async_prefetch_file_count_per_partition(&self) -> usize {
        self.execution_options.prefetch_files_per_partition()
    }

    #[getter]
    fn parquet_metadata_size_hint(&self) -> Option<usize> {
        self.execution_options.parquet_metadata_size_hint_bytes()
    }

    #[getter]
    fn parquet_full_file_read_threshold(&self) -> Option<usize> {
        self.execution_options
            .parquet_full_file_read_threshold_bytes()
    }

    #[getter]
    const fn intra_file_repartitioning(&self) -> PyFileRepartitioning {
        self.file_repartitioning
    }

    #[getter]
    const fn use_view_types(&self) -> bool {
        self.use_view_types
    }
}

pub(crate) fn add_provider_options(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyFileRepartitioning>()?;
    module.add_class::<PyProviderScanOptions>()
}

fn parse_max_concurrent_file_reads_per_scan(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    optional_usize(value, "max_concurrent_file_reads_per_scan")
}

fn parse_max_concurrent_file_reads_per_partition(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    usize_option(value, "max_concurrent_file_reads_per_partition")
}

fn parse_output_buffer_capacity_per_partition(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    usize_option(value, "output_buffer_capacity_per_partition")
}

fn parse_native_async_prefetch_file_count_per_partition(
    value: &Bound<'_, PyAny>,
) -> PyResult<usize> {
    usize_option(value, "native_async_prefetch_file_count_per_partition")
}

fn parse_parquet_metadata_size_hint(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    optional_usize(value, "parquet_metadata_size_hint")
}

fn parse_parquet_full_file_read_threshold(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    optional_usize(value, "parquet_full_file_read_threshold")
}

fn parse_file_repartitioning(value: &Bound<'_, PyAny>) -> PyResult<Option<PyFileRepartitioning>> {
    if value.is_none() {
        return Ok(None);
    }
    value
        .extract::<PyFileRepartitioning>()
        .map(Some)
        .map_err(|_| {
            config_py_error(
                value.py(),
                "invalid_option_value",
                "`intra_file_repartitioning` must be a FileRepartitioning value".to_owned(),
            )
        })
}

fn parse_use_view_types(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    value
        .cast::<PyBool>()
        .map(|value| value.is_true())
        .map_err(|_| {
            config_py_error(
                value.py(),
                "invalid_option_value",
                "`use_view_types` must be a bool".to_owned(),
            )
        })
}

fn optional_usize(value: &Bound<'_, PyAny>, option_name: &str) -> PyResult<Option<usize>> {
    if value.is_none() {
        Ok(None)
    } else {
        usize_option(value, option_name).map(Some)
    }
}

fn usize_option(value: &Bound<'_, PyAny>, option_name: &str) -> PyResult<usize> {
    if value.is_instance_of::<PyBool>() {
        return Err(config_py_error(
            value.py(),
            "invalid_option_value",
            format!("`{option_name}` must be a non-negative integer"),
        ));
    }
    value.extract::<usize>().map_err(|_| {
        config_py_error(
            value.py(),
            "invalid_option_value",
            format!("`{option_name}` must be a non-negative integer"),
        )
    })
}

fn provider_scan_bound_error_to_py(py: Python<'_>, option_name: &str) -> PyErr {
    config_error_to_py(py, format!("{option_name} must be greater than zero"))
}

fn config_error_to_py(py: Python<'_>, error: impl std::fmt::Display) -> PyErr {
    match delta_funnel_error_to_py(
        py,
        delta_funnel::DeltaFunnelError::Config {
            message: error.to_string(),
        },
    ) {
        Ok(error) => error,
        Err(error) => error,
    }
}

#[cfg(test)]
mod tests {
    use pyo3::exceptions::PyTypeError;
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyModule};

    use crate::{deltafunnel, test_support::python_state};

    #[test]
    fn module_exports_typed_provider_options_with_reader_defaults() -> PyResult<()> {
        let _python_state = python_state();
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            let options = module.getattr("ProviderScanOptions")?.call0()?;
            let policy = module
                .getattr("FileRepartitioning")?
                .getattr("FILL_MISSING_PARALLELISM")?;

            assert_eq!(
                options
                    .getattr("max_concurrent_file_reads_per_scan")?
                    .extract::<Option<usize>>()?,
                None
            );
            assert_eq!(
                options
                    .getattr("max_concurrent_file_reads_per_partition")?
                    .extract::<usize>()?,
                3
            );
            assert_eq!(
                options
                    .getattr("output_buffer_capacity_per_partition")?
                    .extract::<usize>()?,
                1
            );
            assert_eq!(
                options
                    .getattr("native_async_prefetch_file_count_per_partition")?
                    .extract::<usize>()?,
                2
            );
            assert_eq!(
                options
                    .getattr("parquet_metadata_size_hint")?
                    .extract::<Option<usize>>()?,
                Some(65_536)
            );
            assert!(
                options
                    .getattr("parquet_full_file_read_threshold")?
                    .is_none()
            );
            assert!(options.getattr("intra_file_repartitioning")?.is(&policy));
            assert!(!options.getattr("use_view_types")?.extract::<bool>()?);
            assert!(options.setattr("use_view_types", true).is_err());

            Ok(())
        })
    }

    #[test]
    fn typed_provider_options_accept_explicit_values() -> PyResult<()> {
        let _python_state = python_state();
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("max_concurrent_file_reads_per_scan", 8)?;
            kwargs.set_item("max_concurrent_file_reads_per_partition", 2)?;
            kwargs.set_item("output_buffer_capacity_per_partition", 4)?;
            kwargs.set_item("native_async_prefetch_file_count_per_partition", 0)?;
            kwargs.set_item("parquet_metadata_size_hint", py.None())?;
            kwargs.set_item("parquet_full_file_read_threshold", 2_097_152)?;
            let rebalance = module.getattr("FileRepartitioning")?.getattr("REBALANCE")?;
            kwargs.set_item("intra_file_repartitioning", &rebalance)?;
            kwargs.set_item("use_view_types", true)?;

            let options = module
                .getattr("ProviderScanOptions")?
                .call((), Some(&kwargs))?;

            assert_eq!(
                options
                    .getattr("max_concurrent_file_reads_per_scan")?
                    .extract::<usize>()?,
                8
            );
            assert_eq!(
                options
                    .getattr("native_async_prefetch_file_count_per_partition")?
                    .extract::<usize>()?,
                0
            );
            assert!(options.getattr("parquet_metadata_size_hint")?.is_none());
            assert_eq!(
                options
                    .getattr("parquet_full_file_read_threshold")?
                    .extract::<usize>()?,
                2_097_152
            );
            assert!(options.getattr("intra_file_repartitioning")?.is(&rebalance));
            assert!(options.getattr("use_view_types")?.extract::<bool>()?);

            Ok(())
        })
    }

    #[test]
    fn typed_provider_options_reject_typos_strings_bools_and_invalid_bounds() -> PyResult<()> {
        let _python_state = python_state();
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let options_type = module.getattr("ProviderScanOptions")?;

            let typo = PyDict::new(py);
            typo.set_item("use_view_type", true)?;
            let error = options_type
                .call((), Some(&typo))
                .expect_err("misspelled keyword must fail");
            assert!(error.is_instance_of::<PyTypeError>(py));

            let string_policy = PyDict::new(py);
            string_policy.set_item("intra_file_repartitioning", "rebalance")?;
            assert_config_error(
                py,
                options_type
                    .call((), Some(&string_policy))
                    .expect_err("string policy must fail"),
                "invalid_option_value",
            )?;

            let bool_number = PyDict::new(py);
            bool_number.set_item("max_concurrent_file_reads_per_partition", true)?;
            assert_config_error(
                py,
                options_type
                    .call((), Some(&bool_number))
                    .expect_err("bool numeric option must fail"),
                "invalid_option_value",
            )?;

            let int_bool = PyDict::new(py);
            int_bool.set_item("use_view_types", 1)?;
            assert_config_error(
                py,
                options_type
                    .call((), Some(&int_bool))
                    .expect_err("integer bool option must fail"),
                "invalid_option_value",
            )?;

            let zero_bound = PyDict::new(py);
            zero_bound.set_item("max_concurrent_file_reads_per_partition", 0)?;
            assert_config_error(
                py,
                options_type
                    .call((), Some(&zero_bound))
                    .expect_err("zero bound must fail"),
                "config",
            )?;

            Ok(())
        })
    }

    fn assert_config_error(py: Python<'_>, error: PyErr, kind: &str) -> PyResult<()> {
        assert_eq!(
            error.value(py).getattr("phase")?.extract::<String>()?,
            "config"
        );
        assert_eq!(error.value(py).getattr("kind")?.extract::<String>()?, kind);
        Ok(())
    }
}
