//! Python profiler configuration.

use std::path::{Path, PathBuf};

use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBool};

use crate::session::config_py_error;

/// Immutable configuration for one operation-scoped profiling report.
#[pyclass(frozen, name = "ProfilerConfig", module = "deltafunnel")]
pub(crate) struct PyProfilerConfig {
    output: PathBuf,
    sample_hz: u16,
}

#[pymethods]
impl PyProfilerConfig {
    #[new]
    #[pyo3(signature = (output, *, sample_hz=1000))]
    fn new(
        py: Python<'_>,
        output: PathBuf,
        #[pyo3(from_py_with = parse_sample_hz)] sample_hz: u16,
    ) -> PyResult<Self> {
        if output.file_name().is_none() {
            return Err(config_py_error(
                py,
                "invalid_option_value",
                "`output` must name a file".to_owned(),
            ));
        }
        Ok(Self { output, sample_hz })
    }

    #[getter]
    fn output(&self) -> &Path {
        &self.output
    }

    #[getter]
    const fn sample_hz(&self) -> u16 {
        self.sample_hz
    }

    fn __repr__(&self) -> String {
        format!(
            "deltafunnel.ProfilerConfig(output={:?}, sample_hz={})",
            self.output, self.sample_hz
        )
    }
}

pub(crate) fn add_profiler(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyProfilerConfig>()
}

fn parse_sample_hz(value: &Bound<'_, PyAny>) -> PyResult<u16> {
    let sample_hz = if value.is_instance_of::<PyBool>() {
        None
    } else {
        value.extract::<u16>().ok()
    };
    match sample_hz {
        Some(sample_hz @ (100 | 1000)) => Ok(sample_hz),
        _ => Err(config_py_error(
            value.py(),
            "invalid_option_value",
            "`sample_hz` must be 100 or 1000".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use pyo3::IntoPyObjectExt;
    use pyo3::exceptions::PyAttributeError;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyModule};

    use super::*;
    use crate::deltafunnel;

    #[test]
    fn profiler_config_exposes_one_immutable_validated_python_contract() -> PyResult<()> {
        let stub = include_str!("../deltafunnel.pyi");
        assert!(stub.contains("class ProfilerConfig:"));
        assert!(stub.contains("sample_hz: Literal[100, 1000] = 1000"));

        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let profiler = module.getattr("ProfilerConfig")?;

            let default = profiler.call1(("query.profile.html",))?;
            assert_eq!(
                default.getattr("output")?.extract::<PathBuf>()?,
                PathBuf::from("query.profile.html")
            );
            assert_eq!(default.getattr("sample_hz")?.extract::<u16>()?, 1000);
            assert_eq!(
                default.repr()?.to_str()?,
                "deltafunnel.ProfilerConfig(output=\"query.profile.html\", sample_hz=1000)"
            );
            assert!(
                default
                    .setattr("sample_hz", 100)
                    .expect_err("the config must be immutable")
                    .is_instance_of::<PyAttributeError>(py)
            );

            let pathlib_path = py
                .import("pathlib")?
                .getattr("Path")?
                .call1(("lower-volume.profile.html",))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("sample_hz", 100)?;
            let lower_volume = profiler.call((pathlib_path,), Some(&kwargs))?;
            assert_eq!(
                lower_volume.getattr("output")?.extract::<PathBuf>()?,
                PathBuf::from("lower-volume.profile.html")
            );
            assert_eq!(lower_volume.getattr("sample_hz")?.extract::<u16>()?, 100);

            let invalid_values = [
                99_i32.into_bound_py_any(py)?,
                true.into_bound_py_any(py)?,
                (-1_i32).into_bound_py_any(py)?,
                "1000".into_bound_py_any(py)?,
            ];
            for invalid in invalid_values {
                let kwargs = PyDict::new(py);
                kwargs.set_item("sample_hz", invalid)?;
                let error = profiler
                    .call(("query.profile.html",), Some(&kwargs))
                    .expect_err("invalid sample frequency must be rejected");
                let value = error.value(py);
                assert_eq!(value.getattr("phase")?.extract::<String>()?, "config");
                assert_eq!(
                    value.getattr("kind")?.extract::<String>()?,
                    "invalid_option_value"
                );
            }

            let empty_output = profiler
                .call1(("",))
                .expect_err("an empty output path must be rejected");
            assert_eq!(
                empty_output
                    .value(py)
                    .getattr("kind")?
                    .extract::<String>()?,
                "invalid_option_value"
            );
            Ok(())
        })
    }
}
