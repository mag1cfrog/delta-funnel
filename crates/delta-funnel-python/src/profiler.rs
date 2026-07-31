//! Python profiling configuration and ranked-profile lifecycle entry points.

use std::{
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBool};

use crate::{exception::delta_funnel_py_error, session::config_py_error};

#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
mod capture;

const RANKED_PROFILE_PHASE: &str = "ranked_profile";
const DEFAULT_MAX_OPERATOR_ACTIVITY_SPANS: u64 = delta_funnel::DEFAULT_MAX_OPERATOR_ACTIVITY_SPANS;

pub(crate) fn execution_profile_mode(enabled: bool) -> delta_funnel::ExecutionProfileMode {
    if enabled {
        delta_funnel::ExecutionProfileMode::Detailed
    } else {
        delta_funnel::ExecutionProfileMode::Disabled
    }
}

/// Immutable configuration for one operation-scoped ranked profiling report.
///
/// Use 1000 Hz for short operations and 100 Hz for longer, bounded-volume captures.
#[pyclass(frozen, name = "RankedProfileConfig", module = "deltafunnel")]
pub(crate) struct PyRankedProfileConfig {
    report_path: PathBuf,
    sample_hz: u16,
    artifact_path: Option<PathBuf>,
    max_operator_activity_spans: NonZeroU64,
}

impl PyRankedProfileConfig {
    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    fn report_path(&self) -> &Path {
        &self.report_path
    }

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    const fn sampling_frequency(&self) -> u16 {
        self.sample_hz
    }

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    fn artifact_path(&self) -> Option<&Path> {
        self.artifact_path.as_deref()
    }

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    const fn maximum_operator_activity_spans(&self) -> NonZeroU64 {
        self.max_operator_activity_spans
    }
}

#[pymethods]
impl PyRankedProfileConfig {
    #[new]
    #[pyo3(signature = (
        report_path,
        *,
        sample_hz=1000,
        artifact_path=None,
        max_operator_activity_spans=DEFAULT_MAX_OPERATOR_ACTIVITY_SPANS,
    ))]
    fn new(
        py: Python<'_>,
        report_path: PathBuf,
        #[pyo3(from_py_with = parse_sample_hz)] sample_hz: u16,
        artifact_path: Option<PathBuf>,
        #[pyo3(from_py_with = parse_max_operator_activity_spans)] max_operator_activity_spans: u64,
    ) -> PyResult<Self> {
        if report_path.file_name().is_none() {
            return Err(config_py_error(
                py,
                "invalid_option_value",
                "`report_path` must name a file".to_owned(),
            ));
        }
        if artifact_path
            .as_deref()
            .is_some_and(|path| path.file_name().is_none())
        {
            return Err(config_py_error(
                py,
                "invalid_option_value",
                "`artifact_path` must name a file".to_owned(),
            ));
        }
        let max_operator_activity_spans =
            NonZeroU64::new(max_operator_activity_spans).ok_or_else(|| {
                config_py_error(
                    py,
                    "invalid_option_value",
                    "`max_operator_activity_spans` must be a positive integer".to_owned(),
                )
            })?;
        Ok(Self {
            report_path,
            sample_hz,
            artifact_path,
            max_operator_activity_spans,
        })
    }

    #[getter]
    fn get_report_path(&self) -> &Path {
        &self.report_path
    }

    #[getter]
    const fn sample_hz(&self) -> u16 {
        self.sample_hz
    }

    #[getter]
    fn get_artifact_path(&self) -> Option<&Path> {
        self.artifact_path.as_deref()
    }

    #[getter]
    const fn max_operator_activity_spans(&self) -> u64 {
        self.max_operator_activity_spans.get()
    }

    fn __repr__(&self) -> String {
        let artifact_path = self
            .artifact_path
            .as_ref()
            .map_or_else(|| "None".to_owned(), |path| format!("{path:?}"));
        format!(
            "deltafunnel.RankedProfileConfig(report_path={:?}, sample_hz={}, artifact_path={}, max_operator_activity_spans={})",
            self.report_path, self.sample_hz, artifact_path, self.max_operator_activity_spans,
        )
    }
}

pub(crate) fn add_profiler(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyRankedProfileConfig>()
}

pub(crate) struct RankedProfileCapture {
    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    capture: capture::OperationCapture,
}

pub(crate) fn in_ranked_profile_scope<T>(
    profile: Option<&RankedProfileCapture>,
    operation: impl FnOnce() -> T,
) -> T {
    match profile {
        Some(profile) => profile.in_scope(operation),
        None => operation(),
    }
}

pub(crate) fn start_ranked_profile(
    py: Python<'_>,
    config: Option<&PyRankedProfileConfig>,
) -> PyResult<Option<RankedProfileCapture>> {
    let Some(config) = config else {
        return Ok(None);
    };

    #[cfg(not(all(feature = "perfetto-profile", target_os = "linux")))]
    {
        let _ = config;
        Err(ranked_profile_py_error(
            py,
            "not_available",
            "ranked profiling requires a diagnostics-enabled Linux build".to_owned(),
        ))
    }

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    {
        let output = resolve_output_path(py, config.report_path())?;
        let artifact_output = config
            .artifact_path()
            .map(|path| resolve_output_path(py, path))
            .transpose()?;
        validate_output_paths(py, &output, artifact_output.as_deref())?;
        crate::perfetto_diagnostics::ensure_perfetto_subscriber(py)?;
        let sample_hz = config.sampling_frequency();
        let max_operator_activity_spans = config.maximum_operator_activity_spans();
        let tracebox = tracebox_launcher(py)?;
        py.detach(move || {
            capture::OperationCapture::start(
                output,
                artifact_output,
                sample_hz,
                max_operator_activity_spans,
                tracebox,
            )
        })
        .map(|capture| Some(RankedProfileCapture { capture }))
        .map_err(|error| ranked_profile_failure_py_error(py, error))
    }
}

#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
fn validate_output_paths(
    py: Python<'_>,
    output: &Path,
    artifact_output: Option<&Path>,
) -> PyResult<()> {
    let paths_alias = |left: &Path, right: &Path| {
        delta_funnel::perfetto_profile::output_paths_alias(left, right).map_err(|_| {
            ranked_profile_py_error(
                py,
                "output_unavailable",
                "profile output paths could not be inspected".to_owned(),
            )
        })
    };
    if let Some(artifact_output) = artifact_output
        && paths_alias(output, artifact_output)?
    {
        return Err(config_py_error(
            py,
            "invalid_option_value",
            "`RankedProfileConfig.report_path` and `RankedProfileConfig.artifact_path` must name different files"
                .to_owned(),
        ));
    }
    Ok(())
}

#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
fn resolve_output_path(py: Python<'_>, output: &Path) -> PyResult<PathBuf> {
    std::path::absolute(output).map_err(|_| {
        ranked_profile_py_error(
            py,
            "output_unavailable",
            "profile output path could not be resolved".to_owned(),
        )
    })
}

#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
fn tracebox_launcher(py: Python<'_>) -> PyResult<PathBuf> {
    if let Some(tracebox) = std::env::var_os("TRACEBOX") {
        return Ok(tracebox.into());
    }
    let extension = py
        .import("deltafunnel.deltafunnel")
        .and_then(|module| module.getattr("__file__"))
        .and_then(|path| path.extract::<PathBuf>())
        .map_err(|_| {
            ranked_profile_py_error(
                py,
                "tracebox_unavailable",
                "packaged Perfetto tracebox launcher could not be located".to_owned(),
            )
        })?;
    let package = extension.parent().ok_or_else(|| {
        ranked_profile_py_error(
            py,
            "tracebox_unavailable",
            "packaged Perfetto tracebox launcher could not be located".to_owned(),
        )
    })?;
    Ok(package.join("perfetto/delta-funnel-tracebox"))
}

impl RankedProfileCapture {
    fn in_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
        {
            self.capture.in_scope(operation)
        }

        #[cfg(not(all(feature = "perfetto-profile", target_os = "linux")))]
        operation()
    }

    pub(crate) fn finish(self, py: Python<'_>) -> PyResult<()> {
        #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
        {
            py.detach(move || self.capture.finish())
                .map_err(|error| ranked_profile_failure_py_error(py, error))
        }

        #[cfg(not(all(feature = "perfetto-profile", target_os = "linux")))]
        {
            let _ = self;
            Err(ranked_profile_py_error(
                py,
                "not_available",
                "ranked profiling requires a diagnostics-enabled Linux build".to_owned(),
            ))
        }
    }
}

fn ranked_profile_py_error(py: Python<'_>, kind: &'static str, message: String) -> PyErr {
    match delta_funnel_py_error(py, RANKED_PROFILE_PHASE, kind, message, None) {
        Ok(error) => error,
        Err(error) => error,
    }
}

#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
fn ranked_profile_failure_py_error(py: Python<'_>, error: capture::ProfilerFailure) -> PyErr {
    ranked_profile_py_error(py, error.kind, error.message)
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

fn parse_max_operator_activity_spans(value: &Bound<'_, PyAny>) -> PyResult<u64> {
    let maximum_spans = if value.is_instance_of::<PyBool>() {
        None
    } else {
        value.extract::<u64>().ok().filter(|maximum| *maximum > 0)
    };
    maximum_spans.ok_or_else(|| {
        config_py_error(
            value.py(),
            "invalid_option_value",
            "`max_operator_activity_spans` must be a positive integer".to_owned(),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use pyo3::IntoPyObjectExt;
    use pyo3::exceptions::PyAttributeError;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyModule};

    use super::*;
    use crate::deltafunnel;

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    const DEFAULT_MAX_OPERATOR_ACTIVITY_SPANS_NON_ZERO: NonZeroU64 =
        match NonZeroU64::new(DEFAULT_MAX_OPERATOR_ACTIVITY_SPANS) {
            Some(maximum_spans) => maximum_spans,
            None => panic!("the default operator activity span limit must be positive"),
        };

    #[test]
    fn ranked_config_and_exact_boolean_expose_distinct_python_contracts() -> PyResult<()> {
        let stub = include_str!("../deltafunnel.pyi");
        assert!(!stub.contains("class ExecutionProfileConfig:"));
        assert!(stub.contains("execution_profile: bool = False"));
        assert!(stub.contains("class RankedProfileConfig:"));
        assert!(stub.contains("report_path: str | PathLike[str]"));
        assert!(stub.contains("sample_hz: Literal[100, 1000] = 1000"));
        assert!(stub.contains("artifact_path: str | PathLike[str] | None = None"));
        assert!(stub.contains("max_operator_activity_spans: int = 100_000"));
        assert_eq!(
            execution_profile_mode(false),
            delta_funnel::ExecutionProfileMode::Disabled
        );
        assert_eq!(
            execution_profile_mode(true),
            delta_funnel::ExecutionProfileMode::Detailed
        );

        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            assert!(module.getattr("ProfilerConfig").is_err());
            assert!(module.getattr("ExecutionProfileConfig").is_err());

            let ranked_profile = module.getattr("RankedProfileConfig")?;
            let default = ranked_profile.call1(("query.profile.html",))?;
            assert_eq!(
                default.getattr("report_path")?.extract::<PathBuf>()?,
                PathBuf::from("query.profile.html")
            );
            assert_eq!(default.getattr("sample_hz")?.extract::<u16>()?, 1000);
            assert_eq!(
                default
                    .getattr("max_operator_activity_spans")?
                    .extract::<u64>()?,
                DEFAULT_MAX_OPERATOR_ACTIVITY_SPANS
            );
            assert_eq!(
                default
                    .getattr("artifact_path")?
                    .extract::<Option<PathBuf>>()?,
                None
            );
            assert_eq!(
                default.repr()?.to_str()?,
                "deltafunnel.RankedProfileConfig(report_path=\"query.profile.html\", sample_hz=1000, artifact_path=None, max_operator_activity_spans=100000)"
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
            kwargs.set_item("artifact_path", "lower-volume.dfprofile")?;
            kwargs.set_item("max_operator_activity_spans", 500_000)?;
            let lower_volume = ranked_profile.call((pathlib_path,), Some(&kwargs))?;
            assert_eq!(
                lower_volume.getattr("report_path")?.extract::<PathBuf>()?,
                PathBuf::from("lower-volume.profile.html")
            );
            assert_eq!(lower_volume.getattr("sample_hz")?.extract::<u16>()?, 100);
            assert_eq!(
                lower_volume
                    .getattr("max_operator_activity_spans")?
                    .extract::<u64>()?,
                500_000
            );
            assert_eq!(
                lower_volume
                    .getattr("artifact_path")?
                    .extract::<PathBuf>()?,
                PathBuf::from("lower-volume.dfprofile")
            );

            let invalid_values = [
                99_i32.into_bound_py_any(py)?,
                true.into_bound_py_any(py)?,
                (-1_i32).into_bound_py_any(py)?,
                "1000".into_bound_py_any(py)?,
            ];
            for invalid in invalid_values {
                let kwargs = PyDict::new(py);
                kwargs.set_item("sample_hz", invalid)?;
                let error = ranked_profile
                    .call(("query.profile.html",), Some(&kwargs))
                    .expect_err("invalid sample frequency must be rejected");
                let value = error.value(py);
                assert_eq!(value.getattr("phase")?.extract::<String>()?, "config");
                assert_eq!(
                    value.getattr("kind")?.extract::<String>()?,
                    "invalid_option_value"
                );
            }

            let invalid_span_limits = [
                0_i32.into_bound_py_any(py)?,
                true.into_bound_py_any(py)?,
                (-1_i32).into_bound_py_any(py)?,
                "100000".into_bound_py_any(py)?,
            ];
            for invalid in invalid_span_limits {
                let kwargs = PyDict::new(py);
                kwargs.set_item("max_operator_activity_spans", invalid)?;
                let error = ranked_profile
                    .call(("query.profile.html",), Some(&kwargs))
                    .expect_err("invalid operator activity span limit must be rejected");
                let value = error.value(py);
                assert_eq!(value.getattr("phase")?.extract::<String>()?, "config");
                assert_eq!(
                    value.getattr("kind")?.extract::<String>()?,
                    "invalid_option_value"
                );
            }

            let empty_output = ranked_profile
                .call1(("",))
                .expect_err("an empty output path must be rejected");
            assert_eq!(
                empty_output
                    .value(py)
                    .getattr("kind")?
                    .extract::<String>()?,
                "invalid_option_value"
            );
            let kwargs = PyDict::new(py);
            kwargs.set_item("artifact_path", "")?;
            let empty_artifact = ranked_profile
                .call(("query.profile.html",), Some(&kwargs))
                .expect_err("an empty artifact output path must be rejected");
            assert_eq!(
                empty_artifact
                    .value(py)
                    .getattr("kind")?
                    .extract::<String>()?,
                "invalid_option_value"
            );
            Ok(())
        })
    }

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    #[test]
    fn profile_outputs_must_resolve_to_distinct_files() -> PyResult<()> {
        Python::attach(|py| {
            let config = PyRankedProfileConfig {
                report_path: PathBuf::from("profile.html"),
                sample_hz: 1_000,
                artifact_path: Some(PathBuf::from("./profile.html")),
                max_operator_activity_spans: DEFAULT_MAX_OPERATOR_ACTIVITY_SPANS_NON_ZERO,
            };
            let output = resolve_output_path(py, config.report_path())?;
            let artifact_output = config
                .artifact_path()
                .map(|output| resolve_output_path(py, output))
                .transpose()?;
            assert!(output.is_absolute());
            assert!(artifact_output.as_deref().is_some_and(Path::is_absolute));
            let error = validate_output_paths(py, &output, artifact_output.as_deref())
                .expect_err("HTML and artifact outputs must not alias");
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_option_value"
            );

            Ok(())
        })
    }
}
