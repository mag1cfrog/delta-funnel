//! Python Perfetto diagnostics bridge.

use std::{
    env,
    time::{Duration, Instant},
};
#[cfg(feature = "perfetto-profile")]
use std::{io, path::PathBuf};

#[cfg(feature = "perfetto-profile")]
use delta_funnel::perfetto_profile::{
    PerfettoProfileLayer, initialize_perfetto, is_profile_target,
    run_perfetto_diagnostics_cli_with_args, wait_for_capture,
};
use pyo3::prelude::*;
use pyo3::types::PyModuleMethods;
#[cfg(feature = "perfetto-profile")]
use tracing::Subscriber;
use tracing_subscriber::EnvFilter;
#[cfg(feature = "perfetto-profile")]
use tracing_subscriber::filter::filter_fn;
#[cfg(feature = "perfetto-profile")]
use tracing_subscriber::{Layer, Registry, prelude::*};

#[cfg(feature = "perfetto-profile")]
use crate::logging::python_logging_layer;
use crate::{
    exception::delta_funnel_py_error,
    logging::{DEFAULT_LOGGER, LOG_FILTER_ENV, parse_logging_filter},
};

const DEFAULT_PERFETTO_WAIT_TIMEOUT_SECONDS: f64 = 10.0;
const PERFETTO_DIAGNOSTICS_PHASE: &str = "perfetto_diagnostics";

pub(crate) fn add_perfetto_diagnostics(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(init_perfetto_diagnostics, module)?)?;
    module.add_function(wrap_pyfunction!(_run_perfetto_cli, module)?)?;
    Ok(())
}

#[cfg(feature = "perfetto-profile")]
#[pyfunction]
fn _run_perfetto_cli(py: Python<'_>) -> PyResult<i32> {
    // Python console scripts normalize away the interpreter and script path here.
    let args = py
        .import("sys")?
        .getattr("argv")?
        .extract::<Vec<PathBuf>>()?;
    Ok(py.detach(move || {
        run_perfetto_diagnostics_cli_with_args(
            args.into_iter().skip(1).map(PathBuf::into_os_string),
        )
    }))
}

#[cfg(not(feature = "perfetto-profile"))]
#[pyfunction]
fn _run_perfetto_cli() -> i32 {
    eprintln!(
        "delta-funnel-perfetto: this deltafunnel build does not include Perfetto diagnostics"
    );
    69
}

#[pyfunction]
#[pyo3(signature = (
    filter=None,
    logger=DEFAULT_LOGGER.to_owned(),
    wait_timeout_seconds=DEFAULT_PERFETTO_WAIT_TIMEOUT_SECONDS,
))]
fn init_perfetto_diagnostics(
    py: Python<'_>,
    filter: Option<String>,
    logger: String,
    wait_timeout_seconds: f64,
) -> PyResult<bool> {
    let filter = parse_logging_filter(py, filter, env::var(LOG_FILTER_ENV).ok())?;
    if logger.trim().is_empty() {
        return Err(perfetto_diagnostics_py_error(
            py,
            "invalid_logger",
            "Perfetto diagnostics logger name must not be empty".to_owned(),
        ));
    }
    let wait_timeout = parse_perfetto_wait_timeout(py, wait_timeout_seconds)?;

    init_perfetto_diagnostics_inner(py, filter, logger, wait_timeout)
}

fn parse_perfetto_wait_timeout(py: Python<'_>, seconds: f64) -> PyResult<Duration> {
    let timeout = Duration::try_from_secs_f64(seconds).map_err(|error| {
        perfetto_diagnostics_py_error(
            py,
            "invalid_wait_timeout",
            format!("invalid Perfetto diagnostics wait timeout: {error}"),
        )
    })?;
    Instant::now().checked_add(timeout).ok_or_else(|| {
        perfetto_diagnostics_py_error(
            py,
            "invalid_wait_timeout",
            format!("Perfetto diagnostics wait timeout {timeout:?} is too large"),
        )
    })?;
    Ok(timeout)
}

#[cfg(not(feature = "perfetto-profile"))]
fn init_perfetto_diagnostics_inner(
    py: Python<'_>,
    _filter: EnvFilter,
    _logger: String,
    _wait_timeout: Duration,
) -> PyResult<bool> {
    Err(perfetto_diagnostics_py_error(
        py,
        "not_available",
        "this deltafunnel build does not include Perfetto diagnostics".to_owned(),
    ))
}

#[cfg(feature = "perfetto-profile")]
fn init_perfetto_diagnostics_inner(
    py: Python<'_>,
    filter: EnvFilter,
    logger: String,
    wait_timeout: Duration,
) -> PyResult<bool> {
    py.detach(move || {
        activate_perfetto_diagnostics(
            filter,
            logger,
            wait_timeout,
            tracing::dispatcher::has_been_set,
            initialize_perfetto,
            wait_for_capture,
            install_perfetto_subscriber,
        )
    })
    .map_err(|error| perfetto_activation_py_error(py, error))
}

#[cfg(feature = "perfetto-profile")]
fn install_perfetto_subscriber(filter: EnvFilter, logger: String) -> bool {
    tracing::subscriber::set_global_default(perfetto_diagnostics_subscriber(filter, logger)).is_ok()
}

#[cfg(feature = "perfetto-profile")]
pub(crate) fn perfetto_diagnostics_subscriber(
    filter: EnvFilter,
    logger: String,
) -> impl Subscriber + Send + Sync + 'static {
    let logging_layer = python_logging_layer(logger).with_filter(filter);
    let perfetto_layer = PerfettoProfileLayer
        .with_filter(filter_fn(|metadata| is_profile_target(metadata.target())));
    Registry::default().with(logging_layer).with(perfetto_layer)
}

#[cfg(feature = "perfetto-profile")]
pub(crate) fn activate_perfetto_diagnostics(
    filter: EnvFilter,
    logger: String,
    wait_timeout: Duration,
    subscriber_has_been_set: impl FnOnce() -> bool,
    initialize: impl FnOnce() -> io::Result<()>,
    wait_for_capture: impl FnOnce(Duration) -> io::Result<()>,
    install_subscriber: impl FnOnce(EnvFilter, String) -> bool,
) -> Result<bool, PerfettoActivationError> {
    if subscriber_has_been_set() {
        return Ok(false);
    }
    initialize().map_err(PerfettoActivationError::ProducerInitialization)?;
    wait_for_capture(wait_timeout).map_err(PerfettoActivationError::CaptureReadiness)?;
    Ok(install_subscriber(filter, logger))
}

#[cfg(feature = "perfetto-profile")]
#[derive(Debug)]
pub(crate) enum PerfettoActivationError {
    ProducerInitialization(io::Error),
    CaptureReadiness(io::Error),
}

fn perfetto_diagnostics_py_error(py: Python<'_>, kind: &'static str, message: String) -> PyErr {
    match delta_funnel_py_error(py, PERFETTO_DIAGNOSTICS_PHASE, kind, message, None) {
        Ok(error) => error,
        Err(error) => error,
    }
}

#[cfg(feature = "perfetto-profile")]
pub(crate) fn perfetto_activation_py_error(
    py: Python<'_>,
    error: PerfettoActivationError,
) -> PyErr {
    match error {
        PerfettoActivationError::ProducerInitialization(error) => {
            perfetto_diagnostics_py_error(py, "producer_initialization_failed", error.to_string())
        }
        PerfettoActivationError::CaptureReadiness(error) => {
            let kind = match error.kind() {
                io::ErrorKind::InvalidInput => "invalid_wait_timeout",
                io::ErrorKind::TimedOut => "capture_timeout",
                _ => "capture_unavailable",
            };
            perfetto_diagnostics_py_error(py, kind, error.to_string())
        }
    }
}
