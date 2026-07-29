//! Python Perfetto diagnostics bridge.

#[cfg(feature = "perfetto-profile")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    env,
    time::{Duration, Instant},
};
#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
use std::{
    ffi::OsString,
    io::{Read, Write},
    mem::{MaybeUninit, size_of},
    os::{fd::AsRawFd, unix::net::UnixStream},
};
#[cfg(feature = "perfetto-profile")]
use std::{io, path::PathBuf};

#[cfg(feature = "perfetto-profile")]
use delta_funnel::perfetto_profile::{
    PerfettoProfileLayer, initialize_perfetto, is_profile_capture_active, is_profile_target,
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
use tracing_subscriber::{Layer, Registry, layer::Filter, prelude::*};

#[cfg(feature = "perfetto-profile")]
use crate::logging::python_logging_layer;
use crate::{
    exception::delta_funnel_py_error,
    logging::{DEFAULT_LOGGER, LOG_FILTER_ENV, parse_logging_filter},
};

const DEFAULT_PERFETTO_WAIT_TIMEOUT_SECONDS: f64 = 10.0;
const PERFETTO_DIAGNOSTICS_PHASE: &str = "perfetto_diagnostics";
#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
const EXTERNAL_TRACEBOX_PID_ENV: &str = "DELTA_FUNNEL_PERFETTO_TRACEBOX_PID";
#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
const EXTERNAL_TRACEBOX_GATE_SOCKET_ENV: &str = "DELTA_FUNNEL_PERFETTO_TRACEBOX_GATE_SOCKET";
#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
const EXTERNAL_WORKLOAD_PID_ENV: &str = "DELTA_FUNNEL_PERFETTO_WORKLOAD_PID";
#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
#[cfg(not(test))]
const EXTERNAL_CAPTURE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(all(feature = "perfetto-profile", target_os = "linux", test))]
const EXTERNAL_CAPTURE_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(50);
#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
#[cfg(not(test))]
const EXTERNAL_CAPTURE_READY_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(all(feature = "perfetto-profile", target_os = "linux", test))]
const EXTERNAL_CAPTURE_READY_TIMEOUT: Duration = Duration::from_millis(50);
#[cfg(feature = "perfetto-profile")]
static PERFETTO_SUBSCRIBER_INSTALLED: AtomicBool = AtomicBool::new(false);

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
    #[cfg(target_os = "linux")]
    let external_capture = take_external_capture_config(py)?;
    py.detach(move || {
        activate_perfetto_diagnostics(
            filter,
            logger,
            tracing::dispatcher::has_been_set,
            initialize_perfetto,
            || {
                #[cfg(target_os = "linux")]
                if let Some(config) = external_capture {
                    return config.release();
                }
                Ok(())
            },
            || wait_for_capture(wait_timeout),
            install_perfetto_subscriber,
        )
    })
    .map_err(|error| perfetto_activation_py_error(py, error))
}

#[cfg(feature = "perfetto-profile")]
pub(super) fn install_perfetto_subscriber(filter: EnvFilter, logger: String) -> bool {
    let installed =
        tracing::subscriber::set_global_default(perfetto_diagnostics_subscriber(filter, logger))
            .is_ok();
    if installed {
        PERFETTO_SUBSCRIBER_INSTALLED.store(true, Ordering::Release);
    }
    installed
}

#[cfg(feature = "perfetto-profile")]
pub(super) fn ensure_perfetto_subscriber(py: Python<'_>) -> PyResult<()> {
    if PERFETTO_SUBSCRIBER_INSTALLED.load(Ordering::Acquire) {
        return Ok(());
    }
    if tracing::dispatcher::has_been_set() {
        return Err(perfetto_diagnostics_py_error(
            py,
            "subscriber_unavailable",
            "ranked profiling cannot attach to the installed tracing subscriber".to_owned(),
        ));
    }
    let filter = parse_logging_filter(py, None, env::var(LOG_FILTER_ENV).ok())?;
    if install_perfetto_subscriber(filter, DEFAULT_LOGGER.to_owned())
        || PERFETTO_SUBSCRIBER_INSTALLED.load(Ordering::Acquire)
    {
        Ok(())
    } else {
        Err(perfetto_diagnostics_py_error(
            py,
            "subscriber_unavailable",
            "ranked profiling could not install its tracing subscriber".to_owned(),
        ))
    }
}

#[cfg(feature = "perfetto-profile")]
fn perfetto_diagnostics_subscriber(
    filter: EnvFilter,
    logger: String,
) -> impl Subscriber + Send + Sync + 'static {
    let logging_layer = python_logging_layer(logger).with_filter(filter);
    let perfetto_layer =
        PerfettoProfileLayer.with_filter(perfetto_capture_filter(is_profile_capture_active));
    Registry::default().with(logging_layer).with(perfetto_layer)
}

#[cfg(feature = "perfetto-profile")]
fn perfetto_capture_filter<S>(
    capture_active: impl Fn() -> bool + Send + Sync + 'static,
) -> impl Filter<S>
where
    S: Subscriber,
{
    filter_fn(move |metadata| is_profile_target(metadata.target()) && capture_active())
}

#[cfg(feature = "perfetto-profile")]
pub(super) fn refresh_perfetto_capture_filter() {
    tracing::callsite::rebuild_interest_cache();
}

#[cfg(feature = "perfetto-profile")]
fn activate_perfetto_diagnostics(
    filter: EnvFilter,
    logger: String,
    subscriber_has_been_set: impl FnOnce() -> bool,
    initialize: impl FnOnce() -> io::Result<()>,
    release_external_capture: impl FnOnce() -> Result<(), ExternalCaptureError>,
    wait_for_capture: impl FnOnce() -> io::Result<()>,
    install_subscriber: impl FnOnce(EnvFilter, String) -> bool,
) -> Result<bool, PerfettoActivationError> {
    if subscriber_has_been_set() {
        return Ok(false);
    }
    initialize().map_err(PerfettoActivationError::ProducerInitialization)?;
    release_external_capture().map_err(PerfettoActivationError::ExternalCapture)?;
    wait_for_capture().map_err(PerfettoActivationError::CaptureReadiness)?;
    Ok(install_subscriber(filter, logger))
}

#[cfg(feature = "perfetto-profile")]
#[derive(Debug)]
struct ExternalCaptureError {
    kind: &'static str,
    message: String,
}

#[cfg(feature = "perfetto-profile")]
impl ExternalCaptureError {
    fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
#[derive(Debug, PartialEq, Eq)]
struct ExternalCaptureConfig {
    tracebox_pid: u32,
    workload_pid: u32,
    gate_socket: PathBuf,
}

#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
impl ExternalCaptureConfig {
    fn validate_workload(&self) -> Result<(), ExternalCaptureError> {
        if self.workload_pid == std::process::id() {
            return Ok(());
        }
        Err(ExternalCaptureError::new(
            "capture_handshake_failed",
            "Perfetto external capture handshake reached an unintended process",
        ))
    }

    fn connect(&self) -> Result<UnixStream, ExternalCaptureError> {
        let gate = UnixStream::connect(&self.gate_socket).map_err(|_| {
            ExternalCaptureError::new(
                "capture_handshake_failed",
                "Perfetto tracebox start gate was not available",
            )
        })?;
        let peer_pid = unix_stream_peer_pid(&gate).map_err(|_| {
            ExternalCaptureError::new(
                "capture_handshake_failed",
                "Perfetto tracebox start gate identity could not be verified",
            )
        })?;
        if peer_pid != self.tracebox_pid {
            return Err(ExternalCaptureError::new(
                "capture_handshake_failed",
                "Perfetto tracebox start gate identity did not match its process",
            ));
        }
        gate.set_read_timeout(Some(EXTERNAL_CAPTURE_HANDSHAKE_TIMEOUT))
            .and_then(|()| gate.set_write_timeout(Some(EXTERNAL_CAPTURE_HANDSHAKE_TIMEOUT)))
            .map_err(|_| {
                ExternalCaptureError::new(
                    "capture_handshake_failed",
                    "Perfetto tracebox start gate timeout could not be configured",
                )
            })?;
        Ok(gate)
    }

    fn release(self) -> Result<(), ExternalCaptureError> {
        self.release_with(|tracebox_pid| {
            crate::yama::authorize_tracebox(tracebox_pid)
                .map_err(|error| ExternalCaptureError::new(error.kind, error.message))
        })
    }

    fn release_with(
        self,
        authorize: impl FnOnce(u32) -> Result<(), ExternalCaptureError>,
    ) -> Result<(), ExternalCaptureError> {
        let mut gate = self.connect()?;
        let mut ready = [0];
        gate.read_exact(&mut ready).map_err(|_| {
            ExternalCaptureError::new(
                "capture_handshake_failed",
                "Perfetto tracebox start gate did not become ready",
            )
        })?;
        if ready != *b"R" {
            return Err(ExternalCaptureError::new(
                "capture_handshake_failed",
                "Perfetto tracebox start gate returned an invalid ready signal",
            ));
        }
        gate.set_read_timeout(Some(EXTERNAL_CAPTURE_READY_TIMEOUT))
            .map_err(|_| {
                ExternalCaptureError::new(
                    "capture_handshake_failed",
                    "Perfetto tracebox readiness timeout could not be configured",
                )
            })?;
        authorize(self.tracebox_pid)?;
        gate.write_all(b"\n").map_err(|_| {
            ExternalCaptureError::new(
                "capture_handshake_failed",
                "Perfetto tracebox start gate could not be released",
            )
        })?;
        let mut readiness = [0];
        match gate.read_exact(&mut readiness) {
            Ok(()) if readiness == [0] => Ok(()),
            Ok(()) => Err(ExternalCaptureError::new(
                "capture_unavailable",
                "managed Perfetto tracebox reported a readiness failure",
            )),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                Err(ExternalCaptureError::new(
                    "capture_timeout",
                    "managed Perfetto tracebox did not become ready before the deadline",
                ))
            }
            Err(_) => Err(ExternalCaptureError::new(
                "capture_unavailable",
                "managed Perfetto tracebox exited before reporting readiness",
            )),
        }
    }
}

#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
fn take_external_capture_config(py: Python<'_>) -> PyResult<Option<ExternalCaptureConfig>> {
    let pid = env::var_os(EXTERNAL_TRACEBOX_PID_ENV);
    let gate_socket = env::var_os(EXTERNAL_TRACEBOX_GATE_SOCKET_ENV);
    let workload_pid = env::var_os(EXTERNAL_WORKLOAD_PID_ENV);
    if pid.is_some() || gate_socket.is_some() || workload_pid.is_some() {
        let environ = py.import("os")?.getattr("environ")?;
        for name in [
            EXTERNAL_TRACEBOX_PID_ENV,
            EXTERNAL_TRACEBOX_GATE_SOCKET_ENV,
            EXTERNAL_WORKLOAD_PID_ENV,
        ] {
            environ.call_method1("pop", (name, py.None()))?;
        }
    }
    parse_external_capture_config(pid, gate_socket, workload_pid)
        .and_then(|config| {
            config
                .map(|config| {
                    config.validate_workload()?;
                    Ok(config)
                })
                .transpose()
        })
        .map_err(|error| perfetto_diagnostics_py_error(py, error.kind, error.message))
}

#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
fn parse_external_capture_config(
    pid: Option<OsString>,
    gate_socket: Option<OsString>,
    workload_pid: Option<OsString>,
) -> Result<Option<ExternalCaptureConfig>, ExternalCaptureError> {
    let (pid, gate_socket, workload_pid) = match (pid, gate_socket, workload_pid) {
        (None, None, None) => return Ok(None),
        (Some(pid), Some(gate_socket), Some(workload_pid)) => (pid, gate_socket, workload_pid),
        _ => {
            return Err(ExternalCaptureError::new(
                "capture_handshake_invalid",
                "Perfetto external capture handshake was incomplete",
            ));
        }
    };
    let tracebox_pid = parse_external_capture_pid(pid, "Perfetto tracebox PID was invalid")?;
    let workload_pid =
        parse_external_capture_pid(workload_pid, "Perfetto workload PID was invalid")?;
    let gate_socket = PathBuf::from(gate_socket);
    if !gate_socket.is_absolute() {
        return Err(ExternalCaptureError::new(
            "capture_handshake_invalid",
            "Perfetto tracebox start gate path was not absolute",
        ));
    }
    Ok(Some(ExternalCaptureConfig {
        tracebox_pid,
        workload_pid,
        gate_socket,
    }))
}

#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
fn parse_external_capture_pid(
    value: OsString,
    message: &'static str,
) -> Result<u32, ExternalCaptureError> {
    value
        .to_str()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0 && *value <= libc::pid_t::MAX as u32)
        .ok_or_else(|| ExternalCaptureError::new("capture_handshake_invalid", message))
}

#[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
fn unix_stream_peer_pid(stream: &UnixStream) -> io::Result<u32> {
    let mut credentials = MaybeUninit::<libc::ucred>::uninit();
    let mut length = size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: credentials points to writable storage for the declared length,
    // and the stream descriptor remains valid for the duration of the call.
    if unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            credentials.as_mut_ptr().cast(),
            &mut length,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if length as usize != size_of::<libc::ucred>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected peer credential size",
        ));
    }
    // SAFETY: a successful SO_PEERCRED call initialized the full ucred value.
    let credentials = unsafe { credentials.assume_init() };
    if credentials.pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid peer process ID",
        ));
    }
    u32::try_from(credentials.pid).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "peer process ID was out of range",
        )
    })
}

#[cfg(feature = "perfetto-profile")]
#[derive(Debug)]
enum PerfettoActivationError {
    ProducerInitialization(io::Error),
    ExternalCapture(ExternalCaptureError),
    CaptureReadiness(io::Error),
}

fn perfetto_diagnostics_py_error(py: Python<'_>, kind: &'static str, message: String) -> PyErr {
    match delta_funnel_py_error(py, PERFETTO_DIAGNOSTICS_PHASE, kind, message, None) {
        Ok(error) => error,
        Err(error) => error,
    }
}

#[cfg(feature = "perfetto-profile")]
fn perfetto_activation_py_error(py: Python<'_>, error: PerfettoActivationError) -> PyErr {
    match error {
        PerfettoActivationError::ProducerInitialization(error) => {
            perfetto_diagnostics_py_error(py, "producer_initialization_failed", error.to_string())
        }
        PerfettoActivationError::ExternalCapture(error) => {
            perfetto_diagnostics_py_error(py, error.kind, error.message)
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

#[cfg(test)]
mod tests {
    #[cfg(feature = "perfetto-profile")]
    use std::{
        cell::{Cell, RefCell},
        io,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use pyo3::prelude::*;
    #[cfg(feature = "perfetto-profile")]
    use pyo3::types::PyDict;
    use pyo3::types::PyModule;
    #[cfg(feature = "perfetto-profile")]
    use tracing::Level;
    #[cfg(feature = "perfetto-profile")]
    use tracing_subscriber::EnvFilter;

    #[cfg(feature = "perfetto-profile")]
    use super::*;
    use crate::deltafunnel;
    #[cfg(feature = "perfetto-profile")]
    use crate::logging::{
        DEFAULT_FILTER,
        tests::{install_capture_handler, only_record},
    };

    #[test]
    fn perfetto_initializer_rejects_invalid_arguments_before_activation() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            for (arguments, expected_kind) in [
                ((py.None(), " ", 10.0), "invalid_logger"),
                ((py.None(), "deltafunnel", -1.0), "invalid_wait_timeout"),
                ((py.None(), "deltafunnel", f64::NAN), "invalid_wait_timeout"),
                ((py.None(), "deltafunnel", 1e19), "invalid_wait_timeout"),
            ] {
                let error = module
                    .call_method1("init_perfetto_diagnostics", arguments)
                    .expect_err("invalid diagnostics arguments must fail");
                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "perfetto_diagnostics"
                );
                assert_eq!(
                    error.value(py).getattr("kind")?.extract::<String>()?,
                    expected_kind
                );
            }

            let error = module
                .call_method1(
                    "init_perfetto_diagnostics",
                    ("delta_funnel=[", "deltafunnel", 10.0),
                )
                .expect_err("invalid diagnostics filter must fail");
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_logging_filter"
            );

            Ok(())
        })
    }

    #[cfg(not(feature = "perfetto-profile"))]
    #[test]
    fn feature_off_perfetto_initializer_is_stable_and_side_effect_free() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let subscriber_was_set = tracing::dispatcher::has_been_set();

            let error = module
                .call_method0("init_perfetto_diagnostics")
                .expect_err("feature-off diagnostics must fail");

            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "perfetto_diagnostics"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "not_available"
            );
            assert!(error.value(py).getattr("context")?.is_none());
            assert_eq!(tracing::dispatcher::has_been_set(), subscriber_was_set);

            Ok(())
        })
    }

    #[cfg(feature = "perfetto-profile")]
    #[test]
    fn perfetto_activation_runs_each_step_once_in_order() -> io::Result<()> {
        let events = RefCell::new(Vec::new());

        let installed = activate_perfetto_diagnostics(
            EnvFilter::new(DEFAULT_FILTER),
            "deltafunnel.test.perfetto".to_owned(),
            || {
                events.borrow_mut().push("check_subscriber");
                false
            },
            || {
                events.borrow_mut().push("initialize_producer");
                Ok(())
            },
            || {
                events.borrow_mut().push("release_external_capture");
                Ok(())
            },
            || {
                events.borrow_mut().push("wait_for_capture");
                Ok(())
            },
            |_filter, logger| {
                assert_eq!(logger, "deltafunnel.test.perfetto");
                events.borrow_mut().push("install_subscriber");
                true
            },
        )
        .map_err(activation_test_error)?;

        assert!(installed);
        assert_eq!(
            events.into_inner(),
            [
                "check_subscriber",
                "initialize_producer",
                "release_external_capture",
                "wait_for_capture",
                "install_subscriber",
            ]
        );
        Ok(())
    }

    #[cfg(feature = "perfetto-profile")]
    #[test]
    fn existing_subscriber_short_circuits_before_perfetto_initialization() -> io::Result<()> {
        let producer_initializations = Cell::new(0);
        let external_capture_releases = Cell::new(0);
        let readiness_waits = Cell::new(0);
        let subscriber_installations = Cell::new(0);
        let installed = activate_perfetto_diagnostics(
            EnvFilter::new(DEFAULT_FILTER),
            DEFAULT_LOGGER.to_owned(),
            || true,
            || {
                producer_initializations.set(producer_initializations.get() + 1);
                Ok(())
            },
            || {
                external_capture_releases.set(external_capture_releases.get() + 1);
                Ok(())
            },
            || {
                readiness_waits.set(readiness_waits.get() + 1);
                Ok(())
            },
            |_, _| {
                subscriber_installations.set(subscriber_installations.get() + 1);
                true
            },
        )
        .map_err(activation_test_error)?;

        assert!(!installed);
        assert_eq!(producer_initializations.get(), 0);
        assert_eq!(external_capture_releases.get(), 0);
        assert_eq!(readiness_waits.get(), 0);
        assert_eq!(subscriber_installations.get(), 0);
        Ok(())
    }

    #[cfg(feature = "perfetto-profile")]
    #[test]
    fn subscriber_installation_race_returns_false_after_readiness() -> io::Result<()> {
        let events = RefCell::new(Vec::new());
        let installed = activate_perfetto_diagnostics(
            EnvFilter::new(DEFAULT_FILTER),
            DEFAULT_LOGGER.to_owned(),
            || {
                events.borrow_mut().push("check_subscriber");
                false
            },
            || {
                events.borrow_mut().push("initialize_producer");
                Ok(())
            },
            || {
                events.borrow_mut().push("release_external_capture");
                Ok(())
            },
            || {
                events.borrow_mut().push("wait_for_capture");
                Ok(())
            },
            |_, _| {
                events.borrow_mut().push("install_subscriber_lost_race");
                false
            },
        )
        .map_err(activation_test_error)?;

        assert!(!installed);
        assert_eq!(
            events.into_inner(),
            [
                "check_subscriber",
                "initialize_producer",
                "release_external_capture",
                "wait_for_capture",
                "install_subscriber_lost_race",
            ]
        );
        Ok(())
    }

    #[cfg(feature = "perfetto-profile")]
    #[test]
    fn repeated_perfetto_activation_does_not_repeat_side_effects() -> io::Result<()> {
        let subscriber_is_set = Cell::new(false);
        let producer_initializations = Cell::new(0);
        let external_capture_releases = Cell::new(0);
        let readiness_waits = Cell::new(0);
        let subscriber_installations = Cell::new(0);

        for expected in [true, false] {
            let installed = activate_perfetto_diagnostics(
                EnvFilter::new(DEFAULT_FILTER),
                DEFAULT_LOGGER.to_owned(),
                || subscriber_is_set.get(),
                || {
                    producer_initializations.set(producer_initializations.get() + 1);
                    Ok(())
                },
                || {
                    external_capture_releases.set(external_capture_releases.get() + 1);
                    Ok(())
                },
                || {
                    readiness_waits.set(readiness_waits.get() + 1);
                    Ok(())
                },
                |_, _| {
                    subscriber_installations.set(subscriber_installations.get() + 1);
                    subscriber_is_set.set(true);
                    true
                },
            )
            .map_err(activation_test_error)?;
            assert_eq!(installed, expected);
        }

        assert_eq!(producer_initializations.get(), 1);
        assert_eq!(external_capture_releases.get(), 1);
        assert_eq!(readiness_waits.get(), 1);
        assert_eq!(subscriber_installations.get(), 1);
        Ok(())
    }

    #[cfg(feature = "perfetto-profile")]
    #[test]
    fn external_capture_failure_stops_activation_before_readiness() {
        let events = RefCell::new(Vec::new());
        let error = activate_perfetto_diagnostics(
            EnvFilter::new(DEFAULT_FILTER),
            DEFAULT_LOGGER.to_owned(),
            || {
                events.borrow_mut().push("check_subscriber");
                false
            },
            || {
                events.borrow_mut().push("initialize_producer");
                Ok(())
            },
            || {
                events.borrow_mut().push("release_external_capture");
                Err(ExternalCaptureError::new(
                    "capture_handshake_failed",
                    "gate failed",
                ))
            },
            || {
                events.borrow_mut().push("wait_for_capture");
                Ok(())
            },
            |_, _| {
                events.borrow_mut().push("install_subscriber");
                true
            },
        )
        .expect_err("a failed external handshake must stop activation");

        assert!(matches!(
            error,
            PerfettoActivationError::ExternalCapture(ExternalCaptureError {
                kind: "capture_handshake_failed",
                ..
            })
        ));
        assert_eq!(
            events.into_inner(),
            [
                "check_subscriber",
                "initialize_producer",
                "release_external_capture",
            ]
        );
    }

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    #[test]
    fn external_capture_configuration_is_paired_positive_and_absolute() {
        assert!(
            parse_external_capture_config(None, None, None)
                .expect("an absent handshake must be valid")
                .is_none()
        );
        assert_eq!(
            parse_external_capture_config(
                Some(OsString::from("123")),
                Some(OsString::from("/run/user/123/gate.sock")),
                Some(OsString::from("456")),
            )
            .expect("a complete handshake must be valid"),
            Some(ExternalCaptureConfig {
                tracebox_pid: 123,
                workload_pid: 456,
                gate_socket: PathBuf::from("/run/user/123/gate.sock"),
            })
        );

        for (pid, gate_socket, workload_pid) in [
            (Some("123"), None, None),
            (None, Some("/tmp/gate.sock"), None),
            (None, None, Some("456")),
            (Some("123"), Some("/tmp/gate.sock"), None),
            (Some("0"), Some("/tmp/gate.sock"), Some("456")),
            (Some("-1"), Some("/tmp/gate.sock"), Some("456")),
            (Some("not-a-pid"), Some("/tmp/gate.sock"), Some("456")),
            (Some("123"), Some("/tmp/gate.sock"), Some("0")),
            (Some("123"), Some("/tmp/gate.sock"), Some("-1")),
            (Some("123"), Some("/tmp/gate.sock"), Some("not-a-pid")),
            (Some("123"), Some("relative/gate.sock"), Some("456")),
            (Some("123"), Some(""), Some("456")),
        ] {
            let error = parse_external_capture_config(
                pid.map(OsString::from),
                gate_socket.map(OsString::from),
                workload_pid.map(OsString::from),
            )
            .expect_err("a malformed handshake must fail");
            assert_eq!(error.kind, "capture_handshake_invalid");
        }
    }

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    #[test]
    fn external_capture_is_bound_to_the_intended_workload_process() {
        let config = ExternalCaptureConfig {
            tracebox_pid: 1,
            workload_pid: std::process::id(),
            gate_socket: PathBuf::from("/tmp/gate.sock"),
        };
        config
            .validate_workload()
            .expect("the intended workload process must be accepted");

        let error = ExternalCaptureConfig {
            workload_pid: std::process::id().saturating_add(1),
            ..config
        }
        .validate_workload()
        .expect_err("an inherited child handshake must be rejected");
        assert_eq!(error.kind, "capture_handshake_failed");
    }

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    #[test]
    fn external_capture_connects_only_to_the_expected_process() -> io::Result<()> {
        use std::os::unix::net::UnixListener;

        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("gate.sock");
        let listener = UnixListener::bind(&socket)?;
        let actual_pid = std::process::id();

        let gate = ExternalCaptureConfig {
            tracebox_pid: actual_pid,
            workload_pid: actual_pid,
            gate_socket: socket.clone(),
        }
        .connect()
        .expect("the current process must match its Unix socket credentials");
        drop(gate);
        let (_connection, _) = listener.accept()?;

        let error = ExternalCaptureConfig {
            tracebox_pid: actual_pid.saturating_add(1),
            workload_pid: actual_pid,
            gate_socket: socket,
        }
        .connect()
        .expect_err("a mismatched peer PID must be rejected");
        assert_eq!(error.kind, "capture_handshake_failed");
        Ok(())
    }

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    #[test]
    fn external_capture_rejects_eof_invalid_and_stalled_ready_signals() -> io::Result<()> {
        use std::{os::unix::net::UnixListener, thread};

        for behavior in ["eof", "invalid", "stalled"] {
            let directory = tempfile::tempdir()?;
            let socket = directory.path().join("gate.sock");
            let listener = UnixListener::bind(&socket)?;
            let server = thread::spawn(move || -> io::Result<()> {
                let (mut connection, _) = listener.accept()?;
                match behavior {
                    "invalid" => connection.write_all(b"X")?,
                    "stalled" => thread::sleep(Duration::from_millis(200)),
                    _ => {}
                }
                Ok(())
            });
            let error = ExternalCaptureConfig {
                tracebox_pid: std::process::id(),
                workload_pid: std::process::id(),
                gate_socket: socket,
            }
            .release_with(|_| {
                Err(ExternalCaptureError::new(
                    "authorization_reached",
                    "an invalid ready signal must not authorize tracebox",
                ))
            })
            .expect_err("an invalid ready signal must fail the handshake");
            assert_eq!(error.kind, "capture_handshake_failed");
            server
                .join()
                .expect("the fake gate thread must not panic")?;
        }
        Ok(())
    }

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    #[test]
    fn external_capture_waits_for_its_managed_tracebox_readiness() -> io::Result<()> {
        use std::{os::unix::net::UnixListener, thread};

        for (behavior, expected_error) in [
            ("success", None),
            ("failure", Some("capture_unavailable")),
            ("eof", Some("capture_unavailable")),
            ("stalled", Some("capture_timeout")),
        ] {
            let directory = tempfile::tempdir()?;
            let socket = directory.path().join("gate.sock");
            let listener = UnixListener::bind(&socket)?;
            let server = thread::spawn(move || -> io::Result<()> {
                let (mut connection, _) = listener.accept()?;
                connection.write_all(b"R")?;
                let mut release = [0];
                connection.read_exact(&mut release)?;
                if release != *b"\n" {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected release signal",
                    ));
                }
                match behavior {
                    "success" => connection.write_all(&[0])?,
                    "failure" => connection.write_all(&[1])?,
                    "stalled" => thread::sleep(Duration::from_millis(200)),
                    _ => {}
                }
                Ok(())
            });
            let result = ExternalCaptureConfig {
                tracebox_pid: std::process::id(),
                workload_pid: std::process::id(),
                gate_socket: socket,
            }
            .release_with(|tracebox_pid| {
                assert_eq!(tracebox_pid, std::process::id());
                Ok(())
            });
            match expected_error {
                Some(kind) => assert_eq!(
                    result
                        .expect_err("the managed readiness failure must be reported")
                        .kind,
                    kind
                ),
                None => result.expect("the managed readiness signal must release activation"),
            }
            server
                .join()
                .expect("the fake gate thread must not panic")?;
        }
        Ok(())
    }

    #[cfg(feature = "perfetto-profile")]
    #[test]
    fn perfetto_filter_rechecks_capture_state_for_the_same_callsite() {
        let active = Arc::new(AtomicBool::new(false));
        let event_count = Arc::new(AtomicUsize::new(0));
        let capture_active = Arc::clone(&active);
        let subscriber =
            Registry::default().with(EventCounter(Arc::clone(&event_count)).with_filter(
                perfetto_capture_filter(move || capture_active.load(Ordering::Acquire)),
            ));

        tracing::subscriber::with_default(subscriber, || {
            emit_profile_callsite();
            assert_eq!(event_count.load(Ordering::Acquire), 0);
            active.store(true, Ordering::Release);
            refresh_perfetto_capture_filter();
            emit_profile_callsite();
            assert_eq!(event_count.load(Ordering::Acquire), 1);
            active.store(false, Ordering::Release);
            refresh_perfetto_capture_filter();
            emit_profile_callsite();
            assert_eq!(event_count.load(Ordering::Acquire), 1);
        });
    }

    #[cfg(feature = "perfetto-profile")]
    fn emit_profile_callsite() {
        tracing::trace!(target: "delta_funnel::profile", "profile state transition");
    }

    #[cfg(feature = "perfetto-profile")]
    struct EventCounter(Arc<AtomicUsize>);

    #[cfg(feature = "perfetto-profile")]
    impl<S> Layer<S> for EventCounter
    where
        S: Subscriber,
    {
        fn on_event(
            &self,
            _event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0.fetch_add(1, Ordering::Release);
        }
    }

    #[cfg(feature = "perfetto-profile")]
    #[test]
    fn combined_subscriber_keeps_logging_and_perfetto_filters_independent() -> PyResult<()> {
        Python::attach(|py| {
            let logger_name = "deltafunnel.test.combined";
            let (logger, handler, records) = install_capture_handler(py, logger_name)?;
            let subscriber = perfetto_diagnostics_subscriber(
                EnvFilter::new("delta_funnel=info"),
                logger_name.to_owned(),
            );

            tracing::subscriber::with_default(subscriber, || {
                assert!(!tracing::enabled!(
                    target: "delta_funnel::profile",
                    Level::TRACE
                ));
                assert!(!tracing::enabled!(
                    target: "tiberius_raw_bulk::protocol",
                    Level::INFO
                ));
                assert!(tracing::enabled!(target: "delta_funnel", Level::INFO));
                assert!(!tracing::enabled!(target: "unrelated", Level::TRACE));
                tracing::trace!(target: "delta_funnel::profile", "profile.trace");
                tracing::info!(
                    target: "tiberius_raw_bulk::protocol",
                    "protocol.bulk_load.finalize.result"
                );
                tracing::info!(target: "delta_funnel", "application.info");
                tracing::trace!(target: "unrelated", "unrelated.trace");
            });

            logger.call_method1("removeHandler", (&handler,))?;
            let record = only_record(&records)?;
            assert_eq!(
                record.getattr("msg")?.extract::<String>()?,
                "application.info"
            );
            Ok(())
        })
    }

    #[cfg(feature = "perfetto-profile")]
    #[test]
    fn perfetto_activation_errors_have_stable_python_fields() -> PyResult<()> {
        Python::attach(|py| {
            for (error, expected_kind) in [
                (
                    PerfettoActivationError::ProducerInitialization(io::Error::other(
                        "producer unavailable",
                    )),
                    "producer_initialization_failed",
                ),
                (
                    PerfettoActivationError::CaptureReadiness(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "capture timed out",
                    )),
                    "capture_timeout",
                ),
                (
                    PerfettoActivationError::CaptureReadiness(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "capture disconnected",
                    )),
                    "capture_unavailable",
                ),
                (
                    PerfettoActivationError::CaptureReadiness(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "timeout cannot be represented",
                    )),
                    "invalid_wait_timeout",
                ),
                (
                    PerfettoActivationError::ExternalCapture(ExternalCaptureError::new(
                        "capture_handshake_failed",
                        "gate failed",
                    )),
                    "capture_handshake_failed",
                ),
            ] {
                let error = perfetto_activation_py_error(py, error);
                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "perfetto_diagnostics"
                );
                assert_eq!(
                    error.value(py).getattr("kind")?.extract::<String>()?,
                    expected_kind
                );
                assert!(error.value(py).getattr("context")?.is_none());
            }
            Ok(())
        })
    }

    #[cfg(feature = "perfetto-profile")]
    #[test]
    fn inactive_capture_does_not_change_preview_result() -> Result<(), Box<dyn std::error::Error>> {
        let subscriber = perfetto_diagnostics_subscriber(
            EnvFilter::new("off"),
            "deltafunnel.test.inactive_capture".to_owned(),
        );
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;

        let preview = tracing::subscriber::with_default(subscriber, || {
            runtime
                .block_on(async {
                    let mut session = delta_funnel::DeltaFunnelSession::new(
                        delta_funnel::SessionOptions::default(),
                    )?;
                    let table = session.table_from_sql("SELECT 1 AS value").await?;
                    session.preview_table(&table, 20).await
                })
                .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
        })?;

        assert!(preview.text().contains('1'));
        Ok(())
    }

    #[cfg(feature = "perfetto-profile")]
    #[test]
    fn inactive_capture_does_not_change_dry_run_write_result() -> PyResult<()> {
        let subscriber = perfetto_diagnostics_subscriber(
            EnvFilter::new("off"),
            "deltafunnel.test.inactive_capture_write".to_owned(),
        );

        tracing::subscriber::with_default(subscriber, || {
            Python::attach(|py| {
                let module = PyModule::new(py, "deltafunnel")?;
                deltafunnel(&module)?;
                let session = module.getattr("Session")?.call0()?;
                let table = session.call_method1("table_from_sql", ("SELECT 1 AS value",))?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("schema", "dbo")?;
                kwargs.set_item("table", "diagnostic_write")?;
                kwargs.set_item("load_mode", "create_and_load")?;
                kwargs.set_item("connection_string", "server=tcp:sql.example.com")?;
                kwargs.set_item("dry_run", true)?;
                kwargs.set_item("progress", false)?;

                let report = table
                    .call_method("write_to_mssql", (), Some(&kwargs))?
                    .cast_into::<PyDict>()?;
                assert_eq!(
                    report
                        .get_item("run_mode")?
                        .expect("dry-run report must include run_mode")
                        .extract::<String>()?,
                    "dry_run"
                );
                Ok(())
            })
        })
    }

    #[cfg(feature = "perfetto-profile")]
    fn activation_test_error(error: PerfettoActivationError) -> io::Error {
        io::Error::other(format!("unexpected activation error: {error:?}"))
    }
}
