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
fn perfetto_diagnostics_subscriber(
    filter: EnvFilter,
    logger: String,
) -> impl Subscriber + Send + Sync + 'static {
    let logging_layer = python_logging_layer(logger).with_filter(filter);
    let perfetto_layer = PerfettoProfileLayer
        .with_filter(filter_fn(|metadata| is_profile_target(metadata.target())));
    Registry::default().with(logging_layer).with(perfetto_layer)
}

#[cfg(feature = "perfetto-profile")]
fn activate_perfetto_diagnostics(
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
enum PerfettoActivationError {
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
fn perfetto_activation_py_error(py: Python<'_>, error: PerfettoActivationError) -> PyErr {
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

#[cfg(test)]
mod tests {
    #[cfg(feature = "perfetto-profile")]
    use std::{
        cell::{Cell, RefCell},
        io,
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
        let timeout = Duration::from_millis(250);

        let installed = activate_perfetto_diagnostics(
            EnvFilter::new(DEFAULT_FILTER),
            "deltafunnel.test.perfetto".to_owned(),
            timeout,
            || {
                events.borrow_mut().push("check_subscriber");
                false
            },
            || {
                events.borrow_mut().push("initialize_producer");
                Ok(())
            },
            |actual_timeout| {
                assert_eq!(actual_timeout, timeout);
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
        let readiness_waits = Cell::new(0);
        let subscriber_installations = Cell::new(0);
        let installed = activate_perfetto_diagnostics(
            EnvFilter::new(DEFAULT_FILTER),
            DEFAULT_LOGGER.to_owned(),
            Duration::from_secs(1),
            || true,
            || {
                producer_initializations.set(producer_initializations.get() + 1);
                Ok(())
            },
            |_| {
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
            Duration::from_secs(1),
            || {
                events.borrow_mut().push("check_subscriber");
                false
            },
            || {
                events.borrow_mut().push("initialize_producer");
                Ok(())
            },
            |_| {
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
        let readiness_waits = Cell::new(0);
        let subscriber_installations = Cell::new(0);

        for expected in [true, false] {
            let installed = activate_perfetto_diagnostics(
                EnvFilter::new(DEFAULT_FILTER),
                DEFAULT_LOGGER.to_owned(),
                Duration::from_secs(1),
                || subscriber_is_set.get(),
                || {
                    producer_initializations.set(producer_initializations.get() + 1);
                    Ok(())
                },
                |_| {
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
        assert_eq!(readiness_waits.get(), 1);
        assert_eq!(subscriber_installations.get(), 1);
        Ok(())
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
                assert!(tracing::enabled!(
                    target: "delta_funnel::profile",
                    Level::TRACE
                ));
                assert!(tracing::enabled!(
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
