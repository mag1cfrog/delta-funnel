//! Python logging bridge for Rust tracing events.

use std::collections::BTreeMap;
use std::env;
use std::fmt;
#[cfg(feature = "perfetto-profile")]
use std::io;
use std::time::{Duration, Instant};

#[cfg(feature = "perfetto-profile")]
use delta_funnel::perfetto_profile::{
    PerfettoProfileLayer, initialize_perfetto, is_profile_target, wait_for_capture,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModuleMethods};
use tracing::{Event, Level, Subscriber, field::Field, field::Visit};
#[cfg(feature = "perfetto-profile")]
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::{
    EnvFilter, Layer, Registry, layer::Context, prelude::*, registry::LookupSpan,
};

use crate::exception::delta_funnel_py_error;

const DEFAULT_LOGGER: &str = "deltafunnel";
const DEFAULT_FILTER: &str = "delta_funnel=info,arrow_tiberius=info";
const DEFAULT_PERFETTO_WAIT_TIMEOUT_SECONDS: f64 = 10.0;
const LOG_FILTER_ENV: &str = "DELTAFUNNEL_LOG";
const PERFETTO_DIAGNOSTICS_PHASE: &str = "perfetto_diagnostics";

pub(crate) fn add_logging(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(init_logging, module)?)?;
    module.add_function(wrap_pyfunction!(init_perfetto_diagnostics, module)?)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (filter=None, logger=DEFAULT_LOGGER.to_owned()))]
fn init_logging(py: Python<'_>, filter: Option<String>, logger: String) -> PyResult<bool> {
    let filter = parse_logging_filter(py, filter, env::var(LOG_FILTER_ENV).ok())?;
    let subscriber = Registry::default()
        .with(filter)
        .with(python_logging_layer(logger));

    Ok(tracing::subscriber::set_global_default(subscriber).is_ok())
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

fn parse_logging_filter(
    py: Python<'_>,
    filter: Option<String>,
    env_filter: Option<String>,
) -> PyResult<EnvFilter> {
    let filter = filter
        .or(env_filter)
        .filter(|filter| !filter.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_FILTER.to_owned());
    EnvFilter::try_new(filter).map_err(|error| {
        invalid_logging_filter_py_error(py, format!("invalid DeltaFunnel logging filter: {error}"))
    })
}

fn invalid_logging_filter_py_error(py: Python<'_>, message: String) -> PyErr {
    match delta_funnel_py_error(py, "config", "invalid_logging_filter", message, None) {
        Ok(error) => error,
        Err(error) => error,
    }
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

fn python_logging_layer(logger_name: String) -> PythonLoggingLayer {
    PythonLoggingLayer { logger_name }
}

struct PythonLoggingLayer {
    logger_name: String,
}

impl<S> Layer<S> for PythonLoggingLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = FieldVisitor::default();
        event.record(&mut fields);
        let message = fields
            .fields
            .get("message")
            .cloned()
            .or_else(|| fields.fields.get("telemetry_event").cloned())
            .unwrap_or_else(|| event.metadata().name().to_owned());
        let span_names = ctx.event_scope(event).map(|scope| {
            scope
                .from_root()
                .map(|span| span.metadata().name())
                .collect::<Vec<_>>()
                .join(",")
        });

        let _ = Python::try_attach(|py| {
            let logging = py.import("logging")?;
            let logger = logging.call_method1("getLogger", (&self.logger_name,))?;
            let extra = PyDict::new(py);
            extra.set_item("deltafunnel_target", event.metadata().target())?;
            extra.set_item("deltafunnel_level", event.metadata().level().as_str())?;
            if let Some(span_names) = span_names {
                extra.set_item("deltafunnel_spans", span_names)?;
            }
            for (key, value) in &fields.fields {
                if key != "message" {
                    extra.set_item(format!("deltafunnel_{key}"), value)?;
                }
            }
            let kwargs = PyDict::new(py);
            kwargs.set_item("extra", extra)?;
            logger.call_method(
                "log",
                (python_log_level(event.metadata().level()), message),
                Some(&kwargs),
            )?;
            Ok::<_, PyErr>(())
        });
    }
}

fn python_log_level(level: &Level) -> u8 {
    match *level {
        Level::ERROR => 40,
        Level::WARN => 30,
        Level::INFO => 20,
        Level::DEBUG | Level::TRACE => 10,
    }
}

#[derive(Default)]
struct FieldVisitor {
    fields: BTreeMap<String, String>,
}

impl FieldVisitor {
    fn record_value(&mut self, field: &Field, value: impl Into<String>) {
        self.fields.insert(field.name().to_owned(), value.into());
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_value(field, format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_value(field, value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_value(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_value(field, value.to_string());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record_value(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field, value.to_string());
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "perfetto-profile")]
    use std::cell::{Cell, RefCell};
    #[cfg(feature = "perfetto-profile")]
    use std::io;
    #[cfg(feature = "perfetto-profile")]
    use std::time::Duration;

    use pyo3::prelude::*;
    use pyo3::types::{PyAny, PyAnyMethods, PyDict, PyList, PyModule};
    use tracing::Level;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::prelude::*;

    #[cfg(feature = "perfetto-profile")]
    use super::DEFAULT_LOGGER;
    use super::{DEFAULT_FILTER, PythonLoggingLayer, parse_logging_filter, python_log_level};
    use crate::deltafunnel;

    #[test]
    fn module_exports_logging_initializers() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            assert!(module.hasattr("init_logging")?);
            assert!(module.hasattr("init_perfetto_diagnostics")?);

            Ok(())
        })
    }

    #[test]
    fn init_logging_returns_bool_and_repeated_calls_do_not_panic() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let logging = py.import("logging")?;
            let logger = logging.call_method1("getLogger", ("deltafunnel.test.global",))?;
            let null_handler = logging.getattr("NullHandler")?.call0()?;
            logger.setattr("propagate", false)?;
            logger.call_method1("addHandler", (&null_handler,))?;

            let _first = module
                .call_method1("init_logging", (DEFAULT_FILTER, "deltafunnel.test.global"))?
                .extract::<bool>()?;
            let second = module.call_method0("init_logging")?.extract::<bool>()?;

            assert!(!second);

            Ok(())
        })
    }

    #[test]
    fn pyi_stub_exports_logging_initializers() {
        let stub = include_str!("../deltafunnel.pyi");

        assert!(stub.contains("def init_logging("));
        assert!(stub.contains("def init_perfetto_diagnostics("));
        assert!(stub.contains("wait_timeout_seconds: float = 10.0"));
    }

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

        let installed = super::activate_perfetto_diagnostics(
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
        let installed = super::activate_perfetto_diagnostics(
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
        let installed = super::activate_perfetto_diagnostics(
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
            let installed = super::activate_perfetto_diagnostics(
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
            let subscriber = super::perfetto_diagnostics_subscriber(
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
                    super::PerfettoActivationError::ProducerInitialization(io::Error::other(
                        "producer unavailable",
                    )),
                    "producer_initialization_failed",
                ),
                (
                    super::PerfettoActivationError::CaptureReadiness(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "capture timed out",
                    )),
                    "capture_timeout",
                ),
                (
                    super::PerfettoActivationError::CaptureReadiness(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "capture disconnected",
                    )),
                    "capture_unavailable",
                ),
                (
                    super::PerfettoActivationError::CaptureReadiness(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "timeout cannot be represented",
                    )),
                    "invalid_wait_timeout",
                ),
            ] {
                let error = super::perfetto_activation_py_error(py, error);
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
        let subscriber = super::perfetto_diagnostics_subscriber(
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
        let subscriber = super::perfetto_diagnostics_subscriber(
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
    fn activation_test_error(error: super::PerfettoActivationError) -> io::Error {
        io::Error::other(format!("unexpected activation error: {error:?}"))
    }

    #[test]
    fn invalid_logging_filter_uses_delta_funnel_error_shape() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let error = match module.call_method1("init_logging", ("delta_funnel=[",)) {
                Ok(_) => {
                    return Err(pyo3::exceptions::PyAssertionError::new_err(
                        "expected invalid logging filter error",
                    ));
                }
                Err(error) => error,
            };

            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_logging_filter"
            );
            assert!(error.value(py).getattr("context")?.is_none());

            Ok(())
        })
    }

    #[test]
    fn logging_filter_uses_deltafunnel_log_value_when_filter_is_none() -> PyResult<()> {
        Python::attach(|py| {
            let (logger, handler, records) = install_capture_handler(py, "deltafunnel.test.env")?;
            let filter = parse_logging_filter(py, None, Some("delta_funnel=warn".to_owned()))?;
            let subscriber = tracing_subscriber::registry()
                .with(filter)
                .with(PythonLoggingLayer {
                    logger_name: "deltafunnel.test.env".to_owned(),
                });
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(target: "delta_funnel", "filtered.info");
                tracing::warn!(target: "delta_funnel", "kept.warn");
            });

            logger.call_method1("removeHandler", (&handler,))?;
            let record = only_record(&records)?;
            assert_eq!(record.getattr("levelno")?.extract::<u8>()?, 30);
            assert_eq!(record.getattr("msg")?.extract::<String>()?, "kept.warn");

            Ok(())
        })
    }

    #[test]
    fn explicit_logging_filter_wins_over_deltafunnel_log_value() -> PyResult<()> {
        Python::attach(|py| {
            let (logger, handler, records) =
                install_capture_handler(py, "deltafunnel.test.explicit_filter")?;
            let filter = parse_logging_filter(
                py,
                Some("delta_funnel=info".to_owned()),
                Some("delta_funnel=error".to_owned()),
            )?;
            let subscriber = tracing_subscriber::registry()
                .with(filter)
                .with(PythonLoggingLayer {
                    logger_name: "deltafunnel.test.explicit_filter".to_owned(),
                });
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(target: "delta_funnel", "explicit.info");
            });

            logger.call_method1("removeHandler", (&handler,))?;
            let record = only_record(&records)?;
            assert_eq!(record.getattr("levelno")?.extract::<u8>()?, 20);
            assert_eq!(record.getattr("msg")?.extract::<String>()?, "explicit.info");

            Ok(())
        })
    }

    #[test]
    fn python_log_level_maps_tracing_levels() {
        assert_eq!(python_log_level(&tracing::Level::ERROR), 40);
        assert_eq!(python_log_level(&tracing::Level::WARN), 30);
        assert_eq!(python_log_level(&tracing::Level::INFO), 20);
        assert_eq!(python_log_level(&tracing::Level::DEBUG), 10);
        assert_eq!(python_log_level(&tracing::Level::TRACE), 10);
    }

    #[test]
    fn scoped_logging_layer_forwards_events_to_python_logging() -> PyResult<()> {
        Python::attach(|py| {
            let (logger, handler, records) = install_capture_handler(py, "deltafunnel.test.basic")?;
            let subscriber = logging_subscriber("deltafunnel.test.basic");
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(
                    target: "delta_funnel",
                    telemetry_event = "test.event",
                    output_name = "orders",
                    "test.event"
                );
            });

            logger.call_method1("removeHandler", (&handler,))?;
            let record = only_record(&records)?;
            assert_eq!(
                record.getattr("name")?.extract::<String>()?,
                "deltafunnel.test.basic"
            );
            assert_eq!(record.getattr("levelno")?.extract::<u8>()?, 20);
            assert_eq!(record.getattr("msg")?.extract::<String>()?, "test.event");
            assert_eq!(
                record
                    .getattr("deltafunnel_telemetry_event")?
                    .extract::<String>()?,
                "test.event"
            );
            assert_eq!(
                record
                    .getattr("deltafunnel_output_name")?
                    .extract::<String>()?,
                "orders"
            );

            Ok(())
        })
    }

    #[test]
    fn scoped_logging_layer_preserves_span_names_and_typed_fields() -> PyResult<()> {
        Python::attach(|py| {
            let (logger, handler, records) =
                install_capture_handler(py, "deltafunnel.test.fields")?;
            let subscriber = logging_subscriber("deltafunnel.test.fields");
            tracing::subscriber::with_default(subscriber, || {
                let span = tracing::info_span!(
                    target: "delta_funnel",
                    "delta_funnel.workflow"
                );
                let _guard = span.enter();
                tracing::info!(
                    target: "delta_funnel",
                    telemetry_event = "typed.event",
                    signed = -7_i64,
                    unsigned = 7_u64,
                    ratio = 1.5_f64,
                    enabled = true,
                    debug_value = ?["north", "south"],
                    "typed.event"
                );
            });

            logger.call_method1("removeHandler", (&handler,))?;
            let record = only_record(&records)?;
            assert_eq!(
                record.getattr("deltafunnel_spans")?.extract::<String>()?,
                "delta_funnel.workflow"
            );
            assert_eq!(
                record.getattr("deltafunnel_signed")?.extract::<String>()?,
                "-7"
            );
            assert_eq!(
                record
                    .getattr("deltafunnel_unsigned")?
                    .extract::<String>()?,
                "7"
            );
            assert_eq!(
                record.getattr("deltafunnel_ratio")?.extract::<String>()?,
                "1.5"
            );
            assert_eq!(
                record.getattr("deltafunnel_enabled")?.extract::<String>()?,
                "true"
            );
            assert_eq!(
                record
                    .getattr("deltafunnel_debug_value")?
                    .extract::<String>()?,
                "[\"north\", \"south\"]"
            );

            Ok(())
        })
    }

    #[test]
    fn scoped_logging_layer_uses_telemetry_event_when_message_is_absent() -> PyResult<()> {
        Python::attach(|py| {
            let (logger, handler, records) =
                install_capture_handler(py, "deltafunnel.test.fallback")?;
            let subscriber = logging_subscriber("deltafunnel.test.fallback");
            tracing::subscriber::with_default(subscriber, || {
                tracing::event!(
                    target: "delta_funnel",
                    Level::INFO,
                    telemetry_event = "fallback.event"
                );
            });

            logger.call_method1("removeHandler", (&handler,))?;
            let record = only_record(&records)?;
            assert_eq!(
                record.getattr("msg")?.extract::<String>()?,
                "fallback.event"
            );

            Ok(())
        })
    }

    #[test]
    fn scoped_logging_layer_respects_env_filter() -> PyResult<()> {
        Python::attach(|py| {
            let (logger, handler, records) =
                install_capture_handler(py, "deltafunnel.test.filter")?;
            let subscriber = tracing_subscriber::registry()
                .with(EnvFilter::new("delta_funnel=warn"))
                .with(PythonLoggingLayer {
                    logger_name: "deltafunnel.test.filter".to_owned(),
                });
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(target: "delta_funnel", "filtered.info");
                tracing::warn!(target: "delta_funnel", "kept.warn");
                tracing::error!(target: "other_target", "filtered.error");
            });

            logger.call_method1("removeHandler", (&handler,))?;
            let record = only_record(&records)?;
            assert_eq!(record.getattr("levelno")?.extract::<u8>()?, 30);
            assert_eq!(record.getattr("msg")?.extract::<String>()?, "kept.warn");

            Ok(())
        })
    }

    #[test]
    fn parquet_io_summary_extras_preserve_the_python_string_contract() -> PyResult<()> {
        Python::attach(|py| {
            let (logger, handler, records) =
                install_capture_handler(py, "deltafunnel.test.parquet_summary")?;
            let subscriber = filtered_logging_subscriber(
                "deltafunnel.test.parquet_summary",
                "delta_funnel=debug",
            );
            tracing::subscriber::with_default(subscriber, || {
                emit_available_parquet_io_summary();
                emit_unavailable_parquet_io_summary();
            });

            logger.call_method1("removeHandler", (&handler,))?;
            assert_eq!(records.len(), 2);

            let available = records.get_item(0)?;
            assert_eq!(
                available
                    .getattr("deltafunnel_telemetry_event")?
                    .extract::<String>()?,
                "delta_provider_parquet_io_summary"
            );
            assert_eq!(
                available
                    .getattr("deltafunnel_outcome")?
                    .extract::<String>()?,
                "success"
            );
            assert_eq!(
                available
                    .getattr("deltafunnel_metrics_available")?
                    .extract::<String>()?,
                "true"
            );
            for (field, value) in [
                ("deltafunnel_snapshot_version", "7"),
                ("deltafunnel_parquet_data_file_range_get_operations", "0"),
                ("deltafunnel_parquet_data_file_full_get_operations", "2"),
                ("deltafunnel_parquet_data_file_bytes_received", "512"),
                ("deltafunnel_parquet_data_file_opened_bytes", "2048"),
            ] {
                assert_eq!(available.getattr(field)?.extract::<String>()?, value);
            }

            let unavailable = records.get_item(1)?;
            assert_eq!(
                unavailable
                    .getattr("deltafunnel_metrics_available")?
                    .extract::<String>()?,
                "false"
            );
            for field in [
                "deltafunnel_parquet_data_file_range_get_operations",
                "deltafunnel_parquet_data_file_full_get_operations",
                "deltafunnel_parquet_data_file_bytes_received",
                "deltafunnel_parquet_data_file_opened_bytes",
            ] {
                assert!(!unavailable.hasattr(field)?);
            }

            Ok(())
        })
    }

    #[test]
    fn execution_profile_summary_extras_preserve_the_python_string_contract() -> PyResult<()> {
        Python::attach(|py| {
            let (logger, handler, records) =
                install_capture_handler(py, "deltafunnel.test.execution_profile_summary")?;
            let subscriber = filtered_logging_subscriber(
                "deltafunnel.test.execution_profile_summary",
                "delta_funnel=debug",
            );
            tracing::subscriber::with_default(subscriber, || {
                emit_available_execution_profile_summary();
                emit_minimal_execution_profile_summary();
            });

            logger.call_method1("removeHandler", (&handler,))?;
            assert_eq!(records.len(), 2);

            let available = records.get_item(0)?;
            assert_eq!(available.getattr("levelno")?.extract::<u8>()?, 10);
            assert_eq!(
                available.getattr("msg")?.extract::<String>()?,
                "query_execution_profile_terminal"
            );
            for (field, value) in [
                (
                    "deltafunnel_telemetry_event",
                    "query_execution_profile_terminal",
                ),
                ("deltafunnel_scope", "preview"),
                ("deltafunnel_outcome", "error"),
                ("deltafunnel_partial", "true"),
                ("deltafunnel_delta_funnel_row_limit", "20"),
                ("deltafunnel_operator_count", "4"),
                ("deltafunnel_operators_with_metrics", "3"),
                ("deltafunnel_root_output_rows", "42"),
                ("deltafunnel_max_elapsed_compute_operator", "HashJoinExec"),
                ("deltafunnel_max_elapsed_compute_nanos", "100"),
            ] {
                assert_eq!(available.getattr(field)?.extract::<String>()?, value);
            }

            let minimal = records.get_item(1)?;
            for (field, value) in [
                (
                    "deltafunnel_telemetry_event",
                    "query_execution_profile_terminal",
                ),
                ("deltafunnel_scope", "mssql_output"),
                ("deltafunnel_outcome", "success"),
                ("deltafunnel_partial", "false"),
                ("deltafunnel_operator_count", "1"),
                ("deltafunnel_operators_with_metrics", "0"),
            ] {
                assert_eq!(minimal.getattr(field)?.extract::<String>()?, value);
            }
            for field in [
                "deltafunnel_delta_funnel_row_limit",
                "deltafunnel_root_output_rows",
                "deltafunnel_max_elapsed_compute_operator",
                "deltafunnel_max_elapsed_compute_nanos",
            ] {
                assert!(!minimal.hasattr(field)?);
            }

            Ok(())
        })
    }

    #[test]
    fn debug_summaries_require_both_rust_and_python_debug_levels() -> PyResult<()> {
        Python::attach(|py| {
            let (logger, handler, records) =
                install_capture_handler(py, "deltafunnel.test.debug_summary_gates")?;

            // Python admits DEBUG, but the Rust filter rejects the event first.
            let info_subscriber = filtered_logging_subscriber(
                "deltafunnel.test.debug_summary_gates",
                "delta_funnel=info",
            );
            tracing::subscriber::with_default(info_subscriber, || {
                emit_available_parquet_io_summary();
                emit_available_execution_profile_summary();
            });
            assert!(records.is_empty());

            // Rust admits DEBUG, but the Python handler rejects the forwarded record.
            handler.call_method1("setLevel", (20,))?;
            let debug_subscriber = filtered_logging_subscriber(
                "deltafunnel.test.debug_summary_gates",
                "delta_funnel=debug",
            );
            tracing::subscriber::with_default(debug_subscriber, || {
                emit_available_parquet_io_summary();
                emit_available_execution_profile_summary();
            });
            assert!(records.is_empty());

            logger.call_method1("removeHandler", (&handler,))?;
            Ok(())
        })
    }

    #[test]
    fn scoped_logging_layer_ignores_python_handler_failures() -> PyResult<()> {
        Python::attach(|py| {
            let logging = py.import("logging")?;
            let locals = PyDict::new(py);
            py.run(
                c"
import logging

class FailingHandler(logging.Handler):
    def emit(self, record):
        raise RuntimeError('handler failed')
",
                Some(&locals),
                Some(&locals),
            )?;
            let handler_type = locals
                .get_item("FailingHandler")?
                .ok_or_else(|| pyo3::exceptions::PyAssertionError::new_err("missing handler"))?;
            let handler = handler_type.call0()?;
            let logger = logging.call_method1("getLogger", ("deltafunnel.test.failure",))?;
            logger.setattr("propagate", false)?;
            logger.call_method1("setLevel", (10,))?;
            logger.call_method1("addHandler", (&handler,))?;

            let subscriber = logging_subscriber("deltafunnel.test.failure");
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(target: "delta_funnel", "handler.failure");
            });

            logger.call_method1("removeHandler", (&handler,))?;

            Ok(())
        })
    }

    fn logging_subscriber(logger_name: &str) -> impl tracing::Subscriber + Send + Sync + 'static {
        tracing_subscriber::registry().with(PythonLoggingLayer {
            logger_name: logger_name.to_owned(),
        })
    }

    fn filtered_logging_subscriber(
        logger_name: &str,
        filter: &str,
    ) -> impl tracing::Subscriber + Send + Sync + 'static {
        tracing_subscriber::registry()
            .with(EnvFilter::new(filter))
            .with(PythonLoggingLayer {
                logger_name: logger_name.to_owned(),
            })
    }

    fn emit_available_parquet_io_summary() {
        tracing::debug!(
            target: "delta_funnel",
            telemetry_event = "delta_provider_parquet_io_summary",
            source_name = "orders",
            snapshot_version = 7_u64,
            reader_backend = "native_async",
            outcome = "success",
            metrics_available = true,
            parquet_data_file_range_get_operations = 0_u64,
            parquet_data_file_full_get_operations = 2_u64,
            parquet_data_file_bytes_received = 512_u64,
            parquet_data_file_opened_bytes = 2048_u64,
            message = "delta_provider_parquet_io_summary"
        );
    }

    fn emit_unavailable_parquet_io_summary() {
        tracing::debug!(
            target: "delta_funnel",
            telemetry_event = "delta_provider_parquet_io_summary",
            source_name = "orders",
            snapshot_version = 7_u64,
            reader_backend = "official_kernel",
            outcome = "success",
            metrics_available = false,
            message = "delta_provider_parquet_io_summary"
        );
    }

    fn emit_available_execution_profile_summary() {
        tracing::debug!(
            target: "delta_funnel",
            telemetry_event = "query_execution_profile_terminal",
            scope = "preview",
            outcome = "error",
            partial = true,
            delta_funnel_row_limit = Some(20_u64),
            operator_count = 4_u64,
            operators_with_metrics = 3_u64,
            root_output_rows = Some(42_u64),
            max_elapsed_compute_operator = Some("HashJoinExec"),
            max_elapsed_compute_nanos = Some(100_u64),
        );
    }

    fn emit_minimal_execution_profile_summary() {
        tracing::debug!(
            target: "delta_funnel",
            telemetry_event = "query_execution_profile_terminal",
            scope = "mssql_output",
            outcome = "success",
            partial = false,
            delta_funnel_row_limit = None::<u64>,
            operator_count = 1_u64,
            operators_with_metrics = 0_u64,
            root_output_rows = None::<u64>,
            max_elapsed_compute_operator = None::<&str>,
            max_elapsed_compute_nanos = None::<u64>,
        );
    }

    fn install_capture_handler<'py>(
        py: Python<'py>,
        logger_name: &str,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>, Bound<'py, PyList>)> {
        let records = PyList::empty(py);
        let logging = py.import("logging")?;
        let locals = PyDict::new(py);
        locals.set_item("records", records.clone())?;
        py.run(
            c"
import logging

class CaptureHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = records

    def emit(self, record):
        self.records.append(record)
",
            Some(&locals),
            Some(&locals),
        )?;
        let handler_type = locals
            .get_item("CaptureHandler")?
            .ok_or_else(|| pyo3::exceptions::PyAssertionError::new_err("missing handler"))?;
        let handler = handler_type.call0()?;
        let logger = logging.call_method1("getLogger", (logger_name,))?;
        logger.setattr("propagate", false)?;
        logger.call_method1("setLevel", (10,))?;
        logger.call_method1("addHandler", (&handler,))?;

        Ok((logger, handler, records))
    }

    fn only_record<'py>(records: &Bound<'py, PyList>) -> PyResult<Bound<'py, PyAny>> {
        assert_eq!(records.len(), 1);
        records.get_item(0)
    }
}
