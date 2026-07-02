//! Python logging bridge for Rust tracing events.

use std::collections::BTreeMap;
use std::env;
use std::fmt;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModuleMethods};
use tracing::{Event, Level, Subscriber, field::Field, field::Visit};
use tracing_subscriber::{
    EnvFilter, Layer, Registry, layer::Context, prelude::*, registry::LookupSpan,
};

use crate::exception::delta_funnel_py_error;

const DEFAULT_LOGGER: &str = "deltafunnel";
const DEFAULT_FILTER: &str = "delta_funnel=info,arrow_tiberius=info";
const LOG_FILTER_ENV: &str = "DELTAFUNNEL_LOG";

pub(crate) fn add_logging(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(init_logging, module)?)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (filter=None, logger=DEFAULT_LOGGER.to_owned()))]
fn init_logging(py: Python<'_>, filter: Option<String>, logger: String) -> PyResult<bool> {
    let filter = filter
        .or_else(|| env::var(LOG_FILTER_ENV).ok())
        .filter(|filter| !filter.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_FILTER.to_owned());
    let filter = EnvFilter::try_new(filter).map_err(|error| {
        invalid_logging_filter_py_error(py, format!("invalid DeltaFunnel logging filter: {error}"))
    })?;
    let subscriber = Registry::default().with(filter).with(PythonLoggingLayer {
        logger_name: logger,
    });

    Ok(tracing::subscriber::set_global_default(subscriber).is_ok())
}

fn invalid_logging_filter_py_error(py: Python<'_>, message: String) -> PyErr {
    match delta_funnel_py_error(py, "config", "invalid_logging_filter", message, None) {
        Ok(error) => error,
        Err(error) => error,
    }
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

        let _ = Python::attach(|py| {
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

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field, value.to_string());
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyDict, PyList, PyModule};
    use tracing_subscriber::prelude::*;

    use super::{PythonLoggingLayer, python_log_level};
    use crate::deltafunnel;

    #[test]
    fn module_exports_init_logging() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            assert!(module.hasattr("init_logging")?);

            Ok(())
        })
    }

    #[test]
    fn init_logging_returns_bool_and_repeated_calls_do_not_panic() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            let _first = module.call_method0("init_logging")?.extract::<bool>()?;
            let second = module.call_method0("init_logging")?.extract::<bool>()?;

            assert!(!second);

            Ok(())
        })
    }

    #[test]
    fn pyi_stub_exports_init_logging() {
        let stub = include_str!("../deltafunnel.pyi");

        assert!(stub.contains("def init_logging("));
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
            let logger = logging.call_method1("getLogger", ("deltafunnel.test",))?;
            logger.setattr("propagate", false)?;
            logger.call_method1("setLevel", (10,))?;
            logger.call_method1("addHandler", (&handler,))?;

            let subscriber = tracing_subscriber::registry().with(PythonLoggingLayer {
                logger_name: "deltafunnel.test".to_owned(),
            });
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(
                    target: "delta_funnel",
                    telemetry_event = "test.event",
                    output_name = "orders",
                    "test.event"
                );
            });

            logger.call_method1("removeHandler", (&handler,))?;
            assert_eq!(records.len(), 1);
            let record = records.get_item(0)?;
            assert_eq!(
                record.getattr("name")?.extract::<String>()?,
                "deltafunnel.test"
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
}
