//! Python session wrapper.

use pyo3::prelude::*;

use crate::exception::delta_funnel_error_to_py;

#[pyclass(name = "Session", module = "deltafunnel")]
pub(crate) struct PySession {
    #[allow(dead_code)]
    inner: delta_funnel::DeltaFunnelSession,
}

#[pymethods]
impl PySession {
    #[new]
    #[pyo3(signature = (*, default_mssql_connection_string=None, target_partitions=None, output_batch_size=None))]
    fn new(
        py: Python<'_>,
        default_mssql_connection_string: Option<String>,
        target_partitions: Option<usize>,
        output_batch_size: Option<usize>,
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

#[cfg(test)]
mod tests {
    use super::PySession;
    use crate::deltafunnel;
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
            let session = Py::new(py, PySession::new(py, None, None, None)?)?;
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
            let session = Py::new(py, PySession::new(py, None, Some(3), Some(17))?)?;
            let repr = session.bind(py).repr()?.extract::<String>()?;

            assert_eq!(repr, "deltafunnel.Session()");

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
                let error = match PySession::new(py, None, target_partitions, output_batch_size) {
                    Ok(_) => return Err(PyAssertionError::new_err("expected zero value error")),
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
