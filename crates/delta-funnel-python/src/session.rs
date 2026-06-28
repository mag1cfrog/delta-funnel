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
    fn new(py: Python<'_>) -> PyResult<Self> {
        let inner = delta_funnel::DeltaFunnelSession::new(delta_funnel::SessionOptions::default())
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
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyModule};

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
            let session = Py::new(py, PySession::new(py)?)?;
            let repr = session.bind(py).repr()?.extract::<String>()?;

            assert_eq!(repr, "deltafunnel.Session()");
            assert!(!repr.contains("server=tcp"));
            assert!(!repr.contains("password"));
            assert!(!repr.contains("token"));
            assert!(!repr.contains("secret"));

            Ok(())
        })
    }
}
