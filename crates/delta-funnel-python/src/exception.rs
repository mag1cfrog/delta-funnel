//! Python exception helpers.

use pyo3::PyTypeInfo;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

create_exception!(
    deltafunnel,
    DeltaFunnelError,
    PyException,
    "DeltaFunnel operation failed."
);

pub(crate) fn add_exception(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add(
        "DeltaFunnelError",
        module.py().get_type::<DeltaFunnelError>(),
    )
}

#[allow(dead_code)]
pub(crate) fn delta_funnel_py_error(
    py: Python<'_>,
    phase: &'static str,
    kind: &'static str,
    message: String,
    context: Option<Py<PyAny>>,
) -> PyResult<PyErr> {
    let error = PyErr::from_type(DeltaFunnelError::type_object(py), (message.clone(),));
    let value = error.value(py);
    value.setattr("phase", phase)?;
    value.setattr("kind", kind)?;
    value.setattr("message", message)?;
    value.setattr("context", context.unwrap_or_else(|| py.None()))?;
    Ok(error)
}

#[cfg(test)]
mod tests {
    use super::{DeltaFunnelError, delta_funnel_py_error};
    use crate::deltafunnel;
    use pyo3::IntoPyObjectExt;
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyModule};

    #[test]
    fn module_exports_delta_funnel_error() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            let error_type = module.getattr("DeltaFunnelError")?;
            assert_eq!(
                error_type.getattr("__name__")?.extract::<String>()?,
                "DeltaFunnelError"
            );

            Ok(())
        })
    }

    #[test]
    fn python_error_exposes_stable_attributes_and_display() -> PyResult<()> {
        Python::attach(|py| {
            let context = PyDict::new(py);
            context.set_item("field", "value")?;

            let error = delta_funnel_py_error(
                py,
                "python_conversion",
                "unsupported_json_number",
                "unsupported JSON number".to_owned(),
                Some(context.into_py_any(py)?),
            )?;

            assert!(error.is_instance_of::<DeltaFunnelError>(py));
            assert_eq!(error.value(py).to_string(), "unsupported JSON number");
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "python_conversion"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "unsupported_json_number"
            );
            assert_eq!(
                error.value(py).getattr("message")?.extract::<String>()?,
                "unsupported JSON number"
            );
            let context = error.value(py).getattr("context")?;
            assert_eq!(context.get_item("field")?.extract::<String>()?, "value");

            Ok(())
        })
    }

    #[test]
    fn python_error_defaults_context_to_none() -> PyResult<()> {
        Python::attach(|py| {
            let error = delta_funnel_py_error(
                py,
                "python_conversion",
                "conversion_failed",
                "conversion failed".to_owned(),
                None,
            )?;

            assert!(error.value(py).getattr("context")?.is_none());

            Ok(())
        })
    }
}
