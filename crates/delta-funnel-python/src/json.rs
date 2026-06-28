//! JSON conversion helpers for Python boundary values.

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde_json::Value;

#[allow(dead_code)]
pub(crate) fn json_value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(value) => value.into_py_any(py),
        Value::Number(value) => json_number_to_py(py, value),
        Value::String(value) => value.into_py_any(py),
        Value::Array(values) => {
            let list = PyList::empty(py);
            for value in values {
                list.append(json_value_to_py(py, value)?)?;
            }
            list.into_py_any(py)
        }
        Value::Object(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, json_value_to_py(py, value)?)?;
            }
            dict.into_py_any(py)
        }
    }
}

#[allow(dead_code)]
fn json_number_to_py(py: Python<'_>, value: &serde_json::Number) -> PyResult<Py<PyAny>> {
    if let Some(value) = value.as_i64() {
        value.into_py_any(py)
    } else if let Some(value) = value.as_u64() {
        value.into_py_any(py)
    } else if let Some(value) = value.as_f64() {
        value.into_py_any(py)
    } else {
        Err(pyo3::exceptions::PyValueError::new_err(
            "unsupported JSON number",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::json_value_to_py;
    use pyo3::exceptions::PyKeyError;
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods};
    use serde_json::json;

    #[test]
    fn converts_json_object_with_nested_values() -> PyResult<()> {
        Python::attach(|py| {
            let value = json!({
                "name": "orders",
                "items": [{"count": 3, "active": true}],
                "none": null
            });

            let object = json_value_to_py(py, &value)?;
            let dict = object.bind(py).cast::<PyDict>()?;
            assert_eq!(required_item(dict, "name")?.extract::<String>()?, "orders");

            let items = required_item(dict, "items")?;
            let items = items.cast::<PyList>()?;
            assert_eq!(items.len(), 1);

            let item = items.get_item(0)?;
            let item = item.cast::<PyDict>()?;
            assert_eq!(required_item(item, "count")?.extract::<i64>()?, 3);
            assert!(required_item(item, "active")?.extract::<bool>()?);
            assert!(required_item(dict, "none")?.is_none());

            Ok(())
        })
    }

    #[test]
    fn converts_json_array() -> PyResult<()> {
        Python::attach(|py| {
            let value = json!(["alpha", 2, false]);

            let object = json_value_to_py(py, &value)?;
            let list = object.bind(py).cast::<PyList>()?;
            assert_eq!(list.get_item(0)?.extract::<String>()?, "alpha");
            assert_eq!(list.get_item(1)?.extract::<u64>()?, 2);
            assert!(!list.get_item(2)?.extract::<bool>()?);

            Ok(())
        })
    }

    #[test]
    fn converts_json_scalars() -> PyResult<()> {
        Python::attach(|py| {
            assert_eq!(
                json_value_to_py(py, &json!("value"))?
                    .bind(py)
                    .extract::<String>()?,
                "value"
            );
            assert_eq!(
                json_value_to_py(py, &json!(-7))?
                    .bind(py)
                    .extract::<i64>()?,
                -7
            );
            assert_eq!(
                json_value_to_py(py, &json!(7))?.bind(py).extract::<u64>()?,
                7
            );
            assert_eq!(
                json_value_to_py(py, &json!(1.5))?
                    .bind(py)
                    .extract::<f64>()?,
                1.5
            );
            assert!(
                json_value_to_py(py, &json!(true))?
                    .bind(py)
                    .extract::<bool>()?
            );
            assert!(json_value_to_py(py, &json!(null))?.bind(py).is_none());

            Ok(())
        })
    }

    #[test]
    fn generic_json_conversion_preserves_arbitrary_values() -> PyResult<()> {
        Python::attach(|py| {
            let value = json!({
                "sql": "select * from dbo.secret where password = 'raw'",
                "uri": "s3://user:password@example.com/table?token=secret",
                "row": ["alice", "secret"]
            });

            let object = json_value_to_py(py, &value)?;
            let dict = object.bind(py).cast::<PyDict>()?;
            assert_eq!(
                required_item(dict, "sql")?.extract::<String>()?,
                "select * from dbo.secret where password = 'raw'"
            );
            assert_eq!(
                required_item(dict, "uri")?.extract::<String>()?,
                "s3://user:password@example.com/table?token=secret"
            );

            let row = required_item(dict, "row")?;
            let row = row.cast::<PyList>()?;
            assert_eq!(row.get_item(0)?.extract::<String>()?, "alice");
            assert_eq!(row.get_item(1)?.extract::<String>()?, "secret");

            Ok(())
        })
    }

    fn required_item<'py>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Bound<'py, PyAny>> {
        dict.get_item(key)?
            .ok_or_else(|| PyKeyError::new_err(key.to_owned()))
    }
}
