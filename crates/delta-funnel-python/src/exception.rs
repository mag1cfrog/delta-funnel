//! Python exception helpers.

use pyo3::PyTypeInfo;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

use crate::json::json_value_to_py;

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

#[allow(dead_code)]
pub(crate) fn delta_funnel_error_to_py(
    py: Python<'_>,
    error: delta_funnel::DeltaFunnelError,
) -> PyResult<PyErr> {
    let (phase, kind, context) = delta_funnel_error_parts(py, &error)?;
    delta_funnel_py_error(py, phase, kind, error.to_string(), context)
}

fn delta_funnel_error_parts(
    py: Python<'_>,
    error: &delta_funnel::DeltaFunnelError,
) -> PyResult<(&'static str, &'static str, Option<Py<PyAny>>)> {
    match error {
        delta_funnel::DeltaFunnelError::Config { .. } => Ok(("config", "config", None)),
        delta_funnel::DeltaFunnelError::InvalidSourceName { .. } => {
            Ok(("source_config", "invalid_source_name", None))
        }
        delta_funnel::DeltaFunnelError::DuplicateSourceName { .. } => {
            Ok(("source_config", "duplicate_source_name", None))
        }
        delta_funnel::DeltaFunnelError::InvalidSourceUri { .. } => {
            Ok(("source_config", "invalid_source_uri", None))
        }
        delta_funnel::DeltaFunnelError::DeltaSourceEngine { .. } => {
            Ok(("delta_source", "delta_source_engine", None))
        }
        delta_funnel::DeltaFunnelError::DeltaSnapshotLoad { .. } => {
            Ok(("delta_source", "delta_snapshot_load", None))
        }
        delta_funnel::DeltaFunnelError::DeltaProtocolCompatibility { .. } => {
            Ok(("delta_protocol", "delta_protocol_compatibility", None))
        }
        delta_funnel::DeltaFunnelError::DeltaSourceSchema { .. } => {
            Ok(("delta_source", "delta_source_schema", None))
        }
        delta_funnel::DeltaFunnelError::DataFusionRegistration { .. } => {
            Ok(("datafusion_registration", "datafusion_registration", None))
        }
        delta_funnel::DeltaFunnelError::SqlTable { .. } => Ok(("sql_table", "sql_table", None)),
        delta_funnel::DeltaFunnelError::DeltaScanProjection { .. } => {
            Ok(("delta_scan", "delta_scan_projection", None))
        }
        delta_funnel::DeltaFunnelError::DeltaScanFilter { .. } => {
            Ok(("delta_scan", "delta_scan_filter", None))
        }
        delta_funnel::DeltaFunnelError::DeltaScanConstruction { .. } => {
            Ok(("delta_scan", "delta_scan_construction", None))
        }
        delta_funnel::DeltaFunnelError::DeltaScanMetadataExpansion { .. } => {
            Ok(("delta_scan", "delta_scan_metadata_expansion", None))
        }
        delta_funnel::DeltaFunnelError::DeltaScanFileTaskPlanning { .. } => {
            Ok(("delta_scan", "delta_scan_file_task_planning", None))
        }
        delta_funnel::DeltaFunnelError::DeltaScanFileTaskPartitionPlanning { .. } => Ok((
            "delta_scan",
            "delta_scan_file_task_partition_planning",
            None,
        )),
        delta_funnel::DeltaFunnelError::DeltaScanFileRead { .. } => {
            Ok(("delta_scan", "delta_scan_file_read", None))
        }
        delta_funnel::DeltaFunnelError::DeltaScanDeletionVector { .. } => {
            Ok(("delta_scan", "delta_scan_deletion_vector", None))
        }
        delta_funnel::DeltaFunnelError::DependencyCompatibility { .. } => {
            Ok(("dependency_compatibility", "dependency_compatibility", None))
        }
        delta_funnel::DeltaFunnelError::BatchPipeline { .. } => {
            Ok(("batch_pipeline", "batch_pipeline", None))
        }
        delta_funnel::DeltaFunnelError::MssqlTargetConfig { .. } => {
            Ok(("mssql_target_config", "mssql_target_config", None))
        }
        delta_funnel::DeltaFunnelError::MissingMssqlConnection { .. } => {
            Ok(("mssql_target_config", "missing_mssql_connection", None))
        }
        delta_funnel::DeltaFunnelError::InvalidMssqlOutputIdentity { .. } => Ok((
            "mssql_schema_planning",
            "invalid_mssql_output_identity",
            None,
        )),
        delta_funnel::DeltaFunnelError::DuplicateMssqlOutputField { .. } => Ok((
            "mssql_schema_planning",
            "duplicate_mssql_output_field",
            None,
        )),
        delta_funnel::DeltaFunnelError::MssqlSchemaPlanning { .. } => {
            Ok(("mssql_schema_planning", "mssql_schema_planning", None))
        }
        delta_funnel::DeltaFunnelError::MssqlSchemaPlanningFailed { .. } => Ok((
            "mssql_schema_planning",
            "mssql_schema_planning_failed",
            None,
        )),
        delta_funnel::DeltaFunnelError::MssqlDdlTargetIdentifier { .. } => {
            Ok(("mssql_ddl_planning", "mssql_ddl_target_identifier", None))
        }
        delta_funnel::DeltaFunnelError::MssqlDdlPlanning { .. } => {
            Ok(("mssql_ddl_planning", "mssql_ddl_planning", None))
        }
        delta_funnel::DeltaFunnelError::MssqlLifecyclePlanning { .. } => {
            Ok(("mssql_lifecycle_planning", "mssql_lifecycle_planning", None))
        }
        delta_funnel::DeltaFunnelError::MssqlWrite { .. } => {
            Ok(("mssql_write", "mssql_write", None))
        }
        delta_funnel::DeltaFunnelError::MssqlWritePhase { context, .. } => Ok((
            mssql_write_phase(context.phase()),
            "mssql_write_phase",
            Some(json_value_to_py(py, &context.to_json_value())?),
        )),
        delta_funnel::DeltaFunnelError::MssqlBatchSchemaValidation { context, .. } => Ok((
            mssql_write_phase(context.phase()),
            "mssql_batch_schema_validation",
            Some(json_value_to_py(py, &context.to_json_value())?),
        )),
        delta_funnel::DeltaFunnelError::MssqlWorkflowPlanning { .. } => {
            Ok(("mssql_workflow_planning", "mssql_workflow_planning", None))
        }
    }
}

fn mssql_write_phase(phase: delta_funnel::MssqlWritePhase) -> &'static str {
    match phase {
        delta_funnel::MssqlWritePhase::Connect => "connect",
        delta_funnel::MssqlWritePhase::PrepareTargetLifecycle => "prepare_target_lifecycle",
        delta_funnel::MssqlWritePhase::InitializeWriter => "initialize_writer",
        delta_funnel::MssqlWritePhase::PollBatchStream => "poll_batch_stream",
        delta_funnel::MssqlWritePhase::ValidateBatchSchema => "validate_batch_schema",
        delta_funnel::MssqlWritePhase::WriteBatch => "write_batch",
        delta_funnel::MssqlWritePhase::Finalize => "finalize",
        delta_funnel::MssqlWritePhase::Validation => "validation",
        delta_funnel::MssqlWritePhase::Cleanup => "cleanup",
    }
}

#[cfg(test)]
mod tests {
    use super::{DeltaFunnelError, delta_funnel_error_to_py, delta_funnel_py_error};
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

    #[test]
    fn rust_error_mapping_exposes_stable_attributes() -> PyResult<()> {
        Python::attach(|py| {
            let error = delta_funnel_error_to_py(
                py,
                delta_funnel::DeltaFunnelError::InvalidSourceName {
                    name: "orders.latest".to_owned(),
                    reason: "source names may contain only ASCII letters, digits, and underscores",
                },
            )?;

            assert!(error.is_instance_of::<DeltaFunnelError>(py));
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "source_config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_source_name"
            );
            assert_eq!(
                error.value(py).getattr("message")?.extract::<String>()?,
                error.value(py).to_string()
            );
            assert!(error.value(py).getattr("context")?.is_none());

            Ok(())
        })
    }

    #[test]
    fn rust_error_mapping_preserves_sanitized_display() -> PyResult<()> {
        Python::attach(|py| {
            let error = delta_funnel_error_to_py(
                py,
                delta_funnel::DeltaFunnelError::DeltaProtocolCompatibility {
                    source_name: "orders\nlatest".to_owned(),
                    table_uri: "s3://user:password@example.com/table?access_key=AKIA&secret_key=secret&session_token=token".to_owned(),
                    snapshot_version: 7,
                    reason: "unsupported Delta reader feature `deletionVectors`".to_owned(),
                },
            )?;

            let message = error.value(py).getattr("message")?.extract::<String>()?;
            assert!(message.contains(r"orders\nlatest"));
            assert!(message.contains("s3://example.com/table"));
            assert!(!message.contains('\n'));
            assert!(!message.contains("user"));
            assert!(!message.contains("password"));
            assert!(!message.contains("AKIA"));
            assert!(!message.contains("secret_key"));
            assert!(!message.contains("session_token"));
            assert!(!message.contains("token"));

            Ok(())
        })
    }
}
