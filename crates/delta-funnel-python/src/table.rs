//! Python lazy table wrapper.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use pyo3::exceptions::{PyOSError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

use crate::exception::attach_operation_result;
use crate::json::json_value_to_py;
use crate::output::PyMssqlOutputSpec;
use crate::profiler::{
    PyExecutionProfileConfig, PyRankedProfileConfig, execution_profile_mode,
    in_ranked_profile_scope, start_ranked_profile,
};
use crate::progress::PythonProgress;
use crate::session::{PySession, borrow_session_mut, config_py_error};

/// Rendered preview of a Delta Funnel table.
#[pyclass(name = "Preview", module = "deltafunnel")]
pub(crate) struct PyPreview {
    inner: delta_funnel::TablePreview,
    phase_timings: Py<PyAny>,
    execution_profile: Py<PyAny>,
}

impl PyPreview {
    fn new(py: Python<'_>, preview: delta_funnel::TablePreview) -> PyResult<Self> {
        let phase_timings = serde_json::Value::Array(
            preview
                .phase_timings()
                .iter()
                .map(delta_funnel::PhaseTimingReport::to_json_value)
                .collect(),
        );
        let execution_profile = preview
            .execution_profile()
            .map(delta_funnel::QueryExecutionProfile::to_json_value)
            .unwrap_or(serde_json::Value::Null);

        Ok(Self {
            inner: preview,
            phase_timings: json_value_to_py(py, &phase_timings)?,
            execution_profile: json_value_to_py(py, &execution_profile)?,
        })
    }
}

#[pymethods]
impl PyPreview {
    #[getter]
    fn text(&self) -> &str {
        self.inner.text()
    }

    #[getter]
    fn html(&self) -> &str {
        self.inner.html()
    }

    #[getter]
    fn phase_timings(&self, py: Python<'_>) -> Py<PyAny> {
        self.phase_timings.clone_ref(py)
    }

    #[getter]
    fn execution_profile(&self, py: Python<'_>) -> Py<PyAny> {
        self.execution_profile.clone_ref(py)
    }

    fn __str__(&self) -> &str {
        self.inner.text()
    }

    fn __repr__(&self) -> &str {
        self.inner.text()
    }

    fn _repr_html_(&self) -> &str {
        self.inner.html()
    }
}

/// Lazy Delta Funnel table.
///
/// A `Table` can be aliased for later SQL, converted to a `MssqlOutputSpec`,
/// or written directly to SQL Server.
#[pyclass(name = "Table", module = "deltafunnel")]
pub(crate) struct PyTable {
    session: Py<PySession>,
    pub(crate) inner: delta_funnel::LazyTable,
}

impl PyTable {
    pub(crate) const fn from_inner(session: Py<PySession>, inner: delta_funnel::LazyTable) -> Self {
        Self { session, inner }
    }
}

#[pymethods]
impl PyTable {
    /// Registers this pending SQL-derived table under `name` and returns a `Table`.
    fn alias(&self, py: Python<'_>, name: String) -> PyResult<Self> {
        let table =
            borrow_session_mut(&self.session, py)?.register_table_alias(py, name, &self.inner)?;
        Ok(Self::from_inner(self.session.clone_ref(py), table))
    }

    /// Builds a SQL Server output spec without executing rows.
    ///
    /// The default output name is the target `table`; pass `name` to override
    /// the report/output identity.
    #[pyo3(signature = (*, schema, table, load_mode, name=None, connection_string=None))]
    fn to_mssql(
        &self,
        py: Python<'_>,
        schema: String,
        table: String,
        load_mode: String,
        name: Option<String>,
        connection_string: Option<String>,
    ) -> PyResult<PyMssqlOutputSpec> {
        PyMssqlOutputSpec::new(
            py,
            self.session.clone_ref(py),
            self.inner.clone(),
            schema,
            table,
            load_mode,
            name,
            connection_string,
        )
    }

    /// Writes this table to SQL Server, or runs a dry-run plan when requested.
    ///
    /// Pass `dry_run=True` to plan without writing. Returns a plain Python
    /// `dict` report. Pass `execution_profile=ExecutionProfileConfig(...)` to
    /// attach a detailed query execution profile and optionally export Chrome
    /// Trace Event JSON. Pass `ranked_profile=RankedProfileConfig(...)` to
    /// record this write and export an interactive ranked HTML report plus an
    /// optional reusable ranked artifact. Profiling is not available for dry
    /// runs.
    ///
    /// By default, shows an indeterminate phase display in interactive
    /// terminals and Jupyter, and stays quiet elsewhere. Pass `progress=True`
    /// to force the display or `progress=False` to disable it. Terminal
    /// progress uses stderr and remains separate from diagnostic logging.
    /// After planning, eligible Delta scans show selected file progress and
    /// available runtime and approximate planning pruning counts. Progress
    /// display does not provide cancellation.
    ///
    /// If Python interrupts progress rendering, Delta Funnel finishes action
    /// cleanup before raising the interruption. When possible, the exception
    /// includes `deltafunnel_operation_status` and, for a failed action,
    /// `deltafunnel_operation_error`.
    #[pyo3(signature = (*, schema, table, load_mode, dry_run=None, name=None, connection_string=None, progress=None, execution_profile=None, ranked_profile=None))]
    #[allow(clippy::too_many_arguments)]
    fn write_to_mssql(
        &self,
        py: Python<'_>,
        schema: String,
        table: String,
        load_mode: String,
        dry_run: Option<bool>,
        name: Option<String>,
        connection_string: Option<String>,
        progress: Option<bool>,
        execution_profile: Option<PyRef<'_, PyExecutionProfileConfig>>,
        ranked_profile: Option<PyRef<'_, PyRankedProfileConfig>>,
    ) -> PyResult<Py<PyAny>> {
        if dry_run == Some(true) && (execution_profile.is_some() || ranked_profile.is_some()) {
            return Err(config_py_error(
                py,
                "invalid_option_value",
                "profiling is only supported for execute `write_to_mssql` calls".to_owned(),
            ));
        }
        let spec = PyMssqlOutputSpec::new(
            py,
            self.session.clone_ref(py),
            self.inner.clone(),
            schema,
            table,
            load_mode,
            name,
            connection_string,
        )?;
        let progress = PythonProgress::new(progress);
        if dry_run == Some(true) {
            return self.session.borrow(py).dry_run_to_mssql(
                py,
                &spec.write_plan(delta_funnel::RunMode::DryRun),
                progress.as_ref(),
            );
        }

        let profile_mode = execution_profile_mode(execution_profile.as_deref());
        let trace_path = execution_profile
            .as_deref()
            .and_then(PyExecutionProfileConfig::chrome_trace_path)
            .map(std::path::absolute)
            .transpose()
            .map_err(PyOSError::new_err)?;
        let trace_destination = trace_path
            .as_deref()
            .map(TraceDestination::open)
            .transpose()?;
        let ranked_capture =
            start_ranked_profile(py, ranked_profile.as_deref(), trace_path.as_deref())?;
        drop((execution_profile, ranked_profile));
        let write = in_ranked_profile_scope(ranked_capture.as_ref(), || {
            self.session.borrow(py).write_to_mssql(
                py,
                &spec.write_plan(delta_funnel::RunMode::Execute),
                profile_mode,
                progress.as_ref(),
                trace_destination,
            )
        });
        let ranked_result = ranked_capture
            .map(|ranked_capture| ranked_capture.finish(py))
            .transpose();
        match (write, ranked_result) {
            (Err(error), _) => Err(error),
            (Ok(report), Err(error)) => {
                let _ = error
                    .value(py)
                    .setattr("deltafunnel_operation_status", "completed");
                let _ = error
                    .value(py)
                    .setattr("deltafunnel_operation_report", report.bind(py));
                Err(error)
            }
            (Ok(report), Ok(_)) => Ok(report),
        }
    }

    /// Returns a bounded rendered preview of this lazy table.
    ///
    /// Progress appears automatically in interactive terminals and notebooks.
    /// Pass `progress=True` to force it or `progress=False` to disable it. The
    /// progress display closes before the `Preview` object is returned. Phase
    /// timings are always attached. Pass
    /// `execution_profile=ExecutionProfileConfig(...)` to also attach the
    /// detailed execution profile and optionally export Chrome Trace Event
    /// JSON. Pass `ranked_profile=RankedProfileConfig(...)` to record this
    /// preview and write an interactive ranked HTML report plus an optional
    /// reusable ranked artifact.
    #[pyo3(signature = (limit=20, *, progress=None, execution_profile=None, ranked_profile=None))]
    fn preview(
        &self,
        py: Python<'_>,
        limit: usize,
        progress: Option<bool>,
        execution_profile: Option<PyRef<'_, PyExecutionProfileConfig>>,
        ranked_profile: Option<PyRef<'_, PyRankedProfileConfig>>,
    ) -> PyResult<PyPreview> {
        let profile_mode = execution_profile_mode(execution_profile.as_deref());
        let options =
            delta_funnel::PreviewOptions::new(limit).with_execution_profile_mode(profile_mode);
        let progress = PythonProgress::for_preview(progress);
        let trace_path = execution_profile
            .as_deref()
            .and_then(PyExecutionProfileConfig::chrome_trace_path)
            .map(std::path::absolute)
            .transpose()
            .map_err(PyOSError::new_err)?;
        let trace_destination = trace_path
            .as_deref()
            .map(TraceDestination::open)
            .transpose()?;
        let ranked_capture =
            start_ranked_profile(py, ranked_profile.as_deref(), trace_path.as_deref())?;
        drop((execution_profile, ranked_profile));
        let preview = in_ranked_profile_scope(ranked_capture.as_ref(), || {
            self.session
                .borrow(py)
                .preview_table(py, &self.inner, options, progress.as_ref())
        });
        let trace_result = preview
            .as_ref()
            .ok()
            .zip(trace_destination)
            .map(|(preview, destination)| {
                preview
                    .to_trace_event_json_value()
                    .ok_or_else(|| {
                        config_py_error(
                            py,
                            "execution_profile_unavailable",
                            "preview trace export requires exact execution profiling".to_owned(),
                        )
                    })
                    .and_then(|trace| destination.write(&trace))
                    .inspect_err(|error| {
                        attach_operation_result(py, error, "completed", None, None);
                    })
            })
            .transpose();
        let ranked_result = ranked_capture
            .map(|ranked_capture| ranked_capture.finish(py))
            .transpose();
        match (preview, trace_result, ranked_result) {
            (Err(error), _, _) => Err(error),
            (Ok(_), Err(error), _) => Err(error),
            (Ok(_), Ok(_), Err(error)) => {
                attach_operation_result(py, &error, "completed", None, None);
                Err(error)
            }
            (Ok(preview), Ok(_), Ok(_)) => PyPreview::new(py, preview),
        }
    }

    /// Prints a bounded preview of this lazy table to Python stdout.
    ///
    /// Progress closes before the preview text is printed.
    #[pyo3(signature = (limit=20, *, progress=None))]
    fn show(&self, py: Python<'_>, limit: usize, progress: Option<bool>) -> PyResult<()> {
        let progress = PythonProgress::for_preview(progress);
        let preview = self.session.borrow(py).preview_table(
            py,
            &self.inner,
            delta_funnel::PreviewOptions::new(limit),
            progress.as_ref(),
        )?;
        py.import("builtins")?
            .getattr("print")?
            .call1((preview.text(),))?;
        Ok(())
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let kind = match self.inner.kind() {
            delta_funnel::LazyTableKind::DeltaSource => "delta_source",
            delta_funnel::LazyTableKind::DerivedSql => "derived_sql",
        };
        if let Some((source_uri, snapshot_version)) =
            self.session.borrow(py).source_repr_details(&self.inner)
        {
            return format!(
                "deltafunnel.Table(id={}, kind={kind:?}, name={:?}, source_uri={source_uri:?}, snapshot_version={snapshot_version})",
                self.inner.id(),
                self.inner.name()
            );
        }
        format!(
            "deltafunnel.Table(id={}, kind={kind:?}, name={:?})",
            self.inner.id(),
            self.inner.name()
        )
    }
}

/// A trace file opened before a mutating operation starts.
pub(crate) struct TraceDestination {
    path: PathBuf,
    file: fs::File,
    remove_on_drop: bool,
}

impl TraceDestination {
    pub(crate) fn open(path: &Path) -> PyResult<Self> {
        let (file, remove_on_drop) = match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(file) => (file, true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (
                fs::OpenOptions::new()
                    .write(true)
                    .open(path)
                    .map_err(PyOSError::new_err)?,
                false,
            ),
            Err(error) => return Err(PyOSError::new_err(error)),
        };
        Ok(Self {
            path: path.to_owned(),
            file,
            remove_on_drop,
        })
    }

    pub(crate) fn write(mut self, trace: &serde_json::Value) -> PyResult<()> {
        let bytes = serde_json::to_vec(trace)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        self.file.set_len(0).map_err(PyOSError::new_err)?;
        self.file.write_all(&bytes).map_err(PyOSError::new_err)?;
        self.file.flush().map_err(PyOSError::new_err)?;
        self.remove_on_drop = false;
        Ok(())
    }
}

impl Drop for TraceDestination {
    fn drop(&mut self) {
        if self.remove_on_drop {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub(crate) fn add_table(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyPreview>()?;
    module.add_class::<PyTable>()
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::TraceDestination;
    use crate::{
        deltafunnel, exception::DeltaFunnelError, progress::adapter_creation_count,
        test_support::python_state,
    };
    use pyo3::exceptions::{PyAttributeError, PyKeyError, PyOSError, PyRuntimeError, PyTypeError};
    use pyo3::prelude::*;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods, PyModule};
    use serde_json::Value;

    const PREVIEW_PHASES: [&str; 7] = [
        "preview_dataframe_planning",
        "preview_physical_planning",
        "preview_stream_setup",
        "preview_execute_collect",
        "preview_format_text",
        "preview_format_html",
        "preview_total",
    ];

    #[test]
    fn unused_trace_destination_preserves_the_preoperation_file_state() -> PyResult<()> {
        let new_path = temp_trace_path("preflight-new")?;
        let destination = TraceDestination::open(&new_path)?;
        assert!(new_path.exists());
        drop(destination);
        assert!(!new_path.exists());

        let existing_path = temp_trace_path("preflight-existing")?;
        fs::write(&existing_path, b"existing trace")
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let destination = TraceDestination::open(&existing_path)?;
        drop(destination);
        assert_eq!(
            fs::read(&existing_path).map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
            b"existing trace"
        );
        fs::remove_file(existing_path)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(())
    }

    #[test]
    fn module_exports_table_type() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;

            let table_type = module.getattr("Table")?;
            assert_eq!(
                table_type.getattr("__name__")?.extract::<String>()?,
                "Table"
            );
            let preview_type = module.getattr("Preview")?;
            assert_eq!(
                preview_type.getattr("__name__")?.extract::<String>()?,
                "Preview"
            );

            Ok(())
        })
    }

    #[test]
    fn pyi_stub_exposes_explicit_profile_configs_without_preview_export_method() {
        let stub = include_str!("../deltafunnel.pyi");

        assert!(!stub.contains("def export_trace("));
        assert!(stub.contains("execution_profile: ExecutionProfileConfig | None = None"));
        assert!(stub.contains("ranked_profile: RankedProfileConfig | None = None"));
    }

    #[cfg(all(feature = "perfetto-profile", target_os = "linux"))]
    #[test]
    fn preview_and_write_reject_artifact_output_collisions_before_execution() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1("table_from_sql", ("select 1 as id",))?;
            let ranked_type = module.getattr("RankedProfileConfig")?;

            let preview_output = temp_trace_path("preview-artifact-alias")?.with_extension("html");
            let config_kwargs = PyDict::new(py);
            config_kwargs.set_item("artifact_path", preview_output.to_string_lossy().as_ref())?;
            let ranked_profile = ranked_type.call(
                (preview_output.to_string_lossy().as_ref(),),
                Some(&config_kwargs),
            )?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", false)?;
            kwargs.set_item("ranked_profile", ranked_profile)?;
            let error = table
                .call_method("preview", (), Some(&kwargs))
                .expect_err("preview outputs must not alias");
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_option_value"
            );
            assert!(!preview_output.exists());

            let write_output = temp_trace_path("write-profile")?.with_extension("html");
            let artifact_output = temp_trace_path("write-artifact")?.with_extension("dfprofile");
            let config_kwargs = PyDict::new(py);
            config_kwargs.set_item("artifact_path", artifact_output.to_string_lossy().as_ref())?;
            let ranked_profile = ranked_type.call(
                (write_output.to_string_lossy().as_ref(),),
                Some(&config_kwargs),
            )?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("schema", "dbo")?;
            kwargs.set_item("table", "orders")?;
            kwargs.set_item("load_mode", "append_existing")?;
            kwargs.set_item("progress", false)?;
            let execution_kwargs = PyDict::new(py);
            execution_kwargs.set_item(
                "chrome_trace_path",
                artifact_output.to_string_lossy().as_ref(),
            )?;
            let execution_profile = module
                .getattr("ExecutionProfileConfig")?
                .call((), Some(&execution_kwargs))?;
            kwargs.set_item("execution_profile", execution_profile)?;
            kwargs.set_item("ranked_profile", ranked_profile)?;
            let error = table
                .call_method("write_to_mssql", (), Some(&kwargs))
                .expect_err("write trace and artifact outputs must not alias");
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_option_value"
            );
            assert!(!write_output.exists());
            assert!(!artifact_output.exists());
            Ok(())
        })
    }

    #[test]
    fn table_write_retires_legacy_profile_arguments_and_preflights_chrome_output() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1("table_from_sql", ("select 1 as id",))?;
            let signature = py
                .import("inspect")?
                .call_method1("signature", (table.getattr("write_to_mssql")?,))?
                .to_string();
            assert!(signature.ends_with("execution_profile=None, ranked_profile=None)"));

            for retired in ["profile", "trace_path", "profiler"] {
                let kwargs = PyDict::new(py);
                kwargs.set_item("schema", "dbo")?;
                kwargs.set_item("table", "orders")?;
                kwargs.set_item("load_mode", "append_existing")?;
                kwargs.set_item(retired, true)?;
                let error = table
                    .call_method("write_to_mssql", (), Some(&kwargs))
                    .unwrap_err();
                assert!(error.is_instance_of::<PyTypeError>(py));
            }

            let missing_parent = temp_trace_path("missing-trace-parent")?;
            let trace_path = missing_parent.join("trace.json");
            let config_kwargs = PyDict::new(py);
            config_kwargs.set_item("chrome_trace_path", trace_path.to_string_lossy().as_ref())?;
            let execution_profile = module
                .getattr("ExecutionProfileConfig")?
                .call((), Some(&config_kwargs))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("schema", "dbo")?;
            kwargs.set_item("table", "orders")?;
            kwargs.set_item("load_mode", "append_existing")?;
            kwargs.set_item("execution_profile", execution_profile)?;
            let error = table
                .call_method("write_to_mssql", (), Some(&kwargs))
                .unwrap_err();
            assert!(error.is_instance_of::<PyOSError>(py));
            assert!(!missing_parent.exists());
            Ok(())
        })
    }

    #[test]
    fn table_preview_returns_limited_preview_object() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1(
                "table_from_sql",
                ("select 1 as id union all select 2 as id order by id",),
            )?;

            let preview = table.call_method1("preview", (1,))?;
            let text = preview.getattr("text")?.extract::<String>()?;
            let html = preview.getattr("html")?.extract::<String>()?;
            let preview_signature = py
                .import("inspect")?
                .call_method1("signature", (table.getattr("preview")?,))?
                .to_string();

            assert_eq!(
                preview
                    .get_type()
                    .getattr("__name__")?
                    .extract::<String>()?,
                "Preview"
            );
            assert_eq!(preview.str()?.extract::<String>()?, text);
            assert_eq!(preview.repr()?.extract::<String>()?, text);
            assert_eq!(
                preview_signature,
                "(limit=20, *, progress=None, execution_profile=None, ranked_profile=None)"
            );
            assert_eq!(
                preview.call_method0("_repr_html_")?.extract::<String>()?,
                html
            );
            assert!(html.contains("class=\"deltafunnel-preview\""));
            assert!(html.contains("<td class=\"df-num\">1</td>"));
            assert!(!html.contains("<td class=\"df-num\">2</td>"));
            assert!(
                html.contains("<th class=\"df-num\"><span>id</span><br><span class=\"df-type\">")
            );
            assert!(text.contains("| id |"));
            assert!(text.lines().any(|line| line.contains("| 1  |")));
            assert!(!text.lines().any(|line| line.contains("| 2  |")));
            let phase_timings = preview.getattr("phase_timings")?;
            let phase_timings = phase_timings.cast::<PyList>()?;
            assert_eq!(phase_timings.len(), PREVIEW_PHASES.len());
            for (timing, expected_phase) in phase_timings.iter().zip(PREVIEW_PHASES) {
                let timing = timing.cast::<PyDict>()?;
                assert_eq!(
                    required_item(timing, "phase_name")?.extract::<String>()?,
                    expected_phase
                );
                let status = required_item(timing, "status")?.cast_into::<PyDict>()?;
                assert_eq!(
                    required_item(&status, "kind")?.extract::<String>()?,
                    "completed"
                );
            }
            assert!(preview.getattr("execution_profile")?.is_none());
            assert!(preview.getattr("export_trace").is_err());
            for field in ["phase_timings", "execution_profile"] {
                assert!(
                    preview
                        .setattr(field, py.None())
                        .is_err_and(|error| { error.is_instance_of::<PyAttributeError>(py) })
                );
            }
            Ok(())
        })
    }

    #[cfg(not(all(feature = "perfetto-profile", target_os = "linux")))]
    #[test]
    fn preview_ranked_profile_requires_a_diagnostics_build_before_execution() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1(
                "table_from_sql",
                ("select cast(1 as bigint) / cast(0 as bigint) as value",),
            )?;
            let output = temp_trace_path("ranked-profile-unavailable")?.with_extension("html");
            let ranked_profile = module
                .getattr("RankedProfileConfig")?
                .call1((output.to_string_lossy().as_ref(),))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", false)?;
            kwargs.set_item("ranked_profile", ranked_profile)?;

            let error = table
                .call_method("preview", (), Some(&kwargs))
                .expect_err("a standard wheel must reject operation profiling");

            assert!(error.is_instance_of::<DeltaFunnelError>(py));
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "ranked_profile"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "not_available"
            );

            assert!(!output.exists());
            Ok(())
        })
    }

    #[cfg(not(all(feature = "perfetto-profile", target_os = "linux")))]
    #[test]
    fn write_ranked_profile_requires_a_diagnostics_build_before_execution() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1("table_from_sql", ("select 1 as id",))?;
            let output =
                temp_trace_path("write-ranked-profile-unavailable")?.with_extension("html");
            let ranked_profile = module
                .getattr("RankedProfileConfig")?
                .call1((output.to_string_lossy().as_ref(),))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("schema", "dbo")?;
            kwargs.set_item("table", "orders")?;
            kwargs.set_item("load_mode", "append_existing")?;
            kwargs.set_item("progress", false)?;
            kwargs.set_item("ranked_profile", ranked_profile)?;

            let error = table
                .call_method("write_to_mssql", (), Some(&kwargs))
                .expect_err("a standard wheel must reject operation profiling");

            assert!(error.is_instance_of::<DeltaFunnelError>(py));
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "ranked_profile"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "not_available"
            );

            kwargs.set_item("dry_run", true)?;
            let error = table
                .call_method("write_to_mssql", (), Some(&kwargs))
                .expect_err("dry-run operation profiling must be rejected");
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "config"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_option_value"
            );
            assert!(!output.exists());
            Ok(())
        })
    }

    #[test]
    fn detailed_table_preview_returns_an_execution_profile() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1(
                "table_from_sql",
                ("select 1 as id union all select 2 as id",),
            )?;
            let path = temp_trace_path("detailed")?;
            let path_object = py
                .import("pathlib")?
                .getattr("Path")?
                .call1((path.to_string_lossy().as_ref(),))?;
            let config_kwargs = PyDict::new(py);
            config_kwargs.set_item("chrome_trace_path", path_object)?;
            let execution_profile = module
                .getattr("ExecutionProfileConfig")?
                .call((), Some(&config_kwargs))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", false)?;
            kwargs.set_item("execution_profile", execution_profile)?;

            let preview = table.call_method("preview", (1,), Some(&kwargs))?;
            let profile = preview
                .getattr("execution_profile")?
                .cast_into::<PyDict>()?;

            assert_eq!(
                required_item(&profile, "scope")?.extract::<String>()?,
                "preview"
            );
            assert_eq!(
                required_item(&profile, "outcome")?.extract::<String>()?,
                "success"
            );
            assert!(!required_item(&profile, "partial")?.extract::<bool>()?);
            assert_eq!(
                required_item(&profile, "delta_funnel_row_limit")?.extract::<u64>()?,
                1
            );
            assert!(
                !required_item(&profile, "operators")?
                    .cast::<PyList>()?
                    .is_empty()
            );

            assert!(preview.getattr("export_trace").is_err());
            let trace: Value = serde_json::from_slice(
                &fs::read(&path).map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
            )
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let _ = fs::remove_file(path);

            assert_eq!(trace["delta_funnel_profile"]["scope"], "preview");
            assert_eq!(trace["delta_funnel_timeline"]["name"], "Preview total");
            let total_duration = trace["delta_funnel_timeline"]["total_duration_micros"]
                .as_u64()
                .ok_or_else(|| PyRuntimeError::new_err("missing preview total duration"))?;
            let events = trace["traceEvents"]
                .as_array()
                .ok_or_else(|| PyRuntimeError::new_err("missing trace events"))?;
            assert!(events.iter().any(|event| {
                event["name"] == "Preview total"
                    && event["ts"] == 0
                    && event["dur"] == total_duration
            }));
            assert!(events.iter().any(|event| {
                event["name"] == "Physical planning"
                    && event["args"]["time_semantics"] == "wall_clock"
            }));
            assert!(
                events
                    .iter()
                    .filter(|event| event["cat"] == "datafusion.operator.lifecycle")
                    .all(|event| event["args"]["time_semantics"] == "lifecycle")
            );
            assert!(
                events
                    .iter()
                    .filter(|event| event["ph"] == "X")
                    .all(|event| {
                        event["ts"].as_u64().zip(event["dur"].as_u64()).is_some_and(
                            |(start, duration)| start.saturating_add(duration) <= total_duration,
                        )
                    })
            );
            Ok(())
        })
    }

    #[test]
    fn preview_failure_exposes_structured_python_context() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1(
                "table_from_sql",
                ("select cast(1 as bigint) / cast(0 as bigint) as value",),
            )?;
            let execution_profile = module.getattr("ExecutionProfileConfig")?.call0()?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", false)?;
            kwargs.set_item("execution_profile", execution_profile)?;

            let error = table.call_method("preview", (), Some(&kwargs)).unwrap_err();
            assert!(error.is_instance_of::<DeltaFunnelError>(py));
            assert_eq!(
                error.value(py).getattr("phase")?.extract::<String>()?,
                "preview"
            );
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "preview_failed"
            );
            let context = error.value(py).getattr("context")?.cast_into::<PyDict>()?;
            assert!(context.get_item("source")?.is_none());
            assert!(
                !error
                    .value(py)
                    .getattr("message")?
                    .extract::<String>()?
                    .contains("select")
            );
            assert_eq!(
                required_item(&context, "failed_phase")?.extract::<String>()?,
                "preview_execute_collect"
            );
            assert_eq!(
                required_item(&context, "phase_timings")?
                    .cast::<PyList>()?
                    .len(),
                PREVIEW_PHASES.len()
            );
            let profile = required_item(&context, "execution_profile")?.cast_into::<PyDict>()?;
            assert_eq!(
                required_item(&profile, "outcome")?.extract::<String>()?,
                "error"
            );
            assert!(required_item(&profile, "partial")?.extract::<bool>()?);
            let timeline = required_item(&context, "operation_timeline")?.cast_into::<PyDict>()?;
            assert_eq!(
                required_item(&timeline, "status")?.extract::<String>()?,
                "failed"
            );
            Ok(())
        })
    }

    #[test]
    fn table_show_prints_preview_to_python_stdout() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1("table_from_sql", ("select 'west' as region",))?;
            let sys = py.import("sys")?;
            let io = py.import("io")?;
            let capture = io.call_method0("StringIO")?;
            let old_stdout = sys.getattr("stdout")?;

            sys.setattr("stdout", &capture)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", false)?;
            let show_result = table.call_method("show", (20,), Some(&kwargs));
            sys.setattr("stdout", old_stdout)?;
            show_result?;

            let output = capture.call_method0("getvalue")?.extract::<String>()?;
            assert!(output.contains("| region |"));
            assert!(output.lines().any(|line| line.contains("| west   |")));
            Ok(())
        })
    }

    #[test]
    fn preview_progress_arguments_are_validated_before_adapter_creation() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1("table_from_sql", ("select 1 as id",))?;
            let initial_count = adapter_creation_count();

            for argument in ["execution_profile", "ranked_profile"] {
                for invalid in [0, 1] {
                    let kwargs = PyDict::new(py);
                    kwargs.set_item("progress", true)?;
                    kwargs.set_item(argument, invalid)?;
                    let error = table.call_method("preview", (), Some(&kwargs)).unwrap_err();
                    assert!(error.is_instance_of::<PyTypeError>(py));
                }
            }

            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", true)?;
            kwargs.set_item("profile", "detailed")?;
            let error = table.call_method("preview", (), Some(&kwargs)).unwrap_err();
            assert!(error.is_instance_of::<PyTypeError>(py));
            assert_eq!(adapter_creation_count(), initial_count);

            for argument in ["execution_profile", "ranked_profile"] {
                let kwargs = PyDict::new(py);
                kwargs.set_item("progress", false)?;
                kwargs.set_item(argument, py.None())?;
                let preview = table.call_method("preview", (), Some(&kwargs))?;
                assert!(preview.getattr("execution_profile")?.is_none());
            }

            for method in ["preview", "show"] {
                let kwargs = PyDict::new(py);
                kwargs.set_item("progress", "always")?;
                let error = table.call_method(method, (), Some(&kwargs)).unwrap_err();
                assert!(error.is_instance_of::<PyTypeError>(py));

                let kwargs = PyDict::new(py);
                kwargs.set_item("progress", 1)?;
                let error = table.call_method(method, (), Some(&kwargs)).unwrap_err();
                assert!(error.is_instance_of::<PyTypeError>(py));

                let kwargs = PyDict::new(py);
                kwargs.set_item("progress", true)?;
                for invalid_limit in [-1_i128, i128::MAX] {
                    table
                        .call_method(method, (invalid_limit,), Some(&kwargs))
                        .unwrap_err();
                }

                let error = table.call_method1(method, (20, false)).unwrap_err();
                assert!(error.is_instance_of::<PyTypeError>(py));
            }

            assert_eq!(adapter_creation_count(), initial_count);

            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", false)?;
            table.call_method("preview", (), Some(&kwargs))?;
            assert_eq!(adapter_creation_count(), initial_count);
            Ok(())
        })
    }

    fn required_item<'py>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Bound<'py, PyAny>> {
        dict.get_item(key)?
            .ok_or_else(|| PyKeyError::new_err(key.to_owned()))
    }

    fn temp_trace_path(name: &str) -> PyResult<PathBuf> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
            .as_nanos();
        Ok(std::env::temp_dir().join(format!(
            "delta-funnel-preview-trace-{name}-{}-{nanos}.json",
            std::process::id()
        )))
    }
}
