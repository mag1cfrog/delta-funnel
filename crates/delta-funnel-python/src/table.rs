//! Python lazy table wrapper.

use std::{
    fs,
    path::{Path, PathBuf},
};

use pyo3::exceptions::{PyOSError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBool};

use crate::json::json_value_to_py;
use crate::output::PyMssqlOutputSpec;
use crate::progress::PythonProgress;
use crate::session::{PySession, config_py_error};

/// Rendered preview of a Delta Funnel table.
#[pyclass(name = "Preview", module = "deltafunnel")]
pub(crate) struct PyPreview {
    text: String,
    html: String,
    phase_timings: Py<PyAny>,
    execution_profile: Py<PyAny>,
    trace_report: Option<serde_json::Value>,
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
        let trace_report = preview.to_trace_event_json_value();

        Ok(Self {
            text: preview.text().to_owned(),
            html: preview.html().to_owned(),
            phase_timings: json_value_to_py(py, &phase_timings)?,
            execution_profile: json_value_to_py(py, &execution_profile)?,
            trace_report,
        })
    }
}

#[pymethods]
impl PyPreview {
    #[getter]
    fn text(&self) -> &str {
        &self.text
    }

    #[getter]
    fn html(&self) -> &str {
        &self.html
    }

    #[getter]
    fn phase_timings(&self, py: Python<'_>) -> Py<PyAny> {
        self.phase_timings.clone_ref(py)
    }

    #[getter]
    fn execution_profile(&self, py: Python<'_>) -> Py<PyAny> {
        self.execution_profile.clone_ref(py)
    }

    /// Writes the full preview wall-clock timeline as Chrome Trace Event JSON.
    ///
    /// The resulting file can be opened by VizTracer's `vizviewer`, Perfetto,
    /// and other viewers that accept Chrome Trace Event JSON.
    fn export_trace(&self, py: Python<'_>, path: PathBuf) -> PyResult<()> {
        let trace = self.trace_report.as_ref().ok_or_else(|| {
            config_py_error(
                py,
                "execution_profile_unavailable",
                "trace export requires a preview created with `profile=True`".to_owned(),
            )
        })?;
        write_trace_json(&path, trace)
    }

    fn __str__(&self) -> &str {
        &self.text
    }

    fn __repr__(&self) -> &str {
        &self.text
    }

    fn _repr_html_(&self) -> &str {
        &self.html
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
        let table = self
            .session
            .borrow_mut(py)
            .register_table_alias(py, name, &self.inner)?;
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
    /// `dict` report. Pass `profile=True` on execute calls to attach a detailed
    /// query execution profile and full operation timeline. Pass `trace_path`
    /// with `profile=True` to export Chrome Trace Event JSON after success.
    /// Profiling and trace export are not available for dry runs.
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
    #[pyo3(signature = (*, schema, table, load_mode, dry_run=None, name=None, connection_string=None, progress=None, profile=false, trace_path=None))]
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
        #[pyo3(from_py_with = parse_profile_arg)] profile: bool,
        trace_path: Option<PathBuf>,
    ) -> PyResult<Py<PyAny>> {
        if dry_run == Some(true) && profile {
            return Err(config_py_error(
                py,
                "invalid_option_value",
                "`profile=True` is only supported for execute `write_to_mssql` calls".to_owned(),
            ));
        }
        if dry_run == Some(true) && trace_path.is_some() {
            return Err(config_py_error(
                py,
                "invalid_option_value",
                "`trace_path` is only supported for execute `write_to_mssql` calls".to_owned(),
            ));
        }
        if trace_path.is_some() && !profile {
            return Err(config_py_error(
                py,
                "invalid_option_value",
                "`trace_path` requires `profile=True`".to_owned(),
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

        let profile_mode = if profile {
            delta_funnel::ExecutionProfileMode::Detailed
        } else {
            delta_funnel::ExecutionProfileMode::Disabled
        };
        self.session.borrow(py).write_to_mssql(
            py,
            &spec.write_plan(delta_funnel::RunMode::Execute),
            profile_mode,
            progress.as_ref(),
            trace_path.as_deref(),
        )
    }

    /// Returns a bounded rendered preview of this lazy table.
    ///
    /// Progress appears automatically in interactive terminals and notebooks.
    /// Pass `progress=True` to force it or `progress=False` to disable it. The
    /// progress display closes before the `Preview` object is returned. Phase
    /// timings are always attached. Pass `profile=True` to also attach the
    /// detailed execution profile.
    #[pyo3(signature = (limit=20, *, progress=None, profile=false))]
    fn preview(
        &self,
        py: Python<'_>,
        limit: usize,
        progress: Option<bool>,
        #[pyo3(from_py_with = parse_profile_arg)] profile: bool,
    ) -> PyResult<PyPreview> {
        let profile_mode = if profile {
            delta_funnel::ExecutionProfileMode::Detailed
        } else {
            delta_funnel::ExecutionProfileMode::Disabled
        };
        let options =
            delta_funnel::PreviewOptions::new(limit).with_execution_profile_mode(profile_mode);
        let progress = PythonProgress::for_preview(progress);
        let preview =
            self.session
                .borrow(py)
                .preview_table(py, &self.inner, options, progress.as_ref())?;
        PyPreview::new(py, preview)
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

pub(crate) fn write_trace_json(path: &Path, trace: &serde_json::Value) -> PyResult<()> {
    let bytes =
        serde_json::to_vec(trace).map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    fs::write(path, bytes).map_err(PyOSError::new_err)
}

fn parse_profile_arg(profile: &Bound<'_, PyAny>) -> PyResult<bool> {
    if profile.is_none() {
        return Ok(false);
    }
    if !profile.is_instance_of::<PyBool>() {
        return Err(config_py_error(
            profile.py(),
            "invalid_option_value",
            "`profile` must be a bool or None".to_owned(),
        ));
    }
    profile.extract::<bool>()
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

    use crate::{
        deltafunnel, exception::DeltaFunnelError, progress::adapter_creation_count,
        test_support::python_state,
    };
    use pyo3::exceptions::{PyAttributeError, PyKeyError, PyRuntimeError, PyTypeError};
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
    fn pyi_stub_exposes_preview_trace_export() {
        let stub = include_str!("../deltafunnel.pyi");

        assert!(stub.contains("def export_trace(self, path: str | PathLike[str]) -> None: ..."));
        assert!(stub.contains("trace_path: str | PathLike[str] | None = None"));
    }

    #[test]
    fn write_trace_path_is_keyword_only_and_requires_detailed_profile() -> PyResult<()> {
        Python::attach(|py| {
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1("table_from_sql", ("select 1 as id",))?;
            let signature = py
                .import("inspect")?
                .call_method1("signature", (table.getattr("write_to_mssql")?,))?
                .to_string();
            assert!(signature.ends_with("profile=False, trace_path=None)"));

            let trace_path = temp_trace_path("write-without-profile")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("schema", "dbo")?;
            kwargs.set_item("table", "orders")?;
            kwargs.set_item("load_mode", "append_existing")?;
            kwargs.set_item("trace_path", trace_path.to_string_lossy().as_ref())?;
            let error = table
                .call_method("write_to_mssql", (), Some(&kwargs))
                .unwrap_err();

            assert!(error.is_instance_of::<DeltaFunnelError>(py));
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_option_value"
            );
            assert!(
                error
                    .value(py)
                    .getattr("message")?
                    .extract::<String>()?
                    .contains("requires `profile=True`")
            );
            assert!(!trace_path.exists());
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
                "(limit=20, *, progress=None, profile=False)"
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
            let trace_path = temp_trace_path("disabled")?;
            let error = preview
                .call_method1("export_trace", (trace_path.to_string_lossy().as_ref(),))
                .unwrap_err();
            assert!(error.is_instance_of::<DeltaFunnelError>(py));
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "execution_profile_unavailable"
            );
            assert!(!trace_path.exists());
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
            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", false)?;
            kwargs.set_item("profile", true)?;

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

            let path = temp_trace_path("detailed")?;
            let path_object = py
                .import("pathlib")?
                .getattr("Path")?
                .call1((path.to_string_lossy().as_ref(),))?;
            preview.call_method1("export_trace", (path_object,))?;
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
            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", false)?;
            kwargs.set_item("profile", true)?;

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

            let invalid_profiles = PyList::empty(py);
            invalid_profiles.append(0)?;
            invalid_profiles.append(1)?;
            invalid_profiles.append("detailed")?;
            invalid_profiles.append(PyList::empty(py))?;
            invalid_profiles.append(PyDict::new(py))?;
            for profile in invalid_profiles.iter() {
                let kwargs = PyDict::new(py);
                kwargs.set_item("progress", true)?;
                kwargs.set_item("profile", profile)?;
                let error = table.call_method("preview", (), Some(&kwargs)).unwrap_err();
                assert!(error.is_instance_of::<DeltaFunnelError>(py));
                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "config"
                );
                assert_eq!(
                    error.value(py).getattr("kind")?.extract::<String>()?,
                    "invalid_option_value"
                );
            }
            assert_eq!(adapter_creation_count(), initial_count);

            let failing_table = session.call_method1(
                "table_from_sql",
                ("select cast(1 as bigint) / cast(0 as bigint) as value",),
            )?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("progress", true)?;
            kwargs.set_item("profile", "detailed")?;
            let error = failing_table
                .call_method("preview", (), Some(&kwargs))
                .unwrap_err();
            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "invalid_option_value"
            );
            assert_eq!(adapter_creation_count(), initial_count);

            for profile in [
                py.None(),
                false.into_pyobject(py)?.to_owned().unbind().into_any(),
            ] {
                let kwargs = PyDict::new(py);
                kwargs.set_item("progress", false)?;
                kwargs.set_item("profile", profile)?;
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
