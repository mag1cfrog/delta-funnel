//! Shows core progress events with one lazily created Rich progress task.
//!
//! Each Python write creates its own adapter. The adapter imports nothing from
//! Rich until the Rust action starts, then updates the same task until the
//! action finishes. Rich chooses terminal or Jupyter rendering.

use std::sync::{Arc, Mutex};

use delta_funnel::progress::{
    ProgressEvent, ProgressEventKind, ProgressOperation, ProgressPhase, ProgressReporter,
};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Connects one Rust action to one optional Rich progress display.
pub(crate) struct PythonProgress {
    reporter: ProgressReporter,
    state: Arc<Mutex<ProgressState>>,
}

impl PythonProgress {
    /// Creates an adapter unless the caller passed `progress=False`.
    ///
    /// `progress=None` shows progress only when Rich detects an interactive
    /// terminal or Jupyter. `progress=True` also shows it in scripts and CI.
    pub(crate) fn new(progress: Option<bool>) -> Option<Self> {
        let mode = match progress {
            Some(false) => return None,
            Some(true) => ProgressMode::Forced,
            None => ProgressMode::Automatic,
        };
        let state = Arc::new(Mutex::new(ProgressState::new(mode)));
        let reporter_state = Arc::clone(&state);
        let reporter = ProgressReporter::new(move |event| render_event(&reporter_state, event));
        Some(Self { reporter, state })
    }

    /// Returns the reporter that the Rust action uses to send progress events.
    pub(crate) fn reporter(&self) -> ProgressReporter {
        self.reporter.clone()
    }

    /// Closes the Rich progress display after the Rust action finishes.
    ///
    /// If Rich raised a Python interruption such as `KeyboardInterrupt` during
    /// the action, returns that same exception now. When the Rust action also
    /// failed, attaches its sanitized Python error for callers to inspect.
    pub(crate) fn finish(&self, py: Python<'_>, operation_error: Option<&PyErr>) -> PyResult<()> {
        // Set the shared state to Done before calling Rich. If Rich calls back
        // into this adapter while stopping, it cannot stop the display twice.
        let mut state = {
            let mut shared = match self.state.lock() {
                Ok(shared) => shared,
                Err(poisoned) => poisoned.into_inner(),
            };
            std::mem::replace(&mut *shared, ProgressState::done())
        };

        if let RenderState::Active { renderer, .. } = state.render
            && let Err(error) = renderer.progress.call_method0(py, "stop")
            && !error.is_instance_of::<PyException>(py)
            && state.pending_interruption.is_none()
        {
            state.pending_interruption = Some(error);
        }

        let status = action_status(state.final_event, operation_error.is_some());
        match state.pending_interruption {
            Some(error) => {
                // Metadata is best effort. A custom exception may reject
                // attributes, but it must still be raised unchanged.
                let _ = error
                    .value(py)
                    .setattr("deltafunnel_operation_status", status);
                if let Some(operation_error) = operation_error {
                    let _ = error
                        .value(py)
                        .setattr("deltafunnel_operation_error", operation_error.value(py));
                }
                write_interruption_notice(py, status);
                Err(error)
            }
            None => Ok(()),
        }
    }
}

/// Controls whether Rich may hide progress in a non-interactive process.
#[derive(Clone, Copy)]
enum ProgressMode {
    /// Show progress only in an interactive terminal or Jupyter.
    Automatic,
    /// Show progress even in scripts, pipes, or CI.
    Forced,
}

/// Python handles needed to update one Rich task.
struct RichRenderer {
    progress: Py<PyAny>,
    task_id: Py<PyAny>,
}

/// Current display state and the first Python interruption waiting to be raised.
struct ProgressState {
    render: RenderState,
    pending_interruption: Option<PyErr>,
    final_event: Option<ProgressEventKind>,
}

impl ProgressState {
    const fn new(mode: ProgressMode) -> Self {
        Self {
            render: RenderState::Pending(mode),
            pending_interruption: None,
            final_event: None,
        }
    }

    const fn busy() -> Self {
        Self {
            render: RenderState::Busy,
            pending_interruption: None,
            final_event: None,
        }
    }

    const fn done() -> Self {
        Self {
            render: RenderState::Done,
            pending_interruption: None,
            final_event: None,
        }
    }
}

/// Current state of one action's Rich display.
enum RenderState {
    /// The Rust action has not started, so no Rich objects exist yet.
    Pending(ProgressMode),
    /// A Rich task exists and must still be stopped after the Rust action.
    Active {
        renderer: RichRenderer,
        /// False after an update fails; final cleanup is still allowed.
        updates_enabled: bool,
    },
    /// A Rich call is running while the task is temporarily outside the mutex.
    Busy,
    /// Progress is disabled, unavailable, or already closed.
    Done,
}

/// Result of trying to call Rich from a Rust progress callback.
enum PythonCall<T> {
    /// Rich returned normally.
    Succeeded(T),
    /// Python was unavailable or Rich raised an ordinary `Exception`.
    Failed,
    /// Rich raised a `BaseException` such as `KeyboardInterrupt`.
    Interrupted(PyErr),
}

/// Calls Rich when Python is available and classifies any Python exception.
fn try_python<T>(call: impl for<'py> FnOnce(Python<'py>) -> PyResult<T>) -> PythonCall<T> {
    Python::try_attach(|py| match call(py) {
        Ok(value) => PythonCall::Succeeded(value),
        Err(error) if error.is_instance_of::<PyException>(py) => PythonCall::Failed,
        Err(error) => PythonCall::Interrupted(error),
    })
    .unwrap_or(PythonCall::Failed)
}

/// Returns Rich's value, or saves the first interruption for `finish`.
fn successful<T>(call: PythonCall<T>, pending_interruption: &mut Option<PyErr>) -> Option<T> {
    match call {
        PythonCall::Succeeded(value) => Some(value),
        PythonCall::Failed => None,
        PythonCall::Interrupted(error) => {
            if pending_interruption.is_none() {
                *pending_interruption = Some(error);
            }
            None
        }
    }
}

/// Handles one Rust progress event and updates Rich when needed.
fn render_event(state: &Mutex<ProgressState>, event: &ProgressEvent) {
    // Take the state out and release the mutex before calling Rich. Python code
    // may call other code, so running it while holding the mutex could deadlock.
    let current = {
        let mut state = match state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        std::mem::replace(&mut *state, ProgressState::busy())
    };

    let ProgressState {
        render,
        mut pending_interruption,
        final_event,
    } = current;
    let final_event = if ends_action(event.kind()) {
        Some(event.kind())
    } else {
        final_event
    };
    let render = match render {
        // Wait for Started before importing Rich. Requests rejected before the
        // action begins should not perform any progress-related Python work.
        RenderState::Pending(mode) if event.kind() == ProgressEventKind::Started => successful(
            try_python(|py| create_renderer(py, mode, event)),
            &mut pending_interruption,
        )
        .flatten()
        .map_or(RenderState::Done, |renderer| {
            // Keep ownership even if start fails so finish can still make the
            // one stop attempt after the Rust action returns.
            let updates_enabled = successful(
                try_python(|py| start_renderer(py, &renderer)),
                &mut pending_interruption,
            )
            .is_some();
            RenderState::Active {
                renderer,
                updates_enabled,
            }
        }),
        // Show the final result now, but keep the task open. `finish` stops it
        // only after the Rust action has returned and completed its cleanup.
        RenderState::Active {
            renderer,
            updates_enabled,
        } if ends_action(event.kind()) => {
            let updates_enabled = if updates_enabled {
                successful(
                    try_python(|py| update_renderer(py, &renderer, terminal_label(event.kind()))),
                    &mut pending_interruption,
                )
                .is_some()
            } else {
                false
            };
            RenderState::Active {
                renderer,
                updates_enabled,
            }
        }
        RenderState::Active {
            renderer,
            updates_enabled: true,
        } if event.kind() == ProgressEventKind::PhaseChanged => {
            // This issue shows phases only. #434 will handle numeric Progress
            // events. If this update fails, stop later updates but keep the
            // task so `finish` can still close it once.
            let updated = event.phase().is_none_or(|phase| {
                successful(
                    try_python(|py| update_renderer(py, &renderer, phase_label(phase))),
                    &mut pending_interruption,
                )
                .is_some()
            });
            RenderState::Active {
                renderer,
                updates_enabled: updated,
            }
        }
        state @ RenderState::Active { .. }
        | state @ RenderState::Pending(_)
        | state @ RenderState::Done => state,
        RenderState::Busy => RenderState::Done,
    };
    let next = ProgressState {
        render,
        pending_interruption,
        final_event,
    };

    // Rust sends these events one at a time, so returning the state here cannot
    // overwrite work from another progress callback.
    let mut state = match state.lock() {
        Ok(state) => state,
        Err(poisoned) => poisoned.into_inner(),
    };
    *state = next;
}

/// Creates one Rich display after the Rust action has started.
///
/// Returns `Ok(None)` when automatic progress should stay hidden.
fn create_renderer(
    py: Python<'_>,
    mode: ProgressMode,
    event: &ProgressEvent,
) -> PyResult<Option<RichRenderer>> {
    // Rich detects the terminal or Jupyter environment. Delta Funnel only asks
    // for stderr in terminals and tells Rich when progress is forced.
    let console_type = py.import("rich.console")?.getattr("Console")?;
    let console_kwargs = PyDict::new(py);
    console_kwargs.set_item("stderr", true)?;
    if matches!(mode, ProgressMode::Forced) {
        console_kwargs.set_item("force_interactive", true)?;
    }
    let console = console_type.call((), Some(&console_kwargs))?;

    // Rich reports Jupyter separately from interactive terminals. Automatic
    // mode stays quiet only when Rich reports neither one.
    let is_interactive = console.getattr("is_interactive")?.extract::<bool>()?;
    let is_jupyter = console.getattr("is_jupyter")?.extract::<bool>()?;
    if matches!(mode, ProgressMode::Automatic) && !is_interactive && !is_jupyter {
        return Ok(None);
    }

    // Use the same columns in terminals and notebooks: elapsed time, current
    // phase, progress bar, and numeric progress when a total becomes available.
    let progress_module = py.import("rich.progress")?;
    let progress_type = progress_module.getattr("Progress")?;
    let elapsed_column = progress_module.getattr("TimeElapsedColumn")?.call0()?;
    let bar_column = progress_module.getattr("BarColumn")?.call0()?;
    let task_progress_column = progress_module.getattr("TaskProgressColumn")?.call0()?;

    // Refresh only when Rust sends an event. A background refresh thread is not
    // useful for these infrequent phase changes.
    let progress_kwargs = PyDict::new(py);
    progress_kwargs.set_item("console", console)?;
    progress_kwargs.set_item("auto_refresh", false)?;
    progress_kwargs.set_item("transient", false)?;
    progress_kwargs.set_item("redirect_stdout", false)?;
    progress_kwargs.set_item("redirect_stderr", false)?;
    let progress = progress_type.call(
        (
            elapsed_column,
            "{task.description}",
            bar_column,
            task_progress_column,
        ),
        Some(&progress_kwargs),
    )?;

    // Start without a total. #434 will add the file total to this same task so
    // the display and elapsed time continue without restarting.
    let task_kwargs = PyDict::new(py);
    task_kwargs.set_item("total", py.None())?;
    let task_id = progress.call_method(
        "add_task",
        (operation_label(event.operation()),),
        Some(&task_kwargs),
    )?;
    Ok(Some(RichRenderer {
        progress: progress.unbind(),
        task_id: task_id.unbind(),
    }))
}

/// Starts a fully constructed Rich task.
fn start_renderer(py: Python<'_>, renderer: &RichRenderer) -> PyResult<()> {
    renderer.progress.call_method0(py, "start")?;
    Ok(())
}

/// Shows a new description immediately in both terminals and notebooks.
fn update_renderer(py: Python<'_>, renderer: &RichRenderer, description: &str) -> PyResult<()> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("description", description)?;
    kwargs.set_item("refresh", true)?;
    renderer
        .progress
        .call_method(py, "update", (renderer.task_id.bind(py),), Some(&kwargs))?;
    Ok(())
}

/// Returns the Rust action result reported with a saved Python interruption.
const fn action_status(final_event: Option<ProgressEventKind>, failed: bool) -> &'static str {
    match final_event {
        Some(ProgressEventKind::Completed) => "completed",
        Some(ProgressEventKind::CompletedWithFailures) => "completed_with_failures",
        Some(ProgressEventKind::Failed) => "failed",
        Some(ProgressEventKind::Cancelled) => "cancelled",
        _ if failed => "failed",
        _ => "completed",
    }
}

/// Makes one best-effort attempt to explain why no Python result was returned.
fn write_interruption_notice(py: Python<'_>, status: &str) {
    let message =
        format!("DeltaFunnel action status: {status}; the Python result was not delivered.\n");
    let _ = py.import("sys").and_then(|sys| {
        sys.getattr("stderr")?.call_method1("write", (message,))?;
        Ok(())
    });
}

/// Returns true when Rust will send no more progress for this action.
///
/// This describes the action state, not terminal versus Jupyter rendering.
const fn ends_action(kind: ProgressEventKind) -> bool {
    matches!(
        kind,
        ProgressEventKind::Completed
            | ProgressEventKind::CompletedWithFailures
            | ProgressEventKind::Failed
            | ProgressEventKind::Cancelled
    )
}

/// Returns the text shown before Rust reports the first phase.
const fn operation_label(operation: Option<ProgressOperation>) -> &'static str {
    match operation {
        Some(ProgressOperation::WriteToMssql) => "Writing to SQL Server",
        Some(ProgressOperation::DryRunToMssql) => "Planning SQL Server write",
        _ => "Running SQL Server action",
    }
}

/// Returns safe, stable text for an internal Rust phase.
const fn phase_label(phase: ProgressPhase) -> &'static str {
    match phase {
        ProgressPhase::PlanningOutput => "Planning output",
        ProgressPhase::SettingUpStream => "Preparing data stream",
        ProgressPhase::Connecting => "Connecting to SQL Server",
        ProgressPhase::PreparingTarget => "Preparing target table",
        ProgressPhase::Writing => "Writing to SQL Server",
        ProgressPhase::Validating => "Validating write",
        ProgressPhase::SwappingTarget => "Swapping target table",
        ProgressPhase::CleaningUp => "Cleaning up",
        _ => "Working",
    }
}

/// Returns the final text shown when the action ends.
const fn terminal_label(kind: ProgressEventKind) -> &'static str {
    match kind {
        ProgressEventKind::Completed => "Completed",
        ProgressEventKind::CompletedWithFailures => "Completed with failures",
        ProgressEventKind::Failed => "Failed",
        ProgressEventKind::Cancelled => "Cancelled",
        _ => "Finished",
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

    use pyo3::exceptions::{PyGeneratorExit, PyKeyboardInterrupt, PyRuntimeError, PySystemExit};
    use pyo3::ffi::c_str;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods, PyModule};

    use super::*;
    use crate::{deltafunnel, test_support::python_state};

    const MODULE_NAMES: [&str; 3] = ["rich", "rich.console", "rich.progress"];
    type ModuleSnapshot = Vec<(&'static str, Option<Py<PyAny>>)>;

    struct ModuleGuard {
        originals: Vec<(&'static str, Option<Py<PyAny>>)>,
    }

    struct StderrGuard {
        original: Py<PyAny>,
    }

    impl StderrGuard {
        fn capture(py: Python<'_>) -> PyResult<(Self, Py<PyAny>)> {
            let capture = py.import("io")?.call_method0("StringIO")?.unbind();
            let guard = Self::replace(py, capture.bind(py))?;
            Ok((guard, capture))
        }

        fn replace(py: Python<'_>, replacement: &Bound<'_, PyAny>) -> PyResult<Self> {
            let sys = py.import("sys")?;
            let original = sys.getattr("stderr")?.unbind();
            sys.setattr("stderr", replacement)?;
            Ok(Self { original })
        }
    }

    impl Drop for StderrGuard {
        fn drop(&mut self) {
            let _ = Python::try_attach(|py| {
                py.import("sys")?.setattr("stderr", self.original.bind(py))
            });
        }
    }

    impl ModuleGuard {
        fn snapshot(py: Python<'_>) -> PyResult<ModuleSnapshot> {
            let modules = py
                .import("sys")?
                .getattr("modules")?
                .cast_into::<PyDict>()?;
            MODULE_NAMES
                .iter()
                .map(|name| {
                    modules
                        .get_item(name)
                        .map(|value| (*name, value.map(Bound::unbind)))
                })
                .collect()
        }

        fn install(
            py: Python<'_>,
            interactive: bool,
            jupyter: bool,
        ) -> PyResult<(Self, Py<PyList>)> {
            let (guard, records, _) =
                Self::install_with_failure(py, interactive, jupyter, None, false, false)?;
            Ok((guard, records))
        }

        fn install_with_failure(
            py: Python<'_>,
            interactive: bool,
            jupyter: bool,
            fail_call: Option<&str>,
            interruption: bool,
            stop_also_interrupts: bool,
        ) -> PyResult<(Self, Py<PyList>, Py<PyAny>)> {
            let failure = if interruption {
                py.get_type::<PyKeyboardInterrupt>()
                    .call1(("renderer interrupted",))?
            } else {
                py.get_type::<PyRuntimeError>()
                    .call1(("renderer failed",))?
            };
            Self::install_with_exception(
                py,
                interactive,
                jupyter,
                fail_call,
                failure,
                stop_also_interrupts,
            )
        }

        fn install_with_exception(
            py: Python<'_>,
            interactive: bool,
            jupyter: bool,
            fail_call: Option<&str>,
            failure: Bound<'_, PyAny>,
            stop_also_interrupts: bool,
        ) -> PyResult<(Self, Py<PyList>, Py<PyAny>)> {
            let originals = Self::snapshot(py)?;
            let records = PyList::empty(py);
            let locals = PyDict::new(py);
            locals.set_item("records", &records)?;
            locals.set_item("interactive", interactive)?;
            locals.set_item("jupyter", jupyter)?;
            locals.set_item("fail_call", fail_call)?;
            locals.set_item("failure", &failure)?;
            locals.set_item("stop_also_interrupts", stop_also_interrupts)?;
            locals.set_item(
                "stop_failure",
                py.get_type::<PySystemExit>().call1(("stop interrupted",))?,
            )?;
            py.run(
                c_str!(
                    r#"
import sys
import types

def maybe_fail(call):
    if fail_call == call:
        raise failure
    if stop_also_interrupts and call == "stop":
        raise stop_failure

class Console:
    def __init__(self, **kwargs):
        records.append({"call": "console", **kwargs})
        maybe_fail("console")
        self.is_interactive = interactive or kwargs.get("force_interactive", False)
        self.is_jupyter = jupyter

class Progress:
    def __init__(self, *columns, **kwargs):
        records.append({
            "call": "progress",
            "columns": len(columns),
            "auto_refresh": kwargs.get("auto_refresh"),
            "transient": kwargs.get("transient"),
            "redirect_stdout": kwargs.get("redirect_stdout"),
            "redirect_stderr": kwargs.get("redirect_stderr"),
        })
        maybe_fail("progress")

    def add_task(self, description, **kwargs):
        records.append({"call": "add_task", "description": description, "total": kwargs.get("total")})
        maybe_fail("add_task")
        return 7

    def start(self):
        records.append({"call": "start"})
        maybe_fail("start")

    def update(self, task_id, **kwargs):
        records.append({"call": "update", "task_id": task_id, **kwargs})
        terminal = kwargs.get("description") in {
            "Completed", "Completed with failures", "Failed", "Cancelled"
        }
        maybe_fail("terminal" if terminal else "update")

    def stop(self):
        records.append({"call": "stop"})
        maybe_fail("stop")

rich = types.ModuleType("rich")
rich.__path__ = []
console_module = types.ModuleType("rich.console")
console_module.Console = Console
progress_module = types.ModuleType("rich.progress")
progress_module.Progress = Progress
progress_module.TimeElapsedColumn = object
progress_module.BarColumn = object
progress_module.TaskProgressColumn = object
rich.console = console_module
rich.progress = progress_module
sys.modules["rich"] = rich
sys.modules["rich.console"] = console_module
sys.modules["rich.progress"] = progress_module
"#
                ),
                Some(&locals),
                Some(&locals),
            )?;
            Ok((Self { originals }, records.unbind(), failure.unbind()))
        }
    }

    impl Drop for ModuleGuard {
        fn drop(&mut self) {
            let _ = Python::try_attach(|py| -> PyResult<()> {
                let modules = py
                    .import("sys")?
                    .getattr("modules")?
                    .cast_into::<PyDict>()?;
                for (name, original) in &self.originals {
                    if let Some(original) = original {
                        modules.set_item(name, original.bind(py))?;
                    } else {
                        modules.del_item(name)?;
                    }
                }
                Ok(())
            });
        }
    }

    fn dry_run(py: Python<'_>, progress: Option<Option<bool>>) -> PyResult<()> {
        let module = PyModule::new(py, "deltafunnel")?;
        deltafunnel(&module)?;
        let session = module.getattr("Session")?.call0()?;
        let table = session.call_method1("table_from_sql", ("select 1 as id",))?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("schema", "dbo")?;
        kwargs.set_item("table", "orders")?;
        kwargs.set_item("load_mode", "create_and_load")?;
        kwargs.set_item("dry_run", true)?;
        kwargs.set_item(
            "connection_string",
            "server=tcp:sql.example.com;password=secret-token",
        )?;
        if let Some(progress) = progress {
            kwargs.set_item("progress", progress)?;
        }
        table.call_method("write_to_mssql", (), Some(&kwargs))?;
        Ok(())
    }

    fn execute_without_connection(py: Python<'_>) -> PyResult<()> {
        let module = PyModule::new(py, "deltafunnel")?;
        deltafunnel(&module)?;
        let session = module.getattr("Session")?.call0()?;
        let table = session.call_method1("table_from_sql", ("select 1 as id",))?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("schema", "dbo")?;
        kwargs.set_item("table", "orders")?;
        kwargs.set_item("load_mode", "append_existing")?;
        kwargs.set_item("progress", true)?;
        table.call_method("write_to_mssql", (), Some(&kwargs))?;
        Ok(())
    }

    fn record_strings(records: &Bound<'_, PyList>, key: &str) -> PyResult<Vec<String>> {
        records
            .iter()
            .map(|record| record.get_item(key)?.extract::<String>())
            .collect()
    }

    fn assert_modules_match(
        py: Python<'_>,
        expected: &[(&str, Option<Py<PyAny>>)],
    ) -> PyResult<()> {
        let modules = py
            .import("sys")?
            .getattr("modules")?
            .cast_into::<PyDict>()?;
        for (name, expected) in expected {
            let actual = modules.get_item(name)?;
            match (actual, expected) {
                (Some(actual), Some(expected)) => assert!(actual.is(expected.bind(py))),
                (None, None) => {}
                _ => {
                    return Err(PyRuntimeError::new_err(format!(
                        "module {name} was not restored"
                    )));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn python_state_is_restored_after_unwind_and_poisoned_lock_recovers() -> PyResult<()> {
        let mut baseline = None;

        let unwind = catch_unwind(AssertUnwindSafe(|| {
            let _state = python_state();
            let snapshot = Python::attach(ModuleGuard::snapshot);
            let Ok(snapshot) = snapshot else {
                resume_unwind(Box::new("failed to capture Python module state"));
            };
            baseline = Some(snapshot);
            let installed = Python::attach(|py| ModuleGuard::install(py, true, false));
            let Ok((_modules, _records)) = installed else {
                resume_unwind(Box::new("failed to install fake Rich modules"));
            };
            resume_unwind(Box::new("test unwind"));
        }));
        assert!(unwind.is_err());
        let Some(baseline) = baseline else {
            return Err(PyRuntimeError::new_err(
                "test unwind occurred before capturing Python module state",
            ));
        };

        // The first guard was poisoned by the unwind. Reacquiring it proves
        // that python_state recovers the lock instead of failing later tests.
        let _state = python_state();
        Python::attach(|py| assert_modules_match(py, &baseline))
    }

    #[test]
    fn progress_false_skips_rich_entirely() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, true)?;
            dry_run(py, Some(Some(false)))?;
            assert!(records.bind(py).is_empty());
            Ok(())
        })
    }

    #[test]
    fn binding_validation_finishes_before_progress_starts() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;
            let module = PyModule::new(py, "deltafunnel")?;
            deltafunnel(&module)?;
            let session = module.getattr("Session")?.call0()?;
            let table = session.call_method1("table_from_sql", ("select 1 as id",))?;

            for (target, load_mode) in [
                ("orders", "not_a_load_mode"),
                ("orders.invalid", "create_and_load"),
            ] {
                let kwargs = PyDict::new(py);
                kwargs.set_item("schema", "dbo")?;
                kwargs.set_item("table", target)?;
                kwargs.set_item("load_mode", load_mode)?;
                kwargs.set_item("progress", true)?;
                assert!(
                    table
                        .call_method("write_to_mssql", (), Some(&kwargs))
                        .is_err()
                );
            }

            assert!(records.bind(py).is_empty());
            Ok(())
        })
    }

    #[test]
    fn planning_failure_after_started_shows_one_failed_lifecycle() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;

            let error = execute_without_connection(py).unwrap_err();

            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "missing_mssql_connection"
            );
            assert_eq!(
                record_strings(records.bind(py), "call")?,
                [
                    "console", "progress", "add_task", "start", "update", "update", "stop"
                ]
            );
            assert_eq!(
                records
                    .bind(py)
                    .get_item(4)?
                    .get_item("description")?
                    .extract::<String>()?,
                "Planning output"
            );
            assert_eq!(
                records
                    .bind(py)
                    .get_item(5)?
                    .get_item("description")?
                    .extract::<String>()?,
                "Failed"
            );
            Ok(())
        })
    }

    #[test]
    fn omitted_and_none_progress_are_both_quiet_when_noninteractive() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, false, false)?;
            dry_run(py, None)?;
            dry_run(py, Some(None))?;
            assert_eq!(
                record_strings(records.bind(py), "call")?,
                ["console", "console"]
            );
            Ok(())
        })
    }

    #[test]
    fn automatic_progress_uses_one_rich_task_in_jupyter() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, false, true)?;
            dry_run(py, None)?;

            let records = records.bind(py);
            assert_eq!(
                record_strings(records, "call")?,
                [
                    "console", "progress", "add_task", "start", "update", "update", "stop"
                ]
            );
            let console = records.get_item(0)?.cast_into::<PyDict>()?;
            assert!(console.get_item("stderr")?.unwrap().extract::<bool>()?);
            let progress = records.get_item(1)?.cast_into::<PyDict>()?;
            assert_eq!(progress.get_item("columns")?.unwrap().extract::<u8>()?, 4);
            assert!(
                !progress
                    .get_item("auto_refresh")?
                    .unwrap()
                    .extract::<bool>()?
            );
            assert!(!progress.get_item("transient")?.unwrap().extract::<bool>()?);
            assert!(
                !progress
                    .get_item("redirect_stdout")?
                    .unwrap()
                    .extract::<bool>()?
            );
            assert!(
                !progress
                    .get_item("redirect_stderr")?
                    .unwrap()
                    .extract::<bool>()?
            );
            let task = records.get_item(2)?.cast_into::<PyDict>()?;
            assert_eq!(
                task.get_item("description")?.unwrap().extract::<String>()?,
                "Planning SQL Server write"
            );
            assert!(task.get_item("total")?.unwrap().is_none());
            assert_eq!(
                records
                    .get_item(4)?
                    .get_item("description")?
                    .extract::<String>()?,
                "Planning output"
            );
            assert!(
                records
                    .get_item(4)?
                    .get_item("refresh")?
                    .extract::<bool>()?
            );
            assert_eq!(
                records
                    .get_item(5)?
                    .get_item("description")?
                    .extract::<String>()?,
                "Completed"
            );
            Ok(())
        })
    }

    #[test]
    fn automatic_progress_also_renders_in_interactive_terminals() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;
            dry_run(py, None)?;
            assert!(record_strings(records.bind(py), "call")?.contains(&"progress".to_owned()));
            Ok(())
        })
    }

    #[test]
    fn forced_progress_renders_when_noninteractive() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, false, false)?;
            dry_run(py, Some(Some(true)))?;
            let console = records.bind(py).get_item(0)?.cast_into::<PyDict>()?;
            assert!(
                console
                    .get_item("force_interactive")?
                    .unwrap()
                    .extract::<bool>()?
            );
            assert!(record_strings(records.bind(py), "call")?.contains(&"progress".to_owned()));
            Ok(())
        })
    }

    #[test]
    fn renderer_arguments_exclude_query_and_connection_secrets() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;

            dry_run(py, Some(Some(true)))?;

            let rendered = records.bind(py).repr()?.extract::<String>()?;
            for secret in [
                "select 1 as id",
                "sql.example.com",
                "password=secret-token",
                "secret-token",
            ] {
                assert!(!rendered.contains(secret));
            }
            Ok(())
        })
    }

    #[test]
    fn ordinary_construction_failures_disable_progress_without_cleanup() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            for stage in ["console", "progress", "add_task"] {
                let (guard, records, _failure) =
                    ModuleGuard::install_with_failure(py, true, false, Some(stage), false, false)?;

                dry_run(py, Some(Some(true)))?;

                assert!(!record_strings(records.bind(py), "call")?.contains(&"stop".to_owned()));
                drop(guard);
            }
            Ok(())
        })
    }

    #[test]
    fn construction_interruptions_are_raised_after_the_action_without_cleanup() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            for stage in ["console", "progress", "add_task"] {
                let (guard, records, failure) =
                    ModuleGuard::install_with_failure(py, true, false, Some(stage), true, false)?;
                let (stderr, _capture) = StderrGuard::capture(py)?;

                let error = dry_run(py, Some(Some(true))).unwrap_err();

                assert!(error.value(py).is(failure.bind(py)));
                assert_eq!(
                    error
                        .value(py)
                        .getattr("deltafunnel_operation_status")?
                        .extract::<String>()?,
                    "completed"
                );
                assert!(!record_strings(records.bind(py), "call")?.contains(&"stop".to_owned()));

                drop(stderr);
                drop(guard);
            }
            Ok(())
        })
    }

    #[test]
    fn ordinary_start_failure_still_stops_the_constructed_task() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, _failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("start"), false, false)?;

            dry_run(py, Some(Some(true)))?;

            assert_eq!(
                record_strings(records.bind(py), "call")?,
                ["console", "progress", "add_task", "start", "stop"]
            );
            Ok(())
        })
    }

    #[test]
    fn start_interruption_is_raised_after_stopping_the_task() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("start"), true, false)?;
            let (_stderr, _capture) = StderrGuard::capture(py)?;

            let error = dry_run(py, Some(Some(true))).unwrap_err();

            assert!(error.value(py).is(failure.bind(py)));
            assert_eq!(
                record_strings(records.bind(py), "call")?,
                ["console", "progress", "add_task", "start", "stop"]
            );
            Ok(())
        })
    }

    #[test]
    fn ordinary_rich_update_failure_does_not_replace_the_report() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, _failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("update"), false, false)?;

            dry_run(py, Some(Some(true)))?;

            assert_eq!(
                record_strings(records.bind(py), "call")?,
                ["console", "progress", "add_task", "start", "update", "stop"]
            );
            Ok(())
        })
    }

    #[test]
    fn interruption_from_update_is_raised_after_stop() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("update"), true, false)?;
            let (_stderr, _capture) = StderrGuard::capture(py)?;

            let error = dry_run(py, Some(Some(true))).unwrap_err();

            assert!(error.is_instance_of::<PyKeyboardInterrupt>(py));
            assert!(error.value(py).is(failure.bind(py)));
            assert_eq!(
                record_strings(records.bind(py), "call")?,
                ["console", "progress", "add_task", "start", "update", "stop"]
            );
            Ok(())
        })
    }

    #[test]
    fn terminal_render_failures_follow_the_shared_failure_policy() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            for interruption in [false, true] {
                let (guard, records, failure) = ModuleGuard::install_with_failure(
                    py,
                    true,
                    false,
                    Some("terminal"),
                    interruption,
                    false,
                )?;
                let (stderr, _capture) = StderrGuard::capture(py)?;

                let result = dry_run(py, Some(Some(true)));

                if interruption {
                    let error = result.unwrap_err();
                    assert!(error.value(py).is(failure.bind(py)));
                } else {
                    result?;
                }
                assert_eq!(
                    record_strings(records.bind(py), "call")?,
                    [
                        "console", "progress", "add_task", "start", "update", "update", "stop"
                    ]
                );

                drop(stderr);
                drop(guard);
            }
            Ok(())
        })
    }

    #[test]
    fn interruption_reports_completed_status_once_on_stderr() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, _records, failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("update"), true, false)?;
            let (_stderr, capture) = StderrGuard::capture(py)?;

            let error = dry_run(py, Some(Some(true))).unwrap_err();

            assert!(error.value(py).is(failure.bind(py)));
            assert_eq!(
                error
                    .value(py)
                    .getattr("deltafunnel_operation_status")?
                    .extract::<String>()?,
                "completed"
            );
            assert!(
                error
                    .value(py)
                    .getattr("deltafunnel_operation_error")
                    .is_err()
            );
            assert_eq!(
                capture
                    .bind(py)
                    .call_method0("getvalue")?
                    .extract::<String>()?,
                "DeltaFunnel action status: completed; the Python result was not delivered.\n"
            );
            Ok(())
        })
    }

    #[test]
    fn ordinary_stop_failure_does_not_replace_the_report_or_retry() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, _failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("stop"), false, false)?;

            dry_run(py, Some(Some(true)))?;

            assert_eq!(
                record_strings(records.bind(py), "call")?
                    .iter()
                    .filter(|call| call.as_str() == "stop")
                    .count(),
                1
            );
            Ok(())
        })
    }

    #[test]
    fn interruption_from_stop_is_raised_as_the_same_object() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("stop"), true, false)?;
            let (_stderr, _capture) = StderrGuard::capture(py)?;

            let error = dry_run(py, Some(Some(true))).unwrap_err();

            assert!(error.value(py).is(failure.bind(py)));
            assert_eq!(
                record_strings(records.bind(py), "call")?.last(),
                Some(&"stop".to_owned())
            );
            Ok(())
        })
    }

    #[test]
    fn detached_execute_raises_saved_interruption_after_core_failure() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("update"), true, false)?;
            let (_stderr, _capture) = StderrGuard::capture(py)?;

            let error = execute_without_connection(py).unwrap_err();

            assert!(error.value(py).is(failure.bind(py)));
            let operation_error = error.value(py).getattr("deltafunnel_operation_error")?;
            assert_eq!(
                error
                    .value(py)
                    .getattr("deltafunnel_operation_status")?
                    .extract::<String>()?,
                "failed"
            );
            assert_eq!(
                operation_error.getattr("phase")?.extract::<String>()?,
                "mssql_target_config"
            );
            assert_eq!(
                operation_error.getattr("kind")?.extract::<String>()?,
                "missing_mssql_connection"
            );
            assert_eq!(
                record_strings(records.bind(py), "call")?.last(),
                Some(&"stop".to_owned())
            );
            Ok(())
        })
    }

    #[test]
    fn update_interruption_wins_when_stop_also_interrupts() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, first_interruption) =
                ModuleGuard::install_with_failure(py, true, false, Some("update"), true, true)?;
            let (_stderr, _capture) = StderrGuard::capture(py)?;

            let error = dry_run(py, Some(Some(true))).unwrap_err();

            assert!(error.value(py).is(first_interruption.bind(py)));
            assert_eq!(
                record_strings(records.bind(py), "call")?.last(),
                Some(&"stop".to_owned())
            );
            Ok(())
        })
    }

    #[test]
    fn built_in_interruptions_keep_identity_and_python_state() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            for exception_type in [
                py.get_type::<PyKeyboardInterrupt>(),
                py.get_type::<PySystemExit>(),
                py.get_type::<PyGeneratorExit>(),
            ] {
                let failure = exception_type.call1(("renderer interrupted", 42))?;
                let cause = py.get_type::<PyRuntimeError>().call1(("original cause",))?;
                let context = py
                    .get_type::<PyRuntimeError>()
                    .call1(("original context",))?;
                failure.setattr("__cause__", &cause)?;
                failure.setattr("__context__", &context)?;
                let (guard, records, failure) = ModuleGuard::install_with_exception(
                    py,
                    true,
                    false,
                    Some("update"),
                    failure,
                    false,
                )?;
                let (stderr, _capture) = StderrGuard::capture(py)?;

                let error = dry_run(py, Some(Some(true))).unwrap_err();
                let value = error.value(py);

                assert!(value.is(failure.bind(py)));
                assert_eq!(
                    value.getattr("args")?.extract::<(String, u8)>()?,
                    ("renderer interrupted".to_owned(), 42)
                );
                assert!(!value.getattr("__traceback__")?.is_none());
                assert!(value.getattr("__cause__")?.is(&cause));
                assert!(value.getattr("__context__")?.is(&context));
                assert_eq!(
                    value
                        .getattr("deltafunnel_operation_status")?
                        .extract::<String>()?,
                    "completed"
                );
                assert_eq!(
                    record_strings(records.bind(py), "call")?.last(),
                    Some(&"stop".to_owned())
                );

                drop(stderr);
                drop(guard);
            }
            Ok(())
        })
    }

    #[test]
    fn exceptions_that_reject_metadata_are_still_raised_unchanged() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let locals = PyDict::new(py);
            py.run(
                c_str!(
                    r#"
class RejectingKeyboardInterrupt(KeyboardInterrupt):
    def __setattr__(self, name, value):
        raise RuntimeError("metadata rejected")

class RejectingBaseException(BaseException):
    def __setattr__(self, name, value):
        raise RuntimeError("metadata rejected")

failures = [
    RejectingKeyboardInterrupt("keyboard marker"),
    RejectingBaseException("custom marker"),
]
"#
                ),
                Some(&locals),
                Some(&locals),
            )?;
            let failures = locals
                .get_item("failures")?
                .ok_or_else(|| PyRuntimeError::new_err("missing hostile exceptions"))?
                .cast_into::<PyList>()?;

            for failure in failures.iter() {
                let (guard, _records, failure) = ModuleGuard::install_with_exception(
                    py,
                    true,
                    false,
                    Some("update"),
                    failure,
                    false,
                )?;
                let (stderr, _capture) = StderrGuard::capture(py)?;

                let error = dry_run(py, Some(Some(true))).unwrap_err();

                assert!(error.value(py).is(failure.bind(py)));
                assert!(
                    error
                        .value(py)
                        .getattr("deltafunnel_operation_status")
                        .is_err()
                );

                drop(stderr);
                drop(guard);
            }
            Ok(())
        })
    }

    #[test]
    fn hostile_stderr_is_called_once_and_cannot_replace_the_interruption() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            for mode in ["short", "buffer", "fail", "interrupt", "mutate"] {
                let (module_guard, _records, failure) = ModuleGuard::install_with_failure(
                    py,
                    true,
                    false,
                    Some("update"),
                    true,
                    false,
                )?;
                let locals = PyDict::new(py);
                locals.set_item("failure", failure.bind(py))?;
                locals.set_item("mode", mode)?;
                py.run(
                    c_str!(
                        r#"
class HostileStderr:
    def __init__(self):
        self.calls = []
        self.buffered = None

    def write(self, message):
        self.calls.append(message)
        if mode == "short":
            return 1
        if mode == "buffer":
            self.buffered = message
            return len(message)
        if mode == "fail":
            raise RuntimeError("stderr failed")
        if mode == "interrupt":
            raise SystemExit("stderr interrupted")
        failure.stderr_touched = True
        return len(message)

stream = HostileStderr()
"#
                    ),
                    Some(&locals),
                    Some(&locals),
                )?;
                let stream = locals
                    .get_item("stream")?
                    .ok_or_else(|| PyRuntimeError::new_err("missing hostile stderr"))?;
                let stderr_guard = StderrGuard::replace(py, &stream)?;

                let error = dry_run(py, Some(Some(true))).unwrap_err();

                assert!(error.value(py).is(failure.bind(py)));
                let calls = stream.getattr("calls")?.cast_into::<PyList>()?;
                assert_eq!(calls.len(), 1);
                assert_eq!(
                    calls.get_item(0)?.extract::<String>()?,
                    "DeltaFunnel action status: completed; the Python result was not delivered.\n"
                );
                if mode == "mutate" {
                    assert!(
                        error
                            .value(py)
                            .getattr("stderr_touched")?
                            .extract::<bool>()?
                    );
                }

                drop(stderr_guard);
                drop(module_guard);
            }
            Ok(())
        })
    }

    #[test]
    fn all_core_phases_have_curated_labels() {
        assert_eq!(
            phase_label(ProgressPhase::PlanningOutput),
            "Planning output"
        );
        assert_eq!(
            phase_label(ProgressPhase::SettingUpStream),
            "Preparing data stream"
        );
        assert_eq!(
            phase_label(ProgressPhase::Connecting),
            "Connecting to SQL Server"
        );
        assert_eq!(
            phase_label(ProgressPhase::PreparingTarget),
            "Preparing target table"
        );
        assert_eq!(phase_label(ProgressPhase::Writing), "Writing to SQL Server");
        assert_eq!(phase_label(ProgressPhase::Validating), "Validating write");
        assert_eq!(
            phase_label(ProgressPhase::SwappingTarget),
            "Swapping target table"
        );
        assert_eq!(phase_label(ProgressPhase::CleaningUp), "Cleaning up");
    }
}
