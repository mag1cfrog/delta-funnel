//! Bridges core progress events to one lazily created Rich progress task.
//!
//! The bridge is created per Python action. It owns no Python objects until the
//! core emits `Started`, then updates the same task through the final lifecycle
//! event. Rich decides whether that task uses terminal or Jupyter rendering.

use std::sync::{Arc, Mutex};

use delta_funnel::progress::{
    ProgressEvent, ProgressEventKind, ProgressOperation, ProgressPhase, ProgressReporter,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Per-action owner of the core reporter connected to Python rendering.
pub(crate) struct PythonProgress {
    reporter: ProgressReporter,
}

impl PythonProgress {
    /// Creates the bridge unless the caller explicitly disabled progress.
    ///
    /// `None` uses Rich's environment detection, while `Some(true)` forces
    /// interactive rendering without selecting a terminal or Jupyter backend.
    pub(crate) fn new(progress: Option<bool>) -> Option<Self> {
        let mode = match progress {
            Some(false) => return None,
            Some(true) => ProgressMode::Forced,
            None => ProgressMode::Automatic,
        };
        let state = Arc::new(Mutex::new(RenderState::Pending(mode)));
        let reporter_state = Arc::clone(&state);
        let reporter = ProgressReporter::new(move |event| render_event(&reporter_state, event));
        Some(Self { reporter })
    }

    /// Returns the cloneable reporter passed into the core action.
    pub(crate) fn reporter(&self) -> ProgressReporter {
        self.reporter.clone()
    }
}

/// Controls whether Rich may suppress rendering in a non-interactive process.
#[derive(Clone, Copy)]
enum ProgressMode {
    /// Render only when Rich detects an interactive terminal or Jupyter.
    Automatic,
    /// Ask Rich to render even when its environment is non-interactive.
    Forced,
}

/// The two Python objects needed to update one Rich task.
struct RichRenderer {
    progress: Py<PyAny>,
    task_id: Py<PyAny>,
}

/// Lifecycle state for one action's renderer.
enum RenderState {
    /// No Python object exists yet; wait for the core `Started` event.
    Pending(ProgressMode),
    /// One Rich task exists and remains eligible for boundary cleanup.
    Active {
        renderer: RichRenderer,
        /// False after a phase update cannot attach to Python or render.
        updates_enabled: bool,
    },
    /// Temporary ownership sentinel while arbitrary Python runs without the mutex.
    Busy,
    /// Rendering is disabled, unavailable, or finalized for this action.
    Done,
}

/// Applies one synchronous core event without holding the state mutex in Python.
fn render_event(state: &Mutex<RenderState>, event: &ProgressEvent) {
    // Move ownership out of the mutex before attaching to Python. Rich methods
    // are arbitrary Python and must never run while this lock is held.
    let current = {
        let Ok(mut state) = state.lock() else {
            return;
        };
        std::mem::replace(&mut *state, RenderState::Busy)
    };

    let next = match current {
        // Renderer creation is intentionally delayed until the core confirms
        // that the action crossed its public Started boundary.
        RenderState::Pending(mode) if event.kind() == ProgressEventKind::Started => {
            Python::try_attach(|py| start_renderer(py, mode, event))
                .flatten()
                .map_or(RenderState::Done, |renderer| RenderState::Active {
                    renderer,
                    updates_enabled: true,
                })
        }
        // Every action-ending event gets one final label and one stop attempt,
        // even when an earlier phase update disabled further updates.
        RenderState::Active { renderer, .. } if ends_action(event.kind()) => {
            let _ = Python::try_attach(|py| finish_renderer(py, &renderer, event.kind()));
            RenderState::Done
        }
        RenderState::Active {
            renderer,
            updates_enabled: true,
        } if event.kind() == ProgressEventKind::PhaseChanged => {
            // Phase-only progress ignores numeric Progress events until #434.
            // Losing Python attachment or a Rich update disables later phase
            // updates, while retaining the renderer for boundary cleanup.
            let updated = event.phase().is_none_or(|phase| {
                Python::try_attach(|py| update_renderer(py, &renderer, phase_label(phase)))
                    .is_some_and(|result| result.is_ok())
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

    // Core callbacks are serial, so the state can be returned after the Python
    // call without another callback legitimately taking ownership in between.
    if let Ok(mut state) = state.lock() {
        *state = next;
    }
}

/// Lazily creates one Rich console, progress display, and indeterminate task.
fn start_renderer(
    py: Python<'_>,
    mode: ProgressMode,
    event: &ProgressEvent,
) -> Option<RichRenderer> {
    // Rich owns terminal and Jupyter capability detection. Delta Funnel only
    // chooses stderr for terminal output and whether rendering is forced.
    let console_type = py.import("rich.console").ok()?.getattr("Console").ok()?;
    let console_kwargs = PyDict::new(py);
    console_kwargs.set_item("stderr", true).ok()?;
    if matches!(mode, ProgressMode::Forced) {
        console_kwargs.set_item("force_interactive", true).ok()?;
    }
    let console = console_type.call((), Some(&console_kwargs)).ok()?;

    // Automatic mode stays quiet for scripts, pipes, and CI. A Jupyter console
    // is considered renderable even though it is not an interactive terminal.
    if matches!(mode, ProgressMode::Automatic)
        && !console
            .getattr("is_interactive")
            .and_then(|value| value.extract::<bool>())
            .unwrap_or(false)
        && !console
            .getattr("is_jupyter")
            .and_then(|value| value.extract::<bool>())
            .unwrap_or(false)
    {
        return None;
    }

    // Keep presentation identical across Rich backends: elapsed time, stable
    // description, the bar, then its determinate or indeterminate status.
    let progress_module = py.import("rich.progress").ok()?;
    let progress_type = progress_module.getattr("Progress").ok()?;
    let elapsed_column = progress_module
        .getattr("TimeElapsedColumn")
        .ok()?
        .call0()
        .ok()?;
    let bar_column = progress_module.getattr("BarColumn").ok()?.call0().ok()?;
    let task_progress_column = progress_module
        .getattr("TaskProgressColumn")
        .ok()?
        .call0()
        .ok()?;

    // Core events drive every refresh. Disabling Rich's background refresher
    // avoids a worker thread and makes notebook updates deterministic.
    let progress_kwargs = PyDict::new(py);
    progress_kwargs.set_item("console", console).ok()?;
    progress_kwargs.set_item("auto_refresh", false).ok()?;
    progress_kwargs.set_item("transient", false).ok()?;
    progress_kwargs.set_item("redirect_stdout", false).ok()?;
    progress_kwargs.set_item("redirect_stderr", false).ok()?;
    let progress = progress_type
        .call(
            (
                elapsed_column,
                "{task.description}",
                bar_column,
                task_progress_column,
            ),
            Some(&progress_kwargs),
        )
        .ok()?;

    // Start indeterminate. #434 will set total and completed on this same task,
    // preserving the renderer and elapsed time when file totals become known.
    let task_kwargs = PyDict::new(py);
    task_kwargs.set_item("total", py.None()).ok()?;
    let task_id = progress
        .call_method(
            "add_task",
            (operation_label(event.operation()),),
            Some(&task_kwargs),
        )
        .ok()?;
    progress.call_method0("start").ok()?;
    Some(RichRenderer {
        progress: progress.unbind(),
        task_id: task_id.unbind(),
    })
}

/// Changes the task description and explicitly refreshes notebook output.
fn update_renderer(py: Python<'_>, renderer: &RichRenderer, description: &str) -> PyResult<()> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("description", description)?;
    kwargs.set_item("refresh", true)?;
    renderer
        .progress
        .call_method(py, "update", (renderer.task_id.bind(py),), Some(&kwargs))?;
    Ok(())
}

/// Shows the final action state and makes the single boundary stop attempt.
fn finish_renderer(py: Python<'_>, renderer: &RichRenderer, kind: ProgressEventKind) {
    let _ = update_renderer(py, renderer, terminal_label(kind));
    let _ = renderer.progress.call_method0(py, "stop");
}

/// Returns whether an event permanently ends the progress action.
///
/// This is a lifecycle boundary and is unrelated to terminal versus Jupyter
/// rendering.
const fn ends_action(kind: ProgressEventKind) -> bool {
    matches!(
        kind,
        ProgressEventKind::Completed
            | ProgressEventKind::CompletedWithFailures
            | ProgressEventKind::Failed
            | ProgressEventKind::Cancelled
    )
}

/// Returns the description shown before the first phase is available.
const fn operation_label(operation: Option<ProgressOperation>) -> &'static str {
    match operation {
        Some(ProgressOperation::WriteToMssql) => "Writing to SQL Server",
        Some(ProgressOperation::DryRunToMssql) => "Planning SQL Server write",
        _ => "Running SQL Server action",
    }
}

/// Maps internal phases to stable, sanitized user-facing descriptions.
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

/// Maps an action-ending event to its final user-facing description.
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
    use std::sync::{MutexGuard, PoisonError};

    use pyo3::ffi::c_str;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods, PyModule};

    use super::*;
    use crate::deltafunnel;

    const MODULE_NAMES: [&str; 3] = ["rich", "rich.console", "rich.progress"];
    static PYTHON_STATE: Mutex<()> = Mutex::new(());

    fn python_state() -> MutexGuard<'static, ()> {
        PYTHON_STATE.lock().unwrap_or_else(PoisonError::into_inner)
    }

    struct ModuleGuard {
        originals: Vec<(&'static str, Option<Py<PyAny>>)>,
    }

    impl ModuleGuard {
        fn install(
            py: Python<'_>,
            interactive: bool,
            jupyter: bool,
        ) -> PyResult<(Self, Py<PyList>)> {
            let modules = py
                .import("sys")?
                .getattr("modules")?
                .cast_into::<PyDict>()?;
            let originals = MODULE_NAMES
                .iter()
                .map(|name| {
                    modules
                        .get_item(name)
                        .map(|value| (*name, value.map(Bound::unbind)))
                })
                .collect::<PyResult<Vec<_>>>()?;
            let records = PyList::empty(py);
            let locals = PyDict::new(py);
            locals.set_item("records", &records)?;
            locals.set_item("interactive", interactive)?;
            locals.set_item("jupyter", jupyter)?;
            py.run(
                c_str!(
                    r#"
import sys
import types

class Console:
    def __init__(self, **kwargs):
        records.append({"call": "console", **kwargs})
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

    def add_task(self, description, **kwargs):
        records.append({"call": "add_task", "description": description, "total": kwargs.get("total")})
        return 7

    def start(self):
        records.append({"call": "start"})

    def update(self, task_id, **kwargs):
        records.append({"call": "update", "task_id": task_id, **kwargs})

    def stop(self):
        records.append({"call": "stop"})

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
            Ok((Self { originals }, records.unbind()))
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

    fn record_strings(records: &Bound<'_, PyList>, key: &str) -> PyResult<Vec<String>> {
        records
            .iter()
            .map(|record| record.get_item(key)?.extract::<String>())
            .collect()
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
