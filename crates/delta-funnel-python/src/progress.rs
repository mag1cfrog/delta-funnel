//! Shows core progress events with one lazily created Rich progress task.
//!
//! Each Python action creates its own adapter. The adapter imports nothing from
//! Rich until the Rust action starts, then updates the same task until the
//! action finishes. Rich chooses terminal or Jupyter rendering.

#[cfg(test)]
use std::cell::Cell;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use delta_funnel::progress::{
    ProgressEvent, ProgressEventKind, ProgressOperation, ProgressPhase, ProgressReporter,
};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json::Value;

use crate::json::json_value_to_py;

#[cfg(test)]
thread_local! {
    static ADAPTER_CREATION_COUNT: Cell<usize> = const { Cell::new(0) };
    static ATTACHMENT_FAILURE_COUNTDOWN: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn adapter_creation_count() -> usize {
    ADAPTER_CREATION_COUNT.get()
}

const METRIC_RENDER_INTERVAL: Duration = Duration::from_millis(250);

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
        Self::new_with_output(progress, ProgressOutput::Stderr)
    }

    /// Creates preview progress that uses stdout only when Rich detects Jupyter.
    pub(crate) fn for_preview(progress: Option<bool>) -> Option<Self> {
        Self::new_with_output(progress, ProgressOutput::StdoutInNotebook)
    }

    fn new_with_output(progress: Option<bool>, output: ProgressOutput) -> Option<Self> {
        let mode = match progress {
            Some(false) => return None,
            Some(true) => ProgressMode::Forced,
            None => ProgressMode::Automatic,
        };
        #[cfg(test)]
        ADAPTER_CREATION_COUNT.set(ADAPTER_CREATION_COUNT.get().saturating_add(1));
        let state = Arc::new(Mutex::new(ProgressState::new(RenderState::Pending(
            ProgressSettings { mode, output },
        ))));
        let reporter_state = Arc::clone(&state);
        let reporter = ProgressReporter::new(move |event| {
            render_event(&reporter_state, event, Instant::now());
        });
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
    /// failed, attaches its sanitized Python error for callers to inspect. A
    /// completed-with-failures action may instead attach its sanitized report.
    pub(crate) fn finish(
        &self,
        py: Python<'_>,
        operation_error: Option<&PyErr>,
        operation_report: Option<&Value>,
    ) -> PyResult<()> {
        // Set the shared state to Done before calling Rich. If Rich calls back
        // into this adapter while stopping, it cannot stop the display twice.
        let mut state = {
            let mut shared = match self.state.lock() {
                Ok(shared) => shared,
                Err(poisoned) => poisoned.into_inner(),
            };
            std::mem::replace(&mut *shared, ProgressState::new(RenderState::Done))
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
                } else if state.final_event == Some(ProgressEventKind::CompletedWithFailures)
                    && let Some(operation_report) = operation_report
                    && let Ok(operation_report) = json_value_to_py(py, operation_report)
                {
                    let _ = error
                        .value(py)
                        .setattr("deltafunnel_operation_report", operation_report.bind(py));
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

/// Selects the stream without duplicating Rich's environment detection.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ProgressOutput {
    /// Always use stderr, including inside notebooks.
    Stderr,
    /// Use stdout in notebooks and stderr everywhere else.
    StdoutInNotebook,
}

#[derive(Clone, Copy)]
struct ProgressSettings {
    mode: ProgressMode,
    output: ProgressOutput,
}

/// Python handles needed to update one Rich task.
struct RichRenderer {
    progress: Py<PyAny>,
    task_id: Py<PyAny>,
    indeterminate_columns: Py<PyAny>,
    terminal_columns: Py<PyAny>,
    determinate_columns: Py<PyAny>,
}

/// Current display state and the first Python interruption waiting to be raised.
struct ProgressState {
    render: RenderState,
    pending_interruption: Option<PyErr>,
    final_event: Option<ProgressEventKind>,
    visible: VisibleProgress,
    metric_throttle: MetricRenderThrottle,
}

impl ProgressState {
    const fn new(render: RenderState) -> Self {
        Self {
            render,
            pending_interruption: None,
            final_event: None,
            visible: VisibleProgress::new(),
            metric_throttle: MetricRenderThrottle::new(),
        }
    }
}

/// Limits how often numeric progress enters Python while retaining new values.
struct MetricRenderThrottle {
    /// When the latest metric values were last included in a Rich update.
    last_metrics_shown_at: Option<Instant>,
    /// True when `VisibleProgress` contains newer metrics than Rich has shown.
    metrics_waiting_to_render: bool,
}

impl MetricRenderThrottle {
    const fn new() -> Self {
        Self {
            last_metrics_shown_at: None,
            metrics_waiting_to_render: false,
        }
    }

    /// Returns true when this event should update Rich now.
    fn should_render(
        &mut self,
        kind: ProgressEventKind,
        visible_metrics_changed: bool,
        now: Instant,
    ) -> bool {
        if visible_metrics_changed {
            self.metrics_waiting_to_render = true;
        }
        if kind != ProgressEventKind::Progress {
            // Phase and terminal updates include any pending metrics immediately.
            if self.metrics_waiting_to_render {
                self.last_metrics_shown_at = Some(now);
                self.metrics_waiting_to_render = false;
            }
            return true;
        }
        if !self.metrics_waiting_to_render {
            return false;
        }
        if self
            .last_metrics_shown_at
            .is_some_and(|last| now.saturating_duration_since(last) < METRIC_RENDER_INTERVAL)
        {
            return false;
        }
        self.last_metrics_shown_at = Some(now);
        self.metrics_waiting_to_render = false;
        true
    }

    /// Returns true when Rust has newer counters that have not been sent to Rich.
    const fn has_pending_update(&self) -> bool {
        self.metrics_waiting_to_render
    }
}

/// Latest progress values retained even when a display update is throttled.
#[derive(Clone)]
struct VisibleProgress {
    phase: Option<ProgressPhase>,
    output_name: Option<String>,
    output_index: Option<u64>,
    output_count: Option<u64>,
    files_handled: Option<u64>,
    files_total: Option<u64>,
    files_runtime_pruned: Option<u64>,
    files_planning_pruned: Option<u64>,
    rows: Option<u64>,
    batches: Option<u64>,
}

impl VisibleProgress {
    const fn new() -> Self {
        Self {
            phase: None,
            output_name: None,
            output_index: None,
            output_count: None,
            files_handled: None,
            files_total: None,
            files_runtime_pruned: None,
            files_planning_pruned: None,
            rows: None,
            batches: None,
        }
    }

    /// Applies an event to the values shown by Rich.
    ///
    /// Starting another output, cache operation, restoration, or source report
    /// clears the previous file and write counters first. Returns true when the
    /// event changes a numeric counter.
    fn incorporate(&mut self, event: &ProgressEvent) -> bool {
        if let Some(phase) = event.phase() {
            let output_name = event.output_name().map(str::to_owned);
            let output_position = event.output_index().zip(event.output_count());
            if self.scope_changes(event) {
                self.clear_metrics();
            }
            self.phase = Some(phase);
            self.output_name = output_name;
            (self.output_index, self.output_count) = output_position.unzip();
        }
        let file_progress = match (
            event.files_handled(),
            event.files_total(),
            event.files_runtime_pruned(),
        ) {
            (Some(handled), Some(total), Some(runtime_pruned)) => Some((
                handled,
                total,
                runtime_pruned,
                event.files_planning_pruned(),
            )),
            _ => None,
        };
        self.incorporate_metrics(file_progress, event.rows().zip(event.batches()))
    }

    /// Returns true when this event must stop using the current counters.
    ///
    /// One output keeps its counters while moving from writing to validation.
    /// Starting another output clears them. Cache work has no output name, so
    /// starting cache materialization, cache restoration, or source reporting
    /// also clears them. Every cache materialization starts with empty counters,
    /// even when the preceding event used the same phase name.
    fn scope_changes(&self, event: &ProgressEvent) -> bool {
        let Some(phase) = event.phase() else {
            return false;
        };
        self.output_name.as_deref() != event.output_name()
            || self.output_index.zip(self.output_count)
                != event.output_index().zip(event.output_count())
            || (event.kind() == ProgressEventKind::PhaseChanged
                && event.output_name().is_none()
                && starts_new_unnamed_metric_scope(self.phase, phase))
    }

    /// Clears file and write counters so the next work cannot show old values.
    fn clear_metrics(&mut self) {
        self.files_handled = None;
        self.files_total = None;
        self.files_runtime_pruned = None;
        self.files_planning_pruned = None;
        self.rows = None;
        self.batches = None;
    }

    /// Merges monotonic counters and reports whether a visible value changed.
    fn incorporate_metrics(
        &mut self,
        file_progress: Option<(u64, u64, u64, Option<u64>)>,
        write_progress: Option<(u64, u64)>,
    ) -> bool {
        let before = self.metric_values();
        if let Some((handled, total, runtime_pruned, planning_pruned)) = file_progress {
            // The first eligible snapshot fixes the total for this plan scope.
            let fixed_total = self.files_total.unwrap_or(total);
            self.files_total = Some(fixed_total);
            let handled = self
                .files_handled
                .unwrap_or(0)
                .max(handled.min(fixed_total));
            self.files_handled = Some(handled);
            self.files_runtime_pruned = Some(
                self.files_handled
                    .unwrap_or(0)
                    .min(self.files_runtime_pruned.unwrap_or(0).max(runtime_pruned)),
            );
            if let Some(planning_pruned) = planning_pruned {
                self.files_planning_pruned =
                    Some(self.files_planning_pruned.unwrap_or(0).max(planning_pruned));
            }
        }
        if let Some((rows, batches)) = write_progress {
            self.rows = Some(self.rows.unwrap_or(0).max(rows));
            self.batches = Some(self.batches.unwrap_or(0).max(batches));
        }
        self.metric_values() != before
    }

    const fn metric_values(&self) -> [Option<u64>; 6] {
        [
            self.files_handled,
            self.files_total,
            self.files_runtime_pruned,
            self.files_planning_pruned,
            self.rows,
            self.batches,
        ]
    }

    fn file_progress_text(&self) -> Option<String> {
        let (Some(handled), Some(total), Some(runtime_pruned)) = (
            self.files_handled,
            self.files_total,
            self.files_runtime_pruned,
        ) else {
            return None;
        };
        let prefix = format!("Delta files {handled}/{total}");
        match (
            runtime_pruned,
            self.files_planning_pruned.filter(|pruned| *pruned > 0),
        ) {
            (0, None) => Some(prefix),
            (runtime, None) => Some(format!("{prefix} | pruned {runtime} at runtime")),
            (0, Some(planning)) => Some(format!("{prefix} | pruned ~{planning} in planning")),
            (runtime, Some(planning)) => Some(format!(
                "{prefix} | pruned {runtime} at runtime, ~{planning} in planning"
            )),
        }
    }

    fn description(&self, kind: ProgressEventKind) -> String {
        let label = if ends_action(kind) {
            terminal_label(kind)
        } else {
            self.phase.map_or("Working", phase_label)
        };
        let mut description = match self.output_index.zip(self.output_count) {
            Some((index, count)) => format!("Output {index}/{count} - {label}"),
            None => label.to_owned(),
        };
        if let Some(output_name) = &self.output_name {
            description.push_str(": ");
            description.push_str(output_name);
        }
        if let (Some(rows), Some(batches)) = (self.rows, self.batches) {
            let row_label = count_label(rows, "row", "rows");
            let rows = compact_row_count(rows);
            description.push_str(&format!(
                " - {rows} {}, {batches} {}",
                row_label,
                count_label(batches, "batch", "batches")
            ));
        }
        description
    }
}

/// Returns true when an unnamed phase starts unrelated work with fresh metrics.
fn starts_new_unnamed_metric_scope(current: Option<ProgressPhase>, next: ProgressPhase) -> bool {
    next == ProgressPhase::MaterializingCache
        || (current != Some(next)
            && !(current == Some(ProgressPhase::CollectingPreview)
                && next == ProgressPhase::FormattingPreview))
}

/// Current state of one action's Rich display.
enum RenderState {
    /// The Rust action has not started, so no Rich objects exist yet.
    Pending(ProgressSettings),
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
    #[cfg(test)]
    if attachment_unavailable_for_test() {
        return PythonCall::Failed;
    }

    Python::try_attach(|py| match call(py) {
        Ok(value) => PythonCall::Succeeded(value),
        Err(error) if error.is_instance_of::<PyException>(py) => PythonCall::Failed,
        Err(error) => PythonCall::Interrupted(error),
    })
    .unwrap_or(PythonCall::Failed)
}

#[cfg(test)]
fn attachment_unavailable_for_test() -> bool {
    let remaining = ATTACHMENT_FAILURE_COUNTDOWN.get();
    ATTACHMENT_FAILURE_COUNTDOWN.set(remaining.saturating_sub(1));
    remaining == 1
}

/// Returns Rich's value, ignores ordinary failures, or saves the first
/// interruption for `finish`.
fn python_value_or_save_interruption<T>(
    call: PythonCall<T>,
    pending_interruption: &mut Option<PyErr>,
) -> Option<T> {
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

/// Sends the last saved counters to Rich before they are cleared.
///
/// Nothing happens when there is no active Rich display or no saved update. If
/// Rich raises a normal error, later display updates are disabled. If it raises
/// `KeyboardInterrupt` or another exception that must stop Python, that same
/// exception is saved and raised after the Rust action ends.
fn flush_previous_metrics(
    render: RenderState,
    previous_visible: Option<&VisibleProgress>,
    pending_interruption: &mut Option<PyErr>,
) -> RenderState {
    match (render, previous_visible) {
        (
            RenderState::Active {
                renderer,
                updates_enabled: true,
            },
            Some(previous_visible),
        ) => {
            let description = previous_visible.description(ProgressEventKind::Progress);
            let updates_enabled = python_value_or_save_interruption(
                try_python(|py| {
                    update_renderer(
                        py,
                        &renderer,
                        ProgressEventKind::Progress,
                        &description,
                        previous_visible,
                    )
                }),
                pending_interruption,
            )
            .is_some();
            RenderState::Active {
                renderer,
                updates_enabled,
            }
        }
        (render, _) => render,
    }
}

/// Handles one Rust progress event and updates Rich when needed.
fn render_event(state: &Mutex<ProgressState>, event: &ProgressEvent, now: Instant) {
    // Take the state out and release the mutex before calling Rich. Python code
    // may call other code, so running it while holding the mutex could deadlock.
    let current = {
        let mut state = match state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        let previous_visible = (state.visible.scope_changes(event)
            && state.metric_throttle.has_pending_update())
        .then(|| state.visible.clone());
        let visible_metrics_changed = state.visible.incorporate(event);
        if !state
            .metric_throttle
            .should_render(event.kind(), visible_metrics_changed, now)
            && previous_visible.is_none()
        {
            return;
        }
        (
            std::mem::replace(&mut *state, ProgressState::new(RenderState::Busy)),
            previous_visible,
        )
    };

    let (
        ProgressState {
            render,
            mut pending_interruption,
            final_event,
            visible,
            metric_throttle,
        },
        previous_visible,
    ) = current;
    let final_event = if ends_action(event.kind()) {
        Some(event.kind())
    } else {
        final_event
    };
    // Show the last throttled counters before replacing their output or plan
    // scope with the phase carried by the current event.
    let render =
        flush_previous_metrics(render, previous_visible.as_ref(), &mut pending_interruption);
    let render = match render {
        // Wait for Started before importing Rich. Requests rejected before the
        // action begins should not perform any progress-related Python work.
        RenderState::Pending(settings) if event.kind() == ProgressEventKind::Started => {
            python_value_or_save_interruption(
                try_python(|py| create_renderer(py, settings, event)),
                &mut pending_interruption,
            )
            .flatten()
            .map_or(RenderState::Done, |renderer| {
                // Keep ownership even if start fails so finish can still make the
                // one stop attempt after the Rust action returns.
                let updates_enabled = python_value_or_save_interruption(
                    try_python(|py| start_renderer(py, &renderer)),
                    &mut pending_interruption,
                )
                .is_some();
                RenderState::Active {
                    renderer,
                    updates_enabled,
                }
            })
        }
        // Show the final result now, but keep the task open. `finish` stops it
        // only after the Rust action has returned and completed its cleanup.
        RenderState::Active {
            renderer,
            updates_enabled,
        } if ends_action(event.kind()) => {
            let updates_enabled = if updates_enabled {
                let description = visible.description(event.kind());
                python_value_or_save_interruption(
                    try_python(|py| {
                        update_renderer(py, &renderer, event.kind(), &description, &visible)
                    }),
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
        } if matches!(
            event.kind(),
            ProgressEventKind::PhaseChanged | ProgressEventKind::Progress
        ) =>
        {
            let description = visible.description(event.kind());
            let updated = python_value_or_save_interruption(
                try_python(|py| {
                    update_renderer(py, &renderer, event.kind(), &description, &visible)
                }),
                &mut pending_interruption,
            )
            .is_some();
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
        visible,
        metric_throttle,
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
    settings: ProgressSettings,
    event: &ProgressEvent,
) -> PyResult<Option<RichRenderer>> {
    // Rich detects the terminal or Jupyter environment. Preview starts with a
    // stdout console so the same object can render in notebook output. Other
    // actions start directly on stderr.
    let console_type = py.import("rich.console")?.getattr("Console")?;
    let console_kwargs = PyDict::new(py);
    if settings.output == ProgressOutput::Stderr {
        console_kwargs.set_item("stderr", true)?;
    }
    if matches!(settings.mode, ProgressMode::Forced) {
        console_kwargs.set_item("force_interactive", true)?;
    }
    let mut console = console_type.call((), Some(&console_kwargs))?;

    // Rich reports Jupyter separately from interactive terminals. A notebook
    // keeps the stdout console; other previews select their stderr console
    // before automatic mode checks whether the rendering stream is interactive.
    let is_jupyter = console.getattr("is_jupyter")?.extract::<bool>()?;
    if settings.output == ProgressOutput::StdoutInNotebook && !is_jupyter {
        // Rich ruled out Jupyter, so terminal and forced script previews move
        // to stderr before any progress task is constructed.
        console_kwargs.set_item("stderr", true)?;
        console = console_type.call((), Some(&console_kwargs))?;
    }
    let is_interactive = console.getattr("is_interactive")?.extract::<bool>()?;
    if matches!(settings.mode, ProgressMode::Automatic) && !is_interactive && !is_jupyter {
        return Ok(None);
    }

    // Keep unknown work visually distinct from measurable file progress. Rich
    // renders the same task with a spinner until a truthful total is available,
    // then swaps to a compact determinate bar without restarting the display.
    let progress_module = py.import("rich.progress")?;
    let progress_type = progress_module.getattr("Progress")?;
    let text_column_type = progress_module.getattr("TextColumn")?;

    let spinner_kwargs = PyDict::new(py);
    spinner_kwargs.set_item("spinner_name", "arc")?;
    spinner_kwargs.set_item("style", "bright_blue")?;
    let spinner_column = progress_module
        .getattr("SpinnerColumn")?
        .call((), Some(&spinner_kwargs))?;

    let description_kwargs = PyDict::new(py);
    description_kwargs.set_item("markup", false)?;
    let description_column =
        text_column_type.call(("{task.description}",), Some(&description_kwargs))?;

    let elapsed_kwargs = PyDict::new(py);
    elapsed_kwargs.set_item("style", "dim")?;
    elapsed_kwargs.set_item("markup", false)?;
    let elapsed_column =
        text_column_type.call(("{task.elapsed:>5.1f}s",), Some(&elapsed_kwargs))?;

    let bar_kwargs = PyDict::new(py);
    bar_kwargs.set_item("bar_width", 24)?;
    bar_kwargs.set_item("style", "grey35")?;
    bar_kwargs.set_item("complete_style", "bright_blue")?;
    bar_kwargs.set_item("finished_style", "bright_green")?;
    let bar_column = progress_module
        .getattr("BarColumn")?
        .call((), Some(&bar_kwargs))?;

    let task_progress_kwargs = PyDict::new(py);
    task_progress_kwargs.set_item("text_format", "{task.percentage:>3.0f}%")?;
    task_progress_kwargs.set_item("style", "bright_blue")?;
    task_progress_kwargs.set_item("markup", false)?;
    let task_progress_column = progress_module
        .getattr("TaskProgressColumn")?
        .call((), Some(&task_progress_kwargs))?;

    let file_count_kwargs = PyDict::new(py);
    file_count_kwargs.set_item("style", "dim")?;
    file_count_kwargs.set_item("markup", false)?;
    let file_count_column = progress_module
        .getattr("TextColumn")?
        .call(("{task.fields[file_progress]}",), Some(&file_count_kwargs))?;

    let indeterminate_columns = (
        spinner_column,
        description_column.clone(),
        elapsed_column.clone(),
    )
        .into_pyobject(py)?;
    let terminal_columns =
        (description_column.clone(), elapsed_column.clone()).into_pyobject(py)?;
    let determinate_columns = (
        description_column,
        bar_column,
        task_progress_column,
        file_count_column,
        elapsed_column,
    )
        .into_pyobject(py)?;

    let progress_kwargs = PyDict::new(py);
    progress_kwargs.set_item("console", console)?;
    progress_kwargs.set_item("auto_refresh", true)?;
    progress_kwargs.set_item("transient", false)?;
    progress_kwargs.set_item("redirect_stdout", false)?;
    progress_kwargs.set_item("redirect_stderr", false)?;
    let progress = progress_type.call(&indeterminate_columns, Some(&progress_kwargs))?;

    // Start without a total. The first eligible file snapshot updates this
    // same task so its display and elapsed time continue without restarting.
    let task_kwargs = PyDict::new(py);
    task_kwargs.set_item("total", py.None())?;
    task_kwargs.set_item("file_progress", "")?;
    let task_id = progress.call_method(
        "add_task",
        (operation_label(event.operation()),),
        Some(&task_kwargs),
    )?;
    Ok(Some(RichRenderer {
        progress: progress.unbind(),
        task_id: task_id.unbind(),
        indeterminate_columns: indeterminate_columns.into_any().unbind(),
        terminal_columns: terminal_columns.into_any().unbind(),
        determinate_columns: determinate_columns.into_any().unbind(),
    }))
}

/// Starts a fully constructed Rich task.
fn start_renderer(py: Python<'_>, renderer: &RichRenderer) -> PyResult<()> {
    renderer.progress.call_method0(py, "start")?;
    Ok(())
}

/// Shows the latest description and determinate file position immediately.
fn update_renderer(
    py: Python<'_>,
    renderer: &RichRenderer,
    kind: ProgressEventKind,
    description: &str,
    visible: &VisibleProgress,
) -> PyResult<()> {
    let columns = if visible.files_total.is_some() {
        &renderer.determinate_columns
    } else if ends_action(kind) {
        &renderer.terminal_columns
    } else {
        &renderer.indeterminate_columns
    };
    renderer
        .progress
        .bind(py)
        .setattr("columns", columns.bind(py))?;

    let kwargs = PyDict::new(py);
    kwargs.set_item("description", description)?;
    match (visible.files_handled, visible.files_total) {
        (Some(handled), Some(total)) => {
            kwargs.set_item("completed", handled)?;
            kwargs.set_item("total", total)?;
        }
        _ => {
            kwargs.set_item("completed", 0)?;
            kwargs.set_item("total", py.None())?;
        }
    }
    kwargs.set_item(
        "file_progress",
        visible.file_progress_text().unwrap_or_default(),
    )?;
    kwargs.set_item("refresh", true)?;
    renderer
        .progress
        .call_method(py, "update", (renderer.task_id.bind(py),), Some(&kwargs))?;
    Ok(())
}

const fn count_label(count: u64, singular: &'static str, plural: &'static str) -> &'static str {
    if count == 1 { singular } else { plural }
}

/// Shortens large row counts while keeping two decimal places.
fn compact_row_count(rows: u64) -> String {
    if rows < 1_000 {
        return rows.to_string();
    }

    let rounded_hundredths = |divisor: u128| (u128::from(rows) * 100 + divisor / 2) / divisor;
    let thousands = rounded_hundredths(1_000);
    let (value, suffix) = if thousands < 100_000 {
        (thousands, "K")
    } else {
        (rounded_hundredths(1_000_000), "M")
    };
    format!("{}.{:02}{suffix}", value / 100, value % 100)
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
        Some(ProgressOperation::RegisterDeltaSource) => "Loading Delta source",
        Some(ProgressOperation::PreviewTable) => "Previewing table",
        Some(ProgressOperation::WriteToMssql) => "Writing to SQL Server",
        Some(ProgressOperation::DryRunToMssql) => "Planning SQL Server write",
        Some(ProgressOperation::WriteAllToMssql) => "Writing outputs to SQL Server",
        Some(ProgressOperation::DryRunAllToMssql) => "Planning SQL Server outputs",
        _ => "Running SQL Server action",
    }
}

/// Returns safe, stable text for an internal Rust phase.
const fn phase_label(phase: ProgressPhase) -> &'static str {
    match phase {
        ProgressPhase::LoadingDeltaMetadata => "Loading Delta metadata",
        ProgressPhase::ValidatingDeltaProtocol => "Validating Delta protocol",
        ProgressPhase::PreparingDeltaProvider => "Preparing Delta provider",
        ProgressPhase::RegisteringDeltaSource => "Registering Delta source",
        ProgressPhase::PreparingPreview => "Preparing preview",
        ProgressPhase::CollectingPreview => "Collecting preview",
        ProgressPhase::FormattingPreview => "Formatting preview",
        ProgressPhase::PlanningOutput => "Planning output",
        ProgressPhase::SettingUpStream => "Preparing data stream",
        ProgressPhase::MaterializingCache => "Caching shared data",
        ProgressPhase::RestoringCache => "Restoring shared cache",
        ProgressPhase::Connecting => "Connecting to SQL Server",
        ProgressPhase::PreparingTarget => "Preparing target table",
        ProgressPhase::Writing => "Writing to SQL Server",
        ProgressPhase::Validating => "Validating write",
        ProgressPhase::SwappingTarget => "Swapping target table",
        ProgressPhase::CleaningUp => "Cleaning up",
        ProgressPhase::ReportingSources => "Preparing source reports",
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
pub(crate) mod tests {
    use std::{
        ffi::CString,
        panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
        thread,
    };

    use pyo3::exceptions::{PyGeneratorExit, PyKeyboardInterrupt, PyRuntimeError, PySystemExit};
    use pyo3::ffi::c_str;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods, PyModule};
    use serde_json::json;

    use super::*;
    use crate::{deltafunnel, test_support::python_state};

    const MODULE_NAMES: [&str; 3] = ["rich", "rich.console", "rich.progress"];
    type ModuleSnapshot = Vec<(&'static str, Option<Py<PyAny>>)>;

    pub(crate) struct ModuleGuard {
        originals: Vec<(&'static str, Option<Py<PyAny>>)>,
    }

    pub(crate) struct StderrGuard {
        original: Py<PyAny>,
    }

    struct BuiltinPrintGuard {
        builtins: Py<PyModule>,
        original: Py<PyAny>,
    }

    struct AttachmentFailureGuard;

    impl AttachmentFailureGuard {
        fn fail_on_call(call: usize) -> Self {
            ATTACHMENT_FAILURE_COUNTDOWN.set(call);
            Self
        }
    }

    impl Drop for AttachmentFailureGuard {
        fn drop(&mut self) {
            ATTACHMENT_FAILURE_COUNTDOWN.set(0);
        }
    }

    impl StderrGuard {
        pub(crate) fn capture(py: Python<'_>) -> PyResult<(Self, Py<PyAny>)> {
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

    impl BuiltinPrintGuard {
        fn replace(py: Python<'_>, replacement: &Bound<'_, PyAny>) -> PyResult<Self> {
            let builtins = py.import("builtins")?;
            let original = builtins.getattr("print")?.unbind();
            builtins.setattr("print", replacement)?;
            Ok(Self {
                builtins: builtins.unbind(),
                original,
            })
        }
    }

    impl Drop for BuiltinPrintGuard {
        fn drop(&mut self) {
            let _ = Python::try_attach(|py| {
                self.builtins
                    .bind(py)
                    .setattr("print", self.original.bind(py))
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

        pub(crate) fn install(
            py: Python<'_>,
            interactive: bool,
            jupyter: bool,
        ) -> PyResult<(Self, Py<PyList>)> {
            let (guard, records, _) =
                Self::install_with_failure(py, interactive, jupyter, None, false, false)?;
            Ok((guard, records))
        }

        fn install_with_stream_interactivity(
            py: Python<'_>,
            stdout_interactive: bool,
            stderr_interactive: bool,
        ) -> PyResult<(Self, Py<PyList>)> {
            let (guard, records) = Self::install(py, false, false)?;
            let globals = py
                .import("rich.console")?
                .getattr("Console")?
                .getattr("__init__")?
                .getattr("__globals__")?
                .cast_into::<PyDict>()?;
            globals.set_item("stdout_interactive", stdout_interactive)?;
            globals.set_item("stderr_interactive", stderr_interactive)?;
            Ok((guard, records))
        }

        pub(crate) fn install_with_failure(
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
            locals.set_item("stdout_interactive", interactive)?;
            locals.set_item("stderr_interactive", interactive)?;
            locals.set_item("jupyter", jupyter)?;
            locals.set_item("fail_call", fail_call)?;
            locals.set_item("failure", &failure)?;
            locals.set_item("stop_also_interrupts", stop_also_interrupts)?;
            locals.set_item(
                "stop_failure",
                py.get_type::<PySystemExit>().call1(("stop interrupted",))?,
            )?;
            let fake_rich = CString::new(include_str!("../tests/fake_rich.py"))
                .map_err(|_| PyRuntimeError::new_err("fake Rich fixture contains a null byte"))?;
            py.run(&fake_rich, Some(&locals), Some(&locals))?;
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

    #[test]
    fn metric_throttle_renders_first_latest_and_bypass_events_without_sleeping() {
        let started_at = Instant::now();
        let mut throttle = MetricRenderThrottle::new();

        assert!(throttle.should_render(ProgressEventKind::Progress, true, started_at,));
        assert!(!throttle.should_render(
            ProgressEventKind::Progress,
            true,
            started_at + Duration::from_millis(249),
        ));
        assert!(throttle.should_render(
            ProgressEventKind::Progress,
            true,
            started_at + Duration::from_millis(250),
        ));
        assert!(throttle.should_render(
            ProgressEventKind::PhaseChanged,
            false,
            started_at + Duration::from_millis(251),
        ));
        assert!(throttle.should_render(
            ProgressEventKind::Failed,
            false,
            started_at + Duration::from_millis(252),
        ));
    }

    #[test]
    fn rapid_metric_updates_are_bounded_by_the_injected_clock() {
        let started_at = Instant::now();
        let mut throttle = MetricRenderThrottle::new();
        let rendered = (0..1_000_u64)
            .filter(|millis| {
                throttle.should_render(
                    ProgressEventKind::Progress,
                    true,
                    started_at + Duration::from_millis(*millis),
                )
            })
            .count();

        assert_eq!(rendered, 4);
    }

    #[test]
    fn visible_progress_merges_file_and_write_snapshots_monotonically() {
        let mut visible = VisibleProgress::new();
        visible.phase = Some(ProgressPhase::Writing);
        visible.output_name = Some("orders".to_owned());

        assert!(visible.incorporate_metrics(Some((2, 10, 0, Some(90))), None));
        assert!(visible.incorporate_metrics(None, Some((40, 1))));
        assert!(visible.incorporate_metrics(Some((6, 10, 3, Some(90))), Some((75, 2))));
        assert!(!visible.incorporate_metrics(Some((4, 20, 1, None)), Some((60, 1))));

        assert_eq!(
            visible.metric_values(),
            [Some(6), Some(10), Some(3), Some(90), Some(75), Some(2)]
        );
        assert_eq!(
            visible.file_progress_text().as_deref(),
            Some("Delta files 6/10 | pruned 3 at runtime, ~90 in planning")
        );
        assert_eq!(
            visible.description(ProgressEventKind::Progress),
            "Writing to SQL Server: orders - 75 rows, 2 batches"
        );
        assert_eq!(
            visible.description(ProgressEventKind::Failed),
            "Failed: orders - 75 rows, 2 batches"
        );
    }

    #[test]
    fn preview_formatting_keeps_the_collection_metric_scope() {
        assert!(!starts_new_unnamed_metric_scope(
            Some(ProgressPhase::CollectingPreview),
            ProgressPhase::FormattingPreview,
        ));
        assert!(starts_new_unnamed_metric_scope(
            Some(ProgressPhase::FormattingPreview),
            ProgressPhase::ReportingSources,
        ));
        assert!(starts_new_unnamed_metric_scope(
            Some(ProgressPhase::MaterializingCache),
            ProgressPhase::MaterializingCache,
        ));
    }

    #[test]
    fn large_row_counts_use_two_decimal_k_and_m_suffixes() {
        assert_eq!(compact_row_count(999), "999");
        assert_eq!(compact_row_count(1_000), "1.00K");
        assert_eq!(compact_row_count(12_345), "12.35K");
        assert_eq!(compact_row_count(999_994), "999.99K");
        assert_eq!(compact_row_count(999_995), "1.00M");
        assert_eq!(compact_row_count(1_234_567), "1.23M");

        let mut visible = VisibleProgress::new();
        visible.phase = Some(ProgressPhase::Writing);
        visible.incorporate_metrics(None, Some((1_234_567, 152)));
        assert_eq!(
            visible.description(ProgressEventKind::Progress),
            "Writing to SQL Server - 1.23M rows, 152 batches"
        );
    }

    #[test]
    fn file_progress_text_omits_unavailable_or_zero_pruning_counts() {
        let mut visible = VisibleProgress::new();
        visible.incorporate_metrics(Some((8, 10, 0, None)), None);
        assert_eq!(
            visible.file_progress_text().as_deref(),
            Some("Delta files 8/10")
        );

        visible.incorporate_metrics(Some((8, 10, 3, None)), None);
        assert_eq!(
            visible.file_progress_text().as_deref(),
            Some("Delta files 8/10 | pruned 3 at runtime")
        );

        visible.incorporate_metrics(Some((8, 10, 3, Some(90))), None);
        assert_eq!(
            visible.file_progress_text().as_deref(),
            Some("Delta files 8/10 | pruned 3 at runtime, ~90 in planning")
        );

        let mut planning_only = VisibleProgress::new();
        planning_only.incorporate_metrics(Some((8, 10, 0, Some(90))), None);
        assert_eq!(
            planning_only.file_progress_text().as_deref(),
            Some("Delta files 8/10 | pruned ~90 in planning")
        );
    }

    #[test]
    fn renderer_switches_the_existing_task_to_determinate_progress() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;
            let progress = py.import("rich.progress")?.getattr("Progress")?.call0()?;
            let task_id = progress.call_method1("add_task", ("Starting",))?;
            let renderer = renderer_with_test_columns(py, progress, task_id)?;
            let mut visible = VisibleProgress::new();
            visible.phase = Some(ProgressPhase::Writing);
            visible.output_name = Some("orders".to_owned());
            visible.incorporate_metrics(Some((3, 10, 2, Some(90))), Some((42, 2)));

            update_renderer(
                py,
                &renderer,
                ProgressEventKind::Progress,
                &visible.description(ProgressEventKind::Progress),
                &visible,
            )?;

            let update = records.bind(py).get_item(2)?.cast_into::<PyDict>()?;
            assert_eq!(update.get_item("task_id")?.unwrap().extract::<u8>()?, 7);
            assert_eq!(update.get_item("completed")?.unwrap().extract::<u64>()?, 3);
            assert_eq!(update.get_item("total")?.unwrap().extract::<u64>()?, 10);
            assert_eq!(
                update
                    .get_item("column_types")?
                    .unwrap()
                    .extract::<Vec<String>>()?,
                ["determinate"]
            );
            assert_eq!(
                update
                    .get_item("file_progress")?
                    .unwrap()
                    .extract::<String>()?,
                "Delta files 3/10 | pruned 2 at runtime, ~90 in planning"
            );
            assert_eq!(
                update
                    .get_item("description")?
                    .unwrap()
                    .extract::<String>()?,
                "Writing to SQL Server: orders - 42 rows, 2 batches"
            );
            assert!(update.get_item("refresh")?.unwrap().extract::<bool>()?);

            update_renderer(
                py,
                &renderer,
                ProgressEventKind::Failed,
                &visible.description(ProgressEventKind::Failed),
                &visible,
            )?;
            let terminal = records.bind(py).get_item(3)?.cast_into::<PyDict>()?;
            assert_eq!(
                terminal
                    .get_item("description")?
                    .unwrap()
                    .extract::<String>()?,
                "Failed: orders - 42 rows, 2 batches"
            );
            assert_eq!(
                terminal.get_item("completed")?.unwrap().extract::<u64>()?,
                3
            );
            assert_eq!(terminal.get_item("total")?.unwrap().extract::<u64>()?, 10);
            Ok(())
        })
    }

    #[test]
    fn pending_metrics_are_flushed_before_their_scope_is_replaced() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;
            let progress = py.import("rich.progress")?.getattr("Progress")?.call0()?;
            let task_id = progress.call_method1("add_task", ("Starting",))?;
            let render = RenderState::Active {
                renderer: renderer_with_test_columns(py, progress, task_id)?,
                updates_enabled: true,
            };
            let mut visible = VisibleProgress::new();
            visible.phase = Some(ProgressPhase::Writing);
            visible.output_name = Some("orders".to_owned());
            visible.output_index = Some(1);
            visible.output_count = Some(2);
            visible.incorporate_metrics(Some((8, 10, 3, Some(90))), Some((1_250, 4)));
            let mut interruption = None;

            let render = flush_previous_metrics(render, Some(&visible), &mut interruption);

            assert!(matches!(
                render,
                RenderState::Active {
                    updates_enabled: true,
                    ..
                }
            ));
            assert!(interruption.is_none());
            let update = records.bind(py).get_item(2)?.cast_into::<PyDict>()?;
            assert_eq!(
                update
                    .get_item("description")?
                    .unwrap()
                    .extract::<String>()?,
                "Output 1/2 - Writing to SQL Server: orders - 1.25K rows, 4 batches"
            );
            assert_eq!(update.get_item("completed")?.unwrap().extract::<u64>()?, 8);
            assert_eq!(update.get_item("total")?.unwrap().extract::<u64>()?, 10);
            Ok(())
        })
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

    fn preview(py: Python<'_>, progress: Option<Option<bool>>) -> PyResult<Py<PyAny>> {
        preview_sql(py, "select 1 as id", progress)
    }

    fn preview_sql(
        py: Python<'_>,
        sql: &str,
        progress: Option<Option<bool>>,
    ) -> PyResult<Py<PyAny>> {
        let module = PyModule::new(py, "deltafunnel")?;
        deltafunnel(&module)?;
        let session = module.getattr("Session")?.call0()?;
        let table = session.call_method1("table_from_sql", (sql,))?;
        let kwargs = PyDict::new(py);
        if let Some(progress) = progress {
            kwargs.set_item("progress", progress)?;
        }
        table
            .call_method("preview", (), Some(&kwargs))
            .map(Bound::unbind)
    }

    fn show(py: Python<'_>, progress: Option<Option<bool>>) -> PyResult<()> {
        let module = PyModule::new(py, "deltafunnel")?;
        deltafunnel(&module)?;
        let session = module.getattr("Session")?.call0()?;
        let table = session.call_method1("table_from_sql", ("select 1 as id",))?;
        let kwargs = PyDict::new(py);
        if let Some(progress) = progress {
            kwargs.set_item("progress", progress)?;
        }
        table.call_method("show", (), Some(&kwargs))?;
        Ok(())
    }

    fn dry_run_all(
        py: Python<'_>,
        output_names: &[&str],
        progress: Option<Option<bool>>,
    ) -> PyResult<()> {
        let module = PyModule::new(py, "deltafunnel")?;
        deltafunnel(&module)?;
        let session_kwargs = PyDict::new(py);
        session_kwargs.set_item(
            "default_mssql_connection_string",
            "server=tcp:sql.example.com;password=secret-token",
        )?;
        let session = module.getattr("Session")?.call((), Some(&session_kwargs))?;
        let outputs = output_names
            .iter()
            .map(|output_name| {
                let table = session.call_method1("table_from_sql", ("select 1 as id",))?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("schema", "dbo")?;
                kwargs.set_item("table", output_name)?;
                kwargs.set_item("load_mode", "create_and_load")?;
                table.call_method("to_mssql", (), Some(&kwargs))
            })
            .collect::<PyResult<Vec<_>>>()?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("dry_run", true)?;
        if let Some(progress) = progress {
            kwargs.set_item("progress", progress)?;
        }
        session.call_method("write_all", (PyList::new(py, outputs)?,), Some(&kwargs))?;
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

    fn execute_all_without_connection(py: Python<'_>) -> PyResult<()> {
        let module = PyModule::new(py, "deltafunnel")?;
        deltafunnel(&module)?;
        let session = module.getattr("Session")?.call0()?;
        let table = session.call_method1("table_from_sql", ("select 1 as id",))?;
        let output_kwargs = PyDict::new(py);
        output_kwargs.set_item("schema", "dbo")?;
        output_kwargs.set_item("table", "orders")?;
        output_kwargs.set_item("load_mode", "append_existing")?;
        let output = table.call_method("to_mssql", (), Some(&output_kwargs))?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("progress", true)?;
        session.call_method("write_all", (PyList::new(py, [output])?,), Some(&kwargs))?;
        Ok(())
    }

    pub(crate) fn record_strings(records: &Bound<'_, PyList>, key: &str) -> PyResult<Vec<String>> {
        records
            .iter()
            .map(|record| record.get_item(key)?.extract::<String>())
            .collect()
    }

    fn renderer_with_test_columns(
        py: Python<'_>,
        progress: Bound<'_, PyAny>,
        task_id: Bound<'_, PyAny>,
    ) -> PyResult<RichRenderer> {
        Ok(RichRenderer {
            progress: progress.unbind(),
            task_id: task_id.unbind(),
            indeterminate_columns: ("indeterminate",).into_pyobject(py)?.into_any().unbind(),
            terminal_columns: ("terminal",).into_pyobject(py)?.into_any().unbind(),
            determinate_columns: ("determinate",).into_pyobject(py)?.into_any().unbind(),
        })
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
    fn fake_rich_failures_do_not_escape_to_other_threads() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, _failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("console"), true, true)?;

            let worker = thread::spawn(|| {
                Python::attach(|py| {
                    py.import("rich.console")?.getattr("Console")?.call0()?;
                    Ok::<_, PyErr>(())
                })
            });
            let result = py
                .detach(|| worker.join())
                .map_err(|_| PyRuntimeError::new_err("fake Rich thread panicked"))?;

            result?;
            assert!(records.bind(py).is_empty());
            Ok(())
        })
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
    fn preview_uses_one_shared_rich_lifecycle() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;

            preview(py, Some(Some(true)))?;

            assert_eq!(
                record_strings(records.bind(py), "call")?,
                [
                    "console", "console", "progress", "add_task", "start", "update", "update",
                    "update", "update", "stop"
                ]
            );
            assert!(
                records
                    .bind(py)
                    .get_item(2)?
                    .get_item("console_stderr")?
                    .extract::<bool>()?
            );
            let descriptions = records
                .bind(py)
                .iter()
                .filter_map(|record| {
                    (record.get_item("call").ok()?.extract::<String>().ok()? == "update")
                        .then(|| {
                            record
                                .get_item("description")
                                .ok()?
                                .extract::<String>()
                                .ok()
                        })
                        .flatten()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                descriptions,
                [
                    "Preparing preview",
                    "Collecting preview",
                    "Formatting preview",
                    "Completed",
                ]
            );
            Ok(())
        })
    }

    #[test]
    fn preview_uses_stdout_when_rich_detects_jupyter() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, false, true)?;

            preview(py, None)?;

            assert_eq!(
                record_strings(records.bind(py), "call")?,
                [
                    "console", "progress", "add_task", "start", "update", "update", "update",
                    "update", "stop"
                ]
            );
            assert!(
                !records
                    .bind(py)
                    .get_item(1)?
                    .get_item("console_stderr")?
                    .extract::<bool>()?
            );
            Ok(())
        })
    }

    #[test]
    fn automatic_preview_uses_the_final_output_stream_interactivity() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            for (stdout_interactive, stderr_interactive, should_render) in
                [(false, true, true), (true, false, false)]
            {
                let (guard, records) = ModuleGuard::install_with_stream_interactivity(
                    py,
                    stdout_interactive,
                    stderr_interactive,
                )?;

                preview(py, None)?;

                let calls = record_strings(records.bind(py), "call")?;
                assert_eq!(
                    calls.iter().any(|call| call == "progress"),
                    should_render,
                    "stdout interactive: {stdout_interactive}, stderr interactive: {stderr_interactive}"
                );
                drop(guard);
            }
            Ok(())
        })
    }

    #[test]
    fn preview_stderr_console_failures_follow_the_shared_failure_policy() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            for interruption in [false, true] {
                let (guard, records, failure) = ModuleGuard::install_with_failure(
                    py,
                    true,
                    false,
                    Some("stderr_console"),
                    interruption,
                    false,
                )?;
                let (stderr, _capture) = StderrGuard::capture(py)?;

                let result = preview(py, Some(Some(true)));

                if interruption {
                    let error = result.unwrap_err();
                    assert!(error.value(py).is(failure.bind(py)));
                    assert_eq!(
                        error
                            .value(py)
                            .getattr("deltafunnel_operation_status")?
                            .extract::<String>()?,
                        "completed"
                    );
                } else {
                    result?;
                }
                assert_eq!(
                    record_strings(records.bind(py), "call")?,
                    ["console", "console"]
                );

                drop(stderr);
                drop(guard);
            }
            Ok(())
        })
    }

    #[test]
    fn preview_defers_renderer_interruption_until_query_completion() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("update"), true, false)?;

            let error = preview(py, Some(Some(true))).unwrap_err();

            assert!(error.value(py).is(failure.bind(py)));
            assert_eq!(
                error
                    .value(py)
                    .getattr("deltafunnel_operation_status")?
                    .extract::<String>()?,
                "completed"
            );
            assert_eq!(
                record_strings(records.bind(py), "call")?,
                [
                    "console", "console", "progress", "add_task", "start", "update", "stop"
                ]
            );
            Ok(())
        })
    }

    #[test]
    fn ordinary_preview_renderer_failure_preserves_the_preview() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, _failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("update"), false, false)?;

            let preview = preview(py, Some(Some(true)))?;

            assert_eq!(
                preview
                    .bind(py)
                    .get_type()
                    .getattr("__name__")?
                    .extract::<String>()?,
                "Preview"
            );
            assert_eq!(
                record_strings(records.bind(py), "call")?,
                [
                    "console", "console", "progress", "add_task", "start", "update", "stop"
                ]
            );
            Ok(())
        })
    }

    #[test]
    fn ordinary_renderer_failure_does_not_replace_a_failed_preview() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records, renderer_failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("update"), false, false)?;

            let error = preview_sql(
                py,
                "select cast(1 as bigint) / cast(0 as bigint) as value",
                Some(Some(true)),
            )
            .unwrap_err();

            assert!(error.is_instance_of::<crate::exception::DeltaFunnelError>(py));
            assert!(!error.value(py).is(renderer_failure.bind(py)));
            assert_eq!(
                record_strings(records.bind(py), "call")?.last(),
                Some(&"stop".to_owned())
            );
            Ok(())
        })
    }

    #[test]
    fn show_does_not_print_after_a_deferred_renderer_interruption() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, _records, failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("update"), true, false)?;
            let sys = py.import("sys")?;
            let original_stdout = sys.getattr("stdout")?.unbind();
            let stdout = py.import("io")?.call_method0("StringIO")?.unbind();
            sys.setattr("stdout", stdout.bind(py))?;

            let result = show(py, Some(Some(true)));
            sys.setattr("stdout", original_stdout.bind(py))?;
            let error = result.unwrap_err();
            assert!(error.value(py).is(failure.bind(py)));
            assert_eq!(
                error
                    .value(py)
                    .getattr("deltafunnel_operation_status")?
                    .extract::<String>()?,
                "completed"
            );
            assert!(
                stdout
                    .bind(py)
                    .call_method0("getvalue")?
                    .extract::<String>()?
                    .is_empty()
            );
            Ok(())
        })
    }

    #[test]
    fn show_print_control_flow_stays_outside_the_progress_adapter() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;
            let failure = py
                .get_type::<PySystemExit>()
                .call1(("print interrupted",))?
                .unbind();
            let locals = PyDict::new(py);
            locals.set_item("failure", failure.bind(py))?;
            py.run(
                c_str!(
                    r#"
def fail_print(*args, **kwargs):
    raise failure
"#
                ),
                Some(&locals),
                Some(&locals),
            )?;
            let replacement = locals
                .get_item("fail_print")?
                .ok_or_else(|| PyRuntimeError::new_err("missing failing print function"))?;
            let print_guard = BuiltinPrintGuard::replace(py, &replacement)?;

            let result = show(py, Some(Some(true)));
            drop(print_guard);

            let error = result.unwrap_err();
            assert!(error.value(py).is(failure.bind(py)));
            assert!(
                error
                    .value(py)
                    .getattr("deltafunnel_operation_status")
                    .is_err()
            );
            assert_eq!(
                record_strings(records.bind(py), "call")?.last(),
                Some(&"stop".to_owned())
            );
            Ok(())
        })
    }

    #[test]
    fn failed_preview_is_attached_to_a_deferred_renderer_interruption() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, _records, failure) =
                ModuleGuard::install_with_failure(py, true, false, Some("update"), true, false)?;

            let error = preview_sql(
                py,
                "select cast(1 as bigint) / cast(0 as bigint) as value",
                Some(Some(true)),
            )
            .unwrap_err();

            assert!(error.value(py).is(failure.bind(py)));
            assert_eq!(
                error
                    .value(py)
                    .getattr("deltafunnel_operation_status")?
                    .extract::<String>()?,
                "failed"
            );
            assert!(
                error
                    .value(py)
                    .getattr("deltafunnel_operation_error")?
                    .is_instance_of::<crate::exception::DeltaFunnelError>()
            );
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
                "Planning output: orders"
            );
            assert_eq!(
                records
                    .bind(py)
                    .get_item(5)?
                    .get_item("description")?
                    .extract::<String>()?,
                "Failed: orders"
            );
            Ok(())
        })
    }

    #[test]
    fn write_all_execute_failure_uses_one_failed_rich_lifecycle() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;

            let error = execute_all_without_connection(py).unwrap_err();

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
                "Output 1/1 - Planning output: orders"
            );
            assert_eq!(
                records
                    .bind(py)
                    .get_item(5)?
                    .get_item("description")?
                    .extract::<String>()?,
                "Output 1/1 - Failed: orders"
            );
            Ok(())
        })
    }

    #[test]
    fn write_all_uses_one_task_and_clears_output_scope_before_completion() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;

            dry_run_all(py, &["west", "east"], Some(Some(true)))?;

            assert_eq!(
                record_strings(records.bind(py), "call")?,
                [
                    "console", "progress", "add_task", "start", "update", "update", "update",
                    "update", "stop"
                ]
            );
            let descriptions = records
                .bind(py)
                .iter()
                .filter_map(|record| {
                    (record.get_item("call").ok()?.extract::<String>().ok()? == "update")
                        .then(|| {
                            record
                                .get_item("description")
                                .ok()?
                                .extract::<String>()
                                .ok()
                        })
                        .flatten()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                descriptions,
                [
                    "Output 1/2 - Planning output: west",
                    "Output 2/2 - Planning output: east",
                    "Preparing source reports",
                    "Completed",
                ]
            );
            Ok(())
        })
    }

    #[test]
    fn duplicate_write_all_outputs_fail_before_rich_starts() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;

            let error = dry_run_all(py, &["orders", "orders"], Some(Some(true))).unwrap_err();

            assert_eq!(
                error.value(py).getattr("kind")?.extract::<String>()?,
                "mssql_workflow_planning"
            );
            assert!(records.bind(py).is_empty());
            Ok(())
        })
    }

    #[test]
    fn write_all_uses_the_shared_automatic_forced_and_disabled_modes() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, false, false)?;

            dry_run_all(py, &["orders"], None)?;
            dry_run_all(py, &["orders"], Some(None))?;
            dry_run_all(py, &["orders"], Some(Some(false)))?;

            assert_eq!(
                record_strings(records.bind(py), "call")?,
                ["console", "console"]
            );

            dry_run_all(py, &["orders"], Some(Some(true)))?;
            assert_eq!(
                record_strings(records.bind(py), "call")?,
                [
                    "console", "console", "console", "progress", "add_task", "start", "update",
                    "update", "update", "stop"
                ]
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
            assert_eq!(progress.get_item("columns")?.unwrap().extract::<u8>()?, 3);
            assert_eq!(
                progress
                    .get_item("column_types")?
                    .unwrap()
                    .extract::<Vec<String>>()?,
                ["SpinnerColumn", "TextColumn", "TextColumn"]
            );
            assert!(
                progress
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
                "Planning output: orders"
            );
            assert!(
                records
                    .get_item(4)?
                    .get_item("refresh")?
                    .extract::<bool>()?
            );
            assert_eq!(
                records
                    .get_item(4)?
                    .get_item("column_types")?
                    .extract::<Vec<String>>()?,
                ["SpinnerColumn", "TextColumn", "TextColumn"]
            );
            assert_eq!(
                records
                    .get_item(5)?
                    .get_item("description")?
                    .extract::<String>()?,
                "Completed: orders"
            );
            assert_eq!(
                records
                    .get_item(5)?
                    .get_item("column_types")?
                    .extract::<Vec<String>>()?,
                ["TextColumn", "TextColumn"]
            );
            assert!(
                records
                    .get_item(5)?
                    .get_item("refresh")?
                    .extract::<bool>()?
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
    fn ordinary_rich_import_failure_does_not_replace_the_report() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let (_guard, records) = ModuleGuard::install(py, true, false)?;
            py.import("sys")?
                .getattr("modules")?
                .cast_into::<PyDict>()?
                .set_item("rich.progress", py.None())?;

            dry_run(py, Some(Some(true)))?;

            assert_eq!(record_strings(records.bind(py), "call")?, ["console"]);
            Ok(())
        })
    }

    #[test]
    fn unavailable_python_attachment_stops_callback_rendering_without_retry() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let cases: [(usize, &[&str]); 3] = [
                (1, &[]),
                (3, &["console", "progress", "add_task", "start", "stop"]),
                (
                    4,
                    &["console", "progress", "add_task", "start", "update", "stop"],
                ),
            ];

            for (fail_on_call, expected_calls) in cases {
                let _attachment = AttachmentFailureGuard::fail_on_call(fail_on_call);
                let (guard, records) = ModuleGuard::install(py, true, false)?;

                dry_run(py, Some(Some(true)))?;

                assert_eq!(record_strings(records.bind(py), "call")?, expected_calls);
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
    fn completed_with_failures_interruption_carries_the_operation_report() -> PyResult<()> {
        let _state = python_state();
        Python::attach(|py| {
            let progress = PythonProgress::new(Some(true))
                .ok_or_else(|| PyRuntimeError::new_err("progress should be enabled"))?;
            let interruption = PyKeyboardInterrupt::new_err("renderer interrupted");
            let interruption_object = interruption.value(py).clone().unbind();
            {
                let mut state = match progress.state.lock() {
                    Ok(state) => state,
                    Err(poisoned) => poisoned.into_inner(),
                };
                state.pending_interruption = Some(interruption);
                state.final_event = Some(ProgressEventKind::CompletedWithFailures);
            }
            let (_stderr, _capture) = StderrGuard::capture(py)?;
            let report = json!({
                "all_succeeded": false,
                "failed_count": 1,
                "skipped_count": 1,
            });

            let error = progress.finish(py, None, Some(&report)).unwrap_err();

            assert!(error.value(py).is(interruption_object.bind(py)));
            assert_eq!(
                error
                    .value(py)
                    .getattr("deltafunnel_operation_status")?
                    .extract::<String>()?,
                "completed_with_failures"
            );
            let attached = error
                .value(py)
                .getattr("deltafunnel_operation_report")?
                .cast_into::<PyDict>()?;
            assert!(
                !attached
                    .get_item("all_succeeded")?
                    .unwrap()
                    .extract::<bool>()?
            );
            assert_eq!(
                attached
                    .get_item("failed_count")?
                    .unwrap()
                    .extract::<u64>()?,
                1
            );
            assert!(
                error
                    .value(py)
                    .getattr("deltafunnel_operation_error")
                    .is_err()
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
            for stage in ["console", "start", "update", "terminal", "stop"] {
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
                        Some(stage),
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
                    let calls = record_strings(records.bind(py), "call")?;
                    if stage == "console" {
                        assert!(!calls.contains(&"stop".to_owned()));
                    } else {
                        assert_eq!(calls.last(), Some(&"stop".to_owned()));
                    }

                    drop(stderr);
                    drop(guard);
                }
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
            phase_label(ProgressPhase::LoadingDeltaMetadata),
            "Loading Delta metadata"
        );
        assert_eq!(
            phase_label(ProgressPhase::ValidatingDeltaProtocol),
            "Validating Delta protocol"
        );
        assert_eq!(
            phase_label(ProgressPhase::PreparingDeltaProvider),
            "Preparing Delta provider"
        );
        assert_eq!(
            phase_label(ProgressPhase::RegisteringDeltaSource),
            "Registering Delta source"
        );
        assert_eq!(
            phase_label(ProgressPhase::PreparingPreview),
            "Preparing preview"
        );
        assert_eq!(
            phase_label(ProgressPhase::CollectingPreview),
            "Collecting preview"
        );
        assert_eq!(
            phase_label(ProgressPhase::FormattingPreview),
            "Formatting preview"
        );
        assert_eq!(
            phase_label(ProgressPhase::PlanningOutput),
            "Planning output"
        );
        assert_eq!(
            phase_label(ProgressPhase::SettingUpStream),
            "Preparing data stream"
        );
        assert_eq!(
            phase_label(ProgressPhase::MaterializingCache),
            "Caching shared data"
        );
        assert_eq!(
            phase_label(ProgressPhase::RestoringCache),
            "Restoring shared cache"
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
        assert_eq!(
            phase_label(ProgressPhase::ReportingSources),
            "Preparing source reports"
        );
    }

    #[test]
    fn all_core_operations_have_curated_labels() {
        assert_eq!(
            operation_label(Some(ProgressOperation::RegisterDeltaSource)),
            "Loading Delta source"
        );
        assert_eq!(
            operation_label(Some(ProgressOperation::PreviewTable)),
            "Previewing table"
        );
        assert_eq!(
            operation_label(Some(ProgressOperation::WriteToMssql)),
            "Writing to SQL Server"
        );
        assert_eq!(
            operation_label(Some(ProgressOperation::DryRunToMssql)),
            "Planning SQL Server write"
        );
        assert_eq!(
            operation_label(Some(ProgressOperation::WriteAllToMssql)),
            "Writing outputs to SQL Server"
        );
        assert_eq!(
            operation_label(Some(ProgressOperation::DryRunAllToMssql)),
            "Planning SQL Server outputs"
        );
    }
}
