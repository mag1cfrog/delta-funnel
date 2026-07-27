//! Shared activation and identity state for one profiled operation.

use std::{
    cell::Cell,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use tracing::Instrument;

pub(crate) const PROFILE_TARGET: &str = "delta_funnel::profile";
#[cfg(feature = "perfetto-profile")]
pub(crate) const OBJECT_STORE_TRANSPORT_ACTIVITY: &str = "object_store_transport";
#[cfg(feature = "perfetto-profile")]
pub(crate) const OBJECT_STORE_TRANSPORT_CONTEXT_NAME: &str =
    "DataFusion object store transport context";
#[cfg(feature = "perfetto-profile")]
pub(crate) const OBJECT_STORE_TRANSPORT_DISPLAY_NAME: &str = "Object store transport";
const MAX_OPERATOR_ACTIVITY_SPANS: u64 = 100_000;

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static OPERATION_CAPTURE_SCOPE_ID: Cell<Option<u64>> = const { Cell::new(None) };
}

#[cfg(feature = "perfetto-profile")]
pub(crate) fn in_operation_capture_scope<T>(
    capture_scope_id: u64,
    operation: impl FnOnce() -> T,
) -> T {
    debug_assert_ne!(capture_scope_id, 0);
    let previous =
        OPERATION_CAPTURE_SCOPE_ID.with(|current| current.replace(Some(capture_scope_id)));
    let _reset = OperationCaptureScopeReset(previous);
    operation()
}

fn current_operation_capture_scope_id() -> Option<u64> {
    OPERATION_CAPTURE_SCOPE_ID.get()
}

#[cfg(feature = "perfetto-profile")]
struct OperationCaptureScopeReset(Option<u64>);

#[cfg(feature = "perfetto-profile")]
impl Drop for OperationCaptureScopeReset {
    fn drop(&mut self) {
        OPERATION_CAPTURE_SCOPE_ID.set(self.0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationTraceKind {
    Preview,
    MssqlWrite,
    WriteAll,
}

impl OperationTraceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Preview => "preview",
            Self::MssqlWrite => "mssql_write",
            Self::WriteAll => "write_all",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationTracePhase {
    Planning,
    Execution,
    Finalization,
}

impl OperationTracePhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Execution => "execution",
            Self::Finalization => "finalization",
        }
    }
}

/// One canonical identity for a profiled operation.
#[derive(Debug, Clone)]
pub(crate) struct OperationTraceContext {
    operation_id: u64,
    kind: OperationTraceKind,
    next_query_execution_id: Arc<AtomicU64>,
    process_trace: Option<Arc<ProcessOperationTrace>>,
    operator_activity_budget: Arc<OperatorActivityBudget>,
}

impl OperationTraceContext {
    pub(crate) fn start(kind: OperationTraceKind) -> Option<Self> {
        Self::start_for_mode(kind, process_spans_enabled())
    }

    fn start_for_mode(kind: OperationTraceKind, process_spans_enabled: bool) -> Option<Self> {
        if !process_spans_enabled {
            return None;
        }
        let operation_id = allocate_id(&NEXT_OPERATION_ID)?;
        Some(Self {
            operation_id,
            kind,
            next_query_execution_id: Arc::new(AtomicU64::new(1)),
            process_trace: Some(Arc::new(ProcessOperationTrace::new(
                kind,
                operation_id,
                current_operation_capture_scope_id(),
            ))),
            operator_activity_budget: Arc::new(OperatorActivityBudget::new(
                MAX_OPERATOR_ACTIVITY_SPANS,
            )),
        })
    }

    #[cfg(test)]
    pub(crate) fn start_for_test(process_spans_enabled: bool) -> Option<Self> {
        Self::start_for_test_with_operator_activity_limit(
            process_spans_enabled,
            MAX_OPERATOR_ACTIVITY_SPANS,
        )
    }

    #[cfg(test)]
    pub(crate) fn start_for_test_with_operator_activity_limit(
        process_spans_enabled: bool,
        maximum_spans: u64,
    ) -> Option<Self> {
        let mut context = Self::start_for_mode(OperationTraceKind::Preview, process_spans_enabled)?;
        context.operator_activity_budget = Arc::new(OperatorActivityBudget::new(maximum_spans));
        Some(context)
    }

    pub(crate) const fn operation_id(&self) -> u64 {
        self.operation_id
    }

    pub(crate) const fn process_spans_enabled(&self) -> bool {
        self.process_trace.is_some()
    }

    pub(crate) fn process_root_span(&self) -> Option<&tracing::Span> {
        self.process_trace.as_deref().map(|trace| &trace.span)
    }

    pub(crate) fn record_process_result(&self, result: &'static str) {
        if let Some(trace) = &self.process_trace {
            trace.record_result(result);
        }
    }

    fn start_process_phase(&self, phase: OperationTracePhase) -> Option<ProcessSpanTrace> {
        let root = self.process_root_span()?;
        let span = tracing::trace_span!(
            target: PROFILE_TARGET,
            parent: root,
            "Delta Funnel operation phase",
            operation_id = self.operation_id,
            phase = phase.as_str(),
            result = tracing::field::Empty,
            time_semantics = "wall_clock",
        );
        Some(ProcessSpanTrace {
            span,
            _parent: root.clone(),
            result_recorded: false,
        })
    }

    fn start_process_stage(
        &self,
        name: &'static str,
        category: &'static str,
        owner_id: Option<u64>,
    ) -> Option<ProcessSpanTrace> {
        let root = self.process_root_span()?;
        let span = tracing::trace_span!(
            target: PROFILE_TARGET,
            parent: root,
            "Delta Funnel operation stage",
            operation_id = self.operation_id,
            operation_kind = self.kind.as_str(),
            stage_name = name,
            stage_category = category,
            stage_owner_id = owner_id,
            result = tracing::field::Empty,
            time_semantics = "wall_clock",
        );
        Some(ProcessSpanTrace {
            span,
            _parent: root.clone(),
            result_recorded: false,
        })
    }

    pub(crate) fn next_query_execution_id(&self) -> Option<u64> {
        allocate_id(&self.next_query_execution_id)
    }

    pub(crate) fn reserve_operator_activity(&self) -> Result<(), OperatorActivityLimit> {
        self.operator_activity_budget.reserve()
    }
}

#[derive(Debug, Default)]
pub(crate) struct ProcessOperationPhaseTracker {
    // Drop the active child before releasing the operation root.
    active: Option<ProcessSpanTrace>,
    context: Option<OperationTraceContext>,
}

impl ProcessOperationPhaseTracker {
    pub(crate) fn start(
        context: Option<&OperationTraceContext>,
        phase: OperationTracePhase,
    ) -> Self {
        let context = context.cloned();
        let active = context
            .as_ref()
            .and_then(|context| context.start_process_phase(phase));
        Self { active, context }
    }

    pub(crate) fn transition(&mut self, phase: OperationTracePhase) {
        self.transition_with_result("ok", phase);
    }

    pub(crate) fn transition_with_result(
        &mut self,
        result: &'static str,
        phase: OperationTracePhase,
    ) {
        self.finish(result);
        self.active = self
            .context
            .as_ref()
            .and_then(|context| context.start_process_phase(phase));
    }

    pub(crate) fn finish(&mut self, result: &'static str) {
        if let Some(active) = self.active.take() {
            active.finish(result);
        }
    }
}

#[derive(Debug)]
struct ProcessSpanTrace {
    span: tracing::Span,
    // Keep the parent open until this child closes.
    _parent: tracing::Span,
    result_recorded: bool,
}

impl ProcessSpanTrace {
    pub(crate) fn finish(mut self, result: &'static str) {
        self.span.record("result", result);
        self.result_recorded = true;
    }
}

impl Drop for ProcessSpanTrace {
    fn drop(&mut self) {
        if !self.result_recorded {
            self.span.record("result", "cancelled");
        }
    }
}

/// Read-only identity shared by one operation's bounded stages.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct OperationStageContext<'a> {
    operation: Option<&'a OperationTraceContext>,
    owner_id: Option<u64>,
}

impl<'a> OperationStageContext<'a> {
    pub(crate) const fn new(
        operation: Option<&'a OperationTraceContext>,
        owner_id: Option<u64>,
    ) -> Self {
        Self {
            operation,
            owner_id,
        }
    }

    pub(crate) fn start(
        self,
        name: &'static str,
        category: &'static str,
    ) -> Option<OperationStageTrace> {
        OperationStageTrace::start(self.operation, name, category, self.owner_id)
    }
}

/// One bounded wall-clock stage in the process trace.
#[derive(Debug)]
pub(crate) struct OperationStageTrace {
    process_span: ProcessSpanTrace,
}

impl OperationStageTrace {
    pub(crate) fn from_process_span(
        process_span: Option<(tracing::Span, tracing::Span)>,
    ) -> Option<Self> {
        process_span.map(|(span, parent)| Self {
            process_span: ProcessSpanTrace {
                span,
                _parent: parent,
                result_recorded: false,
            },
        })
    }

    pub(crate) fn start(
        context: Option<&OperationTraceContext>,
        name: &'static str,
        category: &'static str,
        owner_id: Option<u64>,
    ) -> Option<Self> {
        debug_assert!(owner_id.is_none_or(|owner_id| owner_id != 0));
        context
            .and_then(|context| {
                context.start_process_stage(
                    name,
                    category,
                    owner_id.filter(|owner_id| *owner_id != 0),
                )
            })
            .map(|process_span| Self { process_span })
    }

    pub(crate) async fn instrument_future<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        future.instrument(self.process_span.span.clone()).await
    }

    pub(crate) fn in_process_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        self.process_span.span.in_scope(operation)
    }

    pub(crate) fn completed(self) {
        self.finish("ok");
    }

    pub(crate) fn failed(self) {
        self.finish("error");
    }

    fn finish(self, result: &'static str) {
        self.process_span.finish(result);
    }
}

#[derive(Debug)]
struct OperatorActivityBudget {
    maximum_spans: u64,
    remaining_spans: AtomicU64,
    truncation_reported: AtomicBool,
}

impl OperatorActivityBudget {
    const fn new(maximum_spans: u64) -> Self {
        Self {
            maximum_spans,
            remaining_spans: AtomicU64::new(maximum_spans),
            truncation_reported: AtomicBool::new(false),
        }
    }

    fn reserve(&self) -> Result<(), OperatorActivityLimit> {
        if self
            .remaining_spans
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Ok(());
        }
        Err(OperatorActivityLimit {
            maximum_spans: self.maximum_spans,
            should_report: !self.truncation_reported.swap(true, Ordering::Relaxed),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OperatorActivityLimit {
    pub(crate) maximum_spans: u64,
    pub(crate) should_report: bool,
}

#[derive(Debug)]
struct ProcessOperationTrace {
    span: tracing::Span,
    result_recorded: AtomicBool,
}

impl ProcessOperationTrace {
    fn new(kind: OperationTraceKind, operation_id: u64, capture_scope_id: Option<u64>) -> Self {
        let capture_scope_id = capture_scope_id.unwrap_or_default();
        let span = match kind {
            OperationTraceKind::Preview => tracing::trace_span!(
                target: PROFILE_TARGET,
                parent: None,
                "Delta Funnel preview",
                operation_id,
                capture_scope_id,
                result = tracing::field::Empty,
                time_semantics = "wall_clock",
            ),
            OperationTraceKind::MssqlWrite => tracing::trace_span!(
                target: PROFILE_TARGET,
                parent: None,
                "Delta Funnel SQL Server write",
                operation_id,
                capture_scope_id,
                result = tracing::field::Empty,
                time_semantics = "wall_clock",
            ),
            OperationTraceKind::WriteAll => tracing::trace_span!(
                target: PROFILE_TARGET,
                parent: None,
                "Delta Funnel SQL Server write_all",
                operation_id,
                capture_scope_id,
                result = tracing::field::Empty,
                time_semantics = "wall_clock",
            ),
        };
        Self {
            span,
            result_recorded: AtomicBool::new(false),
        }
    }

    fn record_result(&self, result: &'static str) {
        if !self.result_recorded.swap(true, Ordering::Relaxed) {
            self.span.record("result", result);
        }
    }
}

impl Drop for ProcessOperationTrace {
    fn drop(&mut self) {
        if !self.result_recorded.load(Ordering::Relaxed) {
            self.span.record("result", "cancelled");
        }
    }
}

pub(crate) fn allocate_id(counter: &AtomicU64) -> Option<u64> {
    loop {
        let current = counter.load(Ordering::Relaxed);
        if current == 0 {
            return None;
        }
        let next = current.checked_add(1).unwrap_or(0);
        if counter
            .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return Some(current);
        }
    }
}

fn process_spans_enabled() -> bool {
    tracing::enabled!(target: PROFILE_TARGET, tracing::Level::TRACE)
}

#[cfg(test)]
mod tests {
    use crate::observability::test_capture::TracingCapture;

    use super::*;

    #[test]
    fn identity_allocation_stops_instead_of_wrapping() {
        let counter = AtomicU64::new(u64::MAX);

        assert_eq!(allocate_id(&counter), Some(u64::MAX));
        assert_eq!(allocate_id(&counter), None);
    }

    #[test]
    fn operation_and_stage_spans_record_identity_and_terminal_results() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let operation = OperationTraceContext::start_for_test(true)
            .expect("profile tracing should create an operation");
        let operation_id = operation.operation_id();
        let completed = OperationStageContext::new(Some(&operation), Some(7))
            .start("Completed stage", "delta_funnel.test")
            .expect("the completed stage should start");
        completed.completed();
        let cancelled = OperationStageContext::new(Some(&operation), None)
            .start("Cancelled stage", "delta_funnel.test")
            .expect("the cancelled stage should start");
        drop(cancelled);
        operation.record_process_result("ok");
        drop(operation);

        let spans = capture.captured().spans();
        let root = spans
            .iter()
            .find(|span| span.name == "Delta Funnel preview")
            .expect("the operation root should be captured");
        assert_eq!(root.fields["operation_id"], operation_id.to_string());
        assert_eq!(root.fields["result"], "ok");

        let stages = spans
            .iter()
            .filter(|span| span.name == "Delta Funnel operation stage")
            .collect::<Vec<_>>();
        assert_eq!(stages.len(), 2);
        assert!(stages.iter().all(|span| span.parent_id == Some(root.id)));
        assert_eq!(stages[0].fields["stage_name"], "Completed stage");
        assert_eq!(stages[0].fields["stage_owner_id"], "7");
        assert_eq!(stages[0].fields["result"], "ok");
        assert_eq!(stages[1].fields["stage_name"], "Cancelled stage");
        assert_eq!(stages[1].fields["result"], "cancelled");
    }

    #[test]
    fn disabled_process_tracing_creates_no_context_or_stage() {
        assert!(OperationTraceContext::start_for_test(false).is_none());
        assert!(
            OperationStageContext::default()
                .start("Disabled stage", "delta_funnel.test")
                .is_none()
        );
    }
}
