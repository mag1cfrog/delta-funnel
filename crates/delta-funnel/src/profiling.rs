//! Shared activation and identity state for one profiled operation.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use crate::report::OperationTimelineRecorder;

pub(crate) const PROFILE_TARGET: &str = "delta_funnel::profile";
const MAX_OPERATOR_ACTIVITY_SPANS: u64 = 100_000;

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy)]
pub(crate) enum OperationTraceKind {
    Preview,
    MssqlWrite,
    WriteAll,
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

/// One canonical identity and optional semantic sink for a profiled operation.
#[derive(Debug, Clone)]
pub(crate) struct OperationTraceContext {
    operation_id: u64,
    next_query_execution_id: Arc<AtomicU64>,
    timeline: Option<OperationTimelineRecorder>,
    process_trace: Option<Arc<ProcessOperationTrace>>,
    operator_activity_budget: Arc<OperatorActivityBudget>,
}

impl OperationTraceContext {
    pub(crate) fn start(
        kind: OperationTraceKind,
        timeline: Option<OperationTimelineRecorder>,
    ) -> Option<Self> {
        Self::start_for_modes(kind, timeline, process_spans_enabled())
    }

    fn start_for_modes(
        kind: OperationTraceKind,
        timeline: Option<OperationTimelineRecorder>,
        process_spans_enabled: bool,
    ) -> Option<Self> {
        if timeline.is_none() && !process_spans_enabled {
            return None;
        }
        let operation_id = allocate_id(&NEXT_OPERATION_ID)?;
        Some(Self {
            operation_id,
            next_query_execution_id: Arc::new(AtomicU64::new(1)),
            timeline,
            process_trace: process_spans_enabled
                .then(|| Arc::new(ProcessOperationTrace::new(kind, operation_id))),
            operator_activity_budget: Arc::new(OperatorActivityBudget::new(
                MAX_OPERATOR_ACTIVITY_SPANS,
            )),
        })
    }

    #[cfg(test)]
    pub(crate) fn start_for_test(
        timeline: Option<OperationTimelineRecorder>,
        process_spans_enabled: bool,
    ) -> Option<Self> {
        Self::start_for_test_with_operator_activity_limit(
            timeline,
            process_spans_enabled,
            MAX_OPERATOR_ACTIVITY_SPANS,
        )
    }

    #[cfg(test)]
    pub(crate) fn start_for_test_with_operator_activity_limit(
        timeline: Option<OperationTimelineRecorder>,
        process_spans_enabled: bool,
        maximum_spans: u64,
    ) -> Option<Self> {
        let mut context =
            Self::start_for_modes(OperationTraceKind::Preview, timeline, process_spans_enabled)?;
        context.operator_activity_budget = Arc::new(OperatorActivityBudget::new(maximum_spans));
        Some(context)
    }

    pub(crate) const fn operation_id(&self) -> u64 {
        self.operation_id
    }

    pub(crate) const fn timeline(&self) -> Option<&OperationTimelineRecorder> {
        self.timeline.as_ref()
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

    pub(crate) fn start_process_phase(
        &self,
        phase: OperationTracePhase,
    ) -> Option<ProcessOperationPhaseTrace> {
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
        Some(ProcessOperationPhaseTrace {
            span,
            _context: self.clone(),
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

#[derive(Debug)]
pub(crate) struct ProcessOperationPhaseTrace {
    span: tracing::Span,
    // Retain the operation root until this child span closes on cancellation.
    _context: OperationTraceContext,
    result_recorded: bool,
}

impl ProcessOperationPhaseTrace {
    pub(crate) fn finish(mut self, result: &'static str) {
        self.span.record("result", result);
        self.result_recorded = true;
    }
}

impl Drop for ProcessOperationPhaseTrace {
    fn drop(&mut self) {
        if !self.result_recorded {
            self.span.record("result", "cancelled");
        }
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
    fn new(kind: OperationTraceKind, operation_id: u64) -> Self {
        let span = match kind {
            OperationTraceKind::Preview => tracing::trace_span!(
                target: PROFILE_TARGET,
                parent: None,
                "Delta Funnel preview",
                operation_id,
                result = tracing::field::Empty,
                time_semantics = "wall_clock",
            ),
            OperationTraceKind::MssqlWrite => tracing::trace_span!(
                target: PROFILE_TARGET,
                parent: None,
                "Delta Funnel SQL Server write",
                operation_id,
                result = tracing::field::Empty,
                time_semantics = "wall_clock",
            ),
            OperationTraceKind::WriteAll => tracing::trace_span!(
                target: PROFILE_TARGET,
                parent: None,
                "Delta Funnel SQL Server write_all",
                operation_id,
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

fn allocate_id(counter: &AtomicU64) -> Option<u64> {
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
    use crate::{observability::test_capture::TracingCapture, report::OperationTimelineRecorder};

    use super::*;

    #[test]
    fn activation_modes_share_one_context_without_requiring_a_timeline() {
        assert!(
            OperationTraceContext::start_for_modes(OperationTraceKind::Preview, None, false)
                .is_none()
        );

        let semantic_timeline = OperationTimelineRecorder::start();
        let semantic = OperationTraceContext::start_for_modes(
            OperationTraceKind::Preview,
            Some(semantic_timeline.clone()),
            false,
        )
        .expect("semantic tracing should create a context");
        let process =
            OperationTraceContext::start_for_modes(OperationTraceKind::MssqlWrite, None, true)
                .expect("process tracing should create a context");
        let combined_timeline = OperationTimelineRecorder::start();
        let combined = OperationTraceContext::start_for_modes(
            OperationTraceKind::WriteAll,
            Some(combined_timeline.clone()),
            true,
        )
        .expect("combined tracing should create one context");

        assert!(semantic.timeline().is_some());
        assert!(!semantic.process_spans_enabled());
        assert!(process.timeline().is_none());
        assert!(process.process_spans_enabled());
        assert!(combined.timeline().is_some());
        assert!(combined.process_spans_enabled());
        assert!(semantic.operation_id() < process.operation_id());
        assert!(process.operation_id() < combined.operation_id());

        semantic_timeline
            .start_span("semantic", "test", "test")
            .completed();
        assert_eq!(
            semantic_timeline
                .finish("semantic", crate::TimelineSpanStatus::Completed)
                .spans()
                .len(),
            1
        );
        combined_timeline
            .start_span("combined", "test", "test")
            .completed();
        assert_eq!(
            combined_timeline
                .finish("combined", crate::TimelineSpanStatus::Completed)
                .spans()
                .len(),
            1
        );
    }

    #[test]
    fn query_ids_are_local_to_their_operation_context() {
        let first = OperationTraceContext::start_for_modes(OperationTraceKind::Preview, None, true)
            .expect("process tracing should create a context");
        let second =
            OperationTraceContext::start_for_modes(OperationTraceKind::Preview, None, true)
                .expect("process tracing should create a context");

        assert_eq!(first.next_query_execution_id(), Some(1));
        assert_eq!(first.next_query_execution_id(), Some(2));
        assert_eq!(second.next_query_execution_id(), Some(1));
    }

    #[test]
    fn identity_allocation_stops_instead_of_wrapping() {
        let counter = AtomicU64::new(u64::MAX);

        assert_eq!(allocate_id(&counter), Some(u64::MAX));
        assert_eq!(allocate_id(&counter), None);
    }

    #[test]
    fn process_activation_uses_the_profile_callsite() {
        use tracing::subscriber::{NoSubscriber, with_default};
        use tracing_subscriber::Registry;

        let _guard = crate::observability::test_capture::tracing_test_guard();
        let disabled = with_default(NoSubscriber::default(), || {
            tracing::callsite::rebuild_interest_cache();
            OperationTraceContext::start(OperationTraceKind::Preview, None)
        });
        let enabled = with_default(Registry::default(), || {
            tracing::callsite::rebuild_interest_cache();
            OperationTraceContext::start(OperationTraceKind::Preview, None)
        });

        assert!(disabled.is_none());
        assert!(enabled.is_some());
    }

    #[test]
    fn operation_spans_record_bounded_identity_result_and_cancellation() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let application_span = tracing::info_span!("application operation");
        let application_guard = application_span.enter();
        let completed = OperationTraceContext::start(OperationTraceKind::MssqlWrite, None)
            .expect("process tracing should create a context");
        let completed_id = completed.operation_id();
        completed.record_process_result("ok");
        drop(completed);

        let cancelled = OperationTraceContext::start(OperationTraceKind::Preview, None)
            .expect("process tracing should create a context");
        let cancelled_id = cancelled.operation_id();
        drop(cancelled);
        drop(application_guard);
        drop(application_span);

        let spans = capture
            .captured()
            .spans()
            .into_iter()
            .filter(|span| span.target == PROFILE_TARGET)
            .collect::<Vec<_>>();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].name, "Delta Funnel SQL Server write");
        assert_eq!(spans[0].fields["operation_id"], completed_id.to_string());
        assert_eq!(spans[0].fields["result"], "ok");
        assert_eq!(spans[1].name, "Delta Funnel preview");
        assert_eq!(spans[1].fields["operation_id"], cancelled_id.to_string());
        assert_eq!(spans[1].fields["result"], "cancelled");
        assert!(spans.iter().all(|span| {
            span.target == PROFILE_TARGET
                && span.level == tracing::Level::TRACE
                && span.parent_id.is_none()
                && span.fields["time_semantics"] == "wall_clock"
                && span.enter_count == 0
                && span.exit_count == 0
                && span.closed
        }));
    }

    #[test]
    fn operation_phase_retains_its_root_and_records_cancellation() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let context = OperationTraceContext::start(OperationTraceKind::Preview, None)
            .expect("process tracing should create a context");
        let root_id = context
            .process_root_span()
            .expect("process tracing should create a root span")
            .id()
            .expect("the root span should be enabled")
            .into_u64();
        let phase = context
            .start_process_phase(OperationTracePhase::Execution)
            .expect("process tracing should create a phase span");

        drop(context);
        let open_root = capture
            .captured()
            .spans()
            .into_iter()
            .find(|span| span.id == root_id)
            .expect("the operation root should be captured");
        assert!(!open_root.closed);

        drop(phase);
        let spans = capture.captured().spans();
        let root = spans
            .iter()
            .find(|span| span.id == root_id)
            .expect("the operation root should be captured");
        let phase = spans
            .iter()
            .find(|span| span.name == "Delta Funnel operation phase")
            .expect("the operation phase should be captured");
        assert!(root.closed);
        assert_eq!(root.fields["result"], "cancelled");
        assert!(phase.closed);
        assert_eq!(phase.parent_id, Some(root_id));
        assert_eq!(phase.fields["phase"], "execution");
        assert_eq!(phase.fields["result"], "cancelled");
        assert_eq!(phase.fields["time_semantics"], "wall_clock");
        assert_eq!(phase.enter_count, 0);
        assert_eq!(phase.exit_count, 0);
    }
}
