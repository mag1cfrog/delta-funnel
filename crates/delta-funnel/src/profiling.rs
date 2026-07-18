//! Shared activation and identity state for one profiled operation.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::report::OperationTimelineRecorder;

pub(crate) const PROFILE_TARGET: &str = "delta_funnel::profile";

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

/// One canonical identity and optional semantic sink for a profiled operation.
#[derive(Debug, Clone)]
pub(crate) struct OperationTraceContext {
    operation_id: u64,
    next_query_execution_id: Arc<AtomicU64>,
    timeline: Option<OperationTimelineRecorder>,
    process_spans_enabled: bool,
}

impl OperationTraceContext {
    pub(crate) fn start(timeline: Option<OperationTimelineRecorder>) -> Option<Self> {
        Self::start_for_modes(timeline, process_spans_enabled())
    }

    fn start_for_modes(
        timeline: Option<OperationTimelineRecorder>,
        process_spans_enabled: bool,
    ) -> Option<Self> {
        if timeline.is_none() && !process_spans_enabled {
            return None;
        }
        Some(Self {
            operation_id: allocate_id(&NEXT_OPERATION_ID)?,
            next_query_execution_id: Arc::new(AtomicU64::new(1)),
            timeline,
            process_spans_enabled,
        })
    }

    #[cfg(test)]
    pub(crate) fn start_for_test(
        timeline: Option<OperationTimelineRecorder>,
        process_spans_enabled: bool,
    ) -> Option<Self> {
        Self::start_for_modes(timeline, process_spans_enabled)
    }

    pub(crate) const fn operation_id(&self) -> u64 {
        self.operation_id
    }

    pub(crate) const fn timeline(&self) -> Option<&OperationTimelineRecorder> {
        self.timeline.as_ref()
    }

    pub(crate) const fn process_spans_enabled(&self) -> bool {
        self.process_spans_enabled
    }

    pub(crate) fn next_query_execution_id(&self) -> Option<u64> {
        allocate_id(&self.next_query_execution_id)
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
    use crate::report::OperationTimelineRecorder;

    use super::*;

    #[test]
    fn activation_modes_share_one_context_without_requiring_a_timeline() {
        assert!(OperationTraceContext::start_for_modes(None, false).is_none());

        let semantic_timeline = OperationTimelineRecorder::start();
        let semantic =
            OperationTraceContext::start_for_modes(Some(semantic_timeline.clone()), false)
                .expect("semantic tracing should create a context");
        let process = OperationTraceContext::start_for_modes(None, true)
            .expect("process tracing should create a context");
        let combined_timeline = OperationTimelineRecorder::start();
        let combined =
            OperationTraceContext::start_for_modes(Some(combined_timeline.clone()), true)
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
        let first = OperationTraceContext::start_for_modes(None, true)
            .expect("process tracing should create a context");
        let second = OperationTraceContext::start_for_modes(None, true)
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
            OperationTraceContext::start(None)
        });
        let enabled = with_default(Registry::default(), || {
            tracing::callsite::rebuild_interest_cache();
            OperationTraceContext::start(None)
        });

        assert!(disabled.is_none());
        assert!(enabled.is_some());
    }
}
