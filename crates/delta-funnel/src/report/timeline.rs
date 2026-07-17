//! Shared wall-clock timeline values for profiled operations.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

use super::{QueryExecutionProfile, duration_to_micros_saturating};

/// Status recorded for an operation or one measured timeline span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimelineSpanStatus {
    /// The measured work completed successfully.
    Completed,
    /// The measured work failed after starting.
    Failed,
    /// The measured work stopped because its owner was cancelled or dropped.
    Cancelled,
}

impl TimelineSpanStatus {
    /// Returns the stable JSON spelling for this status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Describes what a positioned timeline span measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimelineSpanTimeSemantics {
    /// The span is measured work positioned on the operation's wall clock.
    WallClock,
    /// The span is an object's lifetime, which may include waiting or idle time.
    Lifecycle,
}

impl TimelineSpanTimeSemantics {
    /// Returns the stable JSON spelling for these timing semantics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WallClock => "wall_clock",
            Self::Lifecycle => "lifecycle",
        }
    }
}

/// One measured interval positioned relative to its operation's start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineSpan {
    id: u64,
    parent_id: Option<u64>,
    name: String,
    track_name: Option<String>,
    category: String,
    start_offset_micros: u64,
    duration_micros: u64,
    status: TimelineSpanStatus,
    time_semantics: TimelineSpanTimeSemantics,
    attributes: BTreeMap<String, Value>,
}

impl TimelineSpan {
    /// Creates a positioned span with no attributes.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: u64,
        parent_id: Option<u64>,
        name: impl Into<String>,
        category: impl Into<String>,
        start_offset: Duration,
        duration: Duration,
        status: TimelineSpanStatus,
        time_semantics: TimelineSpanTimeSemantics,
    ) -> Self {
        Self {
            id,
            parent_id,
            name: name.into(),
            track_name: None,
            category: category.into(),
            start_offset_micros: duration_to_micros_saturating(start_offset),
            duration_micros: duration_to_micros_saturating(duration),
            status,
            time_semantics,
            attributes: BTreeMap::new(),
        }
    }

    /// Adds one redacted attribute shown in exported trace event details.
    #[must_use]
    pub fn with_attribute(mut self, name: impl Into<String>, value: Value) -> Self {
        self.attributes.insert(name.into(), value);
        self
    }

    /// Sets a trace track label that is more specific than the event name.
    #[must_use]
    pub fn with_track_name(mut self, track_name: impl Into<String>) -> Self {
        self.track_name = Some(track_name.into());
        self
    }

    /// Returns the operation-local span identifier.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Returns the parent span identifier, or `None` for a direct child of the operation.
    #[must_use]
    pub const fn parent_id(&self) -> Option<u64> {
        self.parent_id
    }

    /// Returns the stable display name for this span.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the trace track label, falling back to the event name.
    #[must_use]
    pub fn track_name(&self) -> &str {
        self.track_name.as_deref().unwrap_or(&self.name)
    }

    /// Returns the stable trace category for this span.
    #[must_use]
    pub fn category(&self) -> &str {
        &self.category
    }

    /// Returns the span start relative to the operation start, in microseconds.
    #[must_use]
    pub const fn start_offset_micros(&self) -> u64 {
        self.start_offset_micros
    }

    /// Returns the measured span duration in microseconds.
    #[must_use]
    pub const fn duration_micros(&self) -> u64 {
        self.duration_micros
    }

    /// Returns how the span reached its terminal state.
    #[must_use]
    pub const fn status(&self) -> TimelineSpanStatus {
        self.status
    }

    /// Returns what the positioned interval measures.
    #[must_use]
    pub const fn time_semantics(&self) -> TimelineSpanTimeSemantics {
        self.time_semantics
    }

    /// Returns the explicitly redacted span attributes.
    #[must_use]
    pub const fn attributes(&self) -> &BTreeMap<String, Value> {
        &self.attributes
    }
}

/// One operation timeline with every span using the same monotonic origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationTimeline {
    name: String,
    status: TimelineSpanStatus,
    total_duration_micros: u64,
    spans: Vec<TimelineSpan>,
}

impl OperationTimeline {
    /// Current stable JSON schema version for exported timeline data.
    pub const SCHEMA_VERSION: u64 = 1;

    /// Creates an operation timeline from already positioned spans.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        status: TimelineSpanStatus,
        total_duration: Duration,
        spans: Vec<TimelineSpan>,
    ) -> Self {
        Self {
            name: name.into(),
            status,
            total_duration_micros: duration_to_micros_saturating(total_duration),
            spans,
        }
    }

    /// Returns the stable operation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns how the operation reached its terminal state.
    #[must_use]
    pub const fn status(&self) -> TimelineSpanStatus {
        self.status
    }

    /// Returns the operation wall-clock duration in microseconds.
    #[must_use]
    pub const fn total_duration_micros(&self) -> u64 {
        self.total_duration_micros
    }

    /// Returns measured spans in stable display order.
    #[must_use]
    pub fn spans(&self) -> &[TimelineSpan] {
        &self.spans
    }
}

#[derive(Debug)]
struct OperationTimelineRecorderState {
    next_span_id: u64,
    next_query_execution_id: u64,
    finished_duration: Option<Duration>,
    pending_spans: BTreeMap<u64, Arc<Mutex<PendingTimelineSpan>>>,
    spans: Vec<TimelineSpan>,
}

/// Shared monotonic recorder used while a profiled operation crosses async layers.
#[derive(Debug, Clone)]
pub(crate) struct OperationTimelineRecorder {
    started_at: Instant,
    wall_clock_origin_nanos: i128,
    state: Arc<Mutex<OperationTimelineRecorderState>>,
}

impl OperationTimelineRecorder {
    pub(crate) fn start() -> Self {
        let started_at = Instant::now();
        let wall_clock_origin_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                i128::try_from(duration.as_nanos()).unwrap_or(i128::MAX)
            });
        Self {
            started_at,
            wall_clock_origin_nanos,
            state: Arc::new(Mutex::new(OperationTimelineRecorderState {
                next_span_id: 1,
                next_query_execution_id: 1,
                finished_duration: None,
                pending_spans: BTreeMap::new(),
                spans: Vec::new(),
            })),
        }
    }

    pub(crate) fn next_query_execution_id(&self) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let id = state.next_query_execution_id;
        state.next_query_execution_id = state.next_query_execution_id.saturating_add(1);
        id
    }

    pub(crate) fn start_span(
        &self,
        name: impl Into<String>,
        category: impl Into<String>,
        track_name: impl Into<String>,
    ) -> OperationTimelineSpanRecorder {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.finished_duration.is_some() {
            return OperationTimelineSpanRecorder {
                recorder: self.clone(),
                span_id: None,
                span: None,
            };
        }
        let id = state.next_span_id;
        state.next_span_id = state.next_span_id.saturating_add(1);
        let start_offset = Instant::now().saturating_duration_since(self.started_at);
        let span = Arc::new(Mutex::new(PendingTimelineSpan {
            id,
            parent_id: None,
            name: name.into(),
            category: category.into(),
            track_name: track_name.into(),
            start_offset,
            attributes: BTreeMap::new(),
        }));
        state.pending_spans.insert(id, Arc::clone(&span));
        OperationTimelineSpanRecorder {
            recorder: self.clone(),
            span_id: Some(id),
            span: Some(span),
        }
    }

    pub(crate) fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub(crate) const fn wall_clock_origin_nanos(&self) -> i128 {
        self.wall_clock_origin_nanos
    }

    fn append_spans_with_fresh_ids(&self, spans: impl IntoIterator<Item = TimelineSpan>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.finished_duration.is_some() {
            return;
        }
        for mut span in spans {
            span.id = state.next_span_id;
            state.next_span_id = state.next_span_id.saturating_add(1);
            state.spans.push(span);
        }
    }

    pub(crate) fn append_operator_lifecycles(&self, profile: &QueryExecutionProfile) {
        self.append_spans_with_fresh_ids(profile.operator_lifecycle_timeline_spans(
            0,
            self.wall_clock_origin_nanos(),
            duration_to_micros_saturating(self.elapsed()),
        ));
    }

    pub(crate) fn append_operator_lifecycles_with_owner(
        &self,
        profile: &QueryExecutionProfile,
        owner_attribute: &str,
        owner_name: &str,
        owner_track_name: &str,
    ) {
        let spans = profile
            .operator_lifecycle_timeline_spans(
                0,
                self.wall_clock_origin_nanos(),
                duration_to_micros_saturating(self.elapsed()),
            )
            .into_iter()
            .map(|span| {
                let track_name = format!("{owner_track_name} / {}", span.track_name());
                span.with_track_name(track_name)
                    .with_attribute(owner_attribute, Value::String(owner_name.to_owned()))
            });
        self.append_spans_with_fresh_ids(spans);
    }

    pub(crate) fn finish(
        &self,
        name: impl Into<String>,
        status: TimelineSpanStatus,
    ) -> OperationTimeline {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let total_duration = match state.finished_duration {
            Some(duration) => duration,
            None => {
                // The state lock makes this cutoff atomic with span starts and
                // completions. Pending spans belong to the finished snapshot.
                let duration = self.elapsed();
                for pending in std::mem::take(&mut state.pending_spans).into_values() {
                    let pending = pending
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .clone();
                    let (span, _) =
                        finish_pending_span(pending, TimelineSpanStatus::Cancelled, duration);
                    state.spans.push(span);
                }
                state.finished_duration = Some(duration);
                duration
            }
        };
        let mut spans = state.spans.clone();
        spans.sort_by_key(|span| (span.start_offset_micros(), span.id()));
        OperationTimeline::new(name, status, total_duration, spans)
    }

    fn record(
        &self,
        id: u64,
        pending: Arc<Mutex<PendingTimelineSpan>>,
        status: TimelineSpanStatus,
    ) -> Option<Duration> {
        let end_offset = Instant::now().saturating_duration_since(self.started_at);
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        // Removing the registration decides whether completion or the one-time
        // snapshot won the race. A missing registration means the snapshot won.
        let registered = state.pending_spans.remove(&id)?;
        debug_assert!(Arc::ptr_eq(&registered, &pending));
        drop(registered);
        let pending = match Arc::try_unwrap(pending) {
            Ok(pending) => pending
                .into_inner()
                .unwrap_or_else(|error| error.into_inner()),
            Err(pending) => pending
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone(),
        };
        let (span, duration) = finish_pending_span(pending, status, end_offset);
        state.spans.push(span);
        Some(duration)
    }
}

fn finish_pending_span(
    pending: PendingTimelineSpan,
    status: TimelineSpanStatus,
    end_offset: Duration,
) -> (TimelineSpan, Duration) {
    let start_offset_micros = duration_to_micros_saturating(pending.start_offset);
    let end_offset_micros = duration_to_micros_saturating(end_offset);
    let duration = Duration::from_micros(end_offset_micros.saturating_sub(start_offset_micros));
    let span = TimelineSpan::new(
        pending.id,
        pending.parent_id,
        pending.name,
        pending.category,
        pending.start_offset,
        duration,
        status,
        TimelineSpanTimeSemantics::WallClock,
    )
    .with_track_name(pending.track_name);
    let span = pending
        .attributes
        .into_iter()
        .fold(span, |span, (name, value)| span.with_attribute(name, value));
    (span, duration)
}

#[derive(Debug, Clone)]
struct PendingTimelineSpan {
    id: u64,
    parent_id: Option<u64>,
    name: String,
    category: String,
    track_name: String,
    start_offset: Duration,
    attributes: BTreeMap<String, Value>,
}

/// An in-progress span that records cancellation if its owner exits early.
#[derive(Debug)]
pub(crate) struct OperationTimelineSpanRecorder {
    recorder: OperationTimelineRecorder,
    span_id: Option<u64>,
    span: Option<Arc<Mutex<PendingTimelineSpan>>>,
}

impl OperationTimelineSpanRecorder {
    pub(crate) fn id(&self) -> Option<u64> {
        self.span_id
    }

    pub(crate) fn with_parent_id(self, parent_id: Option<u64>) -> Self {
        if let Some(span) = &self.span {
            span.lock()
                .unwrap_or_else(|error| error.into_inner())
                .parent_id = parent_id;
        }
        self
    }

    pub(crate) fn with_attribute(self, name: impl Into<String>, value: Value) -> Self {
        if let Some(span) = &self.span {
            span.lock()
                .unwrap_or_else(|error| error.into_inner())
                .attributes
                .insert(name.into(), value);
        }
        self
    }

    pub(crate) fn completed(mut self) {
        let _ = self.finish(TimelineSpanStatus::Completed);
    }

    pub(crate) fn failed(mut self) {
        let _ = self.finish(TimelineSpanStatus::Failed);
    }

    pub(crate) fn finish_with_duration(mut self, status: TimelineSpanStatus) -> Duration {
        self.finish(status).unwrap_or_default()
    }

    fn finish(&mut self, status: TimelineSpanStatus) -> Option<Duration> {
        self.recorder
            .record(self.span_id.take()?, self.span.take()?, status)
    }
}

impl Drop for OperationTimelineSpanRecorder {
    fn drop(&mut self) {
        let _ = self.finish(TimelineSpanStatus::Cancelled);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{Arc, Barrier},
        time::Duration,
    };

    use serde_json::json;

    use crate::{
        QueryExecutionMetric, QueryExecutionMetricCategory, QueryExecutionMetricValue,
        QueryExecutionOperatorProfile, QueryExecutionOutcome,
    };

    use super::*;

    #[test]
    fn timeline_preserves_nested_and_overlapping_relative_spans() {
        let planning = TimelineSpan::new(
            1,
            None,
            "planning",
            "delta_funnel.phase",
            Duration::from_micros(100),
            Duration::from_micros(400),
            TimelineSpanStatus::Completed,
            TimelineSpanTimeSemantics::WallClock,
        );
        let operator = TimelineSpan::new(
            2,
            Some(1),
            "FilterExec",
            "datafusion.operator",
            Duration::from_micros(250),
            Duration::from_micros(200),
            TimelineSpanStatus::Completed,
            TimelineSpanTimeSemantics::Lifecycle,
        )
        .with_attribute("partition", json!(3));
        let timeline = OperationTimeline::new(
            "preview",
            TimelineSpanStatus::Completed,
            Duration::from_micros(800),
            vec![planning, operator],
        );

        assert_eq!(timeline.name(), "preview");
        assert_eq!(timeline.total_duration_micros(), 800);
        assert_eq!(timeline.spans()[1].parent_id(), Some(1));
        assert_eq!(timeline.spans()[1].start_offset_micros(), 250);
        assert_eq!(timeline.spans()[1].duration_micros(), 200);
        assert_eq!(timeline.spans()[1].attributes()["partition"], 3);
        assert_eq!(
            timeline.spans()[1].time_semantics(),
            TimelineSpanTimeSemantics::Lifecycle
        );
    }

    #[test]
    fn recorder_preserves_parent_identity_and_positioned_nesting() {
        let recorder = OperationTimelineRecorder::start();
        let parent = recorder.start_span("parent", "test", "task 1");
        let parent_id = parent.id().expect("active parent should expose its ID");
        let child = recorder
            .start_span("child", "test", "task 1")
            .with_parent_id(Some(parent_id));
        child.completed();
        parent.completed();

        let timeline = recorder.finish("nested", TimelineSpanStatus::Completed);
        let parent = timeline
            .spans()
            .iter()
            .find(|span| span.id() == parent_id)
            .expect("parent span should be recorded");
        let child = timeline
            .spans()
            .iter()
            .find(|span| span.parent_id() == Some(parent_id))
            .expect("child span should preserve its parent");
        assert!(parent.start_offset_micros() <= child.start_offset_micros());
        assert!(
            parent
                .start_offset_micros()
                .saturating_add(parent.duration_micros())
                >= child
                    .start_offset_micros()
                    .saturating_add(child.duration_micros())
        );
    }

    #[test]
    fn concurrent_operator_appends_assign_unique_span_ids() {
        const THREAD_COUNT: usize = 8;
        const PARTITION_COUNT: u64 = 256;

        let mut metrics = Vec::new();
        for partition in 0..PARTITION_COUNT {
            for (name, timestamp) in [("start_timestamp", 0), ("end_timestamp", 1)] {
                metrics.push(QueryExecutionMetric::new(
                    name,
                    QueryExecutionMetricCategory::Summary,
                    Some(partition),
                    None,
                    QueryExecutionMetricValue::TimestampNanoseconds(Some(timestamp)),
                ));
            }
        }
        let profile = Arc::new(QueryExecutionProfile::mssql_output(
            QueryExecutionOutcome::Success,
            vec![QueryExecutionOperatorProfile::new(
                1,
                None,
                "TestExec",
                PARTITION_COUNT,
                true,
                Vec::new(),
                metrics,
                None,
            )],
        ));
        let recorder = Arc::new(OperationTimelineRecorder::start());
        let barrier = Arc::new(Barrier::new(THREAD_COUNT));

        std::thread::scope(|scope| {
            for _ in 0..THREAD_COUNT {
                let profile = Arc::clone(&profile);
                let recorder = Arc::clone(&recorder);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    recorder.append_operator_lifecycles(&profile);
                });
            }
        });

        let timeline = recorder.finish("concurrent", TimelineSpanStatus::Completed);
        let ids = timeline
            .spans()
            .iter()
            .map(TimelineSpan::id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            timeline.spans().len(),
            THREAD_COUNT * usize::try_from(PARTITION_COUNT).expect("partition count fits usize")
        );
        assert_eq!(ids.len(), timeline.spans().len());
    }

    #[test]
    fn finish_cancels_active_span_at_cutoff_and_late_completion_is_a_noop() {
        let recorder = OperationTimelineRecorder::start();
        let span = recorder
            .start_span("active", "test", "test track")
            .with_attribute("detail", json!("preserved"));
        let span_id = span.id().expect("active span should have an ID");

        let timeline = recorder.finish("operation", TimelineSpanStatus::Completed);
        let snapshotted = timeline
            .spans()
            .iter()
            .find(|span| span.id() == span_id)
            .expect("active span should be included in the snapshot");
        assert_eq!(snapshotted.status(), TimelineSpanStatus::Cancelled);
        assert_eq!(snapshotted.attributes()["detail"], "preserved");
        assert_eq!(
            snapshotted
                .start_offset_micros()
                .checked_add(snapshotted.duration_micros()),
            Some(timeline.total_duration_micros())
        );
        crate::report::trace_contract::validate_operation_trace(&timeline)
            .expect("snapshot should satisfy the structural trace contract");

        span.completed();
        let repeated = recorder.finish("operation", TimelineSpanStatus::Completed);
        assert_eq!(
            repeated, timeline,
            "late completion must not mutate the snapshot"
        );
    }

    #[test]
    fn finished_recorder_rejects_new_spans() {
        let recorder = OperationTimelineRecorder::start();
        let timeline = recorder.finish("operation", TimelineSpanStatus::Completed);

        let span = recorder.start_span("late", "test", "test track");
        assert_eq!(span.id(), None);
        span.completed();

        assert_eq!(
            recorder.finish("operation", TimelineSpanStatus::Completed),
            timeline
        );
    }
}
