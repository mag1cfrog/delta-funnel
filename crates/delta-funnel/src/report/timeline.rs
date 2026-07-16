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
                spans: Vec::new(),
            })),
        }
    }

    pub(crate) fn start_span(
        &self,
        name: impl Into<String>,
        category: impl Into<String>,
        track_name: impl Into<String>,
    ) -> OperationTimelineSpanRecorder {
        let id = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let id = state.next_span_id;
            state.next_span_id = state.next_span_id.saturating_add(1);
            id
        };
        let start_offset = self.started_at.elapsed();
        OperationTimelineSpanRecorder {
            recorder: self.clone(),
            span: Some(PendingTimelineSpan {
                id,
                name: name.into(),
                category: category.into(),
                track_name: track_name.into(),
                started_at: Instant::now(),
                start_offset,
                attributes: BTreeMap::new(),
            }),
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
        let total_duration = self.elapsed();
        let mut spans = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .spans
            .clone();
        spans.sort_by_key(|span| (span.start_offset_micros(), span.id()));
        OperationTimeline::new(name, status, total_duration, spans)
    }

    fn record(&self, pending: PendingTimelineSpan, status: TimelineSpanStatus) {
        let span = TimelineSpan::new(
            pending.id,
            None,
            pending.name,
            pending.category,
            pending.start_offset,
            pending.started_at.elapsed(),
            status,
            TimelineSpanTimeSemantics::WallClock,
        )
        .with_track_name(pending.track_name);
        let span = pending
            .attributes
            .into_iter()
            .fold(span, |span, (name, value)| span.with_attribute(name, value));
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .spans
            .push(span);
    }
}

#[derive(Debug)]
struct PendingTimelineSpan {
    id: u64,
    name: String,
    category: String,
    track_name: String,
    started_at: Instant,
    start_offset: Duration,
    attributes: BTreeMap<String, Value>,
}

/// An in-progress span that records cancellation if its owner exits early.
#[derive(Debug)]
pub(crate) struct OperationTimelineSpanRecorder {
    recorder: OperationTimelineRecorder,
    span: Option<PendingTimelineSpan>,
}

impl OperationTimelineSpanRecorder {
    pub(crate) fn with_attribute(mut self, name: impl Into<String>, value: Value) -> Self {
        if let Some(span) = &mut self.span {
            span.attributes.insert(name.into(), value);
        }
        self
    }

    pub(crate) fn completed(mut self) {
        self.finish(TimelineSpanStatus::Completed);
    }

    pub(crate) fn failed(mut self) {
        self.finish(TimelineSpanStatus::Failed);
    }

    fn finish(&mut self, status: TimelineSpanStatus) {
        if let Some(span) = self.span.take() {
            self.recorder.record(span, status);
        }
    }
}

impl Drop for OperationTimelineSpanRecorder {
    fn drop(&mut self) {
        self.finish(TimelineSpanStatus::Cancelled);
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
}
