//! Shared wall-clock timeline values for profiled operations.

use std::{collections::BTreeMap, time::Duration};

use serde_json::Value;

use super::duration_to_micros_saturating;

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

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
}
