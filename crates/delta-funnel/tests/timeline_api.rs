//! Compile-time coverage for the public operation timeline API.

use std::{collections::BTreeMap, time::Duration};

use delta_funnel::{
    OperationTimeline, TimelineSpan, TimelineSpanStatus, TimelineSpanTimeSemantics,
};
use serde_json::Value;

#[test]
fn operation_timeline_types_and_accessors_are_exported_from_the_crate_root() {
    let _: fn(&OperationTimeline) -> &str = OperationTimeline::name;
    let _: fn(&OperationTimeline) -> TimelineSpanStatus = OperationTimeline::status;
    let _: fn(&OperationTimeline) -> u64 = OperationTimeline::total_duration_micros;
    let _: for<'a> fn(&'a OperationTimeline) -> &'a [TimelineSpan] = OperationTimeline::spans;
    let _: fn(&OperationTimeline) -> Value = OperationTimeline::to_json_value;
    let _: fn(&OperationTimeline) -> Value = OperationTimeline::to_trace_event_json_value;

    let _: fn(&TimelineSpan) -> u64 = TimelineSpan::id;
    let _: fn(&TimelineSpan) -> Option<u64> = TimelineSpan::parent_id;
    let _: fn(&TimelineSpan) -> &str = TimelineSpan::name;
    let _: fn(&TimelineSpan) -> &str = TimelineSpan::track_name;
    let _: fn(&TimelineSpan) -> &str = TimelineSpan::category;
    let _: fn(&TimelineSpan) -> u64 = TimelineSpan::start_offset_micros;
    let _: fn(&TimelineSpan) -> u64 = TimelineSpan::duration_micros;
    let _: fn(&TimelineSpan) -> TimelineSpanStatus = TimelineSpan::status;
    let _: fn(&TimelineSpan) -> TimelineSpanTimeSemantics = TimelineSpan::time_semantics;
    let _: fn(&TimelineSpan) -> &BTreeMap<String, Value> = TimelineSpan::attributes;

    assert_eq!(OperationTimeline::SCHEMA_VERSION, 1);
    assert_eq!(TimelineSpanStatus::Completed.as_str(), "completed");
    assert_eq!(TimelineSpanTimeSemantics::Lifecycle.as_str(), "lifecycle");
    let timeline = OperationTimeline::new(
        "preview",
        TimelineSpanStatus::Completed,
        Duration::from_secs(8),
        Vec::new(),
    );
    assert_eq!(timeline.total_duration_micros(), 8_000_000);
}
