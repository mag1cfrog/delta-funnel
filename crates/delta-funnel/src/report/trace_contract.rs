//! Test-only correctness contracts shared by operation trace producers.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use super::{OperationTimeline, TimelineSpan, TimelineSpanStatus, TimelineSpanTimeSemantics};

const OPERATION_CATEGORY: &str = "delta_funnel.operation";
const OPERATOR_ACTIVITY_CATEGORY: &str = "datafusion.operator.activity";
const OPERATOR_LIFECYCLE_CATEGORY: &str = "datafusion.operator.lifecycle";
const PLANNING_ACTIVITY_CATEGORY: &str = "datafusion.planning.activity";
const TRUNCATION_MARKER: &str = "Operator activity trace truncated";

/// Validates both the typed operation timeline and its serialized Chrome trace.
pub(crate) fn validate_operation_trace(timeline: &OperationTimeline) -> Result<(), String> {
    validate_operation_trace_with_truncation(timeline, false)
}

/// Validates a trace that is expected to contain one operator truncation marker.
pub(crate) fn validate_truncated_operation_trace(
    timeline: &OperationTimeline,
) -> Result<(), String> {
    validate_operation_trace_with_truncation(timeline, true)
}

fn validate_operation_trace_with_truncation(
    timeline: &OperationTimeline,
    expect_truncation: bool,
) -> Result<(), String> {
    validate_typed_timeline(timeline)?;

    // Exercise the same serialization boundary as file export. Validation must
    // not accidentally depend on Value insertion order or borrowed Rust data.
    let encoded = serde_json::to_string(&timeline.to_trace_event_json_value())
        .map_err(|error| format!("Chrome trace serialization failed: {error}"))?;
    let trace: Value = serde_json::from_str(&encoded)
        .map_err(|error| format!("Chrome trace parsing failed: {error}"))?;
    validate_trace_document(timeline, &trace, expect_truncation)
}

fn validate_typed_timeline(timeline: &OperationTimeline) -> Result<(), String> {
    let mut spans = BTreeMap::new();
    for span in timeline.spans() {
        if span.id() == 0 {
            return Err(format!(
                "typed span ID 0 is reserved for the operation root: {}",
                typed_span_summary(span)
            ));
        }
        if let Some(previous) = spans.insert(span.id(), span) {
            return Err(format!(
                "duplicate typed span ID {}: first {}, second {}",
                span.id(),
                typed_span_summary(previous),
                typed_span_summary(span)
            ));
        }
        let end = typed_span_end(span)?;
        if end > timeline.total_duration_micros() {
            return Err(format!(
                "typed span ends outside operation duration {}: {}",
                timeline.total_duration_micros(),
                typed_span_summary(span)
            ));
        }
    }

    for span in timeline.spans() {
        let Some(parent_id) = span.parent_id() else {
            continue;
        };
        let parent = spans.get(&parent_id).ok_or_else(|| {
            format!(
                "typed span parent {parent_id} does not resolve: {}",
                typed_span_summary(span)
            )
        })?;
        if span.start_offset_micros() < parent.start_offset_micros()
            || typed_span_end(span)? > typed_span_end(parent)?
        {
            return Err(format!(
                "typed child is not contained by parent: child {}, parent {}",
                typed_span_summary(span),
                typed_span_summary(parent)
            ));
        }
    }

    let mut acyclic = BTreeSet::new();
    for span in timeline.spans() {
        let mut path = BTreeSet::new();
        let mut current_id = Some(span.id());
        while let Some(id) = current_id {
            if acyclic.contains(&id) {
                break;
            }
            if !path.insert(id) {
                return Err(format!(
                    "typed parent cycle reaches span {id}: {}",
                    typed_span_summary(span)
                ));
            }
            current_id = spans
                .get(&id)
                .ok_or_else(|| format!("typed parent path does not resolve span {id}"))?
                .parent_id();
        }
        acyclic.extend(path);
    }

    Ok(())
}

fn typed_span_end(span: &TimelineSpan) -> Result<u64, String> {
    span.start_offset_micros()
        .checked_add(span.duration_micros())
        .ok_or_else(|| {
            format!(
                "typed span timestamp overflows u64: {}",
                typed_span_summary(span)
            )
        })
}

fn typed_span_summary(span: &TimelineSpan) -> String {
    format!(
        "id={} name={:?} category={:?} track={:?} ts={} dur={}",
        span.id(),
        span.name(),
        span.category(),
        span.track_name(),
        span.start_offset_micros(),
        span.duration_micros()
    )
}

#[derive(Debug)]
struct TraceEvent {
    index: usize,
    id: u64,
    parent_id: Option<u64>,
    name: String,
    category: String,
    pid: u64,
    tid: u64,
    timestamp: u64,
    duration: u64,
    status: String,
    time_semantics: String,
    attributes: Option<Value>,
}

impl TraceEvent {
    fn end(&self) -> Result<u64, String> {
        self.timestamp.checked_add(self.duration).ok_or_else(|| {
            format!(
                "Chrome duration event timestamp overflows u64: {}",
                self.summary()
            )
        })
    }

    fn summary(&self) -> String {
        format!(
            "event_index={} id={} name={:?} category={:?} pid={} tid={} ts={} dur={}",
            self.index,
            self.id,
            self.name,
            self.category,
            self.pid,
            self.tid,
            self.timestamp,
            self.duration
        )
    }
}

fn validate_trace_document(
    timeline: &OperationTimeline,
    trace: &Value,
    expect_truncation: bool,
) -> Result<(), String> {
    if trace.get("delta_funnel_timeline") != Some(&timeline.to_json_value()) {
        return Err("embedded delta_funnel_timeline does not match the typed timeline".to_owned());
    }

    let raw_events = trace
        .get("traceEvents")
        .and_then(Value::as_array)
        .ok_or_else(|| "traceEvents must be an array".to_owned())?;
    let process_names = metadata_names(raw_events, "process_name", false)?;
    if process_names.len() != 1
        || process_names.get(&(1, 0)).map(String::as_str)
            != Some(format!("Delta Funnel {}", timeline.name()).as_str())
    {
        return Err(format!(
            "expected exactly one process_name for pid 1 named {:?}, got {process_names:?}",
            format!("Delta Funnel {}", timeline.name())
        ));
    }
    let lane_names = metadata_names(raw_events, "thread_name", true)?;

    let mut events = Vec::new();
    for (index, raw_event) in raw_events.iter().enumerate() {
        if raw_event.get("ph").and_then(Value::as_str) == Some("X") {
            events.push(parse_duration_event(index, raw_event)?);
        }
    }

    let root_indexes = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| (event.category == OPERATION_CATEGORY).then_some(index))
        .collect::<Vec<_>>();
    if root_indexes.len() != 1 {
        return Err(format!(
            "expected exactly one operation root, found {}: {}",
            root_indexes.len(),
            root_indexes
                .iter()
                .map(|index| events[*index].summary())
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    let root = &events[root_indexes[0]];
    validate_root(timeline, root)?;

    let mut events_by_id = BTreeMap::new();
    for event in &events {
        if let Some(previous) = events_by_id.insert(event.id, event) {
            return Err(format!(
                "duplicate Chrome duration event ID {}: first {}, second {}",
                event.id,
                previous.summary(),
                event.summary()
            ));
        }
        validate_known_enums(event)?;
        if event.end()? > timeline.total_duration_micros() {
            return Err(format!(
                "Chrome duration event ends outside operation duration {}: {}",
                timeline.total_duration_micros(),
                event.summary()
            ));
        }
        let lane_name = lane_names.get(&(event.pid, event.tid)).ok_or_else(|| {
            format!(
                "duration event has no thread_name metadata: {}",
                event.summary()
            )
        })?;
        if event.id == 0 {
            if lane_name != timeline.name() {
                return Err(format!(
                    "operation lane metadata is inconsistent: expected {:?}, got {:?}: {}",
                    timeline.name(),
                    lane_name,
                    event.summary()
                ));
            }
        } else {
            validate_attributes(event, lane_name)?;
        }
    }

    for event in events.iter().filter(|event| event.id != 0) {
        let parent_id = event.parent_id.ok_or_else(|| {
            format!(
                "child event requires a numeric parent_id: {}",
                event.summary()
            )
        })?;
        let parent = events_by_id.get(&parent_id).ok_or_else(|| {
            format!(
                "Chrome duration event parent {parent_id} does not resolve: {}",
                event.summary()
            )
        })?;
        if event.timestamp < parent.timestamp || event.end()? > parent.end()? {
            return Err(format!(
                "Chrome child is not contained by parent: child {}, parent {}",
                event.summary(),
                parent.summary()
            ));
        }
    }

    validate_worker_nesting(&events)?;
    validate_typed_and_chrome_agree(timeline, &events, &lane_names)?;
    validate_truncation(&events, expect_truncation)?;
    Ok(())
}

fn metadata_names(
    events: &[Value],
    metadata_name: &str,
    require_tid: bool,
) -> Result<BTreeMap<(u64, u64), String>, String> {
    let mut names = BTreeMap::new();
    for (index, event) in events.iter().enumerate().filter(|(_, event)| {
        event.get("ph").and_then(Value::as_str) == Some("M")
            && event.get("name").and_then(Value::as_str) == Some(metadata_name)
    }) {
        let pid = numeric_field(event, "pid", index)?;
        let tid = if require_tid {
            numeric_field(event, "tid", index)?
        } else {
            0
        };
        let name = event
            .pointer("/args/name")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("metadata event {index} requires string args.name"))?
            .to_owned();
        if let Some(previous) = names.insert((pid, tid), name.clone()) {
            return Err(format!(
                "duplicate {metadata_name} metadata for pid {pid}, tid {tid}: {previous:?} and {name:?}"
            ));
        }
    }
    Ok(names)
}

fn parse_duration_event(index: usize, event: &Value) -> Result<TraceEvent, String> {
    let args = event
        .get("args")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("duration event {index} requires an args object"))?;
    let id = args
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("duration event {index} requires numeric args.id"))?;
    let parent_id = match args.get("parent_id") {
        Some(Value::Null) => None,
        Some(value) => Some(value.as_u64().ok_or_else(|| {
            format!("duration event {index} requires numeric or null args.parent_id")
        })?),
        None => return Err(format!("duration event {index} requires args.parent_id")),
    };
    Ok(TraceEvent {
        index,
        id,
        parent_id,
        name: string_field(event, "name", index)?,
        category: string_field(event, "cat", index)?,
        pid: numeric_field(event, "pid", index)?,
        tid: numeric_field(event, "tid", index)?,
        timestamp: numeric_field(event, "ts", index)?,
        duration: numeric_field(event, "dur", index)?,
        status: args
            .get("status")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("duration event {index} requires string args.status"))?
            .to_owned(),
        time_semantics: args
            .get("time_semantics")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("duration event {index} requires string args.time_semantics"))?
            .to_owned(),
        attributes: args.get("attributes").cloned(),
    })
}

fn numeric_field(event: &Value, name: &str, index: usize) -> Result<u64, String> {
    event
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("event {index} requires numeric {name}"))
}

fn string_field(event: &Value, name: &str, index: usize) -> Result<String, String> {
    event
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("event {index} requires string {name}"))
}

fn validate_root(timeline: &OperationTimeline, root: &TraceEvent) -> Result<(), String> {
    if root.id != 0
        || root.parent_id.is_some()
        || root.name != timeline.name()
        || root.pid != 1
        || root.tid != 1
        || root.timestamp != 0
        || root.duration != timeline.total_duration_micros()
        || root.status != timeline.status().as_str()
        || root.time_semantics != "wall_clock"
        || root.attributes.is_some()
    {
        return Err(format!(
            "operation root must use id=0, null parent, pid=1, tid=1, ts=0, total duration, matching name/status, wall_clock semantics, and no attributes: {}",
            root.summary()
        ));
    }
    Ok(())
}

fn validate_known_enums(event: &TraceEvent) -> Result<(), String> {
    if !matches!(event.status.as_str(), "completed" | "failed" | "cancelled") {
        return Err(format!(
            "unknown duration event status {:?}: {}",
            event.status,
            event.summary()
        ));
    }
    if !matches!(event.time_semantics.as_str(), "wall_clock" | "lifecycle") {
        return Err(format!(
            "unknown duration event time_semantics {:?}: {}",
            event.time_semantics,
            event.summary()
        ));
    }
    Ok(())
}

fn validate_attributes(event: &TraceEvent, lane_name: &str) -> Result<(), String> {
    let attributes = event
        .attributes
        .as_ref()
        .and_then(Value::as_object)
        .ok_or_else(|| {
            format!(
                "child event requires an attributes object: {}",
                event.summary()
            )
        })?;

    const WORKER_ONLY: [&str; 8] = [
        "worker_lane_id",
        "worker_kind",
        "task_kind",
        "runtime_task_id",
        "execution_stream_id",
        "operator_partition",
        "worker_thread_id",
        "worker_thread_name",
    ];
    if (event.category != OPERATOR_ACTIVITY_CATEGORY || event.name == TRUNCATION_MARKER)
        && let Some(key) = WORKER_ONLY
            .iter()
            .find(|key| attributes.contains_key(**key))
    {
        return Err(format!(
            "attribute {key:?} only applies to operator worker activity: {}",
            event.summary()
        ));
    }

    if event.category == OPERATOR_ACTIVITY_CATEGORY && event.name != TRUNCATION_MARKER {
        let query_id = required_u64(attributes, "query_execution_id", event)?;
        required_str(attributes, "query_scope", event)?;
        let worker_id = required_u64(attributes, "worker_lane_id", event)?;
        let worker_kind = required_str(attributes, "worker_kind", event)?;
        required_str(attributes, "task_kind", event)?;
        required_nullable_str(attributes, "runtime_task_id", event)?;
        required_u64(attributes, "execution_stream_id", event)?;
        required_u64(attributes, "node_id", event)?;
        required_nullable_u64(attributes, "parent_node_id", event)?;
        required_u64(attributes, "operator_partition", event)?;
        required_str(attributes, "worker_thread_id", event)?;
        required_nullable_str(attributes, "worker_thread_name", event)?;
        required_str(attributes, "activity", event)?;

        let expected_lane = match worker_kind {
            "coordinator" if worker_id == 0 => {
                format!("DataFusion query [{query_id}] / coordinator")
            }
            "runtime" if worker_id != 0 => {
                format!("DataFusion query [{query_id}] / worker [{worker_id}]")
            }
            "external" if worker_id != 0 => {
                format!("DataFusion query [{query_id}] / external worker [{worker_id}]")
            }
            _ => {
                return Err(format!(
                    "worker_kind {worker_kind:?} and worker_lane_id {worker_id} are inconsistent: {}",
                    event.summary()
                ));
            }
        };
        if lane_name != expected_lane {
            return Err(format!(
                "worker lane metadata is inconsistent: expected {expected_lane:?}, got {lane_name:?}: {}",
                event.summary()
            ));
        }
    } else if event.category == OPERATOR_LIFECYCLE_CATEGORY {
        required_u64(attributes, "node_id", event)?;
        required_nullable_u64(attributes, "parent_node_id", event)?;
        required_u64(attributes, "partition", event)?;
        required_u64(attributes, "output_partition_count", event)?;
        required_array(attributes, "metrics", event)?;
    } else if event.category == PLANNING_ACTIVITY_CATEGORY {
        required_u64(attributes, "query_execution_id", event)?;
        required_str(attributes, "query_scope", event)?;
        required_str(attributes, "activity", event)?;
    } else if event.name == TRUNCATION_MARKER {
        required_u64(attributes, "query_execution_id", event)?;
        required_str(attributes, "query_scope", event)?;
        required_u64(attributes, "maximum_spans", event)?;
    }

    Ok(())
}

fn required_u64(
    attributes: &serde_json::Map<String, Value>,
    name: &str,
    event: &TraceEvent,
) -> Result<u64, String> {
    attributes
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("attribute {name:?} must be numeric: {}", event.summary()))
}

fn required_str<'a>(
    attributes: &'a serde_json::Map<String, Value>,
    name: &str,
    event: &TraceEvent,
) -> Result<&'a str, String> {
    attributes
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("attribute {name:?} must be a string: {}", event.summary()))
}

fn required_nullable_u64(
    attributes: &serde_json::Map<String, Value>,
    name: &str,
    event: &TraceEvent,
) -> Result<(), String> {
    match attributes.get(name) {
        Some(Value::Null) => Ok(()),
        Some(value) if value.as_u64().is_some() => Ok(()),
        _ => Err(format!(
            "attribute {name:?} must be an unsigned integer or null: {}",
            event.summary()
        )),
    }
}

fn required_nullable_str(
    attributes: &serde_json::Map<String, Value>,
    name: &str,
    event: &TraceEvent,
) -> Result<(), String> {
    match attributes.get(name) {
        Some(Value::Null | Value::String(_)) => Ok(()),
        _ => Err(format!(
            "attribute {name:?} must be a string or null: {}",
            event.summary()
        )),
    }
}

fn required_array(
    attributes: &serde_json::Map<String, Value>,
    name: &str,
    event: &TraceEvent,
) -> Result<(), String> {
    attributes
        .get(name)
        .and_then(Value::as_array)
        .map(|_| ())
        .ok_or_else(|| format!("attribute {name:?} must be an array: {}", event.summary()))
}

fn validate_worker_nesting(events: &[TraceEvent]) -> Result<(), String> {
    let mut lanes: BTreeMap<(u64, u64), Vec<&TraceEvent>> = BTreeMap::new();
    for event in events.iter().filter(|event| {
        event.category == OPERATOR_ACTIVITY_CATEGORY && event.name != TRUNCATION_MARKER
    }) {
        let attributes = event
            .attributes
            .as_ref()
            .and_then(Value::as_object)
            .ok_or_else(|| format!("worker event has no attributes: {}", event.summary()))?;
        let query_id = required_u64(attributes, "query_execution_id", event)?;
        let worker_id = required_u64(attributes, "worker_lane_id", event)?;
        lanes.entry((query_id, worker_id)).or_default().push(event);
    }

    for ((query_id, worker_id), lane) in &mut lanes {
        lane.sort_by_key(|event| (event.timestamp, std::cmp::Reverse(event.duration), event.id));
        let mut active: Vec<&TraceEvent> = Vec::new();
        for event in lane {
            while active
                .last()
                .is_some_and(|parent| event.timestamp >= parent.end().unwrap_or(u64::MAX))
            {
                active.pop();
            }
            if let Some(parent) = active.last()
                && event.end()? > parent.end()?
            {
                return Err(format!(
                    "operator activity spans cross on query {query_id}, worker {worker_id}: earlier {}, later {}",
                    parent.summary(),
                    event.summary()
                ));
            }
            active.push(event);
        }
    }
    Ok(())
}

fn validate_typed_and_chrome_agree(
    timeline: &OperationTimeline,
    events: &[TraceEvent],
    lane_names: &BTreeMap<(u64, u64), String>,
) -> Result<(), String> {
    let typed = timeline
        .spans()
        .iter()
        .map(|span| (span.id(), span))
        .collect::<BTreeMap<_, _>>();
    let chrome = events
        .iter()
        .filter(|event| event.id != 0)
        .map(|event| (event.id, event))
        .collect::<BTreeMap<_, _>>();
    if typed.keys().copied().collect::<BTreeSet<_>>()
        != chrome.keys().copied().collect::<BTreeSet<_>>()
    {
        return Err(format!(
            "typed and Chrome span IDs differ: typed={:?}, Chrome={:?}",
            typed.keys().collect::<Vec<_>>(),
            chrome.keys().collect::<Vec<_>>()
        ));
    }

    for (id, span) in typed {
        let event = chrome[&id];
        let expected_parent = span.parent_id().unwrap_or(0);
        let lane_name = lane_names.get(&(event.pid, event.tid));
        let attributes_match = event.attributes.as_ref() == Some(&json!(span.attributes()));
        if event.name != span.name()
            || event.category != span.category()
            || event.pid != 1
            || event.timestamp != span.start_offset_micros()
            || event.duration != span.duration_micros()
            || event.parent_id != Some(expected_parent)
            || event.status != span.status().as_str()
            || event.time_semantics != span.time_semantics().as_str()
            || lane_name.map(String::as_str) != Some(span.track_name())
            || !attributes_match
        {
            return Err(format!(
                "Chrome event does not match typed span: typed {}, Chrome {}",
                typed_span_summary(span),
                event.summary()
            ));
        }
    }
    Ok(())
}

fn validate_truncation(events: &[TraceEvent], expect_truncation: bool) -> Result<(), String> {
    let markers = events
        .iter()
        .filter(|event| event.name == TRUNCATION_MARKER)
        .collect::<Vec<_>>();
    let expected = usize::from(expect_truncation);
    if markers.len() != expected {
        return Err(format!(
            "expected {expected} operator truncation marker(s), found {}: {}",
            markers.len(),
            markers
                .iter()
                .map(|event| event.summary())
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::{Map, Value, json};

    use super::*;

    fn worker_span(id: u64, parent_id: Option<u64>, start: u64, duration: u64) -> TimelineSpan {
        TimelineSpan::new(
            id,
            parent_id,
            format!("Worker span {id}"),
            OPERATOR_ACTIVITY_CATEGORY,
            Duration::from_micros(start),
            Duration::from_micros(duration),
            TimelineSpanStatus::Completed,
            TimelineSpanTimeSemantics::WallClock,
        )
        .with_track_name("DataFusion query [1] / worker [1]")
        .with_attribute("query_execution_id", json!(1))
        .with_attribute("query_scope", json!("preview"))
        .with_attribute("worker_lane_id", json!(1))
        .with_attribute("worker_kind", json!("runtime"))
        .with_attribute("task_kind", json!("tokio"))
        .with_attribute("runtime_task_id", json!("7"))
        .with_attribute("execution_stream_id", json!(2))
        .with_attribute("node_id", json!(3))
        .with_attribute("parent_node_id", Value::Null)
        .with_attribute("operator_partition", json!(0))
        .with_attribute("worker_thread_id", json!("ThreadId(2)"))
        .with_attribute("worker_thread_name", json!("tokio-runtime-worker"))
        .with_attribute("activity", json!("poll_next"))
    }

    fn valid_timeline() -> OperationTimeline {
        OperationTimeline::new(
            "preview",
            TimelineSpanStatus::Completed,
            Duration::from_micros(100),
            vec![
                TimelineSpan::new(
                    1,
                    None,
                    "Logical phase A",
                    "delta_funnel.phase",
                    Duration::from_micros(5),
                    Duration::from_micros(50),
                    TimelineSpanStatus::Completed,
                    TimelineSpanTimeSemantics::WallClock,
                )
                .with_track_name("Logical phases"),
                worker_span(2, None, 10, 50),
                worker_span(3, Some(2), 20, 10),
                TimelineSpan::new(
                    4,
                    None,
                    "Logical phase B",
                    "delta_funnel.phase",
                    Duration::from_micros(25),
                    Duration::from_micros(60),
                    TimelineSpanStatus::Completed,
                    TimelineSpanTimeSemantics::WallClock,
                )
                .with_track_name("Logical phases"),
            ],
        )
    }

    fn trace_error(mutate: impl FnOnce(&mut Value), expect_truncation: bool) -> String {
        let timeline = valid_timeline();
        let mut trace = timeline.to_trace_event_json_value();
        mutate(&mut trace);
        validate_trace_document(&timeline, &trace, expect_truncation)
            .expect_err("malformed trace should fail validation")
    }

    fn duration_event_mut(trace: &mut Value, id: u64) -> &mut Value {
        trace["traceEvents"]
            .as_array_mut()
            .expect("trace events")
            .iter_mut()
            .find(|event| event.pointer("/args/id").and_then(Value::as_u64) == Some(id))
            .expect("duration event")
    }

    fn remove_metadata(trace: &mut Value, name: &str, tid: Option<u64>) {
        trace["traceEvents"]
            .as_array_mut()
            .expect("trace events")
            .retain(|event| {
                event.get("name").and_then(Value::as_str) != Some(name)
                    || tid.is_some_and(|tid| event.get("tid").and_then(Value::as_u64) != Some(tid))
            });
    }

    #[test]
    fn valid_trace_is_independent_of_event_order_and_allows_logical_overlap() {
        let timeline = valid_timeline();
        validate_operation_trace(&timeline).expect("canonical trace should be valid");

        let mut trace = timeline.to_trace_event_json_value();
        trace["traceEvents"]
            .as_array_mut()
            .expect("trace events")
            .reverse();
        validate_trace_document(&timeline, &trace, false)
            .expect("reordered trace should remain valid");
    }

    #[test]
    fn rejects_multiple_roots() {
        let error = trace_error(
            |trace| {
                let root = duration_event_mut(trace, 0).clone();
                trace["traceEvents"]
                    .as_array_mut()
                    .expect("trace events")
                    .push(root);
            },
            false,
        );
        assert!(error.contains("exactly one operation root"), "{error}");
    }

    #[test]
    fn rejects_duplicate_chrome_ids() {
        let error = trace_error(
            |trace| duration_event_mut(trace, 3)["args"]["id"] = json!(2),
            false,
        );
        assert!(
            error.contains("duplicate Chrome duration event ID"),
            "{error}"
        );
    }

    #[test]
    fn rejects_wrong_root_origin_and_duration() {
        let error = trace_error(
            |trace| {
                let root = duration_event_mut(trace, 0);
                root["ts"] = json!(1);
                root["dur"] = json!(99);
            },
            false,
        );
        assert!(error.contains("operation root must"), "{error}");
    }

    #[test]
    fn rejects_nonnumeric_and_overflowing_trace_times() {
        let nonnumeric = trace_error(
            |trace| duration_event_mut(trace, 1)["ts"] = json!("5"),
            false,
        );
        assert!(nonnumeric.contains("requires numeric ts"), "{nonnumeric}");

        let overflow = trace_error(
            |trace| {
                let event = duration_event_mut(trace, 1);
                event["ts"] = json!(u64::MAX);
                event["dur"] = json!(1);
            },
            false,
        );
        assert!(overflow.contains("overflows u64"), "{overflow}");

        let outside = trace_error(
            |trace| {
                let event = duration_event_mut(trace, 1);
                event["ts"] = json!(95);
                event["dur"] = json!(10);
            },
            false,
        );
        assert!(outside.contains("outside operation duration"), "{outside}");
    }

    #[test]
    fn rejects_typed_overflow_and_root_escape() {
        let overflow = OperationTimeline::new(
            "overflow",
            TimelineSpanStatus::Completed,
            Duration::from_micros(u64::MAX),
            vec![TimelineSpan::new(
                1,
                None,
                "overflow",
                "test",
                Duration::from_micros(u64::MAX),
                Duration::from_micros(1),
                TimelineSpanStatus::Completed,
                TimelineSpanTimeSemantics::WallClock,
            )],
        );
        let error = validate_operation_trace(&overflow).expect_err("overflow should fail");
        assert!(error.contains("overflows u64"), "{error}");

        let outside = OperationTimeline::new(
            "outside",
            TimelineSpanStatus::Completed,
            Duration::from_micros(10),
            vec![TimelineSpan::new(
                1,
                None,
                "outside",
                "test",
                Duration::from_micros(9),
                Duration::from_micros(2),
                TimelineSpanStatus::Completed,
                TimelineSpanTimeSemantics::WallClock,
            )],
        );
        let error = validate_operation_trace(&outside).expect_err("root escape should fail");
        assert!(error.contains("outside operation duration"), "{error}");
    }

    #[test]
    fn rejects_duplicate_or_reserved_typed_ids() {
        let duplicate = OperationTimeline::new(
            "duplicate",
            TimelineSpanStatus::Completed,
            Duration::from_micros(10),
            vec![worker_span(1, None, 1, 2), worker_span(1, None, 4, 2)],
        );
        let error = validate_operation_trace(&duplicate).expect_err("duplicate should fail");
        assert!(error.contains("duplicate typed span ID"), "{error}");

        let reserved = OperationTimeline::new(
            "reserved",
            TimelineSpanStatus::Completed,
            Duration::from_micros(10),
            vec![worker_span(0, None, 1, 2)],
        );
        let error = validate_operation_trace(&reserved).expect_err("reserved ID should fail");
        assert!(error.contains("ID 0 is reserved"), "{error}");
    }

    #[test]
    fn rejects_missing_parent_and_child_outside_parent() {
        let missing = OperationTimeline::new(
            "missing parent",
            TimelineSpanStatus::Completed,
            Duration::from_micros(20),
            vec![worker_span(1, Some(99), 1, 2)],
        );
        let error = validate_operation_trace(&missing).expect_err("missing parent should fail");
        assert!(error.contains("parent 99 does not resolve"), "{error}");

        let outside = OperationTimeline::new(
            "outside parent",
            TimelineSpanStatus::Completed,
            Duration::from_micros(20),
            vec![worker_span(1, None, 1, 5), worker_span(2, Some(1), 5, 3)],
        );
        let error = validate_operation_trace(&outside).expect_err("outside parent should fail");
        assert!(error.contains("not contained by parent"), "{error}");
    }

    #[test]
    fn rejects_typed_parent_cycle() {
        let timeline = OperationTimeline::new(
            "cycle",
            TimelineSpanStatus::Completed,
            Duration::from_micros(10),
            vec![worker_span(1, Some(1), 1, 2)],
        );

        let error = validate_operation_trace(&timeline).expect_err("parent cycle should fail");
        assert!(error.contains("typed parent cycle"), "{error}");
    }

    #[test]
    fn rejects_unresolved_or_noncontaining_chrome_parents() {
        let missing = trace_error(
            |trace| duration_event_mut(trace, 3)["args"]["parent_id"] = json!(99),
            false,
        );
        assert!(missing.contains("parent 99 does not resolve"), "{missing}");

        let outside = trace_error(
            |trace| {
                let event = duration_event_mut(trace, 3);
                event["ts"] = json!(55);
                event["dur"] = json!(10);
            },
            false,
        );
        assert!(outside.contains("not contained by parent"), "{outside}");
    }

    #[test]
    fn rejects_mismatched_embedded_timeline() {
        let error = trace_error(
            |trace| trace["delta_funnel_timeline"]["name"] = json!("different"),
            false,
        );
        assert!(
            error.contains("does not match the typed timeline"),
            "{error}"
        );
    }

    #[test]
    fn rejects_missing_or_duplicate_metadata() {
        let process = trace_error(|trace| remove_metadata(trace, "process_name", None), false);
        assert!(process.contains("exactly one process_name"), "{process}");

        let lane = trace_error(
            |trace| {
                let tid = duration_event_mut(trace, 2)["tid"].as_u64().expect("tid");
                remove_metadata(trace, "thread_name", Some(tid));
            },
            false,
        );
        assert!(lane.contains("no thread_name metadata"), "{lane}");

        let duplicate = trace_error(
            |trace| {
                let metadata = trace["traceEvents"]
                    .as_array()
                    .expect("trace events")
                    .iter()
                    .find(|event| event.get("name").and_then(Value::as_str) == Some("thread_name"))
                    .expect("thread metadata")
                    .clone();
                trace["traceEvents"]
                    .as_array_mut()
                    .expect("trace events")
                    .push(metadata);
            },
            false,
        );
        assert!(
            duplicate.contains("duplicate thread_name metadata"),
            "{duplicate}"
        );
    }

    #[test]
    fn rejects_unknown_status_and_time_semantics() {
        let status = trace_error(
            |trace| duration_event_mut(trace, 1)["args"]["status"] = json!("pending"),
            false,
        );
        assert!(status.contains("unknown duration event status"), "{status}");

        let semantics = trace_error(
            |trace| {
                duration_event_mut(trace, 1)["args"]["time_semantics"] = json!("cpu_time");
            },
            false,
        );
        assert!(
            semantics.contains("unknown duration event time_semantics"),
            "{semantics}"
        );
    }

    #[test]
    fn rejects_missing_worker_attributes_but_not_generic_phase_attributes() {
        let error = trace_error(
            |trace| {
                duration_event_mut(trace, 2)["args"]["attributes"]
                    .as_object_mut()
                    .expect("attributes")
                    .remove("worker_lane_id");
            },
            false,
        );
        assert!(error.contains("worker_lane_id"), "{error}");

        let timeline = valid_timeline();
        assert!(
            timeline.spans()[0].attributes().is_empty(),
            "generic phases deliberately have no worker attributes"
        );
        validate_operation_trace(&timeline).expect("generic phase should remain valid");
    }

    #[test]
    fn rejects_negative_nullable_unsigned_worker_attribute() {
        let timeline = OperationTimeline::new(
            "negative attribute",
            TimelineSpanStatus::Completed,
            Duration::from_micros(10),
            vec![worker_span(1, None, 1, 2).with_attribute("parent_node_id", json!(-1))],
        );

        let error =
            validate_operation_trace(&timeline).expect_err("negative parent_node_id should fail");
        assert!(error.contains("parent_node_id"), "{error}");
    }

    #[test]
    fn rejects_worker_attributes_on_unrelated_categories() {
        let mut attributes = Map::new();
        attributes.insert("worker_lane_id".to_owned(), json!(1));
        let error = trace_error(
            |trace| {
                duration_event_mut(trace, 1)["args"]["attributes"] = Value::Object(attributes);
            },
            false,
        );
        assert!(error.contains("only applies"), "{error}");
    }

    #[test]
    fn rejects_missing_operator_lifecycle_attributes() {
        let span = TimelineSpan::new(
            1,
            None,
            "FilterExec",
            OPERATOR_LIFECYCLE_CATEGORY,
            Duration::from_micros(1),
            Duration::from_micros(5),
            TimelineSpanStatus::Completed,
            TimelineSpanTimeSemantics::Lifecycle,
        )
        .with_attribute("node_id", json!(1))
        .with_attribute("parent_node_id", Value::Null)
        .with_attribute("partition", json!(0))
        .with_attribute("output_partition_count", json!(1))
        .with_attribute("metrics", json!([]));
        let timeline = OperationTimeline::new(
            "lifecycle",
            TimelineSpanStatus::Completed,
            Duration::from_micros(10),
            vec![span],
        );
        validate_operation_trace(&timeline).expect("complete lifecycle attributes should pass");

        let mut trace = timeline.to_trace_event_json_value();
        duration_event_mut(&mut trace, 1)["args"]["attributes"]
            .as_object_mut()
            .expect("attributes")
            .remove("metrics");
        let error = validate_trace_document(&timeline, &trace, false)
            .expect_err("missing lifecycle metrics should fail");
        assert!(error.contains("metrics"), "{error}");
    }

    #[test]
    fn rejects_inconsistent_worker_lane_identity() {
        let error = trace_error(
            |trace| {
                duration_event_mut(trace, 2)["args"]["attributes"]["worker_lane_id"] = json!(10);
            },
            false,
        );
        assert!(
            error.contains("worker lane metadata is inconsistent"),
            "{error}"
        );
    }

    #[test]
    fn rejects_crossing_worker_activity() {
        let timeline = OperationTimeline::new(
            "crossing",
            TimelineSpanStatus::Completed,
            Duration::from_micros(100),
            vec![worker_span(1, None, 10, 40), worker_span(2, None, 20, 40)],
        );
        let error = validate_operation_trace(&timeline).expect_err("crossing spans should fail");
        assert!(error.contains("activity spans cross"), "{error}");
    }

    #[test]
    fn validates_truncation_marker_contract() {
        let absent = validate_truncated_operation_trace(&valid_timeline())
            .expect_err("expected marker is absent");
        assert!(
            absent.contains("expected 1 operator truncation marker"),
            "{absent}"
        );

        let marker = TimelineSpan::new(
            5,
            None,
            TRUNCATION_MARKER,
            OPERATOR_ACTIVITY_CATEGORY,
            Duration::from_micros(90),
            Duration::ZERO,
            TimelineSpanStatus::Completed,
            TimelineSpanTimeSemantics::WallClock,
        )
        .with_track_name("DataFusion query [1] / trace status")
        .with_attribute("query_execution_id", json!(1))
        .with_attribute("query_scope", json!("preview"))
        .with_attribute("maximum_spans", json!(100));
        let mut spans = valid_timeline().spans().to_vec();
        spans.push(marker);
        let timeline = OperationTimeline::new(
            "preview",
            TimelineSpanStatus::Completed,
            Duration::from_micros(100),
            spans,
        );
        validate_truncated_operation_trace(&timeline).expect("marker should satisfy contract");
        let unexpected = validate_operation_trace(&timeline).expect_err("marker was not expected");
        assert!(
            unexpected.contains("expected 0 operator truncation marker"),
            "{unexpected}"
        );
    }
}
