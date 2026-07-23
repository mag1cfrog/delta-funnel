use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::Deserialize;

use super::ranked_report::{
    MAX_RECORDS_PER_COLLECTION, RankedFunction, RankedProfileDocument, RankedProfileMetadata,
    RankedSemantic, fold_inclusive_counts,
};
use super::report_cli::{RankedReportFailure, RankedReportFailurePhase};
use super::report_health::{CAPTURE_HEALTH_SQL, capture_health_input_sql, validate_capture_health};
use super::report_trace_processor::run_trace_processor_query;
use super::report_trace_sanitizer::sanitize_trace;

const RECORD_HEADER: &[u8] = b"\"record_hex\"";
const MAX_RECORD_HEX_CHARS: usize = 64 * 1024;
const UNRESOLVED_FUNCTION_ID: i64 = -1;
const SAMPLE_CORRELATION_SQL: &str = include_str!("sql/sample_correlation.sql");
const RANKED_PROFILE_BASE_SQL: &str = include_str!("sql/ranked_profile_base.sql");
const RANKED_REPORT_SQL: &str = include_str!("sql/ranked_report.sql");

#[derive(Deserialize)]
#[serde(
    tag = "record_kind",
    content = "record",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum CompactRecord {
    Metadata(CompactMetadata),
    Semantic(Box<RankedSemantic>),
    Frame(CompactFrame),
    FunctionSelf(CompactFunctionSelf),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactMetadata {
    capture_complete: bool,
    semantic_complete: bool,
    schema_version: u32,
    sample_frequency_hz: u32,
    exact_time_unit: String,
    sample_unit: String,
    eligible_sample_count: i64,
    direct_sample_count: i64,
    ambiguous_sample_count: i64,
    unattributed_sample_count: i64,
    audit_error_count: i64,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactFrame {
    function_id: i64,
    parent_function_id: Option<i64>,
    name: String,
    module_name: Option<String>,
    source_file: Option<String>,
    line_number: Option<i64>,
    official_self_sample_count: i64,
    official_inclusive_sample_count: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactFunctionSelf {
    semantic_id: i64,
    function_id: i64,
    self_sample_count: i64,
}

pub(super) fn load_ranked_profile(
    input: &Path,
) -> Result<RankedProfileDocument, RankedReportFailure> {
    let health_input_sql = capture_health_input_sql(input)?;
    let sql = [
        health_input_sql.as_str(),
        "CREATE PERFETTO TABLE delta_funnel_capture_health AS",
        CAPTURE_HEALTH_SQL,
        SAMPLE_CORRELATION_SQL,
        RANKED_PROFILE_BASE_SQL,
        RANKED_REPORT_SQL,
    ]
    .join("\n");
    let sanitized = sanitize_trace(input)?;
    let output = run_trace_processor_query(sanitized.path(), sql.as_bytes())?;
    parse_ranked_report_output(&output)
}

fn parse_ranked_report_output(output: &[u8]) -> Result<RankedProfileDocument, RankedReportFailure> {
    let mut lines = output
        .split(|byte| *byte == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .filter(|line| !line.is_empty());
    if lines.next() != Some(RECORD_HEADER) {
        return Err(malformed_result());
    }

    let mut metadata = None;
    let mut semantics = Vec::new();
    let mut frames = Vec::new();
    let mut function_self = Vec::new();
    for line in lines {
        let hex = line
            .strip_prefix(b"\"")
            .and_then(|line| line.strip_suffix(b"\""))
            .ok_or_else(malformed_result)?;
        if hex.len() > MAX_RECORD_HEX_CHARS {
            return Err(result_too_large());
        }
        let record = serde_json::from_slice::<CompactRecord>(&decode_hex(hex)?)
            .map_err(|_| malformed_result())?;
        match record {
            CompactRecord::Metadata(record) => {
                if metadata.replace(record).is_some() {
                    return Err(malformed_result());
                }
            }
            CompactRecord::Semantic(record) => push_bounded(&mut semantics, *record)?,
            CompactRecord::Frame(record) => push_bounded(&mut frames, record)?,
            CompactRecord::FunctionSelf(record) => push_bounded(&mut function_self, record)?,
        }
    }

    build_document(
        metadata.ok_or_else(malformed_result)?,
        semantics,
        frames,
        function_self,
    )
}

fn build_document(
    metadata: CompactMetadata,
    mut semantics: Vec<RankedSemantic>,
    frames: Vec<CompactFrame>,
    function_self: Vec<CompactFunctionSelf>,
) -> Result<RankedProfileDocument, RankedReportFailure> {
    validate_capture_health(metadata.capture_complete, metadata.semantic_complete)?;
    if metadata.audit_error_count != 0 {
        return Err(aggregate_failure(
            "audit_failed",
            "Trace Processor aggregate audit failed",
        ));
    }
    semantics.sort_by_key(|semantic| (semantic.operation_id, semantic.semantic_id));
    let mut document = RankedProfileDocument {
        metadata: RankedProfileMetadata {
            schema_version: metadata.schema_version,
            sample_frequency_hz: metadata.sample_frequency_hz,
            exact_time_unit: metadata.exact_time_unit,
            sample_unit: metadata.sample_unit,
            eligible_sample_count: metadata.eligible_sample_count,
            direct_sample_count: metadata.direct_sample_count,
            ambiguous_sample_count: metadata.ambiguous_sample_count,
            unattributed_sample_count: metadata.unattributed_sample_count,
        },
        semantics,
        functions: Vec::new(),
    };
    document
        .validate_structure()
        .map_err(|_| aggregate_failure("invalid_structure", "semantic hierarchy is invalid"))?;
    fold_semantics(&mut document.semantics)?;

    let frames = validate_frames(frames)?;
    document.functions = expand_functions(&frames, function_self)?;
    document
        .functions
        .sort_by_key(|function| (function.semantic_id, function.function_id));
    document
        .validate_structure()
        .map_err(|_| aggregate_failure("invalid_structure", "function hierarchy is invalid"))?;
    fold_functions(&mut document.functions)?;
    reconcile_official_self_counts(&frames, &document.functions)?;
    document.normalize_source_metadata();
    document.validate().map_err(|_| {
        aggregate_failure(
            "invalid_aggregate",
            "ranked profile aggregate validation failed",
        )
    })?;
    Ok(document)
}

fn fold_semantics(semantics: &mut [RankedSemantic]) -> Result<(), RankedReportFailure> {
    let parents = semantics
        .iter()
        .map(|semantic| (semantic.semantic_id, semantic.parent_semantic_id))
        .collect::<HashMap<_, _>>();
    let direct = semantics
        .iter()
        .map(|semantic| (semantic.semantic_id, semantic.direct_sample_count))
        .collect::<HashMap<_, _>>();
    let inclusive = fold_inclusive_counts(&parents, &direct)
        .ok_or_else(|| aggregate_failure("count_overflow", "semantic sample fold failed"))?;
    for semantic in semantics {
        semantic.inclusive_sample_count = *inclusive
            .get(&semantic.semantic_id)
            .ok_or_else(|| aggregate_failure("invalid_structure", "semantic fold is incomplete"))?;
    }
    Ok(())
}

fn validate_frames(
    frames: Vec<CompactFrame>,
) -> Result<HashMap<i64, CompactFrame>, RankedReportFailure> {
    let mut by_id = HashMap::with_capacity(frames.len());
    for frame in frames {
        if frame.function_id < 0
            || frame.official_self_sample_count < 0
            || frame.official_inclusive_sample_count < 0
            || by_id.insert(frame.function_id, frame).is_some()
        {
            return Err(aggregate_failure(
                "invalid_function_graph",
                "native function graph is invalid",
            ));
        }
    }
    let parents = by_id
        .values()
        .map(|frame| (frame.function_id, frame.parent_function_id))
        .collect::<HashMap<_, _>>();
    let self_counts = by_id
        .values()
        .map(|frame| (frame.function_id, frame.official_self_sample_count))
        .collect::<HashMap<_, _>>();
    let inclusive = fold_inclusive_counts(&parents, &self_counts).ok_or_else(|| {
        aggregate_failure(
            "invalid_function_graph",
            "native function graph could not be folded",
        )
    })?;
    for frame in by_id.values() {
        if inclusive.get(&frame.function_id) != Some(&frame.official_inclusive_sample_count) {
            return Err(aggregate_failure(
                "official_count_mismatch",
                "native function summary did not reconcile",
            ));
        }
    }
    Ok(by_id)
}

fn expand_functions(
    frames: &HashMap<i64, CompactFrame>,
    function_self: Vec<CompactFunctionSelf>,
) -> Result<Vec<RankedFunction>, RankedReportFailure> {
    let mut functions = HashMap::<(i64, i64), RankedFunction>::new();
    let mut self_identities = HashSet::with_capacity(function_self.len());
    for record in function_self {
        if record.self_sample_count < 0
            || !self_identities.insert((record.semantic_id, record.function_id))
        {
            return Err(aggregate_failure(
                "invalid_function_self_count",
                "function self counts are invalid",
            ));
        }
        if record.function_id == UNRESOLVED_FUNCTION_ID {
            functions.insert(
                (record.semantic_id, record.function_id),
                RankedFunction {
                    semantic_id: record.semantic_id,
                    function_id: record.function_id,
                    parent_function_id: None,
                    name: "[native stack unavailable]".to_owned(),
                    module_name: None,
                    source_file: None,
                    line_number: None,
                    self_sample_count: record.self_sample_count,
                    inclusive_sample_count: 0,
                },
            );
            if functions.len() > MAX_RECORDS_PER_COLLECTION {
                return Err(result_too_large());
            }
            continue;
        }

        let mut function_id = record.function_id;
        let mut is_self = true;
        let mut path = HashSet::new();
        loop {
            if !path.insert(function_id) {
                return Err(aggregate_failure(
                    "invalid_function_graph",
                    "native function graph contains a cycle",
                ));
            }
            let frame = frames.get(&function_id).ok_or_else(|| {
                aggregate_failure("invalid_function_graph", "native function frame is missing")
            })?;
            let identity = (record.semantic_id, function_id);
            if let Some(function) = functions.get_mut(&identity) {
                if is_self {
                    function.self_sample_count = function
                        .self_sample_count
                        .checked_add(record.self_sample_count)
                        .ok_or_else(|| {
                            aggregate_failure("count_overflow", "function self count overflowed")
                        })?;
                }
                break;
            }
            functions.insert(
                identity,
                RankedFunction {
                    semantic_id: record.semantic_id,
                    function_id,
                    parent_function_id: frame.parent_function_id,
                    name: frame.name.clone(),
                    module_name: frame.module_name.clone(),
                    source_file: frame.source_file.clone(),
                    line_number: frame.line_number,
                    self_sample_count: if is_self { record.self_sample_count } else { 0 },
                    inclusive_sample_count: 0,
                },
            );
            if functions.len() > MAX_RECORDS_PER_COLLECTION {
                return Err(result_too_large());
            }
            let Some(parent_id) = frame.parent_function_id else {
                break;
            };
            function_id = parent_id;
            is_self = false;
        }
    }
    Ok(functions.into_values().collect())
}

fn fold_functions(functions: &mut [RankedFunction]) -> Result<(), RankedReportFailure> {
    let parents = functions
        .iter()
        .map(|function| {
            (
                (function.semantic_id, function.function_id),
                function
                    .parent_function_id
                    .map(|parent| (function.semantic_id, parent)),
            )
        })
        .collect::<HashMap<_, _>>();
    let self_counts = functions
        .iter()
        .map(|function| {
            (
                (function.semantic_id, function.function_id),
                function.self_sample_count,
            )
        })
        .collect::<HashMap<_, _>>();
    let inclusive = fold_inclusive_counts(&parents, &self_counts)
        .ok_or_else(|| aggregate_failure("count_overflow", "function sample fold failed"))?;
    for function in functions {
        function.inclusive_sample_count = *inclusive
            .get(&(function.semantic_id, function.function_id))
            .ok_or_else(|| aggregate_failure("invalid_structure", "function fold is incomplete"))?;
    }
    Ok(())
}

fn reconcile_official_self_counts(
    frames: &HashMap<i64, CompactFrame>,
    functions: &[RankedFunction],
) -> Result<(), RankedReportFailure> {
    let mut actual = HashMap::<i64, i64>::new();
    for function in functions
        .iter()
        .filter(|function| function.function_id != UNRESOLVED_FUNCTION_ID)
    {
        let count = actual.entry(function.function_id).or_default();
        *count = count
            .checked_add(function.self_sample_count)
            .ok_or_else(|| {
                aggregate_failure(
                    "count_overflow",
                    "official function reconciliation overflowed",
                )
            })?;
    }
    if frames.iter().any(|(function_id, frame)| {
        actual.get(function_id).copied().unwrap_or_default() != frame.official_self_sample_count
    }) {
        return Err(aggregate_failure(
            "official_count_mismatch",
            "native function self counts did not reconcile",
        ));
    }
    Ok(())
}

fn push_bounded<T>(records: &mut Vec<T>, record: T) -> Result<(), RankedReportFailure> {
    if records.len() == MAX_RECORDS_PER_COLLECTION {
        return Err(result_too_large());
    }
    records.push(record);
    Ok(())
}

fn decode_hex(value: &[u8]) -> Result<Vec<u8>, RankedReportFailure> {
    if !value.len().is_multiple_of(2) {
        return Err(malformed_result());
    }
    value
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0]).ok_or_else(malformed_result)?;
            let low = hex_digit(pair[1]).ok_or_else(malformed_result)?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn malformed_result() -> RankedReportFailure {
    RankedReportFailure::new(
        RankedReportFailurePhase::Query,
        "malformed_result",
        "ranked profile query returned an unexpected result",
    )
}

fn result_too_large() -> RankedReportFailure {
    RankedReportFailure::new(
        RankedReportFailurePhase::Query,
        "result_too_large",
        "ranked profile query returned too many records",
    )
}

fn aggregate_failure(kind: &'static str, message: &'static str) -> RankedReportFailure {
    RankedReportFailure::new(RankedReportFailurePhase::AggregateValidation, kind, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_folds_compact_ranked_records() -> Result<(), Box<dyn std::error::Error>> {
        let output = fixture_output(1, 0, true);
        let document = parse_ranked_report_output(output.as_bytes())?;
        assert_eq!(document.metadata.direct_sample_count, 1);
        assert_eq!(document.semantics.len(), 1);
        assert_eq!(document.semantics[0].inclusive_sample_count, 1);
        assert_eq!(document.functions.len(), 2);
        assert_eq!(document.functions[0].function_id, 10);
        assert_eq!(document.functions[0].self_sample_count, 0);
        assert_eq!(document.functions[0].inclusive_sample_count, 1);
        assert_eq!(document.functions[1].function_id, 11);
        assert_eq!(document.functions[1].self_sample_count, 1);

        let mismatch = parse_ranked_report_output(fixture_output(2, 0, true).as_bytes())
            .expect_err("mismatched official count should fail");
        assert_eq!(
            mismatch.phase(),
            RankedReportFailurePhase::AggregateValidation
        );
        assert_eq!(mismatch.kind(), "official_count_mismatch");

        let audit = parse_ranked_report_output(fixture_output(1, 1, true).as_bytes())
            .expect_err("failed query audit should fail");
        assert_eq!(audit.kind(), "audit_failed");

        let incomplete = parse_ranked_report_output(fixture_output(1, 0, false).as_bytes())
            .expect_err("incomplete capture should fail");
        assert_eq!(incomplete.phase(), RankedReportFailurePhase::Health);
        assert_eq!(incomplete.kind(), "incomplete_capture");

        let malformed = parse_ranked_report_output(b"\"record_hex\"\n\"xyz\"\n")
            .expect_err("invalid hex should fail");
        assert_eq!(malformed.phase(), RankedReportFailurePhase::Query);
        assert_eq!(malformed.kind(), "malformed_result");
        Ok(())
    }

    #[test]
    #[ignore = "requires trace_processor_shell and a real raw trace"]
    fn loads_a_real_raw_trace_when_requested() -> Result<(), Box<dyn std::error::Error>> {
        let trace = std::env::var_os("DELTA_FUNNEL_TEST_PERFETTO_TRACE")
            .ok_or("DELTA_FUNNEL_TEST_PERFETTO_TRACE is not set")?;
        let document = load_ranked_profile(Path::new(&trace))?;
        assert!(!document.semantics.is_empty());
        assert_eq!(
            document.metadata.direct_sample_count,
            document
                .semantics
                .iter()
                .map(|semantic| semantic.direct_sample_count)
                .sum::<i64>()
        );
        Ok(())
    }

    fn fixture_output(
        official_root_count: i64,
        audit_error_count: i64,
        capture_complete: bool,
    ) -> String {
        let records = [
            serde_json::json!({
                "record_kind": "metadata",
                "record": {
                    "capture_complete": capture_complete,
                    "semantic_complete": capture_complete,
                    "schema_version": 1,
                    "sample_frequency_hz": 1000,
                    "exact_time_unit": "nanoseconds",
                    "sample_unit": "samples",
                    "eligible_sample_count": 1,
                    "direct_sample_count": 1,
                    "ambiguous_sample_count": 0,
                    "unattributed_sample_count": 0,
                    "audit_error_count": audit_error_count,
                }
            }),
            serde_json::json!({
                "record_kind": "semantic",
                "record": {
                    "semantic_id": 1,
                    "parent_semantic_id": null,
                    "operation_id": 1,
                    "name": "Delta Funnel preview",
                    "semantic_kind": "operation",
                    "operation_kind": "preview",
                    "stage_category": null,
                    "stage_name": null,
                    "activity": null,
                    "start_ns": 10,
                    "end_ns": 20,
                    "duration_ns": 10,
                    "time_semantics": "wall_clock",
                    "result": "ok",
                    "is_complete": true,
                    "query_execution_id": null,
                    "query_scope": null,
                    "query_owner": null,
                    "worker_lane_id": null,
                    "worker_kind": null,
                    "node_id": null,
                    "parent_node_id": null,
                    "operator_partition": null,
                    "execution_stream_id": null,
                    "stage_owner_id": null,
                    "direct_sample_count": 1,
                    "inclusive_sample_count": 0,
                }
            }),
            frame_record(10, None, 0, official_root_count),
            frame_record(11, Some(10), 1, 1),
            serde_json::json!({
                "record_kind": "function_self",
                "record": {
                    "semantic_id": 1,
                    "function_id": 11,
                    "self_sample_count": 1,
                }
            }),
        ];
        let mut output = String::from("\n\"record_hex\"\n");
        for record in records {
            output.push('"');
            for byte in record.to_string().as_bytes() {
                use std::fmt::Write as _;
                write!(output, "{byte:02X}").expect("writing to a string should succeed");
            }
            output.push_str("\"\n");
        }
        output
    }

    fn frame_record(
        function_id: i64,
        parent_function_id: Option<i64>,
        self_count: i64,
        inclusive_count: i64,
    ) -> serde_json::Value {
        serde_json::json!({
            "record_kind": "frame",
            "record": {
                "function_id": function_id,
                "parent_function_id": parent_function_id,
                "name": format!("function_{function_id}"),
                "module_name": "delta-funnel",
                "source_file": "src/lib.rs",
                "line_number": function_id,
                "official_self_sample_count": self_count,
                "official_inclusive_sample_count": inclusive_count,
            }
        })
    }
}
