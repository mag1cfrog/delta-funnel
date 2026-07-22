use std::fs;
use std::path::Path;

use super::report_cli::{RankedReportFailure, RankedReportFailurePhase};
use super::report_trace_processor::run_trace_processor_query;

const CAPTURE_HEALTH_SQL: &str = include_str!("sql/capture_health.sql");

#[doc(hidden)]
pub fn validate_ranked_report_capture(input: &Path) -> Result<(), RankedReportFailure> {
    let saved_file_bytes = fs::metadata(input)
        .map_err(|_| {
            RankedReportFailure::new(
                RankedReportFailurePhase::Input,
                "unreadable",
                "input trace metadata could not be read",
            )
        })?
        .len();
    let sql = format!(
        "CREATE PERFETTO TABLE delta_funnel_capture_health_input AS\n\
         SELECT NULL AS configured_file_cap_bytes,\n\
         {saved_file_bytes} AS saved_file_bytes;\n\
         {CAPTURE_HEALTH_SQL}"
    );
    let output = run_trace_processor_query(input, sql.as_bytes())?;
    validate_health_output(&output)
}

fn validate_health_output(output: &[u8]) -> Result<(), RankedReportFailure> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(output);
    let headers = reader.headers().map_err(|_| malformed_health_output())?;
    let capture_complete = headers
        .iter()
        .position(|header| header == "capture_complete")
        .ok_or_else(malformed_health_output)?;
    let semantic_complete = headers
        .iter()
        .position(|header| header == "semantic_complete")
        .ok_or_else(malformed_health_output)?;
    let mut records = reader.records();
    let record = records
        .next()
        .ok_or_else(malformed_health_output)?
        .map_err(|_| malformed_health_output())?;
    let is_complete = parse_bool(record.get(capture_complete))?;
    let semantics_are_complete = parse_bool(record.get(semantic_complete))?;
    if records.next().is_some() {
        return Err(malformed_health_output());
    }
    if !is_complete || !semantics_are_complete {
        return Err(RankedReportFailure::new(
            RankedReportFailurePhase::Health,
            "incomplete_capture",
            "input trace did not pass required capture health checks",
        ));
    }
    Ok(())
}

fn parse_bool(value: Option<&str>) -> Result<bool, RankedReportFailure> {
    match value {
        Some("0") => Ok(false),
        Some("1") => Ok(true),
        _ => Err(malformed_health_output()),
    }
}

fn malformed_health_output() -> RankedReportFailure {
    RankedReportFailure::new(
        RankedReportFailurePhase::Query,
        "malformed_result",
        "capture health query returned an unexpected result",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_one_complete_health_row() {
        validate_health_output(b"\n\"capture_complete\",\"semantic_complete\"\n1,1\n")
            .expect("complete health output should pass");

        let incomplete = validate_health_output(b"capture_complete,semantic_complete\n0,1\n")
            .expect_err("incomplete capture should fail");
        assert_eq!(incomplete.phase(), RankedReportFailurePhase::Health);
        assert_eq!(incomplete.kind(), "incomplete_capture");

        for malformed in [
            b"wrong,semantic_complete\n1,1\n".as_slice(),
            b"capture_complete,semantic_complete\nyes,1\n".as_slice(),
            b"capture_complete,semantic_complete\n1,1\n1,1\n".as_slice(),
        ] {
            let error = validate_health_output(malformed)
                .expect_err("malformed capture health output should fail");
            assert_eq!(error.phase(), RankedReportFailurePhase::Query);
            assert_eq!(error.kind(), "malformed_result");
        }
    }
}
