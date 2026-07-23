use std::fs;
use std::path::Path;

use super::report_cli::{RankedReportFailure, RankedReportFailurePhase};

pub(super) const CAPTURE_HEALTH_SQL: &str = include_str!("sql/capture_health.sql");

pub(super) fn capture_health_input_sql(input: &Path) -> Result<String, RankedReportFailure> {
    let saved_file_bytes = fs::metadata(input)
        .map_err(|_| {
            RankedReportFailure::new(
                RankedReportFailurePhase::Input,
                "unreadable",
                "input trace metadata could not be read",
            )
        })?
        .len();
    Ok(format!(
        "CREATE PERFETTO TABLE delta_funnel_capture_health_input AS\n\
         SELECT NULL AS configured_file_cap_bytes,\n\
         {saved_file_bytes} AS saved_file_bytes;"
    ))
}

pub(super) fn validate_capture_health(
    is_complete: bool,
    semantics_are_complete: bool,
) -> Result<(), RankedReportFailure> {
    if !is_complete || !semantics_are_complete {
        return Err(RankedReportFailure::new(
            RankedReportFailurePhase::Health,
            "incomplete_capture",
            "input trace did not pass required capture health checks",
        ));
    }
    Ok(())
}
