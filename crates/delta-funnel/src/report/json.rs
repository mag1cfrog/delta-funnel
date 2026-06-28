use serde_json::{Value, json};

use crate::{
    FileCount, OutputStatus, PhaseStatus, PhaseTimingReport, ReportReasonCode, RowCount,
    ValidationStatus, WorkflowStatus,
};

impl RowCount {
    /// Returns a JSON-compatible shape that preserves count kind and value.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        count_value(self.kind().as_str(), self.value())
    }
}

impl FileCount {
    /// Returns a JSON-compatible shape that preserves count kind and value.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        count_value(self.kind().as_str(), self.value())
    }
}

impl ValidationStatus {
    /// Returns a JSON-compatible shape that preserves status kind and reason.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        status_value(self.kind().as_str(), self.reason())
    }
}

impl PhaseStatus {
    /// Returns a JSON-compatible shape that preserves phase status kind and reason.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        status_value(self.kind().as_str(), self.reason())
    }
}

impl OutputStatus {
    /// Returns a JSON-compatible shape that preserves output status semantics.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        json!({
            "kind": self.kind().as_str(),
            "reason": reason_value(self.reason()),
            "validation": self.validation().map(ValidationStatus::to_json_value),
        })
    }
}

impl WorkflowStatus {
    /// Returns a JSON-compatible shape that preserves workflow status semantics.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        status_value(self.kind().as_str(), self.reason())
    }
}

impl PhaseTimingReport {
    /// Returns a JSON-compatible shape with structured status and elapsed time.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "phase_name": self.phase_name(),
            "status": self.status().to_json_value(),
            "elapsed_micros": self.elapsed_micros(),
        })
    }
}

fn count_value(kind: &str, value: Option<u64>) -> Value {
    json!({
        "kind": kind,
        "value": value,
    })
}

fn status_value(kind: &str, reason: Option<ReportReasonCode>) -> Value {
    json!({
        "kind": kind,
        "reason": reason_value(reason),
    })
}

fn reason_value(reason: Option<ReportReasonCode>) -> Option<&'static str> {
    reason.map(ReportReasonCode::as_str)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn row_count_json_preserves_kind_and_value() {
        assert_eq!(
            RowCount::exact(3).to_json_value(),
            json!({"kind": "exact", "value": 3})
        );
        assert_eq!(
            RowCount::estimated(5).to_json_value(),
            json!({"kind": "estimated", "value": 5})
        );
        assert_eq!(
            RowCount::partial(2).to_json_value(),
            json!({"kind": "partial", "value": 2})
        );
        assert_eq!(
            RowCount::unavailable().to_json_value(),
            json!({"kind": "unavailable", "value": null})
        );
    }

    #[test]
    fn file_count_json_preserves_non_numeric_kinds() {
        assert_eq!(
            FileCount::skipped().to_json_value(),
            json!({"kind": "skipped", "value": null})
        );
        assert_eq!(
            FileCount::not_executed().to_json_value(),
            json!({"kind": "not_executed", "value": null})
        );
    }

    #[test]
    fn status_json_preserves_stable_kind_and_reason_strings() {
        assert_eq!(
            ValidationStatus::skipped(ReportReasonCode::DryRun).to_json_value(),
            json!({"kind": "skipped", "reason": "dry_run"})
        );
        assert_eq!(
            PhaseStatus::not_started(ReportReasonCode::NotExecuted).to_json_value(),
            json!({"kind": "not_started", "reason": "not_executed"})
        );
        assert_eq!(
            WorkflowStatus::no_op(ReportReasonCode::NotExecuted).to_json_value(),
            json!({"kind": "no_op", "reason": "not_executed"})
        );
    }

    #[test]
    fn output_status_json_preserves_nested_validation_status() {
        assert_eq!(
            OutputStatus::validation_failed(ValidationStatus::required_but_failed(
                ReportReasonCode::MissingExactOutputRows
            ))
            .to_json_value(),
            json!({
                "kind": "validation_failed",
                "reason": null,
                "validation": {
                    "kind": "required_but_failed",
                    "reason": "missing_exact_output_rows"
                }
            })
        );
    }

    #[test]
    fn phase_timing_json_is_json_round_trippable() -> Result<(), serde_json::Error> {
        let value =
            PhaseTimingReport::completed("load_sources", Duration::from_micros(42)).to_json_value();

        assert_eq!(
            value,
            json!({
                "phase_name": "load_sources",
                "status": {"kind": "completed", "reason": null},
                "elapsed_micros": 42
            })
        );
        serde_json::from_str::<Value>(&serde_json::to_string(&value)?).map(|_| ())
    }
}
