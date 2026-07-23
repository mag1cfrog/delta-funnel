use std::fs;
use std::io::Write;
use std::path::Path;

use super::ranked_report::RankedProfileDocument;
use super::report_cli::{RankedReportFailure, RankedReportFailurePhase};

const HTML_PREFIX: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" href="data:,">
<title>Delta Funnel profile report</title>
<style>
body { color: #202124; font: 15px system-ui, sans-serif; margin: 2rem; }
h1, h2 { margin-bottom: .5rem; }
p { max-width: 80rem; }
table { border-collapse: collapse; margin-bottom: 2rem; width: 100%; }
th, td { border-bottom: 1px solid #dadce0; padding: .5rem; text-align: left; }
th { background: #f8f9fa; }
.number { font-variant-numeric: tabular-nums; text-align: right; }
</style>
</head>
<body>
<h1>Delta Funnel profile report</h1>
<p>Semantic durations are exact wall-clock or lifecycle measurements. Function metrics are sampled on-CPU observations, not exact elapsed time.</p>
<p id="summary"></p>
<h2>Operations</h2>
<table><thead><tr><th>Name</th><th>Time basis</th><th class="number">Duration (ns)</th><th class="number">Direct CPU samples</th><th class="number">Inclusive CPU samples</th></tr></thead><tbody id="operations"></tbody></table>
<h2>Function callsites</h2>
<p>Top 20 callsites by sampled inclusive CPU across the report.</p>
<table><thead><tr><th>Function</th><th class="number">Semantic ID</th><th class="number">Self CPU samples</th><th class="number">Inclusive CPU samples</th></tr></thead><tbody id="functions"></tbody></table>
<script id="profile-data" type="application/json">"#;

const HTML_SUFFIX: &str = r#"</script>
<script>
const profile = JSON.parse(document.getElementById("profile-data").textContent);
const addRow = (body, values, textColumns = 1) => {
  const row = document.createElement("tr");
  values.forEach((value, index) => {
    const cell = document.createElement("td");
    cell.textContent = value;
    if (index >= textColumns) cell.className = "number";
    row.appendChild(cell);
  });
  body.appendChild(row);
};
document.getElementById("summary").textContent =
  `${profile.semantics.length} semantic records and ${profile.functions.length} function callsites. ` +
  `At ${profile.metadata.sample_frequency_hz} Hz: ${profile.metadata.eligible_sample_count} eligible, ` +
  `${profile.metadata.direct_sample_count} directly attributed, ${profile.metadata.ambiguous_sample_count} ambiguous, ` +
  `${profile.metadata.unattributed_sample_count} unattributed CPU samples.`;
profile.semantics
  .filter(item => item.parent_semantic_id === null)
  .sort((left, right) => (right.duration_ns || 0) - (left.duration_ns || 0) || left.semantic_id - right.semantic_id)
  .forEach(item => addRow(document.getElementById("operations"), [
    item.name,
    item.time_semantics,
    item.duration_ns === null ? "incomplete" : String(item.duration_ns),
    String(item.direct_sample_count),
    String(item.inclusive_sample_count)
  ], 2));
profile.functions
  .slice()
  .sort((left, right) => right.inclusive_sample_count - left.inclusive_sample_count || left.semantic_id - right.semantic_id || left.function_id - right.function_id)
  .slice(0, 20)
  .forEach(item => addRow(document.getElementById("functions"), [
    item.name,
    String(item.semantic_id),
    String(item.self_sample_count),
    String(item.inclusive_sample_count)
  ]));
</script>
</body>
</html>
"#;

pub(super) fn render_ranked_profile_html(
    document: &RankedProfileDocument,
) -> Result<String, RankedReportFailure> {
    let json = serde_json::to_string(document).map_err(|_| {
        RankedReportFailure::new(
            RankedReportFailurePhase::Serialization,
            "json_failed",
            "ranked profile data could not be serialized",
        )
    })?;
    let mut html = String::with_capacity(HTML_PREFIX.len() + json.len() + HTML_SUFFIX.len());
    html.push_str(HTML_PREFIX);
    push_html_safe_json(&mut html, &json);
    html.push_str(HTML_SUFFIX);
    Ok(html)
}

pub(super) fn write_ranked_profile_html(
    output: &Path,
    html: &str,
) -> Result<(), RankedReportFailure> {
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|_| {
        output_failure(
            "create_parent_failed",
            "report output directory could not be created",
        )
    })?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|_| {
        output_failure(
            "create_temporary_failed",
            "temporary report file could not be created",
        )
    })?;
    temporary.write_all(html.as_bytes()).map_err(|_| {
        output_failure("write_failed", "temporary report file could not be written")
    })?;
    temporary
        .persist(output)
        .map_err(|_| output_failure("persist_failed", "completed report could not be persisted"))?;
    Ok(())
}

fn output_failure(kind: &'static str, message: &'static str) -> RankedReportFailure {
    RankedReportFailure::new(RankedReportFailurePhase::Output, kind, message)
}

fn push_html_safe_json(output: &mut String, json: &str) {
    for character in json.chars() {
        match character {
            '<' => output.push_str("\\u003c"),
            '>' => output.push_str("\\u003e"),
            '&' => output.push_str("\\u0026"),
            '\u{2028}' => output.push_str("\\u2028"),
            '\u{2029}' => output.push_str("\\u2029"),
            _ => output.push(character),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::ranked_report::{RankedFunction, RankedProfileMetadata, RankedSemantic};
    use super::*;

    #[test]
    fn renders_a_safe_self_contained_report() -> Result<(), Box<dyn std::error::Error>> {
        let dangerous = "</script><img src=x onerror=alert(1)> & \"quoted\" \u{2028} \u{2029} 函数";
        let document = RankedProfileDocument {
            metadata: RankedProfileMetadata {
                schema_version: 1,
                sample_frequency_hz: 1000,
                exact_time_unit: "nanoseconds".to_owned(),
                sample_unit: "samples".to_owned(),
                eligible_sample_count: 1,
                direct_sample_count: 1,
                ambiguous_sample_count: 0,
                unattributed_sample_count: 0,
            },
            semantics: vec![RankedSemantic {
                semantic_id: 1,
                parent_semantic_id: None,
                operation_id: 1,
                name: dangerous.to_owned(),
                semantic_kind: "operation".to_owned(),
                operation_kind: Some("preview".to_owned()),
                stage_category: None,
                stage_name: None,
                activity: None,
                start_ns: 0,
                end_ns: Some(1),
                duration_ns: Some(1),
                time_semantics: "wall_clock".to_owned(),
                result: Some("ok".to_owned()),
                is_complete: true,
                query_execution_id: None,
                query_scope: None,
                query_owner: None,
                worker_lane_id: None,
                worker_kind: None,
                node_id: None,
                parent_node_id: None,
                operator_partition: None,
                execution_stream_id: None,
                stage_owner_id: None,
                direct_sample_count: 1,
                inclusive_sample_count: 1,
            }],
            functions: vec![RankedFunction {
                semantic_id: 1,
                function_id: 1,
                parent_function_id: None,
                name: dangerous.to_owned(),
                module_name: None,
                source_file: None,
                line_number: None,
                self_sample_count: 1,
                inclusive_sample_count: 1,
            }],
        };

        let html = render_ranked_profile_html(&document)?;
        let embedded = html
            .strip_prefix(HTML_PREFIX)
            .and_then(|remainder| remainder.strip_suffix(HTML_SUFFIX))
            .ok_or("embedded profile data is missing")?;
        assert!(!embedded.contains(['<', '>', '&', '\u{2028}', '\u{2029}']));
        assert!(!embedded.to_ascii_lowercase().contains("</script"));
        assert!(!html.contains(dangerous));
        assert!(!html.contains("https://"));
        assert!(!html.contains("http://"));
        let decoded: serde_json::Value = serde_json::from_str(embedded)?;
        assert_eq!(decoded["semantics"][0]["name"], dangerous);
        assert_eq!(decoded["functions"][0]["name"], dangerous);
        Ok(())
    }

    #[test]
    fn atomically_replaces_output_and_preserves_it_on_failure() -> std::io::Result<()> {
        let directory = tempfile::tempdir()?;
        let output = directory.path().join("nested/report.profile.html");
        write_ranked_profile_html(&output, "first complete report")
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        assert_eq!(fs::read_to_string(&output)?, "first complete report");
        write_ranked_profile_html(&output, "complete report")
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        assert_eq!(fs::read_to_string(&output)?, "complete report");
        assert_eq!(
            fs::read_dir(output.parent().expect("output has a parent"))?.count(),
            1
        );

        let blocked_output = directory.path().join("existing-output");
        fs::create_dir(&blocked_output)?;
        fs::write(blocked_output.join("keep-me"), "unchanged")?;
        let error = write_ranked_profile_html(&blocked_output, "partial report")
            .expect_err("a report cannot replace an existing directory");
        assert_eq!(error.phase(), RankedReportFailurePhase::Output);
        assert_eq!(error.kind(), "persist_failed");
        assert_eq!(
            fs::read_to_string(blocked_output.join("keep-me"))?,
            "unchanged"
        );
        Ok(())
    }
}
