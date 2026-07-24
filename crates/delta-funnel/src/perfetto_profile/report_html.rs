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
:root {
  color-scheme: light dark;
  font: 15px system-ui, sans-serif;
}
body {
  background: Canvas;
  color: CanvasText;
  margin: 0;
}
main {
  margin: 0 auto;
  max-width: 100rem;
  padding: 2rem;
}
h1, h2 { margin: 0 0 .5rem; }
p { line-height: 1.5; margin: .5rem 0 1rem; max-width: 80rem; }
.summary {
  display: grid;
  gap: .75rem;
  grid-template-columns: repeat(auto-fit, minmax(10rem, 1fr));
  margin: 1.5rem 0 2rem;
}
.summary-item {
  border: 1px solid GrayText;
  border-radius: .4rem;
  padding: .75rem;
}
.summary-label { display: block; font-size: .8rem; }
.summary-value { display: block; font-size: 1.3rem; font-variant-numeric: tabular-nums; }
.table-wrap { overflow-x: auto; }
table { border-collapse: collapse; min-width: 58rem; width: 100%; }
th, td { border-bottom: 1px solid GrayText; padding: .65rem; text-align: left; }
th { background: color-mix(in srgb, CanvasText 8%, Canvas); }
tbody tr:hover { background: color-mix(in srgb, Highlight 12%, Canvas); }
.name { font-weight: 650; }
.detail { display: block; font-size: .8rem; margin-top: .2rem; }
.number { font-variant-numeric: tabular-nums; text-align: right; white-space: nowrap; }
.empty { border: 1px dashed GrayText; padding: 1rem; }
.help { margin-top: 2rem; max-width: 80rem; }
.help summary { cursor: pointer; font-weight: 650; }
@media (max-width: 40rem) {
  main { padding: 1rem; }
}
</style>
</head>
<body>
<main>
<h1>Delta Funnel profile report</h1>
<p>Semantic durations are exact wall-clock or lifecycle measurements. Function metrics are sampled on-CPU observations, not exact elapsed time.</p>
<div id="summary" class="summary" role="status" aria-live="polite"></div>
<section aria-labelledby="operations-heading">
<h2 id="operations-heading">Operations</h2>
<p>Operation roots are ranked by exact duration. CPU columns contain sampled observations.</p>
<div class="table-wrap">
<table role="treegrid" aria-label="Ranked operations">
<thead><tr>
<th scope="col">Operation</th>
<th scope="col">State and time basis</th>
<th scope="col" class="number">Exact duration</th>
<th scope="col" class="number">Operation wall %</th>
<th scope="col" class="number">Direct CPU samples</th>
<th scope="col" class="number">Inclusive CPU samples</th>
</tr></thead>
<tbody id="operations"></tbody>
</table>
</div>
<p id="operations-empty" class="empty" hidden>No operation records are available.</p>
</section>
<details class="help">
<summary>How to read these metrics</summary>
<p>Exact duration is measured wall-clock or lifecycle time. Parallel semantic children may overlap and are not additive. Direct CPU samples belong to one semantic node. Inclusive CPU samples also include its semantic descendants. Sampling observes on-CPU work and does not prove why a thread was off-CPU.</p>
</details>
</main>
<script id="profile-data" type="application/json">"#;

const HTML_SUFFIX: &str = r#"</script>
<script>
const profile = JSON.parse(document.getElementById("profile-data").textContent);
const summary = document.getElementById("summary");
const addSummary = (label, value) => {
  const item = document.createElement("div");
  item.className = "summary-item";
  const itemLabel = document.createElement("span");
  itemLabel.className = "summary-label";
  itemLabel.textContent = label;
  const itemValue = document.createElement("strong");
  itemValue.className = "summary-value";
  itemValue.textContent = String(value);
  item.append(itemLabel, itemValue);
  summary.appendChild(item);
};
const formatDuration = value => {
  if (value === null) return "Incomplete";
  if (value >= 1e9) return `${(value / 1e9).toFixed(3)} s`;
  if (value >= 1e6) return `${(value / 1e6).toFixed(3)} ms`;
  if (value >= 1e3) return `${(value / 1e3).toFixed(3)} us`;
  return `${value} ns`;
};
const formatPercent = (value, total) =>
  total > 0 ? `${(value * 100 / total).toFixed(1)}%` : "N/A";
const addOperation = (body, operation) => {
  const row = document.createElement("tr");
  row.setAttribute("aria-level", "1");
  const values = [
    operation.name,
    operation.is_complete ? (operation.result || "Complete") : "Incomplete",
    formatDuration(operation.duration_ns),
    formatPercent(operation.duration_ns || 0, operation.duration_ns || 0),
    String(operation.direct_sample_count),
    String(operation.inclusive_sample_count)
  ];
  values.forEach((value, index) => {
    const cell = document.createElement("td");
    cell.textContent = value;
    if (index === 0) {
      cell.className = "name";
      const detail = document.createElement("span");
      detail.className = "detail";
      detail.textContent = operation.operation_kind || operation.semantic_kind;
      cell.appendChild(detail);
    } else if (index === 1) {
      const detail = document.createElement("span");
      detail.className = "detail";
      detail.textContent =
        operation.time_semantics === "wall_clock" ? "Exact wall clock" : "Lifecycle";
      cell.appendChild(detail);
    } else {
      cell.className = "number";
    }
    row.appendChild(cell);
  });
  body.appendChild(row);
};
addSummary("Sample frequency", `${profile.metadata.sample_frequency_hz} Hz`);
addSummary("Eligible CPU samples", profile.metadata.eligible_sample_count);
addSummary("Directly attributed", profile.metadata.direct_sample_count);
addSummary("Ambiguous", profile.metadata.ambiguous_sample_count);
addSummary("Unattributed", profile.metadata.unattributed_sample_count);
const operations = profile.semantics
  .filter(item => item.parent_semantic_id === null)
  .sort((left, right) =>
    (right.duration_ns || 0) - (left.duration_ns || 0) ||
    left.semantic_id - right.semantic_id
  );
const operationsBody = document.getElementById("operations");
operations.forEach(operation => addOperation(operationsBody, operation));
document.getElementById("operations-empty").hidden = operations.length !== 0;
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
        assert!(html.contains(r#"role="treegrid""#));
        assert!(html.contains(r#"aria-label="Ranked operations""#));
        assert!(html.contains(r#"id="operations-empty""#));
        assert!(!html.contains(r#"id="functions""#));
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
