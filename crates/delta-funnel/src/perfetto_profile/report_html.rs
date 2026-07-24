use std::fs;
use std::io::Write;
use std::path::Path;

use super::ranked_report::RankedProfileDocument;
use super::report_cli::{RankedReportFailure, RankedReportFailurePhase};

const HTML_DOCUMENT_PREFIX: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" href="data:,">
<title>Delta Funnel profile report</title>
<style>
"#;

const REPORT_STYLE: &str = include_str!("report_html.css");

const HTML_PROFILE_PREFIX: &str = r#"</style>
</head>
<body>
<main>
<h1>Delta Funnel profile report</h1>
<p>Semantic durations are exact wall-clock or lifecycle measurements. Function metrics are sampled on-CPU observations, not exact elapsed time.</p>
<div id="summary" class="summary" role="status" aria-live="polite"></div>
<section aria-labelledby="operations-heading">
<h2 id="operations-heading">Operations</h2>
<p>Operation roots are ranked by exact duration. Expand a row to follow exact semantic children into sampled native callsites.</p>
<div class="controls">
<label class="filter-label" for="profile-filter"><span>Filter profile</span>
<input id="profile-filter" type="search" maxlength="200" autocomplete="off" placeholder="Name, symbol, module, or source file"></label>
<button id="clear-filter" class="clear-filter" type="button" disabled>Clear filter</button>
<button id="previous-filter-page" class="filter-page" type="button" hidden>Previous matches</button>
<button id="next-filter-page" class="filter-page" type="button" hidden>Next matches</button>
<output id="filter-status" class="filter-status" for="profile-filter" role="status" aria-live="polite"></output>
</div>
<div class="controls tree-controls">
<button id="expand-subtree" class="tree-action" type="button">Expand selected subtree</button>
<button id="collapse-subtree" class="tree-action" type="button">Collapse selected subtree</button>
<output id="tree-status" class="filter-status" role="status" aria-live="polite"></output>
</div>
<div class="table-wrap">
<table role="treegrid" aria-label="Ranked operations">
<thead><tr>
<th scope="col" data-sort-column="name"><button class="sort" type="button" data-sort="name" data-label="Operation">Operation</button></th>
<th scope="col">State and time basis</th>
<th scope="col" class="number" data-sort-column="duration"><button class="sort" type="button" data-sort="duration" data-label="Exact duration">Exact duration</button></th>
<th scope="col" class="number">Context %</th>
<th scope="col" class="number" data-sort-column="direct"><button class="sort" type="button" data-sort="direct" data-label="Direct/self CPU samples">Direct/self CPU samples</button></th>
<th scope="col" class="number" data-sort-column="inclusive"><button class="sort" type="button" data-sort="inclusive" data-label="Inclusive CPU samples">Inclusive CPU samples</button></th>
</tr></thead>
<tbody id="operations"></tbody>
</table>
</div>
<output id="render-limit-status" class="render-limit-status" role="status" aria-live="polite"></output>
<p id="operations-empty" class="empty" hidden>No operation records are available.</p>
</section>
<details class="help">
<summary>How to read these metrics</summary>
<p>Exact duration is measured wall-clock or lifecycle time. Parallel semantic children may overlap and are not additive. Direct CPU samples belong to one semantic node. Inclusive CPU samples also include its semantic descendants. Sampling observes on-CPU work and does not prove why a thread was off-CPU.</p>
<p>Self CPU samples were observed directly in one function. Inclusive CPU samples also include sampled callees. Function percentages use direct samples from the owning semantic node as their denominator. Sample counts are statistical observations, not exact function milliseconds.</p>
<p>Eligible samples are the on-CPU samples considered for attribution. Directly attributed samples have one semantic owner. Ambiguous samples have more than one possible owner. Unattributed samples have no semantic owner. Linux sampling does not measure off-CPU waiting time.</p>
<p>Select a row to use the subtree controls. Arrow Up and Arrow Down move between visible rows. Arrow Right expands a row or moves to its first child. Arrow Left collapses a row or moves to its parent. Sibling groups are paged 100 rows at a time. Bulk subtree actions and the visible table are limited to 1000 rows.</p>
</details>
</main>
<script id="profile-data" type="application/json">"#;

const HTML_SCRIPT_PREFIX: &str = r#"</script>
<script>
"#;

const REPORT_SCRIPT: &str = include_str!("report_html.js");

const HTML_SUFFIX: &str = r#"</script>
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
    let mut html = String::with_capacity(
        HTML_DOCUMENT_PREFIX.len()
            + REPORT_STYLE.len()
            + HTML_PROFILE_PREFIX.len()
            + json.len()
            + HTML_SCRIPT_PREFIX.len()
            + REPORT_SCRIPT.len()
            + HTML_SUFFIX.len(),
    );
    html.push_str(HTML_DOCUMENT_PREFIX);
    html.push_str(REPORT_STYLE);
    html.push_str(HTML_PROFILE_PREFIX);
    push_html_safe_json(&mut html, &json);
    html.push_str(HTML_SCRIPT_PREFIX);
    html.push_str(REPORT_SCRIPT);
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

    fn metadata() -> RankedProfileMetadata {
        RankedProfileMetadata {
            schema_version: 1,
            sample_frequency_hz: 1000,
            exact_time_unit: "nanoseconds".to_owned(),
            sample_unit: "samples".to_owned(),
            eligible_sample_count: 0,
            direct_sample_count: 0,
            ambiguous_sample_count: 0,
            unattributed_sample_count: 0,
        }
    }

    fn semantic(
        semantic_id: i64,
        parent_semantic_id: Option<i64>,
        name: impl Into<String>,
    ) -> RankedSemantic {
        RankedSemantic {
            semantic_id,
            parent_semantic_id,
            operation_id: 1,
            name: name.into(),
            semantic_kind: if parent_semantic_id.is_none() {
                "operation"
            } else {
                "stage"
            }
            .to_owned(),
            operation_kind: parent_semantic_id.is_none().then(|| "write".to_owned()),
            stage_category: None,
            stage_name: None,
            activity: None,
            start_ns: 0,
            end_ns: Some(1_000_000),
            duration_ns: Some(1_000_000),
            time_semantics: "wall_clock".to_owned(),
            result: Some("completed".to_owned()),
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
            direct_sample_count: 0,
            inclusive_sample_count: 0,
        }
    }

    fn function(
        function_id: i64,
        parent_function_id: Option<i64>,
        name: impl Into<String>,
    ) -> RankedFunction {
        RankedFunction {
            semantic_id: 1,
            function_id,
            parent_function_id,
            name: name.into(),
            module_name: Some("delta_funnel".to_owned()),
            source_file: Some("src/lib.rs".to_owned()),
            line_number: Some(42),
            self_sample_count: 0,
            inclusive_sample_count: 0,
        }
    }

    fn embedded_json(html: &str) -> Result<&str, &'static str> {
        html.split_once(HTML_PROFILE_PREFIX)
            .and_then(|(_, remainder)| remainder.split_once(HTML_SCRIPT_PREFIX))
            .map(|(json, _)| json)
            .ok_or("embedded profile data is missing")
    }

    #[test]
    fn renders_a_safe_self_contained_report() -> Result<(), Box<dyn std::error::Error>> {
        let dangerous = "</script><img src=x onerror=alert(1)> & \"quoted\" \u{2028} \u{2029} 函数";
        let mut semantic = semantic(1, None, dangerous);
        semantic.operation_kind = Some("preview".to_owned());
        semantic.end_ns = Some(1);
        semantic.duration_ns = Some(1);
        semantic.result = Some("ok".to_owned());
        semantic.direct_sample_count = 1;
        semantic.inclusive_sample_count = 1;
        let mut function = function(1, None, dangerous);
        function.module_name = None;
        function.source_file = None;
        function.line_number = None;
        function.self_sample_count = 1;
        function.inclusive_sample_count = 1;
        let mut profile_metadata = metadata();
        profile_metadata.eligible_sample_count = 1;
        profile_metadata.direct_sample_count = 1;
        let document = RankedProfileDocument {
            metadata: profile_metadata,
            semantics: vec![semantic],
            functions: vec![function],
        };

        let html = render_ranked_profile_html(&document)?;
        let embedded = embedded_json(&html)?;
        assert!(!embedded.contains(['<', '>', '&', '\u{2028}', '\u{2029}']));
        assert!(!embedded.to_ascii_lowercase().contains("</script"));
        assert!(!html.contains(dangerous));
        assert!(!html.contains("https://"));
        assert!(!html.contains("http://"));
        assert!(html.contains(r#"role="treegrid""#));
        assert!(html.contains(r#"aria-label="Ranked operations""#));
        assert!(html.contains(r#"id="operations-empty""#));
        assert!(html.contains(r#"id="profile-filter""#));
        assert!(html.contains(r#"maxlength="200""#));
        assert!(html.contains(r#"id="previous-filter-page""#));
        assert!(html.contains(r#"id="next-filter-page""#));
        assert!(html.contains(r#"id="expand-subtree""#));
        assert!(html.contains(r#"id="collapse-subtree""#));
        assert!(html.contains(r#"id="render-limit-status""#));
        assert!(html.contains(r#"data-sort="duration""#));
        assert!(!html.contains(r#"id="functions""#));
        assert!(html.contains(r#"button.setAttribute("aria-expanded""#));
        assert!(html.contains(r#""aria-selected","#));
        assert!(html.contains("const maximumBulkSubtreeRows = 1000"));
        assert!(html.contains("const maximumRenderedRows = 1000"));
        assert!(html.contains("const maximumIndentDepth = 32"));
        assert!(html.contains("const siblingPageSize = 100"));
        assert!(html.contains("const containsFilter = value =>"));
        assert!(html.contains("operationsBody.replaceChildren(fragment)"));
        assert!(!html.contains("innerHTML"));
        let decoded: serde_json::Value = serde_json::from_str(embedded)?;
        assert_eq!(decoded["semantics"][0]["name"], dangerous);
        assert_eq!(decoded["functions"][0]["name"], dangerous);
        Ok(())
    }

    #[test]
    fn renders_a_deterministic_large_tree_fixture() -> Result<(), Box<dyn std::error::Error>> {
        let mut semantics = vec![semantic(1, None, "large operation")];
        for semantic_id in 2..=257 {
            semantics.push(semantic(
                semantic_id,
                Some(1),
                format!("overlapping sibling {semantic_id}"),
            ));
        }
        let mut parent_semantic_id = 1;
        for semantic_id in 258..=385 {
            semantics.push(semantic(
                semantic_id,
                Some(parent_semantic_id),
                format!("deep semantic {semantic_id}"),
            ));
            parent_semantic_id = semantic_id;
        }
        let mut incomplete = semantic(386, Some(1), "incomplete semantic");
        incomplete.end_ns = None;
        incomplete.duration_ns = None;
        incomplete.result = None;
        incomplete.is_complete = false;
        semantics.push(incomplete);

        let mut functions = vec![function(1, None, "[native stack unavailable]")];
        for function_id in 2..=5_001 {
            functions.push(function(
                function_id,
                None,
                if function_id == 2 {
                    "x".repeat(512)
                } else {
                    format!("wide function {function_id}")
                },
            ));
        }
        let mut parent_function_id = 1;
        for function_id in 5_002..=5_129 {
            functions.push(function(
                function_id,
                Some(parent_function_id),
                format!("deep function {function_id}"),
            ));
            parent_function_id = function_id;
        }
        let mut profile_metadata = metadata();
        profile_metadata.eligible_sample_count = 2;
        profile_metadata.ambiguous_sample_count = 1;
        profile_metadata.unattributed_sample_count = 1;
        let document = RankedProfileDocument {
            metadata: profile_metadata,
            semantics,
            functions,
        };

        document.validate()?;
        let html = render_ranked_profile_html(&document)?;
        let decoded: serde_json::Value = serde_json::from_str(embedded_json(&html)?)?;
        assert_eq!(decoded["semantics"].as_array().map(Vec::len), Some(386));
        assert_eq!(decoded["functions"].as_array().map(Vec::len), Some(5_129));
        assert_eq!(
            decoded["functions"][0]["name"],
            "[native stack unavailable]"
        );
        assert_eq!(
            decoded["functions"][1]["name"].as_str().map(str::len),
            Some(512)
        );
        assert!(html.contains("operationsBody.replaceChildren(fragment)"));
        Ok(())
    }

    #[test]
    fn exercises_the_viewer_in_a_configured_browser() -> Result<(), Box<dyn std::error::Error>> {
        let Some(browser) = std::env::var_os("CHROME_BIN").filter(|value| !value.is_empty()) else {
            return Ok(());
        };

        let mut operation = semantic(1, None, "Root operation");
        operation.end_ns = Some(10_000_000);
        operation.duration_ns = Some(10_000_000);
        let mut zeta = semantic(2, Some(1), "Zeta phase");
        zeta.end_ns = Some(2_000_000);
        zeta.duration_ns = Some(2_000_000);
        let mut alpha = semantic(3, Some(1), "Alpha phase");
        alpha.end_ns = Some(1_000_000);
        alpha.duration_ns = Some(1_000_000);
        let mut semantics = vec![operation, zeta, alpha];
        for group in 0..10 {
            let group_id = 4 + group * 101;
            semantics.push(semantic(group_id, Some(1), format!("Group {group:02}")));
            for child in 1..=100 {
                semantics.push(semantic(
                    group_id + child,
                    Some(group_id),
                    format!("Group {group:02} child {child:03}"),
                ));
            }
        }
        let mut deep_parent_id = 3;
        for semantic_id in 2_000..2_040 {
            semantics.push(semantic(
                semantic_id,
                Some(deep_parent_id),
                format!("Deep semantic {semantic_id}"),
            ));
            deep_parent_id = semantic_id;
        }
        semantics.push(semantic(3_000, Some(1), "Distributed cases"));
        let mut distributed_id = 3_001;
        for case in 0..100 {
            let mut parent_id = 3_000;
            for depth in 0..10 {
                let name = if depth == 9 {
                    format!("distributed target {case:03}")
                } else {
                    format!("Distributed {case:03} context {depth:02}")
                };
                semantics.push(semantic(distributed_id, Some(parent_id), name));
                parent_id = distributed_id;
                distributed_id += 1;
            }
        }
        let mut oversized_parent_id = 3_000;
        for semantic_id in 5_000..6_000 {
            let name = if semantic_id == 5_999 {
                "oversized target".to_owned()
            } else {
                format!("Oversized context {semantic_id}")
            };
            semantics.push(semantic(semantic_id, Some(oversized_parent_id), name));
            oversized_parent_id = semantic_id;
        }
        let mut functions = (1..=101)
            .map(|function_id| {
                function(
                    function_id,
                    None,
                    format!("match function {function_id:03}"),
                )
            })
            .collect::<Vec<_>>();
        functions.push(function(102, Some(1), "nested target"));
        let document = RankedProfileDocument {
            metadata: metadata(),
            semantics,
            functions,
        };
        document.validate()?;

        let rendered = render_ranked_profile_html(&document)?;
        let mut html = rendered
            .strip_suffix(HTML_SUFFIX)
            .ok_or("report suffix is missing")?
            .to_owned();
        html.push_str(
            r#"</script>
<script>
(() => {
  const check = (condition, message) => {
    if (!condition) throw new Error(message);
  };
  try {
    check(operationsBody.rows.length === 1, "initial render was not lazy");
    const rootRow = operationsBody.rows[0];
    check(rootRow.getAttribute("aria-selected") === "true", "root was not selected");
    rootRow.focus();
    rootRow.dispatchEvent(new KeyboardEvent("keydown", {
      key: "ArrowRight",
      bubbles: true
    }));
    check(operationsBody.rows.length === 102, "high-fanout expansion was not paged");
    check(
      operationsBody.rows[0].getAttribute("aria-expanded") === "true",
      "expanded state was not exposed"
    );
    let pagination = operationsBody.querySelector(".pagination-row");
    check(pagination !== null, "sibling pagination was not rendered");
    check(
      pagination.textContent.includes("1-100 of 114"),
      "first sibling page status was incorrect"
    );
    check(
      pagination.getAttribute("aria-label") === "Root operation children pagination",
      "pagination row did not identify its sibling group"
    );
    check(
      pagination.querySelectorAll("button")[1].getAttribute("aria-label") ===
        "Next page for Root operation children",
      "pagination button did not identify its sibling group"
    );
    const lastFirstPageRow = operationsBody.rows[100];
    lastFirstPageRow.focus();
    lastFirstPageRow.dispatchEvent(new KeyboardEvent("keydown", {
      key: "ArrowDown",
      bubbles: true
    }));
    check(
      document.activeElement === pagination && pagination.tabIndex === 0,
      "Arrow Down did not reach sibling pagination"
    );
    pagination.dispatchEvent(new KeyboardEvent("keydown", {
      key: "ArrowUp",
      bubbles: true
    }));
    check(
      document.activeElement === lastFirstPageRow,
      "Arrow Up did not leave sibling pagination"
    );
    pagination.querySelectorAll("button")[1].click();
    check(operationsBody.rows.length === 16, "second sibling page was incorrect");
    pagination = operationsBody.querySelector(".pagination-row");
    check(
      pagination.textContent.includes("101-114 of 114"),
      "second sibling page status was incorrect"
    );
    pagination.querySelector("button").click();
    check(operationsBody.rows.length === 102, "previous sibling page was incorrect");

    document.querySelector('[data-sort="name"]').click();
    const semanticNames = Array.from(
      operationsBody.querySelectorAll('.semantic-row[aria-level="2"] .name-line'),
      line => line.querySelector("span:not(.leaf):not(.match-label)").textContent
    );
    check(
      semanticNames.filter(name => name.endsWith(" phase")).join(",") ===
        "Alpha phase,Zeta phase",
      "name sorting flattened or misordered semantic siblings"
    );

    const deepKeys = [
      "s:3",
      ...Array.from({ length: 39 }, (_, offset) => `s:${2000 + offset}`)
    ];
    deepKeys.forEach(key => expanded.add(key));
    renderRows();
    check(
      operationsBody.querySelector('[aria-level="34"] .depth-label').textContent ===
        "Depth 34",
      "first capped indentation depth was not labeled"
    );
    check(
      operationsBody.querySelector('[aria-level="42"] .depth-label').textContent ===
        "Depth 42",
      "deep hierarchy was visually flattened"
    );
    deepKeys.forEach(key => expanded.delete(key));
    renderRows();

    const groupKeys = Array.from(
      { length: 10 },
      (_, group) => `s:${4 + group * 101}`
    );
    groupKeys.forEach(key => expanded.add(key));
    renderRows();
    check(
      operationsBody.rows.length === maximumRenderedRows,
      "visible row budget was not enforced"
    );
    check(
      renderLimitStatus.textContent.includes("first 1000 visible rows"),
      "visible row limit was not explained"
    );
    groupKeys.forEach(key => expanded.delete(key));
    expanded.add(groupKeys[0]);
    selectedNode = { kind: "semantic", value: semanticsById.get(4) };
    renderRows();
    collapseSubtree.click();
    check(!expanded.has(groupKeys[0]), "bounded subtree collapse failed");
    selectedNode = { kind: "semantic", value: semanticsById.get(1) };

    const originalSemanticLookup = semanticsById.get;
    let oversizedAncestorLookups = 0;
    semanticsById.get = key => {
      oversizedAncestorLookups += 1;
      return originalSemanticLookup.call(semanticsById, key);
    };
    filterInput.value = "oversized target";
    applyFilter();
    semanticsById.get = originalSemanticLookup;
    check(filterResults.length === 1, "oversized filter count was incorrect");
    check(
      oversizedAncestorLookups <= maximumRenderedRows,
      "oversized filter traversed its complete ancestor chain"
    );
    check(operationsBody.rows.length === 1, "oversized match was not rendered flat");
    check(
      operationsBody.querySelector(".match-label").textContent === "Match",
      "oversized match was not labeled"
    );
    check(
      renderLimitStatus.textContent.includes("Ancestor context was omitted"),
      "omitted oversized context was not explained"
    );

    filterInput.value = "distributed target";
    applyFilter();
    check(filterResults.length === 100, "distributed filter count was incorrect");
    check(
      filterStatus.textContent === "Showing 1-99 of 100 matches.",
      "filter page did not account for ancestor context"
    );
    check(operationsBody.rows.length === 992, "first context-limited page was incorrect");
    check(
      Array.from(operationsBody.querySelectorAll(".match-label"))
        .filter(label => label.textContent === "Match").length === 99,
      "first context-limited page omitted a declared match"
    );
    nextFilterPage.click();
    check(
      filterStatus.textContent === "Showing 100-100 of 100 matches.",
      "second context-limited page status was incorrect"
    );
    check(operationsBody.rows.length === 12, "second context-limited page was incorrect");
    check(
      Array.from(operationsBody.querySelectorAll(".match-label"))
        .filter(label => label.textContent === "Match").length === 1,
      "second context-limited page omitted its match"
    );

    filterInput.value = "match function";
    applyFilter();
    check(filterResults.length === 101, "filter match count was incorrect");
    check(operationsBody.rows.length === 101, "first filter page was not bounded");
    const matchCount = Array.from(
      operationsBody.querySelectorAll(".match-label")
    ).filter(label => label.textContent === "Match").length;
    check(matchCount === 100, `first filter page labeled ${matchCount} matches`);
    check(
      operationsBody.querySelectorAll(".filter-context").length === 1,
      "first filter page did not retain its context"
    );
    nextFilterPage.click();
    check(operationsBody.rows.length === 2, "second filter page was incorrect");
    check(
      filterStatus.textContent === "Showing 101-101 of 101 matches.",
      "second filter page status was incorrect"
    );

    clearFilter.click();
    check(operationsBody.rows.length === 102, "clear did not restore expansion state");
    const expandedRoot = operationsBody.rows[0];
    expandedRoot.focus();
    expandedRoot.dispatchEvent(new KeyboardEvent("keydown", {
      key: "ArrowLeft",
      bubbles: true
    }));
    check(operationsBody.rows.length === 1, "ordinary collapse failed");
    document.documentElement.dataset.viewerSmoke = "passed";
  } catch (error) {
    document.documentElement.dataset.viewerSmoke = `failed:${error.message}`;
  }
})();
</script>
</body>
</html>
"#,
        );

        let mut report = tempfile::Builder::new().suffix(".html").tempfile()?;
        report.write_all(html.as_bytes())?;
        report.flush()?;
        let output = std::process::Command::new("timeout")
            .arg("30s")
            .arg(browser)
            .args([
                "--headless",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-background-networking",
                "--dump-dom",
            ])
            .arg(format!("file://{}", report.path().display()))
            .output()?;
        assert!(
            output.status.success(),
            "headless browser failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let dom = String::from_utf8(output.stdout)?;
        let result = dom
            .split_once("data-viewer-smoke=\"")
            .and_then(|(_, result)| result.split_once('"'))
            .map(|(result, _)| result)
            .unwrap_or("missing");
        assert_eq!(result, "passed", "browser smoke result");
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
