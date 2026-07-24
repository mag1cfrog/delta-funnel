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
.controls {
  align-items: end;
  display: flex;
  flex-wrap: wrap;
  gap: .75rem;
  margin: 1rem 0;
}
.filter-label { display: grid; gap: .25rem; }
.filter-label span { font-weight: 650; }
.filter-status { min-height: 1.5rem; }
.tree-controls { margin: 1rem 0; }
input, button { font: inherit; }
input[type="search"] {
  background: Canvas;
  border: 1px solid GrayText;
  border-radius: .25rem;
  color: CanvasText;
  min-width: min(24rem, 80vw);
  padding: .45rem .6rem;
}
.clear-filter, .filter-page, .sort, .tree-action {
  background: Canvas;
  border: 1px solid GrayText;
  border-radius: .25rem;
  color: CanvasText;
  cursor: pointer;
  padding: .35rem .55rem;
}
.clear-filter:disabled, .filter-page:disabled, .tree-action:disabled {
  cursor: default;
  opacity: .55;
}
.table-wrap { overflow-x: auto; }
table { border-collapse: collapse; min-width: 58rem; width: 100%; }
th, td { border-bottom: 1px solid GrayText; padding: .65rem; text-align: left; }
th { background: color-mix(in srgb, CanvasText 8%, Canvas); }
tbody tr:hover { background: color-mix(in srgb, Highlight 12%, Canvas); }
tbody tr[aria-selected="true"] {
  background: color-mix(in srgb, Highlight 22%, Canvas);
  outline: 2px solid Highlight;
  outline-offset: -2px;
}
tbody tr:focus-visible { outline: 3px solid Highlight; outline-offset: -3px; }
.name { font-weight: 650; }
.name-line { align-items: center; display: flex; gap: .35rem; }
.detail { display: block; font-size: .8rem; margin-top: .2rem; }
.number { font-variant-numeric: tabular-nums; text-align: right; white-space: nowrap; }
.disclosure, .leaf {
  align-items: center;
  display: inline-flex;
  flex: 0 0 1.5rem;
  height: 1.5rem;
  justify-content: center;
  width: 1.5rem;
}
.disclosure {
  background: Canvas;
  border: 1px solid GrayText;
  border-radius: .25rem;
  color: CanvasText;
  cursor: pointer;
}
.disclosure:focus-visible { outline: 3px solid Highlight; outline-offset: 2px; }
.disclosure:disabled { cursor: default; opacity: .75; }
.function-row { background: color-mix(in srgb, CanvasText 4%, Canvas); }
.function-row .name { font-weight: 500; }
.filter-context .name { font-style: italic; }
.match-label {
  border: 1px solid GrayText;
  border-radius: 1rem;
  font-size: .7rem;
  font-weight: 500;
  padding: .05rem .35rem;
}
.type-label { font-weight: 650; }
.empty { border: 1px dashed GrayText; padding: 1rem; }
.help { margin-top: 2rem; max-width: 80rem; }
.help summary { cursor: pointer; font-weight: 650; }
:is(input, button):focus-visible { outline: 3px solid Highlight; outline-offset: 2px; }
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
<p>Operation roots are ranked by exact duration. Expand a row to follow exact semantic children into sampled native callsites.</p>
<div class="controls">
<label class="filter-label" for="profile-filter"><span>Filter profile</span>
<input id="profile-filter" type="search" maxlength="200" autocomplete="off" placeholder="Name, symbol, module, or source file"></label>
<button id="clear-filter" class="clear-filter" type="button" disabled>Clear filter</button>
<button id="previous-filter-page" class="filter-page" type="button" hidden>Previous 100</button>
<button id="next-filter-page" class="filter-page" type="button" hidden>Next 100</button>
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
<p id="operations-empty" class="empty" hidden>No operation records are available.</p>
</section>
<details class="help">
<summary>How to read these metrics</summary>
<p>Exact duration is measured wall-clock or lifecycle time. Parallel semantic children may overlap and are not additive. Direct CPU samples belong to one semantic node. Inclusive CPU samples also include its semantic descendants. Sampling observes on-CPU work and does not prove why a thread was off-CPU.</p>
<p>Self CPU samples were observed directly in one function. Inclusive CPU samples also include sampled callees. Function percentages use direct samples from the owning semantic node as their denominator. Sample counts are statistical observations, not exact function milliseconds.</p>
<p>Eligible samples are the on-CPU samples considered for attribution. Directly attributed samples have one semantic owner. Ambiguous samples have more than one possible owner. Unattributed samples have no semantic owner. Linux sampling does not measure off-CPU waiting time.</p>
<p>Select a row to use the subtree controls. Arrow Up and Arrow Down move between visible rows. Arrow Right expands a row or moves to its first child. Arrow Left collapses a row or moves to its parent. Bulk expansion is limited to subtrees with at most 1000 total nodes.</p>
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
  value !== null && total > 0
    ? `${(value * 100 / total).toFixed(1)}%`
    : "N/A";
const formatCoverage = value =>
  `${value} (${formatPercent(
    value,
    profile.metadata.eligible_sample_count
  )} of eligible)`;
const compareValue = (left, right) => left < right ? -1 : left > right ? 1 : 0;
let sortField = "duration";
let sortDirection = "descending";
const semanticSortValue = semantic => {
  if (sortField === "name") return semantic.name;
  if (sortField === "direct") return semantic.direct_sample_count;
  if (sortField === "inclusive") return semantic.inclusive_sample_count;
  return semantic.duration_ns || 0;
};
const functionSortValue = fn => {
  if (sortField === "name") return fn.name;
  if (sortField === "direct") return fn.self_sample_count;
  return fn.inclusive_sample_count;
};
const compareSemantic = (left, right) => {
  const primary = compareValue(semanticSortValue(left), semanticSortValue(right));
  return (sortDirection === "ascending" ? primary : -primary) ||
    compareValue(left.semantic_id, right.semantic_id);
};
const compareFunction = (left, right) => {
  const primary = compareValue(functionSortValue(left), functionSortValue(right));
  return (sortDirection === "ascending" ? primary : -primary) ||
    compareValue(left.function_id, right.function_id);
};
const appendIndexed = (index, key, value) => {
  const values = index.get(key);
  if (values) values.push(value);
  else index.set(key, [value]);
};
const semanticKey = semantic => `s:${semantic.semantic_id}`;
const functionKey = fn => `f:${fn.semantic_id}:${fn.function_id}`;
const functionParentKey = (semanticId, functionId) =>
  `${semanticId}:${functionId}`;
const semanticChildren = new Map();
const functionRoots = new Map();
const functionChildren = new Map();
const semanticsById = new Map();
const functionsByKey = new Map();
profile.semantics.forEach(semantic => {
  semanticsById.set(semantic.semantic_id, semantic);
  if (semantic.parent_semantic_id !== null) {
    appendIndexed(semanticChildren, semantic.parent_semantic_id, semantic);
  }
});
profile.functions.forEach(fn => {
  functionsByKey.set(functionKey(fn), fn);
  if (fn.parent_function_id === null) {
    appendIndexed(functionRoots, fn.semantic_id, fn);
  } else {
    appendIndexed(
      functionChildren,
      functionParentKey(fn.semantic_id, fn.parent_function_id),
      fn
    );
  }
});
const operations = profile.semantics
  .filter(item => item.parent_semantic_id === null);
const operationDurations = new Map(
  operations.map(operation => [operation.operation_id, operation.duration_ns])
);
let selectedNode = operations.length === 0
  ? null
  : {
      kind: "semantic",
      value: operations.slice().sort(compareSemantic)[0]
    };
const expanded = new Set();
const disclosureButtons = new Map();
const maximumBulkExpansionRows = 1000;
const filterPageSize = 100;
let filterQuery = "";
let filterMatches = new Set();
let filterVisible = new Set();
let filterResults = [];
let filterPage = 0;
const operationsBody = document.getElementById("operations");
const operationsEmpty = document.getElementById("operations-empty");
const filterInput = document.getElementById("profile-filter");
const clearFilter = document.getElementById("clear-filter");
const previousFilterPage = document.getElementById("previous-filter-page");
const nextFilterPage = document.getElementById("next-filter-page");
const filterStatus = document.getElementById("filter-status");
const expandSubtree = document.getElementById("expand-subtree");
const collapseSubtree = document.getElementById("collapse-subtree");
const treeStatus = document.getElementById("tree-status");
const isFilterActive = () => filterQuery.length !== 0;
const entryKey = entry =>
  entry.kind === "semantic"
    ? semanticKey(entry.value)
    : functionKey(entry.value);
const isVisible = (kind, value) =>
  !isFilterActive() ||
  filterVisible.has(kind === "semantic" ? semanticKey(value) : functionKey(value));
const filterState = key =>
  !isFilterActive() ? null : filterMatches.has(key) ? "Match" : "Context";
const isAsciiDigit = character =>
  character !== undefined && character >= "0" && character <= "9";
const containsFilter = value => {
  let position = value.indexOf(filterQuery);
  while (position !== -1) {
    const end = position + filterQuery.length;
    const startsCleanly =
      !isAsciiDigit(filterQuery[0]) || !isAsciiDigit(value[position - 1]);
    const endsCleanly =
      !isAsciiDigit(filterQuery.at(-1)) || !isAsciiDigit(value[end]);
    if (startsCleanly && endsCleanly) return true;
    position = value.indexOf(filterQuery, position + 1);
  }
  return false;
};
const sortedVisible = (values, kind) =>
  values.filter(value => isVisible(kind, value)).sort(
    kind === "semantic" ? compareSemantic : compareFunction
  );
const textCell = (primary, detail, className = "") => {
  const cell = document.createElement("td");
  cell.className = className;
  cell.textContent = primary;
  if (detail) {
    const secondary = document.createElement("span");
    secondary.className = "detail";
    secondary.textContent = detail;
    cell.appendChild(secondary);
  }
  return cell;
};
const nameCell = (
  name,
  detail,
  depth,
  key,
  hasChildren,
  isExpanded,
  match,
  node
) => {
  const cell = document.createElement("td");
  cell.className = "name";
  const line = document.createElement("span");
  line.className = "name-line";
  line.style.paddingInlineStart = `${Math.min(depth - 1, 32) * 1.25}rem`;
  if (hasChildren) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "disclosure";
    button.setAttribute("aria-expanded", String(isExpanded));
    button.setAttribute(
      "aria-label",
      isFilterActive()
        ? "Expanded to show filter matches"
        : isExpanded ? "Collapse row" : "Expand row"
    );
    button.textContent = isExpanded ? "-" : "+";
    button.disabled = isFilterActive();
    button.addEventListener("click", () => {
      selectedNode = node;
      treeStatus.textContent = `Selected: ${node.value.name}`;
      if (expanded.has(key)) expanded.delete(key);
      else expanded.add(key);
      renderRows();
      disclosureButtons.get(key)?.focus();
    });
    disclosureButtons.set(key, button);
    line.appendChild(button);
  } else {
    const leaf = document.createElement("span");
    leaf.className = "leaf";
    leaf.setAttribute("aria-label", "Leaf row");
    leaf.textContent = "-";
    line.appendChild(leaf);
  }
  const label = document.createElement("span");
  label.textContent = name;
  line.appendChild(label);
  if (match) {
    const matchLabel = document.createElement("span");
    matchLabel.className = "match-label";
    matchLabel.textContent = match;
    line.appendChild(matchLabel);
  }
  const secondary = document.createElement("span");
  secondary.className = "detail";
  secondary.textContent = detail;
  cell.append(line, secondary);
  return cell;
};
const semanticRow = (semantic, depth) => {
  const children = sortedVisible(
    semanticChildren.get(semantic.semantic_id) || [],
    "semantic"
  );
  const functions = sortedVisible(
    functionRoots.get(semantic.semantic_id) || [],
    "function"
  );
  const hasChildren = children.length !== 0 || functions.length !== 0;
  const key = semanticKey(semantic);
  const isExpanded = isFilterActive() || expanded.has(key);
  const match = filterState(key);
  const row = document.createElement("tr");
  row.className = match === "Context"
    ? "semantic-row filter-context"
    : "semantic-row";
  row.setAttribute("aria-level", String(depth));
  if (hasChildren) row.setAttribute("aria-expanded", String(isExpanded));
  row.append(
    nameCell(
      semantic.name,
      `Exact semantic: ${semantic.operation_kind || semantic.semantic_kind}`,
      depth,
      key,
      hasChildren,
      isExpanded,
      match,
      { kind: "semantic", value: semantic }
    ),
    textCell(
      semantic.is_complete ? (semantic.result || "Complete") : "Incomplete",
      semantic.time_semantics === "wall_clock" ? "Exact wall clock" : "Lifecycle"
    ),
    textCell(formatDuration(semantic.duration_ns), "", "number"),
    textCell(
      formatPercent(
        semantic.duration_ns,
        operationDurations.get(semantic.operation_id) || 0
      ),
      "Owning operation duration",
      "number"
    ),
    textCell(String(semantic.direct_sample_count), "Direct", "number"),
    textCell(String(semantic.inclusive_sample_count), "Semantic subtree", "number")
  );
  return { row, hasChildren, isExpanded, key, children, functions };
};
const functionRow = (fn, depth) => {
  const children = sortedVisible(
    functionChildren.get(functionParentKey(fn.semantic_id, fn.function_id)) || [],
    "function"
  );
  const hasChildren = children.length !== 0;
  const key = functionKey(fn);
  const isExpanded = isFilterActive() || expanded.has(key);
  const match = filterState(key);
  const owner = semanticsById.get(fn.semantic_id);
  const location = [
    fn.module_name,
    fn.source_file === null
      ? null
      : `${fn.source_file}${fn.line_number === null ? "" : `:${fn.line_number}`}`
  ].filter(Boolean).join(" - ");
  const row = document.createElement("tr");
  row.className = match === "Context"
    ? "function-row filter-context"
    : "function-row";
  row.setAttribute("aria-level", String(depth));
  if (hasChildren) row.setAttribute("aria-expanded", String(isExpanded));
  row.append(
    nameCell(
      fn.name,
      location ? `Sampled function - ${location}` : "Sampled function",
      depth,
      key,
      hasChildren,
      isExpanded,
      match,
      { kind: "function", value: fn }
    ),
    textCell("Sampled on-CPU", "Statistical, not exact wall time"),
    textCell("N/A", "No exact function duration", "number"),
    textCell(
      formatPercent(fn.inclusive_sample_count, owner?.direct_sample_count || 0),
      "Owning semantic direct samples",
      "number"
    ),
    textCell(
      String(fn.self_sample_count),
      `${formatPercent(
        fn.self_sample_count,
        owner?.direct_sample_count || 0
      )} of owning semantic direct samples`,
      "number"
    ),
    textCell(String(fn.inclusive_sample_count), "Call subtree", "number")
  );
  return { row, hasChildren, isExpanded, key, children };
};
const renderRows = (focusKey = null) => {
  disclosureButtons.clear();
  const fragment = document.createDocumentFragment();
  const renderedRows = [];
  const stack = sortedVisible(operations, "semantic")
    .reverse()
    .map(operation => ({ kind: "semantic", value: operation, depth: 1 }));
  let rowCount = 0;
  while (stack.length !== 0) {
    const entry = stack.pop();
    if (entry.kind === "semantic") {
      const rendered = semanticRow(entry.value, entry.depth);
      fragment.appendChild(rendered.row);
      renderedRows.push({
        kind: "semantic",
        value: entry.value,
        depth: entry.depth,
        ...rendered
      });
      rowCount += 1;
      if (rendered.hasChildren && rendered.isExpanded) {
        const children = [
          ...rendered.children.map(value => ({ kind: "semantic", value })),
          ...rendered.functions.map(value => ({ kind: "function", value }))
        ];
        for (let index = children.length - 1; index >= 0; index -= 1) {
          stack.push({ ...children[index], depth: entry.depth + 1 });
        }
      }
    } else {
      const rendered = functionRow(entry.value, entry.depth);
      fragment.appendChild(rendered.row);
      renderedRows.push({
        kind: "function",
        value: entry.value,
        depth: entry.depth,
        ...rendered
      });
      rowCount += 1;
      if (rendered.hasChildren && rendered.isExpanded) {
        for (let index = rendered.children.length - 1; index >= 0; index -= 1) {
          stack.push({
            kind: "function",
            value: rendered.children[index],
            depth: entry.depth + 1
          });
        }
      }
    }
  }
  operationsBody.replaceChildren(fragment);
  operationsEmpty.textContent = isFilterActive()
    ? "No profile rows match this filter."
    : "No operation records are available.";
  operationsEmpty.hidden = rowCount !== 0;
  configureRenderedRows(renderedRows, focusKey);
};
const configureRenderedRows = (rows, focusKey) => {
  let selectedKey = selectedNode === null ? null : entryKey(selectedNode);
  if (!rows.some(entry => entry.key === selectedKey) && rows.length !== 0) {
    selectedNode = { kind: rows[0].kind, value: rows[0].value };
    selectedKey = rows[0].key;
  }
  const updateSelection = (selectedIndex, focus) => {
    const selected = rows[selectedIndex];
    selectedNode = { kind: selected.kind, value: selected.value };
    rows.forEach((entry, index) => {
      const isSelected = index === selectedIndex;
      entry.row.setAttribute("aria-selected", String(isSelected));
      entry.row.tabIndex = isSelected ? 0 : -1;
    });
    treeStatus.textContent = `Selected: ${selected.value.name}`;
    updateTreeControls(rows);
    if (focus) selected.row.focus();
  };
  rows.forEach((entry, index) => {
    const isSelected = entry.key === selectedKey;
    entry.row.setAttribute("aria-selected", String(isSelected));
    entry.row.tabIndex = isSelected ? 0 : -1;
    entry.row.addEventListener("click", event => {
      if (event.target.closest("button")) return;
      updateSelection(index, true);
    });
    entry.row.addEventListener("keydown", event => {
      if (event.target !== entry.row) return;
      let nextIndex = null;
      if (event.key === "ArrowDown" && index + 1 < rows.length) {
        nextIndex = index + 1;
      } else if (event.key === "ArrowUp" && index > 0) {
        nextIndex = index - 1;
      } else if (event.key === "Home") {
        nextIndex = 0;
      } else if (event.key === "End") {
        nextIndex = rows.length - 1;
      } else if (event.key === "ArrowRight") {
        if (entry.hasChildren && !entry.isExpanded && !isFilterActive()) {
          selectedNode = { kind: entry.kind, value: entry.value };
          expanded.add(entry.key);
          renderRows(entry.key);
        } else if (
          index + 1 < rows.length &&
          rows[index + 1].depth > entry.depth
        ) {
          nextIndex = index + 1;
        }
      } else if (event.key === "ArrowLeft") {
        if (entry.hasChildren && entry.isExpanded && !isFilterActive()) {
          selectedNode = { kind: entry.kind, value: entry.value };
          expanded.delete(entry.key);
          renderRows(entry.key);
        } else {
          for (let parent = index - 1; parent >= 0; parent -= 1) {
            if (rows[parent].depth < entry.depth) {
              nextIndex = parent;
              break;
            }
          }
        }
      } else {
        return;
      }
      event.preventDefault();
      if (nextIndex !== null) updateSelection(nextIndex, true);
    });
  });
  updateTreeControls(rows);
  if (focusKey !== null) {
    rows.find(entry => entry.key === focusKey)?.row.focus();
  }
};
const updateTreeControls = rows => {
  const selectedKey = selectedNode === null ? null : entryKey(selectedNode);
  const selectedIsVisible = rows.some(entry => entry.key === selectedKey);
  const disabled = !selectedIsVisible || isFilterActive();
  expandSubtree.disabled = disabled;
  collapseSubtree.disabled = disabled;
  if (isFilterActive()) {
    treeStatus.textContent = "Clear the filter to change subtree expansion.";
  } else if (
    treeStatus.textContent === "Clear the filter to change subtree expansion."
  ) {
    treeStatus.textContent = selectedNode === null
      ? ""
      : `Selected: ${selectedNode.value.name}`;
  }
};
const childEntries = entry => {
  if (entry.kind === "semantic") {
    return [
      ...(semanticChildren.get(entry.value.semantic_id) || []).map(
        value => ({ kind: "semantic", value })
      ),
      ...(functionRoots.get(entry.value.semantic_id) || []).map(
        value => ({ kind: "function", value })
      )
    ];
  }
  return (
    functionChildren.get(
      functionParentKey(entry.value.semantic_id, entry.value.function_id)
    ) || []
  ).map(value => ({ kind: "function", value }));
};
const collectExpansionKeys = root => {
  const keys = [];
  const stack = [root];
  let nodeCount = 0;
  while (stack.length !== 0) {
    const entry = stack.pop();
    nodeCount += 1;
    if (nodeCount > maximumBulkExpansionRows) return null;
    const children = childEntries(entry);
    if (children.length !== 0) keys.push(entryKey(entry));
    for (let index = children.length - 1; index >= 0; index -= 1) {
      stack.push(children[index]);
    }
  }
  return keys;
};
const collapseSelectedSubtree = root => {
  const stack = [root];
  let collapsedCount = 0;
  while (stack.length !== 0) {
    const entry = stack.pop();
    if (expanded.delete(entryKey(entry))) collapsedCount += 1;
    const children = childEntries(entry);
    for (let index = children.length - 1; index >= 0; index -= 1) {
      stack.push(children[index]);
    }
  }
  return collapsedCount;
};
const retainSemantic = semantic => {
  let current = semantic;
  while (current) {
    const key = semanticKey(current);
    if (filterVisible.has(key)) break;
    filterVisible.add(key);
    current = current.parent_semantic_id === null
      ? null
      : semanticsById.get(current.parent_semantic_id);
  }
};
const retainFunction = fn => {
  let current = fn;
  while (current) {
    const key = functionKey(current);
    if (filterVisible.has(key)) break;
    filterVisible.add(key);
    current = current.parent_function_id === null
      ? null
      : functionsByKey.get(
          `f:${current.semantic_id}:${current.parent_function_id}`
        );
  }
  retainSemantic(semanticsById.get(fn.semantic_id));
};
const showFilterPage = page => {
  const pageCount = Math.ceil(filterResults.length / filterPageSize);
  filterPage = Math.max(0, Math.min(page, Math.max(0, pageCount - 1)));
  filterMatches = new Set();
  filterVisible = new Set();
  const start = filterPage * filterPageSize;
  const end = Math.min(start + filterPageSize, filterResults.length);
  for (let index = start; index < end; index += 1) {
    const result = filterResults[index];
    if (result.function_id === undefined) {
      filterMatches.add(semanticKey(result));
      retainSemantic(result);
    } else {
      filterMatches.add(functionKey(result));
      retainFunction(result);
    }
  }
  filterStatus.textContent = !isFilterActive()
    ? ""
    : filterResults.length === 0
      ? "0 matches."
      : `Showing ${start + 1}-${end} of ${filterResults.length} matches.`;
  previousFilterPage.hidden = pageCount <= 1;
  previousFilterPage.disabled = filterPage === 0;
  nextFilterPage.hidden = pageCount <= 1;
  nextFilterPage.disabled = filterPage + 1 >= pageCount;
  renderRows();
};
const applyFilter = () => {
  filterQuery = filterInput.value.trim().toLowerCase();
  filterResults = [];
  if (isFilterActive()) {
    profile.semantics.forEach(semantic => {
      if (containsFilter(semantic.name.toLowerCase())) {
        filterResults.push(semantic);
      }
    });
    profile.functions.forEach(fn => {
      const matches = [fn.name, fn.module_name, fn.source_file].some(
        value => value !== null && containsFilter(value.toLowerCase())
      );
      if (matches) filterResults.push(fn);
    });
  }
  clearFilter.disabled = !isFilterActive();
  showFilterPage(0);
};
const updateSortControls = () => {
  document.querySelectorAll("[data-sort-column]").forEach(header => {
    if (header.dataset.sortColumn === sortField) {
      header.setAttribute("aria-sort", sortDirection);
    } else {
      header.removeAttribute("aria-sort");
    }
  });
  document.querySelectorAll("[data-sort]").forEach(button => {
    const active = button.dataset.sort === sortField;
    const state = active ? ` [${sortDirection}]` : "";
    button.textContent = `${button.dataset.label}${state}`;
  });
};
document.querySelectorAll("[data-sort]").forEach(button => {
  button.addEventListener("click", () => {
    const field = button.dataset.sort;
    if (sortField === field) {
      sortDirection = sortDirection === "ascending" ? "descending" : "ascending";
    } else {
      sortField = field;
      sortDirection = field === "name" ? "ascending" : "descending";
    }
    updateSortControls();
    renderRows();
  });
});
let filterTimer;
filterInput.addEventListener("input", () => {
  window.clearTimeout(filterTimer);
  filterTimer = window.setTimeout(applyFilter, 150);
});
clearFilter.addEventListener("click", () => {
  window.clearTimeout(filterTimer);
  filterInput.value = "";
  applyFilter();
  filterInput.focus();
});
previousFilterPage.addEventListener("click", () => {
  showFilterPage(filterPage - 1);
});
nextFilterPage.addEventListener("click", () => {
  showFilterPage(filterPage + 1);
});
expandSubtree.addEventListener("click", () => {
  if (selectedNode === null) return;
  const keys = collectExpansionKeys(selectedNode);
  if (keys === null) {
    treeStatus.textContent =
      `The selected subtree exceeds ${maximumBulkExpansionRows} nodes. Select a narrower branch.`;
    return;
  }
  keys.forEach(key => expanded.add(key));
  treeStatus.textContent =
    `Expanded ${keys.length} rows with children in the selected subtree.`;
  renderRows(entryKey(selectedNode));
});
collapseSubtree.addEventListener("click", () => {
  if (selectedNode === null) return;
  const collapsedCount = collapseSelectedSubtree(selectedNode);
  treeStatus.textContent =
    `Collapsed ${collapsedCount} expanded rows in the selected subtree.`;
  renderRows(entryKey(selectedNode));
});
addSummary("Sample frequency", `${profile.metadata.sample_frequency_hz} Hz`);
addSummary("Eligible CPU samples", profile.metadata.eligible_sample_count);
addSummary("Directly attributed", formatCoverage(profile.metadata.direct_sample_count));
addSummary("Ambiguous", formatCoverage(profile.metadata.ambiguous_sample_count));
addSummary("Unattributed", formatCoverage(profile.metadata.unattributed_sample_count));
updateSortControls();
renderRows();
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
        html.strip_prefix(HTML_PREFIX)
            .and_then(|remainder| remainder.strip_suffix(HTML_SUFFIX))
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
        assert!(html.contains(r#"data-sort="duration""#));
        assert!(!html.contains(r#"id="functions""#));
        assert!(html.contains(r#"button.setAttribute("aria-expanded""#));
        assert!(html.contains(r#"entry.row.setAttribute("aria-selected""#));
        assert!(html.contains("const maximumBulkExpansionRows = 1000"));
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
