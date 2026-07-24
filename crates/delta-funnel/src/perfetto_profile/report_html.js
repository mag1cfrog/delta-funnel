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
const compareText = (left, right) => {
  let leftIndex = 0;
  let rightIndex = 0;
  while (leftIndex < left.length && rightIndex < right.length) {
    const leftCodePoint = left.codePointAt(leftIndex);
    const rightCodePoint = right.codePointAt(rightIndex);
    const comparison = compareValue(leftCodePoint, rightCodePoint);
    if (comparison !== 0) return comparison;
    leftIndex += leftCodePoint > 0xffff ? 2 : 1;
    rightIndex += rightCodePoint > 0xffff ? 2 : 1;
  }
  return compareValue(left.length - leftIndex, right.length - rightIndex);
};
let sortField = "duration";
let sortDirection = "descending";
const semanticSortValue = semantic => {
  if (sortField === "direct") return semantic.direct_sample_count;
  if (sortField === "inclusive") return semantic.inclusive_sample_count;
  return semantic.duration_ns || 0;
};
const functionSortValue = fn => {
  if (sortField === "direct") return fn.self_sample_count;
  return fn.inclusive_sample_count;
};
const compareSemantic = (left, right) => {
  const primary = sortField === "name"
    ? compareText(left.name, right.name)
    : compareValue(semanticSortValue(left), semanticSortValue(right));
  return (sortDirection === "ascending" ? primary : -primary) ||
    compareValue(left.semantic_id, right.semantic_id);
};
const compareFunction = (left, right) => {
  const primary = sortField === "name"
    ? compareText(left.name, right.name)
    : compareValue(functionSortValue(left), functionSortValue(right));
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
const functionSourceLocation = fn =>
  fn.source_file === null
    ? null
    : `${fn.source_file}${fn.line_number === null ? "" : `:${fn.line_number}`}`;
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
const maximumBulkSubtreeRows = 1000;
const maximumRenderedRows = 1000;
const maximumIndentDepth = 32;
const siblingPageSize = 100;
const siblingPages = new Map();
const paginationRows = new Map();
const rootPageKey = "roots";
const bulkSubtreeLimitMessage =
  `The selected subtree exceeds ${maximumBulkSubtreeRows} nodes. Select a narrower branch.`;
const filterPageSize = 100;
let filterQuery = "";
let filterMatches = new Set();
let filterVisible = new Set();
let filterResults = [];
let filterPage = 0;
let filterPageStarts = [0];
let filterOversizedResult = null;
let filterOrderCache = [];
let filterOrderCacheKey = null;
const operationsBody = document.getElementById("operations");
const operationsEmpty = document.getElementById("operations-empty");
const renderLimitStatus = document.getElementById("render-limit-status");
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
const filterState = record =>
  !isFilterActive() ? null : filterMatches.has(record) ? "Match" : "Context";
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
const sortedValues = (values, kind) =>
  values.slice().sort(kind === "semantic" ? compareSemantic : compareFunction);
const sortedVisible = (values, kind) =>
  sortedValues(values.filter(value => isVisible(kind, value)), kind);
const pagedEntries = (entries, key, owner, label) => {
  if (isFilterActive() || entries.length <= siblingPageSize) {
    return { entries, pagination: null };
  }
  const pageCount = Math.ceil(entries.length / siblingPageSize);
  const page = Math.min(siblingPages.get(key) || 0, pageCount - 1);
  siblingPages.set(key, page);
  const start = page * siblingPageSize;
  const end = Math.min(start + siblingPageSize, entries.length);
  return {
    entries: entries.slice(start, end),
    pagination: {
      key,
      owner,
      label,
      page,
      pageCount,
      start,
      end,
      total: entries.length
    }
  };
};
const pushPagedEntries = (stack, entries, depth, key, owner, label) => {
  const page = pagedEntries(entries, key, owner, label);
  if (page.pagination !== null) {
    stack.push({ kind: "pagination", value: page.pagination, depth });
  }
  for (let index = page.entries.length - 1; index >= 0; index -= 1) {
    stack.push({ ...page.entries[index], depth });
  }
};
const filterCandidatesInCurrentOrder = () => {
  const cacheKey = `${sortField}:${sortDirection}`;
  if (filterOrderCacheKey === cacheKey) return filterOrderCache;

  const ordered = [];
  const stack = sortedValues(operations, "semantic")
    .reverse()
    .map(value => ({ kind: "semantic", value }));
  while (stack.length !== 0) {
    const entry = stack.pop();
    ordered.push(entry.value);
    const children = entry.kind === "semantic"
      ? [
          ...sortedValues(
            semanticChildren.get(entry.value.semantic_id) || [],
            "semantic"
          ).map(value => ({ kind: "semantic", value })),
          ...sortedValues(
            functionRoots.get(entry.value.semantic_id) || [],
            "function"
          ).map(value => ({ kind: "function", value }))
        ]
      : sortedValues(
          functionChildren.get(
            functionParentKey(
              entry.value.semantic_id,
              entry.value.function_id
            )
          ) || [],
          "function"
        ).map(value => ({ kind: "function", value }));
    for (let index = children.length - 1; index >= 0; index -= 1) {
      stack.push(children[index]);
    }
  }
  filterOrderCache = ordered;
  filterOrderCacheKey = cacheKey;
  return filterOrderCache;
};
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
  line.style.paddingInlineStart =
    `${Math.min(depth - 1, maximumIndentDepth) * 1.25}rem`;
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
  if (depth - 1 > maximumIndentDepth) {
    const depthLabel = document.createElement("span");
    depthLabel.className = "depth-label";
    depthLabel.setAttribute("aria-hidden", "true");
    depthLabel.textContent = `Depth ${depth}`;
    line.appendChild(depthLabel);
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
const paginationRow = (pagination, depth) => {
  const row = document.createElement("tr");
  row.className = "pagination-row";
  row.setAttribute("aria-level", String(depth));
  row.tabIndex = -1;
  const cell = document.createElement("td");
  cell.colSpan = 6;
  const controls = document.createElement("div");
  controls.className = "pagination-controls";
  const status = document.createElement("span");
  status.textContent =
    `${pagination.label}: ${pagination.start + 1}-${pagination.end} of ${pagination.total}.`;
  const changePage = page => {
    siblingPages.set(pagination.key, page);
    if (pagination.owner !== null) selectedNode = pagination.owner;
    renderRows(null, pagination.key);
  };
  const previous = document.createElement("button");
  previous.type = "button";
  previous.className = "filter-page";
  previous.textContent = `Previous ${siblingPageSize}`;
  previous.disabled = pagination.page === 0;
  previous.addEventListener("click", () => changePage(pagination.page - 1));
  const next = document.createElement("button");
  next.type = "button";
  next.className = "filter-page";
  next.textContent = `Next ${siblingPageSize}`;
  next.disabled = pagination.page + 1 >= pagination.pageCount;
  next.addEventListener("click", () => changePage(pagination.page + 1));
  controls.append(previous, next, status);
  cell.appendChild(controls);
  row.appendChild(cell);
  paginationRows.set(pagination.key, row);
  return row;
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
  const match = filterState(semantic);
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
  const match = filterState(fn);
  const owner = semanticsById.get(fn.semantic_id);
  const location = [
    fn.module_name,
    functionSourceLocation(fn)
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
const renderRows = (focusKey = null, focusPaginationKey = null) => {
  disclosureButtons.clear();
  paginationRows.clear();
  const fragment = document.createDocumentFragment();
  const renderedRows = [];
  const stack = [];
  if (filterOversizedResult === null) {
    pushPagedEntries(
      stack,
      sortedVisible(operations, "semantic").map(
        value => ({ kind: "semantic", value })
      ),
      1,
      rootPageKey,
      null,
      "Operation roots"
    );
  } else {
    stack.push({
      kind: filterOversizedResult.function_id === undefined
        ? "semantic"
        : "function",
      value: filterOversizedResult,
      depth: 1
    });
  }
  let rowCount = 0;
  while (stack.length !== 0 && rowCount < maximumRenderedRows) {
    const entry = stack.pop();
    if (entry.kind === "pagination") {
      fragment.appendChild(paginationRow(entry.value, entry.depth));
      rowCount += 1;
    } else if (entry.kind === "semantic") {
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
        pushPagedEntries(
          stack,
          children,
          entry.depth + 1,
          rendered.key,
          { kind: "semantic", value: entry.value },
          `${entry.value.name} children`
        );
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
        pushPagedEntries(
          stack,
          rendered.children.map(value => ({ kind: "function", value })),
          entry.depth + 1,
          rendered.key,
          { kind: "function", value: entry.value },
          `${entry.value.name} children`
        );
      }
    }
  }
  operationsBody.replaceChildren(fragment);
  operationsEmpty.textContent = isFilterActive()
    ? "No profile rows match this filter."
    : "No operation records are available.";
  operationsEmpty.hidden = renderedRows.length !== 0;
  renderLimitStatus.textContent = filterOversizedResult !== null
    ? `Ancestor context was omitted because the complete context for this match exceeds the ${maximumRenderedRows}-row visible limit.`
    : stack.length === 0
      ? ""
      : `Showing the first ${maximumRenderedRows} visible rows. Collapse a branch or filter the profile to continue.`;
  configureRenderedRows(renderedRows, focusKey);
  if (focusPaginationKey !== null) {
    paginationRows.get(focusPaginationKey)?.focus();
  }
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
const collectSubtreeBranchKeys = root => {
  const keys = [];
  const stack = [root];
  let nodeCount = 0;
  while (stack.length !== 0) {
    const entry = stack.pop();
    nodeCount += 1;
    if (nodeCount > maximumBulkSubtreeRows) return null;
    const children = childEntries(entry);
    if (children.length !== 0) keys.push(entryKey(entry));
    for (let index = children.length - 1; index >= 0; index -= 1) {
      stack.push(children[index]);
    }
  }
  return keys;
};
const collapseSelectedSubtree = root => {
  const keys = collectSubtreeBranchKeys(root);
  if (keys === null) return null;
  let collapsedCount = 0;
  keys.forEach(key => {
    if (expanded.delete(key)) collapsedCount += 1;
  });
  return collapsedCount;
};
const retainSemantic = (semantic, visible) => {
  let current = semantic;
  while (current) {
    const key = semanticKey(current);
    if (visible.has(key)) break;
    visible.add(key);
    current = current.parent_semantic_id === null
      ? null
      : semanticsById.get(current.parent_semantic_id);
  }
};
const retainFunction = (fn, visible) => {
  let current = fn;
  while (current) {
    const key = functionKey(current);
    if (visible.has(key)) break;
    visible.add(key);
    current = current.parent_function_id === null
      ? null
      : functionsByKey.get(
          `f:${current.semantic_id}:${current.parent_function_id}`
        );
  }
  retainSemantic(semanticsById.get(fn.semantic_id), visible);
};
const retainFilterResult = (result, visible) => {
  if (result.function_id === undefined) {
    retainSemantic(result, visible);
  } else {
    retainFunction(result, visible);
  }
};
const showFilterPage = page => {
  filterPage = Math.max(0, Math.min(page, filterPageStarts.length - 1));
  const start = filterPageStarts[filterPage];
  let end = start;
  filterVisible = new Set();
  filterOversizedResult = null;
  while (
    end < filterResults.length &&
    end - start < filterPageSize
  ) {
    const candidateVisible = new Set(filterVisible);
    retainFilterResult(filterResults[end], candidateVisible);
    if (candidateVisible.size > maximumRenderedRows) {
      if (end === start) {
        filterOversizedResult = filterResults[end];
        filterVisible.add(
          filterOversizedResult.function_id === undefined
            ? semanticKey(filterOversizedResult)
            : functionKey(filterOversizedResult)
        );
        end += 1;
      }
      break;
    }
    filterVisible = candidateVisible;
    end += 1;
  }
  filterPageStarts.length = filterPage + 1;
  if (end < filterResults.length) filterPageStarts.push(end);
  filterStatus.textContent = !isFilterActive()
    ? ""
    : filterResults.length === 0
      ? "0 matches."
      : `Showing ${start + 1}-${end} of ${filterResults.length} matches.`;
  const hasMultiplePages = start !== 0 || end < filterResults.length;
  previousFilterPage.hidden = !hasMultiplePages;
  previousFilterPage.disabled = start === 0;
  nextFilterPage.hidden = !hasMultiplePages;
  nextFilterPage.disabled = end >= filterResults.length;
  renderRows();
};
const applyFilter = () => {
  filterQuery = filterInput.value.trim().toLowerCase();
  filterResults = [];
  filterMatches = new Set();
  filterPageStarts = [0];
  filterOversizedResult = null;
  if (isFilterActive()) {
    filterCandidatesInCurrentOrder().forEach(record => {
      if (record.function_id === undefined) {
        if (containsFilter(record.name.toLowerCase())) {
          filterResults.push(record);
          filterMatches.add(record);
        }
      } else {
        const matches = [
          record.name,
          record.module_name,
          functionSourceLocation(record)
        ].some(
          value => value !== null && containsFilter(value.toLowerCase())
        );
        if (matches) {
          filterResults.push(record);
          filterMatches.add(record);
        }
      }
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
    siblingPages.clear();
    updateSortControls();
    if (isFilterActive()) applyFilter();
    else renderRows();
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
  const keys = collectSubtreeBranchKeys(selectedNode);
  if (keys === null) {
    treeStatus.textContent = bulkSubtreeLimitMessage;
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
  if (collapsedCount === null) {
    treeStatus.textContent = bulkSubtreeLimitMessage;
    return;
  }
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
