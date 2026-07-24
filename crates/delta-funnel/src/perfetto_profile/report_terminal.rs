use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;

use clap::ValueEnum;

use super::ranked_report::{RankedFunction, RankedProfileDocument, RankedSemantic};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub(super) enum InspectSort {
    #[default]
    Duration,
    InclusiveCpu,
    SelfCpu,
    Name,
}

impl InspectSort {
    fn as_str(self) -> &'static str {
        match self {
            Self::Duration => "duration",
            Self::InclusiveCpu => "inclusive-cpu",
            Self::SelfCpu => "self-cpu",
            Self::Name => "name",
        }
    }

    fn for_functions(self) -> Self {
        match self {
            Self::Duration => Self::InclusiveCpu,
            sort => sort,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum InspectSelection {
    Root,
    Semantic(i64),
    Function { semantic_id: i64, function_id: i64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TerminalInspectError {
    SemanticNotFound(i64),
    FunctionNotFound { semantic_id: i64, function_id: i64 },
}

impl TerminalInspectError {
    pub(super) const fn kind(self) -> &'static str {
        match self {
            Self::SemanticNotFound(_) => "semantic_not_found",
            Self::FunctionNotFound { .. } => "function_not_found",
        }
    }
}

impl fmt::Display for TerminalInspectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SemanticNotFound(semantic_id) => {
                write!(formatter, "semantic:{semantic_id} does not exist")
            }
            Self::FunctionNotFound {
                semantic_id,
                function_id,
            } => write!(
                formatter,
                "function:{semantic_id}:{function_id} does not exist"
            ),
        }
    }
}

impl std::error::Error for TerminalInspectError {}

pub(super) struct TerminalProfileIndex<'a> {
    document: &'a RankedProfileDocument,
    semantics: HashMap<i64, &'a RankedSemantic>,
    semantic_children: HashMap<Option<i64>, Vec<&'a RankedSemantic>>,
    functions: HashMap<(i64, i64), &'a RankedFunction>,
    function_children: HashMap<(i64, Option<i64>), Vec<&'a RankedFunction>>,
    operation_durations: HashMap<i64, Option<i64>>,
}

impl<'a> TerminalProfileIndex<'a> {
    pub(super) fn new(document: &'a RankedProfileDocument) -> Self {
        let mut semantics = HashMap::with_capacity(document.semantics.len());
        let mut semantic_children = HashMap::new();
        let mut operation_durations = HashMap::new();
        for semantic in &document.semantics {
            semantics.insert(semantic.semantic_id, semantic);
            semantic_children
                .entry(semantic.parent_semantic_id)
                .or_insert_with(Vec::new)
                .push(semantic);
            if semantic.parent_semantic_id.is_none() {
                operation_durations.insert(semantic.operation_id, semantic.duration_ns);
            }
        }
        let mut functions = HashMap::with_capacity(document.functions.len());
        let mut function_children = HashMap::new();
        for function in &document.functions {
            functions.insert((function.semantic_id, function.function_id), function);
            function_children
                .entry((function.semantic_id, function.parent_function_id))
                .or_insert_with(Vec::new)
                .push(function);
        }
        Self {
            document,
            semantics,
            semantic_children,
            functions,
            function_children,
            operation_durations,
        }
    }

    pub(super) fn render(
        &self,
        selection: InspectSelection,
        sort: InspectSort,
        filter: Option<&str>,
        limit: usize,
        max_depth: usize,
    ) -> Result<String, TerminalInspectError> {
        match selection {
            InspectSelection::Root => {
                render_semantic_view(self, None, sort, filter, limit, max_depth)
            }
            InspectSelection::Semantic(semantic_id) => {
                render_semantic_view(self, Some(semantic_id), sort, filter, limit, max_depth)
            }
            InspectSelection::Function {
                semantic_id,
                function_id,
            } => render_function_view(
                self,
                semantic_id,
                function_id,
                sort,
                filter,
                limit,
                max_depth,
            ),
        }
    }

    pub(super) fn open_semantic(
        &self,
        selection: InspectSelection,
        semantic_id: i64,
    ) -> Result<InspectSelection, &'static str> {
        let parent = match selection {
            InspectSelection::Root => None,
            InspectSelection::Semantic(parent) => Some(parent),
            InspectSelection::Function { .. } => {
                return Err("semantic target is not an immediate child");
            }
        };
        if self
            .semantics
            .get(&semantic_id)
            .map(|semantic| semantic.parent_semantic_id)
            != Some(parent)
        {
            return Err("semantic target is not an immediate child");
        }
        Ok(InspectSelection::Semantic(semantic_id))
    }

    pub(super) fn open_function(
        &self,
        selection: InspectSelection,
        semantic_id: i64,
        function_id: i64,
    ) -> Result<InspectSelection, &'static str> {
        let parent = match selection {
            InspectSelection::Semantic(owner) if owner == semantic_id => None,
            InspectSelection::Function {
                semantic_id: owner,
                function_id: parent,
            } if owner == semantic_id => Some(parent),
            InspectSelection::Root
            | InspectSelection::Semantic(_)
            | InspectSelection::Function { .. } => {
                return Err("function target is not an immediate child");
            }
        };
        if self
            .functions
            .get(&(semantic_id, function_id))
            .map(|function| function.parent_function_id)
            != Some(parent)
        {
            return Err("function target is not an immediate child");
        }
        Ok(InspectSelection::Function {
            semantic_id,
            function_id,
        })
    }

    pub(super) fn up(&self, selection: InspectSelection) -> Result<InspectSelection, &'static str> {
        match selection {
            InspectSelection::Root => Err("already at operation roots"),
            InspectSelection::Semantic(semantic_id) => self
                .semantics
                .get(&semantic_id)
                .map(|semantic| {
                    semantic
                        .parent_semantic_id
                        .map_or(InspectSelection::Root, InspectSelection::Semantic)
                })
                .ok_or("current semantic selection does not exist"),
            InspectSelection::Function {
                semantic_id,
                function_id,
            } => self
                .functions
                .get(&(semantic_id, function_id))
                .map(|function| {
                    function.parent_function_id.map_or(
                        InspectSelection::Semantic(semantic_id),
                        |function_id| InspectSelection::Function {
                            semantic_id,
                            function_id,
                        },
                    )
                })
                .ok_or("current function selection does not exist"),
        }
    }
}

#[cfg(test)]
pub(super) fn render_terminal_view(
    document: &RankedProfileDocument,
    selection: InspectSelection,
    sort: InspectSort,
    filter: Option<&str>,
    limit: usize,
    max_depth: usize,
) -> Result<String, TerminalInspectError> {
    TerminalProfileIndex::new(document).render(selection, sort, filter, limit, max_depth)
}

fn render_semantic_view(
    index: &TerminalProfileIndex<'_>,
    selected_semantic_id: Option<i64>,
    sort: InspectSort,
    filter: Option<&str>,
    limit: usize,
    max_depth: usize,
) -> Result<String, TerminalInspectError> {
    let selected_semantic = selected_semantic_id
        .map(|semantic_id| {
            index
                .semantics
                .get(&semantic_id)
                .copied()
                .ok_or(TerminalInspectError::SemanticNotFound(semantic_id))
        })
        .transpose()?;

    let included_semantics = filter
        .map(|filter| contextual_semantic_matches(index, selected_semantic_id, max_depth, filter));

    let first_depth = usize::from(selected_semantic_id.is_some());
    let (semantic_total, semantic_rows) = collect_semantic_rows(
        &index.semantic_children,
        selected_semantic_id,
        first_depth,
        max_depth,
        limit,
        sort,
        included_semantics.as_ref(),
    );
    let function_sort = sort.for_functions();
    let mut function_roots = selected_semantic
        .filter(|_| max_depth > 0)
        .into_iter()
        .flat_map(|semantic| {
            index
                .function_children
                .get(&(semantic.semantic_id, None))
                .into_iter()
                .flatten()
                .copied()
                .filter(move |function| {
                    filter.is_none_or(|filter| function_matches(function, filter))
                })
        })
        .collect::<Vec<_>>();
    function_roots.sort_unstable_by(|left, right| compare_functions(left, right, function_sort));
    let function_total = function_roots.len();
    let function_rows = function_roots
        .into_iter()
        .take(limit.saturating_sub(semantic_rows.len()))
        .collect::<Vec<_>>();
    let total = semantic_total + function_total;
    let shown = semantic_rows.len() + function_rows.len();

    let context = selected_semantic_id.map_or_else(
        || "operation-roots".to_owned(),
        |semantic_id| format!("semantic:{semantic_id}"),
    );
    let filter = filter_label(filter);
    let mut output = format!(
        "view: ranked-profile\ncontext: {context}\nsort: {}\nfilter: {filter}\ndepth: {max_depth}\nshowing: {} of {total}; truncated: {}\ntime_unit: {}\nsample_unit: {}\n",
        sort.as_str(),
        shown,
        shown < total,
        terminal_text(&index.document.metadata.exact_time_unit),
        terminal_text(&index.document.metadata.sample_unit),
    );
    for (depth, semantic) in semantic_rows {
        write_semantic_row(
            &mut output,
            depth,
            semantic,
            index
                .operation_durations
                .get(&semantic.operation_id)
                .copied()
                .flatten(),
        );
    }
    if let Some(semantic) = selected_semantic {
        output.push_str(&format!(
            "transition: semantic:{} -> function-roots; sort: {}; showing: {} of {function_total}; truncated: {}; sample_basis: sampled-cpu\n",
            semantic.semantic_id,
            function_sort.as_str(),
            function_rows.len(),
            function_rows.len() < function_total,
        ));
        for function in function_rows {
            write_function_row(&mut output, 1, function, semantic.direct_sample_count);
        }
    }
    Ok(output)
}

fn contextual_semantic_matches(
    index: &TerminalProfileIndex<'_>,
    selected_id: Option<i64>,
    max_depth: usize,
    filter: &str,
) -> HashSet<i64> {
    let first_depth = usize::from(selected_id.is_some());
    if first_depth > max_depth {
        return HashSet::new();
    }
    let mut stack = index
        .semantic_children
        .get(&selected_id)
        .into_iter()
        .flatten()
        .map(|semantic| (first_depth, *semantic))
        .collect::<Vec<_>>();
    let mut included = HashSet::new();
    while let Some((depth, semantic)) = stack.pop() {
        if semantic_matches(semantic, filter) {
            let mut id = semantic.semantic_id;
            loop {
                if Some(id) == selected_id {
                    break;
                }
                included.insert(id);
                let Some(parent_id) = index
                    .semantics
                    .get(&id)
                    .and_then(|semantic| semantic.parent_semantic_id)
                else {
                    break;
                };
                id = parent_id;
            }
        }
        if depth < max_depth {
            stack.extend(
                index
                    .semantic_children
                    .get(&Some(semantic.semantic_id))
                    .into_iter()
                    .flatten()
                    .map(|child| (depth + 1, *child)),
            );
        }
    }
    included
}

fn semantic_matches(semantic: &RankedSemantic, filter: &str) -> bool {
    if let Some(id) = filter.strip_prefix("semantic:") {
        return id.parse::<i64>() == Ok(semantic.semantic_id);
    }
    [
        Some(semantic.name.as_str()),
        Some(semantic.semantic_kind.as_str()),
        semantic.result.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|value| value.contains(filter))
}

fn collect_semantic_rows<'a>(
    children: &HashMap<Option<i64>, Vec<&'a RankedSemantic>>,
    parent_semantic_id: Option<i64>,
    first_depth: usize,
    max_depth: usize,
    limit: usize,
    sort: InspectSort,
    included: Option<&HashSet<i64>>,
) -> (usize, Vec<(usize, &'a RankedSemantic)>) {
    if first_depth > max_depth {
        return (0, Vec::new());
    }
    let mut stack = Vec::new();
    push_sorted_siblings(children, parent_semantic_id, first_depth, sort, &mut stack);
    let mut total = 0;
    let mut rows = Vec::with_capacity(limit);
    while let Some((depth, semantic)) = stack.pop() {
        if included.is_none_or(|included| included.contains(&semantic.semantic_id)) {
            total += 1;
            if rows.len() < limit {
                rows.push((depth, semantic));
            }
        }
        if depth < max_depth {
            push_sorted_siblings(
                children,
                Some(semantic.semantic_id),
                depth + 1,
                sort,
                &mut stack,
            );
        }
    }
    (total, rows)
}

fn push_sorted_siblings<'a>(
    children: &HashMap<Option<i64>, Vec<&'a RankedSemantic>>,
    parent_semantic_id: Option<i64>,
    depth: usize,
    sort: InspectSort,
    stack: &mut Vec<(usize, &'a RankedSemantic)>,
) {
    let Some(siblings) = children.get(&parent_semantic_id) else {
        return;
    };
    let mut siblings = siblings.clone();
    siblings.sort_unstable_by(|left, right| compare_semantics(left, right, sort));
    stack.extend(siblings.into_iter().rev().map(|semantic| (depth, semantic)));
}

fn compare_semantics(left: &RankedSemantic, right: &RankedSemantic, sort: InspectSort) -> Ordering {
    let ordering = match sort {
        InspectSort::Duration => right
            .duration_ns
            .unwrap_or_default()
            .cmp(&left.duration_ns.unwrap_or_default()),
        InspectSort::InclusiveCpu => right
            .inclusive_sample_count
            .cmp(&left.inclusive_sample_count),
        InspectSort::SelfCpu => right.direct_sample_count.cmp(&left.direct_sample_count),
        InspectSort::Name => left.name.cmp(&right.name),
    };
    ordering.then_with(|| left.semantic_id.cmp(&right.semantic_id))
}

fn render_function_view(
    index: &TerminalProfileIndex<'_>,
    semantic_id: i64,
    function_id: i64,
    sort: InspectSort,
    filter: Option<&str>,
    limit: usize,
    max_depth: usize,
) -> Result<String, TerminalInspectError> {
    let selected = index
        .functions
        .get(&(semantic_id, function_id))
        .copied()
        .ok_or(TerminalInspectError::FunctionNotFound {
            semantic_id,
            function_id,
        })?;
    let included_functions = filter.map(|filter| {
        contextual_function_matches(index, semantic_id, function_id, max_depth, filter)
    });
    let (total, rows) = collect_function_rows(
        &index.function_children,
        (semantic_id, Some(function_id)),
        1,
        max_depth,
        limit,
        sort,
        included_functions.as_ref(),
    );
    let filter = filter_label(filter);
    let mut output = format!(
        "view: ranked-profile\ncontext: function:{semantic_id}:{function_id}\nsort: {}\nfilter: {filter}\ndepth: {max_depth}\nshowing: {} of {total}; truncated: {}\nsample_unit: {}\nmetric_basis: sampled-cpu; exact_wall_time: not-applicable\n",
        sort.as_str(),
        rows.len(),
        rows.len() < total,
        terminal_text(&index.document.metadata.sample_unit),
    );
    for (depth, function) in rows {
        write_function_row(
            &mut output,
            depth,
            function,
            selected.inclusive_sample_count,
        );
    }
    Ok(output)
}

fn contextual_function_matches(
    index: &TerminalProfileIndex<'_>,
    semantic_id: i64,
    selected_id: i64,
    max_depth: usize,
    filter: &str,
) -> HashSet<i64> {
    if max_depth == 0 {
        return HashSet::new();
    }
    let mut stack = index
        .function_children
        .get(&(semantic_id, Some(selected_id)))
        .into_iter()
        .flatten()
        .map(|function| (1, *function))
        .collect::<Vec<_>>();
    let mut included = HashSet::new();
    while let Some((depth, function)) = stack.pop() {
        if function_matches(function, filter) {
            let mut id = function.function_id;
            loop {
                if id == selected_id {
                    break;
                }
                included.insert(id);
                let Some(parent_id) = index
                    .functions
                    .get(&(semantic_id, id))
                    .and_then(|function| function.parent_function_id)
                else {
                    break;
                };
                id = parent_id;
            }
        }
        if depth < max_depth {
            stack.extend(
                index
                    .function_children
                    .get(&(semantic_id, Some(function.function_id)))
                    .into_iter()
                    .flatten()
                    .map(|child| (depth + 1, *child)),
            );
        }
    }
    included
}

fn function_matches(function: &RankedFunction, filter: &str) -> bool {
    if let Some(identity) = filter.strip_prefix("function:") {
        let Some((semantic_id, function_id)) = identity.split_once(':') else {
            return false;
        };
        return semantic_id.parse::<i64>() == Ok(function.semantic_id)
            && function_id.parse::<i64>() == Ok(function.function_id);
    }
    [
        Some(function.name.as_str()),
        function.module_name.as_deref(),
        function.source_file.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|value| value.contains(filter))
}

fn collect_function_rows<'a>(
    children: &HashMap<(i64, Option<i64>), Vec<&'a RankedFunction>>,
    parent: (i64, Option<i64>),
    first_depth: usize,
    max_depth: usize,
    limit: usize,
    sort: InspectSort,
    included: Option<&HashSet<i64>>,
) -> (usize, Vec<(usize, &'a RankedFunction)>) {
    let (semantic_id, parent_function_id) = parent;
    if first_depth > max_depth {
        return (0, Vec::new());
    }
    let mut stack = Vec::new();
    push_sorted_function_siblings(
        children,
        semantic_id,
        parent_function_id,
        first_depth,
        sort,
        &mut stack,
    );
    let mut total = 0;
    let mut rows = Vec::with_capacity(limit);
    while let Some((depth, function)) = stack.pop() {
        if included.is_none_or(|included| included.contains(&function.function_id)) {
            total += 1;
            if rows.len() < limit {
                rows.push((depth, function));
            }
        }
        if depth < max_depth {
            push_sorted_function_siblings(
                children,
                semantic_id,
                Some(function.function_id),
                depth + 1,
                sort,
                &mut stack,
            );
        }
    }
    (total, rows)
}

fn push_sorted_function_siblings<'a>(
    children: &HashMap<(i64, Option<i64>), Vec<&'a RankedFunction>>,
    semantic_id: i64,
    parent_function_id: Option<i64>,
    depth: usize,
    sort: InspectSort,
    stack: &mut Vec<(usize, &'a RankedFunction)>,
) {
    let Some(siblings) = children.get(&(semantic_id, parent_function_id)) else {
        return;
    };
    let mut siblings = siblings.clone();
    siblings.sort_unstable_by(|left, right| compare_functions(left, right, sort));
    stack.extend(siblings.into_iter().rev().map(|function| (depth, function)));
}

fn compare_functions(left: &RankedFunction, right: &RankedFunction, sort: InspectSort) -> Ordering {
    let ordering = match sort {
        InspectSort::Duration | InspectSort::InclusiveCpu => right
            .inclusive_sample_count
            .cmp(&left.inclusive_sample_count),
        InspectSort::SelfCpu => right.self_sample_count.cmp(&left.self_sample_count),
        InspectSort::Name => left.name.cmp(&right.name),
    };
    ordering
        .then_with(|| left.semantic_id.cmp(&right.semantic_id))
        .then_with(|| left.function_id.cmp(&right.function_id))
}

fn write_function_row(
    output: &mut String,
    depth: usize,
    function: &RankedFunction,
    context_sample_count: i64,
) {
    let module = function
        .module_name
        .as_deref()
        .map_or_else(|| "null".to_owned(), quoted_terminal_text);
    let source = function
        .source_file
        .as_deref()
        .map_or_else(|| "null".to_owned(), quoted_terminal_text);
    let line = function
        .line_number
        .map_or_else(|| "null".to_owned(), |line| line.to_string());
    output.push_str(&format!(
        "function depth={depth} id=function:{}:{} symbol={} module={module} source={source} line={line} inclusive_cpu_samples={} inclusive_context_percent={} self_cpu_samples={} self_context_percent={}\n",
        function.semantic_id,
        function.function_id,
        quoted_terminal_text(&function.name),
        function.inclusive_sample_count,
        sample_percent(function.inclusive_sample_count, context_sample_count),
        function.self_sample_count,
        sample_percent(function.self_sample_count, context_sample_count),
    ));
}

fn sample_percent(sample_count: i64, context_sample_count: i64) -> String {
    if context_sample_count > 0 {
        format!(
            "{:.2}%",
            sample_count as f64 * 100.0 / context_sample_count as f64
        )
    } else {
        "n/a".to_owned()
    }
}

fn write_semantic_row(
    output: &mut String,
    depth: usize,
    semantic: &RankedSemantic,
    operation_duration_ns: Option<i64>,
) {
    let duration = semantic
        .duration_ns
        .map_or_else(|| "n/a".to_owned(), |value| value.to_string());
    let wall_percent = match (semantic.duration_ns, operation_duration_ns) {
        (Some(duration), Some(operation_duration)) if operation_duration > 0 => {
            format!(
                "{:.2}%",
                duration as f64 * 100.0 / operation_duration as f64
            )
        }
        _ => "n/a".to_owned(),
    };
    let result = semantic
        .result
        .as_deref()
        .map_or_else(|| "null".to_owned(), quoted_terminal_text);
    output.push_str(&format!(
        "semantic depth={depth} id=semantic:{} name={} kind={} duration_ns={duration} time_basis=exact:{} operation_wall_percent={wall_percent} complete={} result={result} direct_cpu_samples={} inclusive_cpu_samples={}",
        semantic.semantic_id,
        quoted_terminal_text(&semantic.name),
        quoted_terminal_text(&semantic.semantic_kind),
        terminal_text(&semantic.time_semantics),
        semantic.is_complete,
        semantic.direct_sample_count,
        semantic.inclusive_sample_count,
    ));
    output.push('\n');
}

fn quoted_terminal_text(value: &str) -> String {
    format!("\"{}\"", terminal_text(value))
}

fn filter_label(filter: Option<&str>) -> String {
    filter.map_or_else(|| "none".to_owned(), quoted_terminal_text)
}

fn terminal_text(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            character if is_unsafe_terminal_character(character) => {
                output.push_str(&format!("\\u{{{:X}}}", u32::from(character)));
            }
            character => output.push(character),
        }
    }
    output
}

fn is_unsafe_terminal_character(character: char) -> bool {
    character.is_control()
        || matches!(
            character,
            '\u{061c}'
                | '\u{200b}'..='\u{200f}'
                | '\u{2028}'..='\u{2029}'
                | '\u{202a}'..='\u{202e}'
                | '\u{2060}'..='\u{2069}'
                | '\u{feff}'
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::perfetto_profile::ranked_report::{
        RankedFunction, RankedProfileMetadata, RankedSemantic,
    };

    fn operation(semantic_id: i64, name: &str, duration_ns: Option<i64>) -> RankedSemantic {
        RankedSemantic {
            semantic_id,
            parent_semantic_id: None,
            operation_id: semantic_id,
            name: name.to_owned(),
            semantic_kind: "operation".to_owned(),
            operation_kind: Some("preview".to_owned()),
            stage_category: None,
            stage_name: None,
            activity: None,
            start_ns: 0,
            end_ns: duration_ns,
            duration_ns,
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
            direct_sample_count: semantic_id,
            inclusive_sample_count: semantic_id + 1,
        }
    }

    fn document(semantics: Vec<RankedSemantic>) -> RankedProfileDocument {
        RankedProfileDocument {
            metadata: RankedProfileMetadata {
                schema_version: 1,
                sample_frequency_hz: 100,
                exact_time_unit: "nanoseconds".to_owned(),
                sample_unit: "samples".to_owned(),
                eligible_sample_count: 0,
                direct_sample_count: 0,
                ambiguous_sample_count: 0,
                unattributed_sample_count: 0,
            },
            semantics,
            functions: Vec::new(),
        }
    }

    fn function(
        semantic_id: i64,
        function_id: i64,
        parent_function_id: Option<i64>,
        name: &str,
        self_sample_count: i64,
        inclusive_sample_count: i64,
    ) -> RankedFunction {
        RankedFunction {
            semantic_id,
            function_id,
            parent_function_id,
            name: name.to_owned(),
            module_name: Some("delta_funnel".to_owned()),
            source_file: Some("src/lib.rs".to_owned()),
            line_number: Some(42),
            self_sample_count,
            inclusive_sample_count,
        }
    }

    #[test]
    fn ranks_and_bounds_operation_roots_with_terminal_safe_text() {
        let document = document(vec![
            operation(3, "safe lambda", Some(10)),
            operation(1, "slow\u{1b}[31m\u{2028}\u{202e}\"operation", Some(30)),
            operation(2, "safe snowman \u{2603}", Some(30)),
        ]);
        let rendered = render_terminal_view(
            &document,
            InspectSelection::Root,
            InspectSort::Duration,
            None,
            2,
            0,
        )
        .expect("root view should render");

        assert!(rendered.contains("showing: 2 of 3; truncated: true"));
        let first = rendered.find("id=semantic:1").expect("first root");
        let second = rendered.find("id=semantic:2").expect("second root");
        assert!(first < second);
        assert!(rendered.contains(r#"name="slow\u{1B}[31m\u{2028}\u{202E}\"operation""#));
        assert!(rendered.contains(&format!("name=\"safe snowman {}\"", '\u{2603}')));
        assert!(!rendered.contains('\u{1b}'));
        assert!(!rendered.contains('\u{2028}'));
        assert!(!rendered.contains('\u{202e}'));
    }

    #[test]
    fn selects_exact_semantic_ids_and_sorts_each_bounded_level() {
        let mut root = operation(1, "operation", Some(100));
        root.inclusive_sample_count = 20;
        let mut first = operation(2, "first", Some(60));
        first.parent_semantic_id = Some(1);
        first.operation_id = 1;
        first.direct_sample_count = 5;
        first.inclusive_sample_count = 8;
        let mut second = operation(3, "second", Some(80));
        second.parent_semantic_id = Some(1);
        second.operation_id = 1;
        second.direct_sample_count = 2;
        second.inclusive_sample_count = 10;
        let mut grandchild = operation(4, "grandchild", Some(20));
        grandchild.parent_semantic_id = Some(3);
        grandchild.operation_id = 1;

        let document = document(vec![first, grandchild, root, second]);
        let rendered = render_terminal_view(
            &document,
            InspectSelection::Semantic(1),
            InspectSort::InclusiveCpu,
            None,
            10,
            2,
        )
        .expect("selected view should render");

        assert!(rendered.contains("context: semantic:1"));
        assert!(rendered.contains("showing: 3 of 3; truncated: false"));
        let second = rendered.find("id=semantic:3").expect("second child");
        let grandchild = rendered.find("id=semantic:4").expect("grandchild");
        let first = rendered.find("id=semantic:2").expect("first child");
        assert!(second < grandchild);
        assert!(grandchild < first);
        assert!(rendered.contains(
            "id=semantic:3 name=\"second\" kind=\"operation\" duration_ns=80 time_basis=exact:wall_clock operation_wall_percent=80.00%"
        ));
        assert_eq!(
            render_terminal_view(
                &document,
                InspectSelection::Semantic(99),
                InspectSort::Duration,
                None,
                10,
                1,
            ),
            Err(TerminalInspectError::SemanticNotFound(99))
        );
    }

    #[test]
    fn filters_exact_identities_and_retains_contextual_ancestors() {
        let root = operation(1, "operation", Some(100));
        let other_root = operation(10, "other operation", Some(90));
        let mut parent = operation(2, "parent", Some(60));
        parent.parent_semantic_id = Some(1);
        parent.operation_id = 1;
        let mut match_node = operation(3, "needle", Some(20));
        match_node.parent_semantic_id = Some(2);
        match_node.operation_id = 1;
        let document = document(vec![root, other_root, parent, match_node]);

        let contextual = render_terminal_view(
            &document,
            InspectSelection::Semantic(1),
            InspectSort::Duration,
            Some("needle"),
            10,
            2,
        )
        .expect("filtered semantics should render");
        assert!(contextual.contains("filter: \"needle\""));
        assert!(contextual.contains("id=semantic:2 "));
        assert!(contextual.contains("id=semantic:3 "));

        let exact = render_terminal_view(
            &document,
            InspectSelection::Root,
            InspectSort::Duration,
            Some("semantic:1"),
            10,
            0,
        )
        .expect("exact identity filter should render");
        assert!(exact.contains("id=semantic:1 "));
        assert!(!exact.contains("id=semantic:10 "));
    }

    #[test]
    fn transitions_from_semantics_to_exact_function_callsites() {
        let mut root = operation(1, "operation", Some(100));
        root.direct_sample_count = 10;
        root.inclusive_sample_count = 10;
        let mut document = document(vec![root]);
        document.functions = vec![
            function(1, 20, None, "second root", 4, 4),
            function(1, 12, Some(11), "leaf", 3, 3),
            function(1, 10, None, "first root", 1, 6),
            function(1, 11, Some(10), "child\u{1b}", 2, 5),
            function(1, 13, Some(10), "other child", 0, 0),
        ];

        let semantic = render_terminal_view(
            &document,
            InspectSelection::Semantic(1),
            InspectSort::Duration,
            None,
            10,
            1,
        )
        .expect("semantic functions should render");
        assert!(semantic.contains(
            "transition: semantic:1 -> function-roots; sort: inclusive-cpu; showing: 2 of 2; truncated: false; sample_basis: sampled-cpu"
        ));
        let first = semantic
            .find("id=function:1:10")
            .expect("largest function root");
        let second = semantic
            .find("id=function:1:20")
            .expect("smaller function root");
        assert!(first < second);
        assert!(semantic.contains("inclusive_context_percent=60.00%"));

        let depth_zero = render_terminal_view(
            &document,
            InspectSelection::Semantic(1),
            InspectSort::Duration,
            None,
            10,
            0,
        )
        .expect("depth zero semantic view should render");
        assert!(depth_zero.contains("depth: 0"));
        assert!(!depth_zero.contains("function depth="));

        let function = render_terminal_view(
            &document,
            InspectSelection::Function {
                semantic_id: 1,
                function_id: 10,
            },
            InspectSort::InclusiveCpu,
            None,
            10,
            2,
        )
        .expect("function children should render");
        assert!(function.contains("context: function:1:10"));
        assert!(function.contains("metric_basis: sampled-cpu; exact_wall_time: not-applicable"));
        assert!(function.contains("id=function:1:11"));
        assert!(function.contains("id=function:1:12"));
        assert!(function.contains("inclusive_context_percent=83.33%"));
        assert!(function.contains(r#"symbol="child\u{1B}""#));

        let filtered = render_terminal_view(
            &document,
            InspectSelection::Function {
                semantic_id: 1,
                function_id: 10,
            },
            InspectSort::InclusiveCpu,
            Some("leaf"),
            10,
            2,
        )
        .expect("filtered function children should render");
        assert!(filtered.contains("id=function:1:11 "));
        assert!(filtered.contains("id=function:1:12 "));
        assert!(!filtered.contains("id=function:1:13 "));

        let exact = render_terminal_view(
            &document,
            InspectSelection::Function {
                semantic_id: 1,
                function_id: 10,
            },
            InspectSort::InclusiveCpu,
            Some("function:1:11"),
            10,
            2,
        )
        .expect("exact function filter should render");
        assert!(exact.contains("id=function:1:11 "));
        assert!(!exact.contains("id=function:1:12 "));
        assert!(!exact.contains("id=function:1:13 "));

        assert_eq!(
            render_terminal_view(
                &document,
                InspectSelection::Function {
                    semantic_id: 2,
                    function_id: 10,
                },
                InspectSort::InclusiveCpu,
                None,
                10,
                1,
            ),
            Err(TerminalInspectError::FunctionNotFound {
                semantic_id: 2,
                function_id: 10,
            })
        );
    }
}
