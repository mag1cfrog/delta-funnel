use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;

use clap::ValueEnum;

use super::ranked_report::{RankedProfileDocument, RankedSemantic};

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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TerminalInspectError {
    SemanticNotFound(i64),
}

impl TerminalInspectError {
    pub(super) const fn kind(self) -> &'static str {
        match self {
            Self::SemanticNotFound(_) => "semantic_not_found",
        }
    }
}

impl fmt::Display for TerminalInspectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SemanticNotFound(semantic_id) => {
                write!(formatter, "semantic:{semantic_id} does not exist")
            }
        }
    }
}

impl std::error::Error for TerminalInspectError {}

pub(super) fn render_semantic_view(
    document: &RankedProfileDocument,
    selected_semantic_id: Option<i64>,
    sort: InspectSort,
    limit: usize,
    max_depth: usize,
) -> Result<String, TerminalInspectError> {
    if let Some(semantic_id) = selected_semantic_id
        && !document
            .semantics
            .iter()
            .any(|semantic| semantic.semantic_id == semantic_id)
    {
        return Err(TerminalInspectError::SemanticNotFound(semantic_id));
    }

    let mut children = HashMap::<Option<i64>, Vec<&RankedSemantic>>::new();
    for semantic in &document.semantics {
        children
            .entry(semantic.parent_semantic_id)
            .or_default()
            .push(semantic);
    }

    let first_depth = usize::from(selected_semantic_id.is_some());
    let (total, rows) = collect_semantic_rows(
        &mut children,
        selected_semantic_id,
        first_depth,
        max_depth,
        limit,
        sort,
    );

    let operation_durations = children
        .get(&None)
        .into_iter()
        .flatten()
        .map(|semantic| (semantic.operation_id, semantic.duration_ns))
        .collect::<HashMap<_, _>>();
    let context = selected_semantic_id.map_or_else(
        || "operation-roots".to_owned(),
        |semantic_id| format!("semantic:{semantic_id}"),
    );
    let mut output = format!(
        "view: ranked-profile\ncontext: {context}\nsort: {}\nfilter: none\ndepth: {max_depth}\nshowing: {} of {total}; truncated: {}\ntime_unit: {}\nsample_unit: {}\n",
        sort.as_str(),
        rows.len(),
        rows.len() < total,
        terminal_text(&document.metadata.exact_time_unit),
        terminal_text(&document.metadata.sample_unit),
    );
    for (depth, semantic) in rows {
        write_semantic_row(
            &mut output,
            depth,
            semantic,
            operation_durations
                .get(&semantic.operation_id)
                .copied()
                .flatten(),
        );
    }
    Ok(output)
}

fn collect_semantic_rows<'a>(
    children: &mut HashMap<Option<i64>, Vec<&'a RankedSemantic>>,
    parent_semantic_id: Option<i64>,
    first_depth: usize,
    max_depth: usize,
    limit: usize,
    sort: InspectSort,
) -> (usize, Vec<(usize, &'a RankedSemantic)>) {
    if first_depth > max_depth {
        return (0, Vec::new());
    }
    let mut stack = Vec::new();
    push_sorted_siblings(children, parent_semantic_id, first_depth, sort, &mut stack);
    let mut total = 0;
    let mut rows = Vec::with_capacity(limit);
    while let Some((depth, semantic)) = stack.pop() {
        total += 1;
        if rows.len() < limit {
            rows.push((depth, semantic));
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
    children: &mut HashMap<Option<i64>, Vec<&'a RankedSemantic>>,
    parent_semantic_id: Option<i64>,
    depth: usize,
    sort: InspectSort,
    stack: &mut Vec<(usize, &'a RankedSemantic)>,
) {
    let Some(siblings) = children.get_mut(&parent_semantic_id) else {
        return;
    };
    siblings.sort_unstable_by(|left, right| compare_semantics(left, right, sort));
    stack.extend(siblings.iter().rev().map(|semantic| (depth, *semantic)));
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
    use crate::perfetto_profile::ranked_report::{RankedProfileMetadata, RankedSemantic};

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

    #[test]
    fn ranks_and_bounds_operation_roots_with_terminal_safe_text() {
        let rendered = render_semantic_view(
            &document(vec![
                operation(3, "safe lambda", Some(10)),
                operation(1, "slow\u{1b}[31m\u{2028}\u{202e}\"operation", Some(30)),
                operation(2, "safe snowman \u{2603}", Some(30)),
            ]),
            None,
            InspectSort::Duration,
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
        let rendered = render_semantic_view(&document, Some(1), InspectSort::InclusiveCpu, 10, 2)
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
            render_semantic_view(&document, Some(99), InspectSort::Duration, 10, 1),
            Err(TerminalInspectError::SemanticNotFound(99))
        );
    }
}
