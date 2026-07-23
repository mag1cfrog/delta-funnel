use super::ranked_report::{RankedProfileDocument, RankedSemantic};

pub(super) fn render_operation_roots(document: &RankedProfileDocument, limit: usize) -> String {
    let mut roots = document
        .semantics
        .iter()
        .filter(|semantic| semantic.parent_semantic_id.is_none())
        .collect::<Vec<_>>();
    roots.sort_by(|left, right| {
        right
            .duration_ns
            .unwrap_or_default()
            .cmp(&left.duration_ns.unwrap_or_default())
            .then_with(|| left.semantic_id.cmp(&right.semantic_id))
    });

    let shown = roots.len().min(limit);
    let mut output = format!(
        "view: ranked-profile\ncontext: operation-roots\nsort: duration\nfilter: none\nshowing: {shown} of {}; truncated: {}\ntime_unit: {}\nsample_unit: {}\n",
        roots.len(),
        shown < roots.len(),
        terminal_text(&document.metadata.exact_time_unit),
        terminal_text(&document.metadata.sample_unit),
    );
    for root in roots.into_iter().take(shown) {
        write_semantic_row(&mut output, root);
    }
    output
}

fn write_semantic_row(output: &mut String, semantic: &RankedSemantic) {
    let duration = semantic
        .duration_ns
        .map_or_else(|| "n/a".to_owned(), |value| value.to_string());
    let wall_percent = semantic
        .duration_ns
        .filter(|duration| *duration > 0)
        .map_or("n/a", |_| "100.00%");
    let result = semantic
        .result
        .as_deref()
        .map_or_else(|| "null".to_owned(), quoted_terminal_text);
    output.push_str(&format!(
        "semantic id=semantic:{} name={} kind={} duration_ns={duration} time_basis=exact:{} operation_wall_percent={wall_percent} complete={} result={result} direct_cpu_samples={} inclusive_cpu_samples={}",
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
        let rendered = render_operation_roots(
            &document(vec![
                operation(3, "safe lambda", Some(10)),
                operation(1, "slow\u{1b}[31m\u{2028}\u{202e}\"operation", Some(30)),
                operation(2, "safe snowman \u{2603}", Some(30)),
            ]),
            2,
        );

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
}
