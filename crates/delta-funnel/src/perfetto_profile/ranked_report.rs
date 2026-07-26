use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

// This covers the 246,095-node production fixture while bounding
// report memory. Raise it only with production and browser evidence.
pub(super) const MAX_RECORDS_PER_COLLECTION: usize = 500_000;
const MAX_DISPLAY_STRING_CHARS: usize = 512;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RankedProfileMetadata {
    pub capture_complete: bool,
    pub semantic_complete: bool,
    pub schema_version: u32,
    pub sample_frequency_hz: u32,
    pub sampled_cpu_count: u32,
    pub exact_time_unit: String,
    pub sample_unit: String,
    pub eligible_sample_count: i64,
    pub direct_sample_count: i64,
    pub ambiguous_sample_count: i64,
    pub unattributed_sample_count: i64,
    pub resolved_function_sample_count: i64,
    pub unresolved_function_sample_count: i64,
    pub unwind_error_sample_count: i64,
    pub missing_callstack_sample_count: i64,
    pub trace_profiler_dropped_sample_count: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RankedSemantic {
    pub semantic_id: i64,
    pub parent_semantic_id: Option<i64>,
    pub operation_id: i64,
    pub name: String,
    pub semantic_kind: String,
    pub operation_kind: Option<String>,
    pub stage_category: Option<String>,
    pub stage_name: Option<String>,
    pub activity: Option<String>,
    pub start_ns: i64,
    pub end_ns: Option<i64>,
    pub duration_ns: Option<i64>,
    pub time_semantics: String,
    pub result: Option<String>,
    pub is_complete: bool,
    pub query_execution_id: Option<i64>,
    pub query_scope: Option<String>,
    pub query_owner: Option<String>,
    pub worker_lane_id: Option<i64>,
    pub worker_kind: Option<String>,
    pub node_id: Option<i64>,
    pub parent_node_id: Option<i64>,
    pub operator_partition: Option<i64>,
    pub execution_stream_id: Option<i64>,
    pub stage_owner_id: Option<i64>,
    pub direct_sample_count: i64,
    pub inclusive_sample_count: i64,
    pub resolved_function_sample_count: i64,
    pub unresolved_function_sample_count: i64,
    pub unwind_error_sample_count: i64,
    pub missing_callstack_sample_count: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct RankedFunction {
    pub semantic_id: i64,
    pub function_id: i64,
    pub parent_function_id: Option<i64>,
    pub name: String,
    pub module_name: Option<String>,
    pub source_file: Option<String>,
    pub line_number: Option<i64>,
    pub self_sample_count: i64,
    pub inclusive_sample_count: i64,
}

impl RankedFunction {
    pub(super) fn display_name(&self) -> String {
        compact_function_name(&self.name)
    }
}

pub(super) struct CompactFunctionTree {
    parents: HashMap<(i64, i64), Option<i64>>,
}

impl CompactFunctionTree {
    pub(super) fn new(functions: &[RankedFunction]) -> Self {
        let parents = functions
            .iter()
            .map(|function| {
                (
                    (function.semantic_id, function.function_id),
                    function.parent_function_id,
                )
            })
            .collect::<HashMap<_, _>>();
        let mut children = HashMap::<(i64, i64), (usize, i64)>::new();
        for function in functions {
            let Some(parent_id) = function.parent_function_id else {
                continue;
            };
            let child = children
                .entry((function.semantic_id, parent_id))
                .or_insert((0, function.inclusive_sample_count));
            child.0 += 1;
            child.1 = function.inclusive_sample_count;
        }
        let collapsed = functions
            .iter()
            .filter(|function| {
                function.self_sample_count == 0
                    && children
                        .get(&(function.semantic_id, function.function_id))
                        .is_some_and(|(count, child_inclusive)| {
                            *count == 1 && *child_inclusive == function.inclusive_sample_count
                        })
            })
            .map(|function| (function.semantic_id, function.function_id))
            .collect::<HashSet<_>>();
        let mut retained_parents = HashMap::with_capacity(collapsed.len());
        let mut compact_parents =
            HashMap::with_capacity(functions.len().saturating_sub(collapsed.len()));
        for function in functions {
            let key = (function.semantic_id, function.function_id);
            if collapsed.contains(&key) {
                continue;
            }
            let parent = retained_function_parent(
                function.semantic_id,
                function.parent_function_id,
                &parents,
                &collapsed,
                &mut retained_parents,
            );
            compact_parents.insert(key, parent);
        }
        Self {
            parents: compact_parents,
        }
    }

    pub(super) fn contains(&self, function: &RankedFunction) -> bool {
        self.parents
            .contains_key(&(function.semantic_id, function.function_id))
    }

    pub(super) fn parent_function_id(&self, function: &RankedFunction) -> Option<i64> {
        self.parents
            .get(&(function.semantic_id, function.function_id))
            .copied()
            .flatten()
    }
}

fn retained_function_parent(
    semantic_id: i64,
    mut parent: Option<i64>,
    parents: &HashMap<(i64, i64), Option<i64>>,
    collapsed: &HashSet<(i64, i64)>,
    retained_parents: &mut HashMap<(i64, i64), Option<i64>>,
) -> Option<i64> {
    let mut path = Vec::new();
    while let Some(parent_id) = parent {
        let key = (semantic_id, parent_id);
        if !collapsed.contains(&key) {
            break;
        }
        if let Some(retained) = retained_parents.get(&key) {
            parent = *retained;
            break;
        }
        path.push(key);
        parent = parents.get(&key).copied().flatten();
    }
    for key in path {
        retained_parents.insert(key, parent);
    }
    parent
}

fn compact_function_name(symbol: &str) -> String {
    let symbol = symbol
        .rsplit_once(" (.llvm.")
        .filter(|(_, suffix)| {
            suffix
                .strip_suffix(')')
                .is_some_and(|suffix| suffix.bytes().all(|byte| byte.is_ascii_digit()))
        })
        .map_or(symbol, |(symbol, _)| symbol);
    let Some(method_separator) = last_top_level_path_separator(symbol) else {
        let display_name = trim_function_details(symbol);
        return if display_name.is_empty() {
            symbol.to_owned()
        } else {
            display_name.to_owned()
        };
    };
    let method = trim_function_details(&symbol[method_separator + 2..]);
    let owner = function_owner(&symbol[..method_separator]);
    if owner.is_empty() || method.is_empty() {
        symbol.to_owned()
    } else {
        format!("{owner}::{method}")
    }
}

fn function_owner(prefix: &str) -> &str {
    let prefix = prefix.trim();
    let owner = if prefix.starts_with('<') && prefix.ends_with('>') {
        let qualified = &prefix[1..prefix.len() - 1];
        split_top_level_as(qualified).unwrap_or(qualified)
    } else {
        prefix
    };
    let owner = last_top_level_path_separator(owner)
        .map_or(owner, |separator| &owner[separator + 2..])
        .trim_start_matches(['&', '*'])
        .trim_start_matches("mut ")
        .trim_start_matches("const ");
    owner.find('<').map_or(owner, |generic| &owner[..generic])
}

#[derive(Default)]
struct SymbolNesting {
    angles: u32,
    parentheses: u32,
    brackets: u32,
    braces: u32,
}

impl SymbolNesting {
    fn is_top_level(&self) -> bool {
        self.angles == 0 && self.is_outside_groups()
    }

    fn is_outside_groups(&self) -> bool {
        self.parentheses == 0 && self.brackets == 0 && self.braces == 0
    }

    fn advance(&mut self, value: &str, index: usize) {
        let bytes = value.as_bytes();
        match bytes[index] {
            b'<' if !is_operator_less_than(value, index) => {
                self.angles = self.angles.saturating_add(1);
            }
            b'>' if !is_directional_arrow(bytes, index) => {
                self.angles = self.angles.saturating_sub(1);
            }
            b'(' => self.parentheses = self.parentheses.saturating_add(1),
            b')' => self.parentheses = self.parentheses.saturating_sub(1),
            b'[' => self.brackets = self.brackets.saturating_add(1),
            b']' => self.brackets = self.brackets.saturating_sub(1),
            b'{' => self.braces = self.braces.saturating_add(1),
            b'}' => self.braces = self.braces.saturating_sub(1),
            _ => {}
        }
    }
}

fn split_top_level_as(value: &str) -> Option<&str> {
    let mut nesting = SymbolNesting::default();
    for (index, byte) in value.bytes().enumerate() {
        if byte == b' ' && nesting.is_top_level() && value[index..].starts_with(" as ") {
            return Some(&value[..index]);
        }
        nesting.advance(value, index);
    }
    None
}

fn last_top_level_path_separator(value: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut nesting = SymbolNesting::default();
    let mut last = None;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b':'
            && nesting.is_top_level()
            && bytes.get(index + 1) == Some(&b':')
            && bytes.get(index + 2) != Some(&b'<')
        {
            last = Some(index);
            index += 1;
        } else {
            nesting.advance(value, index);
        }
        index += 1;
    }
    last
}

fn trim_function_details(value: &str) -> &str {
    let mut nesting = SymbolNesting::default();
    for (index, byte) in value.bytes().enumerate() {
        if byte == b'<' && nesting.is_outside_groups() && !is_operator_less_than(value, index) {
            return value[..index].trim_end_matches("::");
        }
        if byte == b'(' && nesting.is_top_level() && !is_call_operator(value, index) {
            return value[..index].trim_end();
        }
        nesting.advance(value, index);
    }
    value
}

fn is_call_operator(value: &str, index: usize) -> bool {
    &value[..index] == "operator" && value.as_bytes().get(index + 1) == Some(&b')')
}

fn is_operator_less_than(value: &str, index: usize) -> bool {
    value[..index]
        .rsplit("::")
        .next()
        .unwrap_or_default()
        .strip_prefix("operator")
        .is_some_and(|operator| operator.bytes().all(|byte| byte == b'<'))
        && value
            .as_bytes()
            .get(index + 1)
            .is_none_or(|next| next.is_ascii_whitespace() || matches!(next, b'<' | b'=' | b'('))
}

fn is_directional_arrow(value: &[u8], index: usize) -> bool {
    index > 0 && matches!(value[index - 1], b'-' | b'=')
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct RankedProfileDocument {
    pub metadata: RankedProfileMetadata,
    pub semantics: Vec<RankedSemantic>,
    pub functions: Vec<RankedFunction>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum RankedProfileValidationError {
    TooManyRecords {
        record_kind: &'static str,
        count: usize,
        limit: usize,
    },
    DisplayStringTooLong {
        record_kind: &'static str,
        record_id: i64,
        field: &'static str,
        char_count: usize,
        limit: usize,
    },
    UnsafeFunctionMetadata {
        semantic_id: i64,
        function_id: i64,
        field: &'static str,
    },
    UnsupportedSchemaVersion {
        schema_version: u32,
    },
    InvalidSampleFrequency,
    InvalidUnit {
        field: &'static str,
        expected: &'static str,
    },
    InvalidSemanticTimeSemantics {
        semantic_id: i64,
    },
    InvalidSemanticInterval {
        semantic_id: i64,
        reason: &'static str,
    },
    MissingSemanticNodes,
    DuplicateSemanticId {
        semantic_id: i64,
    },
    MissingSemanticParent {
        semantic_id: i64,
        parent_semantic_id: i64,
    },
    CrossOperationSemanticParent {
        semantic_id: i64,
        parent_semantic_id: i64,
    },
    SemanticCycle {
        semantic_id: i64,
    },
    InvalidOperationRootCount {
        operation_id: i64,
        root_count: usize,
    },
    InvalidOperationRootKind {
        operation_id: i64,
        semantic_id: i64,
    },
    MissingFunctionOwner {
        semantic_id: i64,
        function_id: i64,
    },
    DuplicateFunctionId {
        semantic_id: i64,
        function_id: i64,
    },
    MissingFunctionParent {
        semantic_id: i64,
        function_id: i64,
        parent_function_id: i64,
    },
    CrossSemanticFunctionParent {
        semantic_id: i64,
        function_id: i64,
        parent_function_id: i64,
    },
    FunctionCycle {
        semantic_id: i64,
        function_id: i64,
    },
    NegativeSampleCount {
        record_kind: &'static str,
        record_id: i64,
        field: &'static str,
        value: i64,
    },
    SampleCountOverflow {
        scope: &'static str,
    },
    CoverageMismatch {
        eligible_sample_count: i64,
        classified_sample_count: i64,
    },
    DirectSampleMismatch {
        declared_sample_count: i64,
        semantic_sample_count: i64,
    },
    FunctionCoverageMismatch {
        semantic_id: Option<i64>,
        direct_sample_count: i64,
        classified_sample_count: i64,
    },
    FunctionCoverageSummaryMismatch {
        field: &'static str,
        declared_sample_count: i64,
        semantic_sample_count: i64,
    },
    SemanticInclusiveMismatch {
        semantic_id: i64,
        declared_sample_count: i64,
        computed_sample_count: i64,
    },
    FunctionSelfMismatch {
        semantic_id: i64,
        captured_sample_count: i64,
        function_sample_count: i64,
    },
    FunctionInclusiveMismatch {
        semantic_id: i64,
        function_id: i64,
        declared_sample_count: i64,
        computed_sample_count: i64,
    },
}

impl fmt::Display for RankedProfileValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyRecords {
                record_kind,
                count,
                limit,
            } => write!(
                formatter,
                "profile has {count} {record_kind} records, exceeding the {limit} record limit"
            ),
            Self::DisplayStringTooLong {
                record_kind,
                record_id,
                field,
                char_count,
                limit,
            } => write!(
                formatter,
                "{record_kind} ID {record_id} {field} has {char_count} characters, exceeding the {limit} character limit"
            ),
            Self::UnsafeFunctionMetadata {
                semantic_id,
                function_id,
                field,
            } => write!(
                formatter,
                "function ID {function_id} under semantic ID {semantic_id} has unsafe {field}"
            ),
            Self::UnsupportedSchemaVersion { schema_version } => {
                write!(
                    formatter,
                    "profile schema version {schema_version} is unsupported"
                )
            }
            Self::InvalidSampleFrequency => {
                write!(
                    formatter,
                    "profile sample frequency must be greater than zero"
                )
            }
            Self::InvalidUnit { field, expected } => {
                write!(formatter, "profile {field} must be {expected}")
            }
            Self::InvalidSemanticTimeSemantics { semantic_id } => write!(
                formatter,
                "semantic ID {semantic_id} has invalid time semantics"
            ),
            Self::InvalidSemanticInterval {
                semantic_id,
                reason,
            } => write!(
                formatter,
                "semantic ID {semantic_id} has an invalid interval: {reason}"
            ),
            Self::MissingSemanticNodes => write!(formatter, "profile has no semantic nodes"),
            Self::DuplicateSemanticId { semantic_id } => {
                write!(formatter, "semantic ID {semantic_id} is duplicated")
            }
            Self::MissingSemanticParent {
                semantic_id,
                parent_semantic_id,
            } => write!(
                formatter,
                "semantic ID {semantic_id} has missing parent {parent_semantic_id}"
            ),
            Self::CrossOperationSemanticParent {
                semantic_id,
                parent_semantic_id,
            } => write!(
                formatter,
                "semantic ID {semantic_id} has cross-operation parent {parent_semantic_id}"
            ),
            Self::SemanticCycle { semantic_id } => {
                write!(formatter, "semantic ID {semantic_id} belongs to a cycle")
            }
            Self::InvalidOperationRootCount {
                operation_id,
                root_count,
            } => write!(
                formatter,
                "operation ID {operation_id} has {root_count} semantic roots"
            ),
            Self::InvalidOperationRootKind {
                operation_id,
                semantic_id,
            } => write!(
                formatter,
                "semantic ID {semantic_id} violates operation ID {operation_id} root policy"
            ),
            Self::MissingFunctionOwner {
                semantic_id,
                function_id,
            } => write!(
                formatter,
                "function ID {function_id} has missing semantic owner {semantic_id}"
            ),
            Self::DuplicateFunctionId {
                semantic_id,
                function_id,
            } => write!(
                formatter,
                "function ID {function_id} is duplicated under semantic ID {semantic_id}"
            ),
            Self::MissingFunctionParent {
                semantic_id,
                function_id,
                parent_function_id,
            } => write!(
                formatter,
                "function ID {function_id} under semantic ID {semantic_id} has missing parent {parent_function_id}"
            ),
            Self::CrossSemanticFunctionParent {
                semantic_id,
                function_id,
                parent_function_id,
            } => write!(
                formatter,
                "function ID {function_id} under semantic ID {semantic_id} has cross-semantic parent {parent_function_id}"
            ),
            Self::FunctionCycle {
                semantic_id,
                function_id,
            } => write!(
                formatter,
                "function ID {function_id} under semantic ID {semantic_id} belongs to a cycle"
            ),
            Self::NegativeSampleCount {
                record_kind,
                record_id,
                field,
                value,
            } => write!(
                formatter,
                "{record_kind} ID {record_id} has negative {field}: {value}"
            ),
            Self::SampleCountOverflow { scope } => {
                write!(formatter, "{scope} sample count overflowed")
            }
            Self::CoverageMismatch {
                eligible_sample_count,
                classified_sample_count,
            } => write!(
                formatter,
                "eligible sample count {eligible_sample_count} does not equal classified count {classified_sample_count}"
            ),
            Self::DirectSampleMismatch {
                declared_sample_count,
                semantic_sample_count,
            } => write!(
                formatter,
                "declared direct sample count {declared_sample_count} does not equal semantic count {semantic_sample_count}"
            ),
            Self::FunctionCoverageMismatch {
                semantic_id,
                direct_sample_count,
                classified_sample_count,
            } => write!(
                formatter,
                "{} direct sample count {direct_sample_count} does not equal classified native stack count {classified_sample_count}",
                semantic_id.map_or_else(
                    || "profile".to_owned(),
                    |semantic_id| { format!("semantic ID {semantic_id}") }
                )
            ),
            Self::FunctionCoverageSummaryMismatch {
                field,
                declared_sample_count,
                semantic_sample_count,
            } => write!(
                formatter,
                "declared {field} {declared_sample_count} does not equal semantic count {semantic_sample_count}"
            ),
            Self::SemanticInclusiveMismatch {
                semantic_id,
                declared_sample_count,
                computed_sample_count,
            } => write!(
                formatter,
                "semantic ID {semantic_id} inclusive count {declared_sample_count} does not equal computed count {computed_sample_count}"
            ),
            Self::FunctionSelfMismatch {
                semantic_id,
                captured_sample_count,
                function_sample_count,
            } => write!(
                formatter,
                "semantic ID {semantic_id} captured native stack count {captured_sample_count} does not equal function self count {function_sample_count}"
            ),
            Self::FunctionInclusiveMismatch {
                semantic_id,
                function_id,
                declared_sample_count,
                computed_sample_count,
            } => write!(
                formatter,
                "function ID {function_id} under semantic ID {semantic_id} inclusive count {declared_sample_count} does not equal computed count {computed_sample_count}"
            ),
        }
    }
}

impl std::error::Error for RankedProfileValidationError {}

impl RankedProfileDocument {
    pub fn normalize_source_metadata(&mut self) {
        for function in &mut self.functions {
            function.module_name = function
                .module_name
                .as_deref()
                .and_then(normalize_module_name);
            function.source_file = function
                .source_file
                .as_deref()
                .and_then(normalize_source_file);
        }
    }

    pub fn validate(&self) -> Result<(), RankedProfileValidationError> {
        self.validate_bounds()?;
        self.validate_metadata_and_intervals()?;
        self.validate_structure()?;
        self.validate_sample_counts()
    }

    fn validate_bounds(&self) -> Result<(), RankedProfileValidationError> {
        require_collection_bound("semantic", self.semantics.len())?;
        require_collection_bound("function", self.functions.len())?;

        for semantic in &self.semantics {
            for (field, value) in [
                ("name", Some(semantic.name.as_str())),
                ("semantic_kind", Some(semantic.semantic_kind.as_str())),
                ("operation_kind", semantic.operation_kind.as_deref()),
                ("stage_category", semantic.stage_category.as_deref()),
                ("stage_name", semantic.stage_name.as_deref()),
                ("activity", semantic.activity.as_deref()),
                ("time_semantics", Some(semantic.time_semantics.as_str())),
                ("result", semantic.result.as_deref()),
                ("query_scope", semantic.query_scope.as_deref()),
                ("query_owner", semantic.query_owner.as_deref()),
                ("worker_kind", semantic.worker_kind.as_deref()),
            ] {
                if let Some(value) = value {
                    require_display_string("semantic", semantic.semantic_id, field, value)?;
                }
            }
        }
        for function in &self.functions {
            for (field, value) in [
                ("module_name", function.module_name.as_deref()),
                ("source_file", function.source_file.as_deref()),
            ] {
                if let Some(value) = value {
                    require_display_string("function", function.function_id, field, value)?;
                }
            }
            for (field, value, normalized) in [
                (
                    "module_name",
                    function.module_name.as_deref(),
                    function
                        .module_name
                        .as_deref()
                        .and_then(normalize_module_name),
                ),
                (
                    "source_file",
                    function.source_file.as_deref(),
                    function
                        .source_file
                        .as_deref()
                        .and_then(normalize_source_file),
                ),
            ] {
                if value.is_some() && normalized.as_deref() != value {
                    return Err(RankedProfileValidationError::UnsafeFunctionMetadata {
                        semantic_id: function.semantic_id,
                        function_id: function.function_id,
                        field,
                    });
                }
            }
        }
        Ok(())
    }

    fn validate_metadata_and_intervals(&self) -> Result<(), RankedProfileValidationError> {
        if self.metadata.schema_version != 3 {
            return Err(RankedProfileValidationError::UnsupportedSchemaVersion {
                schema_version: self.metadata.schema_version,
            });
        }
        if self.metadata.sample_frequency_hz == 0 {
            return Err(RankedProfileValidationError::InvalidSampleFrequency);
        }
        for (field, actual, expected) in [
            (
                "exact_time_unit",
                self.metadata.exact_time_unit.as_str(),
                "nanoseconds",
            ),
            ("sample_unit", self.metadata.sample_unit.as_str(), "samples"),
        ] {
            if actual != expected {
                return Err(RankedProfileValidationError::InvalidUnit { field, expected });
            }
        }

        for semantic in &self.semantics {
            if !matches!(semantic.time_semantics.as_str(), "wall_clock" | "lifecycle") {
                return Err(RankedProfileValidationError::InvalidSemanticTimeSemantics {
                    semantic_id: semantic.semantic_id,
                });
            }
            match (semantic.is_complete, semantic.end_ns, semantic.duration_ns) {
                (false, None, None) => {}
                (false, _, _) => {
                    return Err(RankedProfileValidationError::InvalidSemanticInterval {
                        semantic_id: semantic.semantic_id,
                        reason: "incomplete interval has an end or duration",
                    });
                }
                (true, Some(end_ns), Some(duration_ns)) => {
                    let actual_duration = end_ns.checked_sub(semantic.start_ns);
                    if duration_ns < 0 || actual_duration != Some(duration_ns) {
                        return Err(RankedProfileValidationError::InvalidSemanticInterval {
                            semantic_id: semantic.semantic_id,
                            reason: "complete interval has inconsistent bounds",
                        });
                    }
                }
                (true, _, _) => {
                    return Err(RankedProfileValidationError::InvalidSemanticInterval {
                        semantic_id: semantic.semantic_id,
                        reason: "complete interval is missing an end or duration",
                    });
                }
            }
        }
        Ok(())
    }

    pub fn validate_structure(&self) -> Result<(), RankedProfileValidationError> {
        if self.semantics.is_empty() {
            return Err(RankedProfileValidationError::MissingSemanticNodes);
        }

        let mut semantics = HashMap::with_capacity(self.semantics.len());
        for semantic in &self.semantics {
            if semantics.insert(semantic.semantic_id, semantic).is_some() {
                return Err(RankedProfileValidationError::DuplicateSemanticId {
                    semantic_id: semantic.semantic_id,
                });
            }
        }

        let semantic_parents = self
            .semantics
            .iter()
            .map(|semantic| (semantic.semantic_id, semantic.parent_semantic_id))
            .collect::<HashMap<_, _>>();
        for semantic in &self.semantics {
            let Some(parent_id) = semantic.parent_semantic_id else {
                continue;
            };
            let Some(parent) = semantics.get(&parent_id) else {
                return Err(RankedProfileValidationError::MissingSemanticParent {
                    semantic_id: semantic.semantic_id,
                    parent_semantic_id: parent_id,
                });
            };
            if parent.operation_id != semantic.operation_id {
                return Err(RankedProfileValidationError::CrossOperationSemanticParent {
                    semantic_id: semantic.semantic_id,
                    parent_semantic_id: parent_id,
                });
            }
        }
        if let Some(semantic_id) = first_cycle(
            &semantic_parents,
            self.semantics.iter().map(|semantic| semantic.semantic_id),
        ) {
            return Err(RankedProfileValidationError::SemanticCycle { semantic_id });
        }

        let mut checked_operations = HashSet::new();
        for operation_id in self.semantics.iter().map(|semantic| semantic.operation_id) {
            if !checked_operations.insert(operation_id) {
                continue;
            }
            let roots = self
                .semantics
                .iter()
                .filter(|semantic| {
                    semantic.operation_id == operation_id && semantic.parent_semantic_id.is_none()
                })
                .collect::<Vec<_>>();
            if roots.len() != 1 {
                return Err(RankedProfileValidationError::InvalidOperationRootCount {
                    operation_id,
                    root_count: roots.len(),
                });
            }
            let invalid_operation_node = self.semantics.iter().find(|semantic| {
                semantic.operation_id == operation_id
                    && semantic.semantic_kind == "operation"
                    && semantic.semantic_id != roots[0].semantic_id
            });
            if roots[0].semantic_kind != "operation" || invalid_operation_node.is_some() {
                return Err(RankedProfileValidationError::InvalidOperationRootKind {
                    operation_id,
                    semantic_id: invalid_operation_node
                        .map_or(roots[0].semantic_id, |semantic| semantic.semantic_id),
                });
            }
        }

        let mut functions = HashMap::with_capacity(self.functions.len());
        let mut function_owners = HashMap::<i64, HashSet<i64>>::new();
        for function in &self.functions {
            if !semantics.contains_key(&function.semantic_id) {
                return Err(RankedProfileValidationError::MissingFunctionOwner {
                    semantic_id: function.semantic_id,
                    function_id: function.function_id,
                });
            }
            let identity = (function.semantic_id, function.function_id);
            if functions.insert(identity, function).is_some() {
                return Err(RankedProfileValidationError::DuplicateFunctionId {
                    semantic_id: function.semantic_id,
                    function_id: function.function_id,
                });
            }
            function_owners
                .entry(function.function_id)
                .or_default()
                .insert(function.semantic_id);
        }

        let function_parents = self
            .functions
            .iter()
            .map(|function| {
                (
                    (function.semantic_id, function.function_id),
                    function
                        .parent_function_id
                        .map(|parent_id| (function.semantic_id, parent_id)),
                )
            })
            .collect::<HashMap<_, _>>();
        for function in &self.functions {
            let Some(parent_function_id) = function.parent_function_id else {
                continue;
            };
            if functions.contains_key(&(function.semantic_id, parent_function_id)) {
                continue;
            }
            let error = if function_owners
                .get(&parent_function_id)
                .is_some_and(|owners| !owners.is_empty())
            {
                RankedProfileValidationError::CrossSemanticFunctionParent {
                    semantic_id: function.semantic_id,
                    function_id: function.function_id,
                    parent_function_id,
                }
            } else {
                RankedProfileValidationError::MissingFunctionParent {
                    semantic_id: function.semantic_id,
                    function_id: function.function_id,
                    parent_function_id,
                }
            };
            return Err(error);
        }
        if let Some((semantic_id, function_id)) = first_cycle(
            &function_parents,
            self.functions
                .iter()
                .map(|function| (function.semantic_id, function.function_id)),
        ) {
            return Err(RankedProfileValidationError::FunctionCycle {
                semantic_id,
                function_id,
            });
        }
        Ok(())
    }

    fn validate_sample_counts(&self) -> Result<(), RankedProfileValidationError> {
        for (field, value) in [
            ("eligible_sample_count", self.metadata.eligible_sample_count),
            ("direct_sample_count", self.metadata.direct_sample_count),
            (
                "ambiguous_sample_count",
                self.metadata.ambiguous_sample_count,
            ),
            (
                "unattributed_sample_count",
                self.metadata.unattributed_sample_count,
            ),
            (
                "resolved_function_sample_count",
                self.metadata.resolved_function_sample_count,
            ),
            (
                "unresolved_function_sample_count",
                self.metadata.unresolved_function_sample_count,
            ),
            (
                "unwind_error_sample_count",
                self.metadata.unwind_error_sample_count,
            ),
            (
                "missing_callstack_sample_count",
                self.metadata.missing_callstack_sample_count,
            ),
            (
                "trace_profiler_dropped_sample_count",
                self.metadata.trace_profiler_dropped_sample_count,
            ),
        ] {
            require_nonnegative("profile", 0, field, value)?;
        }
        let classified_sample_count = self
            .metadata
            .direct_sample_count
            .checked_add(self.metadata.ambiguous_sample_count)
            .and_then(|count| count.checked_add(self.metadata.unattributed_sample_count))
            .ok_or(RankedProfileValidationError::SampleCountOverflow { scope: "coverage" })?;
        if classified_sample_count != self.metadata.eligible_sample_count {
            return Err(RankedProfileValidationError::CoverageMismatch {
                eligible_sample_count: self.metadata.eligible_sample_count,
                classified_sample_count,
            });
        }
        validate_function_coverage(
            None,
            self.metadata.direct_sample_count,
            self.metadata.resolved_function_sample_count,
            self.metadata.unresolved_function_sample_count,
            self.metadata.unwind_error_sample_count,
            self.metadata.missing_callstack_sample_count,
        )?;

        let semantic_parents = self
            .semantics
            .iter()
            .map(|semantic| (semantic.semantic_id, semantic.parent_semantic_id))
            .collect::<HashMap<_, _>>();
        let mut semantic_direct = HashMap::with_capacity(self.semantics.len());
        let mut semantic_direct_total = 0_i64;
        let mut semantic_function_totals = [0_i64; 4];
        for semantic in &self.semantics {
            require_nonnegative(
                "semantic",
                semantic.semantic_id,
                "direct_sample_count",
                semantic.direct_sample_count,
            )?;
            require_nonnegative(
                "semantic",
                semantic.semantic_id,
                "inclusive_sample_count",
                semantic.inclusive_sample_count,
            )?;
            for (field, value) in [
                (
                    "resolved_function_sample_count",
                    semantic.resolved_function_sample_count,
                ),
                (
                    "unresolved_function_sample_count",
                    semantic.unresolved_function_sample_count,
                ),
                (
                    "unwind_error_sample_count",
                    semantic.unwind_error_sample_count,
                ),
                (
                    "missing_callstack_sample_count",
                    semantic.missing_callstack_sample_count,
                ),
            ] {
                require_nonnegative("semantic", semantic.semantic_id, field, value)?;
            }
            validate_function_coverage(
                Some(semantic.semantic_id),
                semantic.direct_sample_count,
                semantic.resolved_function_sample_count,
                semantic.unresolved_function_sample_count,
                semantic.unwind_error_sample_count,
                semantic.missing_callstack_sample_count,
            )?;
            semantic_direct.insert(semantic.semantic_id, semantic.direct_sample_count);
            semantic_direct_total = semantic_direct_total
                .checked_add(semantic.direct_sample_count)
                .ok_or(RankedProfileValidationError::SampleCountOverflow {
                    scope: "semantic direct",
                })?;
            for (total, value) in semantic_function_totals.iter_mut().zip([
                semantic.resolved_function_sample_count,
                semantic.unresolved_function_sample_count,
                semantic.unwind_error_sample_count,
                semantic.missing_callstack_sample_count,
            ]) {
                *total = total.checked_add(value).ok_or(
                    RankedProfileValidationError::SampleCountOverflow {
                        scope: "semantic function coverage",
                    },
                )?;
            }
        }
        if semantic_direct_total != self.metadata.direct_sample_count {
            return Err(RankedProfileValidationError::DirectSampleMismatch {
                declared_sample_count: self.metadata.direct_sample_count,
                semantic_sample_count: semantic_direct_total,
            });
        }
        for (field, declared_sample_count, semantic_sample_count) in [
            (
                "resolved_function_sample_count",
                self.metadata.resolved_function_sample_count,
                semantic_function_totals[0],
            ),
            (
                "unresolved_function_sample_count",
                self.metadata.unresolved_function_sample_count,
                semantic_function_totals[1],
            ),
            (
                "unwind_error_sample_count",
                self.metadata.unwind_error_sample_count,
                semantic_function_totals[2],
            ),
            (
                "missing_callstack_sample_count",
                self.metadata.missing_callstack_sample_count,
                semantic_function_totals[3],
            ),
        ] {
            if declared_sample_count != semantic_sample_count {
                return Err(
                    RankedProfileValidationError::FunctionCoverageSummaryMismatch {
                        field,
                        declared_sample_count,
                        semantic_sample_count,
                    },
                );
            }
        }
        let semantic_inclusive = fold_inclusive_counts(&semantic_parents, &semantic_direct).ok_or(
            RankedProfileValidationError::SampleCountOverflow {
                scope: "semantic inclusive",
            },
        )?;
        for semantic in &self.semantics {
            let computed_sample_count = semantic_inclusive
                .get(&semantic.semantic_id)
                .copied()
                .ok_or(RankedProfileValidationError::SampleCountOverflow {
                    scope: "semantic inclusive",
                })?;
            if semantic.inclusive_sample_count != computed_sample_count {
                return Err(RankedProfileValidationError::SemanticInclusiveMismatch {
                    semantic_id: semantic.semantic_id,
                    declared_sample_count: semantic.inclusive_sample_count,
                    computed_sample_count,
                });
            }
        }

        let function_parents = self
            .functions
            .iter()
            .map(|function| {
                (
                    (function.semantic_id, function.function_id),
                    function
                        .parent_function_id
                        .map(|parent_id| (function.semantic_id, parent_id)),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut function_self = HashMap::with_capacity(self.functions.len());
        let mut function_self_by_semantic = HashMap::<i64, i64>::new();
        for function in &self.functions {
            require_nonnegative(
                "function",
                function.function_id,
                "self_sample_count",
                function.self_sample_count,
            )?;
            require_nonnegative(
                "function",
                function.function_id,
                "inclusive_sample_count",
                function.inclusive_sample_count,
            )?;
            function_self.insert(
                (function.semantic_id, function.function_id),
                function.self_sample_count,
            );
            let semantic_total = function_self_by_semantic
                .entry(function.semantic_id)
                .or_default();
            *semantic_total = semantic_total
                .checked_add(function.self_sample_count)
                .ok_or(RankedProfileValidationError::SampleCountOverflow {
                    scope: "function self",
                })?;
        }
        for semantic in &self.semantics {
            let function_sample_count = function_self_by_semantic
                .get(&semantic.semantic_id)
                .copied()
                .unwrap_or_default();
            let captured_sample_count = semantic
                .resolved_function_sample_count
                .checked_add(semantic.unresolved_function_sample_count)
                .ok_or(RankedProfileValidationError::SampleCountOverflow {
                    scope: "captured native stacks",
                })?;
            if captured_sample_count != function_sample_count {
                return Err(RankedProfileValidationError::FunctionSelfMismatch {
                    semantic_id: semantic.semantic_id,
                    captured_sample_count,
                    function_sample_count,
                });
            }
        }
        let function_inclusive = fold_inclusive_counts(&function_parents, &function_self).ok_or(
            RankedProfileValidationError::SampleCountOverflow {
                scope: "function inclusive",
            },
        )?;
        for function in &self.functions {
            let computed_sample_count = function_inclusive
                .get(&(function.semantic_id, function.function_id))
                .copied()
                .ok_or(RankedProfileValidationError::SampleCountOverflow {
                    scope: "function inclusive",
                })?;
            if function.inclusive_sample_count != computed_sample_count {
                return Err(RankedProfileValidationError::FunctionInclusiveMismatch {
                    semantic_id: function.semantic_id,
                    function_id: function.function_id,
                    declared_sample_count: function.inclusive_sample_count,
                    computed_sample_count,
                });
            }
        }
        Ok(())
    }
}

fn normalize_module_name(value: &str) -> Option<String> {
    safe_basename(value).map(str::to_owned)
}

fn normalize_source_file(value: &str) -> Option<String> {
    let segments = value
        .split(['/', '\\'])
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect::<Vec<_>>();
    let basename = segments
        .last()
        .copied()
        .filter(|segment| is_safe_path_segment(segment))?;
    if segments.contains(&"..") {
        return Some(basename.to_owned());
    }

    const SOURCE_ROOTS: [&str; 6] = ["benches", "crates", "examples", "src", "tests", "xtask"];
    let start = if is_absolute_path(value) {
        segments
            .iter()
            .rposition(|segment| SOURCE_ROOTS.contains(segment))
    } else if segments
        .first()
        .is_some_and(|segment| SOURCE_ROOTS.contains(segment))
    {
        Some(0)
    } else {
        segments.iter().rposition(|segment| *segment == "src")
    };
    let Some(start) = start else {
        return Some(basename.to_owned());
    };
    let safe_segments = &segments[start..];
    if safe_segments
        .iter()
        .any(|segment| !is_safe_path_segment(segment))
    {
        return Some(basename.to_owned());
    }
    Some(safe_segments.join("/"))
}

fn safe_basename(value: &str) -> Option<&str> {
    value
        .rsplit(['/', '\\'])
        .find(|segment| !segment.is_empty() && *segment != ".")
        .filter(|segment| is_safe_path_segment(segment))
}

fn is_safe_path_segment(segment: &str) -> bool {
    if segment.is_empty() || matches!(segment, "." | "..") || segment.starts_with('~') {
        return false;
    }
    let bytes = segment.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return false;
    }
    true
}

fn is_absolute_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    value.starts_with(['/', '\\'])
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\'))
}

fn require_collection_bound(
    record_kind: &'static str,
    count: usize,
) -> Result<(), RankedProfileValidationError> {
    if count > MAX_RECORDS_PER_COLLECTION {
        return Err(RankedProfileValidationError::TooManyRecords {
            record_kind,
            count,
            limit: MAX_RECORDS_PER_COLLECTION,
        });
    }
    Ok(())
}

fn require_display_string(
    record_kind: &'static str,
    record_id: i64,
    field: &'static str,
    value: &str,
) -> Result<(), RankedProfileValidationError> {
    let char_count = value.chars().count();
    if char_count > MAX_DISPLAY_STRING_CHARS {
        return Err(RankedProfileValidationError::DisplayStringTooLong {
            record_kind,
            record_id,
            field,
            char_count,
            limit: MAX_DISPLAY_STRING_CHARS,
        });
    }
    Ok(())
}

fn require_nonnegative(
    record_kind: &'static str,
    record_id: i64,
    field: &'static str,
    value: i64,
) -> Result<(), RankedProfileValidationError> {
    if value < 0 {
        return Err(RankedProfileValidationError::NegativeSampleCount {
            record_kind,
            record_id,
            field,
            value,
        });
    }
    Ok(())
}

fn validate_function_coverage(
    semantic_id: Option<i64>,
    direct_sample_count: i64,
    resolved_sample_count: i64,
    unresolved_sample_count: i64,
    unwind_error_sample_count: i64,
    missing_callstack_sample_count: i64,
) -> Result<(), RankedProfileValidationError> {
    let classified_sample_count = resolved_sample_count
        .checked_add(unresolved_sample_count)
        .and_then(|count| count.checked_add(unwind_error_sample_count))
        .and_then(|count| count.checked_add(missing_callstack_sample_count))
        .ok_or(RankedProfileValidationError::SampleCountOverflow {
            scope: "native stack coverage",
        })?;
    if classified_sample_count != direct_sample_count {
        return Err(RankedProfileValidationError::FunctionCoverageMismatch {
            semantic_id,
            direct_sample_count,
            classified_sample_count,
        });
    }
    Ok(())
}

pub(super) fn fold_inclusive_counts<Id>(
    parents: &HashMap<Id, Option<Id>>,
    self_counts: &HashMap<Id, i64>,
) -> Option<HashMap<Id, i64>>
where
    Id: Copy + Eq + Hash,
{
    let mut remaining_children = parents
        .keys()
        .copied()
        .map(|id| (id, 0_usize))
        .collect::<HashMap<_, _>>();
    for parent in parents.values().flatten() {
        *remaining_children.get_mut(parent)? += 1;
    }
    let mut ready = remaining_children
        .iter()
        .filter_map(|(&id, &children)| (children == 0).then_some(id))
        .collect::<Vec<_>>();
    let mut inclusive = self_counts.clone();
    let mut visited = 0_usize;
    while let Some(id) = ready.pop() {
        visited += 1;
        let Some(parent) = parents.get(&id).copied().flatten() else {
            continue;
        };
        let count = *inclusive.get(&id)?;
        let parent_count = inclusive.get_mut(&parent)?;
        *parent_count = parent_count.checked_add(count)?;
        let children = remaining_children.get_mut(&parent)?;
        *children = children.checked_sub(1)?;
        if *children == 0 {
            ready.push(parent);
        }
    }
    (visited == parents.len()).then_some(inclusive)
}

fn first_cycle<Id>(
    parents: &HashMap<Id, Option<Id>>,
    starts: impl IntoIterator<Item = Id>,
) -> Option<Id>
where
    Id: Copy + Eq + Hash,
{
    let mut complete = HashSet::with_capacity(parents.len());
    for start in starts {
        if complete.contains(&start) {
            continue;
        }
        let mut path = Vec::new();
        let mut positions = HashSet::new();
        let mut current = Some(start);
        while let Some(node) = current {
            if complete.contains(&node) {
                break;
            }
            if !positions.insert(node) {
                return Some(node);
            }
            path.push(node);
            current = parents.get(&node).copied().flatten();
        }
        complete.extend(path);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semantic(
        semantic_id: i64,
        parent_semantic_id: Option<i64>,
        operation_id: i64,
        semantic_kind: &str,
    ) -> RankedSemantic {
        RankedSemantic {
            semantic_id,
            parent_semantic_id,
            operation_id,
            name: semantic_kind.to_owned(),
            semantic_kind: semantic_kind.to_owned(),
            operation_kind: None,
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
            direct_sample_count: 0,
            inclusive_sample_count: 0,
            resolved_function_sample_count: 0,
            unresolved_function_sample_count: 0,
            unwind_error_sample_count: 0,
            missing_callstack_sample_count: 0,
        }
    }

    fn function(
        semantic_id: i64,
        function_id: i64,
        parent_function_id: Option<i64>,
    ) -> RankedFunction {
        RankedFunction {
            semantic_id,
            function_id,
            parent_function_id,
            name: format!("function {function_id}"),
            module_name: None,
            source_file: None,
            line_number: None,
            self_sample_count: 0,
            inclusive_sample_count: 0,
        }
    }

    #[test]
    fn derives_compact_function_names_without_changing_canonical_symbols() {
        for (symbol, expected) in [
            (
                "<tokio::runtime::blocking::task::BlockingTask<F> as core::future::Future>::poll",
                "BlockingTask::poll",
            ),
            (
                "futures_util::future::future::map::Map<Fut, F>::poll",
                "Map::poll",
            ),
            (
                "<delta_funnel::orchestrator::runtime::DeltaFunnelRuntime>::preview_table",
                "DeltaFunnelRuntime::preview_table",
            ),
            (
                "delta_funnel::session::build_preview::{closure#0}",
                "build_preview::{closure#0}",
            ),
            (
                "delta_funnel::report::finish::<alloc::string::String> (.llvm.1234)",
                "report::finish",
            ),
            (
                "pyo3::impl_::trampoline::do_call<Result, Handler>",
                "trampoline::do_call",
            ),
            ("std::ostream::operator<<(int)", "ostream::operator<<"),
            (
                "std::strong_ordering::operator<=>(int)",
                "strong_ordering::operator<=>",
            ),
            (
                "perfetto::ipc::ClientImpl::BeginInvoke(unsigned int, std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char>> const&, bool)",
                "ClientImpl::BeginInvoke",
            ),
            ("functor::Callable::operator()(int)", "Callable::operator()"),
            (
                "std::sys::backtrace::__rust_begin_short_backtrace::<fn() -> core::result::Result<(), alloc::boxed::Box<dyn core::error::Error>>, core::result::Result<(), alloc::boxed::Box<dyn core::error::Error>>>",
                "backtrace::__rust_begin_short_backtrace",
            ),
            ("delta_funnel::operator<Profile>", "delta_funnel::operator"),
            ("do_call<Result, Handler>", "do_call"),
            ("sqlite3_step", "sqlite3_step"),
            ("<invalid>", "<invalid>"),
        ] {
            let mut function = function(1, 1, None);
            function.name = symbol.to_owned();
            assert_eq!(function.display_name(), expected);
            assert_eq!(function.name, symbol);
        }
    }

    #[test]
    fn compacts_only_zero_self_single_child_chains_without_mutating_functions() {
        let mut functions = vec![
            function(1, 1, None),
            function(1, 2, Some(1)),
            function(1, 3, Some(2)),
            function(1, 4, None),
            function(1, 5, Some(4)),
            function(1, 6, Some(4)),
            function(1, 7, None),
            function(1, 8, None),
        ];
        for function in &mut functions {
            match function.function_id {
                1..=3 => function.inclusive_sample_count = 10,
                4 => function.inclusive_sample_count = 5,
                5 => {
                    function.self_sample_count = 2;
                    function.inclusive_sample_count = 2;
                }
                6 => {
                    function.self_sample_count = 3;
                    function.inclusive_sample_count = 3;
                }
                8 => function.inclusive_sample_count = 10,
                _ => {}
            }
        }
        functions[2].self_sample_count = 10;
        let canonical = functions.clone();

        let compact = CompactFunctionTree::new(&functions);

        assert!(!compact.contains(&functions[0]));
        assert!(!compact.contains(&functions[1]));
        assert!(compact.contains(&functions[2]));
        assert_eq!(compact.parent_function_id(&functions[2]), None);
        assert!(compact.contains(&functions[3]));
        assert_eq!(compact.parent_function_id(&functions[4]), Some(4));
        assert_eq!(compact.parent_function_id(&functions[5]), Some(4));
        assert!(compact.contains(&functions[6]));
        assert!(compact.contains(&functions[7]));
        assert_eq!(functions, canonical);
    }

    #[test]
    fn compacts_deep_function_chains_iteratively() {
        const CHAIN_LENGTH: i64 = 20_000;
        let mut functions = (0..CHAIN_LENGTH)
            .map(|function_id| {
                let mut function = function(
                    1,
                    function_id,
                    (function_id != 0).then_some(function_id - 1),
                );
                function.inclusive_sample_count = 1;
                function
            })
            .collect::<Vec<_>>();
        let mut leaf = function(1, CHAIN_LENGTH, Some(CHAIN_LENGTH - 1));
        leaf.self_sample_count = 1;
        leaf.inclusive_sample_count = 1;
        functions.push(leaf);

        let compact = CompactFunctionTree::new(&functions);

        assert!(compact.contains(functions.last().expect("leaf should exist")));
        assert_eq!(
            compact.parent_function_id(functions.last().expect("leaf should exist")),
            None
        );
        assert!(!compact.contains(&functions[0]));
    }

    fn document() -> RankedProfileDocument {
        let mut operation = semantic(1, None, 10, "operation");
        operation.direct_sample_count = 1;
        operation.inclusive_sample_count = 3;
        operation.resolved_function_sample_count = 1;
        let mut phase = semantic(2, Some(1), 10, "phase");
        phase.direct_sample_count = 2;
        phase.inclusive_sample_count = 2;
        phase.unresolved_function_sample_count = 2;
        let mut second_operation = semantic(3, None, 20, "operation");
        second_operation.direct_sample_count = 1;
        second_operation.inclusive_sample_count = 1;
        second_operation.missing_callstack_sample_count = 1;

        let mut operation_function = function(1, 90, None);
        operation_function.self_sample_count = 1;
        operation_function.inclusive_sample_count = 1;
        let mut phase_root = function(2, 100, None);
        phase_root.inclusive_sample_count = 2;
        let mut phase_leaf = function(2, 101, Some(100));
        phase_leaf.self_sample_count = 2;
        phase_leaf.inclusive_sample_count = 2;
        RankedProfileDocument {
            metadata: RankedProfileMetadata {
                capture_complete: true,
                semantic_complete: true,
                schema_version: 3,
                sample_frequency_hz: 100,
                sampled_cpu_count: 1,
                exact_time_unit: "nanoseconds".to_owned(),
                sample_unit: "samples".to_owned(),
                eligible_sample_count: 6,
                direct_sample_count: 4,
                ambiguous_sample_count: 1,
                unattributed_sample_count: 1,
                resolved_function_sample_count: 1,
                unresolved_function_sample_count: 2,
                unwind_error_sample_count: 0,
                missing_callstack_sample_count: 1,
                trace_profiler_dropped_sample_count: 1,
            },
            semantics: vec![operation, phase, second_operation],
            functions: vec![operation_function, phase_root, phase_leaf],
        }
    }

    #[test]
    fn validates_semantic_and_function_structure() {
        assert_eq!(document().validate_structure(), Ok(()));

        let mut invalid = document();
        invalid.semantics.push(invalid.semantics[0].clone());
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::DuplicateSemanticId { semantic_id: 1 })
        ));

        let mut invalid = document();
        invalid.semantics[1].parent_semantic_id = Some(99);
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::MissingSemanticParent {
                semantic_id: 2,
                parent_semantic_id: 99
            })
        ));

        let mut invalid = document();
        invalid.semantics[1].parent_semantic_id = Some(3);
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::CrossOperationSemanticParent {
                semantic_id: 2,
                parent_semantic_id: 3
            })
        ));

        let mut invalid = document();
        invalid.semantics[0].parent_semantic_id = Some(2);
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::SemanticCycle { .. })
        ));

        let mut invalid = document();
        invalid.semantics[1].semantic_kind = "operation".to_owned();
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::InvalidOperationRootKind {
                operation_id: 10,
                semantic_id: 2
            })
        ));

        let mut invalid = document();
        invalid.functions[1].semantic_id = 99;
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::MissingFunctionOwner {
                semantic_id: 99,
                function_id: 100
            })
        ));

        let mut invalid = document();
        invalid.functions.push(invalid.functions[1].clone());
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::DuplicateFunctionId {
                semantic_id: 2,
                function_id: 100
            })
        ));

        let mut invalid = document();
        invalid.functions[2].parent_function_id = Some(999);
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::MissingFunctionParent {
                semantic_id: 2,
                function_id: 101,
                parent_function_id: 999
            })
        ));

        let mut invalid = document();
        invalid.functions.push(function(3, 999, None));
        invalid.functions[2].parent_function_id = Some(999);
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::CrossSemanticFunctionParent {
                semantic_id: 2,
                function_id: 101,
                parent_function_id: 999
            })
        ));

        let mut invalid = document();
        invalid.functions[1].parent_function_id = Some(101);
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::FunctionCycle { .. })
        ));
    }

    #[test]
    fn validates_sample_conservation_and_linear_inclusive_folds() {
        assert_eq!(document().validate(), Ok(()));

        let mut invalid = document();
        invalid.metadata.unattributed_sample_count = -1;
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::NegativeSampleCount {
                record_kind: "profile",
                field: "unattributed_sample_count",
                ..
            })
        ));

        let mut invalid = document();
        invalid.metadata.eligible_sample_count = 7;
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::CoverageMismatch { .. })
        ));

        let mut invalid = document();
        invalid.semantics[2].missing_callstack_sample_count = 0;
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::FunctionCoverageMismatch {
                semantic_id: Some(3),
                ..
            })
        ));

        let mut invalid = document();
        invalid.metadata.direct_sample_count = 5;
        invalid.metadata.eligible_sample_count = 7;
        invalid.metadata.missing_callstack_sample_count = 2;
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::DirectSampleMismatch { .. })
        ));

        let mut invalid = document();
        invalid.semantics[0].inclusive_sample_count = 2;
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::SemanticInclusiveMismatch { semantic_id: 1, .. })
        ));

        let mut invalid = document();
        invalid.functions[2].self_sample_count = 1;
        invalid.functions[2].inclusive_sample_count = 1;
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::FunctionSelfMismatch { semantic_id: 2, .. })
        ));

        let mut invalid = document();
        invalid.functions[1].inclusive_sample_count = 1;
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::FunctionInclusiveMismatch {
                semantic_id: 2,
                function_id: 100,
                ..
            })
        ));

        let mut invalid = document();
        invalid.metadata.direct_sample_count = i64::MAX;
        invalid.metadata.ambiguous_sample_count = 1;
        invalid.metadata.unattributed_sample_count = 0;
        invalid.metadata.eligible_sample_count = i64::MAX;
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::SampleCountOverflow { scope: "coverage" })
        ));
    }

    #[test]
    fn validates_metadata_units_and_semantic_intervals() {
        let mut invalid = document();
        invalid.metadata.schema_version = 2;
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::UnsupportedSchemaVersion { schema_version: 2 })
        ));

        let mut invalid = document();
        invalid.metadata.sample_frequency_hz = 0;
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::InvalidSampleFrequency)
        ));

        let mut invalid = document();
        invalid.metadata.exact_time_unit = "milliseconds".to_owned();
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::InvalidUnit {
                field: "exact_time_unit",
                expected: "nanoseconds"
            })
        ));

        let mut invalid = document();
        invalid.semantics[0].time_semantics = "active".to_owned();
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::InvalidSemanticTimeSemantics { semantic_id: 1 })
        ));

        let mut valid = document();
        valid.semantics[0].is_complete = false;
        valid.semantics[0].end_ns = None;
        valid.semantics[0].duration_ns = None;
        assert_eq!(valid.validate(), Ok(()));

        let mut invalid = valid;
        invalid.semantics[0].end_ns = Some(1);
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::InvalidSemanticInterval { semantic_id: 1, .. })
        ));

        let mut invalid = document();
        invalid.semantics[0].duration_ns = None;
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::InvalidSemanticInterval { semantic_id: 1, .. })
        ));

        let mut invalid = document();
        invalid.semantics[0].duration_ns = Some(2);
        assert!(matches!(
            invalid.validate(),
            Err(RankedProfileValidationError::InvalidSemanticInterval { semantic_id: 1, .. })
        ));
    }

    #[test]
    fn bounds_aggregate_collections_and_display_strings_without_truncating_symbols() {
        assert!(matches!(
            require_collection_bound("semantic", MAX_RECORDS_PER_COLLECTION + 1),
            Err(RankedProfileValidationError::TooManyRecords {
                record_kind: "semantic",
                count,
                limit: MAX_RECORDS_PER_COLLECTION
            }) if count == MAX_RECORDS_PER_COLLECTION + 1
        ));

        let mut invalid_display = document();
        invalid_display.semantics[0].name = "x".repeat(MAX_DISPLAY_STRING_CHARS + 1);
        assert!(matches!(
            invalid_display.validate(),
            Err(RankedProfileValidationError::DisplayStringTooLong {
                record_kind: "semantic",
                record_id: 1,
                field: "name",
                char_count,
                limit: MAX_DISPLAY_STRING_CHARS
            }) if char_count == MAX_DISPLAY_STRING_CHARS + 1
        ));

        let mut valid = document();
        valid.functions[0].name = "x".repeat(6_537);
        assert_eq!(valid.validate(), Ok(()));
    }

    #[test]
    fn normalizes_posix_windows_unc_relative_and_missing_source_metadata() {
        for (input, expected) in [
            ("/usr/lib/libdelta.so", Some("libdelta.so")),
            (r"C:\build\delta.dll", Some("delta.dll")),
            (r"\\server\share\delta.dll", Some("delta.dll")),
            ("delta-funnel", Some("delta-funnel")),
            ("C:relative.dll", None),
            ("~user", None),
            ("", None),
        ] {
            assert_eq!(normalize_module_name(input).as_deref(), expected);
        }
        for (input, expected) in [
            (
                "/home/user/repo/crates/delta-funnel/src/query.rs",
                Some("src/query.rs"),
            ),
            (
                r"C:\work\repo\crates\delta-funnel\src\query.rs",
                Some("src/query.rs"),
            ),
            (r"\\server\share\repo\src\query.rs", Some("src/query.rs")),
            (
                "crates/delta-funnel/src/query.rs",
                Some("crates/delta-funnel/src/query.rs"),
            ),
            ("private/build/query.rs", Some("query.rs")),
            ("../private/query.rs", Some("query.rs")),
            ("C:relative.rs", None),
            ("", None),
        ] {
            assert_eq!(normalize_source_file(input).as_deref(), expected);
        }

        let mut document = document();
        document.functions[0].module_name = Some("/usr/lib/libdelta.so".to_owned());
        document.functions[0].source_file =
            Some(r"C:\work\repo\crates\delta-funnel\src\query.rs".to_owned());
        assert!(matches!(
            document.validate(),
            Err(RankedProfileValidationError::UnsafeFunctionMetadata {
                semantic_id: 1,
                function_id: 90,
                field: "module_name"
            })
        ));

        document.normalize_source_metadata();
        assert_eq!(
            document.functions[0].module_name.as_deref(),
            Some("libdelta.so")
        );
        assert_eq!(
            document.functions[0].source_file.as_deref(),
            Some("src/query.rs")
        );
        assert_eq!(document.functions[1].module_name, None);
        assert_eq!(document.functions[1].source_file, None);
        assert_eq!(document.validate(), Ok(()));
    }
}
