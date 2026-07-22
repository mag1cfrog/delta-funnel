use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RankedProfileMetadata {
    pub schema_version: u32,
    pub sample_frequency_hz: u32,
    pub exact_time_unit: String,
    pub sample_unit: String,
    pub eligible_sample_count: i64,
    pub direct_sample_count: i64,
    pub ambiguous_sample_count: i64,
    pub unattributed_sample_count: i64,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RankedSemantic {
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
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RankedFunction {
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

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RankedProfileDocument {
    pub metadata: RankedProfileMetadata,
    pub semantics: Vec<RankedSemantic>,
    pub functions: Vec<RankedFunction>,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RankedProfileValidationError {
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
    SemanticInclusiveMismatch {
        semantic_id: i64,
        declared_sample_count: i64,
        computed_sample_count: i64,
    },
    FunctionSelfMismatch {
        semantic_id: i64,
        direct_sample_count: i64,
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
                direct_sample_count,
                function_sample_count,
            } => write!(
                formatter,
                "semantic ID {semantic_id} direct count {direct_sample_count} does not equal function self count {function_sample_count}"
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
    pub fn validate(&self) -> Result<(), RankedProfileValidationError> {
        self.validate_structure()?;
        self.validate_sample_counts()
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

        let semantic_parents = self
            .semantics
            .iter()
            .map(|semantic| (semantic.semantic_id, semantic.parent_semantic_id))
            .collect::<HashMap<_, _>>();
        let mut semantic_direct = HashMap::with_capacity(self.semantics.len());
        let mut semantic_direct_total = 0_i64;
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
            semantic_direct.insert(semantic.semantic_id, semantic.direct_sample_count);
            semantic_direct_total = semantic_direct_total
                .checked_add(semantic.direct_sample_count)
                .ok_or(RankedProfileValidationError::SampleCountOverflow {
                    scope: "semantic direct",
                })?;
        }
        if semantic_direct_total != self.metadata.direct_sample_count {
            return Err(RankedProfileValidationError::DirectSampleMismatch {
                declared_sample_count: self.metadata.direct_sample_count,
                semantic_sample_count: semantic_direct_total,
            });
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
            if semantic.direct_sample_count != function_sample_count {
                return Err(RankedProfileValidationError::FunctionSelfMismatch {
                    semantic_id: semantic.semantic_id,
                    direct_sample_count: semantic.direct_sample_count,
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

fn fold_inclusive_counts<Id>(
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

    fn document() -> RankedProfileDocument {
        let mut operation = semantic(1, None, 10, "operation");
        operation.direct_sample_count = 1;
        operation.inclusive_sample_count = 3;
        let mut phase = semantic(2, Some(1), 10, "phase");
        phase.direct_sample_count = 2;
        phase.inclusive_sample_count = 2;
        let mut second_operation = semantic(3, None, 20, "operation");
        second_operation.direct_sample_count = 1;
        second_operation.inclusive_sample_count = 1;

        let mut operation_function = function(1, 90, None);
        operation_function.self_sample_count = 1;
        operation_function.inclusive_sample_count = 1;
        let mut phase_root = function(2, 100, None);
        phase_root.inclusive_sample_count = 2;
        let mut phase_leaf = function(2, 101, Some(100));
        phase_leaf.self_sample_count = 2;
        phase_leaf.inclusive_sample_count = 2;
        let mut unresolved = function(3, -1, None);
        unresolved.name = "[native stack unavailable]".to_owned();
        unresolved.self_sample_count = 1;
        unresolved.inclusive_sample_count = 1;

        RankedProfileDocument {
            metadata: RankedProfileMetadata {
                schema_version: 1,
                sample_frequency_hz: 100,
                exact_time_unit: "nanoseconds".to_owned(),
                sample_unit: "samples".to_owned(),
                eligible_sample_count: 6,
                direct_sample_count: 4,
                ambiguous_sample_count: 1,
                unattributed_sample_count: 1,
            },
            semantics: vec![operation, phase, second_operation],
            functions: vec![operation_function, phase_root, phase_leaf, unresolved],
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
        invalid.metadata.direct_sample_count = 5;
        invalid.metadata.eligible_sample_count = 7;
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
}
