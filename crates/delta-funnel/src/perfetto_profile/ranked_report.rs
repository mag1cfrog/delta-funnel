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
        }
    }
}

impl std::error::Error for RankedProfileValidationError {}

impl RankedProfileDocument {
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
            semantics: vec![
                semantic(1, None, 10, "operation"),
                semantic(2, Some(1), 10, "phase"),
                semantic(3, None, 20, "operation"),
            ],
            functions: vec![function(2, 100, None), function(2, 101, Some(100))],
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
        invalid.functions[0].semantic_id = 99;
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::MissingFunctionOwner {
                semantic_id: 99,
                function_id: 100
            })
        ));

        let mut invalid = document();
        invalid.functions.push(invalid.functions[0].clone());
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::DuplicateFunctionId {
                semantic_id: 2,
                function_id: 100
            })
        ));

        let mut invalid = document();
        invalid.functions[1].parent_function_id = Some(999);
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
        invalid.functions[1].parent_function_id = Some(999);
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::CrossSemanticFunctionParent {
                semantic_id: 2,
                function_id: 101,
                parent_function_id: 999
            })
        ));

        let mut invalid = document();
        invalid.functions[0].parent_function_id = Some(101);
        assert!(matches!(
            invalid.validate_structure(),
            Err(RankedProfileValidationError::FunctionCycle { .. })
        ));
    }
}
