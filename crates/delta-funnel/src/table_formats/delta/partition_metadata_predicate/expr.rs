use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use super::names::PhysicalPartitionColumn;
use super::value::PartitionScalar;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PartitionMetadataExpr {
    Eq {
        column: PhysicalPartitionColumn,
        literal: PartitionScalar,
    },
    Compare {
        column: PhysicalPartitionColumn,
        op: PartitionComparisonOperator,
        literal: PartitionScalar,
    },
    In {
        column: PhysicalPartitionColumn,
        literals: HashSet<PartitionScalar>,
    },
    IsNull(PhysicalPartitionColumn),
    IsNotNull(PhysicalPartitionColumn),
    And(Box<PartitionMetadataExpr>, Box<PartitionMetadataExpr>),
    Or(Box<PartitionMetadataExpr>, Box<PartitionMetadataExpr>),
    Not(Box<PartitionMetadataExpr>),
}

impl PartitionMetadataExpr {
    pub(super) fn eval(&self, partition_values: &HashMap<String, String>) -> SqlBool {
        match self {
            Self::Eq { column, literal } => column
                .value(partition_values)
                .and_then(|value| column.value_kind().parse_raw(value))
                .map(|value| SqlBool::from(value == *literal))
                .unwrap_or(SqlBool::Null),
            Self::Compare {
                column,
                op,
                literal,
            } => column
                .value(partition_values)
                .and_then(|value| column.value_kind().parse_raw(value))
                .and_then(|value| value.compare(literal))
                .map(|ordering| SqlBool::from(op.matches(ordering)))
                .unwrap_or(SqlBool::Null),
            Self::In { column, literals } => column
                .value(partition_values)
                .and_then(|value| column.value_kind().parse_raw(value))
                .map(|value| SqlBool::from(literals.contains(&value)))
                .unwrap_or(SqlBool::Null),
            Self::IsNull(column) => SqlBool::from(column.value(partition_values).is_none()),
            Self::IsNotNull(column) => SqlBool::from(column.value(partition_values).is_some()),
            Self::And(left, right) => left
                .eval(partition_values)
                .and(right.eval(partition_values)),
            Self::Or(left, right) => left.eval(partition_values).or(right.eval(partition_values)),
            Self::Not(inner) => inner.eval(partition_values).not(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PartitionComparisonOperator {
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl PartitionComparisonOperator {
    pub(super) fn matches(self, ordering: Ordering) -> bool {
        match self {
            Self::Lt => ordering == Ordering::Less,
            Self::LtEq => matches!(ordering, Ordering::Less | Ordering::Equal),
            Self::Gt => ordering == Ordering::Greater,
            Self::GtEq => matches!(ordering, Ordering::Greater | Ordering::Equal),
        }
    }

    pub(super) fn reverse(self) -> Self {
        match self {
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SqlBool {
    True,
    False,
    Null,
}

impl SqlBool {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            (Self::Null, _) | (_, Self::Null) => Self::Null,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            (Self::Null, _) | (_, Self::Null) => Self::Null,
        }
    }

    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Null => Self::Null,
        }
    }
}

impl From<bool> for SqlBool {
    fn from(value: bool) -> Self {
        if value { Self::True } else { Self::False }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_bool_and_or_not_follow_three_valued_logic() {
        assert_eq!(SqlBool::True.and(SqlBool::True), SqlBool::True);
        assert_eq!(SqlBool::True.and(SqlBool::Null), SqlBool::Null);
        assert_eq!(SqlBool::Null.and(SqlBool::False), SqlBool::False);

        assert_eq!(SqlBool::False.or(SqlBool::False), SqlBool::False);
        assert_eq!(SqlBool::False.or(SqlBool::Null), SqlBool::Null);
        assert_eq!(SqlBool::Null.or(SqlBool::True), SqlBool::True);

        assert_eq!(SqlBool::True.not(), SqlBool::False);
        assert_eq!(SqlBool::False.not(), SqlBool::True);
        assert_eq!(SqlBool::Null.not(), SqlBool::Null);
    }

    #[test]
    fn comparison_operator_matches_and_reverses_orderings() {
        assert!(PartitionComparisonOperator::Lt.matches(Ordering::Less));
        assert!(PartitionComparisonOperator::LtEq.matches(Ordering::Equal));
        assert!(PartitionComparisonOperator::Gt.matches(Ordering::Greater));
        assert!(PartitionComparisonOperator::GtEq.matches(Ordering::Equal));

        assert_eq!(
            PartitionComparisonOperator::Lt.reverse(),
            PartitionComparisonOperator::Gt
        );
        assert_eq!(
            PartitionComparisonOperator::LtEq.reverse(),
            PartitionComparisonOperator::GtEq
        );
    }
}
