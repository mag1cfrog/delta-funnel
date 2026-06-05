//! SQL-compatible Delta partition metadata predicate evaluation.

// The planning and scan-file pruning paths use this predicate before the full
// read path consumes those pruned files end to end, so keep dead-code warnings
// quiet for helper surfaces that are only exercised by planning paths.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::logical_expr::Expr;

mod convert;
mod expr;
mod names;
mod value;

pub(crate) use convert::DeltaPartitionMetadataPredicateError;
use convert::convert_expr;
use expr::{PartitionMetadataExpr, SqlBool};
pub(crate) use names::DeltaPartitionNameMap;
use value::PartitionMetadataValueKind;

/// Returns whether this provider can evaluate a Delta partition column type from metadata.
///
/// Delta stores partition values as serialized text in add-file metadata, but
/// exact SQL pushdown also depends on the logical schema type. This function is
/// the single type gate to update when a new logical partition metadata type is
/// promoted for both support planning and metadata evaluation.
#[must_use]
pub(crate) fn supports_partition_metadata_logical_type(data_type: &DataType) -> bool {
    PartitionMetadataValueKind::from_supported_data_type(data_type).is_some()
}

/// Provider-owned predicate over serialized Delta partition metadata.
///
/// This is intentionally independent from `delta_kernel` predicate pruning.
/// It evaluates `ScanFile.partition_values` with DataFusion SQL semantics:
/// missing partition keys are treated as SQL null, while a present raw empty
/// string remains a non-null empty string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeltaPartitionMetadataPredicate {
    expr: PartitionMetadataExpr,
}

impl DeltaPartitionMetadataPredicate {
    /// Converts a supported DataFusion expression into a metadata predicate.
    ///
    /// The provider policy supports promoted logical partition types for
    /// operators whose literal or ordering semantics are proven. Range
    /// comparisons, `BETWEEN`, and `NOT BETWEEN` are accepted only for promoted
    /// types with proven ordering semantics. Unsupported expressions return a
    /// typed error so the caller can keep DataFusion residual filtering instead
    /// of guessing.
    pub(crate) fn from_datafusion_expr(
        expr: &Expr,
        logical_schema: &SchemaRef,
        partition_columns: &HashSet<String>,
        physical_name_lookup: &DeltaPartitionNameMap,
    ) -> Result<Self, DeltaPartitionMetadataPredicateError> {
        Ok(Self {
            expr: convert_expr(
                expr,
                logical_schema,
                partition_columns,
                physical_name_lookup,
            )?,
        })
    }

    /// Combines multiple metadata predicates with logical `AND`.
    ///
    /// DataFusion may push multiple exact filters into one scan. The scan plan
    /// stores one metadata predicate for that whole provider-owned filter set,
    /// so each accepted input filter becomes a child of this conjunction.
    #[must_use]
    pub(crate) fn and_from(predicates: impl IntoIterator<Item = Self>) -> Option<Self> {
        let mut predicates = predicates
            .into_iter()
            .map(|predicate| predicate.expr)
            .collect::<Vec<_>>();
        let first = predicates.pop()?;

        Some(Self {
            expr: predicates.into_iter().fold(first, |right, left| {
                PartitionMetadataExpr::And(Box::new(left), Box::new(right))
            }),
        })
    }

    /// Returns whether one scan file should be kept by this predicate.
    ///
    /// SQL three-valued logic is collapsed using WHERE semantics: only `TRUE`
    /// keeps a file. `FALSE` and `NULL` both prune it. The input map is the raw
    /// partition metadata attached to a Delta `ScanFile`: missing keys are SQL
    /// nulls, while present empty strings are non-null empty strings.
    #[must_use]
    pub(crate) fn matches_scan_file(&self, partition_values: &HashMap<String, String>) -> bool {
        self.expr.eval(partition_values) == SqlBool::True
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::TimeUnit;

    use super::*;

    #[test]
    fn metadata_type_policy_documents_promoted_scope() {
        let supported = [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Int64,
            DataType::Int32,
            DataType::Int16,
            DataType::Int8,
            DataType::Boolean,
            DataType::Date32,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(38, 18),
        ];
        let unsupported = [
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, -1),
            DataType::Decimal128(10, 11),
            DataType::Decimal128(39, 2),
            DataType::Binary,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ];

        assert!(
            supported
                .iter()
                .all(supports_partition_metadata_logical_type)
        );
        assert!(
            unsupported
                .iter()
                .all(|data_type| !supports_partition_metadata_logical_type(data_type))
        );
    }
}
