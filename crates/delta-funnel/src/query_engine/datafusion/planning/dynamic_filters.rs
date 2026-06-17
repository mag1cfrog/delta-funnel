//! Dynamic physical filter classification for Delta scan planning.
//!
//! Static pushdown is decided earlier at the logical `TableProvider` boundary.
//! This module is only for DataFusion physical filters that arrive after scan
//! planning, such as `DynamicFilterPhysicalExpr` values produced by a join or
//! top-k operator. The classifier is intentionally conservative: it only
//! retains dynamic filters whose referenced provider output columns all resolve
//! to Delta partition columns for this scan.
//!
//! Retaining a filter here does not mean the provider has enforced it. Later
//! slices will evaluate retained filters against partition values and update
//! read statistics. Until then, this state is just a validated handoff from
//! DataFusion's physical pushdown hook to future execution-time pruning.

// This slice introduces the retained model before the execution hook consumes
// it. Remove this allowance once `DeltaScanPlanningExec` stores the plan.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};
use datafusion::physical_plan::PhysicalExpr;

/// Dynamic filter accepted by the Delta scan for later partition pruning.
///
/// The original physical expression is kept rather than normalized into a
/// provider expression because `DynamicFilterPhysicalExpr` is stateful: its
/// producer updates the expression at runtime. Keeping the same `Arc` preserves
/// the connection between DataFusion's producer and this scan consumer.
#[derive(Clone, Debug)]
pub(crate) struct DeltaRetainedDynamicFilter {
    /// Original physical filter pushed by DataFusion, including any dynamic state.
    pub(crate) physical_expr: Arc<dyn PhysicalExpr>,
    /// Provider output partition columns referenced by this filter.
    pub(crate) partition_columns: Vec<DeltaDynamicFilterColumn>,
    /// Provider logical output schema used to validate indexes during retention.
    pub(crate) provider_schema: SchemaRef,
}

/// Provider output partition column referenced by a dynamic filter.
///
/// DataFusion physical expressions identify columns by both name and index.
/// Later partition-value evaluation needs both so it can validate that the
/// retained expression still lines up with the provider output schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeltaDynamicFilterColumn {
    /// Provider output field name.
    pub(crate) name: String,
    /// Provider output field index.
    pub(crate) index: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Classification outcome for one physical filter supplied by DataFusion.
pub(crate) enum DeltaDynamicFilterOutcome {
    /// The filter is dynamic and references only Delta partition columns.
    Accepted,
    /// The filter is unsupported by this hook and must remain a residual concern.
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Conservative reason a physical filter was not retained by this hook.
pub(crate) enum DeltaDynamicFilterRejectionReason {
    /// The expression does not contain a `DynamicFilterPhysicalExpr`.
    NotDynamicFilter,
    /// The expression is dynamic but exposes no physical column references.
    NoReferencedColumns,
    /// The expression references provider-internal synthetic columns.
    InternalColumn,
    /// A physical column name or index cannot be resolved against the provider schema.
    UnknownColumn,
    /// The expression references only non-partition data columns.
    DataColumn,
    /// The expression references at least one partition column and one data column.
    MixedPartitionAndData,
}

#[derive(Clone, Debug)]
/// Retention decision for one physical filter, preserving input order.
pub(crate) struct DeltaDynamicFilterDecision {
    /// Whether this filter was retained.
    pub(crate) outcome: DeltaDynamicFilterOutcome,
    /// Retained filter state when `outcome` is `Accepted`.
    pub(crate) retained_filter: Option<DeltaRetainedDynamicFilter>,
    /// Rejection reason when `outcome` is `Rejected`.
    pub(crate) rejection_reason: Option<DeltaDynamicFilterRejectionReason>,
}

#[derive(Clone, Debug, Default)]
/// Batch classification for the filters offered to one Delta scan node.
pub(crate) struct DeltaDynamicFilterPlan {
    /// One decision per input filter, preserving input order for diagnostics.
    pub(crate) decisions: Vec<DeltaDynamicFilterDecision>,
    /// Accepted filters in input order, ready to be stored on the scan node.
    pub(crate) accepted_filters: Vec<DeltaRetainedDynamicFilter>,
}

impl DeltaDynamicFilterPlan {
    /// Classifies DataFusion physical filters against this scan's output schema.
    ///
    /// `partition_columns` must be the Delta table partition columns retained
    /// during logical scan planning. A physical filter is accepted only when all
    /// referenced columns resolve to those retained partition columns.
    #[must_use]
    pub(crate) fn from_filters(
        filters: &[Arc<dyn PhysicalExpr>],
        provider_schema: &SchemaRef,
        partition_columns: &[String],
    ) -> Self {
        let decisions = filters
            .iter()
            .map(|filter| classify_dynamic_filter(filter, provider_schema, partition_columns))
            .collect::<Vec<_>>();
        let accepted_filters = decisions
            .iter()
            .filter_map(|decision| decision.retained_filter.clone())
            .collect();

        Self {
            decisions,
            accepted_filters,
        }
    }

    /// Returns whether at least one offered physical filter can be retained.
    #[must_use]
    pub(crate) fn has_accepted_filters(&self) -> bool {
        !self.accepted_filters.is_empty()
    }
}

fn classify_dynamic_filter(
    filter: &Arc<dyn PhysicalExpr>,
    provider_schema: &SchemaRef,
    partition_columns: &[String],
) -> DeltaDynamicFilterDecision {
    if !contains_dynamic_filter(filter.as_ref()) {
        return rejected(DeltaDynamicFilterRejectionReason::NotDynamicFilter);
    }

    let references = collect_column_references(filter.as_ref(), provider_schema);
    if references.has_internal_column {
        return rejected(DeltaDynamicFilterRejectionReason::InternalColumn);
    }
    if references.has_unknown_column {
        return rejected(DeltaDynamicFilterRejectionReason::UnknownColumn);
    }
    if references.columns.is_empty() {
        return rejected(DeltaDynamicFilterRejectionReason::NoReferencedColumns);
    }

    let partition_column_set = partition_columns
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    // `references.columns` is a BTreeMap keyed by physical index and name, so
    // retained column mappings are deterministic and match provider output order.
    let (partition_columns, data_column_count): (Vec<_>, usize) =
        references.columns.into_values().fold(
            (Vec::new(), 0),
            |(mut partition_columns, data_count), column| {
                if partition_column_set.contains(column.name.as_str()) {
                    partition_columns.push(column);
                    (partition_columns, data_count)
                } else {
                    (partition_columns, data_count + 1)
                }
            },
        );

    match (partition_columns.is_empty(), data_column_count == 0) {
        (false, true) => DeltaDynamicFilterDecision {
            outcome: DeltaDynamicFilterOutcome::Accepted,
            retained_filter: Some(DeltaRetainedDynamicFilter {
                physical_expr: Arc::clone(filter),
                partition_columns,
                provider_schema: Arc::clone(provider_schema),
            }),
            rejection_reason: None,
        },
        (false, false) => rejected(DeltaDynamicFilterRejectionReason::MixedPartitionAndData),
        (true, false) => rejected(DeltaDynamicFilterRejectionReason::DataColumn),
        (true, true) => rejected(DeltaDynamicFilterRejectionReason::NoReferencedColumns),
    }
}

fn rejected(reason: DeltaDynamicFilterRejectionReason) -> DeltaDynamicFilterDecision {
    DeltaDynamicFilterDecision {
        outcome: DeltaDynamicFilterOutcome::Rejected,
        retained_filter: None,
        rejection_reason: Some(reason),
    }
}

/// Returns whether this expression tree contains DataFusion dynamic state.
///
/// This handles common boolean wrappers around a `DynamicFilterPhysicalExpr`
/// without trying to prove full predicate semantics. Semantic evaluation remains
/// out of scope for this issue.
fn contains_dynamic_filter(expr: &dyn PhysicalExpr) -> bool {
    expr.as_any().is::<DynamicFilterPhysicalExpr>()
        || expr
            .children()
            .into_iter()
            .any(|child| contains_dynamic_filter(child.as_ref()))
}

#[derive(Default)]
struct ColumnReferences {
    /// Resolved provider output columns, keyed for deterministic de-duplication.
    columns: BTreeMap<(usize, String), DeltaDynamicFilterColumn>,
    /// Whether any column reference targets provider-owned synthetic state.
    has_internal_column: bool,
    /// Whether any column reference fails strict provider schema validation.
    has_unknown_column: bool,
}

fn collect_column_references(
    expr: &dyn PhysicalExpr,
    provider_schema: &SchemaRef,
) -> ColumnReferences {
    let mut references = ColumnReferences::default();
    collect_column_references_into(expr, provider_schema, &mut references);
    references
}

fn collect_column_references_into(
    expr: &dyn PhysicalExpr,
    provider_schema: &SchemaRef,
    references: &mut ColumnReferences,
) {
    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
        collect_column_reference(column, provider_schema, references);
    }

    for child in expr.children() {
        collect_column_references_into(child.as_ref(), provider_schema, references);
    }
}

fn collect_column_reference(
    column: &Column,
    provider_schema: &SchemaRef,
    references: &mut ColumnReferences,
) {
    if column.name().starts_with("__delta_funnel_") {
        references.has_internal_column = true;
        return;
    }

    let Some(field) = provider_schema.fields().get(column.index()) else {
        references.has_unknown_column = true;
        return;
    };

    // Physical expressions can carry a valid-looking name with a stale or
    // rewritten index. Require both to match so later partition-value evaluation
    // cannot silently read the wrong provider output field.
    if field.name() != column.name() {
        references.has_unknown_column = true;
        return;
    }

    references.columns.insert(
        (column.index(), column.name().to_owned()),
        DeltaDynamicFilterColumn {
            name: column.name().to_owned(),
            index: column.index(),
        },
    );
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, lit};

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("customer_name", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("event_date", DataType::Date32, true),
        ]))
    }

    fn column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    fn dynamic_filter(children: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(children, lit(true)))
    }

    fn plan_for(filter: Arc<dyn PhysicalExpr>) -> DeltaDynamicFilterPlan {
        DeltaDynamicFilterPlan::from_filters(
            &[filter],
            &test_schema(),
            &["region".to_owned(), "event_date".to_owned()],
        )
    }

    #[test]
    fn partition_dynamic_filter_is_accepted() {
        let plan = plan_for(dynamic_filter(vec![column("region", 2)]));

        assert!(plan.has_accepted_filters());
        assert_eq!(
            plan.decisions[0].outcome,
            DeltaDynamicFilterOutcome::Accepted
        );
        assert_eq!(
            plan.accepted_filters[0].partition_columns,
            vec![DeltaDynamicFilterColumn {
                name: "region".to_owned(),
                index: 2,
            }]
        );
    }

    #[test]
    fn multi_partition_dynamic_filter_retains_sorted_column_mappings() {
        let plan = plan_for(dynamic_filter(vec![
            column("event_date", 3),
            column("region", 2),
        ]));

        assert_eq!(
            plan.accepted_filters[0].partition_columns,
            vec![
                DeltaDynamicFilterColumn {
                    name: "region".to_owned(),
                    index: 2,
                },
                DeltaDynamicFilterColumn {
                    name: "event_date".to_owned(),
                    index: 3,
                },
            ]
        );
    }

    #[test]
    fn data_column_dynamic_filter_is_rejected() {
        let plan = plan_for(dynamic_filter(vec![column("id", 0)]));

        assert!(!plan.has_accepted_filters());
        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaDynamicFilterRejectionReason::DataColumn)
        );
    }

    #[test]
    fn unknown_column_dynamic_filter_is_rejected() {
        let plan = plan_for(dynamic_filter(vec![column("ghost", 99)]));

        assert!(!plan.has_accepted_filters());
        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaDynamicFilterRejectionReason::UnknownColumn)
        );
    }

    #[test]
    fn mixed_partition_and_data_dynamic_filter_is_rejected() {
        let plan = plan_for(dynamic_filter(vec![column("region", 2), column("id", 0)]));

        assert!(!plan.has_accepted_filters());
        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaDynamicFilterRejectionReason::MixedPartitionAndData)
        );
    }

    #[test]
    fn dynamic_filter_wrapped_with_data_column_is_rejected_as_mixed() {
        let dynamic = dynamic_filter(vec![column("region", 2)]);
        let wrapped = Arc::new(BinaryExpr::new(dynamic, Operator::And, column("id", 0)));

        let plan = plan_for(wrapped);

        assert!(!plan.has_accepted_filters());
        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaDynamicFilterRejectionReason::MixedPartitionAndData)
        );
    }

    #[test]
    fn non_dynamic_filter_is_rejected() {
        let filter = Arc::new(BinaryExpr::new(
            column("region", 2),
            Operator::Eq,
            lit("us-west"),
        ));

        let plan = plan_for(filter);

        assert!(!plan.has_accepted_filters());
        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaDynamicFilterRejectionReason::NotDynamicFilter)
        );
    }

    #[test]
    fn internal_column_dynamic_filter_is_rejected() {
        let plan = plan_for(dynamic_filter(vec![column("__delta_funnel_row_index", 0)]));

        assert!(!plan.has_accepted_filters());
        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaDynamicFilterRejectionReason::InternalColumn)
        );
    }
}
