//! Filter pushdown planning for the Delta DataFusion provider.

use std::collections::HashSet;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderFilterPushdownKind {
    Exact,
    Inexact,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderFilterReason {
    UnsupportedInitialPolicy,
    UnsupportedExpressionShape,
    UnsupportedInternalColumn,
    UnsupportedUnknownColumn,
}

impl ProviderFilterReason {
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::UnsupportedInitialPolicy => "unsupported_initial_policy",
            Self::UnsupportedExpressionShape => "unsupported_expression_shape",
            Self::UnsupportedInternalColumn => "unsupported_internal_column",
            Self::UnsupportedUnknownColumn => "unsupported_unknown_column",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProviderFilterDecision {
    pub(crate) input_index: usize,
    pub(crate) pushdown: TableProviderFilterPushDown,
    pub(crate) kind: ProviderFilterPushdownKind,
    pub(crate) residual: bool,
    pub(crate) reason: ProviderFilterReason,
    pub(crate) referenced_columns: Vec<String>,
    pub(crate) partition_columns: Vec<String>,
    pub(crate) data_columns: Vec<String>,
    pub(crate) unknown_columns: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProviderFilterPlan {
    pub(crate) pushdown_statuses: Vec<TableProviderFilterPushDown>,
    pub(crate) decisions: Vec<ProviderFilterDecision>,
    pub(crate) exact_count: usize,
    pub(crate) inexact_count: usize,
    pub(crate) unsupported_count: usize,
    pub(crate) pushed_filter_count: usize,
    pub(crate) residual_filter_count: usize,
}

impl ProviderFilterPlan {
    #[must_use]
    pub(crate) fn unsupported(
        filters: &[&Expr],
        schema: &SchemaRef,
        partition_columns: &HashSet<String>,
    ) -> Self {
        let decisions = filters
            .iter()
            .enumerate()
            .map(|(input_index, filter)| {
                ProviderFilterDecision::unsupported(input_index, filter, schema, partition_columns)
            })
            .collect::<Vec<_>>();

        Self::from_decisions(decisions)
    }

    #[must_use]
    pub(crate) fn empty_pushed() -> Self {
        Self::from_decisions(Vec::new())
    }

    fn from_decisions(decisions: Vec<ProviderFilterDecision>) -> Self {
        let pushdown_statuses = decisions
            .iter()
            .map(|decision| decision.pushdown.clone())
            .collect::<Vec<_>>();
        let exact_count = decisions
            .iter()
            .filter(|decision| decision.kind == ProviderFilterPushdownKind::Exact)
            .count();
        let inexact_count = decisions
            .iter()
            .filter(|decision| decision.kind == ProviderFilterPushdownKind::Inexact)
            .count();
        let unsupported_count = decisions
            .iter()
            .filter(|decision| decision.kind == ProviderFilterPushdownKind::Unsupported)
            .count();
        let residual_filter_count = decisions
            .iter()
            .filter(|decision| decision.residual)
            .count();
        let pushed_filter_count = decisions.len().saturating_sub(unsupported_count);

        Self {
            pushdown_statuses,
            decisions,
            exact_count,
            inexact_count,
            unsupported_count,
            pushed_filter_count,
            residual_filter_count,
        }
    }
}

impl ProviderFilterDecision {
    fn unsupported(
        input_index: usize,
        filter: &Expr,
        schema: &SchemaRef,
        partition_columns: &HashSet<String>,
    ) -> Self {
        let mut referenced_columns = filter
            .column_refs()
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        referenced_columns.sort();
        referenced_columns.dedup();

        let unknown_columns = referenced_columns
            .iter()
            .filter(|column| schema.field_with_name(column).is_err())
            .cloned()
            .collect::<Vec<_>>();
        let referenced_partition_columns = referenced_columns
            .iter()
            .filter(|column| partition_columns.contains(*column))
            .cloned()
            .collect::<Vec<_>>();
        let data_columns = referenced_columns
            .iter()
            .filter(|column| {
                schema.field_with_name(column).is_ok() && !partition_columns.contains(*column)
            })
            .cloned()
            .collect::<Vec<_>>();

        Self {
            input_index,
            pushdown: TableProviderFilterPushDown::Unsupported,
            kind: ProviderFilterPushdownKind::Unsupported,
            residual: true,
            reason: unsupported_filter_reason(filter, &unknown_columns),
            referenced_columns,
            partition_columns: referenced_partition_columns,
            data_columns,
            unknown_columns,
        }
    }
}

fn unsupported_filter_reason(filter: &Expr, unknown_columns: &[String]) -> ProviderFilterReason {
    if filter
        .column_refs()
        .iter()
        .any(|column| column.name.starts_with("__delta_funnel_"))
    {
        return ProviderFilterReason::UnsupportedInternalColumn;
    }

    if !unknown_columns.is_empty() {
        return ProviderFilterReason::UnsupportedUnknownColumn;
    }

    if is_simple_comparison(filter) {
        ProviderFilterReason::UnsupportedInitialPolicy
    } else {
        ProviderFilterReason::UnsupportedExpressionShape
    }
}

fn is_simple_comparison(filter: &Expr) -> bool {
    match filter {
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) =>
        {
            is_column_or_literal(binary.left.as_ref())
                && is_column_or_literal(binary.right.as_ref())
        }
        _ => false,
    }
}

fn is_column_or_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(_, _))
}
