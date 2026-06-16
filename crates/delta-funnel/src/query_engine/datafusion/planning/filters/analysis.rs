//! Shared filter analysis for Delta provider pushdown planning.

use std::collections::HashSet;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{Expr, Operator};

use super::DeltaFilterPushdownRejectionReason;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Classification of which provider schema columns a filter references.
///
/// This is diagnostic and policy input. It does not by itself mean the provider
/// can enforce the filter exactly.
pub(crate) enum DeltaFilterColumnScope {
    /// The filter references only Delta partition columns.
    PartitionOnly,
    /// The filter references only non-partition data columns.
    DataOnly,
    /// The filter references both partition and non-partition data columns.
    PartitionAndData,
    /// The filter has unknown, internal, nested, or otherwise unsupported references.
    Unsupported,
}

#[derive(Clone, Debug, PartialEq)]
/// Reusable analysis for one DataFusion filter at the provider boundary.
///
/// The policy layer uses this to decide whether a filter is exact, inexact, or
/// unsupported.
pub(crate) struct DeltaFilterAnalysis {
    /// Broad partition/data/unsupported classification for referenced columns.
    pub(crate) scope: DeltaFilterColumnScope,
    /// Sorted unique column references as they appeared in the DataFusion expression.
    pub(crate) referenced_columns: Vec<String>,
    /// Referenced columns that match Delta partition columns after schema lookup.
    pub(crate) partition_columns: Vec<String>,
    /// Referenced columns that match provider data columns after schema lookup.
    pub(crate) data_columns: Vec<String>,
    /// Referenced columns that cannot be resolved against the provider schema.
    pub(crate) unknown_columns: Vec<String>,
}

/// Builds reusable diagnostics for a candidate DataFusion filter.
///
/// The analysis records referenced columns and partition/data/unknown scope. It
/// does not decide DataFusion pushdown status by itself; policy modules decide
/// whether this analysis is `Exact`, `Inexact`, or `Unsupported`.
pub(super) fn analyze_filter_for_pushdown(
    filter: &Expr,
    schema: &SchemaRef,
    partition_columns: &HashSet<String>,
) -> (DeltaFilterAnalysis, DeltaFilterPushdownRejectionReason) {
    let mut referenced_columns = filter
        .column_refs()
        .iter()
        .map(|column| column.flat_name())
        .collect::<Vec<_>>();
    referenced_columns.sort();
    referenced_columns.dedup();

    let unknown_columns = referenced_columns
        .iter()
        .filter(|column| {
            schema
                .field_with_name(schema_lookup_name(column, schema).as_str())
                .is_err()
        })
        .cloned()
        .collect::<Vec<_>>();
    let referenced_partition_columns = referenced_columns
        .iter()
        .filter(|column| partition_columns.contains(schema_lookup_name(column, schema).as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let data_columns = referenced_columns
        .iter()
        .filter(|column| {
            let lookup_name = schema_lookup_name(column, schema);
            schema.field_with_name(lookup_name.as_str()).is_ok()
                && !partition_columns.contains(lookup_name.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();

    let rejection_reason = filter_pushdown_rejection_reason(filter, &unknown_columns);

    (
        DeltaFilterAnalysis {
            scope: filter_analysis_scope(
                rejection_reason,
                &referenced_partition_columns,
                &data_columns,
            ),
            referenced_columns,
            partition_columns: referenced_partition_columns,
            data_columns,
            unknown_columns,
        },
        rejection_reason,
    )
}

fn schema_lookup_name(flat_column_ref: &str, schema: &SchemaRef) -> String {
    // Case 1: the flat reference already names a top-level Arrow field. This
    // also preserves unusual but legal top-level names that contain dots.
    // Example: schema has top-level `id`, input is `id`.
    // Example: schema has top-level `a.b`, input is `a.b`.
    if schema.field_with_name(flat_column_ref).is_ok() {
        return flat_column_ref.to_owned();
    }

    // Case 2: no qualifier or dotted path was present, and the exact top-level
    // lookup failed above. Keep the original name so it is reported as unknown.
    // Example: schema has no `ghost`, input is `ghost`.
    let Some((first_segment, _remainder)) = flat_column_ref.split_once('.') else {
        return flat_column_ref.to_owned();
    };

    // Case 3: the first path segment is itself a top-level field, as in
    // `profile.age` against a schema that contains `profile`. Treat this as a
    // nested-field style reference for this planning slice, keep the full
    // reference, and let the top-level lookup fail so the filter stays
    // unsupported.
    // Example: schema has top-level `profile`, input is `profile.age`.
    // Example: schema has top-level `profile`, input is `profile.address.city`.
    if schema.field_with_name(first_segment).is_ok() {
        flat_column_ref.to_owned()
    } else {
        let unqualified_name = flat_column_ref
            .rsplit_once('.')
            .map_or(flat_column_ref, |(_qualifier, name)| name);
        // Case 4: the prefix is not a top-level field, as in `orders.id`
        // against a provider schema with top-level `id`. Treat the prefix as a
        // relation qualifier and use the suffix for top-level schema metadata.
        // Example: schema has top-level `id`, input is `orders.id`.
        // Example: schema has top-level `id`, input is `catalog.public.orders.id`.
        unqualified_name.to_owned()
    }
}

fn filter_pushdown_rejection_reason(
    filter: &Expr,
    unknown_columns: &[String],
) -> DeltaFilterPushdownRejectionReason {
    if filter
        .column_refs()
        .iter()
        .any(|column| column.name.starts_with("__delta_funnel_"))
    {
        return DeltaFilterPushdownRejectionReason::InternalColumn;
    }

    if !unknown_columns.is_empty() {
        return DeltaFilterPushdownRejectionReason::UnknownColumn;
    }

    if is_filter_analysis_candidate(filter) {
        DeltaFilterPushdownRejectionReason::UnsupportedByPolicy
    } else {
        DeltaFilterPushdownRejectionReason::ExpressionShape
    }
}

fn filter_analysis_scope(
    rejection_reason: DeltaFilterPushdownRejectionReason,
    partition_columns: &[String],
    data_columns: &[String],
) -> DeltaFilterColumnScope {
    if rejection_reason != DeltaFilterPushdownRejectionReason::UnsupportedByPolicy {
        return DeltaFilterColumnScope::Unsupported;
    }

    match (partition_columns.is_empty(), data_columns.is_empty()) {
        (false, true) => DeltaFilterColumnScope::PartitionOnly,
        (true, false) => DeltaFilterColumnScope::DataOnly,
        (false, false) => DeltaFilterColumnScope::PartitionAndData,
        (true, true) => DeltaFilterColumnScope::Unsupported,
    }
}

fn is_filter_analysis_candidate(filter: &Expr) -> bool {
    match filter {
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            is_filter_analysis_candidate(binary.left.as_ref())
                && is_filter_analysis_candidate(binary.right.as_ref())
        }
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
        Expr::Not(inner) => is_filter_analysis_candidate(inner.as_ref()),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => is_column_or_literal(inner.as_ref()),
        Expr::Between(between) => {
            is_column_or_literal(between.expr.as_ref())
                && is_column_or_literal(between.low.as_ref())
                && is_column_or_literal(between.high.as_ref())
        }
        Expr::InList(in_list) => {
            is_column_or_literal(in_list.expr.as_ref())
                && in_list.list.iter().all(is_column_or_literal)
        }
        Expr::Column(_) => true,
        _ => false,
    }
}

fn is_column_or_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(_, _))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{
        ColumnarValue, Expr, TableProviderFilterPushDown, Volatility, cast, col, create_udf, lit,
    };

    use super::*;
    use crate::query_engine::datafusion::catalog::provider::DeltaTableProvider;
    use crate::query_engine::datafusion::planning::filters::{
        DeltaFilterPushdownOutcome, DeltaFilterPushdownRejectionReason,
    };
    use crate::query_engine::datafusion::test_support::{
        DEEP_NESTED_WITH_CITY_SCHEMA_FIELDS_JSON, DeltaLogTable, NESTED_SCHEMA_FIELDS_JSON,
        PARTITIONED_SCHEMA_FIELDS_JSON,
    };
    use crate::{DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

    #[test]
    fn filter_plan_preserves_order_duplicates_and_column_classification()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "ordered-filter-plan",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let region_filter = col("region").eq(lit("us-west"));
        let id_filter = col("id").gt(lit(1));
        let id_filter_duplicate = col("id").gt(lit(1));
        let unknown_filter = col("ghost_column").eq(lit("x"));
        let internal_filter = col("__delta_funnel_file_id").eq(lit("part-00001.parquet"));

        let plan = provider.plan_supports_filters_pushdown(&[
            &region_filter,
            &id_filter,
            &id_filter_duplicate,
            &unknown_filter,
            &internal_filter,
        ]);

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
        assert_eq!(plan.exact_count, 1);
        assert_eq!(plan.inexact_count, 2);
        assert_eq!(plan.unsupported_count, 2);
        assert_eq!(plan.pushed_filter_count, 3);
        assert_eq!(plan.residual_filter_count, 4);
        assert_eq!(plan.decisions.len(), 5);

        assert_eq!(plan.decisions[0].outcome, DeltaFilterPushdownOutcome::Exact);
        assert!(!plan.decisions[0].residual);
        assert!(plan.decisions[0].rejection_reason.is_none());
        assert_eq!(
            plan.decisions[0].filter_analysis.referenced_columns,
            vec!["region"]
        );
        assert_eq!(
            plan.decisions[0].filter_analysis.partition_columns,
            vec!["region"]
        );
        assert!(plan.decisions[0].filter_analysis.data_columns.is_empty());
        assert!(plan.decisions[0].filter_analysis.unknown_columns.is_empty());
        assert_eq!(
            plan.decisions[0].filter_analysis.scope,
            DeltaFilterColumnScope::PartitionOnly
        );

        assert_eq!(
            plan.decisions[1].filter_analysis.referenced_columns,
            vec!["id"]
        );
        assert_eq!(plan.decisions[1].filter_analysis.data_columns, vec!["id"]);
        assert!(
            plan.decisions[1]
                .filter_analysis
                .partition_columns
                .is_empty()
        );
        assert_eq!(
            plan.decisions[1].filter_analysis.scope,
            DeltaFilterColumnScope::DataOnly
        );
        assert_eq!(
            plan.decisions[2].filter_analysis.referenced_columns,
            vec!["id"]
        );
        assert_eq!(
            plan.decisions[3].rejection_reason,
            Some(DeltaFilterPushdownRejectionReason::UnknownColumn)
        );
        assert_eq!(
            plan.decisions[3].filter_analysis.unknown_columns,
            vec!["ghost_column"]
        );
        assert_eq!(
            plan.decisions[3].filter_analysis.scope,
            DeltaFilterColumnScope::Unsupported
        );
        assert_eq!(
            plan.decisions[4].rejection_reason,
            Some(DeltaFilterPushdownRejectionReason::InternalColumn)
        );
        assert_eq!(
            plan.decisions[4].filter_analysis.unknown_columns,
            vec!["__delta_funnel_file_id"]
        );
        assert_eq!(
            plan.decisions[4].filter_analysis.scope,
            DeltaFilterColumnScope::Unsupported
        );

        Ok(())
    }

    #[test]
    fn filter_analysis_scope_classifies_mixed_partition_and_data_columns_without_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "mixed-filter-analysis",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let filter = col("region").eq(col("id"));

        let plan = provider.plan_supports_filters_pushdown(&[&filter]);

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 1);
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, 1);
        assert_eq!(
            plan.decisions[0].filter_analysis.scope,
            DeltaFilterColumnScope::PartitionAndData
        );
        assert_eq!(
            plan.decisions[0].filter_analysis.referenced_columns,
            vec!["id", "region"]
        );
        assert_eq!(
            plan.decisions[0].filter_analysis.partition_columns,
            vec!["region"]
        );
        assert_eq!(plan.decisions[0].filter_analysis.data_columns, vec!["id"]);
        assert!(plan.decisions[0].filter_analysis.unknown_columns.is_empty());
        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaFilterPushdownRejectionReason::UnsupportedByPolicy)
        );
        Ok(())
    }

    #[test]
    fn filter_plan_marks_unhandled_expression_shapes_unsupported()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("complex-filter-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let cast_filter = cast(col("id"), DataType::Int64).eq(lit(7_i64));
        let scalar_udf = create_udf(
            "is_interesting",
            vec![DataType::Utf8],
            DataType::Boolean,
            Volatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))))),
        );
        let scalar_function_filter =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![col("customer_name")],
            ));

        let plan =
            provider.plan_supports_filters_pushdown(&[&cast_filter, &scalar_function_filter]);

        assert_eq!(plan.unsupported_count, 2);
        assert_eq!(plan.residual_filter_count, 2);
        assert!(
            plan.decisions
                .iter()
                .all(|decision| decision.filter_analysis.scope
                    == DeltaFilterColumnScope::Unsupported)
        );
        assert_eq!(
            plan.decisions
                .iter()
                .map(|decision| decision.rejection_reason)
                .collect::<Vec<_>>(),
            vec![
                Some(DeltaFilterPushdownRejectionReason::ExpressionShape),
                Some(DeltaFilterPushdownRejectionReason::ExpressionShape)
            ]
        );

        Ok(())
    }

    #[test]
    fn filter_plan_records_filter_analysis_shapes_without_overclaiming_pushdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "supported-kernel-filter-plan",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let mixed_and_filter = col("region").eq(lit("us-west")).and(col("id").gt(lit(1)));
        let data_or_filter = col("id").lt(lit(10)).or(col("id").gt(lit(100)));
        let not_filter = Expr::Not(Box::new(col("id").gt(lit(1))));
        let partition_in_filter =
            col("region").in_list(vec![lit("us-west"), lit("us-east"), lit("us-west")], false);
        let data_between_filter = col("id").between(lit(10), lit(20));
        let null_in_filter = col("region").in_list(
            vec![lit("us-west"), Expr::Literal(ScalarValue::Utf8(None), None)],
            false,
        );

        let plan = provider.plan_supports_filters_pushdown(&[
            &mixed_and_filter,
            &data_or_filter,
            &not_filter,
            &partition_in_filter,
            &data_between_filter,
            &null_in_filter,
        ]);

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
        assert_eq!(plan.exact_count, 1);
        assert_eq!(plan.inexact_count, 1);
        assert_eq!(plan.unsupported_count, 4);
        assert_eq!(plan.pushed_filter_count, 2);
        assert_eq!(plan.residual_filter_count, 5);
        assert_eq!(
            plan.decisions[0].outcome,
            DeltaFilterPushdownOutcome::Inexact
        );
        assert!(plan.decisions[0].rejection_reason.is_none());
        assert!(plan.decisions[0].kernel_scan_filter.is_some());
        assert_eq!(plan.decisions[3].outcome, DeltaFilterPushdownOutcome::Exact);
        assert!(plan.decisions[3].rejection_reason.is_none());
        assert!(
            plan.decisions
                .iter()
                .enumerate()
                .filter(|(index, _)| !matches!(*index, 0 | 3))
                .all(|(_, decision)| decision.rejection_reason
                    == Some(DeltaFilterPushdownRejectionReason::UnsupportedByPolicy))
        );
        assert_eq!(
            plan.decisions[0].filter_analysis.scope,
            DeltaFilterColumnScope::PartitionAndData
        );
        assert_eq!(
            plan.decisions[1].filter_analysis.scope,
            DeltaFilterColumnScope::DataOnly
        );
        assert_eq!(
            plan.decisions[2].filter_analysis.scope,
            DeltaFilterColumnScope::DataOnly
        );
        assert_eq!(
            plan.decisions[3].filter_analysis.scope,
            DeltaFilterColumnScope::PartitionOnly
        );
        assert_eq!(
            plan.decisions[4].filter_analysis.scope,
            DeltaFilterColumnScope::DataOnly
        );
        assert_eq!(
            plan.decisions[5].filter_analysis.scope,
            DeltaFilterColumnScope::PartitionOnly
        );
        assert!(plan.decisions[3].kernel_scan_filter.is_some());

        Ok(())
    }

    #[test]
    fn filter_plan_rejects_mixed_known_unknown_boolean_before_kernel_conversion()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "mixed-known-unknown-filter-plan",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let filter = col("region")
            .eq(lit("us-west"))
            .and(col("ghost_column").eq(lit("x")));

        let plan = provider.plan_supports_filters_pushdown(&[&filter]);

        assert_eq!(plan.unsupported_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaFilterPushdownRejectionReason::UnknownColumn)
        );
        assert_eq!(
            plan.decisions[0].filter_analysis.scope,
            DeltaFilterColumnScope::Unsupported
        );
        assert_eq!(
            plan.decisions[0].filter_analysis.referenced_columns,
            vec!["ghost_column", "region"]
        );
        assert_eq!(
            plan.decisions[0].filter_analysis.partition_columns,
            vec!["region"]
        );
        assert_eq!(
            plan.decisions[0].filter_analysis.unknown_columns,
            vec!["ghost_column"]
        );
        Ok(())
    }

    #[test]
    fn filter_plan_tracks_nested_field_reference_as_unsupported_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "nested-filter-plan",
            NESTED_SCHEMA_FIELDS_JSON,
            "[]",
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let nested_filter = col("profile.age").gt(lit(21));

        let plan = provider.plan_supports_filters_pushdown(&[&nested_filter]);

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 1);
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, 1);
        assert_eq!(
            plan.decisions[0].filter_analysis.referenced_columns,
            vec!["profile.age"]
        );
        assert_eq!(
            plan.decisions[0].filter_analysis.unknown_columns,
            vec!["profile.age"]
        );
        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaFilterPushdownRejectionReason::UnknownColumn)
        );
        Ok(())
    }

    #[test]
    fn filter_plan_does_not_misclassify_deep_nested_ref_as_top_level_suffix()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "deep-nested-filter-plan",
            DEEP_NESTED_WITH_CITY_SCHEMA_FIELDS_JSON,
            "[]",
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let nested_filter = col("profile.address.city").eq(lit("Phoenix"));

        let plan = provider.plan_supports_filters_pushdown(&[&nested_filter]);

        assert_eq!(plan.unsupported_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert_eq!(
            plan.decisions[0].filter_analysis.referenced_columns,
            vec!["profile.address.city"]
        );
        assert_eq!(
            plan.decisions[0].filter_analysis.unknown_columns,
            vec!["profile.address.city"]
        );
        assert!(plan.decisions[0].filter_analysis.data_columns.is_empty());
        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaFilterPushdownRejectionReason::UnknownColumn)
        );
        Ok(())
    }

    #[test]
    fn filter_plan_classifies_qualified_top_level_reference_without_losing_diagnostic_ref()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("qualified-filter-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
            storage_options: Default::default(),
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let qualified_filter = Expr::Column(Column::new(Some("orders"), "id")).eq(lit(7));

        let (analysis, rejection_reason) =
            analyze_filter_for_pushdown(&qualified_filter, &provider.schema(), &HashSet::new());

        assert_eq!(analysis.referenced_columns, vec!["orders.id"]);
        assert_eq!(analysis.data_columns, vec!["orders.id"]);
        assert!(analysis.unknown_columns.is_empty());
        assert_eq!(
            rejection_reason,
            DeltaFilterPushdownRejectionReason::UnsupportedByPolicy
        );
        Ok(())
    }
}
