//! DataFusion projection planning for Delta scans.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;

use crate::{DeltaFunnelError, error::DeltaScanProjectionSnafu};

#[allow(dead_code)]
pub(crate) struct ProjectionPlan {
    pub(crate) projected_schema: SchemaRef,
    pub(crate) scan_projection: Option<Vec<usize>>,
    pub(crate) projected_column_names: Option<Vec<String>>,
}

#[allow(dead_code)]
pub(crate) fn plan_projection(
    source_name: &str,
    table_uri: &str,
    schema: &SchemaRef,
    projection: Option<Vec<usize>>,
) -> Result<ProjectionPlan, DeltaFunnelError> {
    let Some(projection) = projection else {
        return Ok(ProjectionPlan {
            projected_schema: Arc::clone(schema),
            scan_projection: None,
            projected_column_names: None,
        });
    };

    if let Err(reason) = reject_duplicate_projection_indexes(&projection) {
        return projection_error(source_name, table_uri, reason);
    }

    let mut projected_column_names = Vec::with_capacity(projection.len());
    for index in &projection {
        let Some(field) = schema.fields().get(*index) else {
            return projection_error(
                source_name,
                table_uri,
                format!(
                    "projection index {index} is out of bounds for schema with {} fields",
                    schema.fields().len()
                ),
            );
        };

        projected_column_names.push(field.name().to_owned());
    }

    let projected_schema = match schema.as_ref().project(&projection) {
        Ok(schema) => Arc::new(schema),
        Err(error) => return projection_error(source_name, table_uri, error.to_string()),
    };

    Ok(ProjectionPlan {
        projected_schema,
        scan_projection: Some(projection),
        projected_column_names: Some(projected_column_names),
    })
}

#[allow(dead_code)]
fn reject_duplicate_projection_indexes(projection: &[usize]) -> Result<(), String> {
    let mut seen = HashSet::with_capacity(projection.len());

    for index in projection {
        if !seen.insert(*index) {
            return Err(format!("projection index {index} is duplicated"));
        }
    }

    Ok(())
}

fn projection_error<T>(
    source_name: &str,
    table_uri: &str,
    reason: impl Into<String>,
) -> Result<T, DeltaFunnelError> {
    DeltaScanProjectionSnafu {
        source_name: source_name.to_owned(),
        table_uri: table_uri.to_owned(),
        reason: reason.into(),
    }
    .fail()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::super::super::catalog::provider::DeltaTableProvider;
    use super::super::scan_plan::ProviderScanPlanRequest;
    use super::*;
    use crate::query_engine::datafusion::test_support::DeltaLogTable;
    use crate::{DeltaFunnelError, DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

    #[test]
    fn projected_scan_plan_preserves_requested_order() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("ordered-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![1, 0]),
            pushed_filters: Vec::new(),
        })?;

        assert_eq!(plan.scan_projection, Some(vec![1, 0]));
        assert_eq!(plan.projected_schema.fields().len(), 2);
        assert_eq!(plan.projected_schema.field(0).name(), "customer_name");
        assert_eq!(plan.projected_schema.field(1).name(), "id");
        assert_eq!(plan.kernel_scan().kernel_schema().num_fields(), 2);
        let kernel_names = plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(kernel_names, vec!["customer_name", "id"]);

        Ok(())
    }

    #[test]
    fn single_column_scan_plan_projects_kernel_and_arrow_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("single-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![1]),
            pushed_filters: Vec::new(),
        })?;

        assert_eq!(plan.projected_schema.fields().len(), 1);
        assert_eq!(plan.projected_schema.field(0).name(), "customer_name");
        assert_eq!(plan.projected_schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(plan.kernel_scan().kernel_schema().num_fields(), 1);
        let kernel_field = plan
            .kernel_scan()
            .kernel_schema()
            .field_at_index(0)
            .ok_or("missing projected kernel field")?;
        assert_eq!(kernel_field.name(), "customer_name");

        Ok(())
    }

    #[test]
    fn empty_projection_scan_plan_is_valid_for_count_style_scans()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("empty-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![]),
            pushed_filters: Vec::new(),
        })?;

        assert_eq!(plan.scan_projection, Some(vec![]));
        assert_eq!(plan.projected_schema.fields().len(), 0);
        assert_eq!(plan.kernel_scan().kernel_schema().num_fields(), 0);

        Ok(())
    }

    #[test]
    fn duplicate_projection_indexes_fail_before_kernel_scan_construction()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("duplicate-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let result = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![1, 1]),
            pushed_filters: Vec::new(),
        });

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaScanProjection {
                source_name,
                reason,
                ..
            }) if source_name == "orders"
                && reason.contains("projection index 1 is duplicated")
        ));

        Ok(())
    }

    #[test]
    fn invalid_projection_index_fails_before_execution() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("invalid-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let result = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![2]),
            pushed_filters: Vec::new(),
        });

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaScanProjection {
                source_name,
                reason,
                ..
            }) if source_name == "orders"
                && reason.contains("projection index 2 is out of bounds")
        ));

        Ok(())
    }

    #[test]
    fn hostile_projection_index_fails_without_overflow_or_panic()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("hostile-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let result = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![usize::MAX]),
            pushed_filters: Vec::new(),
        });

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaScanProjection {
                source_name,
                reason,
                ..
            }) if source_name == "orders"
                && reason.contains("projection index")
                && reason.contains("out of bounds")
        ));

        Ok(())
    }

    #[test]
    fn schema_drift_between_arrow_and_kernel_fails_instead_of_full_scan_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("schema-drift-projection-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let mut provider = DeltaTableProvider::try_new(source, preflight)?;
        provider.set_schema_for_tests(Arc::new(Schema::new(vec![Field::new(
            "ghost_column",
            DataType::Utf8,
            true,
        )])));

        let result = provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
            pushed_filters: Vec::new(),
        });

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaScanConstruction {
                source_name,
                source,
                ..
            }) if source_name == "orders"
                && source.to_string().contains("ghost_column")
        ));

        Ok(())
    }

    #[test]
    fn scan_construction_error_display_escapes_control_characters()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("schema-drift-redaction-scan-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let mut provider = DeltaTableProvider::try_new(source, preflight)?;
        provider.set_schema_for_tests(Arc::new(Schema::new(vec![Field::new(
            "ghost\ncolumn",
            DataType::Utf8,
            true,
        )])));

        let error = match provider.plan_scan(ProviderScanPlanRequest {
            requested_projection: Some(vec![0]),
            pushed_filters: Vec::new(),
        }) {
            Ok(_) => return Err("tampered schema should not build a kernel scan".into()),
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("ghost\\ncolumn"));
        assert!(!display.contains("ghost\ncolumn"));

        Ok(())
    }
}
