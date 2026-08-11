//! Integration coverage for supported Delta reader features.

use std::{path::Path, sync::Arc};

use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    assert_batches_eq,
    physical_plan::collect as collect_plan,
    prelude::SessionContext,
};
use delta_arrow_reader::{
    DeltaDataFusionScanOptions, DeltaReaderBackend, DeltaReaderExecutionOptions,
    DeltaScanPartitionTargetDiagnosticInput, DeltaTableBuilder, DeltaTableProvider,
    collect_delta_datafusion_metrics, derive_delta_scan_partition_target_diagnostic,
    register_delta_table,
};
use delta_funnel::{
    DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions, DeltaSourceConfig,
    DeltaTableProviderConfig, load_delta_source, preflight_delta_protocol,
    register_delta_sources_with_scan_execution_options,
};
use futures_util::TryStreamExt;

fn type_widening_fixture_uri() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("type-widening")
        .to_string_lossy()
        .into_owned()
}

#[tokio::test]
async fn type_widening_reads_old_and_new_physical_types_with_both_backends()
-> Result<(), Box<dyn std::error::Error>> {
    let table_uri = type_widening_fixture_uri();
    let expected = [
        "+---------------------+---------------------+---------------+--------------+-------------------------------+--------------+----------------------------+",
        "| byte_long           | int_long            | float_widened | byte_widened | decimal_decimal_greater_scale | int_decimal  | date_timestamp_ntz         |",
        "+---------------------+---------------------+---------------+--------------+-------------------------------+--------------+----------------------------+",
        "| 1                   | 2                   | true          | true         | 67.89000                      | 3.0          | 2024-09-09T00:00:00        |",
        "| 9223372036854775807 | 9223372036854775807 | false         | false        | 12345678901.23456             | 1234567890.1 | 2024-09-09T12:34:56.123456 |",
        "+---------------------+---------------------+---------------+--------------+-------------------------------+--------------+----------------------------+",
    ];

    for backend in [
        DeltaProviderReaderBackend::NativeAsync,
        DeltaProviderReaderBackend::OfficialKernel,
    ] {
        let context = SessionContext::new();
        let source = load_delta_source(DeltaSourceConfig::new("widened", &table_uri))?;
        let preflight = preflight_delta_protocol(&source)?;
        assert_eq!(
            preflight.protocol().reader_features,
            vec!["timestampNtz", "typeWidening-preview"]
        );
        register_delta_sources_with_scan_execution_options(
            &context,
            vec![DeltaTableProviderConfig {
                source,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            DeltaProviderScanExecutionOptions::try_new_with_reader_backend(backend, 1, 1)?,
        )?;

        let batches = context
            .sql(
                "select byte_long, int_long, \
                 float_double > 3.3 as float_widened, \
                 byte_double >= 5 as byte_widened, \
                 decimal_decimal_greater_scale, int_decimal, date_timestamp_ntz \
                 from widened order by byte_long",
            )
            .await?
            .collect()
            .await?;

        assert_batches_eq!(expected, &batches);
    }

    Ok(())
}

#[tokio::test]
async fn published_reader_uses_workspace_arrow_datafusion_and_async_types()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaTableBuilder::new(type_widening_fixture_uri())
        .load_async()
        .await?;
    table.validate_protocol()?;
    assert_eq!(
        table.protocol().reader_features(),
        ["timestampNtz", "typeWidening-preview"]
    );

    let schema: SchemaRef = Arc::clone(table.schema());
    assert!(schema.field_with_name("byte_long").is_ok());
    let batches: Vec<RecordBatch> = table
        .scan()
        .with_projection(vec!["byte_long".to_owned()])
        .with_target_partitions(1)?
        .build()
        .await?
        .execute()
        .await?
        .try_collect()
        .await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

    let official_options = DeltaReaderExecutionOptions::new()
        .with_native_async_prefetch_file_count_per_partition(0)?
        .with_parquet_metadata_size_hint(None)?
        .with_reader_backend(DeltaReaderBackend::OfficialKernel)?
        .with_max_concurrent_file_reads_per_scan(Some(2))?
        .with_max_concurrent_file_reads_per_partition(1)?;
    let _official_provider = DeltaTableProvider::try_new(
        table.clone(),
        DeltaDataFusionScanOptions {
            execution_options: official_options,
            target_partitions: Some(1),
        },
    )?;
    let target =
        derive_delta_scan_partition_target_diagnostic(DeltaScanPartitionTargetDiagnosticInput {
            explicit_target_partitions: Some(2),
            ..Default::default()
        })?;
    assert_eq!(target.target_partitions, 2);

    let context = SessionContext::new();
    let registered = register_delta_table(
        &context,
        "published_widened",
        table,
        DeltaDataFusionScanOptions::default(),
    )?;
    assert_eq!(registered.name, "published_widened");
    assert_eq!(registered.version, 2);
    let count_plan = context
        .sql("select count(*) as rows from published_widened")
        .await?;
    let count_plan = count_plan.create_physical_plan().await?;
    let metrics = collect_delta_datafusion_metrics(count_plan.as_ref());
    assert_eq!(metrics.len(), 1);
    let count = collect_plan(count_plan, context.task_ctx()).await?;
    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.reader.snapshot_version, 2);
    assert_eq!(
        metrics.reader.reader_backend,
        DeltaReaderBackend::NativeAsync
    );
    assert_eq!(metrics.reader.rows_produced, 2);
    assert_batches_eq!(
        ["+------+", "| rows |", "+------+", "| 2    |", "+------+",],
        &count
    );

    Ok(())
}
