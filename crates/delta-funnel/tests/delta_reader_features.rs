//! Integration coverage for supported Delta reader features.

use std::path::Path;

use datafusion::{assert_batches_eq, prelude::SessionContext};
use delta_funnel::{
    DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions, DeltaSourceConfig,
    DeltaTableProviderConfig, load_delta_source, preflight_delta_protocol,
    register_delta_sources_with_scan_execution_options,
};

#[tokio::test]
async fn type_widening_reads_old_and_new_physical_types_with_both_backends()
-> Result<(), Box<dyn std::error::Error>> {
    let table_uri = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("type-widening")
        .to_string_lossy()
        .into_owned();
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
