use std::error::Error as _;

use delta_arrow_reader::{
    DeltaReadMetrics, DeltaReadMetricsSnapshot, DeltaReaderBackend, DeltaReaderError,
    DeltaReaderExecutionOptions, DeltaReaderPhase, DeltaSnapshotSelection, DeltaStorageOptions,
};

#[test]
fn configuration_and_error_contract_is_public() -> Result<(), DeltaReaderError> {
    let snapshot: fn(&DeltaReadMetrics) -> DeltaReadMetricsSnapshot = DeltaReadMetrics::snapshot;
    let _ = snapshot;

    let mut storage_options = DeltaStorageOptions::new();
    storage_options.insert("region".into(), "example".into());
    assert_eq!(storage_options.len(), 1);

    assert_eq!(
        DeltaSnapshotSelection::Version(3),
        DeltaSnapshotSelection::Version(3)
    );

    let options = DeltaReaderExecutionOptions::new()
        .with_reader_backend(DeltaReaderBackend::OfficialKernel)?
        .with_max_concurrent_file_reads_per_scan(Some(6))?
        .with_max_concurrent_file_reads_per_partition(3)?
        .with_output_buffer_capacity_per_partition(1)?
        .with_native_async_prefetch_file_count_per_partition(2)?
        .with_parquet_metadata_size_hint(Some(65_536))?
        .with_parquet_full_file_read_threshold(None)?;

    assert_eq!(options.reader_backend(), DeltaReaderBackend::OfficialKernel);
    options.validate()?;

    let error = DeltaReaderExecutionOptions::new()
        .with_output_buffer_capacity_per_partition(0)
        .expect_err("zero output capacity must fail");
    assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
    assert_eq!(error.as_str(), "invalid_configuration");
    assert!(error.source().is_none());

    Ok(())
}
