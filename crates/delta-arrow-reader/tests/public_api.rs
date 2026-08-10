use std::error::Error as _;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use delta_arrow_reader::{
    DeltaBatchStream, DeltaComparison, DeltaPredicate, DeltaProtocolInfo, DeltaReadMetrics,
    DeltaReadMetricsSnapshot, DeltaReaderBackend, DeltaReaderError, DeltaReaderExecutionOptions,
    DeltaReaderPhase, DeltaScalar, DeltaScan, DeltaScanBuilder,
    DeltaScanPartitionTargetDiagnosticInput, DeltaScanPartitionTargetDiagnosticOutput,
    DeltaScanPartitionTargetDiagnosticSource, DeltaScanPartitionTargetLocalEnvironmentDiagnostic,
    DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus, DeltaSnapshotSelection,
    DeltaStorageOptions, DeltaTable, DeltaTableBuilder,
    delta_scan_partition_target_local_environment_diagnostic,
    derive_delta_scan_partition_target_diagnostic,
};
use futures_util::Stream;

#[test]
fn configuration_and_error_contract_is_public() -> Result<(), DeltaReaderError> {
    let snapshot: fn(&DeltaReadMetrics) -> DeltaReadMetricsSnapshot = DeltaReadMetrics::snapshot;
    let _ = snapshot;
    let snapshot_version: fn(&DeltaProtocolInfo) -> u64 = DeltaProtocolInfo::snapshot_version;
    let min_reader_version: fn(&DeltaProtocolInfo) -> i32 = DeltaProtocolInfo::min_reader_version;
    let min_writer_version: fn(&DeltaProtocolInfo) -> i32 = DeltaProtocolInfo::min_writer_version;
    let reader_features: for<'a> fn(&'a DeltaProtocolInfo) -> &'a [String] =
        DeltaProtocolInfo::reader_features;
    let writer_features: for<'a> fn(&'a DeltaProtocolInfo) -> &'a [String] =
        DeltaProtocolInfo::writer_features;
    let _ = (
        snapshot_version,
        min_reader_version,
        min_writer_version,
        reader_features,
        writer_features,
    );

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

#[test]
fn scan_partition_target_diagnostic_contract_is_public() -> Result<(), DeltaReaderError> {
    let _: DeltaScanPartitionTargetDiagnosticInput = Default::default();
    let local: DeltaScanPartitionTargetLocalEnvironmentDiagnostic =
        delta_scan_partition_target_local_environment_diagnostic();
    let _: DeltaScanPartitionTargetDiagnosticInput = local.policy_input;
    let _: Option<u64> = local.memory_total_bytes;
    let _: Option<u64> = local.memory_available_bytes;
    let _: Option<u64> = local.unix_soft_file_descriptor_limit;
    let _: DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus =
        local.unix_soft_file_descriptor_limit_status;
    let _ = [
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unsupported,
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unknown,
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Finite,
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unlimited,
    ];
    let input = DeltaScanPartitionTargetDiagnosticInput {
        explicit_target_partitions: None,
        datafusion_target_partitions: Some(8),
        available_parallelism: Some(4),
        available_memory_bytes: None,
        unix_soft_file_descriptor_limit: None,
        min_default_partitions: 1,
        parallelism_multiplier: 1,
        file_descriptors_per_partition: 16,
        available_memory_bytes_per_partition: 256 * 1024 * 1024,
    };
    let output: DeltaScanPartitionTargetDiagnosticOutput =
        derive_delta_scan_partition_target_diagnostic(input)?;

    assert_eq!(output.target_partitions, 4);
    assert_eq!(
        output.source,
        DeltaScanPartitionTargetDiagnosticSource::AvailableParallelismFallback
    );
    let _ = [
        DeltaScanPartitionTargetDiagnosticSource::ExplicitOverride,
        DeltaScanPartitionTargetDiagnosticSource::AvailableParallelismFallback,
        DeltaScanPartitionTargetDiagnosticSource::StaticFallback,
    ];
    assert_eq!(output.explicit_target_partitions, None);
    assert_eq!(output.datafusion_target_partitions, Some(8));
    assert_eq!(output.available_parallelism, Some(4));
    assert_eq!(output.datafusion_target_cap, Some(8));
    assert_eq!(output.unix_file_descriptor_cap, None);
    assert_eq!(output.memory_cap, None);
    Ok(())
}

#[test]
fn exact_predicate_model_is_public() {
    let comparisons = [
        DeltaComparison::Eq,
        DeltaComparison::NotEq,
        DeltaComparison::Lt,
        DeltaComparison::LtEq,
        DeltaComparison::Gt,
        DeltaComparison::GtEq,
    ];
    let copied_comparisons = comparisons;
    assert_eq!(copied_comparisons, comparisons);

    let scalars = vec![
        DeltaScalar::Boolean(true),
        DeltaScalar::Int8(1),
        DeltaScalar::Int16(2),
        DeltaScalar::Int32(3),
        DeltaScalar::Int64(4),
        DeltaScalar::Float32(5.0),
        DeltaScalar::Float64(6.0),
        DeltaScalar::Date32(7),
        DeltaScalar::Decimal128 {
            value: 8,
            precision: 9,
            scale: 1,
        },
        DeltaScalar::Utf8("utf8".into()),
        DeltaScalar::LargeUtf8("large utf8".into()),
        DeltaScalar::Binary(vec![10]),
        DeltaScalar::LargeBinary(vec![11]),
        DeltaScalar::FixedSizeBinary {
            size: 2,
            value: vec![12, 13],
        },
        DeltaScalar::TimestampMicrosecond {
            value: 14,
            timezone: Some("UTC".into()),
        },
    ];
    assert_eq!(scalars, scalars.clone());

    let predicates = vec![
        DeltaPredicate::Boolean(true),
        DeltaPredicate::Compare {
            column: "id".into(),
            op: DeltaComparison::Eq,
            value: DeltaScalar::Int64(1),
        },
        DeltaPredicate::IsNull {
            column: "optional".into(),
        },
        DeltaPredicate::IsNotNull {
            column: "required".into(),
        },
        DeltaPredicate::And(Vec::new()),
        DeltaPredicate::Or(Vec::new()),
        DeltaPredicate::Not(Box::new(DeltaPredicate::Boolean(false))),
    ];
    assert_eq!(predicates, predicates.clone());
    assert!(format!("{predicates:?}").contains("Compare"));
}

#[test]
fn direct_reader_contract_is_public() {
    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_batch_stream<T: Stream<Item = Result<RecordBatch, DeltaReaderError>>>() {}
    const fn table_version(table: &DeltaTable) -> u64 {
        table.version()
    }
    const fn scan_partition_count(scan: &DeltaScan) -> usize {
        scan.partition_count()
    }

    assert_send_sync::<DeltaTable>();
    assert_send::<DeltaBatchStream>();
    assert_batch_stream::<DeltaBatchStream>();

    let builder = DeltaTableBuilder::new("file:///tmp/table")
        .with_storage_options(DeltaStorageOptions::new())
        .with_snapshot_selection(DeltaSnapshotSelection::Version(1))
        .with_execution_options(DeltaReaderExecutionOptions::new());
    let _: DeltaTableBuilder = builder;
    let load: fn(DeltaTableBuilder) -> Result<DeltaTable, DeltaReaderError> =
        DeltaTableBuilder::load;
    let _ = load;
    let _ = DeltaTableBuilder::load_async;

    let version: fn(&DeltaTable) -> u64 = DeltaTable::version;
    let schema: for<'a> fn(&'a DeltaTable) -> &'a SchemaRef = DeltaTable::schema;
    let protocol: for<'a> fn(&'a DeltaTable) -> &'a DeltaProtocolInfo = DeltaTable::protocol;
    let table_uri: for<'a> fn(&'a DeltaTable) -> &'a str = DeltaTable::table_uri;
    let validate_protocol: fn(&DeltaTable) -> Result<(), DeltaReaderError> =
        DeltaTable::validate_protocol;
    let scan: for<'a> fn(&'a DeltaTable) -> DeltaScanBuilder<'a> = DeltaTable::scan;
    let _ = (
        version,
        table_version,
        schema,
        protocol,
        table_uri,
        validate_protocol,
        scan,
    );

    fn configure_scan<'a>(
        builder: DeltaScanBuilder<'a>,
        predicate: DeltaPredicate,
        options: DeltaReaderExecutionOptions,
    ) -> Result<DeltaScanBuilder<'a>, DeltaReaderError> {
        builder
            .with_projection(vec!["id".into()])
            .with_predicate(predicate)
            .with_limit(1)
            .with_target_partitions(1)?
            .with_execution_options(options)
    }
    let _ = configure_scan;
    let _ = DeltaScanBuilder::build;

    let scan_schema: for<'a> fn(&'a DeltaScan) -> &'a SchemaRef = DeltaScan::schema;
    let partition_count: fn(&DeltaScan) -> usize = DeltaScan::partition_count;
    let _ = (
        scan_schema,
        partition_count,
        scan_partition_count,
        DeltaScan::execute,
    );

    let stream_schema: for<'a> fn(&'a DeltaBatchStream) -> &'a SchemaRef = DeltaBatchStream::schema;
    let metrics: fn(&DeltaBatchStream) -> DeltaReadMetrics = DeltaBatchStream::metrics;
    let _ = (stream_schema, metrics);
}

#[cfg(not(feature = "native-async"))]
#[test]
fn disabled_native_backend_fails_before_uri_access() {
    let error = DeltaTableBuilder::new("this URI must never be inspected")
        .load()
        .expect_err("disabled default backend must fail");
    assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
    assert_eq!(error.as_str(), "unsupported_backend");
}

#[cfg(not(feature = "official-kernel"))]
#[test]
fn disabled_official_backend_fails_before_uri_access() -> Result<(), DeltaReaderError> {
    let options = DeltaReaderExecutionOptions::new()
        .with_reader_backend(DeltaReaderBackend::OfficialKernel)?;
    let error = DeltaTableBuilder::new("this URI must never be inspected")
        .with_execution_options(options)
        .load()
        .expect_err("disabled official backend must fail");
    assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
    assert_eq!(error.as_str(), "unsupported_backend");
    Ok(())
}
