//! Table-format integrations.

mod delta;
mod name;

#[cfg(test)]
pub(crate) use delta::KernelScanFileStats;
#[cfg(test)]
pub(crate) use delta::KernelStructType;
#[cfg(test)]
pub(crate) use delta::build_projected_delta_scan;
#[cfg(test)]
pub(crate) use delta::test_support::RealParquetDeltaTable;
pub(crate) use delta::{
    DeltaKernelPartitionScalarAdapterError, DeltaKernelPredicate, KernelColumnMetadataKey,
    KernelColumnName, KernelDataFilePredicateEvalRequest, KernelDataFileReadRequest,
    KernelDataFileReader, KernelDataFileReaderConfig, KernelDataFileTransformRequest,
    KernelDataType, KernelDecimalData, KernelDeletionVectorReadRequest, KernelDeletionVectorReader,
    KernelDeletionVectorReaderConfig, KernelMetadataColumnSpec, KernelMetadataValue,
    KernelPhysicalToLogicalTransform, KernelPrimitiveType, KernelScalar,
    KernelScanDeletionVectorMetadata, KernelScanFileMetadata, KernelScanMetadataExpansion,
    KernelScanReadSchema, KernelSchemaRef, KernelStructField, ProjectedDeltaScan,
    ProviderDeletionVectorSelection, ProviderDeletionVectorSelectionContext,
    arrow_partition_type_to_kernel_primitive, build_projected_predicated_delta_scan,
    build_projected_predicated_stats_delta_scan, datafusion_expr_to_kernel_predicate,
    delta_source_arrow_schema, kernel_partition_scalar_to_datafusion_scalar,
};
pub use delta::{
    DeltaSourceConfig, DeltaStorageOptions, PlannedDeltaSource, ProtocolPreflight,
    load_delta_source, load_delta_source_with_tracing, load_delta_sources,
    preflight_delta_protocol, preflight_delta_protocol_with_tracing, preflight_delta_sources,
};
pub(crate) use name::validate_table_source_names;
