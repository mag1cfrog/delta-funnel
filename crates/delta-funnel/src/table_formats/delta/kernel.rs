//! Private boundary for stability-sensitive `delta_kernel` APIs.

#![allow(unused_imports)]

use std::sync::Arc;

use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
pub(crate) use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
pub(crate) use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
pub(crate) use delta_kernel::engine::default::DefaultEngineBuilder;
pub(crate) use delta_kernel::engine::default::storage::store_from_url_opts;
pub(crate) use delta_kernel::scan::Scan;
pub(crate) use delta_kernel::scan::ScanMetadata;
pub(crate) use delta_kernel::scan::state::{DvInfo, ScanFile, transform_to_logical};
pub(crate) use delta_kernel::table_features::TABLE_FEATURES_MIN_READER_VERSION;
use delta_kernel::table_features::TableFeature;
pub(crate) use delta_kernel::{Snapshot, SnapshotRef, Version, try_parse_uri};

/// Protocol details extracted through the private kernel adapter boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeltaKernelProtocol {
    pub(crate) min_reader_version: i32,
    pub(crate) min_writer_version: i32,
    pub(crate) reader_features: Vec<String>,
    pub(crate) writer_features: Vec<String>,
}

/// Extracts the Delta protocol from a loaded snapshot.
#[must_use]
pub(crate) fn snapshot_protocol_report(snapshot: &SnapshotRef) -> DeltaKernelProtocol {
    let protocol = snapshot.table_configuration().protocol();

    DeltaKernelProtocol {
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features: feature_names(protocol.reader_features()),
        writer_features: feature_names(protocol.writer_features()),
    }
}

/// Converts the loaded snapshot logical Delta schema to an Arrow schema.
pub(crate) fn snapshot_arrow_schema(snapshot: &SnapshotRef) -> Result<ArrowSchemaRef, ArrowError> {
    let schema: ArrowSchema = snapshot.schema().as_ref().try_into_arrow()?;

    Ok(Arc::new(schema))
}

fn feature_names(features: Option<&[TableFeature]>) -> Vec<String> {
    features
        .unwrap_or_default()
        .iter()
        .map(feature_name)
        .collect()
}

fn feature_name(feature: &TableFeature) -> String {
    match feature {
        TableFeature::Unknown(name) => name.clone(),
        _ => feature.as_ref().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ArrowEngineData, DefaultEngineBuilder, DvInfo, EngineDataArrowExt, Scan, ScanFile,
        ScanMetadata, Snapshot, SnapshotRef, Version, store_from_url_opts, transform_to_logical,
        try_parse_uri,
    };
    use arrow_tiberius::{MssqlProfile, PlanOptions, plan_arrow_schema_to_mssql_mappings};
    use delta_kernel::arrow::datatypes::{DataType, Field, Schema};

    fn collect_scan_file(files: &mut Vec<ScanFile>, file: ScanFile) {
        files.push(file);
    }

    fn snapshot_ref_version(snapshot: SnapshotRef) -> Version {
        snapshot.version()
    }

    #[test]
    fn delta_kernel_internal_api_symbols_are_available() {
        let _ = DefaultEngineBuilder::new;
        let _ = Scan::scan_metadata;
        let _ = ScanMetadata::visit_scan_files::<Vec<ScanFile>>;
        let _ = DvInfo::get_selection_vector;
        let _ = transform_to_logical;
        let _ = ArrowEngineData::new;
        let _ = <Box<dyn delta_kernel::EngineData> as EngineDataArrowExt>::try_into_record_batch;
        let _ = collect_scan_file;
        let _ = snapshot_ref_version;
        let _ = super::snapshot_arrow_schema;
        let _ = super::snapshot_protocol_report;
        let _ = super::TABLE_FEATURES_MIN_READER_VERSION;
    }

    #[test]
    fn delta_kernel_snapshot_loading_path_is_available() -> delta_kernel::DeltaResult<()> {
        let table_url = try_parse_uri("memory:///")?;
        let store = store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>())?;
        let engine = DefaultEngineBuilder::new(store).build();

        let result = Snapshot::builder_for(table_url.as_str())
            .at_version(0)
            .build(&engine);

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn arrow_tiberius_accepts_delta_kernel_arrow_schema() -> arrow_tiberius::Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let outcome = plan_arrow_schema_to_mssql_mappings(
            &schema,
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )?;

        assert_eq!(outcome.value().len(), 1);
        Ok(())
    }
}
