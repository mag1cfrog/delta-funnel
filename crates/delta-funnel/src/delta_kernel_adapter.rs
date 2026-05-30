//! Private boundary for stability-sensitive `delta_kernel` APIs.

#![allow(unused_imports)]

pub(crate) use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
pub(crate) use delta_kernel::engine::default::DefaultEngineBuilder;
pub(crate) use delta_kernel::scan::Scan;
pub(crate) use delta_kernel::scan::ScanMetadata;
pub(crate) use delta_kernel::scan::state::{DvInfo, ScanFile, transform_to_logical};

#[cfg(test)]
mod tests {
    use super::{
        ArrowEngineData, DefaultEngineBuilder, DvInfo, EngineDataArrowExt, Scan, ScanFile,
        ScanMetadata, transform_to_logical,
    };
    use arrow_tiberius::{MssqlProfile, PlanOptions, plan_arrow_schema_to_mssql_mappings};
    use delta_kernel::arrow::datatypes::{DataType, Field, Schema};

    fn collect_scan_file(files: &mut Vec<ScanFile>, file: ScanFile) {
        files.push(file);
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
