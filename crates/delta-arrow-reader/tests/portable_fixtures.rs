mod support;

use std::{error::Error, path::Path};

use support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[test]
fn every_frozen_real_parquet_fixture_is_portable() -> TestResult {
    let fixtures = [
        RealParquetDeltaTable::new_default("portable-default")?,
        RealParquetDeltaTable::new_with_rows("portable-rows", 5)?,
        RealParquetDeltaTable::new_with_rows_and_deletion_vector("portable-rows-dv", 5, &[1, 3])?,
        RealParquetDeltaTable::new_with_two_row_groups_and_deletion_vector(
            "portable-row-groups-dv",
            3,
            &[1, 4],
        )?,
        RealParquetDeltaTable::new_with_two_files("portable-two-files")?,
        RealParquetDeltaTable::new_with_two_files_and_deletion_vector(
            "portable-two-files-dv",
            &[1],
        )?,
        RealParquetDeltaTable::new_with_two_large_files("portable-large-files", 3)?,
        RealParquetDeltaTable::new_with_deletion_vector("portable-dv", &[1])?,
        RealParquetDeltaTable::new_with_partition_value("portable-partition", "us-west")?,
        RealParquetDeltaTable::new_with_two_partition_values("portable-two-partitions")?,
        RealParquetDeltaTable::new_with_null_partition_value("portable-null-partition")?,
        RealParquetDeltaTable::new_with_two_partition_columns("portable-two-columns")?,
        RealParquetDeltaTable::new_with_partition_value_and_deletion_vector(
            "portable-partition-dv",
            "us-west",
            &[1],
        )?,
        RealParquetDeltaTable::new_with_column_mapping("portable-column-mapping")?,
        RealParquetDeltaTable::new_with_supported_types("portable-supported-types")?,
        RealParquetDeltaTable::new_with_mixed_timestamp_physical_types(
            "portable-mixed-timestamps",
        )?,
        RealParquetDeltaTable::new_with_mixed_timestamp_physical_types_with_utc_nanoseconds(
            "portable-mixed-timestamps-utc",
        )?,
        RealParquetDeltaTable::new_with_mixed_nested_timestamp_physical_types(
            "portable-nested-timestamps",
        )?,
        RealParquetDeltaTable::new_with_reordered_nested_struct_fields(
            "portable-reordered-struct",
        )?,
        RealParquetDeltaTable::new_with_missing_nullable_nested_struct_field(
            "portable-missing-nullable-struct",
        )?,
        RealParquetDeltaTable::new_with_missing_non_nullable_nested_struct_field(
            "portable-missing-required-struct",
        )?,
        RealParquetDeltaTable::new_with_nested_column_mapping("portable-nested-column-mapping")?,
        RealParquetDeltaTable::new_with_reordered_array_struct_fields("portable-reordered-array")?,
        RealParquetDeltaTable::new_with_missing_nullable_array_struct_field(
            "portable-missing-nullable-array",
        )?,
        RealParquetDeltaTable::new_with_missing_non_nullable_array_struct_field(
            "portable-missing-required-array",
        )?,
        RealParquetDeltaTable::new_with_array_column_mapping("portable-array-mapping")?,
        RealParquetDeltaTable::new_with_array_struct_long_zip_leaf_cast(
            "portable-array-leaf-cast",
        )?,
        RealParquetDeltaTable::new_with_reordered_map_value_struct_fields(
            "portable-reordered-map-value",
        )?,
        RealParquetDeltaTable::new_with_missing_nullable_map_value_struct_field(
            "portable-missing-nullable-map-value",
        )?,
        RealParquetDeltaTable::new_with_missing_non_nullable_map_value_struct_field(
            "portable-missing-required-map-value",
        )?,
        RealParquetDeltaTable::new_with_map_column_mapping("portable-map-mapping")?,
        RealParquetDeltaTable::new_with_map_key_value_column_mapping(
            "portable-map-key-value-mapping",
        )?,
        RealParquetDeltaTable::new_with_map_long_key_leaf_cast("portable-map-key-cast")?,
        RealParquetDeltaTable::new_with_reordered_map_list_key_struct_fields(
            "portable-reordered-map-list-key",
        )?,
        RealParquetDeltaTable::new_with_reordered_nested_map_key_struct_fields(
            "portable-reordered-nested-map-key",
        )?,
        RealParquetDeltaTable::new_with_missing_nullable_column(
            "portable-missing-nullable-column",
        )?,
        RealParquetDeltaTable::new_with_missing_non_nullable_column(
            "portable-missing-required-column",
        )?,
        RealParquetDeltaTable::new_with_reordered_physical_columns("portable-reordered-columns")?,
    ];

    assert_eq!(fixtures.len(), 38);
    for fixture in fixtures {
        assert!(
            fixture
                .path()
                .join("_delta_log/00000000000000000000.json")
                .is_file()
        );
        assert!(
            fixture
                .path()
                .join("_delta_log/00000000000000000001.json")
                .is_file()
        );
        assert!(fixture.rows() > 0);
        assert!(fixture.data_file_size() > 0);
        assert!(!fixture.data_file_path().is_empty());
        assert!(fixture.path().starts_with(Path::new("target")));
    }
    Ok(())
}
