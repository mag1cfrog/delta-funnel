//! Test fixtures for local Delta tables with real Parquet data files.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use delta_kernel::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int32Array, ListArray, MapArray, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use delta_kernel::arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::file::properties::WriterProperties;

use super::kernel;
const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
const DELETION_VECTOR_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#;
const COLUMN_MAPPING_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["columnMapping"],"writerFeatures":["columnMapping"]}}"#;
const METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const MISSING_NULLABLE_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"loyalty_tier\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const MISSING_NON_NULLABLE_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"required_code\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["region"],"configuration":{},"createdTime":1587968585495}}"#;
const COLUMN_MAPPING_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"phys_id\"}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"phys_customer_name\"}}]}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"2"},"createdTime":1587968585495}}"#;
const SUPPORTED_TYPES_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"active\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"payload\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"score_f32\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"score_f64\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"attributes\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const NESTED_PROFILE_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"first_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const MISSING_NULLABLE_NESTED_PROFILE_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"first_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"loyalty_tier\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const MISSING_NON_NULLABLE_NESTED_PROFILE_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"first_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"required_code\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const NESTED_COLUMN_MAPPING_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"phys_id\"}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"phys_customer_name\"}},{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"first_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":4,\"delta.columnMapping.physicalName\":\"phys_first_name\"}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":5,\"delta.columnMapping.physicalName\":\"phys_age\"}}]},\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":3,\"delta.columnMapping.physicalName\":\"phys_profile\"}}]}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"5"},"createdTime":1587968585495}}"#;
const ARRAY_ADDRESSES_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"addresses\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const MISSING_NULLABLE_ARRAY_ADDRESSES_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"addresses\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"country\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const MISSING_NON_NULLABLE_ARRAY_ADDRESSES_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"addresses\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"required_country\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const ARRAY_COLUMN_MAPPING_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"phys_id\"}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"phys_customer_name\"}},{\"name\":\"addresses\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":5,\"delta.columnMapping.physicalName\":\"phys_city\"}},{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":6,\"delta.columnMapping.physicalName\":\"phys_zip\"}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":3,\"delta.columnMapping.physicalName\":\"phys_addresses\",\"delta.columnMapping.nested.ids\":{\"phys_addresses.element\":4}}}]}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"6"},"createdTime":1587968585495}}"#;
const MAP_ATTRIBUTES_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const MISSING_NULLABLE_MAP_ATTRIBUTES_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"country\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const MISSING_NON_NULLABLE_MAP_ATTRIBUTES_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"required_country\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]},\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const MAP_COLUMN_MAPPING_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"phys_id\"}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"phys_customer_name\"}},{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":6,\"delta.columnMapping.physicalName\":\"phys_city\"}},{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":7,\"delta.columnMapping.physicalName\":\"phys_zip\"}}]},\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":3,\"delta.columnMapping.physicalName\":\"phys_attributes\",\"delta.columnMapping.nested.ids\":{\"phys_attributes.key\":4,\"phys_attributes.value\":5}}}]}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"7"},"createdTime":1587968585495}}"#;
const MAP_KEY_VALUE_COLUMN_MAPPING_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"phys_id\"}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"phys_customer_name\"}},{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"keyType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":6,\"delta.columnMapping.physicalName\":\"phys_key_city\"}},{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":7,\"delta.columnMapping.physicalName\":\"phys_key_zip\"}}]},\"valueType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":8,\"delta.columnMapping.physicalName\":\"phys_value_label\"}},{\"name\":\"score\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":9,\"delta.columnMapping.physicalName\":\"phys_value_score\"}}]},\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":3,\"delta.columnMapping.physicalName\":\"phys_attributes\",\"delta.columnMapping.nested.ids\":{\"phys_attributes.key\":4,\"phys_attributes.value\":5}}}]}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"9"},"createdTime":1587968585495}}"#;
const MAP_LIST_KEY_ATTRIBUTES_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"keyType\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const NESTED_MAP_KEY_ATTRIBUTES_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"keyType\":{\"type\":\"map\",\"keyType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"valueType\":\"integer\",\"valueContainsNull\":true},\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const DATA_FILE: &str = "part-00000.parquet";
const MODIFICATION_TIME_MS: i64 = 1_587_968_586_000;
const RELATIVE_DV_ID: &str = "vBn[lx{q8@P<9BNH/isA";
const RELATIVE_DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";

/// Local Delta fixture with one real Parquet data file.
pub(crate) struct RealParquetDeltaTable {
    path: PathBuf,
    rows: usize,
    data_file_size: u64,
}

impl Drop for RealParquetDeltaTable {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl RealParquetDeltaTable {
    /// Creates a local Delta table with one real Parquet file.
    pub(crate) fn new_default(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_batch(
            name,
            default_batch()?,
            AddStats {
                rows: 3,
                max_id: 3,
                min_customer: "alice".to_owned(),
                max_customer: "bob".to_owned(),
                customer_null_count: 1,
            },
        )
    }

    /// Creates a local Delta table whose single Parquet file has sequential ids.
    pub(crate) fn new_with_rows(
        name: &str,
        rows: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if rows == 0 {
            return Err("row count must be positive".into());
        }

        Self::new_with_batch(
            name,
            sequential_batch(rows)?,
            AddStats {
                rows,
                max_id: i32::try_from(rows)?,
                min_customer: "customer-1".to_owned(),
                max_customer: format!("customer-{rows}"),
                customer_null_count: 0,
            },
        )
    }

    /// Creates a local Delta table whose single sequential data file has a real
    /// deletion vector.
    pub(crate) fn new_with_rows_and_deletion_vector(
        name: &str,
        rows: usize,
        deleted_rows: &[u64],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if rows == 0 {
            return Err("row count must be positive".into());
        }

        Self::new_with_protocol_file_batches(
            name,
            DELETION_VECTOR_PROTOCOL_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![sequential_batch(rows)?],
                stats: AddStats {
                    rows,
                    max_id: i32::try_from(rows)?,
                    min_customer: "customer-1".to_owned(),
                    max_customer: format!("customer-{rows}"),
                    customer_null_count: 0,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: Some(deletion_vector_fixture(deleted_rows)?),
            }],
        )
    }

    /// Creates a local Delta table whose single DV-backed Parquet file is
    /// written from two record batches. The fixture exercises original row
    /// indexes across physical Parquet row-group boundaries.
    pub(crate) fn new_with_two_row_groups_and_deletion_vector(
        name: &str,
        rows_per_group: usize,
        deleted_rows: &[u64],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if rows_per_group == 0 {
            return Err("row count must be positive".into());
        }
        let rows = rows_per_group.saturating_mul(2);

        Self::new_with_protocol_file_batches(
            name,
            DELETION_VECTOR_PROTOCOL_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![
                    sequential_batch_starting_at(1, rows_per_group)?,
                    sequential_batch_starting_at(rows_per_group.saturating_add(1), rows_per_group)?,
                ],
                stats: AddStats {
                    rows,
                    max_id: i32::try_from(rows)?,
                    min_customer: "customer-1".to_owned(),
                    max_customer: format!("customer-{rows}"),
                    customer_null_count: 0,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: Some(deletion_vector_fixture(deleted_rows)?),
            }],
        )
    }

    /// Creates a local Delta table with two real Parquet files.
    pub(crate) fn new_with_two_files(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_file_batches(
            name,
            vec![
                file_batch(1, vec![(1, Some("file-a-1")), (2, Some("file-a-2"))])?,
                file_batch(2, vec![(3, Some("file-b-3")), (4, Some("file-b-4"))])?,
            ],
        )
    }

    /// Creates a local Delta table with two large real Parquet files.
    pub(crate) fn new_with_two_large_files(
        name: &str,
        rows_per_file: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if rows_per_file == 0 {
            return Err("row count must be positive".into());
        }

        Self::new_with_file_batches(
            name,
            vec![
                sequential_file_batch(1, 1, rows_per_file, "file-a")?,
                sequential_file_batch(2, rows_per_file.saturating_add(1), rows_per_file, "file-b")?,
            ],
        )
    }

    /// Creates a local Delta table with one data file and a real deletion vector.
    pub(crate) fn new_with_deletion_vector(
        name: &str,
        deleted_rows: &[u64],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_file_batches(
            name,
            DELETION_VECTOR_PROTOCOL_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![default_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: Some(deletion_vector_fixture(deleted_rows)?),
            }],
        )
    }

    /// Creates a local Delta table whose partition column must be materialized
    /// by the kernel physical-to-logical transform.
    pub(crate) fn new_with_partition_value(
        name: &str,
        region: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            PARTITIONED_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![default_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: format!(r#"{{"region":"{region}"}}"#),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local partitioned Delta table with one data file and a real
    /// deletion vector.
    pub(crate) fn new_with_partition_value_and_deletion_vector(
        name: &str,
        region: &str,
        deleted_rows: &[u64],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            DELETION_VECTOR_PROTOCOL_JSON,
            PARTITIONED_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![default_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: format!(r#"{{"region":"{region}"}}"#),
                deletion_vector: Some(deletion_vector_fixture(deleted_rows)?),
            }],
        )
    }

    /// Creates a local Delta table whose logical columns use different
    /// physical Parquet column names.
    pub(crate) fn new_with_column_mapping(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            COLUMN_MAPPING_PROTOCOL_JSON,
            COLUMN_MAPPING_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![physical_column_mapping_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table covering the scalar and nested data types
    /// the native async reader is expected to preserve without special Delta
    /// metadata features.
    pub(crate) fn new_with_supported_types(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            SUPPORTED_TYPES_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![supported_types_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose nested struct children are stored in a
    /// different order from the Delta schema and have no field-id metadata.
    pub(crate) fn new_with_reordered_nested_struct_fields(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            NESTED_PROFILE_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![reordered_nested_profile_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose log schema has a nullable nested
    /// struct child absent from the older Parquet data file.
    pub(crate) fn new_with_missing_nullable_nested_struct_field(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            MISSING_NULLABLE_NESTED_PROFILE_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![reordered_nested_profile_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose log schema has a non-nullable nested
    /// struct child absent from the older Parquet data file.
    pub(crate) fn new_with_missing_non_nullable_nested_struct_field(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            MISSING_NON_NULLABLE_NESTED_PROFILE_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![reordered_nested_profile_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose nested struct uses column mapping
    /// metadata and whose Parquet child names intentionally differ from Delta
    /// physical names.
    pub(crate) fn new_with_nested_column_mapping(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            COLUMN_MAPPING_PROTOCOL_JSON,
            NESTED_COLUMN_MAPPING_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![nested_column_mapping_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose array element struct children are
    /// stored in a different order from the Delta schema.
    pub(crate) fn new_with_reordered_array_struct_fields(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            ARRAY_ADDRESSES_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![array_addresses_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose log schema has a nullable array
    /// element struct child absent from the older Parquet data file.
    pub(crate) fn new_with_missing_nullable_array_struct_field(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            MISSING_NULLABLE_ARRAY_ADDRESSES_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![array_addresses_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose log schema has a non-nullable array
    /// element struct child absent from the older Parquet data file.
    pub(crate) fn new_with_missing_non_nullable_array_struct_field(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            MISSING_NON_NULLABLE_ARRAY_ADDRESSES_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![array_addresses_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose array element struct uses column
    /// mapping metadata and whose Parquet child names intentionally differ from
    /// Delta physical names.
    pub(crate) fn new_with_array_column_mapping(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            COLUMN_MAPPING_PROTOCOL_JSON,
            ARRAY_COLUMN_MAPPING_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![array_column_mapping_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose map value struct children are stored
    /// in a different order from the Delta schema.
    pub(crate) fn new_with_reordered_map_value_struct_fields(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            MAP_ATTRIBUTES_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![map_attributes_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose log schema has a nullable map value
    /// struct child absent from the older Parquet data file.
    pub(crate) fn new_with_missing_nullable_map_value_struct_field(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            MISSING_NULLABLE_MAP_ATTRIBUTES_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![map_attributes_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose log schema has a non-nullable map
    /// value struct child absent from the older Parquet data file.
    pub(crate) fn new_with_missing_non_nullable_map_value_struct_field(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            MISSING_NON_NULLABLE_MAP_ATTRIBUTES_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![map_attributes_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose map value struct uses column mapping
    /// metadata and whose Parquet child names intentionally differ from Delta
    /// physical names.
    pub(crate) fn new_with_map_column_mapping(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            COLUMN_MAPPING_PROTOCOL_JSON,
            MAP_COLUMN_MAPPING_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![map_column_mapping_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose map key and value structs both use
    /// column mapping metadata and whose Parquet child names intentionally
    /// differ from Delta physical names.
    pub(crate) fn new_with_map_key_value_column_mapping(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            COLUMN_MAPPING_PROTOCOL_JSON,
            MAP_KEY_VALUE_COLUMN_MAPPING_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![map_key_value_column_mapping_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose map key list element struct children
    /// are stored in a different order from the Delta schema.
    pub(crate) fn new_with_reordered_map_list_key_struct_fields(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            MAP_LIST_KEY_ATTRIBUTES_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![map_list_key_attributes_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose nested map key struct children are
    /// stored in a different order from the Delta schema.
    pub(crate) fn new_with_reordered_nested_map_key_struct_fields(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            NESTED_MAP_KEY_ATTRIBUTES_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![nested_map_key_attributes_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose log schema has a nullable column that
    /// is absent from the older Parquet data file.
    pub(crate) fn new_with_missing_nullable_column(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            MISSING_NULLABLE_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![default_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose log schema has a non-nullable column
    /// that is absent from the older Parquet data file.
    pub(crate) fn new_with_missing_non_nullable_column(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            MISSING_NON_NULLABLE_METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![default_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    /// Creates a local Delta table whose Parquet columns are stored in a
    /// different order from the Delta schema and have no field-id metadata.
    pub(crate) fn new_with_reordered_physical_columns(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            METADATA_JSON,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![reordered_physical_columns_batch()?],
                stats: AddStats {
                    rows: 3,
                    max_id: 3,
                    min_customer: "alice".to_owned(),
                    max_customer: "bob".to_owned(),
                    customer_null_count: 1,
                },
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    fn new_with_batch(
        name: &str,
        batch: kernel::RecordBatch,
        stats: AddStats,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_file_batches(
            name,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![batch],
                stats,
                partition_values_json: "{}".to_owned(),
                deletion_vector: None,
            }],
        )
    }

    fn new_with_file_batches(
        name: &str,
        files: Vec<RealParquetDataFile>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_file_batches(name, PROTOCOL_JSON, files)
    }

    fn new_with_protocol_file_batches(
        name: &str,
        protocol_json: &str,
        files: Vec<RealParquetDataFile>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(name, protocol_json, METADATA_JSON, files)
    }

    fn new_with_protocol_metadata_file_batches(
        name: &str,
        protocol_json: &str,
        metadata_json: &str,
        files: Vec<RealParquetDataFile>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path = Path::new("target")
            .join("delta-funnel-real-parquet-fixtures")
            .join(unique_name(name)?);
        let log_path = path.join("_delta_log");
        fs::create_dir_all(&log_path)?;

        let mut add_actions = Vec::with_capacity(files.len());
        let mut rows = 0_usize;
        let mut total_data_file_size = 0_u64;

        for file in files {
            rows = rows.saturating_add(
                file.batches
                    .iter()
                    .map(kernel::RecordBatch::num_rows)
                    .sum::<usize>(),
            );

            let first_batch = file
                .batches
                .first()
                .ok_or("data file must have at least one record batch")?;
            let max_row_group_size = file
                .batches
                .iter()
                .map(kernel::RecordBatch::num_rows)
                .min()
                .ok_or("data file must have at least one record batch")?;
            let writer_properties = WriterProperties::builder()
                .set_max_row_group_row_count(Some(max_row_group_size))
                .build();
            let mut writer = ArrowWriter::try_new(
                fs::File::create(path.join(&file.path))?,
                first_batch.schema(),
                Some(writer_properties),
            )?;
            for batch in &file.batches {
                writer.write(batch)?;
            }
            writer.close()?;

            let data_file_size = fs::metadata(path.join(&file.path))?.len();
            total_data_file_size = total_data_file_size.saturating_add(data_file_size);
            if let Some(deletion_vector) = &file.deletion_vector {
                fs::write(path.join(RELATIVE_DV_FILE), &deletion_vector.bytes)?;
                add_actions.push(dv_add_json(
                    &file.path,
                    data_file_size,
                    &file.stats,
                    &file.partition_values_json,
                    &deletion_vector.descriptor,
                ));
            } else {
                add_actions.push(add_json(
                    &file.path,
                    data_file_size,
                    &file.stats,
                    &file.partition_values_json,
                ));
            }
        }

        fs::write(
            log_path.join("00000000000000000000.json"),
            format!("{protocol_json}\n{metadata_json}\n"),
        )?;
        fs::write(
            log_path.join("00000000000000000001.json"),
            format!("{}\n", add_actions.join("\n")),
        )?;

        Ok(Self {
            path,
            rows,
            data_file_size: total_data_file_size,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn data_file_path(&self) -> &'static str {
        DATA_FILE
    }

    pub(crate) fn data_file_size(&self) -> u64 {
        self.data_file_size
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }
}

struct RealParquetDataFile {
    path: String,
    batches: Vec<kernel::RecordBatch>,
    stats: AddStats,
    partition_values_json: String,
    deletion_vector: Option<RealParquetDeletionVector>,
}

struct RealParquetDeletionVector {
    descriptor: DeletionVectorDescriptor,
    bytes: Vec<u8>,
}

struct AddStats {
    rows: usize,
    max_id: i32,
    min_customer: String,
    max_customer: String,
    customer_null_count: usize,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, true),
    ]))
}

fn physical_column_mapping_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("phys_id", DataType::Int32, false),
        Field::new("phys_customer_name", DataType::Utf8, true),
    ]))
}

fn nested_profile_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, true),
        Field::new(
            "profile",
            DataType::Struct(
                vec![
                    Field::new("first_name", DataType::Utf8, true),
                    Field::new("age", DataType::Int32, true),
                ]
                .into(),
            ),
            true,
        ),
    ]))
}

fn nested_column_mapping_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("phys_id", DataType::Int32, false).with_metadata(field_id_metadata(1)),
        Field::new("phys_customer_name", DataType::Utf8, true).with_metadata(field_id_metadata(2)),
        Field::new(
            "phys_profile",
            DataType::Struct(
                vec![
                    Field::new("stale_age", DataType::Int32, true)
                        .with_metadata(field_id_metadata(5)),
                    Field::new("stale_first_name", DataType::Utf8, true)
                        .with_metadata(field_id_metadata(4)),
                ]
                .into(),
            ),
            true,
        )
        .with_metadata(field_id_metadata(3)),
    ]))
}

fn array_addresses_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, true),
        Field::new(
            "addresses",
            DataType::List(Arc::new(Field::new(
                "element",
                DataType::Struct(
                    vec![
                        Field::new("city", DataType::Utf8, true),
                        Field::new("zip", DataType::Int32, true),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        ),
    ]))
}

fn array_column_mapping_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("phys_id", DataType::Int32, false).with_metadata(field_id_metadata(1)),
        Field::new("phys_customer_name", DataType::Utf8, true).with_metadata(field_id_metadata(2)),
        Field::new(
            "phys_addresses",
            DataType::List(Arc::new(
                Field::new(
                    "element",
                    DataType::Struct(
                        vec![
                            Field::new("stale_zip", DataType::Int32, true)
                                .with_metadata(field_id_metadata(6)),
                            Field::new("stale_city", DataType::Utf8, true)
                                .with_metadata(field_id_metadata(5)),
                        ]
                        .into(),
                    ),
                    true,
                )
                .with_metadata(field_id_metadata(4)),
            )),
            true,
        )
        .with_metadata(field_id_metadata(3)),
    ]))
}

fn map_attributes_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, true),
        Field::new(
            "attributes",
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new(
                                "value",
                                DataType::Struct(
                                    vec![
                                        Field::new("city", DataType::Utf8, true),
                                        Field::new("zip", DataType::Int32, true),
                                    ]
                                    .into(),
                                ),
                                true,
                            ),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]))
}

fn map_column_mapping_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("phys_id", DataType::Int32, false).with_metadata(field_id_metadata(1)),
        Field::new("phys_customer_name", DataType::Utf8, true).with_metadata(field_id_metadata(2)),
        Field::new(
            "phys_attributes",
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false)
                                .with_metadata(field_id_metadata(4)),
                            Field::new(
                                "value",
                                DataType::Struct(
                                    vec![
                                        Field::new("stale_zip", DataType::Int32, true)
                                            .with_metadata(field_id_metadata(7)),
                                        Field::new("stale_city", DataType::Utf8, true)
                                            .with_metadata(field_id_metadata(6)),
                                    ]
                                    .into(),
                                ),
                                true,
                            )
                            .with_metadata(field_id_metadata(5)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )
        .with_metadata(field_id_metadata(3)),
    ]))
}

fn map_key_value_column_mapping_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("phys_id", DataType::Int32, false).with_metadata(field_id_metadata(1)),
        Field::new("phys_customer_name", DataType::Utf8, true).with_metadata(field_id_metadata(2)),
        Field::new(
            "phys_attributes",
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(
                        vec![
                            Field::new(
                                "key",
                                DataType::Struct(
                                    vec![
                                        Field::new("stale_key_zip", DataType::Int32, true)
                                            .with_metadata(field_id_metadata(7)),
                                        Field::new("stale_key_city", DataType::Utf8, true)
                                            .with_metadata(field_id_metadata(6)),
                                    ]
                                    .into(),
                                ),
                                false,
                            )
                            .with_metadata(field_id_metadata(4)),
                            Field::new(
                                "value",
                                DataType::Struct(
                                    vec![
                                        Field::new("stale_value_score", DataType::Int32, true)
                                            .with_metadata(field_id_metadata(9)),
                                        Field::new("stale_value_label", DataType::Utf8, true)
                                            .with_metadata(field_id_metadata(8)),
                                    ]
                                    .into(),
                                ),
                                true,
                            )
                            .with_metadata(field_id_metadata(5)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )
        .with_metadata(field_id_metadata(3)),
    ]))
}

fn map_list_key_attributes_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, true),
        Field::new(
            "attributes",
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(
                        vec![
                            Field::new(
                                "key",
                                DataType::List(Arc::new(Field::new(
                                    "element",
                                    DataType::Struct(
                                        vec![
                                            Field::new("city", DataType::Utf8, true),
                                            Field::new("zip", DataType::Int32, true),
                                        ]
                                        .into(),
                                    ),
                                    true,
                                ))),
                                false,
                            ),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]))
}

fn nested_map_key_attributes_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, true),
        Field::new(
            "attributes",
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(
                        vec![
                            Field::new(
                                "key",
                                DataType::Map(
                                    Arc::new(Field::new(
                                        "key_value",
                                        DataType::Struct(
                                            vec![
                                                Field::new(
                                                    "key",
                                                    DataType::Struct(
                                                        vec![
                                                            Field::new(
                                                                "city",
                                                                DataType::Utf8,
                                                                true,
                                                            ),
                                                            Field::new(
                                                                "zip",
                                                                DataType::Int32,
                                                                true,
                                                            ),
                                                        ]
                                                        .into(),
                                                    ),
                                                    false,
                                                ),
                                                Field::new("value", DataType::Int32, true),
                                            ]
                                            .into(),
                                        ),
                                        false,
                                    )),
                                    false,
                                ),
                                false,
                            ),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]))
}

fn reordered_physical_columns_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("customer_name", DataType::Utf8, true),
        Field::new("id", DataType::Int32, false),
    ]))
}

fn supported_types_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("payload", DataType::Binary, true),
        Field::new("event_date", DataType::Date32, true),
        Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("amount", DataType::Decimal128(10, 2), true),
        Field::new("score_f32", DataType::Float32, true),
        Field::new("score_f64", DataType::Float64, true),
        Field::new(
            "attributes",
            DataType::Struct(
                vec![
                    Field::new("level", DataType::Int32, true),
                    Field::new("label", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
    ]))
}

fn default_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(schema(), columns)?)
}

fn physical_column_mapping_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        physical_column_mapping_schema(),
        columns,
    )?)
}

fn reordered_nested_profile_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let profile = StructArray::from(vec![
        (
            Arc::new(Field::new("first_name", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("age", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(34), Some(41), None])) as ArrayRef,
        ),
    ]);
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
        Arc::new(profile) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        nested_profile_schema(),
        columns,
    )?)
}

fn nested_column_mapping_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let profile = StructArray::from(vec![
        (
            Arc::new(
                Field::new("stale_age", DataType::Int32, true).with_metadata(field_id_metadata(5)),
            ),
            Arc::new(Int32Array::from(vec![Some(34), Some(41), None])) as ArrayRef,
        ),
        (
            Arc::new(
                Field::new("stale_first_name", DataType::Utf8, true)
                    .with_metadata(field_id_metadata(4)),
            ),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as ArrayRef,
        ),
    ]);
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
        Arc::new(profile) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        nested_column_mapping_schema(),
        columns,
    )?)
}

fn array_addresses_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let address_fields = vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("zip", DataType::Int32, true),
    ];
    let address_values = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("city", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![
                Some("san francisco"),
                Some("new york"),
                Some("phoenix"),
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("zip", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(94110), Some(10001), None])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let addresses = ListArray::try_new(
        Arc::new(Field::new(
            "element",
            DataType::Struct(address_fields.into()),
            true,
        )),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        address_values,
        Some(NullBuffer::from(vec![true, false, true])),
    )?;
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
        Arc::new(addresses) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        array_addresses_schema(),
        columns,
    )?)
}

fn array_column_mapping_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let address_fields = vec![
        Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(6)),
        Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(5)),
    ];
    let address_values = Arc::new(StructArray::from(vec![
        (
            Arc::new(
                Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(6)),
            ),
            Arc::new(Int32Array::from(vec![Some(94110), Some(10001), None])) as ArrayRef,
        ),
        (
            Arc::new(
                Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(5)),
            ),
            Arc::new(StringArray::from(vec![
                Some("san francisco"),
                Some("new york"),
                Some("phoenix"),
            ])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let addresses = ListArray::try_new(
        Arc::new(
            Field::new("element", DataType::Struct(address_fields.into()), true)
                .with_metadata(field_id_metadata(4)),
        ),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        address_values,
        Some(NullBuffer::from(vec![true, false, true])),
    )?;
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
        Arc::new(addresses) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        array_column_mapping_schema(),
        columns,
    )?)
}

fn map_attributes_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let key_field = Field::new("key", DataType::Utf8, false);
    let value_field = Field::new(
        "value",
        DataType::Struct(
            vec![
                Field::new("city", DataType::Utf8, true),
                Field::new("zip", DataType::Int32, true),
            ]
            .into(),
        ),
        true,
    );
    let value_array = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("city", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![
                Some("san francisco"),
                Some("new york"),
                Some("phoenix"),
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("zip", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(94110), Some(10001), None])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let entries = StructArray::new(
        vec![Arc::new(key_field.clone()), Arc::new(value_field.clone())].into(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("mailing"),
            ])) as ArrayRef,
            value_array,
        ],
        None,
    );
    let attributes = MapArray::try_new(
        Arc::new(Field::new(
            "key_value",
            DataType::Struct(vec![key_field, value_field].into()),
            false,
        )),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        entries,
        None,
        false,
    )?;
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
        Arc::new(attributes) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        map_attributes_schema(),
        columns,
    )?)
}

fn map_column_mapping_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let key_field = Field::new("key", DataType::Utf8, false).with_metadata(field_id_metadata(4));
    let value_field = Field::new(
        "value",
        DataType::Struct(
            vec![
                Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(7)),
                Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(6)),
            ]
            .into(),
        ),
        true,
    )
    .with_metadata(field_id_metadata(5));
    let value_array = Arc::new(StructArray::from(vec![
        (
            Arc::new(
                Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(7)),
            ),
            Arc::new(Int32Array::from(vec![Some(94110), Some(10001), None])) as ArrayRef,
        ),
        (
            Arc::new(
                Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(6)),
            ),
            Arc::new(StringArray::from(vec![
                Some("san francisco"),
                Some("new york"),
                Some("phoenix"),
            ])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let entries = StructArray::new(
        vec![Arc::new(key_field.clone()), Arc::new(value_field.clone())].into(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("mailing"),
            ])) as ArrayRef,
            value_array,
        ],
        None,
    );
    let attributes = MapArray::try_new(
        Arc::new(Field::new(
            "key_value",
            DataType::Struct(vec![key_field, value_field].into()),
            false,
        )),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        entries,
        None,
        false,
    )?;
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
        Arc::new(attributes) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        map_column_mapping_schema(),
        columns,
    )?)
}

fn map_key_value_column_mapping_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let key_field = Field::new(
        "key",
        DataType::Struct(
            vec![
                Field::new("stale_key_zip", DataType::Int32, true)
                    .with_metadata(field_id_metadata(7)),
                Field::new("stale_key_city", DataType::Utf8, true)
                    .with_metadata(field_id_metadata(6)),
            ]
            .into(),
        ),
        false,
    )
    .with_metadata(field_id_metadata(4));
    let value_field = Field::new(
        "value",
        DataType::Struct(
            vec![
                Field::new("stale_value_score", DataType::Int32, true)
                    .with_metadata(field_id_metadata(9)),
                Field::new("stale_value_label", DataType::Utf8, true)
                    .with_metadata(field_id_metadata(8)),
            ]
            .into(),
        ),
        true,
    )
    .with_metadata(field_id_metadata(5));
    let key_array = Arc::new(StructArray::from(vec![
        (
            Arc::new(
                Field::new("stale_key_zip", DataType::Int32, true)
                    .with_metadata(field_id_metadata(7)),
            ),
            Arc::new(Int32Array::from(vec![Some(94110), Some(10001), None])) as ArrayRef,
        ),
        (
            Arc::new(
                Field::new("stale_key_city", DataType::Utf8, true)
                    .with_metadata(field_id_metadata(6)),
            ),
            Arc::new(StringArray::from(vec![
                Some("san francisco"),
                Some("new york"),
                Some("phoenix"),
            ])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let value_array = Arc::new(StructArray::from(vec![
        (
            Arc::new(
                Field::new("stale_value_score", DataType::Int32, true)
                    .with_metadata(field_id_metadata(9)),
            ),
            Arc::new(Int32Array::from(vec![Some(7), Some(8), Some(9)])) as ArrayRef,
        ),
        (
            Arc::new(
                Field::new("stale_value_label", DataType::Utf8, true)
                    .with_metadata(field_id_metadata(8)),
            ),
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("mailing"),
            ])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let entries = StructArray::new(
        vec![Arc::new(key_field.clone()), Arc::new(value_field.clone())].into(),
        vec![key_array, value_array],
        None,
    );
    let attributes = MapArray::try_new(
        Arc::new(Field::new(
            "key_value",
            DataType::Struct(vec![key_field, value_field].into()),
            false,
        )),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        entries,
        None,
        false,
    )?;
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
        Arc::new(attributes) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        map_key_value_column_mapping_schema(),
        columns,
    )?)
}

fn map_list_key_attributes_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let element_field = Field::new(
        "element",
        DataType::Struct(
            vec![
                Field::new("city", DataType::Utf8, true),
                Field::new("zip", DataType::Int32, true),
            ]
            .into(),
        ),
        true,
    );
    let key_field = Field::new(
        "key",
        DataType::List(Arc::new(element_field.clone())),
        false,
    );
    let value_field = Field::new("value", DataType::Utf8, true);
    let key_element_array = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("city", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![
                Some("san francisco"),
                Some("new york"),
                Some("phoenix"),
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("zip", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(94110), Some(10001), None])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let key_array = Arc::new(ListArray::try_new(
        Arc::new(element_field),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        key_element_array,
        None,
    )?) as ArrayRef;
    let entries = StructArray::new(
        vec![Arc::new(key_field.clone()), Arc::new(value_field.clone())].into(),
        vec![
            key_array,
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("mailing"),
            ])) as ArrayRef,
        ],
        None,
    );
    let attributes = MapArray::try_new(
        Arc::new(Field::new(
            "key_value",
            DataType::Struct(vec![key_field, value_field].into()),
            false,
        )),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        entries,
        None,
        false,
    )?;
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
        Arc::new(attributes) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        map_list_key_attributes_schema(),
        columns,
    )?)
}

fn nested_map_key_attributes_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let inner_key_field = Field::new(
        "key",
        DataType::Struct(
            vec![
                Field::new("city", DataType::Utf8, true),
                Field::new("zip", DataType::Int32, true),
            ]
            .into(),
        ),
        false,
    );
    let inner_value_field = Field::new("value", DataType::Int32, true);
    let outer_key_field = Field::new(
        "key",
        DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(vec![inner_key_field.clone(), inner_value_field.clone()].into()),
                false,
            )),
            false,
        ),
        false,
    );
    let outer_value_field = Field::new("value", DataType::Utf8, true);
    let inner_key_array = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("city", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![
                Some("san francisco"),
                Some("new york"),
                Some("phoenix"),
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("zip", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(94110), Some(10001), None])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let inner_entries = StructArray::new(
        vec![
            Arc::new(inner_key_field.clone()),
            Arc::new(inner_value_field.clone()),
        ]
        .into(),
        vec![
            inner_key_array,
            Arc::new(Int32Array::from(vec![Some(7), Some(8), Some(9)])) as ArrayRef,
        ],
        None,
    );
    let outer_key_array = Arc::new(MapArray::try_new(
        Arc::new(Field::new(
            "key_value",
            DataType::Struct(vec![inner_key_field, inner_value_field].into()),
            false,
        )),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        inner_entries,
        None,
        false,
    )?) as ArrayRef;
    let outer_entries = StructArray::new(
        vec![
            Arc::new(outer_key_field.clone()),
            Arc::new(outer_value_field.clone()),
        ]
        .into(),
        vec![
            outer_key_array,
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("mailing"),
            ])) as ArrayRef,
        ],
        None,
    );
    let attributes = MapArray::try_new(
        Arc::new(Field::new(
            "key_value",
            DataType::Struct(vec![outer_key_field, outer_value_field].into()),
            false,
        )),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        outer_entries,
        None,
        false,
    )?;
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
        Arc::new(attributes) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        nested_map_key_attributes_schema(),
        columns,
    )?)
}

fn reordered_physical_columns_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let columns = vec![
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        reordered_physical_columns_schema(),
        columns,
    )?)
}

fn supported_types_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let attributes = StructArray::from(vec![
        (
            Arc::new(Field::new("level", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("label", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("low"), Some("high"), None])) as ArrayRef,
        ),
    ]);
    let tags = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(10), Some(20)]),
        Some(vec![Some(30)]),
        None,
    ]);
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as Arc<dyn Array>,
        Arc::new(BinaryArray::from(vec![
            Some(b"alpha".as_ref()),
            Some(b"beta".as_ref()),
            None,
        ])) as Arc<dyn Array>,
        Arc::new(Date32Array::from(vec![Some(19_723), Some(19_724), None])) as Arc<dyn Array>,
        Arc::new(
            TimestampMicrosecondArray::from(vec![
                Some(1_704_067_200_000_000),
                Some(1_704_153_600_000_000),
                None,
            ])
            .with_timezone("UTC"),
        ) as Arc<dyn Array>,
        Arc::new(
            Decimal128Array::from(vec![Some(12_345), Some(-6_789), None])
                .with_precision_and_scale(10, 2)?,
        ) as Arc<dyn Array>,
        Arc::new(Float32Array::from(vec![Some(1.25), Some(-2.5), None])) as Arc<dyn Array>,
        Arc::new(Float64Array::from(vec![Some(10.5), Some(-20.25), None])) as Arc<dyn Array>,
        Arc::new(attributes) as Arc<dyn Array>,
        Arc::new(tags) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(
        supported_types_schema(),
        columns,
    )?)
}

fn sequential_batch(rows: usize) -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    sequential_batch_starting_at(1, rows)
}

fn sequential_batch_starting_at(
    first_id: usize,
    rows: usize,
) -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    if rows == 0 {
        return Err("row count must be positive".into());
    }

    let first_id = i32::try_from(first_id)?;
    let row_count = i32::try_from(rows)?;
    let ids = (first_id..first_id + row_count).collect::<Vec<_>>();
    let names = (1..=row_count)
        .map(|offset| {
            let id = first_id.saturating_add(offset).saturating_sub(1);
            Some(format!("customer-{id}"))
        })
        .collect::<Vec<_>>();
    let columns = vec![
        Arc::new(Int32Array::from(ids)) as Arc<dyn Array>,
        Arc::new(StringArray::from(names)) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(schema(), columns)?)
}

fn field_id_metadata(field_id: i32) -> HashMap<String, String> {
    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_owned(), field_id.to_string())])
}

fn file_batch(
    index: usize,
    rows: Vec<(i32, Option<&str>)>,
) -> Result<RealParquetDataFile, Box<dyn std::error::Error>> {
    let row_count = rows.len();
    let path = format!("part-{index:05}.parquet");
    let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let names = rows
        .into_iter()
        .map(|(_, name)| name.map(str::to_owned))
        .collect::<Vec<_>>();
    let max_id = ids.iter().copied().max().ok_or("file must have rows")?;
    let min_customer = names
        .iter()
        .flatten()
        .min()
        .ok_or("file must have a non-null customer")?
        .to_string();
    let max_customer = names
        .iter()
        .flatten()
        .max()
        .ok_or("file must have a non-null customer")?
        .to_string();
    let customer_null_count = names.iter().filter(|name| name.is_none()).count();
    let columns = vec![
        Arc::new(Int32Array::from(ids)) as Arc<dyn Array>,
        Arc::new(StringArray::from(names)) as Arc<dyn Array>,
    ];

    Ok(RealParquetDataFile {
        path,
        batches: vec![kernel::RecordBatch::try_new(schema(), columns)?],
        stats: AddStats {
            rows: row_count,
            max_id,
            min_customer,
            max_customer,
            customer_null_count,
        },
        partition_values_json: "{}".to_owned(),
        deletion_vector: None,
    })
}

fn sequential_file_batch(
    index: usize,
    first_id: usize,
    rows: usize,
    customer_name: &str,
) -> Result<RealParquetDataFile, Box<dyn std::error::Error>> {
    let first_id = i32::try_from(first_id)?;
    let row_count = i32::try_from(rows)?;
    let ids = (first_id..first_id + row_count).collect::<Vec<_>>();
    let names = (0..rows)
        .map(|_| Some(customer_name.to_owned()))
        .collect::<Vec<_>>();
    let max_id = ids.iter().copied().max().ok_or("file must have rows")?;
    let columns = vec![
        Arc::new(Int32Array::from(ids)) as Arc<dyn Array>,
        Arc::new(StringArray::from(names)) as Arc<dyn Array>,
    ];

    Ok(RealParquetDataFile {
        path: format!("part-{index:05}.parquet"),
        batches: vec![kernel::RecordBatch::try_new(schema(), columns)?],
        stats: AddStats {
            rows,
            max_id,
            min_customer: customer_name.to_owned(),
            max_customer: customer_name.to_owned(),
            customer_null_count: 0,
        },
        partition_values_json: "{}".to_owned(),
        deletion_vector: None,
    })
}

fn add_json(path: &str, size: u64, stats: &AddStats, partition_values_json: &str) -> String {
    let rows = stats.rows;
    let max_id = stats.max_id;
    let min_customer = &stats.min_customer;
    let max_customer = &stats.max_customer;
    let null_count = stats.customer_null_count;
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{partition_values_json},"size":{size},"modificationTime":{MODIFICATION_TIME_MS},"dataChange":true,"stats":"{{\"numRecords\":{rows},\"minValues\":{{\"id\":1,\"customer_name\":\"{min_customer}\"}},\"maxValues\":{{\"id\":{max_id},\"customer_name\":\"{max_customer}\"}},\"nullCount\":{{\"id\":0,\"customer_name\":{null_count}}}}}"}}}}"#
    )
}

fn dv_add_json(
    path: &str,
    size: u64,
    stats: &AddStats,
    partition_values_json: &str,
    descriptor: &DeletionVectorDescriptor,
) -> String {
    let rows = stats.rows;
    let max_id = stats.max_id;
    let min_customer = &stats.min_customer;
    let max_customer = &stats.max_customer;
    let null_count = stats.customer_null_count;
    let storage_type = descriptor.storage_type;
    let path_or_inline_dv = &descriptor.path_or_inline_dv;
    let offset = descriptor.offset.unwrap_or(0);
    let size_in_bytes = descriptor.size_in_bytes;
    let cardinality = descriptor.cardinality;
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{partition_values_json},"size":{size},"modificationTime":{MODIFICATION_TIME_MS},"dataChange":true,"stats":"{{\"numRecords\":{rows},\"minValues\":{{\"id\":1,\"customer_name\":\"{min_customer}\"}},\"maxValues\":{{\"id\":{max_id},\"customer_name\":\"{max_customer}\"}},\"nullCount\":{{\"id\":0,\"customer_name\":{null_count}}}}}","deletionVector":{{"storageType":"{storage_type}","pathOrInlineDv":"{path_or_inline_dv}","offset":{offset},"sizeInBytes":{size_in_bytes},"cardinality":{cardinality}}}}}}}"#
    )
}

fn deletion_vector_fixture(
    deleted_rows: &[u64],
) -> Result<RealParquetDeletionVector, Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
    let mut deletion_vector = KernelDeletionVector::new();
    deletion_vector.add_deleted_row_indexes(deleted_rows.iter().copied());
    let write_result = writer.write_deletion_vector(deletion_vector)?;
    writer.finalize()?;

    Ok(RealParquetDeletionVector {
        descriptor: DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: RELATIVE_DV_ID.to_owned(),
            offset: Some(write_result.offset),
            size_in_bytes: write_result.size_in_bytes,
            cardinality: write_result.cardinality,
        },
        bytes: buffer,
    })
}

fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

    Ok(format!("{}-{name}-{nanos}", std::process::id()))
}
