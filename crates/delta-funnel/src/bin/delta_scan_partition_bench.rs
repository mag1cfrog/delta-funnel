//! Portable synthetic Delta scan partition benchmark runner.

const MIB: u64 = 1024 * 1024;

fn main() {
    let shape = SyntheticDeltaTableShape::partitioned_event_log();

    println!(
        "shape_name,total_rows,active_files,active_bytes,active_mib,avg_file_size_bytes,partition_count,source_rows,string_columns,int_columns,double_columns,bigint_columns,timestamp_columns,boolean_columns"
    );
    println!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        shape.name,
        shape.total_rows,
        shape.active_file_count,
        shape.active_data_size_bytes,
        shape.active_data_size_mib(),
        shape.average_file_size_bytes(),
        shape.partitioning.partition_count,
        shape.source_split_rows(),
        shape.schema.type_count(SyntheticDataType::String),
        shape.schema.type_count(SyntheticDataType::Int),
        shape.schema.type_count(SyntheticDataType::Double),
        shape.schema.type_count(SyntheticDataType::Bigint),
        shape.schema.type_count(SyntheticDataType::Timestamp),
        shape.schema.type_count(SyntheticDataType::Boolean)
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyntheticDeltaTableShape {
    name: &'static str,
    total_rows: u64,
    active_file_count: usize,
    active_data_size_bytes: u64,
    file_size_bytes: DistributionSummary,
    rows_per_file: DistributionSummary,
    partitioning: PartitioningShape,
    delta_features: DeltaFeatureShape,
    schema: SchemaShape,
    row_distribution: RowDistributionShape,
    null_patterns: Vec<NullPattern>,
    cardinalities: Vec<CardinalityHint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DistributionSummary {
    average: u64,
    p50: u64,
    p90: u64,
    p99: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitioningShape {
    columns: [&'static str; 3],
    partition_count: usize,
    start_date: SyntheticDate,
    end_date: SyntheticDate,
    average_files_per_partition_hundredths: u16,
    max_files_per_partition: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyntheticDate {
    year: i32,
    month: u8,
    day: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeltaFeatureShape {
    min_reader_version: u32,
    min_writer_version: u32,
    compression: &'static str,
    table_features: Vec<&'static str>,
    deletion_vectors_enabled_in_source_shape: bool,
    active_deletion_vectors_in_benchmark: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaShape {
    columns: Vec<ColumnShape>,
    all_columns_nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnShape {
    name: &'static str,
    data_type: SyntheticDataType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyntheticDataType {
    String,
    Int,
    Double,
    Bigint,
    Timestamp,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RowDistributionShape {
    source_split: Vec<CategoryRowCount>,
    uniform_category: UniformCategoryShape,
    seasonality: Vec<CategoryRowCount>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CategoryRowCount {
    category: &'static str,
    rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UniformCategoryShape {
    column_name: &'static str,
    category_count: usize,
    min_rows_per_category: u64,
    max_rows_per_category: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NullPattern {
    column_name: &'static str,
    null_rows: u64,
    rationale: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CardinalityHint {
    column_name: &'static str,
    distinct_count: u64,
    max_length: Option<usize>,
}

impl SyntheticDeltaTableShape {
    fn partitioned_event_log() -> Self {
        Self {
            name: "synthetic_partitioned_event_log",
            total_rows: 12_808_140,
            active_file_count: 956,
            active_data_size_bytes: 411_857_013,
            file_size_bytes: DistributionSummary {
                average: 430_813,
                p50: 342_593,
                p90: 1_002_643,
                p99: 1_306_162,
            },
            rows_per_file: DistributionSummary {
                average: 13_398,
                p50: 10_764,
                p90: 31_156,
                p99: 41_191,
            },
            partitioning: PartitioningShape {
                columns: ["event_year", "event_month", "event_day"],
                partition_count: 933,
                start_date: SyntheticDate {
                    year: 2023,
                    month: 2,
                    day: 3,
                },
                end_date: SyntheticDate {
                    year: 2026,
                    month: 6,
                    day: 12,
                },
                average_files_per_partition_hundredths: 102,
                max_files_per_partition: 5,
            },
            delta_features: DeltaFeatureShape {
                min_reader_version: 3,
                min_writer_version: 7,
                compression: "zstd",
                table_features: vec![
                    "appendOnly",
                    "changeDataFeed",
                    "deletionVectors",
                    "invariants",
                ],
                deletion_vectors_enabled_in_source_shape: true,
                active_deletion_vectors_in_benchmark: 0,
            },
            schema: SchemaShape {
                all_columns_nullable: true,
                columns: synthetic_columns(),
            },
            row_distribution: RowDistributionShape {
                source_split: vec![
                    CategoryRowCount {
                        category: "source_a",
                        rows: 9_961_270,
                    },
                    CategoryRowCount {
                        category: "source_b",
                        rows: 2_846_870,
                    },
                ],
                uniform_category: UniformCategoryShape {
                    column_name: "category_num",
                    category_count: 7,
                    min_rows_per_category: 1_810_000,
                    max_rows_per_category: 1_840_000,
                },
                seasonality: vec![
                    CategoryRowCount {
                        category: "2023",
                        rows: 2_660_000,
                    },
                    CategoryRowCount {
                        category: "2024",
                        rows: 3_890_000,
                    },
                    CategoryRowCount {
                        category: "2025",
                        rows: 3_890_000,
                    },
                    CategoryRowCount {
                        category: "2026_through_06_12",
                        rows: 2_370_000,
                    },
                ],
            },
            null_patterns: vec![
                NullPattern {
                    column_name: "actor_numeric_id",
                    null_rows: 37_997,
                    rationale: "sparse optional numeric identifier",
                },
                NullPattern {
                    column_name: "optional_group_key",
                    null_rows: 390_223,
                    rationale: "sparse optional string grouping key",
                },
                NullPattern {
                    column_name: "metric_z",
                    null_rows: 9_961_270,
                    rationale: "missing for source_a rows",
                },
                NullPattern {
                    column_name: "quality_tier",
                    null_rows: 9_961_270,
                    rationale: "missing for source_a rows",
                },
                NullPattern {
                    column_name: "group_code",
                    null_rows: 9_961_270,
                    rationale: "missing for source_a rows",
                },
                NullPattern {
                    column_name: "validation_flag",
                    null_rows: 9_990_278,
                    rationale: "mostly unavailable boolean flag",
                },
            ],
            cardinalities: vec![
                CardinalityHint {
                    column_name: "primary_event_id",
                    distinct_count: 1_870_000,
                    max_length: Some(32),
                },
                CardinalityHint {
                    column_name: "secondary_event_id",
                    distinct_count: 1_830_000,
                    max_length: Some(36),
                },
                CardinalityHint {
                    column_name: "group_id",
                    distinct_count: 36_900,
                    max_length: Some(38),
                },
                CardinalityHint {
                    column_name: "optional_group_key",
                    distinct_count: 34_500,
                    max_length: Some(40),
                },
                CardinalityHint {
                    column_name: "actor_numeric_id",
                    distinct_count: 11_900,
                    max_length: None,
                },
                CardinalityHint {
                    column_name: "quality_tier",
                    distinct_count: 22,
                    max_length: Some(19),
                },
                CardinalityHint {
                    column_name: "group_code",
                    distinct_count: 50,
                    max_length: Some(10),
                },
            ],
        }
    }

    fn average_file_size_bytes(&self) -> u64 {
        self.active_data_size_bytes / self.active_file_count as u64
    }

    fn active_data_size_mib(&self) -> u64 {
        self.active_data_size_bytes / MIB
    }

    fn source_split_rows(&self) -> u64 {
        self.row_distribution
            .source_split
            .iter()
            .map(|category| category.rows)
            .sum()
    }
}

impl SchemaShape {
    fn type_count(&self, data_type: SyntheticDataType) -> usize {
        self.columns
            .iter()
            .filter(|column| column.data_type == data_type)
            .count()
    }
}

fn synthetic_columns() -> Vec<ColumnShape> {
    vec![
        string_column("primary_event_id"),
        string_column("group_id"),
        string_column("secondary_event_id"),
        string_column("source_kind"),
        bigint_column("actor_numeric_id"),
        bigint_column("category_num"),
        string_column("category_code"),
        double_column("metric_x"),
        double_column("metric_y"),
        double_column("metric_z"),
        timestamp_column("event_time"),
        int_column("event_year"),
        int_column("event_month"),
        int_column("event_day"),
        int_column("event_processed_year"),
        int_column("event_processed_month"),
        int_column("event_processed_day"),
        int_column("category_processed_year"),
        int_column("category_processed_month"),
        int_column("category_processed_day"),
        int_column("record_processed_year"),
        int_column("record_processed_month"),
        int_column("record_processed_day"),
        string_column("optional_group_key"),
        boolean_column("validation_flag"),
        string_column("quality_tier"),
        string_column("group_code"),
    ]
}

fn string_column(name: &'static str) -> ColumnShape {
    ColumnShape {
        name,
        data_type: SyntheticDataType::String,
    }
}

fn int_column(name: &'static str) -> ColumnShape {
    ColumnShape {
        name,
        data_type: SyntheticDataType::Int,
    }
}

fn double_column(name: &'static str) -> ColumnShape {
    ColumnShape {
        name,
        data_type: SyntheticDataType::Double,
    }
}

fn bigint_column(name: &'static str) -> ColumnShape {
    ColumnShape {
        name,
        data_type: SyntheticDataType::Bigint,
    }
}

fn timestamp_column(name: &'static str) -> ColumnShape {
    ColumnShape {
        name,
        data_type: SyntheticDataType::Timestamp,
    }
}

fn boolean_column(name: &'static str) -> ColumnShape {
    ColumnShape {
        name,
        data_type: SyntheticDataType::Boolean,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_partitioned_event_log_preserves_target_scale() {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();

        assert_eq!(shape.total_rows, 12_808_140);
        assert_eq!(shape.active_file_count, 956);
        assert_eq!(shape.active_data_size_bytes, 411_857_013);
        assert_eq!(shape.active_data_size_mib(), 392);
        assert_eq!(shape.average_file_size_bytes(), 430_812);
        assert_eq!(shape.file_size_bytes.average, 430_813);
        assert_eq!(shape.file_size_bytes.p50, 342_593);
        assert_eq!(shape.file_size_bytes.p90, 1_002_643);
        assert_eq!(shape.file_size_bytes.p99, 1_306_162);
        assert_eq!(shape.rows_per_file.average, 13_398);
        assert_eq!(shape.rows_per_file.p50, 10_764);
        assert_eq!(shape.rows_per_file.p90, 31_156);
        assert_eq!(shape.rows_per_file.p99, 41_191);
    }

    #[test]
    fn synthetic_partitioned_event_log_preserves_partition_shape() {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();

        assert_eq!(
            shape.partitioning.columns,
            ["event_year", "event_month", "event_day"]
        );
        assert_eq!(shape.partitioning.partition_count, 933);
        assert_eq!(
            shape.partitioning.start_date,
            SyntheticDate {
                year: 2023,
                month: 2,
                day: 3
            }
        );
        assert_eq!(
            shape.partitioning.end_date,
            SyntheticDate {
                year: 2026,
                month: 6,
                day: 12
            }
        );
        assert_eq!(
            shape.partitioning.average_files_per_partition_hundredths,
            102
        );
        assert_eq!(shape.partitioning.max_files_per_partition, 5);
    }

    #[test]
    fn synthetic_partitioned_event_log_preserves_schema_mix_without_domain_names() {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let names = shape
            .schema
            .columns
            .iter()
            .map(|column| column.name)
            .collect::<Vec<_>>();

        assert_eq!(shape.schema.columns.len(), 27);
        assert!(shape.schema.all_columns_nullable);
        assert_eq!(shape.schema.type_count(SyntheticDataType::String), 8);
        assert_eq!(shape.schema.type_count(SyntheticDataType::Int), 12);
        assert_eq!(shape.schema.type_count(SyntheticDataType::Double), 3);
        assert_eq!(shape.schema.type_count(SyntheticDataType::Bigint), 2);
        assert_eq!(shape.schema.type_count(SyntheticDataType::Timestamp), 1);
        assert_eq!(shape.schema.type_count(SyntheticDataType::Boolean), 1);
        assert!(!names.iter().any(|name| name.contains("game")));
        assert!(!names.iter().any(|name| name.contains("pitch")));
        assert!(!names.iter().any(|name| name.contains("fielder")));
        assert!(!names.iter().any(|name| name.contains("hawkeye")));
        assert!(!names.iter().any(|name| name.contains("trackman")));
    }

    #[test]
    fn synthetic_partitioned_event_log_preserves_distribution_and_null_patterns() {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();

        assert_eq!(shape.source_split_rows(), shape.total_rows);
        assert_eq!(shape.row_distribution.source_split[0].category, "source_a");
        assert_eq!(shape.row_distribution.source_split[0].rows, 9_961_270);
        assert_eq!(shape.row_distribution.source_split[1].category, "source_b");
        assert_eq!(shape.row_distribution.source_split[1].rows, 2_846_870);
        assert_eq!(shape.row_distribution.uniform_category.category_count, 7);
        assert_eq!(
            shape
                .null_patterns
                .iter()
                .find(|pattern| pattern.column_name == "metric_z")
                .map(|pattern| pattern.null_rows),
            Some(9_961_270)
        );
        assert_eq!(
            shape
                .null_patterns
                .iter()
                .find(|pattern| pattern.column_name == "validation_flag")
                .map(|pattern| pattern.null_rows),
            Some(9_990_278)
        );
    }

    #[test]
    fn synthetic_partitioned_event_log_records_delta_features_without_active_dvs() {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();

        assert_eq!(shape.delta_features.min_reader_version, 3);
        assert_eq!(shape.delta_features.min_writer_version, 7);
        assert_eq!(shape.delta_features.compression, "zstd");
        assert!(
            shape
                .delta_features
                .table_features
                .contains(&"deletionVectors")
        );
        assert!(
            shape
                .delta_features
                .deletion_vectors_enabled_in_source_shape
        );
        assert_eq!(shape.delta_features.active_deletion_vectors_in_benchmark, 0);
    }
}
