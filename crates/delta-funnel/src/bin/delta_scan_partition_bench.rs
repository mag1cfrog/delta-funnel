//! Portable synthetic Delta scan partition benchmark runner.

use std::error::Error;
use std::fmt;

use chrono::{Datelike, Days, NaiveDate};

const MIB: u64 = 1024 * 1024;

fn main() -> Result<(), Box<dyn Error>> {
    let shape = SyntheticDeltaTableShape::partitioned_event_log();
    let file_set = shape.generate_file_set()?;

    println!(
        "shape_name,total_rows,active_files,active_bytes,active_mib,avg_file_size_bytes,partition_count,generated_files,generated_rows,generated_bytes,max_files_per_partition,source_rows,string_columns,int_columns,double_columns,bigint_columns,timestamp_columns,boolean_columns"
    );
    println!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        shape.name,
        shape.total_rows,
        shape.active_file_count,
        shape.active_data_size_bytes,
        shape.active_data_size_mib(),
        shape.average_file_size_bytes(),
        shape.partitioning.partition_count,
        file_set.files.len(),
        file_set.total_rows(),
        file_set.total_bytes(),
        file_set.max_files_per_partition(),
        shape.source_split_rows(),
        shape.schema.type_count(SyntheticDataType::String),
        shape.schema.type_count(SyntheticDataType::Int),
        shape.schema.type_count(SyntheticDataType::Double),
        shape.schema.type_count(SyntheticDataType::Bigint),
        shape.schema.type_count(SyntheticDataType::Timestamp),
        shape.schema.type_count(SyntheticDataType::Boolean)
    );

    Ok(())
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyntheticFileSet {
    partitions: Vec<SyntheticPartition>,
    files: Vec<SyntheticFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyntheticPartition {
    date: SyntheticDate,
    rows: u64,
    size_bytes: u64,
    file_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyntheticFile {
    path: String,
    partition_date: SyntheticDate,
    file_index_in_partition: usize,
    rows: u64,
    size_bytes: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SyntheticDate {
    year: i32,
    month: u8,
    day: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyntheticGenerationError {
    message: String,
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

    fn generate_file_set(&self) -> Result<SyntheticFileSet, SyntheticGenerationError> {
        let partition_dates = self.generate_partition_dates()?;
        let partition_rows = self.partition_rows(&partition_dates);
        let partition_bytes = apportion_by_weights(self.active_data_size_bytes, &partition_rows)?;
        let file_counts = self.file_counts_per_partition(partition_dates.len())?;

        let mut partitions = Vec::with_capacity(partition_dates.len());
        let mut files = Vec::with_capacity(self.active_file_count);

        for (partition_index, date) in partition_dates.into_iter().enumerate() {
            let rows = partition_rows[partition_index];
            let size_bytes = partition_bytes[partition_index];
            let file_count = file_counts[partition_index];
            partitions.push(SyntheticPartition {
                date,
                rows,
                size_bytes,
                file_count,
            });

            let file_rows = split_evenly(rows, file_count);
            let file_bytes = split_evenly(size_bytes, file_count);
            for file_index in 0..file_count {
                files.push(SyntheticFile {
                    path: synthetic_file_path(date, file_index),
                    partition_date: date,
                    file_index_in_partition: file_index,
                    rows: file_rows[file_index],
                    size_bytes: file_bytes[file_index],
                });
            }
        }

        Ok(SyntheticFileSet { partitions, files })
    }

    fn generate_partition_dates(&self) -> Result<Vec<SyntheticDate>, SyntheticGenerationError> {
        let year_counts = [
            (2023, 235_usize),
            (2024, 285_usize),
            (2025, 285_usize),
            (2026, 128_usize),
        ];
        let mut dates = Vec::with_capacity(self.partitioning.partition_count);

        for (year, count) in year_counts {
            let start = if year == self.partitioning.start_date.year {
                self.partitioning.start_date
            } else {
                SyntheticDate {
                    year,
                    month: 1,
                    day: 1,
                }
            };
            let end = if year == self.partitioning.end_date.year {
                self.partitioning.end_date
            } else {
                SyntheticDate {
                    year,
                    month: 12,
                    day: 31,
                }
            };
            let required_start =
                (year == self.partitioning.start_date.year).then_some(self.partitioning.start_date);
            let required_end =
                (year == self.partitioning.end_date.year).then_some(self.partitioning.end_date);

            dates.extend(select_active_dates_for_year(
                start,
                end,
                count,
                required_start,
                required_end,
            )?);
        }

        dates.sort();
        if dates.len() != self.partitioning.partition_count {
            return Err(generation_error(format!(
                "generated {} partitions, expected {}",
                dates.len(),
                self.partitioning.partition_count
            )));
        }

        Ok(dates)
    }

    fn partition_rows(&self, partition_dates: &[SyntheticDate]) -> Vec<u64> {
        let year_rows = [
            (2023, 2_660_000_u64),
            (2024, 3_890_000_u64),
            (2025, 3_890_000_u64),
            (2026, self.total_rows - 2_660_000 - 3_890_000 - 3_890_000),
        ];
        let mut rows = Vec::with_capacity(partition_dates.len());

        for (year, total_rows) in year_rows {
            let count = partition_dates
                .iter()
                .filter(|date| date.year == year)
                .count();
            rows.extend(split_evenly(total_rows, count));
        }

        rows
    }

    fn file_counts_per_partition(
        &self,
        partition_count: usize,
    ) -> Result<Vec<usize>, SyntheticGenerationError> {
        if self.active_file_count < partition_count {
            return Err(generation_error(
                "active file count must be at least partition count for this shape",
            ));
        }
        if self.active_file_count > partition_count * self.partitioning.max_files_per_partition {
            return Err(generation_error(
                "active file count exceeds max files per partition for this shape",
            ));
        }
        let mut file_counts = vec![1; partition_count];
        let mut remaining_extra_files = self.active_file_count - partition_count;

        for (seed_index, extra_files) in [(37_usize, 4_usize), (181, 2), (421, 2), (677, 2)] {
            let index = seed_index % partition_count;
            let capacity = self.partitioning.max_files_per_partition - file_counts[index];
            let assigned = capacity.min(extra_files).min(remaining_extra_files);
            file_counts[index] += assigned;
            remaining_extra_files -= assigned;
        }

        let mut cursor = 37_usize;

        while remaining_extra_files > 0 {
            let index = cursor % partition_count;
            if file_counts[index] < self.partitioning.max_files_per_partition {
                file_counts[index] += 1;
                remaining_extra_files -= 1;
            }
            cursor = cursor.wrapping_add(41);
        }

        Ok(file_counts)
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

impl SyntheticFileSet {
    fn total_rows(&self) -> u64 {
        self.files.iter().map(|file| file.rows).sum()
    }

    fn total_bytes(&self) -> u64 {
        self.files.iter().map(|file| file.size_bytes).sum()
    }

    fn max_files_per_partition(&self) -> usize {
        self.partitions
            .iter()
            .map(|partition| partition.file_count)
            .max()
            .unwrap_or_default()
    }
}

impl SyntheticDate {
    fn to_naive(self) -> Result<NaiveDate, SyntheticGenerationError> {
        NaiveDate::from_ymd_opt(self.year, u32::from(self.month), u32::from(self.day))
            .ok_or_else(|| generation_error("invalid synthetic date"))
    }

    fn from_naive(date: NaiveDate) -> Self {
        Self {
            year: date.year(),
            month: date.month() as u8,
            day: date.day() as u8,
        }
    }
}

impl fmt::Display for SyntheticDate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:04}-{:02}-{:02}",
            self.year, self.month, self.day
        )
    }
}

impl fmt::Display for SyntheticGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for SyntheticGenerationError {}

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

fn select_active_dates_for_year(
    start: SyntheticDate,
    end: SyntheticDate,
    count: usize,
    required_start: Option<SyntheticDate>,
    required_end: Option<SyntheticDate>,
) -> Result<Vec<SyntheticDate>, SyntheticGenerationError> {
    let mut selected = Vec::with_capacity(count);
    if let Some(date) = required_start {
        selected.push(date);
    }
    if let Some(date) = required_end
        && !selected.contains(&date)
    {
        selected.push(date);
    }

    let mut candidates = dates_between(start, end)?;
    candidates.retain(|date| !selected.contains(date));
    candidates.sort_by_key(|date| active_date_rank(*date));

    for candidate in candidates {
        if selected.len() == count {
            break;
        }
        selected.push(candidate);
    }

    if selected.len() != count {
        return Err(generation_error(format!(
            "selected {} active dates for {}, expected {}",
            selected.len(),
            start.year,
            count
        )));
    }

    selected.sort();
    Ok(selected)
}

fn dates_between(
    start: SyntheticDate,
    end: SyntheticDate,
) -> Result<Vec<SyntheticDate>, SyntheticGenerationError> {
    let mut current = start.to_naive()?;
    let end = end.to_naive()?;
    let mut dates = Vec::new();

    while current <= end {
        dates.push(SyntheticDate::from_naive(current));
        current = current
            .checked_add_days(Days::new(1))
            .ok_or_else(|| generation_error("synthetic date range overflow"))?;
    }

    Ok(dates)
}

fn active_date_rank(date: SyntheticDate) -> (u8, u8, u8) {
    let preferred_month = if (4..=9).contains(&date.month) { 0 } else { 1 };

    (preferred_month, date.month, date.day)
}

fn split_evenly(total: u64, count: usize) -> Vec<u64> {
    if count == 0 {
        return Vec::new();
    }

    let base = total / count as u64;
    let remainder = total % count as u64;
    (0..count)
        .map(|index| base + if (index as u64) < remainder { 1 } else { 0 })
        .collect()
}

fn apportion_by_weights(total: u64, weights: &[u64]) -> Result<Vec<u64>, SyntheticGenerationError> {
    if weights.is_empty() {
        return Ok(Vec::new());
    }

    let weight_sum: u128 = weights.iter().map(|weight| u128::from(*weight)).sum();
    if weight_sum == 0 {
        return Ok(split_evenly(total, weights.len()));
    }

    let mut values = Vec::with_capacity(weights.len());
    let mut remainders = Vec::with_capacity(weights.len());
    let mut assigned = 0_u64;

    for (index, weight) in weights.iter().enumerate() {
        let scaled = u128::from(total) * u128::from(*weight);
        let base = scaled / weight_sum;
        let value = u64::try_from(base)
            .map_err(|_| generation_error("apportioned value does not fit into u64"))?;
        values.push(value);
        remainders.push((scaled % weight_sum, index));
        assigned = assigned
            .checked_add(value)
            .ok_or_else(|| generation_error("apportioned value sum overflow"))?;
    }

    let remaining = total
        .checked_sub(assigned)
        .ok_or_else(|| generation_error("apportioned value sum exceeded total"))?;
    remainders.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));

    let remaining = usize::try_from(remaining)
        .map_err(|_| generation_error("remaining apportioned value count does not fit usize"))?;
    for (_, index) in remainders.into_iter().take(remaining) {
        values[index] = values[index]
            .checked_add(1)
            .ok_or_else(|| generation_error("apportioned value increment overflow"))?;
    }

    Ok(values)
}

fn synthetic_file_path(date: SyntheticDate, file_index: usize) -> String {
    format!(
        "event_year={}/event_month={:02}/event_day={:02}/part-{:05}.parquet",
        date.year, date.month, date.day, file_index
    )
}

fn generation_error(message: impl Into<String>) -> SyntheticGenerationError {
    SyntheticGenerationError {
        message: message.into(),
    }
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

    #[test]
    fn generated_file_set_preserves_target_partition_and_file_counts() -> Result<(), Box<dyn Error>>
    {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;

        assert_eq!(file_set.partitions.len(), 933);
        assert_eq!(file_set.files.len(), 956);
        assert_eq!(file_set.total_rows(), shape.total_rows);
        assert_eq!(file_set.total_bytes(), shape.active_data_size_bytes);
        assert_eq!(file_set.max_files_per_partition(), 5);
        assert_eq!(
            file_set.partitions.first().map(|partition| partition.date),
            Some(SyntheticDate {
                year: 2023,
                month: 2,
                day: 3
            })
        );
        assert_eq!(
            file_set.partitions.last().map(|partition| partition.date),
            Some(SyntheticDate {
                year: 2026,
                month: 6,
                day: 12
            })
        );

        Ok(())
    }

    #[test]
    fn generated_file_set_preserves_partition_totals() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;

        for partition in &file_set.partitions {
            let files = file_set
                .files
                .iter()
                .filter(|file| file.partition_date == partition.date)
                .collect::<Vec<_>>();

            assert_eq!(files.len(), partition.file_count);
            assert_eq!(
                files.iter().map(|file| file.rows).sum::<u64>(),
                partition.rows
            );
            assert_eq!(
                files.iter().map(|file| file.size_bytes).sum::<u64>(),
                partition.size_bytes
            );
            assert!(files.iter().enumerate().all(|(index, file)| {
                file.file_index_in_partition == index
                    && file.path.contains(&partition.date.year.to_string())
            }));
        }

        Ok(())
    }

    #[test]
    fn generated_file_set_is_deterministic() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();

        assert_eq!(shape.generate_file_set()?, shape.generate_file_set()?);

        Ok(())
    }
}
