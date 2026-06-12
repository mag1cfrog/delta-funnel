//! Portable synthetic Delta scan partition benchmark runner.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

use chrono::{Datelike, Days, NaiveDate};
use delta_funnel::{
    DeltaScanPartitionTargetDiagnosticInput, DeltaScanPartitionTargetDiagnosticOutput,
    DeltaScanPartitionTargetDiagnosticSource, derive_delta_scan_partition_target_diagnostic,
};

const MIB: u64 = 1024 * 1024;
const BENCHMARK_FD_PER_PARTITION_CANDIDATES: [usize; 4] = [4, 8, 16, 32];
const BENCHMARK_MEMORY_BYTES_PER_PARTITION_CANDIDATES: [u64; 4] =
    [64 * MIB, 128 * MIB, 256 * MIB, 512 * MIB];
const BENCHMARK_UNIX_SOFT_FD_LIMIT: u64 = 128;
const BENCHMARK_AVAILABLE_MEMORY_BYTES: u64 = 1024 * MIB;
const BENCHMARK_CSV_HEADER: [&str; 36] = [
    "shape_name",
    "total_rows",
    "active_files",
    "active_bytes",
    "active_mib",
    "avg_file_size_bytes",
    "partition_count",
    "generated_files",
    "generated_rows",
    "generated_bytes",
    "max_files_per_partition",
    "source_rows",
    "string_columns",
    "int_columns",
    "double_columns",
    "bigint_columns",
    "timestamp_columns",
    "boolean_columns",
    "simulation_profile_count",
    "simulation_profile",
    "policy_case",
    "policy_available_parallelism",
    "policy_datafusion_target",
    "policy_available_memory_bytes",
    "policy_unix_soft_fd_limit",
    "policy_fd_per_partition",
    "policy_memory_bytes_per_partition",
    "policy_target",
    "policy_source",
    "policy_datafusion_cap",
    "policy_unix_fd_cap",
    "policy_memory_cap",
    "simulated_serial_micros",
    "simulated_max_file_micros",
    "simulated_output_partitions",
    "simulated_wall_micros",
];

fn main() -> Result<(), Box<dyn Error>> {
    let config = BenchmarkRunnerConfig::parse(env::args_os().skip(1))?;

    if config.show_help {
        print_usage(io::stdout())?;
        return Ok(());
    }

    if let Some(output_path) = config.output_path {
        let mut output = File::create(output_path)?;
        write_benchmark_csv(&mut output)?;
    } else {
        let stdout = io::stdout();
        let mut output = stdout.lock();
        write_benchmark_csv(&mut output)?;
    }

    Ok(())
}

fn write_benchmark_csv(output: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let shape = SyntheticDeltaTableShape::partitioned_event_log();
    let file_set = shape.generate_file_set()?;
    let simulation_profiles = SyntheticWorkSimulationProfile::standard_profiles();
    let policy_cases = BenchmarkPolicyCase::standard_cases(local_available_parallelism());

    writeln!(output, "{}", BENCHMARK_CSV_HEADER.join(","))?;
    for simulation in simulation_profiles {
        let simulated_work = simulation.simulate_file_set(&file_set)?;

        for policy_case in &policy_cases {
            let policy_decision = policy_case.derive_target()?;
            let partitioned_work = simulated_work
                .partition_by_estimated_bytes(&file_set, policy_decision.target_partitions)?;

            writeln!(
                output,
                "{}",
                benchmark_csv_row(BenchmarkCsvRowInput {
                    shape: &shape,
                    file_set: &file_set,
                    simulation_profile_count: simulation_profiles.len(),
                    simulation,
                    policy_case,
                    policy_decision,
                    simulated_work: &simulated_work,
                    partitioned_work: &partitioned_work,
                })
                .join(",")
            )?;
        }
    }

    Ok(())
}

fn print_usage(mut output: impl Write) -> io::Result<()> {
    writeln!(
        output,
        "Usage: delta_scan_partition_bench [--output <path>]"
    )?;
    writeln!(output)?;
    writeln!(
        output,
        "Writes a portable synthetic Delta scan partition benchmark matrix as CSV."
    )?;
    writeln!(
        output,
        "Without --output, CSV is written to stdout for shell pipelines."
    )?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchmarkRunnerConfig {
    output_path: Option<PathBuf>,
    show_help: bool,
}

impl BenchmarkRunnerConfig {
    fn parse<I>(args: I) -> Result<Self, BenchmarkRunnerConfigError>
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        let mut output_path = None;
        let mut show_help = false;
        let mut args = args.into_iter().map(Into::into);

        while let Some(arg) = args.next() {
            if arg == "--help" || arg == "-h" {
                show_help = true;
            } else if arg == "--output" || arg == "-o" {
                let path = args
                    .next()
                    .ok_or(BenchmarkRunnerConfigError::MissingOutputPath)?;
                if output_path.replace(PathBuf::from(path)).is_some() {
                    return Err(BenchmarkRunnerConfigError::DuplicateOutputPath);
                }
            } else {
                return Err(BenchmarkRunnerConfigError::UnknownArgument(
                    arg.to_string_lossy().into(),
                ));
            }
        }

        Ok(Self {
            output_path,
            show_help,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BenchmarkRunnerConfigError {
    MissingOutputPath,
    DuplicateOutputPath,
    UnknownArgument(String),
}

impl fmt::Display for BenchmarkRunnerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingOutputPath => write!(formatter, "--output requires a path"),
            Self::DuplicateOutputPath => write!(formatter, "--output may be provided only once"),
            Self::UnknownArgument(argument) => write!(formatter, "unknown argument `{argument}`"),
        }
    }
}

impl Error for BenchmarkRunnerConfigError {}

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchmarkPolicyCase {
    name: String,
    input: DeltaScanPartitionTargetDiagnosticInput,
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkCsvRowInput<'a> {
    shape: &'a SyntheticDeltaTableShape,
    file_set: &'a SyntheticFileSet,
    simulation_profile_count: usize,
    simulation: SyntheticWorkSimulationProfile,
    policy_case: &'a BenchmarkPolicyCase,
    policy_decision: DeltaScanPartitionTargetDiagnosticOutput,
    simulated_work: &'a SyntheticWorkSimulationResult,
    partitioned_work: &'a SyntheticPartitionedWorkPlan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyntheticWorkSimulationProfile {
    name: &'static str,
    open_latency_micros: u64,
    read_latency_micros: u64,
    bandwidth_bytes_per_second: u64,
    cpu_micros_per_1k_rows: u64,
    jitter_basis_points: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyntheticWorkSimulationResult {
    profile_name: &'static str,
    file_costs: Vec<SyntheticFileWorkCost>,
    serial_micros: u64,
    max_file_micros: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyntheticFileWorkCost {
    file_index: usize,
    size_bytes: u64,
    rows: u64,
    base_micros: u64,
    jitter_micros: u64,
    total_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyntheticPartitionedWorkPlan {
    target_partitions: usize,
    partitions: Vec<SyntheticWorkPartition>,
    wall_micros: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyntheticWorkPartition {
    partition_index: usize,
    file_count: usize,
    rows: u64,
    size_bytes: u64,
    work_micros: u64,
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

impl BenchmarkPolicyCase {
    fn standard_cases(available_parallelism: Option<usize>) -> Vec<Self> {
        let baseline = Self::baseline_input(available_parallelism);
        let mut cases = vec![Self::new("default_policy", baseline)];

        for file_descriptors_per_partition in BENCHMARK_FD_PER_PARTITION_CANDIDATES {
            cases.push(Self::new(
                format!("fd_per_partition_{file_descriptors_per_partition}"),
                DeltaScanPartitionTargetDiagnosticInput {
                    unix_soft_file_descriptor_limit: Some(BENCHMARK_UNIX_SOFT_FD_LIMIT),
                    file_descriptors_per_partition,
                    ..baseline
                },
            ));
        }

        for memory_bytes_per_partition in BENCHMARK_MEMORY_BYTES_PER_PARTITION_CANDIDATES {
            cases.push(Self::new(
                format!(
                    "memory_per_partition_{}mib",
                    memory_bytes_per_partition / MIB
                ),
                DeltaScanPartitionTargetDiagnosticInput {
                    available_memory_bytes: Some(BENCHMARK_AVAILABLE_MEMORY_BYTES),
                    available_memory_bytes_per_partition: memory_bytes_per_partition,
                    ..baseline
                },
            ));
        }

        for file_descriptors_per_partition in BENCHMARK_FD_PER_PARTITION_CANDIDATES {
            for memory_bytes_per_partition in BENCHMARK_MEMORY_BYTES_PER_PARTITION_CANDIDATES {
                cases.push(Self::new(
                    format!(
                        "combined_fd_{file_descriptors_per_partition}_memory_{}mib",
                        memory_bytes_per_partition / MIB
                    ),
                    DeltaScanPartitionTargetDiagnosticInput {
                        available_memory_bytes: Some(BENCHMARK_AVAILABLE_MEMORY_BYTES),
                        unix_soft_file_descriptor_limit: Some(BENCHMARK_UNIX_SOFT_FD_LIMIT),
                        file_descriptors_per_partition,
                        available_memory_bytes_per_partition: memory_bytes_per_partition,
                        ..baseline
                    },
                ));
            }
        }

        cases
    }

    fn new(name: impl Into<String>, input: DeltaScanPartitionTargetDiagnosticInput) -> Self {
        Self {
            name: name.into(),
            input,
        }
    }

    fn baseline_input(
        available_parallelism: Option<usize>,
    ) -> DeltaScanPartitionTargetDiagnosticInput {
        DeltaScanPartitionTargetDiagnosticInput {
            available_parallelism,
            datafusion_target_partitions: available_parallelism,
            ..DeltaScanPartitionTargetDiagnosticInput::default()
        }
    }

    fn derive_target(
        &self,
    ) -> Result<DeltaScanPartitionTargetDiagnosticOutput, delta_funnel::DeltaFunnelError> {
        derive_delta_scan_partition_target_diagnostic(self.input)
    }

    #[cfg(test)]
    fn with_input(name: impl Into<String>, input: DeltaScanPartitionTargetDiagnosticInput) -> Self {
        Self::new(name, input)
    }
}

impl SyntheticWorkSimulationProfile {
    fn local_fast() -> Self {
        Self {
            name: "local_fast",
            open_latency_micros: 100,
            read_latency_micros: 50,
            bandwidth_bytes_per_second: 1_500 * MIB,
            cpu_micros_per_1k_rows: 8,
            jitter_basis_points: 250,
        }
    }

    fn s3_normal() -> Self {
        Self {
            name: "s3_normal",
            open_latency_micros: 8_000,
            read_latency_micros: 4_000,
            bandwidth_bytes_per_second: 125 * MIB,
            cpu_micros_per_1k_rows: 10,
            jitter_basis_points: 1_500,
        }
    }

    fn s3_high_latency() -> Self {
        Self {
            name: "s3_high_latency",
            open_latency_micros: 35_000,
            read_latency_micros: 20_000,
            bandwidth_bytes_per_second: 100 * MIB,
            cpu_micros_per_1k_rows: 10,
            jitter_basis_points: 2_500,
        }
    }

    fn s3_throttled() -> Self {
        Self {
            name: "s3_throttled",
            open_latency_micros: 15_000,
            read_latency_micros: 8_000,
            bandwidth_bytes_per_second: 32 * MIB,
            cpu_micros_per_1k_rows: 10,
            jitter_basis_points: 2_000,
        }
    }

    fn cpu_heavy() -> Self {
        Self {
            name: "cpu_heavy",
            open_latency_micros: 1_000,
            read_latency_micros: 500,
            bandwidth_bytes_per_second: 500 * MIB,
            cpu_micros_per_1k_rows: 80,
            jitter_basis_points: 500,
        }
    }

    fn standard_profiles() -> [Self; 5] {
        [
            Self::local_fast(),
            Self::s3_normal(),
            Self::s3_high_latency(),
            Self::s3_throttled(),
            Self::cpu_heavy(),
        ]
    }

    fn simulate_file_set(
        self,
        file_set: &SyntheticFileSet,
    ) -> Result<SyntheticWorkSimulationResult, SyntheticGenerationError> {
        let mut serial_micros = 0_u64;
        let mut max_file_micros = 0_u64;
        let mut file_costs = Vec::with_capacity(file_set.files.len());

        for (file_index, file) in file_set.files.iter().enumerate() {
            let cost = self.simulate_file(file_index, file)?;
            serial_micros = serial_micros
                .checked_add(cost.total_micros)
                .ok_or_else(|| generation_error("simulated serial time overflow"))?;
            max_file_micros = max_file_micros.max(cost.total_micros);
            file_costs.push(cost);
        }

        Ok(SyntheticWorkSimulationResult {
            profile_name: self.name,
            file_costs,
            serial_micros,
            max_file_micros,
        })
    }

    fn simulate_file(
        self,
        file_index: usize,
        file: &SyntheticFile,
    ) -> Result<SyntheticFileWorkCost, SyntheticGenerationError> {
        let transfer_micros = scaled_ceil_div(
            file.size_bytes,
            1_000_000,
            self.bandwidth_bytes_per_second,
            "transfer time",
        )?;
        let cpu_micros =
            scaled_ceil_div(file.rows, self.cpu_micros_per_1k_rows, 1_000, "cpu time")?;
        let base_micros = self
            .open_latency_micros
            .checked_add(self.read_latency_micros)
            .and_then(|value| value.checked_add(transfer_micros))
            .and_then(|value| value.checked_add(cpu_micros))
            .ok_or_else(|| generation_error("simulated file base time overflow"))?;
        let jitter_micros = scaled_ceil_div(
            base_micros,
            u64::from(deterministic_jitter_basis_points(
                file,
                self.jitter_basis_points,
            )),
            10_000,
            "jitter time",
        )?;
        let total_micros = base_micros
            .checked_add(jitter_micros)
            .ok_or_else(|| generation_error("simulated file total time overflow"))?;

        Ok(SyntheticFileWorkCost {
            file_index,
            size_bytes: file.size_bytes,
            rows: file.rows,
            base_micros,
            jitter_micros,
            total_micros,
        })
    }
}

impl SyntheticWorkSimulationResult {
    fn partition_by_estimated_bytes(
        &self,
        file_set: &SyntheticFileSet,
        target_partitions: usize,
    ) -> Result<SyntheticPartitionedWorkPlan, SyntheticGenerationError> {
        if target_partitions == 0 {
            return Err(generation_error(
                "target partitions must be greater than zero",
            ));
        }
        if self.file_costs.len() != file_set.files.len() {
            return Err(generation_error(
                "simulated file costs must match generated files",
            ));
        }
        if file_set.files.is_empty() {
            return Ok(SyntheticPartitionedWorkPlan {
                target_partitions,
                partitions: Vec::new(),
                wall_micros: 0,
            });
        }

        let output_limit = target_partitions.min(file_set.files.len());
        let target_bytes = file_set.total_bytes().div_ceil(output_limit as u64);
        let mut partitions = Vec::new();
        let mut current = SyntheticWorkPartitionBuilder::default();

        for (file, cost) in file_set.files.iter().zip(&self.file_costs) {
            let can_start_next_partition = current.file_count > 0
                && partitions.len() + 1 < output_limit
                && current.size_bytes.saturating_add(file.size_bytes) > target_bytes;

            if can_start_next_partition {
                partitions.push(current.finish(partitions.len()));
                current = SyntheticWorkPartitionBuilder::default();
            }

            current.add(file, cost)?;
        }

        if current.file_count > 0 {
            partitions.push(current.finish(partitions.len()));
        }

        let wall_micros = partitions
            .iter()
            .map(|partition| partition.work_micros)
            .max()
            .unwrap_or_default();

        Ok(SyntheticPartitionedWorkPlan {
            target_partitions,
            partitions,
            wall_micros,
        })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SyntheticWorkPartitionBuilder {
    file_count: usize,
    rows: u64,
    size_bytes: u64,
    work_micros: u64,
}

impl SyntheticWorkPartitionBuilder {
    fn add(
        &mut self,
        file: &SyntheticFile,
        cost: &SyntheticFileWorkCost,
    ) -> Result<(), SyntheticGenerationError> {
        self.file_count = self
            .file_count
            .checked_add(1)
            .ok_or_else(|| generation_error("partition file count overflow"))?;
        self.rows = self
            .rows
            .checked_add(file.rows)
            .ok_or_else(|| generation_error("partition row count overflow"))?;
        self.size_bytes = self
            .size_bytes
            .checked_add(file.size_bytes)
            .ok_or_else(|| generation_error("partition byte count overflow"))?;
        self.work_micros = self
            .work_micros
            .checked_add(cost.total_micros)
            .ok_or_else(|| generation_error("partition work time overflow"))?;

        Ok(())
    }

    fn finish(self, partition_index: usize) -> SyntheticWorkPartition {
        SyntheticWorkPartition {
            partition_index,
            file_count: self.file_count,
            rows: self.rows,
            size_bytes: self.size_bytes,
            work_micros: self.work_micros,
        }
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

fn scaled_ceil_div(
    value: u64,
    scale: u64,
    denominator: u64,
    label: &str,
) -> Result<u64, SyntheticGenerationError> {
    if value == 0 || scale == 0 {
        return Ok(0);
    }
    if denominator == 0 {
        return Err(generation_error(format!("{label} denominator is zero")));
    }

    let scaled = u128::from(value) * u128::from(scale);
    let denominator = u128::from(denominator);
    let rounded = scaled.div_ceil(denominator);

    u64::try_from(rounded).map_err(|_| generation_error(format!("{label} does not fit into u64")))
}

fn deterministic_jitter_basis_points(file: &SyntheticFile, max_basis_points: u16) -> u16 {
    if max_basis_points == 0 {
        return 0;
    }

    let hash = deterministic_file_hash(file);
    let range = u64::from(max_basis_points) + 1;

    (hash % range) as u16
}

fn deterministic_file_hash(file: &SyntheticFile) -> u64 {
    let mut hash = 14_695_981_039_346_656_037_u64;

    for byte in file.path.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    for byte in file.rows.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    for byte in file.size_bytes.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }

    hash
}

fn local_available_parallelism() -> Option<usize> {
    std::thread::available_parallelism()
        .ok()
        .map(std::num::NonZeroUsize::get)
}

fn benchmark_csv_row(input: BenchmarkCsvRowInput<'_>) -> Vec<String> {
    let shape = input.shape;
    let file_set = input.file_set;
    let policy_case = input.policy_case;
    let policy_decision = input.policy_decision;

    vec![
        shape.name.to_owned(),
        shape.total_rows.to_string(),
        shape.active_file_count.to_string(),
        shape.active_data_size_bytes.to_string(),
        shape.active_data_size_mib().to_string(),
        shape.average_file_size_bytes().to_string(),
        shape.partitioning.partition_count.to_string(),
        file_set.files.len().to_string(),
        file_set.total_rows().to_string(),
        file_set.total_bytes().to_string(),
        file_set.max_files_per_partition().to_string(),
        shape.source_split_rows().to_string(),
        shape
            .schema
            .type_count(SyntheticDataType::String)
            .to_string(),
        shape.schema.type_count(SyntheticDataType::Int).to_string(),
        shape
            .schema
            .type_count(SyntheticDataType::Double)
            .to_string(),
        shape
            .schema
            .type_count(SyntheticDataType::Bigint)
            .to_string(),
        shape
            .schema
            .type_count(SyntheticDataType::Timestamp)
            .to_string(),
        shape
            .schema
            .type_count(SyntheticDataType::Boolean)
            .to_string(),
        input.simulation_profile_count.to_string(),
        input.simulation.name.to_owned(),
        policy_case.name.to_owned(),
        optional_usize(policy_case.input.available_parallelism),
        optional_usize(policy_case.input.datafusion_target_partitions),
        optional_u64(policy_case.input.available_memory_bytes),
        optional_u64(policy_case.input.unix_soft_file_descriptor_limit),
        policy_case.input.file_descriptors_per_partition.to_string(),
        policy_case
            .input
            .available_memory_bytes_per_partition
            .to_string(),
        policy_decision.target_partitions.to_string(),
        policy_source_name(policy_decision.source).to_owned(),
        optional_usize(policy_decision.datafusion_target_cap),
        optional_usize(policy_decision.unix_file_descriptor_cap),
        optional_usize(policy_decision.memory_cap),
        input.simulated_work.serial_micros.to_string(),
        input.simulated_work.max_file_micros.to_string(),
        input.partitioned_work.partitions.len().to_string(),
        input.partitioned_work.wall_micros.to_string(),
    ]
}

fn optional_usize(value: Option<usize>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn policy_source_name(source: DeltaScanPartitionTargetDiagnosticSource) -> &'static str {
    match source {
        DeltaScanPartitionTargetDiagnosticSource::ExplicitOverride => "explicit_override",
        DeltaScanPartitionTargetDiagnosticSource::AvailableParallelismFallback => {
            "available_parallelism_fallback"
        }
        DeltaScanPartitionTargetDiagnosticSource::StaticFallback => "static_fallback",
    }
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

    fn find_policy_case<'a>(
        cases: &'a [BenchmarkPolicyCase],
        name: &str,
    ) -> Result<&'a BenchmarkPolicyCase, Box<dyn Error>> {
        cases
            .iter()
            .find(|case| case.name == name)
            .ok_or_else(|| format!("missing policy case {name}").into())
    }

    #[test]
    fn runner_config_defaults_to_stdout() -> Result<(), Box<dyn Error>> {
        let config = BenchmarkRunnerConfig::parse(Vec::<&str>::new())?;

        assert_eq!(
            config,
            BenchmarkRunnerConfig {
                output_path: None,
                show_help: false
            }
        );

        Ok(())
    }

    #[test]
    fn runner_config_accepts_output_path() -> Result<(), Box<dyn Error>> {
        let config = BenchmarkRunnerConfig::parse(["--output", "target/scan-bench.csv"])?;

        assert_eq!(
            config.output_path,
            Some(PathBuf::from("target/scan-bench.csv"))
        );
        assert!(!config.show_help);

        Ok(())
    }

    #[test]
    fn runner_config_accepts_short_output_path() -> Result<(), Box<dyn Error>> {
        let config = BenchmarkRunnerConfig::parse(["-o", "target/scan-bench.csv"])?;

        assert_eq!(
            config.output_path,
            Some(PathBuf::from("target/scan-bench.csv"))
        );

        Ok(())
    }

    #[test]
    fn runner_config_accepts_help() -> Result<(), Box<dyn Error>> {
        let config = BenchmarkRunnerConfig::parse(["--help"])?;

        assert!(config.show_help);
        assert_eq!(config.output_path, None);

        Ok(())
    }

    #[test]
    fn runner_config_rejects_invalid_arguments() {
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--output"]),
            Err(BenchmarkRunnerConfigError::MissingOutputPath)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--output", "a.csv", "--output", "b.csv"]),
            Err(BenchmarkRunnerConfigError::DuplicateOutputPath)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--unknown"]),
            Err(BenchmarkRunnerConfigError::UnknownArgument(
                "--unknown".to_owned()
            ))
        );
    }

    #[test]
    fn print_usage_describes_output_path() -> Result<(), Box<dyn Error>> {
        let mut output = Vec::new();

        print_usage(&mut output)?;
        let usage = String::from_utf8(output)?;

        assert!(usage.contains("Usage: delta_scan_partition_bench [--output <path>]"));
        assert!(usage.contains("CSV is written to stdout"));

        Ok(())
    }

    #[test]
    fn write_benchmark_csv_outputs_portable_matrix() -> Result<(), Box<dyn Error>> {
        let mut output = Vec::new();

        write_benchmark_csv(&mut output)?;
        let csv = String::from_utf8(output)?;
        let lines = csv.lines().collect::<Vec<_>>();

        assert_eq!(lines.len(), 126);
        assert!(lines[0].starts_with("shape_name,total_rows"));
        assert!(csv.contains(",local_fast,default_policy,"));
        assert!(csv.contains(",s3_normal,combined_fd_16_memory_256mib,"));

        Ok(())
    }

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

    #[test]
    fn standard_simulation_profiles_are_stable() {
        let profiles = SyntheticWorkSimulationProfile::standard_profiles();

        assert_eq!(
            profiles.map(|profile| profile.name),
            [
                "local_fast",
                "s3_normal",
                "s3_high_latency",
                "s3_throttled",
                "cpu_heavy"
            ]
        );
        assert!(profiles.iter().all(|profile| {
            profile.bandwidth_bytes_per_second > 0 && profile.jitter_basis_points <= 10_000
        }));
    }

    #[test]
    fn simulated_file_work_is_deterministic() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let profile = SyntheticWorkSimulationProfile::s3_normal();

        assert_eq!(
            profile.simulate_file_set(&file_set)?,
            profile.simulate_file_set(&file_set)?
        );

        Ok(())
    }

    #[test]
    fn simulated_storage_profiles_change_total_work() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let local = SyntheticWorkSimulationProfile::local_fast().simulate_file_set(&file_set)?;
        let normal = SyntheticWorkSimulationProfile::s3_normal().simulate_file_set(&file_set)?;
        let high_latency =
            SyntheticWorkSimulationProfile::s3_high_latency().simulate_file_set(&file_set)?;
        let cpu_heavy = SyntheticWorkSimulationProfile::cpu_heavy().simulate_file_set(&file_set)?;

        assert_eq!(normal.profile_name, "s3_normal");
        assert_eq!(normal.file_costs.len(), file_set.files.len());
        assert!(normal.serial_micros > local.serial_micros);
        assert!(high_latency.serial_micros > normal.serial_micros);
        assert!(cpu_heavy.serial_micros > local.serial_micros);
        assert!(normal.max_file_micros <= normal.serial_micros);

        Ok(())
    }

    #[test]
    fn scaled_ceil_div_handles_zero_and_rounding() -> Result<(), Box<dyn Error>> {
        assert_eq!(scaled_ceil_div(0, 1_000, 7, "test")?, 0);
        assert_eq!(scaled_ceil_div(10, 1_000, 10, "test")?, 1_000);
        assert_eq!(scaled_ceil_div(10, 1_000, 6, "test")?, 1_667);

        Ok(())
    }

    #[test]
    fn partitioned_work_rejects_zero_target() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let work = SyntheticWorkSimulationProfile::s3_normal().simulate_file_set(&file_set)?;
        let error = work
            .partition_by_estimated_bytes(&file_set, 0)
            .err()
            .ok_or("expected zero target to fail")?;

        assert!(error.to_string().contains("greater than zero"));

        Ok(())
    }

    #[test]
    fn partitioned_work_target_one_matches_serial_work() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let work = SyntheticWorkSimulationProfile::s3_normal().simulate_file_set(&file_set)?;
        let plan = work.partition_by_estimated_bytes(&file_set, 1)?;

        assert_eq!(plan.partitions.len(), 1);
        assert_eq!(plan.wall_micros, work.serial_micros);
        assert_eq!(plan.partitions[0].file_count, file_set.files.len());
        assert_eq!(plan.partitions[0].rows, shape.total_rows);
        assert_eq!(plan.partitions[0].size_bytes, shape.active_data_size_bytes);

        Ok(())
    }

    #[test]
    fn partitioned_work_uses_known_size_grouping_shape() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let work = SyntheticWorkSimulationProfile::s3_normal().simulate_file_set(&file_set)?;
        let plan = work.partition_by_estimated_bytes(&file_set, 16)?;

        assert!(plan.partitions.len() <= 16);
        assert!(
            plan.partitions
                .iter()
                .all(|partition| partition.file_count > 0)
        );
        assert_eq!(
            plan.partitions
                .iter()
                .map(|partition| partition.file_count)
                .sum::<usize>(),
            file_set.files.len()
        );
        assert_eq!(
            plan.partitions
                .iter()
                .map(|partition| partition.rows)
                .sum::<u64>(),
            shape.total_rows
        );
        assert_eq!(
            plan.partitions
                .iter()
                .map(|partition| partition.size_bytes)
                .sum::<u64>(),
            shape.active_data_size_bytes
        );
        assert!(plan.wall_micros < work.serial_micros);
        assert!(plan.wall_micros >= work.max_file_micros);
        assert!(
            plan.partitions
                .iter()
                .enumerate()
                .all(|(index, partition)| partition.partition_index == index)
        );

        Ok(())
    }

    #[test]
    fn policy_case_derives_target_with_production_diagnostic_policy() -> Result<(), Box<dyn Error>>
    {
        let case = BenchmarkPolicyCase::with_input("test_policy", {
            DeltaScanPartitionTargetDiagnosticInput {
                available_parallelism: Some(64),
                datafusion_target_partitions: Some(16),
                ..DeltaScanPartitionTargetDiagnosticInput::default()
            }
        });
        let decision = case.derive_target()?;

        assert_eq!(decision.target_partitions, 16);
        assert_eq!(decision.available_parallelism, Some(64));
        assert_eq!(decision.datafusion_target_cap, Some(16));

        Ok(())
    }

    #[test]
    fn standard_policy_cases_cover_resource_cap_matrix() -> Result<(), Box<dyn Error>> {
        let cases = BenchmarkPolicyCase::standard_cases(Some(16));
        let names = cases
            .iter()
            .map(|case| case.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(cases.len(), 25);
        assert_eq!(names.first(), Some(&"default_policy"));
        assert!(names.contains(&"fd_per_partition_4"));
        assert!(names.contains(&"fd_per_partition_8"));
        assert!(names.contains(&"fd_per_partition_16"));
        assert!(names.contains(&"fd_per_partition_32"));
        assert!(names.contains(&"memory_per_partition_64mib"));
        assert!(names.contains(&"memory_per_partition_128mib"));
        assert!(names.contains(&"memory_per_partition_256mib"));
        assert!(names.contains(&"memory_per_partition_512mib"));
        assert!(names.contains(&"combined_fd_16_memory_256mib"));
        assert!(names.contains(&"combined_fd_32_memory_512mib"));
        assert!(
            cases
                .iter()
                .all(|case| case.input.available_parallelism == Some(16))
        );
        assert!(
            cases
                .iter()
                .all(|case| case.input.datafusion_target_partitions == Some(16))
        );

        let fd_per_partition_16 =
            find_policy_case(&cases, "fd_per_partition_16")?.derive_target()?;
        let fd_per_partition_32 =
            find_policy_case(&cases, "fd_per_partition_32")?.derive_target()?;
        let memory_per_partition_256mib =
            find_policy_case(&cases, "memory_per_partition_256mib")?.derive_target()?;
        let memory_per_partition_512mib =
            find_policy_case(&cases, "memory_per_partition_512mib")?.derive_target()?;
        let combined = find_policy_case(&cases, "combined_fd_32_memory_512mib")?.derive_target()?;

        assert_eq!(fd_per_partition_16.target_partitions, 8);
        assert_eq!(fd_per_partition_16.unix_file_descriptor_cap, Some(8));
        assert_eq!(fd_per_partition_32.target_partitions, 4);
        assert_eq!(fd_per_partition_32.unix_file_descriptor_cap, Some(4));
        assert_eq!(memory_per_partition_256mib.target_partitions, 4);
        assert_eq!(memory_per_partition_256mib.memory_cap, Some(4));
        assert_eq!(memory_per_partition_512mib.target_partitions, 2);
        assert_eq!(memory_per_partition_512mib.memory_cap, Some(2));
        assert_eq!(combined.target_partitions, 2);
        assert_eq!(combined.unix_file_descriptor_cap, Some(4));
        assert_eq!(combined.memory_cap, Some(2));

        Ok(())
    }

    #[test]
    fn policy_target_drives_partitioned_work_plan() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let work = SyntheticWorkSimulationProfile::s3_normal().simulate_file_set(&file_set)?;
        let case = BenchmarkPolicyCase::with_input(
            "test_policy",
            DeltaScanPartitionTargetDiagnosticInput {
                available_parallelism: Some(64),
                datafusion_target_partitions: Some(8),
                ..DeltaScanPartitionTargetDiagnosticInput::default()
            },
        );
        let decision = case.derive_target()?;
        let plan = work.partition_by_estimated_bytes(&file_set, decision.target_partitions)?;

        assert_eq!(decision.target_partitions, 8);
        assert_eq!(plan.target_partitions, 8);
        assert!(plan.partitions.len() <= 8);
        assert!(plan.wall_micros < work.serial_micros);

        Ok(())
    }

    #[test]
    fn optional_usize_renders_csv_fields() {
        assert_eq!(optional_usize(None), "");
        assert_eq!(optional_usize(Some(8)), "8");
    }

    #[test]
    fn optional_u64_renders_csv_fields() {
        assert_eq!(optional_u64(None), "");
        assert_eq!(optional_u64(Some(1024)), "1024");
    }

    #[test]
    fn policy_source_name_renders_csv_fields() {
        assert_eq!(
            policy_source_name(DeltaScanPartitionTargetDiagnosticSource::ExplicitOverride),
            "explicit_override"
        );
        assert_eq!(
            policy_source_name(
                DeltaScanPartitionTargetDiagnosticSource::AvailableParallelismFallback
            ),
            "available_parallelism_fallback"
        );
        assert_eq!(
            policy_source_name(DeltaScanPartitionTargetDiagnosticSource::StaticFallback),
            "static_fallback"
        );
    }

    #[test]
    fn benchmark_csv_header_matches_policy_output_shape() {
        assert_eq!(BENCHMARK_CSV_HEADER.len(), 36);
        assert_eq!(BENCHMARK_CSV_HEADER[20], "policy_case");
        assert_eq!(BENCHMARK_CSV_HEADER[21], "policy_available_parallelism");
        assert_eq!(BENCHMARK_CSV_HEADER[22], "policy_datafusion_target");
        assert_eq!(BENCHMARK_CSV_HEADER[23], "policy_available_memory_bytes");
        assert_eq!(BENCHMARK_CSV_HEADER[24], "policy_unix_soft_fd_limit");
        assert_eq!(BENCHMARK_CSV_HEADER[27], "policy_target");
        assert_eq!(BENCHMARK_CSV_HEADER[28], "policy_source");
    }

    #[test]
    fn benchmark_csv_row_matches_header_width() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let simulation = SyntheticWorkSimulationProfile::s3_normal();
        let simulated_work = simulation.simulate_file_set(&file_set)?;
        let cases = BenchmarkPolicyCase::standard_cases(Some(16));
        let case = &cases[0];
        let decision = case.derive_target()?;
        let partitioned_work =
            simulated_work.partition_by_estimated_bytes(&file_set, decision.target_partitions)?;
        let row = benchmark_csv_row(BenchmarkCsvRowInput {
            shape: &shape,
            file_set: &file_set,
            simulation_profile_count: SyntheticWorkSimulationProfile::standard_profiles().len(),
            simulation,
            policy_case: case,
            policy_decision: decision,
            simulated_work: &simulated_work,
            partitioned_work: &partitioned_work,
        });

        assert_eq!(row.len(), BENCHMARK_CSV_HEADER.len());
        assert_eq!(row[20], "default_policy");
        assert_eq!(row[27], "16");
        assert_eq!(row[28], "available_parallelism_fallback");

        Ok(())
    }
}
