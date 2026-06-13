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
const BENCHMARK_AVAILABLE_PARALLELISM_CANDIDATES: [usize; 3] = [4, 16, 64];
const BENCHMARK_UNIX_SOFT_FD_LIMIT: u64 = 128;
const BENCHMARK_AVAILABLE_MEMORY_BYTES: u64 = 1024 * MIB;
const BENCHMARK_SCHEMA_VERSION: u32 = 5;
const DEFAULT_BENCHMARK_SEED: u64 = 0;
const BENCHMARK_CSV_HEADER: [&str; 61] = [
    "benchmark_schema_version",
    "host_os",
    "host_arch",
    "host_available_parallelism",
    "seed",
    "workload_case_count",
    "workload_case",
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
    "simulation_partition_scheduling_overhead_micros",
    "simulation_effective_parallelism",
    "simulation_aggregate_bandwidth_bytes_per_second",
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
    "unknown_size_fallback_used",
    "simulated_serial_micros",
    "simulated_max_file_micros",
    "simulated_output_partitions",
    "simulated_scheduling_overhead_micros",
    "simulated_aggregate_transfer_floor_micros",
    "simulated_execution_slots",
    "simulated_wall_micros",
    "simulated_throughput_mib_per_second",
    "simulated_rows_per_second",
    "partition_files_p50",
    "partition_files_p95",
    "partition_files_max",
    "partition_bytes_p50",
    "partition_bytes_p95",
    "partition_bytes_max",
    "partition_work_micros_p50",
    "partition_work_micros_p95",
    "partition_work_imbalance_basis_points",
];

fn main() -> Result<(), Box<dyn Error>> {
    let config = BenchmarkRunnerConfig::parse(env::args_os().skip(1))?;

    if config.show_help {
        print_usage(io::stdout())?;
        return Ok(());
    }

    if let Some(output_path) = config.output_path {
        let mut output = File::create(output_path)?;
        write_benchmark_csv(&mut output, config.seed)?;
    } else {
        let stdout = io::stdout();
        let mut output = stdout.lock();
        write_benchmark_csv(&mut output, config.seed)?;
    }

    Ok(())
}

fn write_benchmark_csv(output: &mut impl Write, seed: u64) -> Result<(), Box<dyn Error>> {
    let run_environment = BenchmarkRunEnvironment::local();
    let workload_cases = SyntheticWorkloadCase::standard_cases()?;
    let simulation_profiles = SyntheticWorkSimulationProfile::standard_profiles();
    let policy_cases = BenchmarkPolicyCase::standard_cases(run_environment.available_parallelism);

    writeln!(output, "{}", BENCHMARK_CSV_HEADER.join(","))?;
    for workload_case in &workload_cases {
        for simulation in simulation_profiles {
            let simulated_work = simulation.simulate_file_set(&workload_case.file_set, seed)?;

            for policy_case in &policy_cases {
                let policy_decision = policy_case.derive_target()?;
                let partitioned_work = simulated_work.partition_by_estimated_bytes(
                    &workload_case.file_set,
                    policy_decision.target_partitions,
                )?;

                writeln!(
                    output,
                    "{}",
                    benchmark_csv_row(BenchmarkCsvRowInput {
                        shape: &workload_case.shape,
                        file_set: &workload_case.file_set,
                        run_environment,
                        seed,
                        workload_case: workload_case.name,
                        workload_case_count: workload_cases.len(),
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
    }

    Ok(())
}

fn print_usage(mut output: impl Write) -> io::Result<()> {
    writeln!(
        output,
        "Usage: delta_scan_partition_bench [--output <path>] [--seed <u64>]"
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
    writeln!(output, "The default seed is {DEFAULT_BENCHMARK_SEED}.")?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchmarkRunnerConfig {
    output_path: Option<PathBuf>,
    seed: u64,
    show_help: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BenchmarkRunEnvironment {
    schema_version: u32,
    host_os: &'static str,
    host_arch: &'static str,
    available_parallelism: Option<usize>,
}

impl BenchmarkRunnerConfig {
    fn parse<I>(args: I) -> Result<Self, BenchmarkRunnerConfigError>
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        let mut output_path = None;
        let mut seed = DEFAULT_BENCHMARK_SEED;
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
            } else if arg == "--seed" {
                let value = args.next().ok_or(BenchmarkRunnerConfigError::MissingSeed)?;
                seed = value.to_string_lossy().parse().map_err(|_| {
                    BenchmarkRunnerConfigError::InvalidSeed(value.to_string_lossy().into())
                })?;
            } else {
                return Err(BenchmarkRunnerConfigError::UnknownArgument(
                    arg.to_string_lossy().into(),
                ));
            }
        }

        Ok(Self {
            output_path,
            seed,
            show_help,
        })
    }
}

impl BenchmarkRunEnvironment {
    fn local() -> Self {
        Self {
            schema_version: BENCHMARK_SCHEMA_VERSION,
            host_os: env::consts::OS,
            host_arch: env::consts::ARCH,
            available_parallelism: local_available_parallelism(),
        }
    }
}

impl SyntheticWorkloadCase {
    fn standard_cases() -> Result<Vec<Self>, SyntheticGenerationError> {
        Ok(vec![
            Self::partitioned_event_log_target_shape()?,
            Self::many_tiny_files()?,
            Self::mixed_tiny_large_files()?,
            Self::highly_skewed_files()?,
            Self::unknown_size_files()?,
            Self::zero_byte_files()?,
        ])
    }

    #[cfg(test)]
    fn edge_cases() -> Result<Vec<Self>, SyntheticGenerationError> {
        Ok(vec![
            Self::explicit_files("empty_scan", 0, 0, &[])?,
            Self::explicit_files("one_file", 25_000, 16 * MIB, &[(25_000, 16 * MIB)])?,
            Self::explicit_files(
                "few_medium_files",
                800_000,
                512 * MIB,
                &[(100_000, 64 * MIB); 8],
            )?,
        ])
    }

    fn partitioned_event_log_target_shape() -> Result<Self, SyntheticGenerationError> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;

        Ok(Self {
            name: "partitioned_event_log_target_shape",
            shape,
            file_set,
        })
    }

    fn many_tiny_files() -> Result<Self, SyntheticGenerationError> {
        const FILE_COUNT: usize = 4_096;
        const ROWS_PER_FILE: u64 = 1_000;
        const BYTES_PER_FILE: u64 = 64 * 1024;

        Self::explicit_files(
            "many_tiny_files",
            FILE_COUNT as u64 * ROWS_PER_FILE,
            FILE_COUNT as u64 * BYTES_PER_FILE,
            &[(ROWS_PER_FILE, BYTES_PER_FILE); FILE_COUNT],
        )
    }

    fn mixed_tiny_large_files() -> Result<Self, SyntheticGenerationError> {
        const TINY_FILE_COUNT: usize = 1_024;
        const TINY_ROWS_PER_FILE: u64 = 1_000;
        const TINY_BYTES_PER_FILE: u64 = 64 * 1024;
        const LARGE_FILE_COUNT: usize = 16;
        const LARGE_ROWS_PER_FILE: u64 = 1_000_000;
        const LARGE_BYTES_PER_FILE: u64 = 128 * MIB;

        let mut files = Vec::with_capacity(TINY_FILE_COUNT + LARGE_FILE_COUNT);
        files.extend([(TINY_ROWS_PER_FILE, TINY_BYTES_PER_FILE); TINY_FILE_COUNT]);
        files.extend([(LARGE_ROWS_PER_FILE, LARGE_BYTES_PER_FILE); LARGE_FILE_COUNT]);

        Self::explicit_files(
            "mixed_tiny_large_files",
            TINY_FILE_COUNT as u64 * TINY_ROWS_PER_FILE
                + LARGE_FILE_COUNT as u64 * LARGE_ROWS_PER_FILE,
            TINY_FILE_COUNT as u64 * TINY_BYTES_PER_FILE
                + LARGE_FILE_COUNT as u64 * LARGE_BYTES_PER_FILE,
            &files,
        )
    }

    fn highly_skewed_files() -> Result<Self, SyntheticGenerationError> {
        const SMALL_FILE_COUNT: usize = 255;
        const SMALL_ROWS_PER_FILE: u64 = 10_000;
        const SMALL_BYTES_PER_FILE: u64 = MIB;
        const HUGE_ROWS: u64 = 16_000_000;
        const HUGE_BYTES: u64 = 2 * 1024 * MIB;

        let mut files = Vec::with_capacity(SMALL_FILE_COUNT + 1);
        files.push((HUGE_ROWS, HUGE_BYTES));
        files.extend([(SMALL_ROWS_PER_FILE, SMALL_BYTES_PER_FILE); SMALL_FILE_COUNT]);

        Self::explicit_files(
            "highly_skewed_files",
            HUGE_ROWS + SMALL_FILE_COUNT as u64 * SMALL_ROWS_PER_FILE,
            HUGE_BYTES + SMALL_FILE_COUNT as u64 * SMALL_BYTES_PER_FILE,
            &files,
        )
    }

    fn unknown_size_files() -> Result<Self, SyntheticGenerationError> {
        const FILE_COUNT: usize = 1_024;
        const ROWS_PER_FILE: u64 = 4_000;
        const BYTES_PER_FILE: u64 = 512 * 1024;

        let mut workload = Self::explicit_files(
            "unknown_size_files",
            FILE_COUNT as u64 * ROWS_PER_FILE,
            FILE_COUNT as u64 * BYTES_PER_FILE,
            &[(ROWS_PER_FILE, BYTES_PER_FILE); FILE_COUNT],
        )?;
        for file in &mut workload.file_set.files {
            file.estimated_size_bytes = None;
        }
        Ok(workload)
    }

    fn zero_byte_files() -> Result<Self, SyntheticGenerationError> {
        const FILE_COUNT: usize = 512;

        Self::explicit_files("zero_byte_files", 0, 0, &[(0, 0); FILE_COUNT])
    }

    fn explicit_files(
        name: &'static str,
        total_rows: u64,
        total_bytes: u64,
        files: &[(u64, u64)],
    ) -> Result<Self, SyntheticGenerationError> {
        let file_set = SyntheticFileSet::from_explicit_files(name, files)?;
        if file_set.total_rows() != total_rows {
            return Err(generation_error(format!(
                "workload `{name}` file rows do not match total rows"
            )));
        }
        if file_set.total_bytes() != total_bytes {
            return Err(generation_error(format!(
                "workload `{name}` file bytes do not match total bytes"
            )));
        }

        let shape =
            SyntheticDeltaTableShape::synthetic_workload(name, total_rows, total_bytes, &file_set);

        Ok(Self {
            name,
            shape,
            file_set,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BenchmarkRunnerConfigError {
    MissingOutputPath,
    DuplicateOutputPath,
    MissingSeed,
    InvalidSeed(String),
    UnknownArgument(String),
}

impl fmt::Display for BenchmarkRunnerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingOutputPath => write!(formatter, "--output requires a path"),
            Self::DuplicateOutputPath => write!(formatter, "--output may be provided only once"),
            Self::MissingSeed => write!(formatter, "--seed requires a u64 value"),
            Self::InvalidSeed(value) => write!(formatter, "invalid --seed value `{value}`"),
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
struct SyntheticWorkloadCase {
    name: &'static str,
    shape: SyntheticDeltaTableShape,
    file_set: SyntheticFileSet,
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
    estimated_size_bytes: Option<u64>,
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
    run_environment: BenchmarkRunEnvironment,
    seed: u64,
    workload_case: &'static str,
    workload_case_count: usize,
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
    partition_scheduling_overhead_micros: u64,
    effective_parallelism: usize,
    bandwidth_bytes_per_second: u64,
    aggregate_bandwidth_bytes_per_second: u64,
    cpu_micros_per_1k_rows: u64,
    jitter_basis_points: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyntheticWorkSimulationResult {
    profile_name: &'static str,
    partition_scheduling_overhead_micros: u64,
    effective_parallelism: usize,
    aggregate_bandwidth_bytes_per_second: u64,
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
    unknown_size_fallback_used: bool,
    partitions: Vec<SyntheticWorkPartition>,
    scheduling_overhead_micros: u64,
    aggregate_transfer_floor_micros: u64,
    execution_slots: usize,
    wall_micros: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyntheticWorkPartition {
    partition_index: usize,
    file_count: usize,
    rows: u64,
    estimated_size_bytes: Option<u64>,
    size_bytes: u64,
    work_micros: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyntheticPartitionedWorkSummary {
    files_p50: usize,
    files_p95: usize,
    files_max: usize,
    bytes_p50: u64,
    bytes_p95: u64,
    bytes_max: u64,
    work_micros_p50: u64,
    work_micros_p95: u64,
    work_imbalance_basis_points: u64,
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
    fn synthetic_workload(
        name: &'static str,
        total_rows: u64,
        total_bytes: u64,
        file_set: &SyntheticFileSet,
    ) -> Self {
        let partition_count = file_set.partitions.len();
        let active_file_count = file_set.files.len();
        let average_file_size = average_or_zero(total_bytes, active_file_count);
        let average_rows_per_file = average_or_zero(total_rows, active_file_count);

        Self {
            name,
            total_rows,
            active_file_count,
            active_data_size_bytes: total_bytes,
            file_size_bytes: DistributionSummary {
                average: average_file_size,
                p50: average_file_size,
                p90: average_file_size,
                p99: average_file_size,
            },
            rows_per_file: DistributionSummary {
                average: average_rows_per_file,
                p50: average_rows_per_file,
                p90: average_rows_per_file,
                p99: average_rows_per_file,
            },
            partitioning: PartitioningShape {
                columns: ["event_year", "event_month", "event_day"],
                partition_count,
                start_date: SyntheticDate {
                    year: 2026,
                    month: 1,
                    day: 1,
                },
                end_date: SyntheticDate {
                    year: 2026,
                    month: 1,
                    day: 1,
                },
                average_files_per_partition_hundredths: average_files_per_partition_hundredths(
                    active_file_count,
                    partition_count,
                ),
                max_files_per_partition: file_set.max_files_per_partition(),
            },
            delta_features: DeltaFeatureShape {
                min_reader_version: 3,
                min_writer_version: 7,
                compression: "zstd",
                table_features: Vec::new(),
                deletion_vectors_enabled_in_source_shape: false,
                active_deletion_vectors_in_benchmark: 0,
            },
            schema: SchemaShape {
                all_columns_nullable: true,
                columns: synthetic_columns(),
            },
            row_distribution: RowDistributionShape {
                source_split: vec![CategoryRowCount {
                    category: "synthetic",
                    rows: total_rows,
                }],
                uniform_category: UniformCategoryShape {
                    column_name: "category_num",
                    category_count: 1,
                    min_rows_per_category: total_rows,
                    max_rows_per_category: total_rows,
                },
                seasonality: vec![CategoryRowCount {
                    category: "2026",
                    rows: total_rows,
                }],
            },
            null_patterns: Vec::new(),
            cardinalities: Vec::new(),
        }
    }

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
                    estimated_size_bytes: Some(file_bytes[file_index]),
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
        average_or_zero(self.active_data_size_bytes, self.active_file_count)
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
    fn from_explicit_files(
        workload_name: &str,
        file_specs: &[(u64, u64)],
    ) -> Result<Self, SyntheticGenerationError> {
        let mut partitions = Vec::with_capacity(file_specs.len());
        let mut files = Vec::with_capacity(file_specs.len());

        for (index, (rows, size_bytes)) in file_specs.iter().copied().enumerate() {
            let day = u8::try_from(index % 28 + 1)
                .map_err(|_| generation_error("synthetic day does not fit into u8"))?;
            let date = SyntheticDate {
                year: 2026,
                month: 1,
                day,
            };

            partitions.push(SyntheticPartition {
                date,
                rows,
                size_bytes,
                file_count: 1,
            });
            files.push(SyntheticFile {
                path: format!("{workload_name}/part-{index:05}.parquet"),
                partition_date: date,
                file_index_in_partition: 0,
                rows,
                estimated_size_bytes: Some(size_bytes),
                size_bytes,
            });
        }

        Ok(Self { partitions, files })
    }

    fn total_rows(&self) -> u64 {
        self.files.iter().map(|file| file.rows).sum()
    }

    fn total_bytes(&self) -> u64 {
        self.files.iter().map(|file| file.size_bytes).sum()
    }

    fn total_estimated_size_bytes(&self) -> Result<Option<u64>, SyntheticGenerationError> {
        let mut total = 0_u64;
        for file in &self.files {
            let Some(estimated_size_bytes) = file.estimated_size_bytes else {
                return Ok(None);
            };
            total = total
                .checked_add(estimated_size_bytes)
                .ok_or_else(|| generation_error("estimated file size overflow"))?;
        }

        Ok(Some(total))
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
        cases.extend(Self::strategy_baseline_cases(baseline));
        cases.extend(Self::available_parallelism_override_cases(baseline));

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

    fn strategy_baseline_cases(baseline: DeltaScanPartitionTargetDiagnosticInput) -> [Self; 9] {
        [
            Self::new(
                "fixed_target_1",
                DeltaScanPartitionTargetDiagnosticInput {
                    explicit_target_partitions: Some(1),
                    ..baseline
                },
            ),
            Self::new(
                "fixed_target_4",
                DeltaScanPartitionTargetDiagnosticInput {
                    explicit_target_partitions: Some(4),
                    ..baseline
                },
            ),
            Self::new(
                "fixed_target_8",
                DeltaScanPartitionTargetDiagnosticInput {
                    explicit_target_partitions: Some(8),
                    ..baseline
                },
            ),
            Self::new(
                "fixed_target_16",
                DeltaScanPartitionTargetDiagnosticInput {
                    explicit_target_partitions: Some(16),
                    ..baseline
                },
            ),
            Self::new(
                "fixed_target_32",
                DeltaScanPartitionTargetDiagnosticInput {
                    explicit_target_partitions: Some(32),
                    ..baseline
                },
            ),
            Self::new(
                "fixed_target_64",
                DeltaScanPartitionTargetDiagnosticInput {
                    explicit_target_partitions: Some(64),
                    ..baseline
                },
            ),
            Self::new(
                "available_parallelism_uncapped",
                DeltaScanPartitionTargetDiagnosticInput {
                    datafusion_target_partitions: None,
                    ..baseline
                },
            ),
            Self::new(
                "available_parallelism_x2_uncapped",
                DeltaScanPartitionTargetDiagnosticInput {
                    datafusion_target_partitions: None,
                    parallelism_multiplier: 2,
                    ..baseline
                },
            ),
            Self::new(
                "datafusion_cap_4",
                DeltaScanPartitionTargetDiagnosticInput {
                    datafusion_target_partitions: Some(4),
                    ..baseline
                },
            ),
        ]
    }

    fn available_parallelism_override_cases(
        baseline: DeltaScanPartitionTargetDiagnosticInput,
    ) -> impl Iterator<Item = Self> {
        BENCHMARK_AVAILABLE_PARALLELISM_CANDIDATES
            .into_iter()
            .map(move |available_parallelism| {
                Self::new(
                    format!("available_parallelism_override_{available_parallelism}"),
                    DeltaScanPartitionTargetDiagnosticInput {
                        available_parallelism: Some(available_parallelism),
                        datafusion_target_partitions: Some(available_parallelism),
                        ..baseline
                    },
                )
            })
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
            partition_scheduling_overhead_micros: 150,
            effective_parallelism: 16,
            bandwidth_bytes_per_second: 1_500 * MIB,
            aggregate_bandwidth_bytes_per_second: 1_500 * MIB,
            cpu_micros_per_1k_rows: 8,
            jitter_basis_points: 250,
        }
    }

    fn s3_normal() -> Self {
        Self {
            name: "s3_normal",
            open_latency_micros: 8_000,
            read_latency_micros: 4_000,
            partition_scheduling_overhead_micros: 1_000,
            effective_parallelism: 32,
            bandwidth_bytes_per_second: 125 * MIB,
            aggregate_bandwidth_bytes_per_second: 125 * MIB,
            cpu_micros_per_1k_rows: 10,
            jitter_basis_points: 1_500,
        }
    }

    fn s3_high_latency() -> Self {
        Self {
            name: "s3_high_latency",
            open_latency_micros: 35_000,
            read_latency_micros: 20_000,
            partition_scheduling_overhead_micros: 1_500,
            effective_parallelism: 64,
            bandwidth_bytes_per_second: 100 * MIB,
            aggregate_bandwidth_bytes_per_second: 100 * MIB,
            cpu_micros_per_1k_rows: 10,
            jitter_basis_points: 2_500,
        }
    }

    fn s3_throttled() -> Self {
        Self {
            name: "s3_throttled",
            open_latency_micros: 15_000,
            read_latency_micros: 8_000,
            partition_scheduling_overhead_micros: 2_000,
            effective_parallelism: 16,
            bandwidth_bytes_per_second: 32 * MIB,
            aggregate_bandwidth_bytes_per_second: 12_500_000,
            cpu_micros_per_1k_rows: 10,
            jitter_basis_points: 2_000,
        }
    }

    fn cpu_heavy() -> Self {
        Self {
            name: "cpu_heavy",
            open_latency_micros: 1_000,
            read_latency_micros: 500,
            partition_scheduling_overhead_micros: 500,
            effective_parallelism: 16,
            bandwidth_bytes_per_second: 500 * MIB,
            aggregate_bandwidth_bytes_per_second: 500 * MIB,
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
        seed: u64,
    ) -> Result<SyntheticWorkSimulationResult, SyntheticGenerationError> {
        let mut serial_micros = 0_u64;
        let mut max_file_micros = 0_u64;
        let mut file_costs = Vec::with_capacity(file_set.files.len());

        for (file_index, file) in file_set.files.iter().enumerate() {
            let cost = self.simulate_file(file_index, file, seed)?;
            serial_micros = serial_micros
                .checked_add(cost.total_micros)
                .ok_or_else(|| generation_error("simulated serial time overflow"))?;
            max_file_micros = max_file_micros.max(cost.total_micros);
            file_costs.push(cost);
        }

        Ok(SyntheticWorkSimulationResult {
            profile_name: self.name,
            partition_scheduling_overhead_micros: self.partition_scheduling_overhead_micros,
            effective_parallelism: self.effective_parallelism,
            aggregate_bandwidth_bytes_per_second: self.aggregate_bandwidth_bytes_per_second,
            file_costs,
            serial_micros,
            max_file_micros,
        })
    }

    fn simulate_file(
        self,
        file_index: usize,
        file: &SyntheticFile,
        seed: u64,
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
                seed,
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
                unknown_size_fallback_used: false,
                partitions: Vec::new(),
                scheduling_overhead_micros: 0,
                aggregate_transfer_floor_micros: 0,
                execution_slots: 0,
                wall_micros: 0,
            });
        }

        let output_limit = target_partitions.min(file_set.files.len());
        let Some(total_estimated_size_bytes) = file_set.total_estimated_size_bytes()? else {
            return self.partition_by_file_count(
                file_set,
                target_partitions,
                output_limit,
                false,
                true,
            );
        };
        if total_estimated_size_bytes == 0 {
            return self.partition_by_file_count(
                file_set,
                target_partitions,
                output_limit,
                true,
                false,
            );
        }
        let target_bytes = total_estimated_size_bytes.div_ceil(output_limit as u64);
        let mut partitions = Vec::new();
        let mut current = SyntheticWorkPartitionBuilder::default();

        for (file, cost) in file_set.files.iter().zip(&self.file_costs) {
            let Some(estimated_size_bytes) = file.estimated_size_bytes else {
                return Err(generation_error(
                    "known-size grouping requires every file to have estimated bytes",
                ));
            };
            let can_start_next_partition = current.file_count > 0
                && partitions.len() + 1 < output_limit
                && current
                    .estimated_size_bytes
                    .saturating_add(estimated_size_bytes)
                    > target_bytes;

            if can_start_next_partition {
                partitions.push(current.finish(partitions.len(), true));
                current = SyntheticWorkPartitionBuilder::default();
            }

            current.add(file, cost, Some(estimated_size_bytes))?;
        }

        if current.file_count > 0 {
            partitions.push(current.finish(partitions.len(), true));
        }

        let scheduling_overhead_micros = self.scheduling_overhead_micros(partitions.len())?;
        let aggregate_transfer_floor_micros = self.aggregate_transfer_floor_micros(&partitions)?;
        let execution_slots = self.execution_slots(partitions.len())?;
        let wall_micros = partition_wall_micros(
            &partitions,
            scheduling_overhead_micros,
            aggregate_transfer_floor_micros,
            execution_slots,
        )?;

        Ok(SyntheticPartitionedWorkPlan {
            target_partitions,
            unknown_size_fallback_used: false,
            partitions,
            scheduling_overhead_micros,
            aggregate_transfer_floor_micros,
            execution_slots,
            wall_micros,
        })
    }

    fn partition_by_file_count(
        &self,
        file_set: &SyntheticFileSet,
        target_partitions: usize,
        output_limit: usize,
        known_estimated_sizes: bool,
        unknown_size_fallback_used: bool,
    ) -> Result<SyntheticPartitionedWorkPlan, SyntheticGenerationError> {
        let mut partitions = Vec::new();
        let mut file_costs = file_set.files.iter().zip(&self.file_costs);
        let mut remaining_files = file_set.files.len();

        for partition_index in 0..output_limit {
            let remaining_partitions = output_limit - partition_index;
            let take_count = remaining_files.div_ceil(remaining_partitions);
            let mut current = SyntheticWorkPartitionBuilder::default();

            for _ in 0..take_count {
                let Some((file, cost)) = file_costs.next() else {
                    return Err(generation_error(
                        "file-count grouping exhausted files unexpectedly",
                    ));
                };
                current.add(
                    file,
                    cost,
                    known_estimated_sizes
                        .then_some(file.estimated_size_bytes)
                        .flatten(),
                )?;
            }

            remaining_files -= take_count;
            partitions.push(current.finish(partitions.len(), known_estimated_sizes));
        }

        let scheduling_overhead_micros = self.scheduling_overhead_micros(partitions.len())?;
        let aggregate_transfer_floor_micros = self.aggregate_transfer_floor_micros(&partitions)?;
        let execution_slots = self.execution_slots(partitions.len())?;
        let wall_micros = partition_wall_micros(
            &partitions,
            scheduling_overhead_micros,
            aggregate_transfer_floor_micros,
            execution_slots,
        )?;

        Ok(SyntheticPartitionedWorkPlan {
            target_partitions,
            unknown_size_fallback_used,
            partitions,
            scheduling_overhead_micros,
            aggregate_transfer_floor_micros,
            execution_slots,
            wall_micros,
        })
    }

    fn scheduling_overhead_micros(
        &self,
        output_partitions: usize,
    ) -> Result<u64, SyntheticGenerationError> {
        self.partition_scheduling_overhead_micros
            .checked_mul(output_partitions as u64)
            .ok_or_else(|| generation_error("partition scheduling overhead overflow"))
    }

    fn execution_slots(&self, output_partitions: usize) -> Result<usize, SyntheticGenerationError> {
        if self.effective_parallelism == 0 {
            return Err(generation_error(
                "simulation effective parallelism must be greater than zero",
            ));
        }

        Ok(output_partitions.min(self.effective_parallelism))
    }

    fn aggregate_transfer_floor_micros(
        &self,
        partitions: &[SyntheticWorkPartition],
    ) -> Result<u64, SyntheticGenerationError> {
        if self.aggregate_bandwidth_bytes_per_second == 0 {
            return Err(generation_error(
                "simulation aggregate bandwidth must be greater than zero",
            ));
        }

        let total_bytes = partitions.iter().try_fold(0_u64, |total, partition| {
            total
                .checked_add(partition.size_bytes)
                .ok_or_else(|| generation_error("aggregate transfer byte count overflow"))
        })?;

        scaled_ceil_div(
            total_bytes,
            1_000_000,
            self.aggregate_bandwidth_bytes_per_second,
            "aggregate transfer floor",
        )
    }
}

fn partition_wall_micros(
    partitions: &[SyntheticWorkPartition],
    scheduling_overhead_micros: u64,
    aggregate_transfer_floor_micros: u64,
    execution_slots: usize,
) -> Result<u64, SyntheticGenerationError> {
    if partitions.is_empty() {
        return Ok(scheduling_overhead_micros);
    }
    if execution_slots == 0 {
        return Err(generation_error(
            "partition wall time requires at least one execution slot",
        ));
    }

    let mut slot_load_micros = vec![0_u64; execution_slots];
    for partition in partitions {
        let Some((_, slot_load_micros)) = slot_load_micros
            .iter_mut()
            .enumerate()
            .min_by_key(|(index, slot_load_micros)| (**slot_load_micros, *index))
        else {
            return Err(generation_error("partition wall time lost execution slots"));
        };
        *slot_load_micros = slot_load_micros
            .checked_add(partition.work_micros)
            .ok_or_else(|| generation_error("partition slot work time overflow"))?;
    }
    let scheduled_work_micros = slot_load_micros.into_iter().max().unwrap_or_default();

    scheduled_work_micros
        .max(aggregate_transfer_floor_micros)
        .checked_add(scheduling_overhead_micros)
        .ok_or_else(|| generation_error("partition wall time overflow"))
}

impl SyntheticPartitionedWorkPlan {
    fn summary(&self) -> SyntheticPartitionedWorkSummary {
        let files = self
            .partitions
            .iter()
            .map(|partition| partition.file_count as u64)
            .collect::<Vec<_>>();
        let bytes = self
            .partitions
            .iter()
            .map(|partition| partition.size_bytes)
            .collect::<Vec<_>>();
        let work = self
            .partitions
            .iter()
            .map(|partition| partition.work_micros)
            .collect::<Vec<_>>();
        let average_work = average_or_zero(work.iter().sum(), work.len());
        let max_work = work.iter().copied().max().unwrap_or_default();

        SyntheticPartitionedWorkSummary {
            files_p50: usize::try_from(percentile_nearest_rank(files.clone(), 50))
                .unwrap_or(usize::MAX),
            files_p95: usize::try_from(percentile_nearest_rank(files.clone(), 95))
                .unwrap_or(usize::MAX),
            files_max: usize::try_from(files.iter().copied().max().unwrap_or_default())
                .unwrap_or(usize::MAX),
            bytes_p50: percentile_nearest_rank(bytes.clone(), 50),
            bytes_p95: percentile_nearest_rank(bytes.clone(), 95),
            bytes_max: bytes.iter().copied().max().unwrap_or_default(),
            work_micros_p50: percentile_nearest_rank(work.clone(), 50),
            work_micros_p95: percentile_nearest_rank(work.clone(), 95),
            work_imbalance_basis_points: if average_work == 0 {
                0
            } else {
                max_work.saturating_mul(10_000) / average_work
            },
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SyntheticWorkPartitionBuilder {
    file_count: usize,
    rows: u64,
    estimated_size_bytes: u64,
    size_bytes: u64,
    work_micros: u64,
}

impl SyntheticWorkPartitionBuilder {
    fn add(
        &mut self,
        file: &SyntheticFile,
        cost: &SyntheticFileWorkCost,
        estimated_size_bytes: Option<u64>,
    ) -> Result<(), SyntheticGenerationError> {
        self.file_count = self
            .file_count
            .checked_add(1)
            .ok_or_else(|| generation_error("partition file count overflow"))?;
        self.rows = self
            .rows
            .checked_add(file.rows)
            .ok_or_else(|| generation_error("partition row count overflow"))?;
        if let Some(estimated_size_bytes) = estimated_size_bytes {
            self.estimated_size_bytes = self
                .estimated_size_bytes
                .checked_add(estimated_size_bytes)
                .ok_or_else(|| generation_error("partition estimated byte count overflow"))?;
        }
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

    fn finish(self, partition_index: usize, estimated_size_known: bool) -> SyntheticWorkPartition {
        SyntheticWorkPartition {
            partition_index,
            file_count: self.file_count,
            rows: self.rows,
            estimated_size_bytes: estimated_size_known.then_some(self.estimated_size_bytes),
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

fn average_or_zero(total: u64, count: usize) -> u64 {
    if count == 0 { 0 } else { total / count as u64 }
}

fn average_files_per_partition_hundredths(file_count: usize, partition_count: usize) -> u16 {
    if partition_count == 0 {
        return 0;
    }

    let hundredths = file_count.saturating_mul(100) / partition_count;
    u16::try_from(hundredths).unwrap_or(u16::MAX)
}

fn percentile_nearest_rank(mut values: Vec<u64>, percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }

    values.sort_unstable();
    let bounded_percentile = percentile.clamp(1, 100);
    let rank = (bounded_percentile * values.len()).div_ceil(100);
    values[rank.saturating_sub(1)]
}

fn throughput_per_second(units: u64, elapsed_micros: u64) -> u64 {
    if units == 0 || elapsed_micros == 0 {
        return 0;
    }

    units.saturating_mul(1_000_000) / elapsed_micros
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

fn deterministic_jitter_basis_points(
    file: &SyntheticFile,
    seed: u64,
    max_basis_points: u16,
) -> u16 {
    if max_basis_points == 0 {
        return 0;
    }

    let hash = deterministic_file_hash(file, seed);
    let range = u64::from(max_basis_points) + 1;

    (hash % range) as u16
}

fn deterministic_file_hash(file: &SyntheticFile, seed: u64) -> u64 {
    let mut hash = 14_695_981_039_346_656_037_u64;

    if seed != DEFAULT_BENCHMARK_SEED {
        for byte in seed.to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(1_099_511_628_211);
        }
    }
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
    let partitioned_work_summary = input.partitioned_work.summary();

    vec![
        input.run_environment.schema_version.to_string(),
        input.run_environment.host_os.to_owned(),
        input.run_environment.host_arch.to_owned(),
        optional_usize(input.run_environment.available_parallelism),
        input.seed.to_string(),
        input.workload_case_count.to_string(),
        input.workload_case.to_owned(),
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
        input
            .simulation
            .partition_scheduling_overhead_micros
            .to_string(),
        input.simulation.effective_parallelism.to_string(),
        input
            .simulation
            .aggregate_bandwidth_bytes_per_second
            .to_string(),
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
        input
            .partitioned_work
            .unknown_size_fallback_used
            .to_string(),
        input.simulated_work.serial_micros.to_string(),
        input.simulated_work.max_file_micros.to_string(),
        input.partitioned_work.partitions.len().to_string(),
        input
            .partitioned_work
            .scheduling_overhead_micros
            .to_string(),
        input
            .partitioned_work
            .aggregate_transfer_floor_micros
            .to_string(),
        input.partitioned_work.execution_slots.to_string(),
        input.partitioned_work.wall_micros.to_string(),
        throughput_per_second(
            file_set.total_bytes() / MIB,
            input.partitioned_work.wall_micros,
        )
        .to_string(),
        throughput_per_second(file_set.total_rows(), input.partitioned_work.wall_micros)
            .to_string(),
        partitioned_work_summary.files_p50.to_string(),
        partitioned_work_summary.files_p95.to_string(),
        partitioned_work_summary.files_max.to_string(),
        partitioned_work_summary.bytes_p50.to_string(),
        partitioned_work_summary.bytes_p95.to_string(),
        partitioned_work_summary.bytes_max.to_string(),
        partitioned_work_summary.work_micros_p50.to_string(),
        partitioned_work_summary.work_micros_p95.to_string(),
        partitioned_work_summary
            .work_imbalance_basis_points
            .to_string(),
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
                seed: DEFAULT_BENCHMARK_SEED,
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
    fn runner_config_accepts_seed() -> Result<(), Box<dyn Error>> {
        let config = BenchmarkRunnerConfig::parse(["--seed", "42"])?;

        assert_eq!(config.seed, 42);

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
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--seed"]),
            Err(BenchmarkRunnerConfigError::MissingSeed)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--seed", "not-a-number"]),
            Err(BenchmarkRunnerConfigError::InvalidSeed(
                "not-a-number".to_owned()
            ))
        );
    }

    #[test]
    fn print_usage_describes_output_path() -> Result<(), Box<dyn Error>> {
        let mut output = Vec::new();

        print_usage(&mut output)?;
        let usage = String::from_utf8(output)?;

        assert!(
            usage.contains("Usage: delta_scan_partition_bench [--output <path>] [--seed <u64>]")
        );
        assert!(usage.contains("CSV is written to stdout"));
        assert!(usage.contains("The default seed is 0."));

        Ok(())
    }

    #[test]
    fn write_benchmark_csv_outputs_portable_matrix() -> Result<(), Box<dyn Error>> {
        let mut output = Vec::new();

        write_benchmark_csv(&mut output, 42)?;
        let csv = String::from_utf8(output)?;
        let lines = csv.lines().collect::<Vec<_>>();

        assert_eq!(lines.len(), 1111);
        assert!(lines[0].starts_with("benchmark_schema_version,host_os,host_arch"));
        assert!(csv.contains(",partitioned_event_log_target_shape,"));
        assert!(csv.contains(",many_tiny_files,"));
        assert!(csv.contains(",mixed_tiny_large_files,"));
        assert!(csv.contains(",highly_skewed_files,"));
        assert!(csv.contains(",unknown_size_files,"));
        assert!(csv.contains(",zero_byte_files,"));
        assert!(!csv.contains(",empty_scan,"));
        assert!(!csv.contains(",one_file,"));
        assert!(!csv.contains(",few_medium_files,"));
        assert!(csv.contains(",local_fast,150,16,1572864000,default_policy,"));
        assert!(csv.contains(",s3_normal,1000,32,131072000,available_parallelism_override_64,"));
        assert!(csv.contains(",s3_normal,1000,32,131072000,combined_fd_16_memory_256mib,"));

        Ok(())
    }

    #[test]
    fn benchmark_run_environment_records_local_host_metadata() {
        let environment = BenchmarkRunEnvironment::local();

        assert_eq!(environment.schema_version, BENCHMARK_SCHEMA_VERSION);
        assert_eq!(environment.host_os, env::consts::OS);
        assert_eq!(environment.host_arch, env::consts::ARCH);
    }

    #[test]
    fn workload_case_wraps_current_target_shape() -> Result<(), Box<dyn Error>> {
        let workload_case = SyntheticWorkloadCase::partitioned_event_log_target_shape()?;

        assert_eq!(workload_case.name, "partitioned_event_log_target_shape");
        assert_eq!(workload_case.shape.name, "synthetic_partitioned_event_log");
        assert_eq!(workload_case.shape.active_file_count, 956);

        Ok(())
    }

    #[test]
    fn standard_workload_cases_cover_basic_file_shapes() -> Result<(), Box<dyn Error>> {
        let workload_cases = SyntheticWorkloadCase::standard_cases()?;
        let names = workload_cases
            .iter()
            .map(|workload_case| workload_case.name)
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            [
                "partitioned_event_log_target_shape",
                "many_tiny_files",
                "mixed_tiny_large_files",
                "highly_skewed_files",
                "unknown_size_files",
                "zero_byte_files"
            ]
        );
        assert_eq!(workload_cases[1].file_set.files.len(), 4_096);
        assert!(workload_cases[1].file_set.files.len() > workload_cases[0].file_set.files.len());
        assert_eq!(workload_cases[2].file_set.files.len(), 1_040);
        assert!(workload_cases[2].shape.active_data_size_bytes > 2 * 1024 * MIB);
        assert!(workload_cases[2].shape.average_file_size_bytes() > MIB);
        assert_eq!(workload_cases[3].file_set.files.len(), 256);
        assert_eq!(
            workload_cases[3].file_set.files[0].size_bytes,
            2 * 1024 * MIB
        );
        assert!(
            workload_cases[3].file_set.files[0].size_bytes
                > workload_cases[3].shape.average_file_size_bytes()
        );
        assert_eq!(workload_cases[4].file_set.files.len(), 1_024);
        assert!(
            workload_cases[4]
                .file_set
                .files
                .iter()
                .all(|file| file.estimated_size_bytes.is_none())
        );
        assert_eq!(workload_cases[5].file_set.files.len(), 512);
        assert!(
            workload_cases[5]
                .file_set
                .files
                .iter()
                .all(|file| file.size_bytes == 0 && file.estimated_size_bytes == Some(0))
        );
        assert!(
            workload_cases
                .iter()
                .all(|workload_case| workload_case.file_set.total_rows()
                    == workload_case.shape.total_rows)
        );
        assert!(
            workload_cases
                .iter()
                .all(|workload_case| workload_case.file_set.total_bytes()
                    == workload_case.shape.active_data_size_bytes)
        );

        Ok(())
    }

    #[test]
    fn edge_workload_cases_cover_correctness_only_shapes() -> Result<(), Box<dyn Error>> {
        let workload_cases = SyntheticWorkloadCase::edge_cases()?;
        let names = workload_cases
            .iter()
            .map(|workload_case| workload_case.name)
            .collect::<Vec<_>>();

        assert_eq!(names, ["empty_scan", "one_file", "few_medium_files"]);
        assert_eq!(workload_cases[0].file_set.files.len(), 0);
        assert_eq!(workload_cases[0].shape.average_file_size_bytes(), 0);
        assert_eq!(workload_cases[1].file_set.files.len(), 1);
        assert_eq!(workload_cases[2].file_set.files.len(), 8);

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
            profile.bandwidth_bytes_per_second > 0
                && profile.aggregate_bandwidth_bytes_per_second > 0
                && profile.jitter_basis_points <= 10_000
                && profile.partition_scheduling_overhead_micros > 0
                && profile.effective_parallelism > 0
        }));
        assert_eq!(
            SyntheticWorkSimulationProfile::s3_throttled().aggregate_bandwidth_bytes_per_second,
            12_500_000
        );
    }

    #[test]
    fn simulated_file_work_is_deterministic() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let profile = SyntheticWorkSimulationProfile::s3_normal();

        assert_eq!(
            profile.simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?,
            profile.simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?
        );

        Ok(())
    }

    #[test]
    fn simulation_seed_changes_deterministic_jitter() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let profile = SyntheticWorkSimulationProfile::s3_normal();
        let seed_one = profile.simulate_file_set(&file_set, 1)?;
        let seed_one_again = profile.simulate_file_set(&file_set, 1)?;
        let seed_two = profile.simulate_file_set(&file_set, 2)?;

        assert_eq!(seed_one, seed_one_again);
        assert_ne!(seed_one.serial_micros, seed_two.serial_micros);

        Ok(())
    }

    #[test]
    fn simulated_storage_profiles_change_total_work() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let local = SyntheticWorkSimulationProfile::local_fast()
            .simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?;
        let normal = SyntheticWorkSimulationProfile::s3_normal()
            .simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?;
        let high_latency = SyntheticWorkSimulationProfile::s3_high_latency()
            .simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?;
        let cpu_heavy = SyntheticWorkSimulationProfile::cpu_heavy()
            .simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?;

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
    fn percentile_nearest_rank_handles_empty_and_rounding() {
        assert_eq!(percentile_nearest_rank(Vec::new(), 50), 0);
        assert_eq!(percentile_nearest_rank(vec![10, 20, 30, 40], 50), 20);
        assert_eq!(percentile_nearest_rank(vec![10, 20, 30, 40], 95), 40);
        assert_eq!(percentile_nearest_rank(vec![10, 20, 30, 40], 100), 40);
    }

    #[test]
    fn throughput_per_second_handles_zero_and_integer_rate() {
        assert_eq!(throughput_per_second(0, 100), 0);
        assert_eq!(throughput_per_second(100, 0), 0);
        assert_eq!(throughput_per_second(100, 1_000_000), 100);
        assert_eq!(throughput_per_second(100, 500_000), 200);
    }

    #[test]
    fn partitioned_work_rejects_zero_target() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let work = SyntheticWorkSimulationProfile::s3_normal()
            .simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?;
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
        let work = SyntheticWorkSimulationProfile::s3_normal()
            .simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?;
        let plan = work.partition_by_estimated_bytes(&file_set, 1)?;

        assert_eq!(plan.partitions.len(), 1);
        assert_eq!(
            plan.scheduling_overhead_micros,
            work.partition_scheduling_overhead_micros
        );
        assert_eq!(plan.execution_slots, 1);
        assert!(plan.aggregate_transfer_floor_micros > 0);
        assert_eq!(
            plan.wall_micros,
            work.serial_micros.max(plan.aggregate_transfer_floor_micros)
                + work.partition_scheduling_overhead_micros
        );
        assert_eq!(plan.partitions[0].file_count, file_set.files.len());
        assert_eq!(plan.partitions[0].rows, shape.total_rows);
        assert_eq!(plan.partitions[0].size_bytes, shape.active_data_size_bytes);

        Ok(())
    }

    #[test]
    fn partitioned_work_uses_known_size_grouping_shape() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let work = SyntheticWorkSimulationProfile::s3_normal()
            .simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?;
        let plan = work.partition_by_estimated_bytes(&file_set, 16)?;

        assert!(!plan.unknown_size_fallback_used);
        assert!(plan.partitions.len() <= 16);
        assert_eq!(
            plan.scheduling_overhead_micros,
            work.partition_scheduling_overhead_micros * plan.partitions.len() as u64
        );
        assert_eq!(plan.execution_slots, plan.partitions.len());
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

        let summary = plan.summary();
        assert!(summary.files_p50 > 0);
        assert!(summary.files_p95 >= summary.files_p50);
        assert!(summary.files_max >= summary.files_p95);
        assert!(summary.bytes_p95 >= summary.bytes_p50);
        assert!(summary.bytes_max >= summary.bytes_p95);
        assert!(summary.work_micros_p95 >= summary.work_micros_p50);
        assert!(summary.work_imbalance_basis_points >= 10_000);

        Ok(())
    }

    #[test]
    fn partitioned_work_uses_file_count_fallback_for_unknown_sizes() -> Result<(), Box<dyn Error>> {
        let workload_case = SyntheticWorkloadCase::unknown_size_files()?;
        let file_set = &workload_case.file_set;
        let work = SyntheticWorkSimulationProfile::s3_normal()
            .simulate_file_set(file_set, DEFAULT_BENCHMARK_SEED)?;
        let plan = work.partition_by_estimated_bytes(file_set, 16)?;

        assert!(plan.unknown_size_fallback_used);
        assert_eq!(plan.partitions.len(), 16);
        assert!(
            plan.partitions
                .iter()
                .all(|partition| partition.file_count == 64)
        );
        assert!(
            plan.partitions
                .iter()
                .all(|partition| partition.estimated_size_bytes.is_none())
        );
        assert_eq!(
            plan.partitions
                .iter()
                .map(|partition| partition.size_bytes)
                .sum::<u64>(),
            file_set.total_bytes()
        );

        Ok(())
    }

    #[test]
    fn partitioned_work_balances_zero_byte_files_by_file_count() -> Result<(), Box<dyn Error>> {
        let workload_case = SyntheticWorkloadCase::zero_byte_files()?;
        let file_set = &workload_case.file_set;
        let work = SyntheticWorkSimulationProfile::s3_normal()
            .simulate_file_set(file_set, DEFAULT_BENCHMARK_SEED)?;
        let plan = work.partition_by_estimated_bytes(file_set, 16)?;

        assert!(!plan.unknown_size_fallback_used);
        assert_eq!(plan.partitions.len(), 16);
        assert!(
            plan.partitions
                .iter()
                .all(|partition| partition.file_count == 32)
        );
        assert!(
            plan.partitions
                .iter()
                .all(|partition| partition.estimated_size_bytes == Some(0))
        );
        assert_eq!(
            plan.partitions
                .iter()
                .map(|partition| partition.size_bytes)
                .sum::<u64>(),
            0
        );

        Ok(())
    }

    #[test]
    fn partitioned_work_uses_effective_parallelism_slots() -> Result<(), Box<dyn Error>> {
        let workload_case = SyntheticWorkloadCase::many_tiny_files()?;
        let file_set = &workload_case.file_set;
        let work = SyntheticWorkSimulationProfile::s3_throttled()
            .simulate_file_set(file_set, DEFAULT_BENCHMARK_SEED)?;
        let plan = work.partition_by_estimated_bytes(file_set, 64)?;
        let max_partition_work = plan
            .partitions
            .iter()
            .map(|partition| partition.work_micros)
            .max()
            .unwrap_or_default();

        assert_eq!(plan.partitions.len(), 64);
        assert_eq!(plan.execution_slots, 16);
        assert!(plan.wall_micros > max_partition_work + plan.scheduling_overhead_micros);

        Ok(())
    }

    #[test]
    fn partitioned_work_applies_aggregate_bandwidth_floor() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let work = SyntheticWorkSimulationProfile::s3_throttled()
            .simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?;
        let plan = work.partition_by_estimated_bytes(&file_set, 64)?;
        let expected_floor_micros = scaled_ceil_div(
            file_set.total_bytes(),
            1_000_000,
            work.aggregate_bandwidth_bytes_per_second,
            "test aggregate floor",
        )?;
        let max_partition_work = plan
            .partitions
            .iter()
            .map(|partition| partition.work_micros)
            .max()
            .unwrap_or_default();

        assert_eq!(work.aggregate_bandwidth_bytes_per_second, 12_500_000);
        assert_eq!(plan.aggregate_transfer_floor_micros, expected_floor_micros);
        assert!(plan.aggregate_transfer_floor_micros > max_partition_work);
        assert_eq!(
            plan.wall_micros,
            plan.aggregate_transfer_floor_micros + plan.scheduling_overhead_micros
        );
        assert_eq!(
            plan.summary().work_imbalance_basis_points,
            max_partition_work.saturating_mul(10_000)
                / average_or_zero(
                    plan.partitions
                        .iter()
                        .map(|partition| partition.work_micros)
                        .sum(),
                    plan.partitions.len()
                )
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

        assert_eq!(cases.len(), 37);
        assert_eq!(names.first(), Some(&"default_policy"));
        assert!(names.contains(&"fixed_target_1"));
        assert!(names.contains(&"fixed_target_4"));
        assert!(names.contains(&"fixed_target_8"));
        assert!(names.contains(&"fixed_target_16"));
        assert!(names.contains(&"fixed_target_32"));
        assert!(names.contains(&"fixed_target_64"));
        assert!(names.contains(&"available_parallelism_uncapped"));
        assert!(names.contains(&"available_parallelism_x2_uncapped"));
        assert!(names.contains(&"datafusion_cap_4"));
        assert!(names.contains(&"available_parallelism_override_4"));
        assert!(names.contains(&"available_parallelism_override_16"));
        assert!(names.contains(&"available_parallelism_override_64"));
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
        let fixed_target_1 = find_policy_case(&cases, "fixed_target_1")?.derive_target()?;
        let fixed_target_4 = find_policy_case(&cases, "fixed_target_4")?.derive_target()?;
        let fixed_target_8 = find_policy_case(&cases, "fixed_target_8")?.derive_target()?;
        let fixed_target_16 = find_policy_case(&cases, "fixed_target_16")?.derive_target()?;
        let fixed_target_32 = find_policy_case(&cases, "fixed_target_32")?.derive_target()?;
        let fixed_target_64 = find_policy_case(&cases, "fixed_target_64")?.derive_target()?;
        let available_parallelism_uncapped =
            find_policy_case(&cases, "available_parallelism_uncapped")?.derive_target()?;
        let available_parallelism_x2_uncapped =
            find_policy_case(&cases, "available_parallelism_x2_uncapped")?.derive_target()?;
        let datafusion_cap_4 = find_policy_case(&cases, "datafusion_cap_4")?.derive_target()?;
        let available_parallelism_override_64 =
            find_policy_case(&cases, "available_parallelism_override_64")?.derive_target()?;
        let fd_per_partition_16 =
            find_policy_case(&cases, "fd_per_partition_16")?.derive_target()?;
        let fd_per_partition_32 =
            find_policy_case(&cases, "fd_per_partition_32")?.derive_target()?;
        let memory_per_partition_256mib =
            find_policy_case(&cases, "memory_per_partition_256mib")?.derive_target()?;
        let memory_per_partition_512mib =
            find_policy_case(&cases, "memory_per_partition_512mib")?.derive_target()?;
        let combined = find_policy_case(&cases, "combined_fd_32_memory_512mib")?.derive_target()?;

        assert_eq!(fixed_target_1.target_partitions, 1);
        assert_eq!(
            fixed_target_1.source,
            DeltaScanPartitionTargetDiagnosticSource::ExplicitOverride
        );
        assert_eq!(fixed_target_4.target_partitions, 4);
        assert_eq!(
            fixed_target_4.source,
            DeltaScanPartitionTargetDiagnosticSource::ExplicitOverride
        );
        assert_eq!(fixed_target_8.target_partitions, 8);
        assert_eq!(fixed_target_16.target_partitions, 16);
        assert_eq!(fixed_target_32.target_partitions, 32);
        assert_eq!(fixed_target_64.target_partitions, 64);
        assert_eq!(available_parallelism_uncapped.target_partitions, 16);
        assert_eq!(available_parallelism_uncapped.datafusion_target_cap, None);
        assert_eq!(available_parallelism_x2_uncapped.target_partitions, 32);
        assert_eq!(
            available_parallelism_x2_uncapped.datafusion_target_cap,
            None
        );
        assert_eq!(datafusion_cap_4.target_partitions, 4);
        assert_eq!(datafusion_cap_4.datafusion_target_cap, Some(4));
        assert_eq!(available_parallelism_override_64.target_partitions, 64);
        assert_eq!(
            available_parallelism_override_64.available_parallelism,
            Some(64)
        );
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
        let work = SyntheticWorkSimulationProfile::s3_normal()
            .simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?;
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
        assert_eq!(BENCHMARK_CSV_HEADER.len(), 61);
        assert_eq!(BENCHMARK_CSV_HEADER[0], "benchmark_schema_version");
        assert_eq!(BENCHMARK_CSV_HEADER[1], "host_os");
        assert_eq!(BENCHMARK_CSV_HEADER[2], "host_arch");
        assert_eq!(BENCHMARK_CSV_HEADER[3], "host_available_parallelism");
        assert_eq!(BENCHMARK_CSV_HEADER[4], "seed");
        assert_eq!(BENCHMARK_CSV_HEADER[5], "workload_case_count");
        assert_eq!(BENCHMARK_CSV_HEADER[6], "workload_case");
        assert_eq!(
            BENCHMARK_CSV_HEADER[27],
            "simulation_partition_scheduling_overhead_micros"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[28], "simulation_effective_parallelism");
        assert_eq!(
            BENCHMARK_CSV_HEADER[29],
            "simulation_aggregate_bandwidth_bytes_per_second"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[30], "policy_case");
        assert_eq!(BENCHMARK_CSV_HEADER[31], "policy_available_parallelism");
        assert_eq!(BENCHMARK_CSV_HEADER[32], "policy_datafusion_target");
        assert_eq!(BENCHMARK_CSV_HEADER[33], "policy_available_memory_bytes");
        assert_eq!(BENCHMARK_CSV_HEADER[34], "policy_unix_soft_fd_limit");
        assert_eq!(BENCHMARK_CSV_HEADER[37], "policy_target");
        assert_eq!(BENCHMARK_CSV_HEADER[38], "policy_source");
        assert_eq!(BENCHMARK_CSV_HEADER[42], "unknown_size_fallback_used");
        assert_eq!(
            BENCHMARK_CSV_HEADER[46],
            "simulated_scheduling_overhead_micros"
        );
        assert_eq!(
            BENCHMARK_CSV_HEADER[47],
            "simulated_aggregate_transfer_floor_micros"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[48], "simulated_execution_slots");
        assert_eq!(
            BENCHMARK_CSV_HEADER[50],
            "simulated_throughput_mib_per_second"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[52], "partition_files_p50");
        assert_eq!(
            BENCHMARK_CSV_HEADER[60],
            "partition_work_imbalance_basis_points"
        );
    }

    #[test]
    fn benchmark_csv_row_matches_header_width() -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::partitioned_event_log();
        let file_set = shape.generate_file_set()?;
        let simulation = SyntheticWorkSimulationProfile::s3_normal();
        let simulated_work = simulation.simulate_file_set(&file_set, DEFAULT_BENCHMARK_SEED)?;
        let cases = BenchmarkPolicyCase::standard_cases(Some(16));
        let case = &cases[0];
        let decision = case.derive_target()?;
        let partitioned_work =
            simulated_work.partition_by_estimated_bytes(&file_set, decision.target_partitions)?;
        let row = benchmark_csv_row(BenchmarkCsvRowInput {
            shape: &shape,
            file_set: &file_set,
            run_environment: BenchmarkRunEnvironment {
                schema_version: BENCHMARK_SCHEMA_VERSION,
                host_os: "test-os",
                host_arch: "test-arch",
                available_parallelism: Some(16),
            },
            seed: 7,
            workload_case: "test-workload",
            workload_case_count: SyntheticWorkloadCase::standard_cases()?.len(),
            simulation_profile_count: SyntheticWorkSimulationProfile::standard_profiles().len(),
            simulation,
            policy_case: case,
            policy_decision: decision,
            simulated_work: &simulated_work,
            partitioned_work: &partitioned_work,
        });

        assert_eq!(row.len(), BENCHMARK_CSV_HEADER.len());
        assert_eq!(row[0], "5");
        assert_eq!(row[1], "test-os");
        assert_eq!(row[2], "test-arch");
        assert_eq!(row[3], "16");
        assert_eq!(row[4], "7");
        assert_eq!(row[5], "6");
        assert_eq!(row[6], "test-workload");
        assert_eq!(row[27], "1000");
        assert_eq!(row[28], "32");
        assert_eq!(row[29], "131072000");
        assert_eq!(row[30], "default_policy");
        assert_eq!(row[37], "16");
        assert_eq!(row[38], "available_parallelism_fallback");
        assert_eq!(row[42], "false");
        assert_eq!(row[46], "16000");
        assert!(!row[47].is_empty());
        assert_eq!(row[48], "16");
        assert!(!row[50].is_empty());
        assert!(!row[51].is_empty());
        assert!(!row[60].is_empty());

        Ok(())
    }
}
