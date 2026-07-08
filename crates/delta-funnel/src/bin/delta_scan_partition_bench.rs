//! Portable synthetic Delta scan partition benchmark runner.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{Datelike, Days, NaiveDate};
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use delta_funnel::{
    DeltaFunnelSession, DeltaProviderReadStatsSnapshot, DeltaProviderReaderBackend,
    DeltaProviderScanExecutionOptions, DeltaScanPartitionTargetDiagnosticInput,
    DeltaScanPartitionTargetDiagnosticOutput, DeltaScanPartitionTargetDiagnosticSource,
    DeltaScanPartitionTargetLocalEnvironmentDiagnostic,
    DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus, DeltaSourceConfig,
    DeltaStorageOptions, DeltaTableProviderConfig, LoadMode, MssqlConnectionConfig,
    MssqlOutputTarget, MssqlSchemaPlanOptions, MssqlTargetConfig, MssqlTargetTable,
    MssqlTimezonePolicy, OutputWritePlan, PhaseTimingReport, QueryOptions, RunMode, SessionOptions,
    WriteAllCacheMode, WriteAllOptions, collect_delta_provider_read_stats,
    delta_scan_partition_target_local_environment_diagnostic,
    derive_delta_scan_partition_target_diagnostic, load_delta_source_with_tracing,
    preflight_delta_protocol_with_tracing, register_delta_sources_with_scan_execution_options,
};
use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use futures_util::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tracing_subscriber::fmt::MakeWriter;

const MIB: u64 = 1024 * 1024;
const BENCHMARK_FD_PER_PARTITION_CANDIDATES: [usize; 4] = [4, 8, 16, 32];
const BENCHMARK_MEMORY_BYTES_PER_PARTITION_CANDIDATES: [u64; 4] =
    [64 * MIB, 128 * MIB, 256 * MIB, 512 * MIB];
const BENCHMARK_AVAILABLE_PARALLELISM_CANDIDATES: [usize; 3] = [4, 16, 64];
const BENCHMARK_UNIX_SOFT_FD_LIMIT: u64 = 128;
const BENCHMARK_AVAILABLE_MEMORY_BYTES: u64 = 1024 * MIB;
const HOST_PROBE_MAX_SCHEDULER_CONCURRENCY: usize = 64;
const HOST_PROBE_SCHEDULER_TASKS_PER_WORKER: usize = 256;
const HOST_PROBE_DEFAULT_LOCAL_IO_BYTES: usize = MIB as usize;
const HOST_PROBE_MAX_LOCAL_IO_BYTES: usize = 64 * MIB as usize;
const HOST_PROBE_DEFAULT_LOCAL_IO_REPETITIONS: usize = 3;
const HOST_PROBE_MAX_LOCAL_IO_REPETITIONS: usize = 128;
const BENCHMARK_SCHEMA_VERSION: u32 = 19;
const DEFAULT_BENCHMARK_SEED: u64 = 0;
const DEFAULT_PROVIDER_EXEC_REPETITIONS: usize = 3;
const PROVIDER_EXEC_DEFAULT_CASE_WORKLOAD: &str = "provider_partitioned_event_log_12m";
const PROVIDER_EXEC_DEFAULT_CASE_QUERY: &str = "project_event_keys";
const PROVIDER_EXEC_DEFAULT_CASE_SCHEDULING_PROFILE: &str = "default_execution";
const PROVIDER_EXEC_FIXTURE_CREATE_COMPLETED_EVENT: &str = "provider_exec_fixture_create.completed";
const PROVIDER_EXEC_FIXTURE_CREATE_FAILED_EVENT: &str = "provider_exec_fixture_create.failed";
const PROVIDER_EXEC_FIXTURE_CREATE_STARTED_EVENT: &str = "provider_exec_fixture_create.started";
const PROVIDER_EXEC_QUERY_EXECUTION_COMPLETED_EVENT: &str = "datafusion_query_execution.completed";
const PROVIDER_EXEC_QUERY_EXECUTION_FAILED_EVENT: &str = "datafusion_query_execution.failed";
const PROVIDER_EXEC_QUERY_EXECUTION_FIRST_BATCH_EVENT: &str =
    "datafusion_query_execution.first_batch";
const PROVIDER_EXEC_QUERY_EXECUTION_STARTED_EVENT: &str = "datafusion_query_execution.started";
const PROVIDER_EXEC_QUERY_PLANNING_COMPLETED_EVENT: &str = "datafusion_query_planning.completed";
const PROVIDER_EXEC_QUERY_PLANNING_FAILED_EVENT: &str = "datafusion_query_planning.failed";
const PROVIDER_EXEC_QUERY_PLANNING_STARTED_EVENT: &str = "datafusion_query_planning.started";
const PROVIDER_EXEC_STATS_COLLECT_COMPLETED_EVENT: &str = "provider_read_stats_collect.completed";
const PROVIDER_EXEC_STATS_COLLECT_STARTED_EVENT: &str = "provider_read_stats_collect.started";
const PROVIDER_EXEC_WRITE_WORKFLOW_QUERY: &str = "write_all_exports";
const MAX_PROVIDER_EXEC_REPETITIONS: usize = 128;
const PROVIDER_EXEC_MODIFICATION_TIME_MS: i64 = 1_587_968_586_000;
const PROVIDER_EXEC_PROTOCOL_JSON: &str =
    r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
const PROVIDER_EXEC_DELETION_VECTOR_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#;
const PROVIDER_EXEC_RELATIVE_DV_ID: &str = "vBn[lx{q8@P<9BNH/isA";
const PROVIDER_EXEC_RELATIVE_DV_FILE: &str =
    "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";
const BENCHMARK_CSV_HEADER: [&str; 80] = [
    "benchmark_schema_version",
    "benchmark_mode",
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
    "host_memory_total_bytes",
    "host_memory_available_bytes",
    "host_unix_soft_fd_limit",
    "host_unix_soft_fd_limit_status",
    "host_scheduler_probe_task_count",
    "host_scheduler_probe_completed_task_count",
    "host_scheduler_probe_concurrency",
    "host_scheduler_probe_total_micros",
    "host_scheduler_probe_nanos_per_task",
    "host_runtime_probe_stable_concurrency_hint",
    "host_local_io_probe_enabled",
    "host_local_io_probe_status",
    "host_local_io_probe_repetitions",
    "host_local_io_probe_bytes_per_repetition",
    "host_local_io_probe_bytes_read",
    "host_local_io_probe_total_micros",
    "host_local_io_probe_latency_micros",
    "host_local_io_probe_throughput_bytes_per_second",
];
const PROVIDER_EXEC_CSV_HEADER: [&str; 70] = [
    "benchmark_schema_version",
    "benchmark_mode",
    "host_os",
    "host_arch",
    "host_available_parallelism",
    "seed",
    "workload_case_count",
    "workload_case",
    "provider_exec_storage_profile",
    "query_case",
    "reader_backend",
    "scheduling_mode",
    "scan_target_partitions",
    "max_concurrent_file_reads_per_scan",
    "max_concurrent_file_reads_per_partition",
    "output_buffer_capacity_per_partition",
    "native_async_prefetch_file_count_per_partition",
    "repetitions",
    "file_count",
    "row_count",
    "data_file_bytes",
    "deletion_vector_file_count",
    "deletion_vector_deleted_rows",
    "deletion_vector_deleted_rows_per_file",
    "provider_stats_scan_count",
    "provider_stats_scan_metadata_exhausted",
    "provider_stats_scan_partitions_planned",
    "provider_stats_files_planned",
    "provider_stats_estimated_rows",
    "provider_stats_estimated_bytes",
    "provider_stats_scan_partitions_started_p50",
    "provider_stats_scan_partitions_completed_p50",
    "provider_stats_files_started_p50",
    "provider_stats_files_completed_p50",
    "provider_stats_dynamic_partition_files_pruned_p50",
    "provider_stats_dynamic_partition_files_kept_p50",
    "provider_stats_dynamic_filters_received_p50",
    "provider_stats_dynamic_filters_accepted_p50",
    "provider_stats_dynamic_filters_unsupported_p50",
    "provider_stats_dynamic_filter_snapshots_p50",
    "provider_stats_dynamic_partition_files_not_pruned_missing_metadata_p50",
    "provider_stats_dynamic_partition_files_not_pruned_unsupported_expression_p50",
    "provider_stats_batches_produced_p50",
    "provider_stats_rows_produced_p50",
    "provider_stats_deletion_vector_payloads_loaded_p50",
    "provider_stats_deletion_vectors_applied_p50",
    "provider_stats_deletion_vector_rows_deleted_p50",
    "provider_stats_deletion_vector_failures_p50",
    "provider_stats_deletion_vector_rejections_p50",
    "produced_rows",
    "produced_batches",
    "process_peak_rss_bytes",
    "process_peak_rss_delta_bytes",
    "planning_micros_p50",
    "planning_micros_p95",
    "planning_micros_p99",
    "time_to_first_batch_micros_p50",
    "time_to_first_batch_micros_p95",
    "time_to_first_batch_micros_p99",
    "total_micros_p50",
    "total_micros_p95",
    "total_micros_p99",
    "source_rows_per_second_p50",
    "source_rows_per_second_p95",
    "source_rows_per_second_p99",
    "batch_latency_micros_p50",
    "batch_latency_micros_p95",
    "batch_latency_micros_p99",
    "min_total_micros",
    "max_total_micros",
];

fn main() -> Result<(), Box<dyn Error>> {
    let config = BenchmarkRunnerConfig::parse(env::args_os().skip(1))?;

    if config.show_help {
        print_usage(io::stdout())?;
        return Ok(());
    }

    if let Some(trace_output_path) = &config.trace_output_path {
        let subscriber = provider_exec_trace_subscriber(trace_output_path)?;
        return tracing::subscriber::with_default(subscriber, || run_benchmark(&config));
    }

    run_benchmark(&config)
}

fn run_benchmark(config: &BenchmarkRunnerConfig) -> Result<(), Box<dyn Error>> {
    if let Some(output_path) = &config.output_path {
        let mut output = File::create(output_path)?;
        write_benchmark_csv(&mut output, config)?;
    } else {
        let stdout = io::stdout();
        let mut output = stdout.lock();
        write_benchmark_csv(&mut output, config)?;
    }

    Ok(())
}

fn provider_exec_trace_subscriber(
    path: &Path,
) -> Result<impl tracing::Subscriber + Send + Sync, Box<dyn Error>> {
    let writer = TraceFileMakeWriter::new(path.to_path_buf())?;
    Ok(tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_writer(writer)
        .finish())
}

#[derive(Clone)]
struct TraceFileMakeWriter {
    path: Arc<PathBuf>,
}

impl TraceFileMakeWriter {
    fn new(path: PathBuf) -> io::Result<Self> {
        File::create(&path)?;
        Ok(Self {
            path: Arc::new(path),
        })
    }
}

struct TraceFileWriter {
    file: Option<File>,
}

impl<'a> MakeWriter<'a> for TraceFileMakeWriter {
    type Writer = TraceFileWriter;

    fn make_writer(&'a self) -> Self::Writer {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path.as_ref())
            .ok();
        TraceFileWriter { file }
    }
}

impl Write for TraceFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.file {
            Some(file) => file.write(buf),
            None => Ok(buf.len()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.file {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

fn write_benchmark_csv(
    output: &mut impl Write,
    config: &BenchmarkRunnerConfig,
) -> Result<(), Box<dyn Error>> {
    match config.mode {
        BenchmarkMode::Synthetic => write_synthetic_benchmark_csv(output, config.seed),
        BenchmarkMode::HostProbe => {
            write_host_probe_benchmark_csv(output, config.seed, &config.host_probe_local_io)
        }
        BenchmarkMode::ProviderExec => {
            write_provider_exec_benchmark_csv(output, config.seed, &config.provider_exec)
        }
    }
}

fn write_synthetic_benchmark_csv(output: &mut impl Write, seed: u64) -> Result<(), Box<dyn Error>> {
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
                        mode: BenchmarkMode::Synthetic,
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

fn write_host_probe_benchmark_csv(
    output: &mut impl Write,
    seed: u64,
    local_io_config: &HostProbeLocalIoConfig,
) -> Result<(), Box<dyn Error>> {
    let run_environment = BenchmarkRunEnvironment::local();
    let local_environment = delta_scan_partition_target_local_environment_diagnostic();
    let scheduler_probe = run_host_scheduler_probe(run_environment.available_parallelism);
    let local_io_probe = run_host_local_io_probe(local_io_config, seed);
    let policy_decision =
        derive_delta_scan_partition_target_diagnostic(local_environment.policy_input)?;

    writeln!(output, "{}", BENCHMARK_CSV_HEADER.join(","))?;
    writeln!(
        output,
        "{}",
        host_probe_csv_row(HostProbeCsvRowInput {
            run_environment,
            seed,
            local_environment,
            scheduler_probe,
            local_io_probe,
            policy_decision,
        })
        .join(",")
    )?;

    Ok(())
}

fn write_provider_exec_benchmark_csv(
    output: &mut impl Write,
    seed: u64,
    config: &ProviderExecConfig,
) -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(write_provider_exec_benchmark_csv_async(
        output, seed, config,
    ))
}

async fn write_provider_exec_benchmark_csv_async(
    output: &mut impl Write,
    seed: u64,
    config: &ProviderExecConfig,
) -> Result<(), Box<dyn Error>> {
    let run_environment = BenchmarkRunEnvironment::local();
    let workloads = ProviderExecWorkloadCase::standard_cases()?
        .into_iter()
        .filter(|workload| provider_exec_filter_matches(&config.workload_filter, workload.name))
        .collect::<Vec<_>>();
    validate_provider_exec_filter_result("workload", &config.workload_filter, workloads.len())?;
    let scheduling_profiles = if config.default_case {
        vec![ProviderExecSchedulingProfile::default_execution_case()]
    } else {
        let scheduling_profiles = ProviderExecSchedulingProfile::standard_cases(run_environment);
        let scheduling_profiles = scheduling_profiles
            .into_iter()
            .filter(|profile| {
                provider_exec_filter_matches(&config.scheduling_profile_filter, profile.name)
            })
            .collect::<Vec<_>>();
        validate_provider_exec_filter_result(
            "scheduling profile",
            &config.scheduling_profile_filter,
            scheduling_profiles.len(),
        )?;
        scheduling_profiles
    };
    let backends = if config.default_case {
        vec![DeltaProviderScanExecutionOptions::default().reader_backend]
    } else {
        let backends = [
            DeltaProviderReaderBackend::OfficialKernel,
            DeltaProviderReaderBackend::NativeAsync,
        ]
        .into_iter()
        .filter(|backend| {
            provider_exec_filter_matches(
                &config.backend_filter,
                provider_exec_backend_name(*backend),
            )
        })
        .collect::<Vec<_>>();
        validate_provider_exec_filter_result("backend", &config.backend_filter, backends.len())?;
        backends
    };
    let temp_root = config.temp_dir.clone().unwrap_or_else(env::temp_dir);

    writeln!(output, "{}", PROVIDER_EXEC_CSV_HEADER.join(","))?;
    for workload in &workloads {
        provider_exec_fixture_create_started(workload, config.storage_profile);
        let table =
            match ProviderExecDeltaTable::create(&temp_root, workload, config.storage_profile) {
                Ok(table) => {
                    provider_exec_fixture_create_completed(&table, workload);
                    table
                }
                Err(error) => {
                    provider_exec_fixture_create_failed(workload, config.storage_profile, &*error);
                    return Err(error);
                }
            };
        let query_cases = workload
            .query_cases()
            .into_iter()
            .filter(|query| provider_exec_filter_matches(&config.query_filter, query.name))
            .collect::<Vec<_>>();
        if config.phase_aligned_workflow {
            if workload.schema_kind != ProviderExecSchemaKind::SyntheticWideEventExport {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "--provider-exec-phase-aligned-workflow only supports provider_wide_event_export_13m",
                )
                .into());
            }
            let workflow_query_matches = provider_exec_filter_matches(
                &config.query_filter,
                PROVIDER_EXEC_WRITE_WORKFLOW_QUERY,
            );
            validate_provider_exec_filter_result(
                "query",
                &config.query_filter,
                usize::from(workflow_query_matches),
            )?;
            if !workflow_query_matches {
                continue;
            }
        } else {
            validate_provider_exec_filter_result("query", &config.query_filter, query_cases.len())?;
        }
        let query_cases = if config.phase_aligned_workflow {
            vec![ProviderExecQueryCase {
                name: PROVIDER_EXEC_WRITE_WORKFLOW_QUERY,
                sql: "",
            }]
        } else {
            query_cases
        };
        for query in query_cases {
            for backend in &backends {
                for scheduling_profile in &scheduling_profiles {
                    let summary = if config.phase_aligned_workflow {
                        run_provider_exec_write_workflow_case(
                            &table,
                            workload,
                            *backend,
                            *scheduling_profile,
                            config,
                        )
                        .await?
                    } else {
                        run_provider_exec_benchmark_case(
                            &table,
                            workload,
                            query,
                            *backend,
                            *scheduling_profile,
                            config,
                        )
                        .await?
                    };
                    writeln!(
                        output,
                        "{}",
                        provider_exec_csv_row(ProviderExecCsvRowInput {
                            run_environment,
                            seed,
                            workload_case_count: workloads.len(),
                            table: &table,
                            workload,
                            query,
                            backend: *backend,
                            scheduling_profile: *scheduling_profile,
                            summary: &summary,
                        })
                        .join(",")
                    )?;
                }
            }
        }
    }

    Ok(())
}

fn print_usage(mut output: impl Write) -> io::Result<()> {
    writeln!(
        output,
        "Usage: delta_scan_partition_bench [--mode <synthetic|host-probe|provider-exec>] [--output <path>] [--seed <u64>]"
    )?;
    writeln!(output)?;
    writeln!(
        output,
        "Writes a portable Delta scan partition benchmark matrix as CSV."
    )?;
    writeln!(
        output,
        "Without --output, CSV is written to stdout for shell pipelines."
    )?;
    writeln!(
        output,
        "Use --trace-output <path> to write newline-delimited JSON tracing events."
    )?;
    writeln!(output, "The default mode is synthetic.")?;
    writeln!(
        output,
        "Use --host-probe-local-io with host-probe mode to run the opt-in local IO read probe."
    )?;
    writeln!(
        output,
        "Use --provider-exec-repetitions <n> with provider-exec mode to choose repeated runs."
    )?;
    writeln!(
        output,
        "Use --provider-exec-storage-profile <local|s3-normal|s3-high-latency|s3-throttled> to add provider-exec storage latency."
    )?;
    writeln!(
        output,
        "Use --provider-exec-workload, --provider-exec-query, --provider-exec-backend, and --provider-exec-scheduling-profile to run a focused provider-exec case."
    )?;
    writeln!(
        output,
        "Use --provider-exec-default-case to run one representative provider-exec case with production default scan execution options."
    )?;
    writeln!(
        output,
        "Use --provider-exec-phase-aligned-workflow to run the wide export preset as one no-DB write_all workflow."
    )?;
    writeln!(output, "The default seed is {DEFAULT_BENCHMARK_SEED}.")?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchmarkRunnerConfig {
    output_path: Option<PathBuf>,
    trace_output_path: Option<PathBuf>,
    mode: BenchmarkMode,
    host_probe_local_io: HostProbeLocalIoConfig,
    provider_exec: ProviderExecConfig,
    seed: u64,
    show_help: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HostProbeLocalIoConfig {
    enabled: bool,
    temp_dir: Option<PathBuf>,
    bytes_per_repetition: usize,
    repetitions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProviderExecConfig {
    repetitions: usize,
    temp_dir: Option<PathBuf>,
    storage_profile: ProviderExecStorageProfile,
    default_case: bool,
    phase_aligned_workflow: bool,
    workload_filter: Option<String>,
    query_filter: Option<String>,
    backend_filter: Option<String>,
    scheduling_profile_filter: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProviderExecStorageProfile {
    name: &'static str,
    open_latency_micros: u64,
    read_latency_micros: u64,
    bandwidth_bytes_per_second: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkMode {
    Synthetic,
    HostProbe,
    ProviderExec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BenchmarkRunEnvironment {
    schema_version: u32,
    host_os: &'static str,
    host_arch: &'static str,
    available_parallelism: Option<usize>,
}

#[derive(Debug, Clone)]
struct ProviderExecWorkloadCase {
    name: &'static str,
    schema_kind: ProviderExecSchemaKind,
    file_specs: Vec<ProviderExecFileSpec>,
    deleted_row_indexes_per_file: &'static [u64],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderExecSchemaKind {
    SimpleOrders,
    SyntheticPartitionedEventLog,
    SyntheticWideEventExport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProviderExecFileSpec {
    path: String,
    rows: usize,
    partition_date: Option<SyntheticDate>,
}

#[derive(Debug, Clone, Copy)]
struct ProviderExecQueryCase {
    name: &'static str,
    sql: &'static str,
}

macro_rules! wide_event_transform_sql {
    ($select:literal) => {
        concat!(
            r#"
WITH metadata_raw AS (
    SELECT 0 AS metadata_bucket, 'level_a' AS metadata_level, 10 AS priority
    UNION ALL SELECT 0, 'level_a_fallback', 1
    UNION ALL SELECT 1, 'level_b', 10
    UNION ALL SELECT 2, 'level_c', 10
    UNION ALL SELECT 3, 'level_d', 10
    UNION ALL SELECT 4, 'level_e', 10
    UNION ALL SELECT 5, 'level_f', 10
    UNION ALL SELECT 6, 'level_g', 10
),
metadata_ranked AS (
    SELECT
        metadata_bucket,
        metadata_level,
        row_number() OVER (
            PARTITION BY metadata_bucket
            ORDER BY priority DESC
        ) AS rn
    FROM metadata_raw
),
metadata_one AS (
    SELECT metadata_bucket, metadata_level
    FROM metadata_ranked
    WHERE rn = 1
),
normalized_events AS (
    SELECT
        primary_event_id,
        group_id,
        secondary_event_id,
        CASE
            WHEN source_kind IN ('web', 'api') THEN 'source_a'
            WHEN source_kind = 'mobile' THEN 'source_b'
            ELSE 'source_c'
        END AS source_kind,
        actor_numeric_id,
        category_num,
        position_num,
        position_code,
        metric_x,
        metric_y,
        metric_z,
        event_time,
        event_year,
        event_month,
        event_day,
        event_processed_year,
        event_processed_month,
        event_processed_day,
        position_processed_year,
        position_processed_month,
        position_processed_day,
        record_processed_year,
        record_processed_month,
        record_processed_day,
        CASE
            WHEN category_num < 311 THEN 'secondary'
            ELSE 'primary'
        END AS source_group,
        CASE
            WHEN category_num < 311 THEN secondary_event_id
            ELSE group_id
        END AS resolved_event_key,
        CASE
            WHEN category_num < 311 THEN 'secondary_segment'
            ELSE 'primary_segment'
        END AS resolution_diagnostic,
        validation_flag,
        quality_tier,
        event_year AS local_event_year,
        event_month AS local_event_month,
        event_day AS local_event_day,
        category_num % 7 AS metadata_bucket
    FROM orders
),
enriched_events AS (
    SELECT
        n.*,
        m.metadata_level AS source_level
    FROM normalized_events n
    LEFT JOIN metadata_one m
      ON n.metadata_bucket = m.metadata_bucket
),
primary_keys AS (
    SELECT DISTINCT group_id AS precedence_key
    FROM enriched_events
    WHERE source_group = 'primary'
),
post_precedence AS (
    SELECT e.*
    FROM enriched_events e
    LEFT JOIN primary_keys p
      ON e.resolved_event_key = p.precedence_key
     AND e.source_group = 'secondary'
    WHERE NOT (
        e.source_group = 'secondary'
        AND p.precedence_key IS NOT NULL
    )
),
export_ready AS (
    SELECT
        primary_event_id,
        group_id,
        secondary_event_id,
        source_kind,
        actor_numeric_id,
        category_num,
        position_num,
        position_code,
        metric_x,
        metric_y,
        metric_z,
        event_time,
        event_year,
        event_month,
        event_day,
        event_processed_year,
        event_processed_month,
        event_processed_day,
        position_processed_year,
        position_processed_month,
        position_processed_day,
        record_processed_year,
        record_processed_month,
        record_processed_day,
        resolution_diagnostic,
        resolved_event_key,
        validation_flag,
        source_level,
        source_group,
        CAST(
            local_event_year * 10000
            + local_event_month * 100
            + local_event_day
            AS STRING
        ) AS local_date_key,
        quality_tier,
        local_event_year,
        local_event_month,
        local_event_day
    FROM post_precedence
)
"#,
            $select
        )
    };
}

#[derive(Debug, Clone, Copy)]
struct ProviderExecSchedulingProfile {
    name: &'static str,
    scan_target_partitions: Option<usize>,
    max_concurrent_file_reads_per_scan: Option<usize>,
    max_concurrent_file_reads_per_partition: usize,
    output_buffer_capacity_per_partition: usize,
    native_async_prefetch_file_count_per_partition: usize,
    uses_default_execution_options: bool,
}

struct ProviderExecDeltaTable {
    path: PathBuf,
    table_uri: String,
    storage_options: DeltaStorageOptions,
    storage_profile: ProviderExecStorageProfile,
    delayed_http_server: Option<ProviderExecDelayedHttpServer>,
    file_count: usize,
    row_count: usize,
    data_file_bytes: u64,
    deletion_vector_file_count: usize,
    deletion_vector_deleted_rows: usize,
    deletion_vector_deleted_rows_per_file: usize,
}

struct ProviderExecDeletionVector {
    descriptor: DeletionVectorDescriptor,
    bytes: Vec<u8>,
}

struct ProviderExecDelayedHttpServer {
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    url: String,
}

impl Drop for ProviderExecDeltaTable {
    fn drop(&mut self) {
        if let Some(server) = &self.delayed_http_server {
            server.shutdown();
        }
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl Drop for ProviderExecDelayedHttpServer {
    fn drop(&mut self) {
        self.shutdown();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct ProviderExecRunMeasurement {
    planning_micros: u64,
    time_to_first_batch_micros: u64,
    total_micros: u64,
    source_rows_per_second: u64,
    produced_rows: usize,
    produced_batches: usize,
    process_peak_rss_bytes: Option<u64>,
    process_peak_rss_delta_bytes: Option<u64>,
    batch_latency_micros: Vec<u64>,
    read_stats: ProviderExecReadStatsMeasurement,
}

struct ProviderExecReadStatsMeasurement {
    scan_count: usize,
    scan_metadata_exhausted: ProviderExecScanMetadataExhausted,
    scan_partitions_planned: u64,
    files_planned: u64,
    estimated_rows: Option<u64>,
    estimated_bytes: Option<u64>,
    scan_partitions_started: u64,
    scan_partitions_completed: u64,
    files_started: u64,
    files_completed: u64,
    dynamic_partition_files_pruned: u64,
    dynamic_partition_files_kept: u64,
    dynamic_filters_received: u64,
    dynamic_filters_accepted: u64,
    dynamic_filters_unsupported: u64,
    dynamic_filter_snapshots: u64,
    dynamic_partition_files_not_pruned_missing_metadata: u64,
    dynamic_partition_files_not_pruned_unsupported_expression: u64,
    batches_produced: u64,
    rows_produced: u64,
    deletion_vector_payloads_loaded: u64,
    deletion_vectors_applied: u64,
    deletion_vector_rows_deleted: u64,
    deletion_vector_failures: u64,
    deletion_vector_rejections: u64,
}

struct ProviderExecSummary {
    repetitions: usize,
    produced_rows: usize,
    produced_batches: usize,
    planning_micros: PercentileSummary,
    time_to_first_batch_micros: PercentileSummary,
    total_micros: PercentileSummary,
    source_rows_per_second: PercentileSummary,
    batch_latency_micros: PercentileSummary,
    process_peak_rss_bytes: Option<u64>,
    process_peak_rss_delta_bytes: Option<u64>,
    min_total_micros: u64,
    max_total_micros: u64,
    read_stats: ProviderExecReadStatsSummary,
}

struct ProviderExecReadStatsSummary {
    scan_count: usize,
    scan_metadata_exhausted: ProviderExecScanMetadataExhausted,
    scan_partitions_planned: u64,
    files_planned: u64,
    estimated_rows: Option<u64>,
    estimated_bytes: Option<u64>,
    scan_partitions_started: u64,
    scan_partitions_completed: u64,
    files_started: u64,
    files_completed: u64,
    dynamic_partition_files_pruned: u64,
    dynamic_partition_files_kept: u64,
    dynamic_filters_received: u64,
    dynamic_filters_accepted: u64,
    dynamic_filters_unsupported: u64,
    dynamic_filter_snapshots: u64,
    dynamic_partition_files_not_pruned_missing_metadata: u64,
    dynamic_partition_files_not_pruned_unsupported_expression: u64,
    batches_produced: u64,
    rows_produced: u64,
    deletion_vector_payloads_loaded: u64,
    deletion_vectors_applied: u64,
    deletion_vector_rows_deleted: u64,
    deletion_vector_failures: u64,
    deletion_vector_rejections: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderExecScanMetadataExhausted {
    True,
    False,
    Unknown,
    Mixed,
}

struct ProviderExecCsvRowInput<'a> {
    run_environment: BenchmarkRunEnvironment,
    seed: u64,
    workload_case_count: usize,
    table: &'a ProviderExecDeltaTable,
    workload: &'a ProviderExecWorkloadCase,
    query: ProviderExecQueryCase,
    backend: DeltaProviderReaderBackend,
    scheduling_profile: ProviderExecSchedulingProfile,
    summary: &'a ProviderExecSummary,
}

#[derive(Clone, Copy)]
struct ProviderExecTraceContext<'a> {
    table: &'a ProviderExecDeltaTable,
    query: ProviderExecQueryCase,
    backend: DeltaProviderReaderBackend,
    scheduling_profile: ProviderExecSchedulingProfile,
    repetition_index: usize,
}

#[derive(Debug, Clone, Copy)]
struct PercentileSummary {
    p50: u64,
    p95: u64,
    p99: u64,
}

impl BenchmarkRunnerConfig {
    fn parse<I>(args: I) -> Result<Self, BenchmarkRunnerConfigError>
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        let mut output_path = None;
        let mut trace_output_path = None;
        let mut mode = BenchmarkMode::Synthetic;
        let mut host_probe_local_io = HostProbeLocalIoConfig::default();
        let mut provider_exec = ProviderExecConfig::default();
        let mut mode_seen = false;
        let mut provider_exec_storage_profile_seen = false;
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
            } else if arg == "--trace-output" {
                let path = args
                    .next()
                    .ok_or(BenchmarkRunnerConfigError::MissingTraceOutputPath)?;
                if trace_output_path.replace(PathBuf::from(path)).is_some() {
                    return Err(BenchmarkRunnerConfigError::DuplicateTraceOutputPath);
                }
            } else if arg == "--mode" {
                let value = args.next().ok_or(BenchmarkRunnerConfigError::MissingMode)?;
                let parsed_mode =
                    BenchmarkMode::parse(&value.to_string_lossy()).ok_or_else(|| {
                        BenchmarkRunnerConfigError::InvalidMode(value.to_string_lossy().into())
                    })?;
                if mode_seen {
                    return Err(BenchmarkRunnerConfigError::DuplicateMode);
                }
                mode_seen = true;
                mode = parsed_mode;
            } else if arg == "--host-probe-local-io" {
                host_probe_local_io.enabled = true;
            } else if arg == "--host-probe-temp-dir" {
                let path = args
                    .next()
                    .ok_or(BenchmarkRunnerConfigError::MissingHostProbeTempDir)?;
                if host_probe_local_io
                    .temp_dir
                    .replace(PathBuf::from(path))
                    .is_some()
                {
                    return Err(BenchmarkRunnerConfigError::DuplicateHostProbeTempDir);
                }
            } else if arg == "--host-probe-io-bytes" {
                let value = args
                    .next()
                    .ok_or(BenchmarkRunnerConfigError::MissingHostProbeIoBytes)?;
                let bytes = value.to_string_lossy().parse::<usize>().map_err(|_| {
                    BenchmarkRunnerConfigError::InvalidHostProbeIoBytes(
                        value.to_string_lossy().into(),
                    )
                })?;
                validate_host_probe_io_bytes(bytes)?;
                host_probe_local_io.bytes_per_repetition = bytes;
            } else if arg == "--host-probe-io-repetitions" {
                let value = args
                    .next()
                    .ok_or(BenchmarkRunnerConfigError::MissingHostProbeIoRepetitions)?;
                let repetitions = value.to_string_lossy().parse::<usize>().map_err(|_| {
                    BenchmarkRunnerConfigError::InvalidHostProbeIoRepetitions(
                        value.to_string_lossy().into(),
                    )
                })?;
                validate_host_probe_io_repetitions(repetitions)?;
                host_probe_local_io.repetitions = repetitions;
            } else if arg == "--provider-exec-temp-dir" {
                let path = args
                    .next()
                    .ok_or(BenchmarkRunnerConfigError::MissingProviderExecTempDir)?;
                if provider_exec
                    .temp_dir
                    .replace(PathBuf::from(path))
                    .is_some()
                {
                    return Err(BenchmarkRunnerConfigError::DuplicateProviderExecTempDir);
                }
            } else if arg == "--provider-exec-repetitions" {
                let value = args
                    .next()
                    .ok_or(BenchmarkRunnerConfigError::MissingProviderExecRepetitions)?;
                let repetitions = value.to_string_lossy().parse::<usize>().map_err(|_| {
                    BenchmarkRunnerConfigError::InvalidProviderExecRepetitions(
                        value.to_string_lossy().into(),
                    )
                })?;
                validate_provider_exec_repetitions(repetitions)?;
                provider_exec.repetitions = repetitions;
            } else if arg == "--provider-exec-storage-profile" {
                let value = args
                    .next()
                    .ok_or(BenchmarkRunnerConfigError::MissingProviderExecStorageProfile)?;
                if provider_exec_storage_profile_seen {
                    return Err(BenchmarkRunnerConfigError::DuplicateProviderExecStorageProfile);
                }
                provider_exec_storage_profile_seen = true;
                provider_exec.storage_profile = ProviderExecStorageProfile::parse(
                    &value.to_string_lossy(),
                )
                .ok_or_else(|| {
                    BenchmarkRunnerConfigError::InvalidProviderExecStorageProfile(
                        value.to_string_lossy().into_owned(),
                    )
                })?;
            } else if arg == "--provider-exec-default-case" {
                provider_exec.default_case = true;
            } else if arg == "--provider-exec-phase-aligned-workflow" {
                provider_exec.phase_aligned_workflow = true;
            } else if arg == "--provider-exec-workload" {
                let value = args
                    .next()
                    .ok_or(BenchmarkRunnerConfigError::MissingProviderExecWorkloadFilter)?;
                if provider_exec
                    .workload_filter
                    .replace(value.to_string_lossy().into_owned())
                    .is_some()
                {
                    return Err(BenchmarkRunnerConfigError::DuplicateProviderExecWorkloadFilter);
                }
            } else if arg == "--provider-exec-query" {
                let value = args
                    .next()
                    .ok_or(BenchmarkRunnerConfigError::MissingProviderExecQueryFilter)?;
                if provider_exec
                    .query_filter
                    .replace(value.to_string_lossy().into_owned())
                    .is_some()
                {
                    return Err(BenchmarkRunnerConfigError::DuplicateProviderExecQueryFilter);
                }
            } else if arg == "--provider-exec-backend" {
                let value = args
                    .next()
                    .ok_or(BenchmarkRunnerConfigError::MissingProviderExecBackendFilter)?;
                if provider_exec
                    .backend_filter
                    .replace(value.to_string_lossy().into_owned())
                    .is_some()
                {
                    return Err(BenchmarkRunnerConfigError::DuplicateProviderExecBackendFilter);
                }
            } else if arg == "--provider-exec-scheduling-profile" {
                let value = args.next().ok_or(
                    BenchmarkRunnerConfigError::MissingProviderExecSchedulingProfileFilter,
                )?;
                if provider_exec
                    .scheduling_profile_filter
                    .replace(value.to_string_lossy().into_owned())
                    .is_some()
                {
                    return Err(
                        BenchmarkRunnerConfigError::DuplicateProviderExecSchedulingProfileFilter,
                    );
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
        provider_exec.apply_default_case()?;

        Ok(Self {
            output_path,
            trace_output_path,
            mode,
            host_probe_local_io,
            provider_exec,
            seed,
            show_help,
        })
    }
}

impl Default for HostProbeLocalIoConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            temp_dir: None,
            bytes_per_repetition: HOST_PROBE_DEFAULT_LOCAL_IO_BYTES,
            repetitions: HOST_PROBE_DEFAULT_LOCAL_IO_REPETITIONS,
        }
    }
}

impl Default for ProviderExecConfig {
    fn default() -> Self {
        Self {
            repetitions: DEFAULT_PROVIDER_EXEC_REPETITIONS,
            temp_dir: None,
            storage_profile: ProviderExecStorageProfile::local(),
            default_case: false,
            phase_aligned_workflow: false,
            workload_filter: None,
            query_filter: None,
            backend_filter: None,
            scheduling_profile_filter: None,
        }
    }
}

impl ProviderExecConfig {
    fn apply_default_case(&mut self) -> Result<(), BenchmarkRunnerConfigError> {
        if !self.default_case {
            return Ok(());
        }
        if self.workload_filter.is_some() {
            return Err(BenchmarkRunnerConfigError::ProviderExecDefaultCaseConflict(
                "--provider-exec-workload",
            ));
        }
        if self.query_filter.is_some() {
            return Err(BenchmarkRunnerConfigError::ProviderExecDefaultCaseConflict(
                "--provider-exec-query",
            ));
        }
        if self.backend_filter.is_some() {
            return Err(BenchmarkRunnerConfigError::ProviderExecDefaultCaseConflict(
                "--provider-exec-backend",
            ));
        }
        if self.scheduling_profile_filter.is_some() {
            return Err(BenchmarkRunnerConfigError::ProviderExecDefaultCaseConflict(
                "--provider-exec-scheduling-profile",
            ));
        }

        self.workload_filter = Some(PROVIDER_EXEC_DEFAULT_CASE_WORKLOAD.to_owned());
        self.query_filter = Some(PROVIDER_EXEC_DEFAULT_CASE_QUERY.to_owned());
        Ok(())
    }
}

fn validate_host_probe_io_bytes(bytes: usize) -> Result<(), BenchmarkRunnerConfigError> {
    if bytes == 0 || bytes > HOST_PROBE_MAX_LOCAL_IO_BYTES {
        return Err(BenchmarkRunnerConfigError::HostProbeIoBytesOutOfRange(
            bytes,
        ));
    }

    Ok(())
}

fn validate_host_probe_io_repetitions(
    repetitions: usize,
) -> Result<(), BenchmarkRunnerConfigError> {
    if repetitions == 0 || repetitions > HOST_PROBE_MAX_LOCAL_IO_REPETITIONS {
        return Err(BenchmarkRunnerConfigError::HostProbeIoRepetitionsOutOfRange(repetitions));
    }

    Ok(())
}

fn validate_provider_exec_repetitions(
    repetitions: usize,
) -> Result<(), BenchmarkRunnerConfigError> {
    if repetitions == 0 || repetitions > MAX_PROVIDER_EXEC_REPETITIONS {
        return Err(BenchmarkRunnerConfigError::ProviderExecRepetitionsOutOfRange(repetitions));
    }

    Ok(())
}

impl BenchmarkMode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "synthetic" => Some(Self::Synthetic),
            "host-probe" | "host_probe" => Some(Self::HostProbe),
            "provider-exec" | "provider_exec" => Some(Self::ProviderExec),
            _ => None,
        }
    }

    fn as_csv_value(self) -> &'static str {
        match self {
            Self::Synthetic => "synthetic",
            Self::HostProbe => "host_probe",
            Self::ProviderExec => "provider_exec",
        }
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

impl ProviderExecStorageProfile {
    fn local() -> Self {
        Self {
            name: "local",
            open_latency_micros: 0,
            read_latency_micros: 0,
            bandwidth_bytes_per_second: None,
        }
    }

    fn s3_normal() -> Self {
        Self {
            name: "s3_normal",
            open_latency_micros: 8_000,
            read_latency_micros: 4_000,
            bandwidth_bytes_per_second: Some(125 * MIB),
        }
    }

    fn s3_high_latency() -> Self {
        Self {
            name: "s3_high_latency",
            open_latency_micros: 35_000,
            read_latency_micros: 20_000,
            bandwidth_bytes_per_second: Some(100 * MIB),
        }
    }

    fn s3_throttled() -> Self {
        Self {
            name: "s3_throttled",
            open_latency_micros: 15_000,
            read_latency_micros: 8_000,
            bandwidth_bytes_per_second: Some(32 * MIB),
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "local" => Some(Self::local()),
            "s3-normal" | "s3_normal" => Some(Self::s3_normal()),
            "s3-high-latency" | "s3_high_latency" => Some(Self::s3_high_latency()),
            "s3-throttled" | "s3_throttled" => Some(Self::s3_throttled()),
            _ => None,
        }
    }

    fn uses_delayed_http(self) -> bool {
        self.open_latency_micros != 0
            || self.read_latency_micros != 0
            || self.bandwidth_bytes_per_second.is_some()
    }
}

impl ProviderExecWorkloadCase {
    fn standard_cases() -> Result<Vec<Self>, Box<dyn Error>> {
        Ok(vec![
            Self::simple_orders("provider_many_small_files", 64, 128, &[]),
            Self::simple_orders("provider_few_larger_files", 4, 8_192, &[]),
            Self::simple_orders("provider_many_small_files_sparse_dv", 64, 128, &[1]),
            Self::simple_orders(
                "provider_few_larger_files_sparse_dv",
                4,
                8_192,
                &[1, 4_096, 8_191],
            ),
            Self::synthetic_partitioned_event_log()?,
            Self::synthetic_wide_event_export()?,
        ])
    }

    fn simple_orders(
        name: &'static str,
        file_count: usize,
        rows_per_file: usize,
        deleted_row_indexes_per_file: &'static [u64],
    ) -> Self {
        let file_specs = (0..file_count)
            .map(|file_index| ProviderExecFileSpec {
                path: format!("part-{file_index:05}.parquet"),
                rows: rows_per_file,
                partition_date: None,
            })
            .collect();

        Self {
            name,
            schema_kind: ProviderExecSchemaKind::SimpleOrders,
            file_specs,
            deleted_row_indexes_per_file,
        }
    }

    fn synthetic_partitioned_event_log() -> Result<Self, Box<dyn Error>> {
        let workload = SyntheticWorkloadCase::partitioned_event_log_target_shape()?;
        let file_specs = workload
            .file_set
            .files
            .iter()
            .map(|file| {
                let rows = usize::try_from(file.rows)?;
                Ok(ProviderExecFileSpec {
                    path: file.path.clone(),
                    rows,
                    partition_date: Some(file.partition_date),
                })
            })
            .collect::<Result<Vec<_>, std::num::TryFromIntError>>()?;

        Ok(Self {
            name: "provider_partitioned_event_log_12m",
            schema_kind: ProviderExecSchemaKind::SyntheticPartitionedEventLog,
            file_specs,
            deleted_row_indexes_per_file: &[],
        })
    }

    fn synthetic_wide_event_export() -> Result<Self, Box<dyn Error>> {
        let workload = SyntheticWorkloadCase::wide_event_export_target_shape()?;
        let file_specs = workload
            .file_set
            .files
            .iter()
            .map(|file| {
                let rows = usize::try_from(file.rows)?;
                Ok(ProviderExecFileSpec {
                    path: file.path.clone(),
                    rows,
                    partition_date: Some(file.partition_date),
                })
            })
            .collect::<Result<Vec<_>, std::num::TryFromIntError>>()?;

        Ok(Self {
            name: "provider_wide_event_export_13m",
            schema_kind: ProviderExecSchemaKind::SyntheticWideEventExport,
            file_specs,
            deleted_row_indexes_per_file: &[],
        })
    }

    fn file_count(&self) -> usize {
        self.file_specs.len()
    }

    fn row_count(&self) -> usize {
        self.file_specs
            .iter()
            .map(|file| file.rows)
            .fold(0_usize, usize::saturating_add)
    }

    fn has_deletion_vectors(&self) -> bool {
        !self.deleted_row_indexes_per_file.is_empty()
    }

    fn deletion_vector_deleted_rows(&self) -> usize {
        self.file_count()
            .saturating_mul(self.deleted_row_indexes_per_file.len())
    }

    fn query_cases(&self) -> [ProviderExecQueryCase; 3] {
        ProviderExecQueryCase::standard_cases_for_schema(self.schema_kind)
    }
}

impl ProviderExecQueryCase {
    fn standard_cases_for_schema(schema_kind: ProviderExecSchemaKind) -> [Self; 3] {
        match schema_kind {
            ProviderExecSchemaKind::SimpleOrders => [
                Self {
                    name: "project_id",
                    sql: "select id from orders",
                },
                Self {
                    name: "count_rows",
                    sql: "select count(id) from orders",
                },
                Self {
                    name: "filter_tail_ids",
                    sql: "select id from orders where id > 4096",
                },
            ],
            ProviderExecSchemaKind::SyntheticPartitionedEventLog => [
                Self {
                    name: "project_event_keys",
                    sql: "select primary_event_id, group_id, metric_x from orders",
                },
                Self {
                    name: "count_events",
                    sql: "select count(primary_event_id) from orders",
                },
                Self {
                    name: "filter_recent_events",
                    sql: "select primary_event_id, category_num from orders where event_year >= 2025",
                },
            ],
            ProviderExecSchemaKind::SyntheticWideEventExport => [
                Self {
                    name: "project_primary_export",
                    sql: wide_event_transform_sql!(
                        "SELECT * FROM export_ready WHERE source_group = 'primary'"
                    ),
                },
                Self {
                    name: "project_secondary_export",
                    sql: wide_event_transform_sql!(
                        "SELECT * FROM export_ready WHERE source_group = 'secondary'"
                    ),
                },
                Self {
                    name: "summary_export",
                    sql: wide_event_transform_sql!(
                        "SELECT \
                            COUNT(*) AS transformed_rows, \
                            SUM(CASE WHEN source_group = 'primary' THEN 1 ELSE 0 END) AS primary_rows, \
                            SUM(CASE WHEN source_group = 'secondary' THEN 1 ELSE 0 END) AS secondary_rows, \
                            COUNT(DISTINCT resolved_event_key) AS resolved_key_count, \
                            COUNT(DISTINCT group_id) AS group_key_count, \
                            COUNT(DISTINCT primary_event_id) AS primary_event_count, \
                            COUNT(DISTINCT secondary_event_id) AS secondary_event_count, \
                            COUNT(DISTINCT source_kind || '-' || quality_tier || '-' || local_date_key) AS segment_date_count \
                         FROM export_ready"
                    ),
                },
            ],
        }
    }
}

impl ProviderExecSchedulingProfile {
    fn default_execution_case() -> Self {
        let options = DeltaProviderScanExecutionOptions::default();
        Self {
            name: PROVIDER_EXEC_DEFAULT_CASE_SCHEDULING_PROFILE,
            scan_target_partitions: None,
            max_concurrent_file_reads_per_scan: options.max_concurrent_file_reads_per_scan,
            max_concurrent_file_reads_per_partition: options
                .max_concurrent_file_reads_per_partition,
            output_buffer_capacity_per_partition: options.output_buffer_capacity_per_partition,
            native_async_prefetch_file_count_per_partition: options
                .native_async_prefetch_file_count_per_partition,
            uses_default_execution_options: true,
        }
    }

    fn standard_cases(run_environment: BenchmarkRunEnvironment) -> Vec<Self> {
        let available_parallelism = run_environment.available_parallelism.unwrap_or(4).max(1);
        let mut profiles = vec![
            Self {
                name: "lazy_serial_buffer_1",
                scan_target_partitions: Some(1),
                max_concurrent_file_reads_per_scan: Some(1),
                max_concurrent_file_reads_per_partition: 1,
                output_buffer_capacity_per_partition: 1,
                native_async_prefetch_file_count_per_partition: 0,
                uses_default_execution_options: false,
            },
            Self {
                name: "lazy_parallel_buffer_1",
                scan_target_partitions: Some(4),
                max_concurrent_file_reads_per_scan: Some(4),
                max_concurrent_file_reads_per_partition: 1,
                output_buffer_capacity_per_partition: 1,
                native_async_prefetch_file_count_per_partition: 0,
                uses_default_execution_options: false,
            },
            Self {
                name: "lazy_parallel_buffer_4",
                scan_target_partitions: Some(4),
                max_concurrent_file_reads_per_scan: Some(4),
                max_concurrent_file_reads_per_partition: 1,
                output_buffer_capacity_per_partition: 4,
                native_async_prefetch_file_count_per_partition: 0,
                uses_default_execution_options: false,
            },
            Self {
                name: "prefetch_1_parallel_buffer_1",
                scan_target_partitions: Some(4),
                max_concurrent_file_reads_per_scan: Some(8),
                max_concurrent_file_reads_per_partition: 2,
                output_buffer_capacity_per_partition: 1,
                native_async_prefetch_file_count_per_partition: 1,
                uses_default_execution_options: false,
            },
            Self {
                name: "prefetch_2_parallel_buffer_1",
                scan_target_partitions: Some(4),
                max_concurrent_file_reads_per_scan: Some(12),
                max_concurrent_file_reads_per_partition: 3,
                output_buffer_capacity_per_partition: 1,
                native_async_prefetch_file_count_per_partition: 2,
                uses_default_execution_options: false,
            },
        ];
        profiles.extend(
            [
                ("prefetch_2_ap_target_scan_1x", 1_usize),
                ("prefetch_2_ap_target_scan_2x", 2),
                ("prefetch_2_ap_target_scan_3x", 3),
                ("prefetch_2_ap_target_scan_4x", 4),
            ]
            .into_iter()
            .map(|(name, scan_multiplier)| Self {
                name,
                scan_target_partitions: Some(available_parallelism),
                max_concurrent_file_reads_per_scan: Some(
                    available_parallelism.saturating_mul(scan_multiplier).max(1),
                ),
                max_concurrent_file_reads_per_partition: 3,
                output_buffer_capacity_per_partition: 1,
                native_async_prefetch_file_count_per_partition: 2,
                uses_default_execution_options: false,
            }),
        );
        profiles
    }
}

impl ProviderExecDeltaTable {
    fn create(
        temp_root: &std::path::Path,
        workload: &ProviderExecWorkloadCase,
        storage_profile: ProviderExecStorageProfile,
    ) -> Result<Self, Box<dyn Error>> {
        let path = temp_root.join(unique_benchmark_name(workload.name)?);
        let log_path = path.join("_delta_log");
        fs::create_dir_all(&log_path)?;

        let schema = provider_exec_arrow_schema(workload.schema_kind);
        let mut add_actions = Vec::with_capacity(workload.file_count());
        let mut data_file_bytes = 0_u64;
        let mut next_row_id = 1_usize;
        let deletion_vector = if workload.has_deletion_vectors() {
            Some(provider_exec_deletion_vector_fixture(
                workload.deleted_row_indexes_per_file,
            )?)
        } else {
            None
        };

        for file in &workload.file_specs {
            validate_provider_exec_deletion_vector(workload, file)?;
            let first_row_id = next_row_id;
            let batch = provider_exec_record_batch(
                workload.schema_kind,
                Arc::clone(&schema),
                file,
                first_row_id,
            )?;
            next_row_id = next_row_id.saturating_add(file.rows);
            let writer_properties = WriterProperties::builder()
                .set_max_row_group_row_count(Some(file.rows))
                .build();
            let data_path = path.join(&file.path);
            if let Some(parent) = data_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut writer = ArrowWriter::try_new(
                File::create(&data_path)?,
                Arc::clone(&schema),
                Some(writer_properties),
            )?;
            writer.write(&batch)?;
            writer.close()?;

            let file_size = fs::metadata(&data_path)?.len();
            data_file_bytes = data_file_bytes.saturating_add(file_size);
            add_actions.push(provider_exec_add_json(
                workload.schema_kind,
                file,
                file_size,
                first_row_id,
                deletion_vector.as_ref(),
            )?);
        }

        if let Some(deletion_vector) = &deletion_vector {
            fs::write(
                path.join(PROVIDER_EXEC_RELATIVE_DV_FILE),
                &deletion_vector.bytes,
            )?;
        }

        let protocol = if workload.has_deletion_vectors() {
            PROVIDER_EXEC_DELETION_VECTOR_PROTOCOL_JSON
        } else {
            PROVIDER_EXEC_PROTOCOL_JSON
        };

        fs::write(
            log_path.join("00000000000000000000.json"),
            format!(
                "{protocol}\n{}\n",
                provider_exec_metadata_json(workload.schema_kind)
            ),
        )?;
        fs::write(
            log_path.join("00000000000000000001.json"),
            format!("{}\n", add_actions.join("\n")),
        )?;
        let delayed_http_server = if storage_profile.uses_delayed_http() {
            Some(ProviderExecDelayedHttpServer::start(
                path.clone(),
                storage_profile,
            )?)
        } else {
            None
        };
        let table_uri = delayed_http_server
            .as_ref()
            .map(ProviderExecDelayedHttpServer::url)
            .unwrap_or_else(|| path.to_string_lossy().into_owned());
        let storage_options = if delayed_http_server.is_some() {
            provider_exec_delayed_http_storage_options()
        } else {
            DeltaStorageOptions::default()
        };

        Ok(Self {
            path,
            table_uri,
            storage_options,
            storage_profile,
            delayed_http_server,
            file_count: workload.file_count(),
            row_count: workload.row_count(),
            data_file_bytes,
            deletion_vector_file_count: if workload.has_deletion_vectors() {
                workload.file_count()
            } else {
                0
            },
            deletion_vector_deleted_rows: workload.deletion_vector_deleted_rows(),
            deletion_vector_deleted_rows_per_file: workload.deleted_row_indexes_per_file.len(),
        })
    }

    fn storage_profile_name(&self) -> &'static str {
        self.storage_profile.name
    }
}

fn provider_exec_delayed_http_storage_options() -> DeltaStorageOptions {
    BTreeMap::from([("allow_http".to_owned(), "true".to_owned())])
}

impl ProviderExecDelayedHttpServer {
    fn start(
        root: PathBuf,
        storage_profile: ProviderExecStorageProfile,
    ) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let url = format!("http://{}/", listener.local_addr()?);
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let handle = thread::spawn(move || {
            provider_exec_delayed_http_accept_loop(
                listener,
                root,
                storage_profile,
                worker_shutdown,
            );
        });

        Ok(Self {
            shutdown,
            handle: Some(handle),
            url,
        })
    }

    fn url(&self) -> String {
        self.url.clone()
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

fn provider_exec_delayed_http_accept_loop(
    listener: TcpListener,
    root: PathBuf,
    storage_profile: ProviderExecStorageProfile,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let root = root.clone();
                let shutdown = Arc::clone(&shutdown);
                let _ = thread::Builder::new()
                    .name("delta-provider-exec-http".to_owned())
                    .spawn(move || {
                        let _ = provider_exec_delayed_http_handle_connection(
                            stream,
                            &root,
                            storage_profile,
                            &shutdown,
                        );
                    });
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_error) => break,
        }
    }
}

fn provider_exec_delayed_http_handle_connection(
    mut stream: TcpStream,
    root: &Path,
    storage_profile: ProviderExecStorageProfile,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    let request = match provider_exec_delayed_http_read_request(&stream)? {
        Some(request) => request,
        None => return Ok(()),
    };
    if shutdown.load(Ordering::Relaxed) {
        let _ = stream.shutdown(Shutdown::Both);
        return Ok(());
    }

    provider_exec_delayed_http_sleep(storage_profile.open_latency_micros);
    match request.method.as_str() {
        "PROPFIND" => provider_exec_delayed_http_propfind(&mut stream, root, &request),
        "HEAD" => provider_exec_delayed_http_file_response(
            &mut stream,
            root,
            &request.path,
            request.headers.get("range").map(String::as_str),
            true,
            storage_profile,
        ),
        "GET" => provider_exec_delayed_http_file_response(
            &mut stream,
            root,
            &request.path,
            request.headers.get("range").map(String::as_str),
            false,
            storage_profile,
        ),
        _ => provider_exec_delayed_http_write_response(
            stream,
            405,
            "Method Not Allowed",
            &[("Content-Length", "0".to_owned())],
            &[],
        ),
    }
}

struct ProviderExecDelayedHttpRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
}

fn provider_exec_delayed_http_read_request(
    stream: &TcpStream,
) -> io::Result<Option<ProviderExecDelayedHttpRequest>> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(None);
    }
    let mut parts = request_line.split_whitespace();
    let Some(method) = parts.next() else {
        return Ok(None);
    };
    let Some(target) = parts.next() else {
        return Ok(None);
    };
    let mut headers = BTreeMap::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }

    Ok(Some(ProviderExecDelayedHttpRequest {
        method: method.to_owned(),
        path: provider_exec_delayed_http_request_path(target)?,
        headers,
    }))
}

fn provider_exec_delayed_http_request_path(target: &str) -> io::Result<String> {
    let path = target.split('?').next().unwrap_or_default();
    let path = path.trim_start_matches('/');
    let decoded = percent_decode_ascii(path)?;
    if decoded
        .split('/')
        .any(|component| component == ".." || component.contains('\\'))
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid delayed HTTP path",
        ));
    }

    Ok(decoded)
}

fn provider_exec_delayed_http_propfind(
    stream: &mut TcpStream,
    root: &Path,
    request: &ProviderExecDelayedHttpRequest,
) -> io::Result<()> {
    let requested = root.join(&request.path);
    if !requested.exists() {
        return provider_exec_delayed_http_write_response(
            stream,
            404,
            "Not Found",
            &[("Content-Length", "0".to_owned())],
            &[],
        );
    }
    let depth = request
        .headers
        .get("depth")
        .map(String::as_str)
        .unwrap_or("infinity");
    let mut entries = Vec::new();
    provider_exec_delayed_http_collect_propfind_entries(
        root,
        &request.path,
        &requested,
        depth != "0",
        &mut entries,
    )?;
    let body = provider_exec_delayed_http_multistatus_xml(&entries)?;

    provider_exec_delayed_http_write_response(
        stream,
        207,
        "Multi-Status",
        &[
            ("Content-Type", "application/xml; charset=utf-8".to_owned()),
            ("Content-Length", body.len().to_string()),
        ],
        body.as_bytes(),
    )
}

struct ProviderExecDelayedHttpPropfindEntry {
    href: String,
    size: u64,
    is_dir: bool,
    modified: SystemTime,
}

fn provider_exec_delayed_http_collect_propfind_entries(
    root: &Path,
    relative_path: &str,
    path: &Path,
    recursive: bool,
    entries: &mut Vec<ProviderExecDelayedHttpPropfindEntry>,
) -> io::Result<()> {
    let metadata = fs::metadata(path)?;
    entries.push(ProviderExecDelayedHttpPropfindEntry {
        href: format!("/{}", relative_path.trim_start_matches('/')),
        size: if metadata.is_file() {
            metadata.len()
        } else {
            0
        },
        is_dir: metadata.is_dir(),
        modified: metadata.modified().unwrap_or(UNIX_EPOCH),
    });

    if metadata.is_dir() && recursive {
        let mut children = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        children.sort_by_key(|entry| entry.path());
        for child in children {
            let child_path = child.path();
            let child_relative_path = child_path
                .strip_prefix(root)
                .map_err(|_| io::Error::other("delayed HTTP path escaped root"))?
                .to_string_lossy()
                .replace('\\', "/");
            provider_exec_delayed_http_collect_propfind_entries(
                root,
                &child_relative_path,
                &child_path,
                recursive,
                entries,
            )?;
        }
    }

    Ok(())
}

fn provider_exec_delayed_http_multistatus_xml(
    entries: &[ProviderExecDelayedHttpPropfindEntry],
) -> io::Result<String> {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?><multistatus>"#);
    for entry in entries {
        let href = xml_escape(&entry.href);
        let modified = provider_exec_http_date(entry.modified);
        let resource_type = if entry.is_dir {
            "<resourcetype><collection/></resourcetype>"
        } else {
            "<resourcetype/>"
        };
        xml.push_str(&format!(
            "<response><href>{href}</href><propstat><prop><getlastmodified>{modified}</getlastmodified><getcontentlength>{}</getcontentlength>{resource_type}<getetag>\"{}\"</getetag></prop><status>HTTP/1.1 200 OK</status></propstat></response>",
            entry.size,
            provider_exec_etag(entry.size, entry.modified)?
        ));
    }
    xml.push_str("</multistatus>");
    Ok(xml)
}

fn provider_exec_delayed_http_file_response(
    stream: &mut TcpStream,
    root: &Path,
    request_path: &str,
    range_header: Option<&str>,
    head_only: bool,
    storage_profile: ProviderExecStorageProfile,
) -> io::Result<()> {
    let path = root.join(request_path);
    let metadata = match fs::metadata(&path) {
        Ok(metadata) if metadata.is_file() => metadata,
        _ => {
            return provider_exec_delayed_http_write_response(
                stream,
                404,
                "Not Found",
                &[("Content-Length", "0".to_owned())],
                &[],
            );
        }
    };
    let size = metadata.len();
    let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
    let range = provider_exec_delayed_http_parse_range(range_header, size)?;
    let (status_code, status_text, start, end) = match range {
        Some((start, end)) => (206, "Partial Content", start, end),
        None => (200, "OK", 0, size),
    };
    let content_len = end.saturating_sub(start);
    let mut headers = vec![
        ("Accept-Ranges", "bytes".to_owned()),
        ("Content-Length", content_len.to_string()),
        ("Last-Modified", provider_exec_http_date(modified)),
        (
            "ETag",
            format!("\"{}\"", provider_exec_etag(size, modified)?),
        ),
    ];
    if status_code == 206 {
        headers.push((
            "Content-Range",
            format!("bytes {start}-{}/{}", end.saturating_sub(1), size),
        ));
    }

    provider_exec_delayed_http_sleep(storage_profile.read_latency_micros);
    provider_exec_delayed_http_sleep(provider_exec_transfer_delay_micros(
        content_len,
        storage_profile.bandwidth_bytes_per_second,
    ));

    if head_only {
        return provider_exec_delayed_http_write_response(
            stream,
            status_code,
            status_text,
            &headers,
            &[],
        );
    }

    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut body = Vec::new();
    file.take(content_len).read_to_end(&mut body)?;
    provider_exec_delayed_http_write_response(stream, status_code, status_text, &headers, &body)
}

fn provider_exec_delayed_http_parse_range(
    range_header: Option<&str>,
    size: u64,
) -> io::Result<Option<(u64, u64)>> {
    let Some(range_header) = range_header else {
        return Ok(None);
    };
    let Some(range) = range_header.strip_prefix("bytes=") else {
        return Ok(None);
    };
    if let Some(suffix) = range.strip_prefix('-') {
        let suffix_len = suffix.parse::<u64>().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid HTTP suffix range")
        })?;
        let start = size.saturating_sub(suffix_len);
        return Ok(Some((start, size)));
    }
    let (start, end) = range.split_once('-').unwrap_or((range, ""));
    let start = start
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid HTTP range start"))?;
    let end = if end.is_empty() {
        size
    } else {
        end.parse::<u64>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid HTTP range end"))?
            .saturating_add(1)
            .min(size)
    };
    if start > end || end > size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid HTTP range bounds",
        ));
    }

    Ok(Some((start, end)))
}

fn provider_exec_delayed_http_write_response(
    mut stream: impl Write,
    status_code: u16,
    status_text: &str,
    headers: &[(&str, String)],
    body: &[u8],
) -> io::Result<()> {
    write!(stream, "HTTP/1.1 {status_code} {status_text}\r\n")?;
    write!(stream, "Connection: close\r\n")?;
    for (key, value) in headers {
        write!(stream, "{key}: {value}\r\n")?;
    }
    write!(stream, "\r\n")?;
    stream.write_all(body)
}

fn provider_exec_delayed_http_sleep(micros: u64) {
    if micros != 0 {
        thread::sleep(Duration::from_micros(micros));
    }
}

fn provider_exec_transfer_delay_micros(bytes: u64, bandwidth_bytes_per_second: Option<u64>) -> u64 {
    let Some(bandwidth) = bandwidth_bytes_per_second else {
        return 0;
    };
    if bandwidth == 0 {
        return 0;
    }

    u128_to_u64_saturating(u128::from(bytes) * 1_000_000 / u128::from(bandwidth))
}

fn provider_exec_http_date(time: SystemTime) -> String {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    let Some(datetime) = chrono::DateTime::from_timestamp(i64::try_from(seconds).unwrap_or(0), 0)
    else {
        return "Thu, 01 Jan 1970 00:00:00 GMT".to_owned();
    };

    datetime.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn provider_exec_etag(size: u64, modified: SystemTime) -> io::Result<String> {
    let modified_nanos = modified
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    Ok(format!("{size:x}-{modified_nanos:x}"))
}

fn percent_decode_ascii(value: &str) -> io::Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid percent encoding",
                ));
            }
            let high = hex_value(bytes[index + 1])?;
            let low = hex_value(bytes[index + 2])?;
            decoded.push(high * 16 + low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }

    String::from_utf8(decoded).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
}

fn hex_value(byte: u8) -> io::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid hex digit",
        )),
    }
}

fn xml_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            character => escaped.push(character),
        }
    }

    escaped
}

async fn run_provider_exec_benchmark_case(
    table: &ProviderExecDeltaTable,
    _workload: &ProviderExecWorkloadCase,
    query: ProviderExecQueryCase,
    backend: DeltaProviderReaderBackend,
    scheduling_profile: ProviderExecSchedulingProfile,
    config: &ProviderExecConfig,
) -> Result<ProviderExecSummary, Box<dyn Error>> {
    let mut measurements = Vec::with_capacity(config.repetitions);
    for repetition_index in 0..config.repetitions {
        measurements.push(
            run_provider_exec_once(table, query, backend, scheduling_profile, repetition_index)
                .await?,
        );
    }

    Ok(provider_exec_summary(&measurements))
}

async fn run_provider_exec_write_workflow_case(
    table: &ProviderExecDeltaTable,
    workload: &ProviderExecWorkloadCase,
    backend: DeltaProviderReaderBackend,
    scheduling_profile: ProviderExecSchedulingProfile,
    config: &ProviderExecConfig,
) -> Result<ProviderExecSummary, Box<dyn Error>> {
    let mut measurements = Vec::with_capacity(config.repetitions);
    for repetition_index in 0..config.repetitions {
        measurements.push(
            run_provider_exec_write_workflow_once(
                table,
                workload,
                backend,
                scheduling_profile,
                repetition_index,
            )
            .await?,
        );
    }

    Ok(provider_exec_summary(&measurements))
}

async fn run_provider_exec_write_workflow_once(
    table: &ProviderExecDeltaTable,
    workload: &ProviderExecWorkloadCase,
    backend: DeltaProviderReaderBackend,
    scheduling_profile: ProviderExecSchedulingProfile,
    _repetition_index: usize,
) -> Result<ProviderExecRunMeasurement, Box<dyn Error>> {
    let query_started = Instant::now();
    let execution_options = provider_exec_scan_execution_options(backend, scheduling_profile)?;
    let connection = MssqlConnectionConfig::new(
        "server=benchmark.invalid;database=delta_funnel_benchmark;user=benchmark;password=benchmark",
    )?
    .with_display_label("provider-exec stream benchmark");
    let query_options = QueryOptions {
        target_partitions: scheduling_profile.scan_target_partitions,
        output_batch_size: None,
    };
    let mut session = DeltaFunnelSession::new(
        SessionOptions::new()
            .with_query_options(query_options)
            .with_provider_scan_options(execution_options)
            .with_mssql_schema_options(provider_exec_write_workflow_schema_options())
            .with_default_mssql_connection(connection),
    )?;
    session.delta_lake(
        DeltaSourceConfig::new("orders", table.table_uri.clone())
            .with_storage_options(table.storage_options.clone()),
    )?;

    let mut requests = Vec::new();
    for query in workload.query_cases() {
        let lazy_table = session.table_from_sql(query.sql).await?;
        let target = provider_exec_write_workflow_target(query.name)?;
        requests.push(OutputWritePlan::new(lazy_table, target));
    }

    let process_peak_rss_before_bytes = process_peak_rss_bytes();
    let report = session
        .write_all_for_stream_benchmark(
            &requests,
            WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
        )
        .await?;
    if !report.workflow().all_succeeded() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "provider-exec stream benchmark workflow failed: {}",
                report.workflow()
            ),
        )
        .into());
    }

    let produced_rows = report
        .outputs()
        .iter()
        .map(|output| output.output_row_count().exact_value().unwrap_or(0))
        .map(u64_to_usize_saturating)
        .fold(0_usize, usize::saturating_add);
    let produced_batches = report
        .outputs()
        .iter()
        .map(|output| output.batch_shaping().output_batches())
        .map(u64_to_usize_saturating)
        .fold(0_usize, usize::saturating_add);
    let total_micros = u128_to_u64_saturating(query_started.elapsed().as_micros()).max(1);
    let process_peak_rss_bytes = process_peak_rss_bytes();
    let process_peak_rss_delta_bytes = match (process_peak_rss_before_bytes, process_peak_rss_bytes)
    {
        (Some(before), Some(after)) => Some(after.saturating_sub(before)),
        _ => None,
    };
    let provider_stats = report
        .sources()
        .iter()
        .filter_map(|source| source.provider_read_stats().cloned())
        .collect::<Vec<_>>();
    let batch_latency_micros = report
        .outputs()
        .iter()
        .filter_map(|output| phase_elapsed_micros(output.phase_timings(), "poll_batch_stream"))
        .collect::<Vec<_>>();
    let planning_micros =
        phase_elapsed_micros(report.phase_timings(), "output_planning").unwrap_or(0);
    let time_to_first_batch_micros = report
        .outputs()
        .iter()
        .find_map(|output| phase_elapsed_micros(output.phase_timings(), "output_stream_setup"))
        .unwrap_or(0);
    let source_rows_per_second = u128_to_u64_saturating(
        (table.row_count as u128).saturating_mul(1_000_000) / u128::from(total_micros),
    );

    Ok(ProviderExecRunMeasurement {
        planning_micros,
        time_to_first_batch_micros,
        total_micros,
        source_rows_per_second,
        produced_rows,
        produced_batches,
        process_peak_rss_bytes,
        process_peak_rss_delta_bytes,
        batch_latency_micros,
        read_stats: provider_exec_read_stats_measurement(&provider_stats),
    })
}

fn provider_exec_write_workflow_target(
    query_name: &str,
) -> Result<MssqlOutputTarget, Box<dyn Error>> {
    let table_name = format!("synthetic_{query_name}");
    let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", table_name)?)
        .with_load_mode(LoadMode::Replace);
    Ok(MssqlOutputTarget::new(
        query_name,
        target_config,
        RunMode::Execute,
    ))
}

fn provider_exec_write_workflow_schema_options() -> MssqlSchemaPlanOptions {
    MssqlSchemaPlanOptions {
        timezone_policy: MssqlTimezonePolicy::NormalizeUtcDateTime2,
        ..MssqlSchemaPlanOptions::default()
    }
}

fn phase_elapsed_micros(phase_timings: &[PhaseTimingReport], phase_name: &str) -> Option<u64> {
    phase_timings
        .iter()
        .find(|timing| timing.phase_name() == phase_name)
        .and_then(PhaseTimingReport::elapsed_micros)
}

fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

async fn run_provider_exec_once(
    table: &ProviderExecDeltaTable,
    query: ProviderExecQueryCase,
    backend: DeltaProviderReaderBackend,
    scheduling_profile: ProviderExecSchedulingProfile,
    repetition_index: usize,
) -> Result<ProviderExecRunMeasurement, Box<dyn Error>> {
    let ctx = SessionContext::new();
    let source = load_delta_source_with_tracing(
        DeltaSourceConfig::new("orders", table.table_uri.clone())
            .with_storage_options(table.storage_options.clone()),
    )?;
    let protocol = preflight_delta_protocol_with_tracing(&source)?;
    let execution_options = provider_exec_scan_execution_options(backend, scheduling_profile)?;
    register_delta_sources_with_scan_execution_options(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol,
            scan_target_partitions: scheduling_profile.scan_target_partitions,
        }],
        execution_options,
    )?;
    let trace_context = ProviderExecTraceContext {
        table,
        query,
        backend,
        scheduling_profile,
        repetition_index,
    };

    let query_started = Instant::now();
    let planning_started = Instant::now();
    provider_exec_query_planning_started(trace_context);
    let dataframe = match ctx.sql(query.sql).await {
        Ok(dataframe) => dataframe,
        Err(error) => {
            provider_exec_query_planning_failed(trace_context, &error);
            return Err(error.into());
        }
    };
    let physical_plan = match dataframe.create_physical_plan().await {
        Ok(physical_plan) => physical_plan,
        Err(error) => {
            provider_exec_query_planning_failed(trace_context, &error);
            return Err(error.into());
        }
    };
    let stats_plan = Arc::clone(&physical_plan);
    let planning_micros = u128_to_u64_saturating(planning_started.elapsed().as_micros());
    provider_exec_query_planning_completed(trace_context, planning_micros);
    let process_peak_rss_before_bytes = process_peak_rss_bytes();
    let execution_started = Instant::now();
    provider_exec_query_execution_started(trace_context);
    let mut produced_rows = 0_usize;
    let mut produced_batches = 0_usize;
    let mut stream = match datafusion::physical_plan::execute_stream(physical_plan, ctx.task_ctx())
    {
        Ok(stream) => stream,
        Err(error) => {
            provider_exec_query_execution_failed(
                trace_context,
                produced_rows,
                produced_batches,
                &error,
            );
            return Err(error.into());
        }
    };
    let mut first_batch_micros = None;
    let mut batch_latency_micros = Vec::new();
    let mut previous_batch_at = execution_started;

    while let Some(batch) = stream.next().await {
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => {
                provider_exec_query_execution_failed(
                    trace_context,
                    produced_rows,
                    produced_batches,
                    &error,
                );
                return Err(error.into());
            }
        };
        let now = Instant::now();
        if first_batch_micros.is_none() {
            let elapsed_micros =
                u128_to_u64_saturating(now.duration_since(execution_started).as_micros());
            first_batch_micros = Some(elapsed_micros);
            provider_exec_query_execution_first_batch(trace_context, elapsed_micros);
        }
        batch_latency_micros.push(u128_to_u64_saturating(
            now.duration_since(previous_batch_at).as_micros(),
        ));
        previous_batch_at = now;
        produced_rows = produced_rows.saturating_add(batch.num_rows());
        produced_batches = produced_batches.saturating_add(1);
    }

    let total_micros = u128_to_u64_saturating(query_started.elapsed().as_micros()).max(1);
    provider_exec_query_execution_completed(
        trace_context,
        produced_rows,
        produced_batches,
        total_micros,
    );
    let process_peak_rss_bytes = process_peak_rss_bytes();
    let process_peak_rss_delta_bytes = match (process_peak_rss_before_bytes, process_peak_rss_bytes)
    {
        (Some(before), Some(after)) => Some(after.saturating_sub(before)),
        _ => None,
    };
    provider_exec_stats_collect_started(trace_context);
    let read_stats = provider_exec_read_stats_measurement(&collect_delta_provider_read_stats(
        stats_plan.as_ref(),
    ));
    provider_exec_stats_collect_completed(trace_context, read_stats.scan_count);
    let source_rows_per_second = u128_to_u64_saturating(
        (table.row_count as u128).saturating_mul(1_000_000) / u128::from(total_micros),
    );

    Ok(ProviderExecRunMeasurement {
        planning_micros,
        time_to_first_batch_micros: first_batch_micros.unwrap_or(0),
        total_micros,
        source_rows_per_second,
        produced_rows,
        produced_batches,
        process_peak_rss_bytes,
        process_peak_rss_delta_bytes,
        batch_latency_micros,
        read_stats,
    })
}

fn provider_exec_scan_execution_options(
    backend: DeltaProviderReaderBackend,
    scheduling_profile: ProviderExecSchedulingProfile,
) -> Result<DeltaProviderScanExecutionOptions, Box<dyn Error>> {
    if scheduling_profile.uses_default_execution_options {
        return Ok(DeltaProviderScanExecutionOptions::default());
    }

    let max_concurrent_file_reads_per_scan = scheduling_profile
        .max_concurrent_file_reads_per_scan
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "provider-exec scheduling profile is missing scan-wide capacity",
            )
        })?;
    Ok(
        DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            backend,
            max_concurrent_file_reads_per_scan,
            scheduling_profile.max_concurrent_file_reads_per_partition,
        )?
        .with_output_buffer_capacity_per_partition(
            scheduling_profile.output_buffer_capacity_per_partition,
        )?
        .with_native_async_prefetch_file_count_per_partition(
            scheduling_profile.native_async_prefetch_file_count_per_partition,
        )?,
    )
}

fn provider_exec_summary(measurements: &[ProviderExecRunMeasurement]) -> ProviderExecSummary {
    let produced_rows = measurements
        .iter()
        .map(|measurement| measurement.produced_rows)
        .max()
        .unwrap_or(0);
    let produced_batches = measurements
        .iter()
        .map(|measurement| measurement.produced_batches)
        .max()
        .unwrap_or(0);
    let total_micros = measurements
        .iter()
        .map(|measurement| measurement.total_micros)
        .collect::<Vec<_>>();
    let batch_latency_micros = measurements
        .iter()
        .flat_map(|measurement| measurement.batch_latency_micros.iter().copied())
        .collect::<Vec<_>>();

    ProviderExecSummary {
        repetitions: measurements.len(),
        produced_rows,
        produced_batches,
        planning_micros: percentile_summary(
            &measurements
                .iter()
                .map(|measurement| measurement.planning_micros)
                .collect::<Vec<_>>(),
        ),
        time_to_first_batch_micros: percentile_summary(
            &measurements
                .iter()
                .map(|measurement| measurement.time_to_first_batch_micros)
                .collect::<Vec<_>>(),
        ),
        total_micros: percentile_summary(&total_micros),
        source_rows_per_second: percentile_summary(
            &measurements
                .iter()
                .map(|measurement| measurement.source_rows_per_second)
                .collect::<Vec<_>>(),
        ),
        batch_latency_micros: percentile_summary(&batch_latency_micros),
        process_peak_rss_bytes: measurements
            .iter()
            .filter_map(|measurement| measurement.process_peak_rss_bytes)
            .max(),
        process_peak_rss_delta_bytes: measurements
            .iter()
            .filter_map(|measurement| measurement.process_peak_rss_delta_bytes)
            .max(),
        min_total_micros: total_micros.iter().copied().min().unwrap_or(0),
        max_total_micros: total_micros.iter().copied().max().unwrap_or(0),
        read_stats: provider_exec_read_stats_summary(measurements),
    }
}

fn provider_exec_read_stats_measurement(
    snapshots: &[DeltaProviderReadStatsSnapshot],
) -> ProviderExecReadStatsMeasurement {
    ProviderExecReadStatsMeasurement {
        scan_count: snapshots.len(),
        scan_metadata_exhausted: provider_exec_scan_metadata_exhausted(
            snapshots
                .iter()
                .map(|snapshot| snapshot.scan_metadata_exhausted),
        ),
        scan_partitions_planned: snapshots
            .iter()
            .map(|snapshot| snapshot.scan_partitions_planned)
            .sum(),
        files_planned: snapshots
            .iter()
            .map(|snapshot| snapshot.files_planned)
            .sum(),
        estimated_rows: sum_provider_exec_read_stats_optional(
            snapshots.iter().map(|snapshot| snapshot.estimated_rows),
        ),
        estimated_bytes: sum_provider_exec_read_stats_optional(
            snapshots.iter().map(|snapshot| snapshot.estimated_bytes),
        ),
        scan_partitions_started: snapshots
            .iter()
            .map(|snapshot| snapshot.scan_partitions_started)
            .sum(),
        scan_partitions_completed: snapshots
            .iter()
            .map(|snapshot| snapshot.scan_partitions_completed)
            .sum(),
        files_started: snapshots
            .iter()
            .map(|snapshot| snapshot.files_started)
            .sum(),
        files_completed: snapshots
            .iter()
            .map(|snapshot| snapshot.files_completed)
            .sum(),
        dynamic_partition_files_pruned: snapshots
            .iter()
            .map(|snapshot| snapshot.dynamic_partition_files_pruned)
            .sum(),
        dynamic_partition_files_kept: snapshots
            .iter()
            .map(|snapshot| snapshot.dynamic_partition_files_kept)
            .sum(),
        dynamic_filters_received: snapshots
            .iter()
            .map(|snapshot| snapshot.dynamic_filters_received)
            .sum(),
        dynamic_filters_accepted: snapshots
            .iter()
            .map(|snapshot| snapshot.dynamic_filters_accepted)
            .sum(),
        dynamic_filters_unsupported: snapshots
            .iter()
            .map(|snapshot| snapshot.dynamic_filters_unsupported)
            .sum(),
        dynamic_filter_snapshots: snapshots
            .iter()
            .map(|snapshot| snapshot.dynamic_filter_snapshots)
            .sum(),
        dynamic_partition_files_not_pruned_missing_metadata: snapshots
            .iter()
            .map(|snapshot| snapshot.dynamic_partition_files_not_pruned_missing_metadata)
            .sum(),
        dynamic_partition_files_not_pruned_unsupported_expression: snapshots
            .iter()
            .map(|snapshot| snapshot.dynamic_partition_files_not_pruned_unsupported_expression)
            .sum(),
        batches_produced: snapshots
            .iter()
            .map(|snapshot| snapshot.batches_produced)
            .sum(),
        rows_produced: snapshots
            .iter()
            .map(|snapshot| snapshot.rows_produced)
            .sum(),
        deletion_vector_payloads_loaded: snapshots
            .iter()
            .map(|snapshot| snapshot.deletion_vector_payloads_loaded)
            .sum(),
        deletion_vectors_applied: snapshots
            .iter()
            .map(|snapshot| snapshot.deletion_vectors_applied)
            .sum(),
        deletion_vector_rows_deleted: snapshots
            .iter()
            .map(|snapshot| snapshot.deletion_vector_rows_deleted)
            .sum(),
        deletion_vector_failures: snapshots
            .iter()
            .map(|snapshot| snapshot.deletion_vector_failures)
            .sum(),
        deletion_vector_rejections: snapshots
            .iter()
            .map(|snapshot| snapshot.deletion_vector_rejections)
            .sum(),
    }
}

fn provider_exec_read_stats_summary(
    measurements: &[ProviderExecRunMeasurement],
) -> ProviderExecReadStatsSummary {
    let stats = measurements
        .iter()
        .map(|measurement| &measurement.read_stats)
        .collect::<Vec<_>>();

    ProviderExecReadStatsSummary {
        scan_count: stats
            .iter()
            .map(|stats| stats.scan_count)
            .max()
            .unwrap_or(0),
        scan_metadata_exhausted: provider_exec_scan_metadata_exhausted_from_measurements(
            stats.iter().map(|stats| stats.scan_metadata_exhausted),
        ),
        scan_partitions_planned: stats
            .iter()
            .map(|stats| stats.scan_partitions_planned)
            .max()
            .unwrap_or(0),
        files_planned: stats
            .iter()
            .map(|stats| stats.files_planned)
            .max()
            .unwrap_or(0),
        estimated_rows: provider_exec_read_stats_optional_max(
            stats.iter().map(|stats| stats.estimated_rows),
        ),
        estimated_bytes: provider_exec_read_stats_optional_max(
            stats.iter().map(|stats| stats.estimated_bytes),
        ),
        scan_partitions_started: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.scan_partitions_started
        }),
        scan_partitions_completed: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.scan_partitions_completed
        }),
        files_started: provider_exec_read_stats_counter_p50(&stats, |stats| stats.files_started),
        files_completed: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.files_completed
        }),
        dynamic_partition_files_pruned: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.dynamic_partition_files_pruned
        }),
        dynamic_partition_files_kept: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.dynamic_partition_files_kept
        }),
        dynamic_filters_received: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.dynamic_filters_received
        }),
        dynamic_filters_accepted: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.dynamic_filters_accepted
        }),
        dynamic_filters_unsupported: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.dynamic_filters_unsupported
        }),
        dynamic_filter_snapshots: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.dynamic_filter_snapshots
        }),
        dynamic_partition_files_not_pruned_missing_metadata: provider_exec_read_stats_counter_p50(
            &stats,
            |stats| stats.dynamic_partition_files_not_pruned_missing_metadata,
        ),
        dynamic_partition_files_not_pruned_unsupported_expression:
            provider_exec_read_stats_counter_p50(&stats, |stats| {
                stats.dynamic_partition_files_not_pruned_unsupported_expression
            }),
        batches_produced: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.batches_produced
        }),
        rows_produced: provider_exec_read_stats_counter_p50(&stats, |stats| stats.rows_produced),
        deletion_vector_payloads_loaded: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.deletion_vector_payloads_loaded
        }),
        deletion_vectors_applied: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.deletion_vectors_applied
        }),
        deletion_vector_rows_deleted: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.deletion_vector_rows_deleted
        }),
        deletion_vector_failures: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.deletion_vector_failures
        }),
        deletion_vector_rejections: provider_exec_read_stats_counter_p50(&stats, |stats| {
            stats.deletion_vector_rejections
        }),
    }
}

fn provider_exec_read_stats_counter_p50(
    stats: &[&ProviderExecReadStatsMeasurement],
    value: impl Fn(&ProviderExecReadStatsMeasurement) -> u64,
) -> u64 {
    percentile(
        &stats.iter().map(|stats| value(stats)).collect::<Vec<_>>(),
        50,
    )
}

fn provider_exec_scan_metadata_exhausted(
    values: impl IntoIterator<Item = Option<bool>>,
) -> ProviderExecScanMetadataExhausted {
    let mut saw_true = false;
    let mut saw_false = false;
    let mut saw_unknown = false;

    for value in values {
        match value {
            Some(true) => saw_true = true,
            Some(false) => saw_false = true,
            None => saw_unknown = true,
        }
    }

    if (saw_unknown && (saw_true || saw_false)) || (saw_true && saw_false) {
        ProviderExecScanMetadataExhausted::Mixed
    } else if saw_true {
        ProviderExecScanMetadataExhausted::True
    } else if saw_false {
        ProviderExecScanMetadataExhausted::False
    } else {
        ProviderExecScanMetadataExhausted::Unknown
    }
}

fn provider_exec_scan_metadata_exhausted_from_measurements(
    values: impl IntoIterator<Item = ProviderExecScanMetadataExhausted>,
) -> ProviderExecScanMetadataExhausted {
    let mut saw_true = false;
    let mut saw_false = false;
    let mut saw_unknown = false;
    let mut saw_mixed = false;

    for value in values {
        match value {
            ProviderExecScanMetadataExhausted::True => saw_true = true,
            ProviderExecScanMetadataExhausted::False => saw_false = true,
            ProviderExecScanMetadataExhausted::Unknown => saw_unknown = true,
            ProviderExecScanMetadataExhausted::Mixed => saw_mixed = true,
        }
    }

    if saw_mixed || (saw_unknown && (saw_true || saw_false)) || (saw_true && saw_false) {
        ProviderExecScanMetadataExhausted::Mixed
    } else if saw_true {
        ProviderExecScanMetadataExhausted::True
    } else if saw_false {
        ProviderExecScanMetadataExhausted::False
    } else {
        ProviderExecScanMetadataExhausted::Unknown
    }
}

fn sum_provider_exec_read_stats_optional(
    values: impl IntoIterator<Item = Option<u64>>,
) -> Option<u64> {
    let mut sum = 0_u64;
    for value in values {
        sum = sum.checked_add(value?)?;
    }
    Some(sum)
}

fn provider_exec_read_stats_optional_max(
    values: impl IntoIterator<Item = Option<u64>>,
) -> Option<u64> {
    values.into_iter().flatten().max()
}

fn provider_exec_scan_metadata_exhausted_value(value: ProviderExecScanMetadataExhausted) -> String {
    match value {
        ProviderExecScanMetadataExhausted::True => "true",
        ProviderExecScanMetadataExhausted::False => "false",
        ProviderExecScanMetadataExhausted::Unknown => "",
        ProviderExecScanMetadataExhausted::Mixed => "mixed",
    }
    .to_owned()
}

fn provider_exec_csv_row(input: ProviderExecCsvRowInput<'_>) -> Vec<String> {
    let summary = input.summary;
    let read_stats = &summary.read_stats;
    vec![
        input.run_environment.schema_version.to_string(),
        BenchmarkMode::ProviderExec.as_csv_value().to_owned(),
        input.run_environment.host_os.to_owned(),
        input.run_environment.host_arch.to_owned(),
        optional_usize(input.run_environment.available_parallelism),
        input.seed.to_string(),
        input.workload_case_count.to_string(),
        input.workload.name.to_owned(),
        input.table.storage_profile_name().to_owned(),
        input.query.name.to_owned(),
        provider_exec_backend_name(input.backend).to_owned(),
        input.scheduling_profile.name.to_owned(),
        optional_usize(input.scheduling_profile.scan_target_partitions),
        input
            .scheduling_profile
            .max_concurrent_file_reads_per_scan
            .map(|value| value.to_string())
            .unwrap_or_default(),
        input
            .scheduling_profile
            .max_concurrent_file_reads_per_partition
            .to_string(),
        input
            .scheduling_profile
            .output_buffer_capacity_per_partition
            .to_string(),
        input
            .scheduling_profile
            .native_async_prefetch_file_count_per_partition
            .to_string(),
        summary.repetitions.to_string(),
        input.table.file_count.to_string(),
        input.table.row_count.to_string(),
        input.table.data_file_bytes.to_string(),
        input.table.deletion_vector_file_count.to_string(),
        input.table.deletion_vector_deleted_rows.to_string(),
        input
            .table
            .deletion_vector_deleted_rows_per_file
            .to_string(),
        read_stats.scan_count.to_string(),
        provider_exec_scan_metadata_exhausted_value(read_stats.scan_metadata_exhausted),
        read_stats.scan_partitions_planned.to_string(),
        read_stats.files_planned.to_string(),
        optional_u64(read_stats.estimated_rows),
        optional_u64(read_stats.estimated_bytes),
        read_stats.scan_partitions_started.to_string(),
        read_stats.scan_partitions_completed.to_string(),
        read_stats.files_started.to_string(),
        read_stats.files_completed.to_string(),
        read_stats.dynamic_partition_files_pruned.to_string(),
        read_stats.dynamic_partition_files_kept.to_string(),
        read_stats.dynamic_filters_received.to_string(),
        read_stats.dynamic_filters_accepted.to_string(),
        read_stats.dynamic_filters_unsupported.to_string(),
        read_stats.dynamic_filter_snapshots.to_string(),
        read_stats
            .dynamic_partition_files_not_pruned_missing_metadata
            .to_string(),
        read_stats
            .dynamic_partition_files_not_pruned_unsupported_expression
            .to_string(),
        read_stats.batches_produced.to_string(),
        read_stats.rows_produced.to_string(),
        read_stats.deletion_vector_payloads_loaded.to_string(),
        read_stats.deletion_vectors_applied.to_string(),
        read_stats.deletion_vector_rows_deleted.to_string(),
        read_stats.deletion_vector_failures.to_string(),
        read_stats.deletion_vector_rejections.to_string(),
        summary.produced_rows.to_string(),
        summary.produced_batches.to_string(),
        optional_u64(summary.process_peak_rss_bytes),
        optional_u64(summary.process_peak_rss_delta_bytes),
        summary.planning_micros.p50.to_string(),
        summary.planning_micros.p95.to_string(),
        summary.planning_micros.p99.to_string(),
        summary.time_to_first_batch_micros.p50.to_string(),
        summary.time_to_first_batch_micros.p95.to_string(),
        summary.time_to_first_batch_micros.p99.to_string(),
        summary.total_micros.p50.to_string(),
        summary.total_micros.p95.to_string(),
        summary.total_micros.p99.to_string(),
        summary.source_rows_per_second.p50.to_string(),
        summary.source_rows_per_second.p95.to_string(),
        summary.source_rows_per_second.p99.to_string(),
        summary.batch_latency_micros.p50.to_string(),
        summary.batch_latency_micros.p95.to_string(),
        summary.batch_latency_micros.p99.to_string(),
        summary.min_total_micros.to_string(),
        summary.max_total_micros.to_string(),
    ]
}

fn provider_exec_backend_name(backend: DeltaProviderReaderBackend) -> &'static str {
    match backend {
        DeltaProviderReaderBackend::OfficialKernel => "official_kernel",
        DeltaProviderReaderBackend::NativeAsync => "native_async",
    }
}

fn provider_exec_fixture_create_started(
    workload: &ProviderExecWorkloadCase,
    storage_profile: ProviderExecStorageProfile,
) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_FIXTURE_CREATE_STARTED_EVENT,
        workload_case = workload.name,
        provider_exec_storage_profile = storage_profile.name,
        file_count = workload.file_count(),
        row_count = workload.row_count(),
        message = PROVIDER_EXEC_FIXTURE_CREATE_STARTED_EVENT
    );
}

fn provider_exec_fixture_create_completed(
    table: &ProviderExecDeltaTable,
    workload: &ProviderExecWorkloadCase,
) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_FIXTURE_CREATE_COMPLETED_EVENT,
        workload_case = workload.name,
        provider_exec_storage_profile = table.storage_profile_name(),
        file_count = table.file_count,
        row_count = table.row_count,
        data_file_bytes = table.data_file_bytes,
        message = PROVIDER_EXEC_FIXTURE_CREATE_COMPLETED_EVENT
    );
}

fn provider_exec_fixture_create_failed(
    workload: &ProviderExecWorkloadCase,
    storage_profile: ProviderExecStorageProfile,
    error: &dyn fmt::Display,
) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_FIXTURE_CREATE_FAILED_EVENT,
        workload_case = workload.name,
        provider_exec_storage_profile = storage_profile.name,
        file_count = workload.file_count(),
        row_count = workload.row_count(),
        error_summary = error.to_string(),
        message = PROVIDER_EXEC_FIXTURE_CREATE_FAILED_EVENT
    );
}

fn provider_exec_query_planning_started(context: ProviderExecTraceContext<'_>) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_QUERY_PLANNING_STARTED_EVENT,
        provider_exec_storage_profile = context.table.storage_profile_name(),
        query_case = context.query.name,
        reader_backend = provider_exec_backend_name(context.backend),
        scheduling_mode = context.scheduling_profile.name,
        repetition_index = context.repetition_index,
        message = PROVIDER_EXEC_QUERY_PLANNING_STARTED_EVENT
    );
}

fn provider_exec_query_planning_completed(
    context: ProviderExecTraceContext<'_>,
    planning_micros: u64,
) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_QUERY_PLANNING_COMPLETED_EVENT,
        provider_exec_storage_profile = context.table.storage_profile_name(),
        query_case = context.query.name,
        reader_backend = provider_exec_backend_name(context.backend),
        scheduling_mode = context.scheduling_profile.name,
        repetition_index = context.repetition_index,
        planning_micros,
        message = PROVIDER_EXEC_QUERY_PLANNING_COMPLETED_EVENT
    );
}

fn provider_exec_query_planning_failed(
    context: ProviderExecTraceContext<'_>,
    error: &dyn fmt::Display,
) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_QUERY_PLANNING_FAILED_EVENT,
        provider_exec_storage_profile = context.table.storage_profile_name(),
        query_case = context.query.name,
        reader_backend = provider_exec_backend_name(context.backend),
        scheduling_mode = context.scheduling_profile.name,
        repetition_index = context.repetition_index,
        error_summary = error.to_string(),
        message = PROVIDER_EXEC_QUERY_PLANNING_FAILED_EVENT
    );
}

fn provider_exec_query_execution_started(context: ProviderExecTraceContext<'_>) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_QUERY_EXECUTION_STARTED_EVENT,
        provider_exec_storage_profile = context.table.storage_profile_name(),
        query_case = context.query.name,
        reader_backend = provider_exec_backend_name(context.backend),
        scheduling_mode = context.scheduling_profile.name,
        repetition_index = context.repetition_index,
        message = PROVIDER_EXEC_QUERY_EXECUTION_STARTED_EVENT
    );
}

fn provider_exec_query_execution_first_batch(
    context: ProviderExecTraceContext<'_>,
    elapsed_micros: u64,
) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_QUERY_EXECUTION_FIRST_BATCH_EVENT,
        provider_exec_storage_profile = context.table.storage_profile_name(),
        query_case = context.query.name,
        reader_backend = provider_exec_backend_name(context.backend),
        scheduling_mode = context.scheduling_profile.name,
        repetition_index = context.repetition_index,
        elapsed_micros,
        message = PROVIDER_EXEC_QUERY_EXECUTION_FIRST_BATCH_EVENT
    );
}

fn provider_exec_query_execution_completed(
    context: ProviderExecTraceContext<'_>,
    produced_rows: usize,
    produced_batches: usize,
    total_micros: u64,
) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_QUERY_EXECUTION_COMPLETED_EVENT,
        provider_exec_storage_profile = context.table.storage_profile_name(),
        query_case = context.query.name,
        reader_backend = provider_exec_backend_name(context.backend),
        scheduling_mode = context.scheduling_profile.name,
        repetition_index = context.repetition_index,
        produced_rows,
        produced_batches,
        total_micros,
        message = PROVIDER_EXEC_QUERY_EXECUTION_COMPLETED_EVENT
    );
}

fn provider_exec_query_execution_failed(
    context: ProviderExecTraceContext<'_>,
    produced_rows: usize,
    produced_batches: usize,
    error: &dyn fmt::Display,
) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_QUERY_EXECUTION_FAILED_EVENT,
        provider_exec_storage_profile = context.table.storage_profile_name(),
        query_case = context.query.name,
        reader_backend = provider_exec_backend_name(context.backend),
        scheduling_mode = context.scheduling_profile.name,
        repetition_index = context.repetition_index,
        produced_rows,
        produced_batches,
        error_summary = error.to_string(),
        message = PROVIDER_EXEC_QUERY_EXECUTION_FAILED_EVENT
    );
}

fn provider_exec_stats_collect_started(context: ProviderExecTraceContext<'_>) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_STATS_COLLECT_STARTED_EVENT,
        provider_exec_storage_profile = context.table.storage_profile_name(),
        query_case = context.query.name,
        reader_backend = provider_exec_backend_name(context.backend),
        scheduling_mode = context.scheduling_profile.name,
        repetition_index = context.repetition_index,
        message = PROVIDER_EXEC_STATS_COLLECT_STARTED_EVENT
    );
}

fn provider_exec_stats_collect_completed(
    context: ProviderExecTraceContext<'_>,
    provider_stats_scan_count: usize,
) {
    tracing::info!(
        target: "delta_funnel",
        telemetry_event = PROVIDER_EXEC_STATS_COLLECT_COMPLETED_EVENT,
        provider_exec_storage_profile = context.table.storage_profile_name(),
        query_case = context.query.name,
        reader_backend = provider_exec_backend_name(context.backend),
        scheduling_mode = context.scheduling_profile.name,
        repetition_index = context.repetition_index,
        provider_stats_scan_count,
        message = PROVIDER_EXEC_STATS_COLLECT_COMPLETED_EVENT
    );
}

fn provider_exec_filter_matches(filter: &Option<String>, candidate: &str) -> bool {
    filter.as_deref().is_none_or(|filter| filter == candidate)
}

fn validate_provider_exec_filter_result(
    filter_name: &str,
    filter: &Option<String>,
    match_count: usize,
) -> Result<(), Box<dyn Error>> {
    if match_count == 0
        && let Some(filter) = filter
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("provider-exec {filter_name} filter `{filter}` did not match any case"),
        )
        .into());
    }

    Ok(())
}

fn provider_exec_arrow_schema(schema_kind: ProviderExecSchemaKind) -> SchemaRef {
    match schema_kind {
        ProviderExecSchemaKind::SimpleOrders => Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("customer_name", DataType::Utf8, true),
        ])),
        ProviderExecSchemaKind::SyntheticPartitionedEventLog
        | ProviderExecSchemaKind::SyntheticWideEventExport => Arc::new(Schema::new(
            provider_exec_synthetic_columns(schema_kind)
                .into_iter()
                .map(|column| {
                    Field::new(
                        column.name,
                        provider_exec_arrow_data_type(column.data_type),
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        )),
    }
}

fn provider_exec_arrow_data_type(data_type: SyntheticDataType) -> DataType {
    match data_type {
        SyntheticDataType::String => DataType::Utf8,
        SyntheticDataType::Int => DataType::Int32,
        SyntheticDataType::Double => DataType::Float64,
        SyntheticDataType::Bigint => DataType::Int64,
        SyntheticDataType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        SyntheticDataType::Boolean => DataType::Boolean,
    }
}

fn provider_exec_record_batch(
    schema_kind: ProviderExecSchemaKind,
    schema: SchemaRef,
    file: &ProviderExecFileSpec,
    first_row_id: usize,
) -> Result<RecordBatch, Box<dyn Error>> {
    match schema_kind {
        ProviderExecSchemaKind::SimpleOrders => {
            provider_exec_simple_orders_record_batch(schema, first_row_id, file.rows)
        }
        ProviderExecSchemaKind::SyntheticPartitionedEventLog
        | ProviderExecSchemaKind::SyntheticWideEventExport => {
            provider_exec_synthetic_record_batch(schema_kind, schema, file, first_row_id)
        }
    }
}

fn provider_exec_simple_orders_record_batch(
    schema: SchemaRef,
    first_id: usize,
    rows: usize,
) -> Result<RecordBatch, Box<dyn Error>> {
    let first_id_i32 = i32::try_from(first_id)?;
    let row_count = i32::try_from(rows)?;
    let ids = (first_id_i32..first_id_i32 + row_count).collect::<Vec<_>>();
    let names = (0..rows)
        .map(|offset| Some(format!("customer-{}", first_id.saturating_add(offset))))
        .collect::<Vec<_>>();
    let columns = vec![
        Arc::new(Int32Array::from(ids)) as ArrayRef,
        Arc::new(StringArray::from(names)) as ArrayRef,
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn provider_exec_synthetic_record_batch(
    schema_kind: ProviderExecSchemaKind,
    schema: SchemaRef,
    file: &ProviderExecFileSpec,
    first_row_id: usize,
) -> Result<RecordBatch, Box<dyn Error>> {
    let partition_date = file.partition_date.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "synthetic provider-exec file is missing its partition date",
        )
    })?;
    let columns = provider_exec_synthetic_columns(schema_kind)
        .into_iter()
        .map(|column| {
            provider_exec_synthetic_column_array(
                schema_kind,
                column,
                partition_date,
                first_row_id,
                file.rows,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn provider_exec_synthetic_column_array(
    schema_kind: ProviderExecSchemaKind,
    column: ColumnShape,
    partition_date: SyntheticDate,
    first_row_id: usize,
    rows: usize,
) -> Result<ArrayRef, Box<dyn Error>> {
    let row_ids = first_row_id..first_row_id.saturating_add(rows);
    let array = match column.data_type {
        SyntheticDataType::String => Arc::new(StringArray::from(
            row_ids
                .map(|row_id| {
                    provider_exec_synthetic_string_value(schema_kind, column.name, row_id)
                })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        SyntheticDataType::Int => Arc::new(Int32Array::from(
            row_ids
                .map(|row_id| {
                    provider_exec_synthetic_int_value(column.name, partition_date, row_id)
                })
                .collect::<Result<Vec<_>, _>>()?,
        )) as ArrayRef,
        SyntheticDataType::Double => Arc::new(Float64Array::from(
            row_ids
                .map(|row_id| provider_exec_synthetic_double_value(column.name, row_id))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        SyntheticDataType::Bigint => Arc::new(Int64Array::from(
            row_ids
                .map(|row_id| provider_exec_synthetic_bigint_value(column.name, row_id))
                .collect::<Result<Vec<_>, _>>()?,
        )) as ArrayRef,
        SyntheticDataType::Timestamp => Arc::new(TimestampMicrosecondArray::from(
            row_ids
                .map(|row_id| provider_exec_synthetic_timestamp_value(partition_date, row_id))
                .collect::<Result<Vec<_>, _>>()?,
        )) as ArrayRef,
        SyntheticDataType::Boolean => Arc::new(BooleanArray::from(
            row_ids
                .map(|row_id| Some(!row_id.is_multiple_of(7)))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
    };

    Ok(array)
}

fn provider_exec_synthetic_string_value(
    schema_kind: ProviderExecSchemaKind,
    column_name: &str,
    row_id: usize,
) -> Option<String> {
    if schema_kind == ProviderExecSchemaKind::SyntheticWideEventExport {
        return match column_name {
            "primary_event_id" => Some(format!("event-primary-{row_id:018}")),
            "group_id" => Some(format!("group-{row_id:032}")),
            "secondary_event_id" => Some(format!("event-secondary-{row_id:017}")),
            "source_kind" => Some(["web", "mobile", "api", "batch"][row_id % 4].to_owned()),
            "position_code" => Some(format!("position-code-{:05}", row_id % 36_000)),
            "resolution_diagnostic" => Some(format!("resolution-diagnostic-{:08}", row_id % 9)),
            "resolved_event_key" => Some(format!("resolved-key-{row_id:024}")),
            "source_level" => Some(format!("source-level-{:02}", row_id % 7)),
            "source_group" => Some(["primary", "secondary"][row_id % 2].to_owned()),
            "local_date_key" => Some(format!("local-date-key-{:08}", row_id % 1_500)),
            "quality_tier" => Some(format!("quality-tier-{:02}", row_id % 22)),
            _ => Some(format!("{column_name}-{row_id:016}")),
        };
    }

    match column_name {
        "primary_event_id" => Some(format!("event-{row_id:012}")),
        "group_id" => Some(format!("group-{:05}", row_id % 34_500)),
        "secondary_event_id" => Some(format!("secondary-{row_id:012}")),
        "source_kind" => Some(["web", "mobile", "api", "batch"][row_id % 4].to_owned()),
        "category_code" => Some(format!("category-{:04}", row_id % 4_096)),
        "optional_group_key" if row_id.is_multiple_of(11) => None,
        "optional_group_key" => Some(format!("optional-{:05}", row_id % 12_000)),
        "quality_tier" => Some(["bronze", "silver", "gold", "platinum"][row_id % 4].to_owned()),
        "group_code" => Some(format!("group-code-{:02}", row_id % 50)),
        _ => Some(format!("{column_name}-{}", row_id % 10_000)),
    }
}

fn provider_exec_synthetic_int_value(
    column_name: &str,
    partition_date: SyntheticDate,
    row_id: usize,
) -> Result<Option<i32>, Box<dyn Error>> {
    let value = match column_name {
        "event_year" => partition_date.year,
        "event_month" => i32::from(partition_date.month),
        "event_day" => i32::from(partition_date.day),
        "event_processed_year"
        | "category_processed_year"
        | "position_processed_year"
        | "record_processed_year"
        | "local_event_year" => partition_date.year,
        "event_processed_month"
        | "category_processed_month"
        | "position_processed_month"
        | "record_processed_month"
        | "local_event_month" => i32::from(partition_date.month),
        "event_processed_day"
        | "category_processed_day"
        | "position_processed_day"
        | "record_processed_day"
        | "local_event_day" => i32::from(partition_date.day),
        _ => i32::try_from(row_id % 10_000)?,
    };

    Ok(Some(value))
}

fn provider_exec_synthetic_double_value(column_name: &str, row_id: usize) -> Option<f64> {
    let scale = match column_name {
        "metric_x" => 0.25,
        "metric_y" => 0.5,
        "metric_z" => 0.75,
        _ => 1.0,
    };

    Some((row_id % 1_000_000) as f64 * scale)
}

fn provider_exec_synthetic_bigint_value(
    column_name: &str,
    row_id: usize,
) -> Result<Option<i64>, Box<dyn Error>> {
    let value = match column_name {
        "actor_numeric_id" => row_id % 11_900,
        "category_num" => row_id % 4_096,
        "position_num" => row_id % 10,
        _ => row_id,
    };

    Ok(Some(i64::try_from(value)?))
}

fn provider_exec_synthetic_timestamp_value(
    partition_date: SyntheticDate,
    row_id: usize,
) -> Result<Option<i64>, Box<dyn Error>> {
    let days = i64::from(partition_date.year.saturating_sub(1970))
        .saturating_mul(365)
        .saturating_add(
            i64::from(partition_date.month)
                .saturating_sub(1)
                .saturating_mul(31),
        )
        .saturating_add(i64::from(partition_date.day).saturating_sub(1));
    let micros_per_day = 86_400_000_000_i64;
    let micros_within_day = i64::try_from(row_id % 86_400)?.saturating_mul(1_000_000);

    Ok(Some(
        days.saturating_mul(micros_per_day)
            .saturating_add(micros_within_day),
    ))
}

fn provider_exec_add_json(
    schema_kind: ProviderExecSchemaKind,
    file: &ProviderExecFileSpec,
    size: u64,
    first_row_id: usize,
    deletion_vector: Option<&ProviderExecDeletionVector>,
) -> Result<String, Box<dyn Error>> {
    let stats = provider_exec_stats_json(schema_kind, file, first_row_id)?;
    let escaped_stats = json_string_escape(&stats);
    let deletion_vector_json = deletion_vector
        .map(|deletion_vector| {
            let descriptor = &deletion_vector.descriptor;
            let storage_type = descriptor.storage_type;
            let path_or_inline_dv = &descriptor.path_or_inline_dv;
            let offset = descriptor.offset.unwrap_or(0);
            let size_in_bytes = descriptor.size_in_bytes;
            let cardinality = descriptor.cardinality;
            format!(
                r#","deletionVector":{{"storageType":"{storage_type}","pathOrInlineDv":"{path_or_inline_dv}","offset":{offset},"sizeInBytes":{size_in_bytes},"cardinality":{cardinality}}}"#
            )
        })
        .unwrap_or_default();

    Ok(format!(
        r#"{{"add":{{"path":"{}","partitionValues":{{}},"size":{size},"modificationTime":{PROVIDER_EXEC_MODIFICATION_TIME_MS},"dataChange":true,"stats":"{escaped_stats}"{deletion_vector_json}}}}}"#,
        json_string_escape(&file.path)
    ))
}

fn provider_exec_stats_json(
    schema_kind: ProviderExecSchemaKind,
    file: &ProviderExecFileSpec,
    first_row_id: usize,
) -> Result<String, Box<dyn Error>> {
    match schema_kind {
        ProviderExecSchemaKind::SimpleOrders => {
            let max_id = first_row_id.saturating_add(file.rows).saturating_sub(1);
            let min_id = i32::try_from(first_row_id)?;
            let max_id = i32::try_from(max_id)?;
            let min_customer = format!("customer-{first_row_id}");
            let max_customer = format!("customer-{max_id}");

            Ok(format!(
                r#"{{"numRecords":{},"minValues":{{"id":{min_id},"customer_name":"{min_customer}"}},"maxValues":{{"id":{max_id},"customer_name":"{max_customer}"}},"nullCount":{{"id":0,"customer_name":0}}}}"#,
                file.rows
            ))
        }
        ProviderExecSchemaKind::SyntheticPartitionedEventLog
        | ProviderExecSchemaKind::SyntheticWideEventExport => {
            let partition_date = file.partition_date.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "synthetic provider-exec file is missing its partition date",
                )
            })?;

            Ok(format!(
                r#"{{"numRecords":{},"minValues":{{"event_year":{},"event_month":{},"event_day":{}}},"maxValues":{{"event_year":{},"event_month":{},"event_day":{}}},"nullCount":{{"event_year":0,"event_month":0,"event_day":0}}}}"#,
                file.rows,
                partition_date.year,
                partition_date.month,
                partition_date.day,
                partition_date.year,
                partition_date.month,
                partition_date.day
            ))
        }
    }
}

fn provider_exec_metadata_json(schema_kind: ProviderExecSchemaKind) -> String {
    let schema_string = provider_exec_delta_schema_json(schema_kind);
    let escaped_schema = json_string_escape(&schema_string);

    format!(
        r#"{{"metaData":{{"id":"delta-funnel-provider-exec-benchmark","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{escaped_schema}","partitionColumns":[],"configuration":{{}},"createdTime":1587968585495}}}}"#
    )
}

fn provider_exec_delta_schema_json(schema_kind: ProviderExecSchemaKind) -> String {
    let fields = match schema_kind {
        ProviderExecSchemaKind::SimpleOrders => vec![
            provider_exec_delta_field_json("id", "integer", false),
            provider_exec_delta_field_json("customer_name", "string", true),
        ],
        ProviderExecSchemaKind::SyntheticPartitionedEventLog
        | ProviderExecSchemaKind::SyntheticWideEventExport => {
            provider_exec_synthetic_columns(schema_kind)
                .into_iter()
                .map(|column| {
                    provider_exec_delta_field_json(
                        column.name,
                        provider_exec_delta_data_type(column.data_type),
                        true,
                    )
                })
                .collect()
        }
    };

    format!(r#"{{"type":"struct","fields":[{}]}}"#, fields.join(","))
}

fn provider_exec_delta_field_json(name: &str, data_type: &str, nullable: bool) -> String {
    format!(
        r#"{{"name":"{}","type":"{data_type}","nullable":{nullable},"metadata":{{}}}}"#,
        json_string_escape(name)
    )
}

fn provider_exec_delta_data_type(data_type: SyntheticDataType) -> &'static str {
    match data_type {
        SyntheticDataType::String => "string",
        SyntheticDataType::Int => "integer",
        SyntheticDataType::Double => "double",
        SyntheticDataType::Bigint => "long",
        SyntheticDataType::Timestamp => "timestamp",
        SyntheticDataType::Boolean => "boolean",
    }
}

fn validate_provider_exec_deletion_vector(
    workload: &ProviderExecWorkloadCase,
    file: &ProviderExecFileSpec,
) -> Result<(), Box<dyn Error>> {
    if let Some(max_deleted_row_index) = workload.deleted_row_indexes_per_file.iter().max()
        && *max_deleted_row_index >= u64::try_from(file.rows)?
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "provider-exec deletion vector row index {max_deleted_row_index} exceeds file row count {}",
                file.rows
            ),
        )
        .into());
    }

    Ok(())
}

fn json_string_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                escaped.push_str(&format!("\\u{:04x}", character as u32));
            }
            character => escaped.push(character),
        }
    }

    escaped
}

fn provider_exec_deletion_vector_fixture(
    deleted_rows: &[u64],
) -> Result<ProviderExecDeletionVector, Box<dyn Error>> {
    let mut buffer = Vec::new();
    let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
    let mut deletion_vector = KernelDeletionVector::new();
    deletion_vector.add_deleted_row_indexes(deleted_rows.iter().copied());
    let write_result = writer.write_deletion_vector(deletion_vector)?;
    writer.finalize()?;

    Ok(ProviderExecDeletionVector {
        descriptor: DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: PROVIDER_EXEC_RELATIVE_DV_ID.to_owned(),
            offset: Some(write_result.offset),
            size_in_bytes: write_result.size_in_bytes,
            cardinality: write_result.cardinality,
        },
        bytes: buffer,
    })
}

fn unique_benchmark_name(name: &str) -> Result<String, Box<dyn Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

    Ok(format!(
        "{}-delta-provider-exec-{name}-{nanos}",
        std::process::id()
    ))
}

fn percentile_summary(values: &[u64]) -> PercentileSummary {
    PercentileSummary {
        p50: percentile(values, 50),
        p95: percentile(values, 95),
        p99: percentile(values, 99),
    }
}

fn percentile(values: &[u64], percentile: u64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let numerator = percentile.saturating_mul(sorted.len() as u64);
    let rank = numerator.div_ceil(100).saturating_sub(1);
    let index = usize::try_from(rank)
        .unwrap_or(usize::MAX)
        .min(sorted.len().saturating_sub(1));

    sorted[index]
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

    fn wide_event_export_target_shape() -> Result<Self, SyntheticGenerationError> {
        let shape = SyntheticDeltaTableShape::wide_event_export();
        let file_set = shape.generate_file_set()?;

        Ok(Self {
            name: "wide_event_export_target_shape",
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
    MissingTraceOutputPath,
    DuplicateTraceOutputPath,
    MissingMode,
    DuplicateMode,
    InvalidMode(String),
    MissingHostProbeTempDir,
    DuplicateHostProbeTempDir,
    MissingHostProbeIoBytes,
    InvalidHostProbeIoBytes(String),
    HostProbeIoBytesOutOfRange(usize),
    MissingHostProbeIoRepetitions,
    InvalidHostProbeIoRepetitions(String),
    HostProbeIoRepetitionsOutOfRange(usize),
    MissingProviderExecTempDir,
    DuplicateProviderExecTempDir,
    MissingProviderExecRepetitions,
    InvalidProviderExecRepetitions(String),
    ProviderExecRepetitionsOutOfRange(usize),
    MissingProviderExecStorageProfile,
    InvalidProviderExecStorageProfile(String),
    DuplicateProviderExecStorageProfile,
    MissingProviderExecWorkloadFilter,
    DuplicateProviderExecWorkloadFilter,
    MissingProviderExecQueryFilter,
    DuplicateProviderExecQueryFilter,
    MissingProviderExecBackendFilter,
    DuplicateProviderExecBackendFilter,
    MissingProviderExecSchedulingProfileFilter,
    DuplicateProviderExecSchedulingProfileFilter,
    ProviderExecDefaultCaseConflict(&'static str),
    MissingSeed,
    InvalidSeed(String),
    UnknownArgument(String),
}

impl fmt::Display for BenchmarkRunnerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingOutputPath => write!(formatter, "--output requires a path"),
            Self::DuplicateOutputPath => write!(formatter, "--output may be provided only once"),
            Self::MissingTraceOutputPath => write!(formatter, "--trace-output requires a path"),
            Self::DuplicateTraceOutputPath => {
                write!(formatter, "--trace-output may be provided only once")
            }
            Self::MissingMode => write!(formatter, "--mode requires a value"),
            Self::DuplicateMode => write!(formatter, "--mode may be provided only once"),
            Self::InvalidMode(value) => write!(
                formatter,
                "invalid --mode value `{value}`; expected `synthetic`, `host-probe`, or `provider-exec`"
            ),
            Self::MissingHostProbeTempDir => {
                write!(formatter, "--host-probe-temp-dir requires a path")
            }
            Self::DuplicateHostProbeTempDir => {
                write!(formatter, "--host-probe-temp-dir may be provided only once")
            }
            Self::MissingHostProbeIoBytes => {
                write!(formatter, "--host-probe-io-bytes requires a byte count")
            }
            Self::InvalidHostProbeIoBytes(value) => {
                write!(formatter, "invalid --host-probe-io-bytes value `{value}`")
            }
            Self::HostProbeIoBytesOutOfRange(value) => write!(
                formatter,
                "--host-probe-io-bytes value `{value}` must be between 1 and {HOST_PROBE_MAX_LOCAL_IO_BYTES}"
            ),
            Self::MissingHostProbeIoRepetitions => write!(
                formatter,
                "--host-probe-io-repetitions requires a repetition count"
            ),
            Self::InvalidHostProbeIoRepetitions(value) => write!(
                formatter,
                "invalid --host-probe-io-repetitions value `{value}`"
            ),
            Self::HostProbeIoRepetitionsOutOfRange(value) => write!(
                formatter,
                "--host-probe-io-repetitions value `{value}` must be between 1 and {HOST_PROBE_MAX_LOCAL_IO_REPETITIONS}"
            ),
            Self::MissingProviderExecTempDir => {
                write!(formatter, "--provider-exec-temp-dir requires a path")
            }
            Self::DuplicateProviderExecTempDir => {
                write!(
                    formatter,
                    "--provider-exec-temp-dir may be provided only once"
                )
            }
            Self::MissingProviderExecRepetitions => write!(
                formatter,
                "--provider-exec-repetitions requires a repetition count"
            ),
            Self::InvalidProviderExecRepetitions(value) => write!(
                formatter,
                "invalid --provider-exec-repetitions value `{value}`"
            ),
            Self::ProviderExecRepetitionsOutOfRange(value) => write!(
                formatter,
                "--provider-exec-repetitions value `{value}` must be between 1 and {MAX_PROVIDER_EXEC_REPETITIONS}"
            ),
            Self::MissingProviderExecStorageProfile => write!(
                formatter,
                "--provider-exec-storage-profile requires a profile name"
            ),
            Self::InvalidProviderExecStorageProfile(value) => write!(
                formatter,
                "invalid --provider-exec-storage-profile value `{value}`; expected `local`, `s3-normal`, `s3-high-latency`, or `s3-throttled`"
            ),
            Self::DuplicateProviderExecStorageProfile => write!(
                formatter,
                "--provider-exec-storage-profile may be provided only once"
            ),
            Self::MissingProviderExecWorkloadFilter => {
                write!(
                    formatter,
                    "--provider-exec-workload requires a workload name"
                )
            }
            Self::DuplicateProviderExecWorkloadFilter => {
                write!(
                    formatter,
                    "--provider-exec-workload may be provided only once"
                )
            }
            Self::MissingProviderExecQueryFilter => {
                write!(formatter, "--provider-exec-query requires a query name")
            }
            Self::DuplicateProviderExecQueryFilter => {
                write!(formatter, "--provider-exec-query may be provided only once")
            }
            Self::MissingProviderExecBackendFilter => {
                write!(formatter, "--provider-exec-backend requires a backend name")
            }
            Self::DuplicateProviderExecBackendFilter => {
                write!(
                    formatter,
                    "--provider-exec-backend may be provided only once"
                )
            }
            Self::MissingProviderExecSchedulingProfileFilter => write!(
                formatter,
                "--provider-exec-scheduling-profile requires a scheduling profile name"
            ),
            Self::DuplicateProviderExecSchedulingProfileFilter => write!(
                formatter,
                "--provider-exec-scheduling-profile may be provided only once"
            ),
            Self::ProviderExecDefaultCaseConflict(argument) => write!(
                formatter,
                "--provider-exec-default-case cannot be combined with {argument}"
            ),
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
    mode: BenchmarkMode,
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

#[derive(Debug, Clone)]
struct HostProbeCsvRowInput {
    run_environment: BenchmarkRunEnvironment,
    seed: u64,
    local_environment: DeltaScanPartitionTargetLocalEnvironmentDiagnostic,
    scheduler_probe: HostSchedulerProbeResult,
    local_io_probe: HostLocalIoProbeResult,
    policy_decision: DeltaScanPartitionTargetDiagnosticOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HostSchedulerProbeResult {
    task_count: usize,
    completed_task_count: usize,
    concurrency: usize,
    total_micros: u64,
    nanos_per_task: u64,
    stable_concurrency_hint: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HostLocalIoProbeResult {
    enabled: bool,
    status: HostLocalIoProbeStatus,
    repetitions: usize,
    bytes_per_repetition: usize,
    bytes_read: u64,
    total_micros: Option<u64>,
    latency_micros: Option<u64>,
    throughput_bytes_per_second: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostLocalIoProbeStatus {
    Disabled,
    Ok,
    Error,
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

    fn wide_event_export() -> Self {
        Self {
            name: "synthetic_wide_event_export",
            total_rows: 13_394_789,
            active_file_count: 1_204,
            active_data_size_bytes: 704_643_072,
            file_size_bytes: DistributionSummary {
                average: 585_252,
                p50: 465_342,
                p90: 1_362_711,
                p99: 1_775_296,
            },
            rows_per_file: DistributionSummary {
                average: 11_125,
                p50: 8_941,
                p90: 25_884,
                p99: 34_224,
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
                average_files_per_partition_hundredths: 129,
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
                columns: wide_event_export_columns(),
            },
            row_distribution: RowDistributionShape {
                source_split: vec![
                    CategoryRowCount {
                        category: "primary_segment",
                        rows: 12_378_915,
                    },
                    CategoryRowCount {
                        category: "secondary_segment",
                        rows: 1_015_874,
                    },
                ],
                uniform_category: UniformCategoryShape {
                    column_name: "category_num",
                    category_count: 7,
                    min_rows_per_category: 1_890_000,
                    max_rows_per_category: 1_930_000,
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
                        rows: 2_954_789,
                    },
                ],
            },
            null_patterns: vec![
                NullPattern {
                    column_name: "actor_numeric_id",
                    null_rows: 40_000,
                    rationale: "sparse optional numeric identifier",
                },
                NullPattern {
                    column_name: "metric_z",
                    null_rows: 12_378_915,
                    rationale: "missing for primary_segment rows",
                },
                NullPattern {
                    column_name: "quality_tier",
                    null_rows: 12_378_915,
                    rationale: "missing for primary_segment rows",
                },
                NullPattern {
                    column_name: "validation_flag",
                    null_rows: 12_420_000,
                    rationale: "mostly unavailable boolean flag",
                },
            ],
            cardinalities: vec![
                CardinalityHint {
                    column_name: "primary_event_id",
                    distinct_count: 13_394_789,
                    max_length: Some(32),
                },
                CardinalityHint {
                    column_name: "secondary_event_id",
                    distinct_count: 13_394_789,
                    max_length: Some(36),
                },
                CardinalityHint {
                    column_name: "group_id",
                    distinct_count: 36_900,
                    max_length: Some(38),
                },
                CardinalityHint {
                    column_name: "actor_numeric_id",
                    distinct_count: 11_900,
                    max_length: None,
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
    ) -> Result<DeltaScanPartitionTargetDiagnosticOutput, Box<delta_funnel::DeltaFunnelError>> {
        derive_delta_scan_partition_target_diagnostic(self.input).map_err(Box::new)
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

fn provider_exec_synthetic_columns(schema_kind: ProviderExecSchemaKind) -> Vec<ColumnShape> {
    match schema_kind {
        ProviderExecSchemaKind::SimpleOrders => Vec::new(),
        ProviderExecSchemaKind::SyntheticPartitionedEventLog => synthetic_columns(),
        ProviderExecSchemaKind::SyntheticWideEventExport => wide_event_export_columns(),
    }
}

fn wide_event_export_columns() -> Vec<ColumnShape> {
    vec![
        string_column("primary_event_id"),
        string_column("group_id"),
        string_column("secondary_event_id"),
        string_column("source_kind"),
        bigint_column("actor_numeric_id"),
        bigint_column("category_num"),
        bigint_column("position_num"),
        string_column("position_code"),
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
        int_column("position_processed_year"),
        int_column("position_processed_month"),
        int_column("position_processed_day"),
        int_column("record_processed_year"),
        int_column("record_processed_month"),
        int_column("record_processed_day"),
        string_column("resolution_diagnostic"),
        string_column("resolved_event_key"),
        boolean_column("validation_flag"),
        string_column("source_level"),
        string_column("source_group"),
        string_column("local_date_key"),
        string_column("quality_tier"),
        int_column("local_event_year"),
        int_column("local_event_month"),
        int_column("local_event_day"),
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

fn run_host_scheduler_probe(available_parallelism: Option<usize>) -> HostSchedulerProbeResult {
    let concurrency = available_parallelism
        .unwrap_or(1)
        .clamp(1, HOST_PROBE_MAX_SCHEDULER_CONCURRENCY);
    let task_count = concurrency.saturating_mul(HOST_PROBE_SCHEDULER_TASKS_PER_WORKER);
    let next_task = AtomicUsize::new(0);
    let completed_task_count = AtomicUsize::new(0);
    let started = Instant::now();

    thread::scope(|scope| {
        for _ in 0..concurrency {
            scope.spawn(|| {
                loop {
                    let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                    if task_index >= task_count {
                        break;
                    }
                    std::hint::black_box(task_index);
                    completed_task_count.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });

    let elapsed = started.elapsed();
    let completed_task_count = completed_task_count.load(Ordering::Relaxed);
    let nanos_per_task = if completed_task_count == 0 {
        0
    } else {
        u128_to_u64_saturating(elapsed.as_nanos().div_ceil(completed_task_count as u128))
    };

    HostSchedulerProbeResult {
        task_count,
        completed_task_count,
        concurrency,
        total_micros: u128_to_u64_saturating(elapsed.as_micros()),
        nanos_per_task,
        stable_concurrency_hint: concurrency,
    }
}

fn run_host_local_io_probe(config: &HostProbeLocalIoConfig, seed: u64) -> HostLocalIoProbeResult {
    if !config.enabled {
        return HostLocalIoProbeResult {
            enabled: false,
            status: HostLocalIoProbeStatus::Disabled,
            repetitions: config.repetitions,
            bytes_per_repetition: config.bytes_per_repetition,
            bytes_read: 0,
            total_micros: None,
            latency_micros: None,
            throughput_bytes_per_second: None,
        };
    }

    match run_host_local_io_probe_inner(config, seed) {
        Ok(result) => result,
        Err(_error) => HostLocalIoProbeResult {
            enabled: true,
            status: HostLocalIoProbeStatus::Error,
            repetitions: config.repetitions,
            bytes_per_repetition: config.bytes_per_repetition,
            bytes_read: 0,
            total_micros: None,
            latency_micros: None,
            throughput_bytes_per_second: None,
        },
    }
}

fn run_host_local_io_probe_inner(
    config: &HostProbeLocalIoConfig,
    seed: u64,
) -> io::Result<HostLocalIoProbeResult> {
    let temp_dir = config.temp_dir.clone().unwrap_or_else(env::temp_dir);
    fs::create_dir_all(&temp_dir)?;
    let probe_path = temp_dir.join(format!(
        "delta-funnel-host-probe-{}-{seed}.bin",
        std::process::id()
    ));
    let result = run_host_local_io_probe_at_path(config, &probe_path);
    let _ = fs::remove_file(&probe_path);
    result
}

fn run_host_local_io_probe_at_path(
    config: &HostProbeLocalIoConfig,
    probe_path: &std::path::Path,
) -> io::Result<HostLocalIoProbeResult> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(probe_path)?;
    write_probe_file(&mut file, config.bytes_per_repetition)?;
    file.sync_all()?;

    let mut total_latency_micros = 0_u64;
    let mut bytes_read = 0_u64;
    let mut buffer = vec![0_u8; 8192];
    let started = Instant::now();

    for _ in 0..config.repetitions {
        file.seek(SeekFrom::Start(0))?;
        let first_read_started = Instant::now();
        let mut first_byte = [0_u8; 1];
        file.read_exact(&mut first_byte)?;
        total_latency_micros = total_latency_micros.saturating_add(u128_to_u64_saturating(
            first_read_started.elapsed().as_micros(),
        ));
        bytes_read = bytes_read.saturating_add(1);

        let mut remaining = config.bytes_per_repetition.saturating_sub(1);
        while remaining > 0 {
            let read_size = remaining.min(buffer.len());
            file.read_exact(&mut buffer[..read_size])?;
            bytes_read = bytes_read.saturating_add(read_size as u64);
            remaining -= read_size;
        }

        std::hint::black_box(first_byte);
        std::hint::black_box(&buffer);
    }

    let total_micros = u128_to_u64_saturating(started.elapsed().as_micros()).max(1);
    let throughput_bytes_per_second =
        u128_to_u64_saturating(u128::from(bytes_read) * 1_000_000 / u128::from(total_micros));
    let latency_micros = total_latency_micros / config.repetitions as u64;

    Ok(HostLocalIoProbeResult {
        enabled: true,
        status: HostLocalIoProbeStatus::Ok,
        repetitions: config.repetitions,
        bytes_per_repetition: config.bytes_per_repetition,
        bytes_read,
        total_micros: Some(total_micros),
        latency_micros: Some(latency_micros),
        throughput_bytes_per_second: Some(throughput_bytes_per_second),
    })
}

fn write_probe_file(file: &mut File, bytes: usize) -> io::Result<()> {
    let mut remaining = bytes;
    let mut pattern = vec![0_u8; 8192];
    for (index, byte) in pattern.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }

    while remaining > 0 {
        let write_size = remaining.min(pattern.len());
        file.write_all(&pattern[..write_size])?;
        remaining -= write_size;
    }

    Ok(())
}

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn process_peak_rss_bytes() -> Option<u64> {
    process_status_memory_kib("VmHWM").map(|kib| kib.saturating_mul(1024))
}

fn process_status_memory_kib(key: &str) -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    process_status_memory_kib_from_status(&status, key)
}

fn process_status_memory_kib_from_status(status: &str, key: &str) -> Option<u64> {
    for line in status.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name != key {
            continue;
        }
        let mut fields = value.split_whitespace();
        let kib = fields.next()?.parse::<u64>().ok()?;
        let unit = fields.next();
        if unit.is_some_and(|unit| unit != "kB") {
            return None;
        }

        return Some(kib);
    }

    None
}

fn benchmark_csv_row(input: BenchmarkCsvRowInput<'_>) -> Vec<String> {
    let shape = input.shape;
    let file_set = input.file_set;
    let policy_case = input.policy_case;
    let policy_decision = input.policy_decision;
    let partitioned_work_summary = input.partitioned_work.summary();

    vec![
        input.run_environment.schema_version.to_string(),
        input.mode.as_csv_value().to_owned(),
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
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
    ]
}

fn host_probe_csv_row(input: HostProbeCsvRowInput) -> Vec<String> {
    let policy_input = input.local_environment.policy_input;
    let policy_decision = input.policy_decision;
    let mut row = vec![String::new(); BENCHMARK_CSV_HEADER.len()];

    row[0] = input.run_environment.schema_version.to_string();
    row[1] = BenchmarkMode::HostProbe.as_csv_value().to_owned();
    row[2] = input.run_environment.host_os.to_owned();
    row[3] = input.run_environment.host_arch.to_owned();
    row[4] = optional_usize(input.run_environment.available_parallelism);
    row[5] = input.seed.to_string();
    row[6] = "0".to_owned();
    row[7] = "host_probe".to_owned();
    row[26] = "0".to_owned();
    row[31] = "host_probe_default_policy".to_owned();
    row[32] = optional_usize(policy_input.available_parallelism);
    row[33] = optional_usize(policy_input.datafusion_target_partitions);
    row[34] = optional_u64(policy_input.available_memory_bytes);
    row[35] = optional_u64(policy_input.unix_soft_file_descriptor_limit);
    row[36] = policy_input.file_descriptors_per_partition.to_string();
    row[37] = policy_input
        .available_memory_bytes_per_partition
        .to_string();
    row[38] = policy_decision.target_partitions.to_string();
    row[39] = policy_source_name(policy_decision.source).to_owned();
    row[40] = optional_usize(policy_decision.datafusion_target_cap);
    row[41] = optional_usize(policy_decision.unix_file_descriptor_cap);
    row[42] = optional_usize(policy_decision.memory_cap);
    row[43] = "false".to_owned();
    row[62] = optional_u64(input.local_environment.memory_total_bytes);
    row[63] = optional_u64(input.local_environment.memory_available_bytes);
    row[64] = optional_u64(input.local_environment.unix_soft_file_descriptor_limit);
    row[65] = unix_file_descriptor_status_name(
        input
            .local_environment
            .unix_soft_file_descriptor_limit_status,
    )
    .to_owned();
    row[66] = input.scheduler_probe.task_count.to_string();
    row[67] = input.scheduler_probe.completed_task_count.to_string();
    row[68] = input.scheduler_probe.concurrency.to_string();
    row[69] = input.scheduler_probe.total_micros.to_string();
    row[70] = input.scheduler_probe.nanos_per_task.to_string();
    row[71] = input.scheduler_probe.stable_concurrency_hint.to_string();
    row[72] = input.local_io_probe.enabled.to_string();
    row[73] = local_io_probe_status_name(input.local_io_probe.status).to_owned();
    row[74] = input.local_io_probe.repetitions.to_string();
    row[75] = input.local_io_probe.bytes_per_repetition.to_string();
    row[76] = input.local_io_probe.bytes_read.to_string();
    row[77] = optional_u64(input.local_io_probe.total_micros);
    row[78] = optional_u64(input.local_io_probe.latency_micros);
    row[79] = optional_u64(input.local_io_probe.throughput_bytes_per_second);

    row
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

fn unix_file_descriptor_status_name(
    status: DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus,
) -> &'static str {
    match status {
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unsupported => "unsupported",
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unknown => "unknown",
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Finite => "finite",
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unlimited => "unlimited",
    }
}

fn local_io_probe_status_name(status: HostLocalIoProbeStatus) -> &'static str {
    match status {
        HostLocalIoProbeStatus::Disabled => "disabled",
        HostLocalIoProbeStatus::Ok => "ok",
        HostLocalIoProbeStatus::Error => "error",
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
                trace_output_path: None,
                mode: BenchmarkMode::Synthetic,
                host_probe_local_io: HostProbeLocalIoConfig::default(),
                provider_exec: ProviderExecConfig::default(),
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
    fn runner_config_accepts_trace_output_path() -> Result<(), Box<dyn Error>> {
        let config =
            BenchmarkRunnerConfig::parse(["--trace-output", "target/scan-bench.trace.jsonl"])?;

        assert_eq!(
            config.trace_output_path,
            Some(PathBuf::from("target/scan-bench.trace.jsonl"))
        );

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
    fn runner_config_accepts_benchmark_mode() -> Result<(), Box<dyn Error>> {
        let synthetic = BenchmarkRunnerConfig::parse(["--mode", "synthetic"])?;
        let host_probe = BenchmarkRunnerConfig::parse(["--mode", "host-probe"])?;
        let host_probe_alias = BenchmarkRunnerConfig::parse(["--mode", "host_probe"])?;
        let provider_exec = BenchmarkRunnerConfig::parse(["--mode", "provider-exec"])?;
        let provider_exec_alias = BenchmarkRunnerConfig::parse(["--mode", "provider_exec"])?;

        assert_eq!(synthetic.mode, BenchmarkMode::Synthetic);
        assert_eq!(host_probe.mode, BenchmarkMode::HostProbe);
        assert_eq!(host_probe_alias.mode, BenchmarkMode::HostProbe);
        assert_eq!(provider_exec.mode, BenchmarkMode::ProviderExec);
        assert_eq!(provider_exec_alias.mode, BenchmarkMode::ProviderExec);

        Ok(())
    }

    #[test]
    fn runner_config_accepts_provider_exec_options() -> Result<(), Box<dyn Error>> {
        let config = BenchmarkRunnerConfig::parse([
            "--mode",
            "provider-exec",
            "--provider-exec-temp-dir",
            "target",
            "--provider-exec-repetitions",
            "5",
            "--provider-exec-storage-profile",
            "s3-normal",
            "--provider-exec-phase-aligned-workflow",
            "--provider-exec-workload",
            "provider_partitioned_event_log_12m",
            "--provider-exec-query",
            "count_events",
            "--provider-exec-backend",
            "native_async",
            "--provider-exec-scheduling-profile",
            "lazy_parallel_buffer_4",
        ])?;

        assert_eq!(config.mode, BenchmarkMode::ProviderExec);
        assert_eq!(
            config.provider_exec,
            ProviderExecConfig {
                repetitions: 5,
                temp_dir: Some(PathBuf::from("target")),
                storage_profile: ProviderExecStorageProfile::s3_normal(),
                default_case: false,
                phase_aligned_workflow: true,
                workload_filter: Some("provider_partitioned_event_log_12m".to_owned()),
                query_filter: Some("count_events".to_owned()),
                backend_filter: Some("native_async".to_owned()),
                scheduling_profile_filter: Some("lazy_parallel_buffer_4".to_owned()),
            }
        );

        Ok(())
    }

    #[test]
    fn runner_config_accepts_provider_exec_default_case() -> Result<(), Box<dyn Error>> {
        let config = BenchmarkRunnerConfig::parse([
            "--mode",
            "provider-exec",
            "--provider-exec-default-case",
        ])?;

        assert_eq!(config.mode, BenchmarkMode::ProviderExec);
        assert!(config.provider_exec.default_case);
        assert_eq!(
            config.provider_exec.workload_filter,
            Some(PROVIDER_EXEC_DEFAULT_CASE_WORKLOAD.to_owned())
        );
        assert_eq!(
            config.provider_exec.query_filter,
            Some(PROVIDER_EXEC_DEFAULT_CASE_QUERY.to_owned())
        );
        assert_eq!(config.provider_exec.backend_filter, None);
        assert_eq!(config.provider_exec.scheduling_profile_filter, None);

        Ok(())
    }

    #[test]
    fn runner_config_rejects_default_case_with_explicit_execution_filters() {
        assert_eq!(
            BenchmarkRunnerConfig::parse([
                "--provider-exec-default-case",
                "--provider-exec-backend",
                "native_async",
            ]),
            Err(BenchmarkRunnerConfigError::ProviderExecDefaultCaseConflict(
                "--provider-exec-backend"
            ))
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse([
                "--provider-exec-default-case",
                "--provider-exec-scheduling-profile",
                "lazy_parallel_buffer_1",
            ]),
            Err(BenchmarkRunnerConfigError::ProviderExecDefaultCaseConflict(
                "--provider-exec-scheduling-profile"
            ))
        );
    }

    #[test]
    fn runner_config_accepts_host_probe_local_io_options() -> Result<(), Box<dyn Error>> {
        let config = BenchmarkRunnerConfig::parse([
            "--mode",
            "host-probe",
            "--host-probe-local-io",
            "--host-probe-temp-dir",
            "target",
            "--host-probe-io-bytes",
            "4096",
            "--host-probe-io-repetitions",
            "2",
        ])?;

        assert_eq!(config.mode, BenchmarkMode::HostProbe);
        assert_eq!(
            config.host_probe_local_io,
            HostProbeLocalIoConfig {
                enabled: true,
                temp_dir: Some(PathBuf::from("target")),
                bytes_per_repetition: 4096,
                repetitions: 2,
            }
        );

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
            BenchmarkRunnerConfig::parse(["--trace-output"]),
            Err(BenchmarkRunnerConfigError::MissingTraceOutputPath)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse([
                "--trace-output",
                "a.jsonl",
                "--trace-output",
                "b.jsonl"
            ]),
            Err(BenchmarkRunnerConfigError::DuplicateTraceOutputPath)
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
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--mode"]),
            Err(BenchmarkRunnerConfigError::MissingMode)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--mode", "synthetic", "--mode", "host-probe"]),
            Err(BenchmarkRunnerConfigError::DuplicateMode)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--mode", "real-delta-scan"]),
            Err(BenchmarkRunnerConfigError::InvalidMode(
                "real-delta-scan".to_owned()
            ))
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--host-probe-temp-dir"]),
            Err(BenchmarkRunnerConfigError::MissingHostProbeTempDir)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse([
                "--host-probe-temp-dir",
                "a",
                "--host-probe-temp-dir",
                "b"
            ]),
            Err(BenchmarkRunnerConfigError::DuplicateHostProbeTempDir)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--host-probe-io-bytes"]),
            Err(BenchmarkRunnerConfigError::MissingHostProbeIoBytes)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--host-probe-io-bytes", "nope"]),
            Err(BenchmarkRunnerConfigError::InvalidHostProbeIoBytes(
                "nope".to_owned()
            ))
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--host-probe-io-bytes", "0"]),
            Err(BenchmarkRunnerConfigError::HostProbeIoBytesOutOfRange(0))
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--host-probe-io-repetitions"]),
            Err(BenchmarkRunnerConfigError::MissingHostProbeIoRepetitions)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--host-probe-io-repetitions", "nope"]),
            Err(BenchmarkRunnerConfigError::InvalidHostProbeIoRepetitions(
                "nope".to_owned()
            ))
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--host-probe-io-repetitions", "0"]),
            Err(BenchmarkRunnerConfigError::HostProbeIoRepetitionsOutOfRange(0))
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--provider-exec-temp-dir"]),
            Err(BenchmarkRunnerConfigError::MissingProviderExecTempDir)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse([
                "--provider-exec-temp-dir",
                "a",
                "--provider-exec-temp-dir",
                "b"
            ]),
            Err(BenchmarkRunnerConfigError::DuplicateProviderExecTempDir)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--provider-exec-repetitions"]),
            Err(BenchmarkRunnerConfigError::MissingProviderExecRepetitions)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--provider-exec-repetitions", "nope"]),
            Err(BenchmarkRunnerConfigError::InvalidProviderExecRepetitions(
                "nope".to_owned()
            ))
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--provider-exec-repetitions", "0"]),
            Err(BenchmarkRunnerConfigError::ProviderExecRepetitionsOutOfRange(0))
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--provider-exec-storage-profile"]),
            Err(BenchmarkRunnerConfigError::MissingProviderExecStorageProfile)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--provider-exec-storage-profile", "mars"]),
            Err(BenchmarkRunnerConfigError::InvalidProviderExecStorageProfile("mars".to_owned()))
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse([
                "--provider-exec-storage-profile",
                "local",
                "--provider-exec-storage-profile",
                "s3-normal"
            ]),
            Err(BenchmarkRunnerConfigError::DuplicateProviderExecStorageProfile)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--provider-exec-workload"]),
            Err(BenchmarkRunnerConfigError::MissingProviderExecWorkloadFilter)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse([
                "--provider-exec-workload",
                "a",
                "--provider-exec-workload",
                "b"
            ]),
            Err(BenchmarkRunnerConfigError::DuplicateProviderExecWorkloadFilter)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--provider-exec-query"]),
            Err(BenchmarkRunnerConfigError::MissingProviderExecQueryFilter)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse([
                "--provider-exec-query",
                "a",
                "--provider-exec-query",
                "b"
            ]),
            Err(BenchmarkRunnerConfigError::DuplicateProviderExecQueryFilter)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--provider-exec-backend"]),
            Err(BenchmarkRunnerConfigError::MissingProviderExecBackendFilter)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse([
                "--provider-exec-backend",
                "a",
                "--provider-exec-backend",
                "b"
            ]),
            Err(BenchmarkRunnerConfigError::DuplicateProviderExecBackendFilter)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse(["--provider-exec-scheduling-profile"]),
            Err(BenchmarkRunnerConfigError::MissingProviderExecSchedulingProfileFilter)
        );
        assert_eq!(
            BenchmarkRunnerConfig::parse([
                "--provider-exec-scheduling-profile",
                "a",
                "--provider-exec-scheduling-profile",
                "b"
            ]),
            Err(BenchmarkRunnerConfigError::DuplicateProviderExecSchedulingProfileFilter)
        );
    }

    #[test]
    fn print_usage_describes_output_path() -> Result<(), Box<dyn Error>> {
        let mut output = Vec::new();

        print_usage(&mut output)?;
        let usage = String::from_utf8(output)?;

        assert!(
            usage.contains(
                "Usage: delta_scan_partition_bench [--mode <synthetic|host-probe|provider-exec>] [--output <path>] [--seed <u64>]"
            )
        );
        assert!(usage.contains("CSV is written to stdout"));
        assert!(usage.contains("Use --trace-output"));
        assert!(usage.contains("The default mode is synthetic."));
        assert!(usage.contains("Use --host-probe-local-io"));
        assert!(usage.contains("Use --provider-exec-repetitions"));
        assert!(usage.contains("Use --provider-exec-storage-profile"));
        assert!(usage.contains("Use --provider-exec-workload"));
        assert!(usage.contains("Use --provider-exec-default-case"));
        assert!(usage.contains("Use --provider-exec-phase-aligned-workflow"));
        assert!(usage.contains("The default seed is 0."));

        Ok(())
    }

    #[test]
    fn write_benchmark_csv_outputs_portable_matrix() -> Result<(), Box<dyn Error>> {
        let mut output = Vec::new();
        let config = BenchmarkRunnerConfig {
            output_path: None,
            trace_output_path: None,
            mode: BenchmarkMode::Synthetic,
            host_probe_local_io: HostProbeLocalIoConfig::default(),
            provider_exec: ProviderExecConfig::default(),
            seed: 42,
            show_help: false,
        };

        write_benchmark_csv(&mut output, &config)?;
        let csv = String::from_utf8(output)?;
        let lines = csv.lines().collect::<Vec<_>>();

        assert_eq!(lines.len(), 1111);
        assert!(lines[0].starts_with("benchmark_schema_version,benchmark_mode,host_os,host_arch"));
        assert!(csv.contains(&format!("\n{BENCHMARK_SCHEMA_VERSION},synthetic,")));
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
    fn write_benchmark_csv_outputs_host_probe_profile() -> Result<(), Box<dyn Error>> {
        let mut output = Vec::new();
        let config = BenchmarkRunnerConfig {
            output_path: None,
            trace_output_path: None,
            mode: BenchmarkMode::HostProbe,
            host_probe_local_io: HostProbeLocalIoConfig::default(),
            provider_exec: ProviderExecConfig::default(),
            seed: 42,
            show_help: false,
        };

        write_benchmark_csv(&mut output, &config)?;
        let csv = String::from_utf8(output)?;
        let lines = csv.lines().collect::<Vec<_>>();
        let row = lines[1].split(',').collect::<Vec<_>>();

        assert_eq!(lines.len(), 2);
        assert_eq!(row.len(), BENCHMARK_CSV_HEADER.len());
        assert_eq!(row[0], BENCHMARK_SCHEMA_VERSION.to_string());
        assert_eq!(row[1], "host_probe");
        assert_eq!(row[5], "42");
        assert_eq!(row[6], "0");
        assert_eq!(row[7], "host_probe");
        assert_eq!(row[26], "0");
        assert_eq!(row[31], "host_probe_default_policy");
        assert!(!row[36].is_empty());
        assert!(!row[37].is_empty());
        assert!(!row[38].is_empty());
        assert_eq!(row[43], "false");
        assert!(matches!(
            row[65],
            "finite" | "unlimited" | "unknown" | "unsupported"
        ));
        assert!(!row[66].is_empty());
        assert!(!row[67].is_empty());
        assert!(!row[68].is_empty());
        assert!(!row[69].is_empty());
        assert!(!row[70].is_empty());
        assert!(!row[71].is_empty());
        assert_eq!(row[72], "false");
        assert_eq!(row[73], "disabled");
        assert_eq!(row[76], "0");
        assert_eq!(row[77], "");
        assert_eq!(row[78], "");
        assert_eq!(row[79], "");

        Ok(())
    }

    #[test]
    fn host_probe_csv_row_records_measured_policy_inputs() -> Result<(), Box<dyn Error>> {
        let local_environment = DeltaScanPartitionTargetLocalEnvironmentDiagnostic {
            policy_input: DeltaScanPartitionTargetDiagnosticInput {
                available_parallelism: Some(16),
                datafusion_target_partitions: Some(16),
                available_memory_bytes: Some(1024 * MIB),
                unix_soft_file_descriptor_limit: Some(128),
                ..DeltaScanPartitionTargetDiagnosticInput::default()
            },
            memory_total_bytes: Some(2048 * MIB),
            memory_available_bytes: Some(1024 * MIB),
            unix_soft_file_descriptor_limit: Some(128),
            unix_soft_file_descriptor_limit_status:
                DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Finite,
        };
        let policy_decision =
            derive_delta_scan_partition_target_diagnostic(local_environment.policy_input)?;
        let row = host_probe_csv_row(HostProbeCsvRowInput {
            run_environment: BenchmarkRunEnvironment {
                schema_version: BENCHMARK_SCHEMA_VERSION,
                host_os: "test-os",
                host_arch: "test-arch",
                available_parallelism: Some(16),
            },
            seed: 7,
            local_environment,
            scheduler_probe: HostSchedulerProbeResult {
                task_count: 512,
                completed_task_count: 512,
                concurrency: 2,
                total_micros: 123,
                nanos_per_task: 240,
                stable_concurrency_hint: 2,
            },
            local_io_probe: HostLocalIoProbeResult {
                enabled: true,
                status: HostLocalIoProbeStatus::Ok,
                repetitions: 2,
                bytes_per_repetition: 4096,
                bytes_read: 8192,
                total_micros: Some(100),
                latency_micros: Some(3),
                throughput_bytes_per_second: Some(81_920_000),
            },
            policy_decision,
        });

        assert_eq!(row.len(), BENCHMARK_CSV_HEADER.len());
        assert_eq!(row[0], BENCHMARK_SCHEMA_VERSION.to_string());
        assert_eq!(row[1], "host_probe");
        assert_eq!(row[2], "test-os");
        assert_eq!(row[3], "test-arch");
        assert_eq!(row[4], "16");
        assert_eq!(row[5], "7");
        assert_eq!(row[7], "host_probe");
        assert_eq!(row[31], "host_probe_default_policy");
        assert_eq!(row[32], "16");
        assert_eq!(row[33], "16");
        assert_eq!(row[34], (1024 * MIB).to_string());
        assert_eq!(row[35], "128");
        assert_eq!(row[38], "4");
        assert_eq!(row[41], "8");
        assert_eq!(row[42], "4");
        assert_eq!(row[62], (2048 * MIB).to_string());
        assert_eq!(row[63], (1024 * MIB).to_string());
        assert_eq!(row[64], "128");
        assert_eq!(row[65], "finite");
        assert_eq!(row[66], "512");
        assert_eq!(row[67], "512");
        assert_eq!(row[68], "2");
        assert_eq!(row[69], "123");
        assert_eq!(row[70], "240");
        assert_eq!(row[71], "2");
        assert_eq!(row[72], "true");
        assert_eq!(row[73], "ok");
        assert_eq!(row[74], "2");
        assert_eq!(row[75], "4096");
        assert_eq!(row[76], "8192");
        assert_eq!(row[77], "100");
        assert_eq!(row[78], "3");
        assert_eq!(row[79], "81920000");

        Ok(())
    }

    #[test]
    fn local_io_probe_status_renders_csv_fields() {
        assert_eq!(
            local_io_probe_status_name(HostLocalIoProbeStatus::Disabled),
            "disabled"
        );
        assert_eq!(local_io_probe_status_name(HostLocalIoProbeStatus::Ok), "ok");
        assert_eq!(
            local_io_probe_status_name(HostLocalIoProbeStatus::Error),
            "error"
        );
    }

    #[test]
    fn host_local_io_probe_stays_disabled_by_default() {
        let probe = run_host_local_io_probe(&HostProbeLocalIoConfig::default(), 0);

        assert!(!probe.enabled);
        assert_eq!(probe.status, HostLocalIoProbeStatus::Disabled);
        assert_eq!(probe.bytes_read, 0);
        assert_eq!(probe.total_micros, None);
        assert_eq!(probe.latency_micros, None);
        assert_eq!(probe.throughput_bytes_per_second, None);
    }

    #[test]
    fn host_local_io_probe_reads_temp_file() -> Result<(), Box<dyn Error>> {
        let temp_dir =
            env::temp_dir().join(format!("delta-funnel-test-local-io-{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir)?;
        let config = HostProbeLocalIoConfig {
            enabled: true,
            temp_dir: Some(temp_dir.clone()),
            bytes_per_repetition: 4096,
            repetitions: 2,
        };

        let probe = run_host_local_io_probe(&config, 99);

        assert!(probe.enabled);
        assert_eq!(probe.status, HostLocalIoProbeStatus::Ok);
        assert_eq!(probe.repetitions, 2);
        assert_eq!(probe.bytes_per_repetition, 4096);
        assert_eq!(probe.bytes_read, 8192);
        assert!(probe.total_micros.unwrap_or_default() > 0);
        assert!(probe.throughput_bytes_per_second.unwrap_or_default() > 0);
        assert!(
            !temp_dir
                .join(format!(
                    "delta-funnel-host-probe-{}-99.bin",
                    std::process::id()
                ))
                .exists()
        );
        let _ = fs::remove_dir_all(&temp_dir);

        Ok(())
    }

    #[test]
    fn host_scheduler_probe_uses_bounded_concurrency() {
        let probe = run_host_scheduler_probe(Some(HOST_PROBE_MAX_SCHEDULER_CONCURRENCY * 2));

        assert_eq!(probe.concurrency, HOST_PROBE_MAX_SCHEDULER_CONCURRENCY);
        assert_eq!(
            probe.task_count,
            HOST_PROBE_MAX_SCHEDULER_CONCURRENCY * HOST_PROBE_SCHEDULER_TASKS_PER_WORKER
        );
        assert_eq!(probe.completed_task_count, probe.task_count);
        assert_eq!(
            probe.stable_concurrency_hint,
            HOST_PROBE_MAX_SCHEDULER_CONCURRENCY
        );
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
    fn process_status_memory_kib_parses_linux_status_fields() {
        let status = [
            "Name:\tdelta_scan_partition_bench",
            "VmRSS:\t  1000 kB",
            "VmHWM:\t  2048 kB",
        ]
        .join("\n");

        assert_eq!(
            process_status_memory_kib_from_status(&status, "VmHWM"),
            Some(2048)
        );
        assert_eq!(
            process_status_memory_kib_from_status(&status, "VmSwap"),
            None
        );
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
    fn benchmark_mode_renders_csv_fields() {
        assert_eq!(BenchmarkMode::Synthetic.as_csv_value(), "synthetic");
        assert_eq!(BenchmarkMode::HostProbe.as_csv_value(), "host_probe");
        assert_eq!(BenchmarkMode::ProviderExec.as_csv_value(), "provider_exec");
    }

    #[test]
    fn provider_exec_csv_header_contains_latency_signals() {
        assert_eq!(PROVIDER_EXEC_CSV_HEADER[0], "benchmark_schema_version");
        assert_eq!(PROVIDER_EXEC_CSV_HEADER[1], "benchmark_mode");
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"provider_exec_storage_profile"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"scan_target_partitions"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"max_concurrent_file_reads_per_scan"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"max_concurrent_file_reads_per_partition"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"output_buffer_capacity_per_partition"));
        assert!(
            PROVIDER_EXEC_CSV_HEADER.contains(&"native_async_prefetch_file_count_per_partition")
        );
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"deletion_vector_file_count"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"deletion_vector_deleted_rows"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"deletion_vector_deleted_rows_per_file"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"provider_stats_scan_count"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"provider_stats_files_started_p50"));
        assert!(
            PROVIDER_EXEC_CSV_HEADER.contains(&"provider_stats_dynamic_partition_files_pruned_p50")
        );
        assert!(
            PROVIDER_EXEC_CSV_HEADER.contains(&"provider_stats_dynamic_partition_files_kept_p50")
        );
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"provider_stats_dynamic_filters_received_p50"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"provider_stats_dynamic_filters_accepted_p50"));
        assert!(
            PROVIDER_EXEC_CSV_HEADER.contains(&"provider_stats_dynamic_filters_unsupported_p50")
        );
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"provider_stats_dynamic_filter_snapshots_p50"));
        assert!(
            PROVIDER_EXEC_CSV_HEADER.contains(
                &"provider_stats_dynamic_partition_files_not_pruned_missing_metadata_p50"
            )
        );
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(
            &"provider_stats_dynamic_partition_files_not_pruned_unsupported_expression_p50"
        ));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"provider_stats_rows_produced_p50"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"process_peak_rss_bytes"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"process_peak_rss_delta_bytes"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"planning_micros_p99"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"time_to_first_batch_micros_p99"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"total_micros_p99"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"source_rows_per_second_p99"));
        assert!(PROVIDER_EXEC_CSV_HEADER.contains(&"batch_latency_micros_p99"));
    }

    #[test]
    fn provider_exec_cases_cover_non_dv_sparse_dv_mimic_and_predicate_queries()
    -> Result<(), Box<dyn Error>> {
        let workloads = ProviderExecWorkloadCase::standard_cases()?;
        let workload_names = workloads
            .iter()
            .map(|workload| workload.name)
            .collect::<Vec<_>>();
        let simple_query_cases = workloads[0].query_cases();
        let simple_query_names = simple_query_cases
            .iter()
            .map(|query| query.name)
            .collect::<Vec<_>>();
        let synthetic_query_cases = workloads[4].query_cases();
        let synthetic_query_names = synthetic_query_cases
            .iter()
            .map(|query| query.name)
            .collect::<Vec<_>>();
        let wide_query_cases = workloads[5].query_cases();
        let wide_query_names = wide_query_cases
            .iter()
            .map(|query| query.name)
            .collect::<Vec<_>>();
        let scheduling_profiles =
            ProviderExecSchedulingProfile::standard_cases(BenchmarkRunEnvironment {
                schema_version: BENCHMARK_SCHEMA_VERSION,
                host_os: "test-os",
                host_arch: "test-arch",
                available_parallelism: Some(8),
            });
        let scheduling_profile_names = scheduling_profiles
            .iter()
            .map(|profile| profile.name)
            .collect::<Vec<_>>();

        assert_eq!(
            workload_names,
            [
                "provider_many_small_files",
                "provider_few_larger_files",
                "provider_many_small_files_sparse_dv",
                "provider_few_larger_files_sparse_dv",
                "provider_partitioned_event_log_12m",
                "provider_wide_event_export_13m"
            ]
        );
        assert_eq!(workloads[0].deletion_vector_deleted_rows(), 0);
        assert_eq!(workloads[2].deletion_vector_deleted_rows(), 64);
        assert_eq!(workloads[3].deletion_vector_deleted_rows(), 12);
        assert_eq!(
            workloads[4].schema_kind,
            ProviderExecSchemaKind::SyntheticPartitionedEventLog
        );
        assert_eq!(workloads[4].file_count(), 956);
        assert_eq!(workloads[4].row_count(), 12_808_140);
        assert_eq!(
            workloads[5].schema_kind,
            ProviderExecSchemaKind::SyntheticWideEventExport
        );
        assert_eq!(workloads[5].file_count(), 1_204);
        assert_eq!(workloads[5].row_count(), 13_394_789);
        assert!(simple_query_names.contains(&"project_id"));
        assert!(simple_query_names.contains(&"count_rows"));
        assert!(simple_query_names.contains(&"filter_tail_ids"));
        assert!(synthetic_query_names.contains(&"project_event_keys"));
        assert!(synthetic_query_names.contains(&"count_events"));
        assert!(synthetic_query_names.contains(&"filter_recent_events"));
        assert!(wide_query_names.contains(&"project_primary_export"));
        assert!(wide_query_names.contains(&"project_secondary_export"));
        assert!(wide_query_names.contains(&"summary_export"));
        assert!(wide_query_cases[0].sql.contains("position_num"));
        assert!(wide_query_cases[0].sql.contains("event_time"));
        assert!(wide_query_cases[0].sql.contains("local_event_day"));
        assert!(wide_query_cases[0].sql.contains("metadata_ranked"));
        assert!(wide_query_cases[0].sql.contains("post_precedence"));
        assert!(
            wide_query_cases[1]
                .sql
                .contains("source_group = 'secondary'")
        );
        assert!(
            wide_query_cases[2]
                .sql
                .contains("COUNT(DISTINCT resolved_event_key)")
        );
        assert!(
            !wide_query_cases
                .iter()
                .any(|query| query.sql.contains("player"))
        );
        assert!(
            !wide_query_cases
                .iter()
                .any(|query| query.sql.contains("tracking"))
        );
        assert!(
            !wide_query_cases
                .iter()
                .any(|query| query.sql.contains("pitch"))
        );
        assert!(
            !wide_query_cases
                .iter()
                .any(|query| query.sql.contains("hawkeye"))
        );
        assert!(
            !wide_query_cases
                .iter()
                .any(|query| query.sql.contains("trackman"))
        );
        assert_eq!(
            scheduling_profile_names,
            [
                "lazy_serial_buffer_1",
                "lazy_parallel_buffer_1",
                "lazy_parallel_buffer_4",
                "prefetch_1_parallel_buffer_1",
                "prefetch_2_parallel_buffer_1",
                "prefetch_2_ap_target_scan_1x",
                "prefetch_2_ap_target_scan_2x",
                "prefetch_2_ap_target_scan_3x",
                "prefetch_2_ap_target_scan_4x"
            ]
        );
        assert_eq!(
            scheduling_profiles[2].output_buffer_capacity_per_partition,
            4
        );
        assert_eq!(
            scheduling_profiles[3].native_async_prefetch_file_count_per_partition,
            1
        );
        assert_eq!(
            scheduling_profiles[4].native_async_prefetch_file_count_per_partition,
            2
        );
        assert_eq!(scheduling_profiles[5].scan_target_partitions, Some(8));
        assert_eq!(
            scheduling_profiles[5].max_concurrent_file_reads_per_scan,
            Some(8)
        );
        assert_eq!(
            scheduling_profiles[5].max_concurrent_file_reads_per_partition,
            3
        );
        assert_eq!(
            scheduling_profiles[6].max_concurrent_file_reads_per_scan,
            Some(16)
        );
        assert_eq!(
            scheduling_profiles[7].max_concurrent_file_reads_per_scan,
            Some(24)
        );
        assert_eq!(
            scheduling_profiles[8].max_concurrent_file_reads_per_scan,
            Some(32)
        );
        Ok(())
    }

    #[test]
    fn provider_exec_default_case_uses_scan_execution_defaults() {
        let profile = ProviderExecSchedulingProfile::default_execution_case();
        let options = DeltaProviderScanExecutionOptions::default();

        assert_eq!(profile.name, PROVIDER_EXEC_DEFAULT_CASE_SCHEDULING_PROFILE);
        assert_eq!(profile.scan_target_partitions, None);
        assert_eq!(
            profile.max_concurrent_file_reads_per_scan,
            options.max_concurrent_file_reads_per_scan
        );
        assert_eq!(
            profile.max_concurrent_file_reads_per_partition,
            options.max_concurrent_file_reads_per_partition
        );
        assert_eq!(
            profile.output_buffer_capacity_per_partition,
            options.output_buffer_capacity_per_partition
        );
        assert_eq!(
            profile.native_async_prefetch_file_count_per_partition,
            options.native_async_prefetch_file_count_per_partition
        );
        assert!(profile.uses_default_execution_options);
    }

    #[test]
    fn provider_exec_synthetic_schema_matches_target_mimic_columns() -> Result<(), Box<dyn Error>> {
        let schema =
            provider_exec_arrow_schema(ProviderExecSchemaKind::SyntheticPartitionedEventLog);
        let field_names = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(schema.fields().len(), synthetic_columns().len());
        assert!(field_names.contains(&"primary_event_id"));
        assert!(field_names.contains(&"metric_x"));
        assert!(field_names.contains(&"event_year"));
        assert!(field_names.contains(&"validation_flag"));
        let event_time = schema.field_with_name("event_time")?;
        assert_eq!(
            event_time.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        Ok(())
    }

    #[test]
    fn provider_exec_wide_event_export_schema_is_sanitized_and_target_scaled()
    -> Result<(), Box<dyn Error>> {
        let shape = SyntheticDeltaTableShape::wide_event_export();
        let schema = provider_exec_arrow_schema(ProviderExecSchemaKind::SyntheticWideEventExport);
        let field_names = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(shape.total_rows, 13_394_789);
        assert_eq!(shape.active_file_count, 1_204);
        assert_eq!(shape.partitioning.partition_count, 933);
        assert_eq!(shape.source_split_rows(), shape.total_rows);
        assert_eq!(shape.row_distribution.source_split[0].rows, 12_378_915);
        assert_eq!(shape.row_distribution.source_split[1].rows, 1_015_874);
        assert_eq!(schema.fields().len(), 34);
        assert_eq!(
            wide_event_export_columns()
                .into_iter()
                .filter(|column| column.data_type == SyntheticDataType::String)
                .count(),
            11
        );
        assert!(field_names.contains(&"primary_event_id"));
        assert!(field_names.contains(&"position_num"));
        assert!(field_names.contains(&"local_event_day"));
        assert!(!field_names.iter().any(|name| name.contains("game")));
        assert!(!field_names.iter().any(|name| name.contains("pitch")));
        assert!(!field_names.iter().any(|name| name.contains("fielder")));
        assert!(!field_names.iter().any(|name| name.contains("hawkeye")));
        assert!(!field_names.iter().any(|name| name.contains("trackman")));

        Ok(())
    }

    #[test]
    fn provider_exec_creates_synthetic_mimic_delta_table() -> Result<(), Box<dyn Error>> {
        let partition_date = SyntheticDate {
            year: 2026,
            month: 6,
            day: 12,
        };
        let workload = ProviderExecWorkloadCase {
            name: "test_provider_synthetic_mimic",
            schema_kind: ProviderExecSchemaKind::SyntheticPartitionedEventLog,
            file_specs: vec![ProviderExecFileSpec {
                path: synthetic_file_path(partition_date, 0),
                rows: 16,
                partition_date: Some(partition_date),
            }],
            deleted_row_indexes_per_file: &[],
        };

        let table = ProviderExecDeltaTable::create(
            &env::temp_dir(),
            &workload,
            ProviderExecStorageProfile::local(),
        )?;
        let source = load_delta_source_with_tracing(
            DeltaSourceConfig::new("orders", table.table_uri.clone())
                .with_storage_options(table.storage_options.clone()),
        )?;
        let _protocol = preflight_delta_protocol_with_tracing(&source)?;
        let metadata_log =
            fs::read_to_string(table.path.join("_delta_log/00000000000000000000.json"))?;

        assert_eq!(table.file_count, 1);
        assert_eq!(table.row_count, 16);
        assert!(metadata_log.contains("primary_event_id"));
        assert!(metadata_log.contains("validation_flag"));
        assert!(
            table
                .path
                .join(synthetic_file_path(partition_date, 0))
                .exists()
        );
        Ok(())
    }

    #[test]
    fn provider_exec_creates_wide_event_export_delta_table() -> Result<(), Box<dyn Error>> {
        let partition_date = SyntheticDate {
            year: 2026,
            month: 6,
            day: 12,
        };
        let workload = ProviderExecWorkloadCase {
            name: "test_provider_wide_event_export",
            schema_kind: ProviderExecSchemaKind::SyntheticWideEventExport,
            file_specs: vec![ProviderExecFileSpec {
                path: synthetic_file_path(partition_date, 0),
                rows: 16,
                partition_date: Some(partition_date),
            }],
            deleted_row_indexes_per_file: &[],
        };

        let table = ProviderExecDeltaTable::create(
            &env::temp_dir(),
            &workload,
            ProviderExecStorageProfile::local(),
        )?;
        let source = load_delta_source_with_tracing(
            DeltaSourceConfig::new("orders", table.table_uri.clone())
                .with_storage_options(table.storage_options.clone()),
        )?;
        let _protocol = preflight_delta_protocol_with_tracing(&source)?;
        let metadata_log =
            fs::read_to_string(table.path.join("_delta_log/00000000000000000000.json"))?;

        assert_eq!(table.file_count, 1);
        assert_eq!(table.row_count, 16);
        assert!(metadata_log.contains("primary_event_id"));
        assert!(metadata_log.contains("position_num"));
        assert!(metadata_log.contains("local_event_day"));
        assert!(
            table
                .path
                .join(synthetic_file_path(partition_date, 0))
                .exists()
        );
        Ok(())
    }

    #[test]
    fn provider_exec_reads_synthetic_mimic_table_with_both_backends() -> Result<(), Box<dyn Error>>
    {
        let partition_date = SyntheticDate {
            year: 2026,
            month: 6,
            day: 12,
        };
        let workload = ProviderExecWorkloadCase {
            name: "test_provider_synthetic_mimic_read",
            schema_kind: ProviderExecSchemaKind::SyntheticPartitionedEventLog,
            file_specs: vec![ProviderExecFileSpec {
                path: synthetic_file_path(partition_date, 0),
                rows: 16,
                partition_date: Some(partition_date),
            }],
            deleted_row_indexes_per_file: &[],
        };
        let table = ProviderExecDeltaTable::create(
            &env::temp_dir(),
            &workload,
            ProviderExecStorageProfile::local(),
        )?;
        let query = workload.query_cases()[0];
        let scheduling_profile = ProviderExecSchedulingProfile {
            name: "test_lazy_serial",
            scan_target_partitions: Some(1),
            max_concurrent_file_reads_per_scan: Some(1),
            max_concurrent_file_reads_per_partition: 1,
            output_buffer_capacity_per_partition: 1,
            native_async_prefetch_file_count_per_partition: 0,
            uses_default_execution_options: false,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        for backend in [
            DeltaProviderReaderBackend::OfficialKernel,
            DeltaProviderReaderBackend::NativeAsync,
        ] {
            let measurement = runtime.block_on(run_provider_exec_once(
                &table,
                query,
                backend,
                scheduling_profile,
                0,
            ))?;
            assert_eq!(measurement.produced_rows, 16);
            assert!(measurement.produced_batches > 0);
        }

        Ok(())
    }

    #[test]
    fn provider_exec_reads_synthetic_mimic_table_with_default_execution_case()
    -> Result<(), Box<dyn Error>> {
        let partition_date = SyntheticDate {
            year: 2026,
            month: 6,
            day: 12,
        };
        let workload = ProviderExecWorkloadCase {
            name: "test_provider_synthetic_mimic_default_execution",
            schema_kind: ProviderExecSchemaKind::SyntheticPartitionedEventLog,
            file_specs: vec![ProviderExecFileSpec {
                path: synthetic_file_path(partition_date, 0),
                rows: 16,
                partition_date: Some(partition_date),
            }],
            deleted_row_indexes_per_file: &[],
        };
        let table = ProviderExecDeltaTable::create(
            &env::temp_dir(),
            &workload,
            ProviderExecStorageProfile::local(),
        )?;
        let query = workload.query_cases()[0];
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let measurement = runtime.block_on(run_provider_exec_once(
            &table,
            query,
            DeltaProviderScanExecutionOptions::default().reader_backend,
            ProviderExecSchedulingProfile::default_execution_case(),
            0,
        ))?;

        assert_eq!(measurement.produced_rows, 16);
        assert!(measurement.produced_batches > 0);

        Ok(())
    }

    #[test]
    fn provider_exec_reads_synthetic_mimic_table_over_delayed_http() -> Result<(), Box<dyn Error>> {
        let partition_date = SyntheticDate {
            year: 2026,
            month: 6,
            day: 12,
        };
        let workload = ProviderExecWorkloadCase {
            name: "test_provider_synthetic_mimic_delayed_http",
            schema_kind: ProviderExecSchemaKind::SyntheticPartitionedEventLog,
            file_specs: vec![ProviderExecFileSpec {
                path: synthetic_file_path(partition_date, 0),
                rows: 16,
                partition_date: Some(partition_date),
            }],
            deleted_row_indexes_per_file: &[],
        };
        let table = ProviderExecDeltaTable::create(
            &env::temp_dir(),
            &workload,
            ProviderExecStorageProfile {
                name: "test_delayed_http",
                open_latency_micros: 1_000,
                read_latency_micros: 1_000,
                bandwidth_bytes_per_second: None,
            },
        )?;
        let query = workload.query_cases()[1];
        let scheduling_profile = ProviderExecSchedulingProfile {
            name: "test_lazy_serial",
            scan_target_partitions: Some(1),
            max_concurrent_file_reads_per_scan: Some(1),
            max_concurrent_file_reads_per_partition: 1,
            output_buffer_capacity_per_partition: 1,
            native_async_prefetch_file_count_per_partition: 0,
            uses_default_execution_options: false,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        assert!(table.table_uri.starts_with("http://127.0.0.1:"));
        assert_eq!(
            table.storage_options.get("allow_http").map(String::as_str),
            Some("true")
        );
        for backend in [
            DeltaProviderReaderBackend::OfficialKernel,
            DeltaProviderReaderBackend::NativeAsync,
        ] {
            let measurement = runtime.block_on(run_provider_exec_once(
                &table,
                query,
                backend,
                scheduling_profile,
                0,
            ))?;
            assert_eq!(measurement.produced_rows, 1);
            assert_eq!(measurement.produced_batches, 1);
        }

        Ok(())
    }

    #[test]
    fn provider_exec_csv_row_matches_header_width_and_dv_fields() {
        let workload = ProviderExecWorkloadCase {
            name: "test_sparse_dv",
            schema_kind: ProviderExecSchemaKind::SimpleOrders,
            file_specs: vec![
                ProviderExecFileSpec {
                    path: "part-00000.parquet".to_owned(),
                    rows: 8,
                    partition_date: None,
                },
                ProviderExecFileSpec {
                    path: "part-00001.parquet".to_owned(),
                    rows: 8,
                    partition_date: None,
                },
            ],
            deleted_row_indexes_per_file: &[1, 7],
        };
        let table = ProviderExecDeltaTable {
            path: PathBuf::from("target/nonexistent-provider-exec-test"),
            table_uri: "target/nonexistent-provider-exec-test".to_owned(),
            storage_options: DeltaStorageOptions::default(),
            storage_profile: ProviderExecStorageProfile::local(),
            delayed_http_server: None,
            file_count: 2,
            row_count: 16,
            data_file_bytes: 1024,
            deletion_vector_file_count: 2,
            deletion_vector_deleted_rows: 4,
            deletion_vector_deleted_rows_per_file: 2,
        };
        let summary = ProviderExecSummary {
            repetitions: 3,
            produced_rows: 12,
            produced_batches: 2,
            planning_micros: PercentileSummary {
                p50: 10,
                p95: 11,
                p99: 12,
            },
            time_to_first_batch_micros: PercentileSummary {
                p50: 20,
                p95: 21,
                p99: 22,
            },
            total_micros: PercentileSummary {
                p50: 30,
                p95: 31,
                p99: 32,
            },
            source_rows_per_second: PercentileSummary {
                p50: 40,
                p95: 41,
                p99: 42,
            },
            batch_latency_micros: PercentileSummary {
                p50: 50,
                p95: 51,
                p99: 52,
            },
            process_peak_rss_bytes: Some(4096),
            process_peak_rss_delta_bytes: Some(1024),
            min_total_micros: 29,
            max_total_micros: 33,
            read_stats: ProviderExecReadStatsSummary {
                scan_count: 1,
                scan_metadata_exhausted: ProviderExecScanMetadataExhausted::True,
                scan_partitions_planned: 4,
                files_planned: 2,
                estimated_rows: Some(16),
                estimated_bytes: Some(1024),
                scan_partitions_started: 4,
                scan_partitions_completed: 4,
                files_started: 2,
                files_completed: 2,
                dynamic_partition_files_pruned: 3,
                dynamic_partition_files_kept: 5,
                dynamic_filters_received: 7,
                dynamic_filters_accepted: 4,
                dynamic_filters_unsupported: 3,
                dynamic_filter_snapshots: 9,
                dynamic_partition_files_not_pruned_missing_metadata: 2,
                dynamic_partition_files_not_pruned_unsupported_expression: 1,
                batches_produced: 2,
                rows_produced: 12,
                deletion_vector_payloads_loaded: 2,
                deletion_vectors_applied: 2,
                deletion_vector_rows_deleted: 4,
                deletion_vector_failures: 0,
                deletion_vector_rejections: 0,
            },
        };
        let row = provider_exec_csv_row(ProviderExecCsvRowInput {
            run_environment: BenchmarkRunEnvironment {
                schema_version: BENCHMARK_SCHEMA_VERSION,
                host_os: "test-os",
                host_arch: "test-arch",
                available_parallelism: Some(8),
            },
            seed: 7,
            workload_case_count: 5,
            table: &table,
            workload: &workload,
            query: ProviderExecQueryCase {
                name: "project_id",
                sql: "select id from orders",
            },
            backend: DeltaProviderReaderBackend::NativeAsync,
            scheduling_profile: ProviderExecSchedulingProfile {
                name: "lazy_parallel_buffer_4",
                scan_target_partitions: Some(4),
                max_concurrent_file_reads_per_scan: Some(4),
                max_concurrent_file_reads_per_partition: 1,
                output_buffer_capacity_per_partition: 4,
                native_async_prefetch_file_count_per_partition: 0,
                uses_default_execution_options: false,
            },
            summary: &summary,
        });

        assert_eq!(row.len(), PROVIDER_EXEC_CSV_HEADER.len());
        assert_eq!(row[0], BENCHMARK_SCHEMA_VERSION.to_string());
        assert_eq!(row[7], "test_sparse_dv");
        assert_eq!(row[8], "local");
        assert_eq!(row[10], "native_async");
        assert_eq!(row[11], "lazy_parallel_buffer_4");
        assert_eq!(row[12], "4");
        assert_eq!(row[13], "4");
        assert_eq!(row[14], "1");
        assert_eq!(row[15], "4");
        assert_eq!(row[16], "0");
        assert_eq!(row[21], "2");
        assert_eq!(row[22], "4");
        assert_eq!(row[23], "2");
        assert_eq!(row[24], "1");
        assert_eq!(row[25], "true");
        assert_eq!(row[28], "16");
        assert_eq!(row[32], "2");
        assert_eq!(row[34], "3");
        assert_eq!(row[35], "5");
        assert_eq!(row[36], "7");
        assert_eq!(row[37], "4");
        assert_eq!(row[38], "3");
        assert_eq!(row[39], "9");
        assert_eq!(row[40], "2");
        assert_eq!(row[41], "1");
        assert_eq!(row[43], "12");
        assert_eq!(row[49], "12");
        assert_eq!(row[51], "4096");
        assert_eq!(row[52], "1024");
    }

    #[test]
    fn benchmark_csv_header_matches_policy_output_shape() {
        assert_eq!(BENCHMARK_CSV_HEADER.len(), 80);
        assert_eq!(BENCHMARK_CSV_HEADER[0], "benchmark_schema_version");
        assert_eq!(BENCHMARK_CSV_HEADER[1], "benchmark_mode");
        assert_eq!(BENCHMARK_CSV_HEADER[2], "host_os");
        assert_eq!(BENCHMARK_CSV_HEADER[3], "host_arch");
        assert_eq!(BENCHMARK_CSV_HEADER[4], "host_available_parallelism");
        assert_eq!(BENCHMARK_CSV_HEADER[5], "seed");
        assert_eq!(BENCHMARK_CSV_HEADER[6], "workload_case_count");
        assert_eq!(BENCHMARK_CSV_HEADER[7], "workload_case");
        assert_eq!(
            BENCHMARK_CSV_HEADER[28],
            "simulation_partition_scheduling_overhead_micros"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[29], "simulation_effective_parallelism");
        assert_eq!(
            BENCHMARK_CSV_HEADER[30],
            "simulation_aggregate_bandwidth_bytes_per_second"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[31], "policy_case");
        assert_eq!(BENCHMARK_CSV_HEADER[32], "policy_available_parallelism");
        assert_eq!(BENCHMARK_CSV_HEADER[33], "policy_datafusion_target");
        assert_eq!(BENCHMARK_CSV_HEADER[34], "policy_available_memory_bytes");
        assert_eq!(BENCHMARK_CSV_HEADER[35], "policy_unix_soft_fd_limit");
        assert_eq!(BENCHMARK_CSV_HEADER[38], "policy_target");
        assert_eq!(BENCHMARK_CSV_HEADER[39], "policy_source");
        assert_eq!(BENCHMARK_CSV_HEADER[43], "unknown_size_fallback_used");
        assert_eq!(
            BENCHMARK_CSV_HEADER[47],
            "simulated_scheduling_overhead_micros"
        );
        assert_eq!(
            BENCHMARK_CSV_HEADER[48],
            "simulated_aggregate_transfer_floor_micros"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[49], "simulated_execution_slots");
        assert_eq!(
            BENCHMARK_CSV_HEADER[51],
            "simulated_throughput_mib_per_second"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[53], "partition_files_p50");
        assert_eq!(
            BENCHMARK_CSV_HEADER[61],
            "partition_work_imbalance_basis_points"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[62], "host_memory_total_bytes");
        assert_eq!(BENCHMARK_CSV_HEADER[63], "host_memory_available_bytes");
        assert_eq!(BENCHMARK_CSV_HEADER[64], "host_unix_soft_fd_limit");
        assert_eq!(BENCHMARK_CSV_HEADER[65], "host_unix_soft_fd_limit_status");
        assert_eq!(BENCHMARK_CSV_HEADER[66], "host_scheduler_probe_task_count");
        assert_eq!(
            BENCHMARK_CSV_HEADER[67],
            "host_scheduler_probe_completed_task_count"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[68], "host_scheduler_probe_concurrency");
        assert_eq!(
            BENCHMARK_CSV_HEADER[69],
            "host_scheduler_probe_total_micros"
        );
        assert_eq!(
            BENCHMARK_CSV_HEADER[70],
            "host_scheduler_probe_nanos_per_task"
        );
        assert_eq!(
            BENCHMARK_CSV_HEADER[71],
            "host_runtime_probe_stable_concurrency_hint"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[72], "host_local_io_probe_enabled");
        assert_eq!(BENCHMARK_CSV_HEADER[73], "host_local_io_probe_status");
        assert_eq!(BENCHMARK_CSV_HEADER[74], "host_local_io_probe_repetitions");
        assert_eq!(
            BENCHMARK_CSV_HEADER[75],
            "host_local_io_probe_bytes_per_repetition"
        );
        assert_eq!(BENCHMARK_CSV_HEADER[76], "host_local_io_probe_bytes_read");
        assert_eq!(BENCHMARK_CSV_HEADER[77], "host_local_io_probe_total_micros");
        assert_eq!(
            BENCHMARK_CSV_HEADER[78],
            "host_local_io_probe_latency_micros"
        );
        assert_eq!(
            BENCHMARK_CSV_HEADER[79],
            "host_local_io_probe_throughput_bytes_per_second"
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
            mode: BenchmarkMode::Synthetic,
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
        assert_eq!(row[0], BENCHMARK_SCHEMA_VERSION.to_string());
        assert_eq!(row[1], "synthetic");
        assert_eq!(row[2], "test-os");
        assert_eq!(row[3], "test-arch");
        assert_eq!(row[4], "16");
        assert_eq!(row[5], "7");
        assert_eq!(row[6], "6");
        assert_eq!(row[7], "test-workload");
        assert_eq!(row[28], "1000");
        assert_eq!(row[29], "32");
        assert_eq!(row[30], "131072000");
        assert_eq!(row[31], "default_policy");
        assert_eq!(row[38], "16");
        assert_eq!(row[39], "available_parallelism_fallback");
        assert_eq!(row[43], "false");
        assert_eq!(row[47], "16000");
        assert!(!row[48].is_empty());
        assert_eq!(row[49], "16");
        assert!(!row[51].is_empty());
        assert!(!row[52].is_empty());
        assert!(!row[61].is_empty());
        assert_eq!(row[62], "");
        assert_eq!(row[63], "");
        assert_eq!(row[64], "");
        assert_eq!(row[65], "");
        assert_eq!(row[66], "");
        assert_eq!(row[67], "");
        assert_eq!(row[68], "");
        assert_eq!(row[69], "");
        assert_eq!(row[70], "");
        assert_eq!(row[71], "");
        assert_eq!(row[72], "");
        assert_eq!(row[73], "");
        assert_eq!(row[74], "");
        assert_eq!(row[75], "");
        assert_eq!(row[76], "");
        assert_eq!(row[77], "");
        assert_eq!(row[78], "");
        assert_eq!(row[79], "");

        Ok(())
    }
}
