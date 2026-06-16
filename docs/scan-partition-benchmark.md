# Scan Partition Benchmark

`delta_scan_partition_bench` is a portable benchmark runner for DeltaFunnel
scan planning and provider execution. Its default mode is the deterministic
synthetic matrix.

The production policy is documented in
[`scan-partition-target-policy.md`](scan-partition-target-policy.md).

The synthetic mode does not read Parquet data, contact object storage, require
S3 credentials, or execute production scan reads. It builds deterministic
Delta-like file tasks in memory, runs the production target policy through the
diagnostic facade, simulates scan work, groups files by estimated bytes or the
unknown-size file-count fallback, and writes a CSV matrix.

## Run

Write CSV to stdout:

```bash
cargo run -p delta-funnel --bin delta_scan_partition_bench
```

Write CSV to a file:

```bash
cargo run -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode synthetic \
  --output target/delta-scan-partition-bench.csv
```

Run the cheap host profile probe:

```bash
cargo run -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode host-probe \
  --output target/delta-scan-partition-host-probe.csv
```

Host-probe mode currently records cheap local host signals and runs a bounded
in-process scheduler probe before feeding the host profile through the same
production diagnostic target policy. It does not run local IO probes unless
explicitly requested.

Run the real provider execution benchmark:

```bash
cargo run -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode provider-exec \
  --provider-exec-repetitions 3 \
  --output target/delta-provider-exec-bench.csv
```

Provider-exec mode creates temporary local Delta tables with Parquet data,
loads them through `load_delta_source`, registers them through production
DataFusion provider registration, selects the requested provider backend through
`DeltaProviderScanExecutionOptions`, runs DataFusion SQL, and collects real
provider execution output. It does not use a benchmark-only reader path.

The provider-exec matrix compares the official kernel backend against the
native async backend with lazy scheduling. It covers non-DV and sparse-DV
versions of a many-small-files shape, a fewer-larger-files shape, and a
12,808,140-row `provider_partitioned_event_log_12m` shape based on the
synthetic partitioned event log model. Each workload runs a projection query, a
count-style query, and a predicate query that exercises provider predicate
handling. Each case runs production scheduling profiles for serial lazy reads,
parallel lazy reads, and parallel lazy reads with a larger bounded output
handoff buffer. Later #161 slices should extend this same mode with a bounded
prefetch scheduler only if that variant exists in the production native
execution path.

Provider-exec mode defaults to local filesystem reads. To compare production
provider execution under remote-like storage latency, use an opt-in delayed
HTTP storage facade:

```bash
cargo run -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode provider-exec \
  --provider-exec-storage-profile s3-normal \
  --provider-exec-workload provider_partitioned_event_log_12m \
  --provider-exec-query project_event_keys \
  --provider-exec-backend native_async \
  --provider-exec-scheduling-profile lazy_parallel_buffer_4 \
  --provider-exec-repetitions 1 \
  --output target/delta-provider-exec-s3-normal-project.csv
```

Supported provider-exec storage profiles are:

- `local`: default local filesystem reads.
- `s3-normal`: delayed HTTP reads with moderate request latency and about 1
  Gbps per-request transfer bandwidth.
- `s3-high-latency`: delayed HTTP reads with higher request latency.
- `s3-throttled`: delayed HTTP reads with remote-like latency and lower
  per-request transfer bandwidth.

The delayed HTTP facade is benchmark-only and read-only. It serves the generated
temporary Delta table through generic HTTP/WebDAV-style object-store requests,
including `PROPFIND`, `HEAD`, `GET`, and byte ranges. Both provider backends use
the same delayed HTTP table URI and `allow_http=true` storage option.

Run the opt-in local IO read probe:

```bash
cargo run -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode host-probe \
  --host-probe-local-io \
  --host-probe-io-bytes 1048576 \
  --host-probe-io-repetitions 3 \
  --output target/delta-scan-partition-host-probe-io.csv
```

The local IO probe writes one temporary file, syncs it, reads it repeatedly,
records first-read latency and aggregate read throughput, then removes the temp
file on a best-effort basis. Use `--host-probe-temp-dir <path>` to select the
temporary directory. The probe is bounded: bytes per repetition must be between
1 and 64 MiB, and repetitions must be between 1 and 128.

Use a deterministic jitter seed:

```bash
cargo run -p delta-funnel --bin delta_scan_partition_bench -- \
  --seed 42 \
  --output target/delta-scan-partition-bench-seed-42.csv
```

The default seed is `0`. The seed affects deterministic simulated work jitter.
It does not change workload file shapes or policy target derivation. In
provider-exec mode the seed is recorded for run metadata, but the initial local
Delta workload shapes are deterministic.

## Synthetic Matrix

The default matrix currently covers:

- 1110 data rows:
  - 6 workloads x 5 simulation profiles x 37 policy cases.

- Workloads:
  - `partitioned_event_log_target_shape`: target synthetic table shape, 956
    files, about 393 MiB, 933 daily partitions.
  - `many_tiny_files`: 4096 files, 64 KiB per file, small-file latency pressure.
  - `mixed_tiny_large_files`: 1024 tiny files plus 16 large files, mixed size
    grouping pressure.
  - `highly_skewed_files`: one 2 GiB file plus 255 small files, max-file
    dominated grouping pressure.
  - `unknown_size_files`: 1024 files with real simulated bytes but missing size
    estimates, forcing the file-count fallback grouping path.
  - `zero_byte_files`: 512 selected zero-byte files, useful for scheduler and
    per-file overhead sensitivity.
- Simulation profiles:
  - `local_fast`: low latency, local-like bandwidth, 16 effective execution
    slots.
  - `s3_normal`: moderate request latency, about 1 Gbps aggregate bandwidth,
    32 effective execution slots.
  - `s3_high_latency`: high request latency, 64 effective execution slots.
  - `s3_throttled`: remote-like request latency, 100 Mbps aggregate bandwidth,
    16 effective execution slots.
  - `cpu_heavy`: lower latency but higher per-row CPU cost, 16 effective
    execution slots.
- Policy cases:
  - `default_policy`
  - `fixed_target_1`, `4`, `8`, `16`, `32`, `64`
  - `available_parallelism_uncapped`
  - `available_parallelism_x2_uncapped`
  - `datafusion_cap_4`
  - `available_parallelism_override_4`, `16`, `64`
  - `fd_per_partition_4`, `8`, `16`, `32`
  - `memory_per_partition_64mib`, `128mib`, `256mib`, `512mib`
  - all fd-per-partition x memory-per-partition combined cases

Correctness-only edge shapes such as empty scans, one-file scans, and few-file
scans stay in unit tests and are not part of the default performance matrix.

## Output

The runner writes one CSV header and one row per matrix case.

Important field groups:

- Run metadata:
  - `benchmark_schema_version`
  - `benchmark_mode`
  - `host_os`
  - `host_arch`
  - `host_available_parallelism`
  - `seed`
- Workload shape:
  - `workload_case`
  - `active_files`
  - `active_bytes`
  - `generated_files`
  - `generated_bytes`
- Policy inputs and result:
  - `policy_case`
  - `policy_available_parallelism`
  - `policy_datafusion_target`
  - `policy_fd_per_partition`
  - `policy_memory_bytes_per_partition`
  - `policy_target`
  - `policy_source`
  - `policy_datafusion_cap`
  - `policy_unix_fd_cap`
  - `policy_memory_cap`
  - `unknown_size_fallback_used`
- Simulated execution:
  - `simulation_partition_scheduling_overhead_micros`
  - `simulation_effective_parallelism`
  - `simulation_aggregate_bandwidth_bytes_per_second`
  - `simulated_serial_micros`
  - `simulated_max_file_micros`
  - `simulated_output_partitions`
  - `simulated_scheduling_overhead_micros`
  - `simulated_aggregate_transfer_floor_micros`
  - `simulated_execution_slots`
  - `simulated_wall_micros`
  - `simulated_throughput_mib_per_second`
  - `simulated_rows_per_second`
- Grouping balance:
  - `partition_files_p50`
  - `partition_files_p95`
  - `partition_files_max`
  - `partition_bytes_p50`
  - `partition_bytes_p95`
  - `partition_bytes_max`
  - `partition_work_micros_p50`
  - `partition_work_micros_p95`
  - `partition_work_imbalance_basis_points`
- Host-probe diagnostics:
  - `host_memory_total_bytes`
  - `host_memory_available_bytes`
  - `host_unix_soft_fd_limit`
  - `host_unix_soft_fd_limit_status`
  - `host_scheduler_probe_task_count`
  - `host_scheduler_probe_completed_task_count`
  - `host_scheduler_probe_concurrency`
  - `host_scheduler_probe_total_micros`
  - `host_scheduler_probe_nanos_per_task`
  - `host_runtime_probe_stable_concurrency_hint`
  - `host_local_io_probe_enabled`
  - `host_local_io_probe_status`
  - `host_local_io_probe_repetitions`
  - `host_local_io_probe_bytes_per_repetition`
  - `host_local_io_probe_bytes_read`
  - `host_local_io_probe_total_micros`
  - `host_local_io_probe_latency_micros`
  - `host_local_io_probe_throughput_bytes_per_second`

Provider-exec mode writes a provider execution CSV with a separate header. Its
important field groups are:

- Run metadata:
  - `benchmark_schema_version`
  - `benchmark_mode`
  - `host_os`
  - `host_arch`
  - `host_available_parallelism`
  - `seed`
- Workload and execution shape:
  - `workload_case`
  - `provider_exec_storage_profile`
  - `query_case`
  - `reader_backend`
  - `scheduling_mode`
  - `scan_target_partitions`
  - `max_concurrent_file_reads_per_scan`
  - `max_concurrent_file_reads_per_partition`
  - `output_buffer_capacity_per_partition`
  - `repetitions`
  - `file_count`
  - `row_count`
  - `data_file_bytes`
  - `deletion_vector_file_count`
  - `deletion_vector_deleted_rows`
  - `deletion_vector_deleted_rows_per_file`
- Provider read stats:
  - `provider_stats_scan_count`
  - `provider_stats_scan_metadata_exhausted`
  - `provider_stats_scan_partitions_planned`
  - `provider_stats_files_planned`
  - `provider_stats_estimated_rows`
  - `provider_stats_estimated_bytes`
  - `provider_stats_scan_partitions_started_p50`
  - `provider_stats_scan_partitions_completed_p50`
  - `provider_stats_files_started_p50`
  - `provider_stats_files_completed_p50`
  - `provider_stats_batches_produced_p50`
  - `provider_stats_rows_produced_p50`
  - `provider_stats_deletion_vector_payloads_loaded_p50`
  - `provider_stats_deletion_vectors_applied_p50`
  - `provider_stats_deletion_vector_rows_deleted_p50`
  - `provider_stats_deletion_vector_failures_p50`
  - `provider_stats_deletion_vector_rejections_p50`
- Output shape:
  - `produced_rows`
  - `produced_batches`
- Latency and throughput:
  - `planning_micros_p50`, `planning_micros_p95`, `planning_micros_p99`
  - `time_to_first_batch_micros_p50`, `time_to_first_batch_micros_p95`,
    `time_to_first_batch_micros_p99`
  - `total_micros_p50`, `total_micros_p95`, `total_micros_p99`
  - `source_rows_per_second_p50`, `source_rows_per_second_p95`,
    `source_rows_per_second_p99`
  - `batch_latency_micros_p50`, `batch_latency_micros_p95`,
    `batch_latency_micros_p99`
  - `min_total_micros`
  - `max_total_micros`

`partition_work_imbalance_basis_points` is:

```text
max_partition_work / average_partition_work * 10000
```

`10000` means perfectly balanced simulated partition work. Higher values mean
more imbalance.

## Cross-machine comparison

Run the same command on each machine and keep the CSV output:

```bash
cargo run -p delta-funnel --bin delta_scan_partition_bench -- \
  --seed 0 \
  --output bench-$(uname -s)-$(uname -m).csv
```

Compare rows with the same `workload_case`, `simulation_profile`, and
`policy_case`. The host metadata columns show which machine produced each row.
The `available_parallelism_override_*` policy cases keep fixed parallelism
inputs in the CSV, which makes some rows directly comparable even when machines
have different local core counts.

The policy variables under active calibration are:

- `policy_fd_per_partition`
- `policy_memory_bytes_per_partition`
- `policy_available_parallelism`

Use `policy_target`, applied cap columns, wall time, throughput, and grouping
balance columns together. A policy case that improves wall time but creates
high imbalance or depends on unsafe resource caps is not automatically a better
default.

## Scope

Synthetic mode validates scan partition target policy and synthetic file-task
grouping behavior. It includes synthetic request latency, per-partition
scheduling overhead, bounded execution slots, and aggregate transfer floors so
policy cases can be compared more realistically than with infinite parallelism.

Host-probe mode records real cheap host signals, including available
parallelism, memory hints, Unix fd limit status when available, and a bounded
local scheduler probe. Local IO probing is opt-in and bounded. Host-probe mode
does not run network or stress probes.

Provider-exec mode is the production read benchmark path. It measures real
temporary Delta tables, Parquet decoding, Arrow batch production, provider
backend selection, DataFusion SQL planning, and DataFusion collection. The
default provider-exec mode is local-file based. Opt-in delayed HTTP storage
profiles add controlled object-store-like latency and byte-range reads without
changing the production provider reader path. Provider-exec mode does not expose
a bounded prefetch scheduler. It does compare existing production
read-admission and output-buffer settings, and it records provider owned read
stats from the physical Delta scan plan. Any later prefetch work must still use
production provider execution paths.

Representative native async backend benchmark notes and the current scheduling
decision are recorded in
[`native-async-backend-benchmark-notes.md`](native-async-backend-benchmark-notes.md).
