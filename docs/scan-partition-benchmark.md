# Scan Partition Benchmark

`delta_scan_partition_bench` is a portable benchmark runner for the Delta
DataFusion scan partition target policy. Its default mode is the deterministic
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

The `--mode host-probe` flag is reserved for the opt-in host probe mode. It is
parsed separately from synthetic mode so CSV output can distinguish
`synthetic` from `host_probe`, but host-probe execution is added in the later
implementation slices.

Use a deterministic jitter seed:

```bash
cargo run -p delta-funnel --bin delta_scan_partition_bench -- \
  --seed 42 \
  --output target/delta-scan-partition-bench-seed-42.csv
```

The default seed is `0`. The seed affects deterministic simulated work jitter.
It does not change workload file shapes or policy target derivation.

## Matrix

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

This benchmark validates scan partition target policy and synthetic file-task
grouping behavior. It includes synthetic request latency, per-partition
scheduling overhead, bounded execution slots, and aggregate transfer floors so
policy cases can be compared more realistically than with infinite parallelism.

It is still not a production read benchmark. It does not measure real Parquet
decoding, Arrow batch memory, object-store request behavior, or DataFusion
runtime scheduling. Those belong to later read execution work.
