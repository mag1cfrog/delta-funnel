# Scan Partition Target Policy

Delta DataFusion provider scan planning derives a file task partition target
before Delta scan metadata expansion. The target is passed to metadata-only file
task grouping after Delta Kernel has selected the active files.

This policy decides how many provider scan partitions DeltaFunnel asks for. It
does not read Parquet data, create Arrow `RecordBatch` values, or execute file
reads.

## Policy Order

The production policy is:

1. Use `DeltaTableProviderConfig::scan_target_partitions` when it is `Some`.
   This source-local DeltaFunnel override must be greater than zero.
2. Otherwise, build a fallback baseline from
   `DeltaExecutionEnvironmentProfile::available_parallelism *
   parallelism_multiplier`.
3. Floor the fallback baseline by `min_default_partitions`, currently `1`.
4. Apply DataFusion `target_partitions` as an upper cap during fallback.
5. Apply reliable resource caps during fallback:
   - Unix fd soft limit, when available.
   - Available memory hint, when available.

Explicit DeltaFunnel override wins over all caps. It is the most specific user
choice and applies only to Delta scan file task grouping for the configured
source.

DataFusion `target_partitions` is broader. It is a session execution target, and
DataFusion exposes a non-null default even when the user did not explicitly tune
DeltaFunnel. DeltaFunnel records it as diagnostic input and uses it as an upper
cap for automatic fallback, not as the source winner.

## Execution Environment Profile

`DeltaExecutionEnvironmentProfile` collects cheap, stable, local-only signals:

- `available_parallelism`: Rust's process-aware available parallelism.
- `os_family`: Linux, macOS, Windows, other Unix, or other non-Unix.
- `memory_hint`: total and available memory when cheaply available.
- `unix_file_descriptor_limit`: Unix process fd limits when available.
- `io_latency_hint`: optional future probe field, `None` by default.
- `runtime_probe`: optional future runtime calibration field, `None` by default.

Provider scan planning does not run network probes, disk latency probes, or
runtime stress probes by default. If a local signal cannot be collected, the
profile leaves that field as `None` instead of failing query planning.

Windows does not get a fake Unix fd cap. Windows memory hints are collected when
available, and Windows-specific handle or IOCP modeling should only be added
later if it participates in a documented decision.

## Resource Caps

Current conservative policy variables are:

- `DEFAULT_FILE_DESCRIPTORS_PER_PARTITION = 16`
- `DEFAULT_AVAILABLE_MEMORY_BYTES_PER_PARTITION = 256 MiB`

These are guardrail values, not benchmark-proven optima. The benchmark matrix in
`delta_scan_partition_bench` sweeps candidate values so they can be validated on
different machines before changing the defaults.

There is no arbitrary fixed max such as 64 or 128. Fixed global caps are not
machine-aware enough. The automatic fallback can only be reduced by DataFusion
target, Unix fd limit, and available memory hints.

## Metadata Boundary

Target derivation intentionally happens before Delta scan metadata expansion. It
does not use:

- Delta file count.
- Delta add-action byte size.
- File size skew.
- Row estimates.

Those values belong to file task grouping after metadata expansion. They help
decide how selected files are balanced across the already chosen target, but
they do not choose the production target in this issue.

Reasons:

- Parquet compressed file size is not Arrow memory size.
- A huge single physical file cannot be split by increasing target partitions.
- The grouping policy does not emit empty partitions when target exceeds file
  count.
- Pre-metadata target derivation keeps zero-target validation before scan
  metadata expansion.

## Benchmark

The synthetic benchmark runner is documented in
[`scan-partition-benchmark.md`](scan-partition-benchmark.md). It calls the
production diagnostic facade rather than copying the policy, and it simulates
grouping and scan work for multiple workload shapes, storage-like profiles, and
policy parameters.

The benchmark is for policy calibration. It is not production read execution and
does not measure real Parquet decoding, Arrow batch memory, object-store request
behavior, or DataFusion runtime scheduling.
