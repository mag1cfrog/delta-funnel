# Run Delta Scan Benchmarks

Use `delta_scan_partition_bench` to compare scan partition policy and provider
execution choices. The runner writes versioned CSV rows and can also write
JSONL tracing events.

This is a performance and policy calibration tool. It does not replace
correctness tests.

## Choose a benchmark mode

| Mode | What it measures |
| --- | --- |
| `synthetic` | Deterministic scan partition policy and file grouping models. It does not create Delta tables or read Parquet files. |
| `host-probe` | Local scheduler and host signals used by the partition policy. Local file IO is opt-in. |
| `provider-exec` | Real DataFusion execution over temporary synthetic Delta tables through the production provider path. |

The default mode is `synthetic`. Print the current option reference with:

```bash
cargo run -q -p delta-funnel --bin delta_scan_partition_bench -- --help
```

## Run the synthetic matrix

```bash
cargo run --release -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode synthetic \
  --seed 0 \
  --output target/delta-scan-synthetic.csv
```

Use the same seed when comparing policy changes. Synthetic mode models
scheduling and transfer costs, but it does not measure real object storage,
Parquet decoding, Arrow memory, or DataFusion execution.

## Probe the current host

```bash
cargo run --release -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode host-probe \
  --output target/delta-scan-host.csv
```

This records cheap local signals and runs a bounded scheduler probe. Add
`--host-probe-local-io` only when you also want the bounded local file read
probe. It is not an object-store benchmark.

## Measure provider execution

Start with one representative case that uses the production scan execution
defaults:

```bash
cargo run --release -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode provider-exec \
  --provider-exec-default-case \
  --provider-exec-repetitions 3 \
  --output target/delta-provider-default.csv
```

Provider execution creates temporary Delta tables, registers them through the
production provider, runs DataFusion SQL, and records provider read statistics.
Use the focused workload, query, backend, scheduling profile, and storage
profile options shown by `--help` when comparing one behavior at a time.

Add `--trace-output <path>` when phase-level JSONL tracing is needed:

```bash
cargo run --release -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode provider-exec \
  --provider-exec-default-case \
  --provider-exec-repetitions 1 \
  --output target/delta-provider-default.csv \
  --trace-output target/delta-provider-default.jsonl
```

## Measure exact execution profiling overhead

Use the phase-aligned workflow to compare exact execution profiling with
profiling disabled. This case generates a 13,394,789-row synthetic Delta table
and executes the production DataFusion provider and `write_all` stream paths.
It does not open SQL Server or write target rows.

Build the release binary before collecting results:

```bash
cargo build --release -p delta-funnel --bin delta_scan_partition_bench
```

Run the workflow with profiling disabled:

```bash
cargo run --release -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode provider-exec \
  --seed 0 \
  --provider-exec-storage-profile local \
  --provider-exec-workload provider_wide_event_export_13m \
  --provider-exec-query write_all_exports \
  --provider-exec-phase-aligned-workflow \
  --provider-exec-backend native_async \
  --provider-exec-scheduling-profile prefetch_2_parallel_buffer_1 \
  --provider-exec-repetitions 3 \
  --output target/operation-profile-baseline-disabled.csv
```

Run the same workflow with exact execution profiling enabled:

```bash
cargo run --release -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode provider-exec \
  --seed 0 \
  --provider-exec-storage-profile local \
  --provider-exec-workload provider_wide_event_export_13m \
  --provider-exec-query write_all_exports \
  --provider-exec-phase-aligned-workflow \
  --provider-exec-detailed-profile \
  --provider-exec-backend native_async \
  --provider-exec-scheduling-profile prefetch_2_parallel_buffer_1 \
  --provider-exec-repetitions 3 \
  --output target/operation-profile-baseline-detailed.csv
```

Run both commands on an otherwise idle host. Keep their workload, seed,
backend, scheduling profile, storage profile, repetition count, release build,
and CSV schema version identical. Compare exact execution profiling against
both of these references:

- Profiling disabled measures the total cost added by profiling.
- The current exact execution mode measures whether a replacement profiler
  improves or regresses the existing implementation.

`total_micros` includes the measured workflow. Compare its percentiles,
throughput, and peak RSS. Three repetitions are enough for a directional
development comparison, but not for a hard performance threshold. Investigate
host noise before attributing a small difference to a code change.

### Compare Samply with exact execution profiling

Use the same symbolized optimized binary for the disabled, Samply, and exact
execution profiling cases so that the build profile is not another variable:

```bash
cargo build --locked --profile profiling \
  -p delta-funnel \
  --bin delta_scan_partition_bench
```

On Linux with GNU `time`, run the complete comparison in one Bash or Zsh
session. Keep the common arguments in one array so every case stays identical:

```bash
benchmark_args=(
  --mode provider-exec
  --seed 0
  --provider-exec-storage-profile local
  --provider-exec-workload provider_wide_event_export_13m
  --provider-exec-query write_all_exports
  --provider-exec-phase-aligned-workflow
  --provider-exec-backend native_async
  --provider-exec-scheduling-profile prefetch_2_parallel_buffer_1
  --provider-exec-repetitions 3
)

/usr/bin/time -f 'disabled_before_command_wall_seconds=%e' \
  target/profiling/delta_scan_partition_bench \
  "${benchmark_args[@]}" \
  --output target/operation-profile-disabled-before.csv

/usr/bin/time -f 'detailed_command_wall_seconds=%e' \
  target/profiling/delta_scan_partition_bench \
  "${benchmark_args[@]}" \
  --provider-exec-detailed-profile \
  --output target/operation-profile-detailed.csv

/usr/bin/time -f 'samply_command_wall_seconds=%e' \
  samply record \
  --rate 1000 \
  --save-only \
  --output target/samply-operation-profile.json.gz \
  target/profiling/delta_scan_partition_bench \
  "${benchmark_args[@]}" \
  --output target/operation-profile-samply.csv

/usr/bin/time -f 'disabled_after_command_wall_seconds=%e' \
  target/profiling/delta_scan_partition_bench \
  "${benchmark_args[@]}" \
  --output target/operation-profile-disabled-after.csv
```

Compare Samply's `total_micros` with both disabled controls. The benchmark's
internal timer includes sampling overhead during the workflow, while excluding
Samply startup and profile finalization. Each `/usr/bin/time` result captures
the corresponding command wall time, including startup and finalization.
Bracketing the matrix with two controls makes host drift visible instead of
attributing it to the profiler.

## Compare results

- Compare rows with the same `benchmark_schema_version` and benchmark mode.
- Keep the workload, query, backend, scheduling profile, storage profile, seed,
  and release build consistent.
- Record the host and commit used for each run.
- Consider wall time, throughput, partition balance, applied resource caps, and
  provider read statistics together. One faster value is not enough to justify
  a new default.

The versioned CSV header emitted by the binary is the source of truth for
available fields. This guide intentionally does not copy the full schema or
generated workload matrix because both evolve with the runner.

## Understand the limits

Delayed HTTP storage profiles are controlled benchmark models, not measurements
of a specific S3 deployment. Provider execution uses generated local fixtures,
not production data. The phase-aligned `write_all` option exercises workflow
and Arrow stream boundaries without opening SQL Server or writing target rows.

For the production behavior behind these measurements, see
[Scan partition planning](../internals/scan-partition-planning.md) and
[Provider read scheduling](../internals/provider-read-scheduling.md).
