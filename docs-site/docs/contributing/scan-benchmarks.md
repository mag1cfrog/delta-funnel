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

## Measure detailed profiling overhead

Use the phase-aligned workflow to compare detailed operation profiling with
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

Run the same workflow with detailed profiling enabled:

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
and CSV schema version identical. Compare detailed profiling against both of
these references:

- Profiling disabled measures the total cost added by profiling.
- The current detailed mode measures whether a replacement profiler improves
  or regresses the existing implementation.

`total_micros` includes the measured workflow but excludes Chrome trace
serialization. `trace_export_micros` records trace construction and compact
JSON serialization separately. Peak RSS is sampled after serialization, so it
includes retained profiling data and trace export memory. The
`operation_timeline_span_count_max`, `trace_event_count_max`, and
`trace_json_bytes_max` columns report the largest value observed across the
repetitions.

### Reference result from 2026-07-16

This directional baseline was collected at commit `b464b9e60b1f` on Linux
x86-64 with an AMD Ryzen 7 8845HS, 8 cores, 16 hardware threads, 16 available
parallelism slots, and Rust 1.97.0. Each row summarizes three repetitions.

| Metric | Disabled | Detailed | Change |
| --- | ---: | ---: | ---: |
| Workflow time p50 | 24.874 s | 30.154 s | +21.2% |
| Workflow time p95 | 25.704 s | 32.935 s | +28.1% |
| Source rows per second p50 | 538,507 | 444,216 | -17.5% |
| Peak RSS increase | 14,683.5 MiB | 16,512.2 MiB | +12.5% |
| Operation timeline spans, maximum | 0 | 192,739 | n/a |
| Chrome trace events, maximum | 0 | 193,222 | n/a |
| Compact trace JSON size, maximum | 0 | 344.8 MiB | n/a |
| Trace export time p50 | 0 | 5.478 s | n/a |

Three repetitions are enough for a reproducible development baseline, but not
for a hard performance threshold. Repeat the comparison and investigate host
noise before attributing a small difference to a code change.

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
