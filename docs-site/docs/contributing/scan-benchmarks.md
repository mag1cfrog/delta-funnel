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

### Compare Samply with detailed profiling

Use the same symbolized optimized binary for the disabled, Samply, and detailed
cases so that the build profile is not another variable:

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

#### Samply comparison from 2026-07-17

This directional comparison was collected at commit `c60857d41673` on Fedora
Linux 43 x86-64 with an AMD Ryzen 7 8845HS, 8 cores, 16 hardware threads, 16
available parallelism slots, Samply 0.13.1 at its default 1000 Hz rate, and
Rust 1.97.0. Every case used the same `profiling` binary and three repetitions.
The execution order was disabled control, detailed, Samply, then disabled
control.

| Metric | Disabled before | Detailed | Samply | Disabled after |
| --- | ---: | ---: | ---: | ---: |
| Workflow time p50 | 22.362 s | 30.581 s | 22.021 s | 21.377 s |
| Workflow time p95 | 23.024 s | 31.497 s | 22.209 s | 22.058 s |
| Source rows per second p50 | 598,994 | 438,016 | 608,259 | 626,612 |
| Benchmark process peak RSS increase | 14,800.9 MiB | 16,701.2 MiB | 14,744.7 MiB | 14,595.3 MiB |
| Command wall time, three repetitions | 88.88 s | 127.05 s | 89.18 s | 86.23 s |
| Operation timeline spans, maximum | 0 | 189,666 | 0 | 0 |
| Chrome trace events, maximum | 0 | 190,149 | 0 | 0 |
| Compact Chrome trace size, maximum | 0 | 338.3 MiB | 0 | 0 |
| Chrome trace export time p50 | 0 | 5.310 s | 0 | 0 |
| Samply profile size, three repetitions | 0 | 0 | 4.7 MiB | 0 |

Samply's workflow p50 was 1.5% below the first control and 3.0% above the
second control. Its throughput and the benchmark process's peak RSS also
remained within the range of the controls. The RSS field does not include the
separate Samply recorder process. The controls themselves differed by 4.6%, so
this sample found no measurable Samply slowdown. The profile contained 287,020
samples; 119 events, about 0.04%, were lost.

Detailed profiling was 36.8% to 43.1% slower than the controls, reduced median
throughput by 26.9% to 30.1%, and increased peak RSS by 12.8% to 14.4%. It also
serialized one Chrome trace per repetition after the measured workflow. These
results show a material difference on this workload, but they remain a
directional development measurement rather than a hard performance guarantee.

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
