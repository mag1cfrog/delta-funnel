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
