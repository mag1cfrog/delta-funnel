# Native Async Backend Benchmark Notes

These notes record representative local evidence for issue #161. They are not
golden performance assertions. They document the command, workload shape, and
current scheduling decision so future reviewers can audit why no bounded
prefetch mode is exposed yet.

## Command

Run date: 2026-06-16

```bash
cargo run -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode provider-exec \
  --provider-exec-repetitions 3 \
  --output target/delta-provider-exec-bench-reps3.csv
```

The earlier local run used schema version `13` and produced 73 CSV lines: one
header plus 72 benchmark rows. The current benchmark schema is version `14`,
which adds `provider_exec_storage_profile` and includes the 12,808,140-row
`provider_partitioned_event_log_12m` workload.

## Workloads

Provider-exec mode ran through production Delta source loading, provider
registration, backend selection, DataFusion physical planning, and DataFusion
collection.

The matrix covered:

- Reader backends: `official_kernel`, `native_async`.
- Scheduling profiles: `lazy_serial_buffer_1`, `lazy_parallel_buffer_1`,
  `lazy_parallel_buffer_4`.
- Workloads: many small files, fewer larger files, and sparse-DV variants of
  both shapes.
- Queries: projection, count-style aggregate, and predicate-filtered scan.

DV provider metrics were populated in the run. For example, the sparse
many-small-files projection case recorded 64 DV payloads loaded, 64 DV masks
applied, and 64 deleted rows.

## Representative Results

Average of per-row `total_micros_p50`, `time_to_first_batch_micros_p50`, and
`source_rows_per_second_p50` grouped by backend and scheduling profile:

| Backend | Scheduling profile | Avg total p50 us | Avg first batch p50 us | Avg rows/sec p50 |
| --- | --- | ---: | ---: | ---: |
| native_async | lazy_parallel_buffer_1 | 14263 | 4971 | 2005285 |
| native_async | lazy_parallel_buffer_4 | 14093 | 4939 | 2056664 |
| native_async | lazy_serial_buffer_1 | 36473 | 13491 | 1048100 |
| official_kernel | lazy_parallel_buffer_1 | 16325 | 6904 | 1951978 |
| official_kernel | lazy_parallel_buffer_4 | 15950 | 6894 | 1989663 |
| official_kernel | lazy_serial_buffer_1 | 36651 | 19872 | 1092178 |

## 12M Local Mimic Workload

After adding the real provider-exec path for the partitioned event log mimic
model, a release build run of the focused 12M workload matrix used:

```bash
target/release/delta_scan_partition_bench \
  --mode provider-exec \
  --provider-exec-repetitions 3 \
  --provider-exec-workload provider_partitioned_event_log_12m \
  --output /tmp/delta-provider-exec-12m-rep3.csv
```

The workload had 12,808,140 rows, 956 Parquet files, and about 1.856 GB of
generated Parquet data. On local filesystem reads, native async was
consistently faster, but not by an order of magnitude.

Aggregate of per-case p50 totals across 9 backend/profile/query rows:

| Backend | Sum total p50 | Sum total p99 |
| --- | ---: | ---: |
| native_async | 1.919 s | 1.947 s |
| official_kernel | 2.559 s | 2.700 s |

Representative local ratios:

| Query | Scheduling profile | Native async p50 | Official p50 | Official/native |
| --- | --- | ---: | ---: | ---: |
| project_event_keys | lazy_parallel_buffer_4 | 0.175 s | 0.232 s | 1.33x |
| count_events | lazy_parallel_buffer_4 | 0.116 s | 0.161 s | 1.39x |
| filter_recent_events | lazy_parallel_buffer_4 | 0.064 s | 0.088 s | 1.37x |

## Delayed HTTP Storage Profile

Provider-exec now supports an opt-in benchmark-only delayed HTTP storage facade.
It serves the generated temporary Delta table through generic HTTP/WebDAV-style
object-store requests while injecting remote-like request and transfer latency.
Both provider backends use the same HTTP table URI and storage options, so the
comparison still runs through production source loading and provider execution.

Focused release runs used `s3-normal`, the 12M workload, and
`lazy_parallel_buffer_4`:

```bash
target/release/delta_scan_partition_bench \
  --mode provider-exec \
  --provider-exec-repetitions 1 \
  --provider-exec-workload provider_partitioned_event_log_12m \
  --provider-exec-storage-profile s3-normal \
  --provider-exec-query project_event_keys \
  --provider-exec-backend native_async \
  --provider-exec-scheduling-profile lazy_parallel_buffer_4 \
  --output /tmp/delta-provider-exec-12m-s3-normal-native-project.csv
```

Results:

| Query | Backend | Total | Time to first batch | Produced rows |
| --- | --- | ---: | ---: | ---: |
| project_event_keys | native_async | 15.588 s | 0.088 s | 12,808,140 |
| project_event_keys | official_kernel | 24.290 s | 0.108 s | 12,808,140 |
| count_events | native_async | 13.916 s | 13.882 s | 1 |
| count_events | official_kernel | 22.810 s | 22.773 s | 1 |

On these focused delayed-storage cases, official kernel was about 1.56x to
1.64x slower than native async. This is a stronger signal than the local
filesystem run and matches the expectation that async execution matters more
when object-store request latency is present.

The full 12M `s3-normal` provider-exec matrix with repetitions set to 1 exceeded
120 seconds and was stopped. Future full-matrix delayed-storage runs should be
scoped to selected workloads, queries, backends, or scheduling profiles unless a
long benchmark run is explicitly desired.

## Decision

Keep lazy scheduling as the default.

This evidence supports retaining the existing production lazy scheduler with
bounded read admission and a bounded DataFusion output handoff buffer. Parallel
lazy profiles were materially faster than serial lazy profiles on this local
small-file matrix. Increasing the output handoff buffer from 1 to 4 was a small
improvement in this run, but it is not enough evidence to expose a new public
prefetch scheduler.

No bounded prefetch mode is exposed by this branch. The delayed HTTP evidence
shows a clearer native async advantage under object-store-like latency, but it
does not by itself justify exposing a new prefetch scheduler. A future prefetch
mode must first land in the production execution path with explicit
active-plus-queued file task bounds and cancellation tests that do not require
draining unrelated prefetched data.
