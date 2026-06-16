# Native Async Backend Benchmark Notes

These notes record representative local evidence for issue #161. They are not
golden performance assertions. They document the command, workload shape, and
current scheduling decision so future reviewers can audit why bounded prefetch
is benchmark-only and not the default.

## Command

Run date: 2026-06-16

```bash
cargo run -p delta-funnel --bin delta_scan_partition_bench -- \
  --mode provider-exec \
  --provider-exec-repetitions 3 \
  --output target/delta-provider-exec-bench-reps3.csv
```

The earlier local run used schema version `13` and produced 73 CSV lines: one
header plus 72 benchmark rows. The current benchmark schema is version `16`,
which adds `provider_exec_storage_profile`, includes the 12,808,140-row
`provider_partitioned_event_log_12m` workload, and records
`native_async_prefetch_file_count_per_partition`, `process_peak_rss_bytes`,
and `process_peak_rss_delta_bytes`.

## Workloads

Provider-exec mode ran through production Delta source loading, provider
registration, backend selection, DataFusion physical planning, and DataFusion
collection.

The matrix covered:

- Reader backends: `official_kernel`, `native_async`.
- Scheduling profiles: `lazy_serial_buffer_1`, `lazy_parallel_buffer_1`,
  `lazy_parallel_buffer_4`, `prefetch_1_parallel_buffer_1`, and
  `prefetch_2_parallel_buffer_1`.
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

## Delayed HTTP Native Async Prefetch Comparison

After adding bounded native async prefetch profiles, focused release runs used
`s3-normal`, the 12M workload, native async only, and repetitions set to 1:

```bash
target/release/delta_scan_partition_bench \
  --mode provider-exec \
  --provider-exec-repetitions 1 \
  --provider-exec-workload provider_partitioned_event_log_12m \
  --provider-exec-storage-profile s3-normal \
  --provider-exec-backend native_async \
  --provider-exec-query project_event_keys \
  --output /tmp/delta-provider-exec-12m-s3-normal-native-prefetch-project.csv
```

The first run executed all native async scheduling profiles in one process and
did not have clean per-profile RSS isolation:

| Query | Scheduling profile | Prefetch depth | Total | Time to first batch | Rows/sec |
| --- | --- | ---: | ---: | ---: | ---: |
| project_event_keys | lazy_parallel_buffer_1 | 0 | 15.765 s | 0.086 s | 812,453 |
| project_event_keys | lazy_parallel_buffer_4 | 0 | 15.624 s | 0.083 s | 819,761 |
| project_event_keys | prefetch_1_parallel_buffer_1 | 1 | 8.647 s | 0.086 s | 1,481,244 |
| project_event_keys | prefetch_2_parallel_buffer_1 | 2 | 7.223 s | 0.087 s | 1,773,348 |
| count_events | lazy_parallel_buffer_1 | 0 | 14.266 s | 14.233 s | 897,783 |
| count_events | lazy_parallel_buffer_4 | 0 | 14.260 s | 14.227 s | 898,211 |
| count_events | prefetch_1_parallel_buffer_1 | 1 | 8.720 s | 8.688 s | 1,468,830 |
| count_events | prefetch_2_parallel_buffer_1 | 2 | 5.249 s | 5.215 s | 2,440,123 |

On this delayed-storage mimic, prefetch depth 1 materially outperformed lazy
parallel execution, and prefetch depth 2 improved further. The projection query
did not materially change time to first batch because all profiles emit quickly
after the first file opens. The aggregate query showed a stronger end-to-end
latency signal because it emits only after scanning all selected input.

The RSS values come from Linux `VmHWM` in `/proc/self/status`. Because `VmHWM`
is process-lifetime peak RSS, compare memory-sensitive profiles with one
scheduling profile per benchmark process. If several profiles run in one
process, later rows can inherit an earlier peak.

Separate-process release runs then compared RSS for the focused lazy and
prefetch profiles:

| Query | Scheduling profile | Total | Time to first batch | Peak RSS | Peak RSS delta |
| --- | --- | ---: | ---: | ---: | ---: |
| project_event_keys | lazy_parallel_buffer_1 | 15.761 s | 0.088 s | 106.8 MiB | 44.0 MiB |
| project_event_keys | prefetch_1_parallel_buffer_1 | 8.648 s | 0.092 s | 124.9 MiB | 64.0 MiB |
| project_event_keys | prefetch_2_parallel_buffer_1 | 7.259 s | 0.093 s | 121.5 MiB | 63.1 MiB |
| count_events | lazy_parallel_buffer_1 | 14.329 s | 14.294 s | 91.3 MiB | 24.4 MiB |
| count_events | prefetch_1_parallel_buffer_1 | 8.715 s | 8.682 s | 92.2 MiB | 26.3 MiB |
| count_events | prefetch_2_parallel_buffer_1 | 5.209 s | 5.175 s | 98.5 MiB | 30.2 MiB |

In these one-profile-per-process runs, prefetch depth 2 improved total time by
about 54% on `project_event_keys` and about 64% on `count_events`. Peak RSS
rose by about 14.7 MiB for `project_event_keys` and about 7.2 MiB for
`count_events` compared with `lazy_parallel_buffer_1`.

## Decision

Keep lazy scheduling as the default.

This evidence supports retaining the existing production lazy scheduler with
bounded read admission and a bounded DataFusion output handoff buffer. Parallel
lazy profiles were materially faster than serial lazy profiles on this local
small-file matrix. Increasing the output handoff buffer from 1 to 4 was a small
improvement in this run, but it is not enough evidence to expose a new public
prefetch scheduler.

Bounded prefetch is available as an internal execution option and benchmark
profile, but this branch does not make it the default. The delayed HTTP evidence
shows a clearer native async advantage under object-store-like latency, and the
focused prefetch runs show that a small bounded prefetch window can hide file
open and Parquet setup latency on the 12M mimic workload. Keep the default lazy
until the prefetch path has broader review and at least one repeated benchmark
run, but this is now a strong candidate for the native async default under
remote-object-store-like latency.
