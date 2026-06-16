# Native Async Backend Benchmark Notes

These notes record representative local evidence for issue #161. They are not
golden performance assertions. They document the command, workload shape, and
current scheduling decision so future reviewers can audit why bounded prefetch
is the recommended native-async scheduling profile under remote-like latency.

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
  `prefetch_2_parallel_buffer_1`, plus available-parallelism multiplier sweep
  profiles `prefetch_2_ap_target_scan_1x` through
  `prefetch_2_ap_target_scan_4x`.
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

## Repeated Default-Decision Count Matrix

A later default-decision run used the 12M `count_events` query, native async
only, one scheduling profile per process, and repetitions set to 3:

```bash
target/release/delta_scan_partition_bench \
  --mode provider-exec \
  --provider-exec-repetitions 3 \
  --provider-exec-workload provider_partitioned_event_log_12m \
  --provider-exec-query count_events \
  --provider-exec-backend native_async \
  --provider-exec-storage-profile s3-normal \
  --provider-exec-scheduling-profile prefetch_2_parallel_buffer_1 \
  --output /tmp/delta-provider-default-count-s3-normal-prefetch_2_parallel_buffer_1-rep3.csv
```

The same command shape was run for `local`, `s3-normal`,
`s3-high-latency`, and `s3-throttled`, and for
`lazy_parallel_buffer_1`, `prefetch_1_parallel_buffer_1`, and
`prefetch_2_parallel_buffer_1`.

| Storage | Scheduling profile | Prefetch depth | Total p50 | Total p99 | Time to first batch p50 | Rows/sec p50 | Peak RSS | Peak RSS delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| local | lazy_parallel_buffer_1 | 0 | 0.115 s | 0.115 s | 0.111 s | 111,517,679 | 107.6 MiB | 24.8 MiB |
| local | prefetch_1_parallel_buffer_1 | 1 | 0.104 s | 0.107 s | 0.101 s | 123,046,343 | 109.4 MiB | 29.7 MiB |
| local | prefetch_2_parallel_buffer_1 | 2 | 0.107 s | 0.113 s | 0.103 s | 120,225,843 | 118.9 MiB | 42.0 MiB |
| s3-normal | lazy_parallel_buffer_1 | 0 | 14.290 s | 14.307 s | 14.256 s | 896,306 | 113.1 MiB | 26.1 MiB |
| s3-normal | prefetch_1_parallel_buffer_1 | 1 | 8.716 s | 8.749 s | 8.682 s | 1,469,497 | 111.7 MiB | 26.8 MiB |
| s3-normal | prefetch_2_parallel_buffer_1 | 2 | 5.257 s | 5.273 s | 5.222 s | 2,436,275 | 115.3 MiB | 29.6 MiB |
| s3-high-latency | lazy_parallel_buffer_1 | 0 | 50.786 s | 50.787 s | 50.709 s | 252,196 | 113.9 MiB | 24.7 MiB |
| s3-high-latency | prefetch_1_parallel_buffer_1 | 1 | 33.346 s | 33.363 s | 33.268 s | 384,093 | 114.6 MiB | 28.7 MiB |
| s3-high-latency | prefetch_2_parallel_buffer_1 | 2 | 18.283 s | 18.728 s | 18.207 s | 700,542 | 111.8 MiB | 29.9 MiB |
| s3-throttled | lazy_parallel_buffer_1 | 0 | 24.369 s | 24.383 s | 24.319 s | 525,581 | 106.6 MiB | 23.9 MiB |
| s3-throttled | prefetch_1_parallel_buffer_1 | 1 | 14.401 s | 14.460 s | 14.347 s | 889,398 | 114.9 MiB | 27.2 MiB |
| s3-throttled | prefetch_2_parallel_buffer_1 | 2 | 10.196 s | 10.427 s | 10.145 s | 1,256,174 | 107.0 MiB | 29.0 MiB |

On this repeated count matrix, `prefetch_2_parallel_buffer_1` was neutral to
slightly faster on local filesystem reads and substantially faster across every
delayed-storage profile. Compared with `lazy_parallel_buffer_1`, prefetch depth
2 improved total p50 by 2.72x on `s3-normal`, 2.78x on `s3-high-latency`, and
2.39x on `s3-throttled`. Peak RSS movement stayed modest in delayed profiles,
but local peak RSS increased by about 11.3 MiB.

## Repeated Default-Decision Projection Matrix

The same default-decision matrix was then run for the 12M
`project_event_keys` query. This query emits projected batches throughout the
scan, so it exercises the bounded output handoff path rather than waiting for a
single aggregate result:

```bash
target/release/delta_scan_partition_bench \
  --mode provider-exec \
  --provider-exec-repetitions 3 \
  --provider-exec-workload provider_partitioned_event_log_12m \
  --provider-exec-query project_event_keys \
  --provider-exec-backend native_async \
  --provider-exec-storage-profile s3-normal \
  --provider-exec-scheduling-profile prefetch_2_parallel_buffer_1 \
  --output /tmp/delta-provider-default-project-s3-normal-prefetch_2_parallel_buffer_1-rep3.csv
```

The same command shape was run for `local`, `s3-normal`,
`s3-high-latency`, and `s3-throttled`, and for
`lazy_parallel_buffer_1`, `prefetch_1_parallel_buffer_1`, and
`prefetch_2_parallel_buffer_1`.

| Storage | Scheduling profile | Prefetch depth | Total p50 | Total p99 | Time to first batch p50 | Rows/sec p50 | Batch latency p99 | Peak RSS | Peak RSS delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| local | lazy_parallel_buffer_1 | 0 | 0.177 s | 0.178 s | 0.001 s | 72,484,818 | 0.117 ms | 84.5 MiB | 22.4 MiB |
| local | prefetch_1_parallel_buffer_1 | 1 | 0.162 s | 0.170 s | 0.001 s | 78,942,230 | 0.079 ms | 100.2 MiB | 36.6 MiB |
| local | prefetch_2_parallel_buffer_1 | 2 | 0.159 s | 0.161 s | 0.001 s | 80,512,817 | 0.075 ms | 96.3 MiB | 33.0 MiB |
| s3-normal | lazy_parallel_buffer_1 | 0 | 15.668 s | 15.735 s | 0.086 s | 817,452 | 38.737 ms | 119.3 MiB | 44.5 MiB |
| s3-normal | prefetch_1_parallel_buffer_1 | 1 | 8.649 s | 8.662 s | 0.087 s | 1,480,962 | 24.340 ms | 138.2 MiB | 60.7 MiB |
| s3-normal | prefetch_2_parallel_buffer_1 | 2 | 7.352 s | 7.420 s | 0.085 s | 1,742,056 | 15.139 ms | 145.7 MiB | 64.3 MiB |
| s3-high-latency | lazy_parallel_buffer_1 | 0 | 53.252 s | 53.272 s | 0.218 s | 240,519 | 139.387 ms | 114.5 MiB | 38.1 MiB |
| s3-high-latency | prefetch_1_parallel_buffer_1 | 1 | 33.432 s | 33.485 s | 0.224 s | 383,108 | 114.093 ms | 132.8 MiB | 57.0 MiB |
| s3-high-latency | prefetch_2_parallel_buffer_1 | 2 | 20.453 s | 21.570 s | 0.225 s | 626,228 | 47.410 ms | 138.1 MiB | 57.4 MiB |
| s3-throttled | lazy_parallel_buffer_1 | 0 | 31.450 s | 31.499 s | 0.148 s | 407,256 | 68.151 ms | 115.7 MiB | 39.3 MiB |
| s3-throttled | prefetch_1_parallel_buffer_1 | 1 | 17.273 s | 17.274 s | 0.147 s | 741,524 | 33.827 ms | 127.8 MiB | 46.2 MiB |
| s3-throttled | prefetch_2_parallel_buffer_1 | 2 | 17.174 s | 17.176 s | 0.144 s | 745,801 | 34.715 ms | 148.2 MiB | 65.3 MiB |

On this repeated projection matrix, `prefetch_2_parallel_buffer_1` was again
neutral to slightly faster on local filesystem reads and substantially faster
on `s3-normal` and `s3-high-latency`. Compared with
`lazy_parallel_buffer_1`, prefetch depth 2 improved total p50 by 2.13x on
`s3-normal`, 2.60x on `s3-high-latency`, and 1.83x on `s3-throttled`. On the
throttled projection case, prefetch depth 1 and 2 were effectively tied on
total p50, while prefetch depth 2 used about 20.4 MiB more peak RSS than
prefetch depth 1.

## Sparse-DV Prefetch Sanity Matrix

A sparse-DV sanity run used `s3-normal`, native async only, repetitions set to
3, the two existing sparse-DV provider-exec workloads, projection and count
queries, and the lazy, prefetch depth 1, and prefetch depth 2 scheduling
profiles:

```bash
target/release/delta_scan_partition_bench \
  --mode provider-exec \
  --provider-exec-repetitions 3 \
  --provider-exec-workload provider_many_small_files_sparse_dv \
  --provider-exec-query project_id \
  --provider-exec-backend native_async \
  --provider-exec-storage-profile s3-normal \
  --provider-exec-scheduling-profile prefetch_2_parallel_buffer_1 \
  --output /tmp/delta-provider-sparse-dv-many-small-project-prefetch_2_parallel_buffer_1-rep3.csv
```

| Workload | Query | Scheduling profile | Total p50 | Total p99 | Time to first batch p50 | Rows/sec p50 | Peak RSS | DV payloads loaded p50 | DV rows deleted p50 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| provider_many_small_files_sparse_dv | project_id | lazy_parallel_buffer_1 | 1.175 s | 1.197 s | 0.090 s | 6,970 | 68.6 MiB | 64 | 64 |
| provider_many_small_files_sparse_dv | project_id | prefetch_1_parallel_buffer_1 | 0.901 s | 0.903 s | 0.106 s | 9,089 | 71.1 MiB | 64 | 64 |
| provider_many_small_files_sparse_dv | project_id | prefetch_2_parallel_buffer_1 | 0.775 s | 0.779 s | 0.108 s | 10,565 | 73.8 MiB | 64 | 64 |
| provider_many_small_files_sparse_dv | count_rows | lazy_parallel_buffer_1 | 1.253 s | 1.272 s | 1.225 s | 6,538 | 72.2 MiB | 64 | 64 |
| provider_many_small_files_sparse_dv | count_rows | prefetch_1_parallel_buffer_1 | 0.965 s | 0.975 s | 0.937 s | 8,490 | 73.1 MiB | 64 | 64 |
| provider_many_small_files_sparse_dv | count_rows | prefetch_2_parallel_buffer_1 | 0.847 s | 0.858 s | 0.817 s | 9,673 | 74.0 MiB | 64 | 64 |
| provider_few_larger_files_sparse_dv | project_id | lazy_parallel_buffer_1 | 0.133 s | 0.135 s | 0.091 s | 245,534 | 65.9 MiB | 4 | 12 |
| provider_few_larger_files_sparse_dv | project_id | prefetch_1_parallel_buffer_1 | 0.124 s | 0.133 s | 0.091 s | 264,398 | 68.3 MiB | 4 | 12 |
| provider_few_larger_files_sparse_dv | project_id | prefetch_2_parallel_buffer_1 | 0.122 s | 0.135 s | 0.092 s | 267,885 | 66.6 MiB | 4 | 12 |
| provider_few_larger_files_sparse_dv | count_rows | lazy_parallel_buffer_1 | 0.197 s | 0.204 s | 0.171 s | 166,605 | 68.8 MiB | 4 | 12 |
| provider_few_larger_files_sparse_dv | count_rows | prefetch_1_parallel_buffer_1 | 0.202 s | 0.202 s | 0.175 s | 162,607 | 69.3 MiB | 4 | 12 |
| provider_few_larger_files_sparse_dv | count_rows | prefetch_2_parallel_buffer_1 | 0.200 s | 0.220 s | 0.173 s | 163,994 | 72.3 MiB | 4 | 12 |

The sparse-DV metrics confirm that prefetch did not bypass DV loading or
masking. Every sparse case loaded and applied the expected DV payload count,
and deleted the expected number of rows. Prefetch depth 2 helped the
many-small-files sparse-DV workload by 1.52x for projection and 1.48x for
count. On the few-larger-files sparse-DV workload, prefetch was neutral for
projection and slightly slower for count, where only 4 files are available and
there is little file-open latency to hide.

## Available-Parallelism Scan-Cap Sweep

A scan-wide capacity sweep used the 12M mimic workload, `s3-normal`, native
async only, repetitions set to 3, 16 scan partitions from detected host
available parallelism, per-partition file-read capacity 3, prefetch depth 2, and
scan-wide file-read capacity from 1x through 4x available parallelism:

```bash
target/release/delta_scan_partition_bench \
  --mode provider-exec \
  --provider-exec-repetitions 3 \
  --provider-exec-workload provider_partitioned_event_log_12m \
  --provider-exec-query project_event_keys \
  --provider-exec-backend native_async \
  --provider-exec-storage-profile s3-normal \
  --provider-exec-scheduling-profile prefetch_2_ap_target_scan_3x \
  --output /tmp/delta-provider-ap-sweep-project-prefetch_2_ap_target_scan_3x-rep3.csv
```

| Query | Scheduling profile | Scan partitions | Scan-wide cap | Total p50 | Total p99 | Time to first batch p50 | Rows/sec p50 | Batch latency p99 | Peak RSS | Peak RSS delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| project_event_keys | prefetch_2_ap_target_scan_1x | 16 | 16 | 3.807 s | 3.848 s | 0.105 s | 3,364,378 | 8.255 ms | 216.3 MiB | 120.7 MiB |
| project_event_keys | prefetch_2_ap_target_scan_2x | 16 | 32 | 2.180 s | 2.321 s | 0.103 s | 5,875,441 | 4.706 ms | 246.9 MiB | 148.8 MiB |
| project_event_keys | prefetch_2_ap_target_scan_3x | 16 | 48 | 2.006 s | 2.040 s | 0.108 s | 6,383,527 | 4.382 ms | 269.4 MiB | 179.5 MiB |
| project_event_keys | prefetch_2_ap_target_scan_4x | 16 | 64 | 2.002 s | 2.075 s | 0.109 s | 6,398,023 | 4.261 ms | 275.3 MiB | 175.8 MiB |
| count_events | prefetch_2_ap_target_scan_1x | 16 | 16 | 3.312 s | 3.319 s | 3.282 s | 3,866,667 | 3284.793 ms | 136.9 MiB | 54.7 MiB |
| count_events | prefetch_2_ap_target_scan_2x | 16 | 32 | 1.835 s | 1.843 s | 1.800 s | 6,981,278 | 1808.039 ms | 143.6 MiB | 65.2 MiB |
| count_events | prefetch_2_ap_target_scan_3x | 16 | 48 | 1.741 s | 1.786 s | 1.707 s | 7,356,569 | 1751.387 ms | 152.0 MiB | 73.4 MiB |
| count_events | prefetch_2_ap_target_scan_4x | 16 | 64 | 1.595 s | 1.773 s | 1.561 s | 8,032,533 | 1739.661 ms | 152.3 MiB | 71.7 MiB |

The 1x scan cap is too low for prefetch depth 2 because it gives every scan
partition only one scan-wide read slot on average. The 2x cap gets most of the
benefit, but 3x still improves projection total p50 by about 8% and
substantially improves projection p99. The 4x cap provides no meaningful
projection gain over 3x and slightly worsens projection p99 while using more
peak RSS. Count p50 improves at 4x, but count p99 is nearly tied with 3x and
does not offset the projection result. Based on this sweep, 3x available
parallelism is the conservative scan-wide native-async capacity multiplier.

## Decision

Make native async the default provider backend for this slice.

The default keeps the output handoff buffer at 1 and uses the benchmark-backed
native async file-read shape: per-partition file-read capacity 3 and prefetch
depth 2. Default registration resolves scan-wide file-read capacity after scan
partition planning, using `target_partitions * 3`. Explicit execution options
remain explicit, including depth 0 for fully lazy native async execution and
the official-kernel backend for compatibility or comparison.

Parallel lazy profiles were materially faster than serial lazy profiles on the
local small-file matrix. Increasing the output handoff buffer from 1 to 4 was a
small improvement in that run, but file-level prefetch produced the dominant
delayed storage gains.

The repeated 12M count and projection matrices make
`prefetch_2_parallel_buffer_1` the best native-async default candidate for
remote-object-store-like latency. It wins or ties on local reads, wins strongly
on `s3-normal` and `s3-high-latency`, and wins the count workload on
`s3-throttled`. The main caution is the throttled projection case, where depth 1
and depth 2 are effectively tied but depth 2 uses about 20.4 MiB more peak RSS.
The sparse-DV sanity matrix does not show a correctness or memory blocker for
depth 2.

The selected-backend constructor still applies capacity-aware bounded prefetch
depth. Per-partition file-read capacity 1 stays lazy, capacity 2 defaults to
prefetch depth 1, and capacity 3 or greater defaults to prefetch depth 2.

The scan-wide capacity sweep supports 3x detected host available parallelism
without a fixed absolute cap. Because default scan partition planning already
derives and caps `target_partitions` from DataFusion, available parallelism,
file-descriptor hints, and memory hints, resolving default scan-wide capacity as
`target_partitions * 3` applies the same effective multiplier while preserving
the existing partition-count resource policy.
