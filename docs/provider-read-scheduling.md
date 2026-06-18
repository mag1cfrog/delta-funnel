# Delta Provider Read Scheduling

Delta DataFusion scan execution owns bounded scheduling for provider file reads.
This document describes the provider scheduling state after the #4 execution
work, including the #141 official-kernel limiter and the #145 native async
backend.

## Reader Backends

Provider execution supports two production reader backends selected through
`DeltaProviderScanExecutionOptions`.

- `native_async`: the default provider backend. It uses parquet-rs async
  object-store reads, preserves original row indexes for deletion-vector files,
  applies Delta physical-to-logical transforms through the same kernel adapter
  boundary as the baseline, and applies DV masks before rows reach DataFusion.
- `official_kernel`: the correctness baseline and compatibility backend. It
  reads one file at a time through `DeltaFileReader`, which wraps the official
  `delta_kernel` synchronous iterator-shaped Parquet API.

The official-kernel limiter bounds a conservative unit: one provider file
handoff. A handoff starts before the provider calls `DeltaFileReader::read_file`
for one `DeltaScanFileTask`, and it ends after all batches for that file have
been sent into the bounded DataFusion output channel or the stream exits.

The native async limiter also accounts at the file-read handoff boundary, but
its permits are async semaphore permits held by each file stream until that
stream completes or is dropped. Native async file work starts only after both
the scan-wide and partition-local permits are acquired.

## Execution Options

`DeltaProviderScanExecutionOptions` exposes the active-read caps and handoff
buffers used by provider execution:

- `max_concurrent_file_reads_per_scan`: scan-wide cap shared by all DataFusion
  execution partitions for one provider scan.
- `max_concurrent_file_reads_per_partition`: per-execution-partition cap that
  prevents one partition from consuming all scan-wide file read capacity.
- `output_buffer_capacity_per_partition`: bounded producer-to-DataFusion batch
  handoff queue for each execution partition.
- `native_async_prefetch_file_count_per_partition`: native async file stream
  setup prefetch depth per partition. A value of `0` is fully lazy.

The active-read and output-buffer values must be greater than zero. The native
async backend defaults to per-partition file-read capacity 3, prefetch depth 2,
and output buffer capacity 1. Production registration resolves the native async
scan-wide cap after partition planning as `target_partitions * 3`.

The official-kernel backend keeps native async prefetch disabled and uses the
same active-read option names for compatibility. It does not maintain an extra
provider file-task queue.

## Dynamic Partition Pruning

Provider execution applies dynamic partition pruning at the whole-file
admission boundary. A retained DataFusion dynamic physical filter is snapshotted
for each not-yet-started `DeltaScanFileTask`, then evaluated against the task's
Delta partition values. When the snapshot proves that the partition cannot
match, the task is skipped before the provider starts file IO.

Skipped tasks do not acquire file-read permits, open object-store streams, load
deletion vectors, run physical-to-logical transforms, or emit batches. Kept
tasks continue through the same reader backend path as before, including
deletion-vector handling and residual DataFusion filtering.

The first implementation is intentionally opportunistic. Placeholder,
incomplete, unsupported, missing-metadata, and unparsable dynamic filter states
degrade to reading the file task. Files that have already started are not
cancelled, and native async prefetch only skips tasks before they are opened or
prefetched.

Dynamic pruning extends provider read stats without changing the meaning of
existing counters:

- `dynamic_filters_received`: post-phase physical filters offered to the Delta
  dynamic filter hook.
- `dynamic_filters_accepted`: offered dynamic filters retained because they
  reference only partition columns.
- `dynamic_filters_unsupported`: offered filters rejected by the dynamic filter
  hook policy.
- `dynamic_filter_snapshots`: retained dynamic filter snapshot attempts during
  file admission.
- `dynamic_partition_files_pruned`: file tasks skipped before provider file
  admission.
- `dynamic_partition_files_kept`: file tasks evaluated by dynamic pruning and
  kept.
- `dynamic_partition_files_not_pruned_missing_metadata`: kept file tasks where
  missing, invalid, or unparsable partition metadata prevented pruning.
- `dynamic_partition_files_not_pruned_unsupported_expression`: kept file tasks
  where unsupported or failed dynamic partition evaluation prevented pruning.

`files_planned` still reports the statically planned file-task count. Dynamically
skipped tasks reduce `files_started` and `files_completed`, because skipped
tasks never enter a reader backend.

The provider does not currently expose `dynamic_filters_completed` or
`dynamic_filters_too_late`. DataFusion does not provide a completion signal
that this scan can observe without coupling to producer internals or blocking
file admission. A precise too-late counter would also need to observe that a
dynamic filter became newly useful only after a task had already been opened or
prefetched; the first implementation intentionally avoids that producer-state
coupling.

## Backpressure And Cancellation

Provider execution builds a DataFusion `RecordBatchReceiverStream` with bounded
channel capacity. If downstream drops the stream or stops accepting output,
the provider stops scheduling future files and returns without draining
unrelated queued work.

For native async, the scheduler is demand-driven. In lazy mode, it admits the
next file only after the current file has been drained. In bounded-prefetch
mode, it may open a limited number of future file streams while preserving file
output order. Prefetched streams still hold file permits and count against the
same scan-wide and partition-local active-read caps.

For official-kernel reads, the sync reader runs outside direct DataFusion
stream polling through DataFusion's
`RecordBatchReceiverStreamBuilder::spawn_blocking` helper. The file handoff
permit covers both the synchronous file read and the bounded output-channel
handoff, which prevents the fallback from reading ahead through the full file
list when downstream polling does not progress.

Permit release is RAII-based or semaphore-owned and is covered by tests for
success, failure, cancellation, and stream drop. If a file read fails, the
failure is returned through the same file-level reader path used by correctness
tests.

## Known Limitations

The official-kernel sync iterator may internally fan one provider file handoff
into multiple object-store requests. DeltaFunnel does not claim request-level,
range-level, or row-group-level fairness for that hidden work. The provider
keeps file-level bounds conservative for this backend.

The native async backend routes Parquet reads through the selected
`object_store` handle and bounds active file streams with async semaphore
permits. The current public scheduling surface is file-stream admission plus
bounded file setup prefetch; request-level or range-level counters are not yet
exported as provider metrics.

Benchmark evidence for the current native async defaults is recorded in
[`native-async-backend-benchmark-notes.md`](native-async-backend-benchmark-notes.md).
