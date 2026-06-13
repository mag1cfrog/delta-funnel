# Delta Provider Read Scheduling

Delta DataFusion scan execution owns bounded scheduling for provider file reads.
This document describes the current #141 implementation. It intentionally does
not describe the future native async Parquet backend tracked by #145.

## Current Reader Backend

The current provider execution path uses the official `delta_kernel` reader
baseline through `DeltaFileReader`. That reader exposes a synchronous
iterator-shaped file read boundary. Because lower-level object-store requests
are hidden inside the kernel path, DeltaFunnel cannot inject object-store
request or range-request fairness at #141.

The current limiter therefore bounds a conservative unit: one provider file
handoff. A handoff starts before the provider calls `DeltaFileReader::read_file`
for one `DeltaScanFileTask`, and it ends after all batches for that file have
been sent into the bounded DataFusion output channel or the stream exits.

This means the current limiter bounds provider-scheduled file work, not
individual object-store requests, Parquet row groups, or byte ranges.

## Execution Options

`DeltaProviderScanExecutionOptions` exposes two active-read caps:

- `max_concurrent_file_reads_per_scan`: scan-wide cap shared by all DataFusion
  execution partitions for one provider scan.
- `max_concurrent_file_reads_per_partition`: per-execution-partition cap that
  prevents one partition from consuming all scan-wide file read capacity.

Both values must be greater than zero. Defaults are conservative and keep the
official-kernel sync fallback sequential until a later slice deliberately raises
concurrency.

There is no public active-plus-queued file task cap in #141. The current sync
fallback does not maintain an extra provider file-task queue. A future native
async backend may add lazy scheduling or bounded prefetch/admission, but #145
requires benchmark evidence before exposing a prefetch/admission mode or option.

## Backpressure And Cancellation

Provider execution builds a DataFusion `RecordBatchReceiverStream` with bounded
channel capacity. The sync kernel reader runs outside direct DataFusion stream
polling through DataFusion's `RecordBatchReceiverStreamBuilder::spawn_blocking`
helper. The blocking worker periodically sends batches through the bounded
channel. If downstream drops the stream or stops accepting output, the channel
send fails and the worker exits instead of scheduling future files.

Because the file handoff permit covers both the synchronous file read and the
bounded output channel handoff, downstream backpressure can hold the permit
until the current file either makes progress or exits. This prevents the sync
fallback from reading ahead through the full file list when downstream polling
does not progress.

Permit release is RAII-based and covered by tests for success, failure, and
stream drop. If a file read fails, the failure is returned through the same
file-level reader path used by correctness tests; #141 does not introduce a
second read path.

## Known Limitations

The official-kernel sync iterator may internally fan one provider file handoff
into multiple object-store requests. #141 does not claim request-level,
range-level, or row-group-level fairness for that hidden work. The provider
keeps file-level bounds conservative and leaves native async object-store
scheduling to #145.

The #145 native async backend must preserve the active-read option semantics
above. It should start from a lazy scheduler with async semaphore-style active
read permits, and only expose bounded prefetch/admission if benchmark evidence
shows it is worth the extra queueing complexity.
