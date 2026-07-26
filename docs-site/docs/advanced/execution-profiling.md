# Export and Inspect Execution Profiles

Use this guide to generate an operation-scoped ranked HTML report from a
diagnostics build, or export the stable semantic timeline returned by normal
Delta Funnel preview and SQL Server APIs. Both paths cover preview, one-output,
and multi-output operations.

Install the diagnostics build and its Perfetto tools before generating
ranked HTML. See
[Set up Perfetto diagnostics for Python](../contributing/profiling-perfetto.md).
For field definitions and lifecycle contracts, use the
[Diagnostics reference](../reference/diagnostics.md). If you need scheduler
context or a whole-process capture, start with
[Choose a Delta Funnel profiling method](../contributing/profiling.md).

## Generate an operation-scoped ranked HTML report

Create one immutable profiler configuration for the operation. Use 1000 Hz for
a short query and 100 Hz when lower capture volume matters more than resolving
brief native work:

```python
from deltafunnel import ProfilerConfig

profiler = ProfilerConfig(
    "target/profiles/preview.profile.html",
    sample_hz=1000,
)

preview = table.preview(
    limit=100_000,
    profiler=profiler,
)
```

The capture starts immediately before `preview` executes and stops when that
call reaches its terminal result. Other Python work before or after the call is
not included. Passing `profiler` automatically enables the existing detailed
semantic and operator profile; `profile=True` is not also required.
At 1000 Hz, Delta Funnel collects kernel frame-pointer callchains to bypass
Perfetto's userspace unwind queue, then resolves those stacks locally with
`traceconv`, `llvm-symbolizer`, and matching debug files from `/usr/lib/debug`
or the user's debuginfod cache. The diagnostics wheel is built with the
required native frame pointers. Its pinned tracebox build expands Perfetto's
callchain queue so both 100 and 1000 Hz capture sample every CPU allowed by the
process affinity.
Concurrent Delta Funnel operations are excluded from the semantic and native
function rankings. Process samples without a matching target-operation context
remain visible only in the report's unattributed coverage count.

Use the same configuration type for one SQL Server write:

```python
report = table.write_to_mssql(
    schema="dbo",
    table="orders",
    load_mode="append_existing",
    profiler=ProfilerConfig(
        "target/profiles/orders-write.profile.html",
        sample_hz=100,
    ),
)
```

Or for a complete multi-output workflow:

```python
report = session.write_all(
    outputs,
    options={"cache_mode": "auto"},
    profiler=ProfilerConfig(
        "target/profiles/write-all.profile.html",
        sample_hz=100,
    ),
)
```

Open the resulting HTML file directly in a browser. The self-contained report
ranks exact semantic operation durations and lets you expand each operation
into its semantic children and sampled native callsites. Exact durations and
sample counts use different units: a function's self and inclusive CPU values
are statistical on-CPU samples, not wall-clock time.

The summary separates native capture quality into four mutually exclusive
outcomes:

- `Leaf symbols resolved` means the sampled leaf function has a readable name.
- `Leaf symbols unresolved` means a stack was captured, but its leaf function
  could not be named.
- `Unwind failures` means Perfetto reported a native unwind error.
- `Native stacks missing` means no usable callstack remained for the sample.

`Native stacks captured` is the sum of resolved and unresolved leaf symbols.
If unresolved coverage is high, fetch matching debuginfo for the host using
the setup guide and run the operation profile again.

HTML and terminal reports compact zero-self single-child call chains by
default when the child carries the parent's complete inclusive sample count.
This changes only the view; the complete captured tree remains in the report
model. Select **Show all native frames** in HTML or pass `--all-frames` to
`inspect` to restore every frame.
`ProfilerConfig` keeps only the requested HTML report after deleting its
temporary capture. When you generate a report from your own `.pftrace` file,
that input remains unchanged and retains the complete native call stack.

Only one operation profile can be active in a Python process. The output parent
directory is created when needed, and a completed report replaces an existing
file at the same path. Operation profiling is unavailable for dry runs.

If the operation itself fails, Delta Funnel keeps that operation error as the
primary exception and still attempts to generate the scoped report. If report
generation fails after a SQL Server operation returned a report, the profiler
exception exposes `deltafunnel_operation_status` and the successful or
completed-with-failures report in `deltafunnel_operation_report`. Do not retry
a write solely because profile finalization failed.

The older `profile=True` and `trace_path` APIs below remain useful when only the
stable semantic JSON is needed. They do not collect native CPU stacks and do
not require a diagnostics build.

## Inspect returned preview diagnostics

Bounded previews always return seven ordered phase timings. Detailed execution
profiles are opt-in and reuse the same immutable terminal result that supplies
`query_execution_profile_terminal`.

Enable the full profile from Python:

```python
preview = table.preview(limit=20, profile=True)

for timing in preview.phase_timings:
    print(timing["phase_name"], timing["status"], timing["elapsed_micros"])

profile = preview.execution_profile
```

### Export a preview trace

Export a trace when the operator dictionary is too difficult to compare
directly or when execution overlap matters:

```python
from pathlib import Path

preview = table.preview(limit=100_000, profile=True)
trace_path = Path("preview-trace.json")
preview.export_trace(trace_path)
```

Open the result with VizTracer's viewer:

```bash
vizviewer preview-trace.json
```

The same file can be imported into Perfetto or another viewer that accepts
Chrome Trace Event JSON. No VizTracer instrumentation is required because
Delta Funnel writes the trace document directly.

The first complete `X` event is `Preview total`. It starts at zero and spans the
entire preview wall clock, including DataFrame planning, physical planning,
stream setup, execution and collection, and text and HTML formatting. Each
phase is positioned beneath that same origin, so an eight-second preview is
shown on an eight-second timeline rather than an execution-only subset.

Detailed profiling records nested Delta provider work performed while
DataFusion builds the physical plan. These events use the
`datafusion.planning.activity` category and a `DataFusion query planning`
track. The current activities are Delta scan planning, projection and filter
planning, Delta Kernel scan construction, partition target selection, scan
metadata expansion, file-task partitioning, and scan execution setup. Parent
IDs preserve the call hierarchy beneath `Delta scan planning`.

Physical operator partitions with usable `start_timestamp` and `end_timestamp`
metrics are mapped onto the same preview origin. Their category is
`datafusion.operator.lifecycle`, and their `time_semantics` argument is
`lifecycle`. This distinction is important: the event measures how long the
operator stream existed, including waiting for upstream or downstream work. It
does not claim that the operator was actively computing for the entire bar.

Detailed preview profiling also records each operator's `execute` call and each
synchronous `poll_next` interval. These events use the
`datafusion.operator.activity` category and `wall_clock` time semantics. They
share tracks by query execution and executor worker, so each worker shows one
sequential or properly nested top-down call stack. A synchronous host thread
that polls the merged result uses a separate coordinator track. In VizViewer,
use the funnel's `Filter by Thread` selector to choose one exact track such as
`DataFusion query [1] / worker [2]`. The bracketed IDs also make the substring-
matching name field unambiguous: `worker [1]` does not match `worker [10]`.
The `activity` and `result` arguments distinguish stream creation, batches,
pending polls, end-of-stream, and errors. `query_execution_id`,
`worker_lane_id`, `worker_kind`, and `execution_stream_id` identify the
execution context. `runtime_task_id` remains metadata because many short-lived
Tokio tasks can share one executor worker. `node_id`, `parent_node_id`, and
`operator_partition` link the event to its physical-plan operator.
`worker_thread_id` and `worker_thread_name` expose the underlying thread behind
the normalized worker lane. Planning and operator activity events for the same
query share `query_execution_id`, `query_scope`, and, for SQL
outputs and cache materialization, `query_owner`. Use these fields to associate
worker tracks with `DataFusion query planning / SQL output: <output>` or
`DataFusion query planning / cache alias: <alias>` tracks in a multi-query
trace. Query IDs are local to one exported operation.

Use the phase events first to identify whether planning, execution, or
formatting dominates the preview. Within physical planning, use planning
activity to separate metadata expansion and other Delta provider work. During
execution, use worker activity to locate active work and lifecycle events to
understand overall concurrency and waiting. Lifecycle arguments retain metrics
such as `output_rows` and `elapsed_compute` when DataFusion exposes them.
`elapsed_compute` remains an aggregate metric and is not positioned as a
fabricated wall-clock span. The top-level `delta_funnel_timeline` field
preserves the relative timeline data, while `delta_funnel_profile` preserves
the complete redacted operator profile.

Distinct phases and operator lifecycle partitions use separate synthetic tracks
because they can overlap without forming a call stack. Operator activity events
instead reuse the executor worker that synchronously ran each call. Activity
spans on one worker lane are sequential or properly nested, even when that
worker runs many different Tokio tasks. Partition numbers remain operator-local,
so repartition and coalesce boundaries can change what a given number represents.
Do not add overlapping event durations together. Operator timestamps are
clamped to the preview interval, and operators without a usable timestamp pair
remain in `delta_funnel_profile` without appearing as lifecycle events.

Custom metrics added to an owned DataFusion operator automatically remain in
the lifecycle event arguments and embedded profile. Activity instrumentation is
one level below the lifecycle: it measures `execute` and `poll_next`, but does
not invent positions for aggregate custom timers or collect function-level CPU
stacks inside those calls. An `execute` or `poll_next` error has failed status
and `result="error"`. Successful stream creation uses `result="stream"`; poll
events use `pending`, `batch`, or `eof`. Detailed execution activity recording
has a cap of 100,000 spans. If execution exceeds that bound, the trace contains
one `Operator activity trace truncated` marker and continues the query
normally. During a Perfetto capture, the outermost poll on each executor task
continues to carry query and worker identity so later native samples remain
attributed without restoring every high-cardinality child span.

Omitting `profile`, or passing `None` or `False`, still returns all phase
timings but leaves `execution_profile` as `None`. `Table.show()` always uses
this disabled mode because it does not return the preview diagnostics.

Rust callers use `PreviewOptions` with `ExecutionProfileMode::Detailed`, then
call `TablePreview::to_trace_event_json_value()` or inspect
`TablePreview::operation_timeline()`, `phase_timings()`, and
`execution_profile()`:

```rust
use delta_funnel::{ExecutionProfileMode, PreviewOptions};

let options = PreviewOptions::new(20)
    .with_execution_profile_mode(ExecutionProfileMode::Detailed);
let preview = runtime.preview_table_with_options(&session, &table, options)?;
```

The phases are:

| Phase | Boundary |
| --- | --- |
| `preview_dataframe_planning` | Resolve the session-owned lazy table, capture its schema, and apply the requested Delta Funnel limit. |
| `preview_physical_planning` | Create the DataFusion physical plan. |
| `preview_stream_setup` | Construct the effective merged stream and install progress and terminal observers. |
| `preview_execute_collect` | Poll the merged stream to its terminal state and collect record batches. |
| `preview_format_text` | Format only the plain text table. |
| `preview_format_html` | Format only the HTML table. |
| `preview_total` | Measure the whole preview operation, including every phase above and orchestration between them. |

`preview_total` overlaps every child phase. Do not add the phase durations or
operator durations and treat the result as wall time. Executing operators and
partitions can also overlap.

On success, every phase is `completed`. On failure, the phase returning the
error is `failed`, every later unstarted phase is `not_started` with reason
`prior_failure`, and `preview_total` is `failed`. No elapsed time is invented
for an unstarted phase.

Detailed mode records the requested preview limit in
`delta_funnel_row_limit`. A `success` outcome means the limited execution
reached normal end-of-stream. It does not mean an unbounded query was executed.
The query outcome and the final preview result are intentionally separate: a
text or HTML formatting failure can retain a successful, non-partial execution
profile. A failure before a physical plan exists has no profile.

Rust exposes failure diagnostics through `PreviewFailureContext`. Python maps
them to a `DeltaFunnelError` whose `phase` is `preview`, whose `kind` is
`preview_failed`, and whose `context` contains `failed_phase`, `phase_timings`,
`execution_profile`, and `operation_timeline`. The partial timeline uses the
same preview origin, has root status `failed`, and contains spans recorded
through the failed phase. Rust reads it with
`PreviewFailureContext::operation_timeline()`. These fields follow the same
redaction rules as a successful profile.

The returned diagnostics and terminal tracing event serve different uses:

| Surface | Availability | Content |
| --- | --- | --- |
| Preview phase timings | Every real preview result or returned preview failure. | Full ordered logical phase timings. |
| Preview execution profile | Returned result or failure context when `profile=True` or Rust detailed mode is enabled. | Full operator profile described by the [execution profile model](../reference/execution-profile.md#execution-profile-model). |
| `query_execution_profile_terminal` | One `DEBUG` tracing event when detailed mode reaches the terminal execution transition. | Bounded summary fields for live tracing and logging systems. |

The terminal event is not a serialized copy of the returned profile. It omits
the operator list and can be observed before later preview formatting finishes.
Both surfaces derive execution facts from the same immutable profile; preview
orchestration does not recollect metrics or emit a second profile event.

## Inspect returned SQL Server output diagnostics

After output planning succeeds, one-output SQL Server execute reports include
three query preparation timings in addition to the existing output planning,
stream polling, SQL Server lifecycle, validation, and cleanup timings. Detailed
operator profiling is separately opt-in. See
[API reference](../reference/api.md#table-write-to-mssql) for
the Python signature.

Rust callers select detailed mode on the option-bearing runtime method:

```rust
use delta_funnel::ExecutionProfileMode;

let report = runtime.write_to_mssql_with_profile_mode(
    &session,
    &request,
    ExecutionProfileMode::Detailed,
)?;
```

The query phases are:

| Phase | Boundary |
| --- | --- |
| `query_dataframe_planning` | Resolve the session-owned lazy table into the DataFusion `DataFrame` to execute. |
| `query_physical_planning` | Run `DataFrame::create_physical_plan` for that output query. |
| `query_stream_setup` | Create the physical-plan partition streams, construct the effective merged stream, and install progress, terminal-state, and optional profile observers. It does not poll the stream. |

If one of these phases fails, that phase is `failed`. Each later query phase is
`not_started` with reason `prior_failure`; no elapsed time is invented for it.
A failure before query phase timing begins follows the existing output-planning
error contract. A failure before a physical plan exists cannot return an
execution profile. See [Dry runs and reports](../dry-runs-reports.md#read-report-values)
for the shared phase status and reason vocabulary.

`poll_batch_stream` is the accumulated time spent awaiting each next batch and
the final end-of-stream result. DataFusion executes lazily while the stream is
polled, so this timing includes upstream computation and delivery latency. It
does not include batch schema validation, `write_batch`, or `finalize`, and it
is not a separate total query duration. Operator and partition work can
overlap, so do not add this timing to DataFusion operator durations and treat
the result as wall time.

The execution profile describes the query stream lifecycle, not the final SQL
Server result:

| Query stream transition | Profile outcome | Possible final write result |
| --- | --- | --- |
| Reaches normal end-of-stream | `success` | Success, or a later `finalize`, target validation, swap, or cleanup failure. |
| Returns an upstream DataFusion error | `error` | Failure in `poll_batch_stream`. |
| Is dropped before end-of-stream without an upstream error | `cancelled` | An earlier connection, lifecycle, writer, schema, or write failure. An abandoned call returns no report. |

This means a failed SQL Server call can legitimately retain a non-partial
`success` profile when query execution had already reached end-of-stream. It
can also retain a partial `cancelled` profile when SQL Server work stopped
consumption early. These outcomes follow the same
[terminal stream lifecycle](../reference/diagnostics.md#inspect-terminal-parquet-io) used by provider
snapshots.

On success, Python reads `report["execution_profile"]`, while Rust calls
`MssqlWriteReport::execution_profile()`. For a profiled Python write failure,
the same field is nested under `error.context["report"]["execution_profile"]`
when structured SQL Server failure context is available. Rust reads it from
`MssqlWriteFailureContext::report().execution_profile()` on
`MssqlQueryPhase`, `MssqlWritePhase`, or `MssqlBatchSchemaValidation` errors.
A failure before the profile consumer can be installed has no profile.

### Export a one-output write trace

Pass `trace_path` with detailed profiling to write the successful operation
directly as Chrome Trace Event JSON:

```python
report = table.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
    profile=True,
    trace_path="daily-orders-write.json",
)
```

Open it with `vizviewer daily-orders-write.json`, Perfetto, or another Chrome
Trace Event viewer. The root event starts at zero and covers the complete
one-output wall clock from output schema and target planning through query
planning, SQL Server connection and target preparation, stream consumption,
writer finalization, validation, swap, and any required cleanup.

Repeated `Poll query batch`, `Validate batch schema`, and `Write batch to SQL
Server` events preserve their real positions on that clock and reuse readable
tracks. Batch details include the one-based batch index and, after polling, the
row count. DataFusion operator lifecycles appear on separate tracks with
`time_semantics="lifecycle"`; they can overlap stream polling and SQL Server
writes and must not be added together as sequential wall time.
The same query and worker tracks used by detailed previews contain the output
query's wall-clock `execute` and `poll_next` activity. Select one exact worker
with VizViewer's `Filter by Thread` control for a sequential or properly nested
top-down flame view.

The returned report also contains the relative model under
`report["operation_timeline"]`. Detailed SQL Server failure contexts retain the
partial failed timeline under
`error.context["report"]["operation_timeline"]`, although `trace_path` is only
written after a successful call. Rust callers can inspect
`MssqlWriteReport::operation_timeline()` and export the same document with
`MssqlWriteReport::to_trace_event_json_value()`.

Delta Funnel opens the trace destination before SQL Server work starts. An open
failure therefore prevents the database operation. If final serialization or
writing still fails after SQL Server succeeds, the Python error carries
`deltafunnel_operation_status="completed"` and the sanitized report in
`deltafunnel_operation_report`. Do not blindly retry an append or
create-and-load operation.

The returned value uses the shared
[execution profile model](../reference/execution-profile.md#execution-profile-model),
including its Delta provider snapshots and redaction rules. The same immutable
terminal profile supplies the bounded
[`query_execution_profile_terminal`](../reference/diagnostics.md#inspect-terminal-execution-profiles)
event; the event is not a copy of the returned operator list and can arrive
before later SQL Server work finishes. Neither surface includes plan display
text, raw SQL, row values, paths, URLs, credentials, storage options, headers,
or byte ranges.

### Inspect write-all profiles

Enable detailed profiling for every attempted output and executed cache alias:

```python
report = session.write_all(outputs, options={"profile": True})
```

Profiling works with either cache mode. Profiles stay with the output result
that produced them:

```python
for output in report["workflow"]["outputs"]:
    if output["kind"] == "succeeded":
        profile = output["report"]["execution_profile"]
    elif output["kind"] == "failed":
        context = output["failure"]["context"]
        profile = None if context is None else context["report"]["execution_profile"]
    else:
        profile = output["skipped"]["execution_profile"]  # Always None.
```

An attempted output can have `None` when it failed before a profile observer
was installed. A skipped output was not attempted and always has
`execution_profile=None`. The top-level report and output status wrappers do
not duplicate the field.

Omitting `profile`, or passing `None` or `False`, disables profiling. Only the
actual Boolean `True` enables it. Dry runs reject the `options` argument. See
the [Python API signature](../reference/api.md#session-write-all) and
[execution profile schema](../reference/execution-profile.md#profile-schema)
for the exact contracts.

### Export a write-all trace

Use one root wall clock to understand phase order, overlap, and sequential
output attempts across a multi-output write:

```python
report = session.write_all(
    outputs,
    options={"profile": True},
    trace_path="write-all-trace.json",
)
```

Rust callers select the same mode with `WriteAllOptions`:

```rust
use delta_funnel::{ExecutionProfileMode, WriteAllOptions};

let options = WriteAllOptions::new()
    .with_execution_profile_mode(ExecutionProfileMode::Detailed);
let report = runtime.write_all_with_options(&session, &outputs, options)?;
```

Open the file with `vizviewer write-all-trace.json`, Perfetto, or another
Chrome Trace Event viewer. The root event starts at zero and covers output and
cache planning, workflow execution, and source reporting. Each attempted
output has its own positioned span. Query planning, SQL Server sink phases,
batch work, and DataFusion operator lifecycles use that same origin, so the
trace tells the complete wall-clock story without adding independent elapsed
durations together.
Each output query also records wall-clock operator activity on its query and
worker tracks. Filter one exact worker track to inspect a conventional
top-down flame view while retaining the full write-all clock as context.
Planning and execution events carry the same operation-local query ID, scope,
and output or cache owner so each worker track can be matched to its planning
track without relying on timing order.

For auto-cached calls, each alias gets a labeled cache lane containing
DataFrame resolution, physical planning, stream setup, execution and
collection, `MemTable` construction, installation, and restoration. Cache
operator lifecycles share the root origin and overlap the execution and
collection window that drove them, beginning as early as stream setup.
Cache materialization queries use the same query and worker activity tracks as
output queries.

The returned report contains the relative model under
`report["operation_timeline"]`. Its root status is `failed` when the workflow
report contains failed or skipped outputs, and the trace file is still written
because the report itself was returned. Top-level exceptions that prevent a
report do not write `trace_path`.

Rust callers inspect `WriteAllReport::operation_timeline()` or export the same
document with `WriteAllReport::to_trace_event_json_value()`. As with a
one-output trace, the destination is opened before SQL Server work, while final
serialization occurs afterward. A late Python error carries the
`completed` or `completed_with_failures` operation status and the sanitized
report. It does not roll back completed writes and must not lead to a blind
retry.

## Related reference

- [Diagnostics reference](../reference/diagnostics.md) defines terminal events,
  phase boundaries, stream outcomes, and cache lifecycle fields.
- [API reference](../reference/api.md) defines the Python call signatures and
  links to the published Rust reference.
- [Execution profile reference](../reference/execution-profile.md) defines the
  returned profile schema, metrics, labels, and redaction rules.
