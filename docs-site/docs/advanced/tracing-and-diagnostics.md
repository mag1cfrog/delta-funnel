# Tracing And Diagnostics

Use this guide to observe normal DeltaFunnel workflows or investigate failures
without exposing credentials, raw SQL, or row values.

DeltaFunnel library code emits structured reports and tracing spans/events.
Reports preserve workflow results for later inspection. Tracing exposes live
lifecycle and execution details to a configured subscriber or logging bridge.
Applications, tests, and language bindings own that tracing setup.

## Run A Dry-Run Preflight

Start with a dry run before executing a write. Dry runs plan the source, query,
target schema, target lifecycle, and output shape without contacting SQL Server,
starting row production, constructing a bulk writer, or writing rows.

For dry-run setup, scan-summary collection, validation modes, and the report
field vocabulary, see [Dry runs and reports](../dry-runs-reports.md).

Collect these dry-run sections for a failure report:

- `status`, `output_count`, and `phase_timings`
- `sources`, including protocol, file count, usage status, and provider stats
- each output's `status`, `target_table`, `load_mode`, schema counts, row-count
  evidence, and `validation_status`
- `dry_run` booleans proving that SQL Server, row production, table lifecycle,
  and bulk writer work did not start

## Read The Failure Report

When a workflow fails, use the report vocabulary described in
[Dry runs and reports](../dry-runs-reports.md). Start at the highest-level
report and then drill down:

- `workflow` or workflow-level counts show how many outputs succeeded, failed,
  or were skipped.
- failed outputs include `failure.error` and, when available, structured
  `failure.context`.
- `failure.context.phase` identifies the write phase that failed, such as
  `connect`, `prepare_target_lifecycle`, `initialize_writer`,
  `poll_batch_stream`, `validate_batch_schema`, `write_batch`, `finalize`,
  `validation`, `swap_target`, or `cleanup`.
- `partial_write_possible` means DeltaFunnel cannot claim the target table is
  unchanged. Treat the target as needing operator review before retrying.
- `cleanup` reports whether cleanup was not applicable, not attempted,
  succeeded, or failed.
- skipped outputs include `skipped.reason`; after one output fails, later
  outputs can be skipped to avoid compounding target-side changes.

For source failures, collect the source report and the error display. Source
reports expose sanitized source URI context, protocol facts, provider scheduling,
file-count evidence, and provider read stats when available.

For SQL Server write failures, collect the output report, failure context,
target table, load mode, batch shaping stats, write stats, validation status,
phase timings, and cleanup status.

## Enable Safe Tracing

For Python, follow [Python logging](python-logging.md) to route DeltaFunnel
tracing through standard-library `logging`. The application remains responsible
for handlers, formatters, levels, files, and external exporters.

For private S3 Delta sources, `object_store=debug` is useful for local
debugging because it can show which credential-provider path was selected. Keep
those logs in a restricted location and sanitize them before sharing.

For Rust, enable tracing in the application or test harness that calls
DeltaFunnel. Use target filters that include DeltaFunnel workflow events, Arrow
writer events, and raw bulk protocol events:

```rust
use tracing_subscriber::{EnvFilter, fmt};

fmt()
    .with_env_filter(EnvFilter::new(
        "delta_funnel=info,arrow_tiberius=info,tiberius_raw_bulk::protocol=info",
    ))
    .init();
```

Use `debug` only when the extra volume is needed and the logs will stay in a
restricted location:

```text
delta_funnel=debug,arrow_tiberius=debug,tiberius_raw_bulk::protocol=debug
```

The tracing targets are:

- `delta_funnel` for DeltaFunnel workflow, source, output, validation, and
  DataFusion batch-stream events
- `object_store` for object-store builder and credential-provider debug events
- `arrow_tiberius` for Arrow-to-SQL Server writer lifecycle events
- `tiberius_raw_bulk::protocol` for sanitized raw bulk protocol events

## Inspect Terminal Parquet I/O

Alongside the phase-based lifecycle events above,
`delta_provider_parquet_io_summary` adds one bounded terminal `DEBUG` event on
the `delta_funnel` tracing target. It records one aggregate provider snapshot
instead of per-request or per-range details. The default Rust filter admits
INFO events, so enable it with `delta_funnel=debug`.

For Python, both the Rust filter and the selected Python logger and handler must
admit DEBUG records. `DELTAFUNNEL_LOG` and the filter passed to `init_logging()`
do not change Python logging levels. See [Python logging](python-logging.md) for
a complete configuration example.

The event fields are:

| Field | Value |
| --- | --- |
| `telemetry_event` | Always `delta_provider_parquet_io_summary`. |
| `source_name` | Sanitized source name for this scan. |
| `snapshot_version` | Delta snapshot version used by this scan. |
| `reader_backend` | `native_async` or `official_kernel`. |
| `outcome` | `success`, `error`, or `cancelled`. |
| `metrics_available` | `true` only when all four numeric metrics are available. |
| `parquet_data_file_range_get_operations` | Terminal provider snapshot value when available. |
| `parquet_data_file_full_get_operations` | Terminal provider snapshot value when available. |
| `parquet_data_file_bytes_received` | Terminal provider snapshot value when available. |
| `parquet_data_file_opened_bytes` | Terminal provider snapshot value when available. |

Native async scans include all four numeric fields, including zero. Official
kernel scans omit all four and set `metrics_available=false`. An unexpected
partial metric set is also reported as unavailable without publishing a
misleading numeric subset. See
[Parquet data-file I/O metrics](../internals/provider-read-scheduling.md#parquet-data-file-io-metrics)
for the metric definitions and measurement boundaries.

One event represents one distinct provider scan in one fresh physical-plan
execution. Repeated references to the same scan produce one event. Distinct
scans produce separate events even when their source metadata matches. Multiple
partitions do not produce partition-level copies.

`success` means every required stream reached normal end-of-stream. `error`
means upstream DataFusion execution failed. `cancelled` means downstream
dropped a required stream before end-of-stream and no error occurred. For
partitioned execution, the precedence is `error`, then `cancelled`, then
`success`.

A limited query is successful when its returned stream reaches normal
end-of-stream. A later formatting or SQL finalization failure does not change
that provider outcome. Plans without Delta scans, planning-only work, and dry
runs that do not execute a physical plan produce no summary event.

The Python bridge exposes tracing fields as string-valued `deltafunnel_*`
`LogRecord` attributes. Integers are decimal strings, Booleans are lowercase
`"true"` or `"false"`, and unavailable numeric attributes are absent rather
than zero, `None`, or an empty string.

This event contains sanitized aggregate values. It excludes plan text, SQL,
rows, file paths, table URIs, object URLs, credentials, storage options,
headers, and byte ranges. It is not exact network billing or a replacement for
CPU, syscall, scheduler, stack, or kernel profiling.

## Inspect Terminal Execution Profiles

`query_execution_profile_terminal` is one bounded `DEBUG` summary on the
`delta_funnel` tracing target for an execution that registered a detailed
profile consumer. Profiling is opt-in. When `ExecutionProfileMode` remains at
its default `Disabled` value, DeltaFunnel does not allocate a profile result,
retain a plan for profiling, collect DataFusion metrics, or emit this event.

The event fields are:

| Field | Value |
| --- | --- |
| `telemetry_event` | Always `query_execution_profile_terminal`. |
| `scope` | `preview`, `mssql_output`, or `write_all_cache_alias`. |
| `outcome` | `success`, `error`, or `cancelled`. |
| `partial` | `false` only for `success`. |
| `delta_funnel_row_limit` | Exact saturated preview limit; absent for write scopes. |
| `operator_count` | Saturated count of profiled physical-plan operators. |
| `operators_with_metrics` | Count of operators for which DataFusion exposed a metric set, including empty sets. |
| `root_output_rows` | Aggregated root `output_rows`; absent when unavailable. |
| `max_elapsed_compute_operator` | Exact short operator name for the first operator with the largest aggregated `elapsed_compute`; absent when unavailable. |
| `max_elapsed_compute_nanos` | That largest aggregated duration in nanoseconds; absent when unavailable. |

The event is derived only from the stored immutable profile. It emits no
operator list, raw metrics, plan text, SQL, expressions, schemas, row values,
source or object names, paths, URLs, aliases, credentials, storage options,
headers, or byte ranges. See the
[execution profile model](../reference/api.md#execution-profile-model) for the
full profile content instead of treating this event as a replacement schema.

One profile event represents one query execution, including a plan with no
Delta scan. By contrast, `delta_provider_parquet_io_summary` emits once for
each distinct Delta provider scan. When both are enabled, they use the same
authoritative terminal outcome and provider snapshot set. Neither event
changes a successful limited execution because of later formatting, SQL
finalization, validation, swap, or cleanup work.

The public `datafusion_query_output_stream` function keeps its existing stream
return type. Internally, profiling retains the exact root used for execution:
the planned root for zero or one output partition, and DataFusion's
`CoalescePartitionsExec` root for multiple output partitions. The retained root
is released immediately after terminal profile collection.

DataFusion metrics and Delta provider counters are cumulative within a physical
plan. Production code must create a fresh physical plan for each invocation;
reusing one plan for multiple profiles is unsupported.

The Python bridge exposes these fields as string-valued `deltafunnel_*`
`LogRecord` attributes. Integers use decimal strings, `partial` uses lowercase
`"true"` or `"false"`, and unavailable optional attributes are absent. Both
the Rust `delta_funnel=debug` filter and the selected Python logger and handler
must admit DEBUG records.

## Inspect Returned Preview Diagnostics

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

### Export A Preview Trace

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
DeltaFunnel writes the trace document directly.

The first complete `X` event is `Preview total`. It starts at zero and spans the
entire preview wall clock, including DataFrame planning, physical planning,
stream setup, execution and collection, and text and HTML formatting. Each
phase is positioned beneath that same origin, so an eight-second preview is
shown on an eight-second timeline rather than an execution-only subset.

Physical operator partitions with usable `start_timestamp` and `end_timestamp`
metrics are mapped onto the same preview origin. Their category is
`datafusion.operator.lifecycle`, and their `time_semantics` argument is
`lifecycle`. This distinction is important: the event measures how long the
operator stream existed, including waiting for upstream or downstream work. It
does not claim that the operator was actively computing for the entire bar.

Use the phase events first to identify whether planning, execution, or
formatting dominates the preview. Then inspect operator lifecycle events and
their arguments for metrics such as `output_rows` and `elapsed_compute` when
DataFusion exposes them. `elapsed_compute` remains an aggregate metric in the
event arguments and is not positioned as a fabricated wall-clock span. The
top-level `delta_funnel_timeline` field preserves the relative timeline data,
while `delta_funnel_profile` preserves the complete redacted operator profile.

Distinct phases and operator partitions use separate synthetic tracks because
they can overlap without forming a call stack. Repeated events with the same
track label reuse one track. Do not add overlapping event durations together.
Operator timestamps are clamped to the preview interval, and operators without
a usable timestamp pair remain in
`delta_funnel_profile` without appearing as lifecycle events.

Custom metrics added to an owned DataFusion operator automatically remain in
the event arguments and embedded profile. The exporter currently creates spans
only from each operator partition's standard start and end timestamps; it does
not turn custom timers into nested sub-operator spans or collect function-level
CPU stacks.

Omitting `profile`, or passing `None` or `False`, still returns all phase
timings but leaves `execution_profile` as `None`. `Table.show()` always uses
this disabled mode because it does not return the preview diagnostics.

Rust callers use `PreviewOptions` with `ExecutionProfileMode::Detailed`, then
call `TablePreview::to_trace_event_json_value()` or inspect
`TablePreview::operation_timeline()`, `phase_timings()`, and
`execution_profile()`. See [API references](../reference/api.md) for the
option-bearing call.

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
and `execution_profile`. These fields follow the same redaction rules as a
successful profile.

The returned diagnostics and terminal tracing event serve different uses:

| Surface | Availability | Content |
| --- | --- | --- |
| Preview phase timings | Every real preview result or returned preview failure. | Full ordered logical phase timings. |
| Preview execution profile | Returned result or failure context when `profile=True` or Rust detailed mode is enabled. | Full operator profile described by the [execution profile model](../reference/api.md#execution-profile-model). |
| `query_execution_profile_terminal` | One `DEBUG` tracing event when detailed mode reaches the terminal execution transition. | Bounded summary fields for live tracing and logging systems. |

The terminal event is not a serialized copy of the returned profile. It omits
the operator list and can be observed before later preview formatting finishes.
Both surfaces derive execution facts from the same immutable profile; preview
orchestration does not recollect metrics or emit a second profile event.

## Inspect Returned SQL Server Output Diagnostics

After output planning succeeds, one-output SQL Server execute reports include
three query preparation timings in addition to the existing output planning,
stream polling, SQL Server lifecycle, validation, and cleanup timings. Detailed
operator profiling is separately opt-in. See
[API references](../reference/api.md#one-output-sql-server-profiling) for
Python and Rust enablement.

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
[terminal stream lifecycle](#inspect-terminal-parquet-io) used by provider
snapshots.

On success, Python reads `report["execution_profile"]`, while Rust calls
`MssqlWriteReport::execution_profile()`. For a profiled Python write failure,
the same field is nested under `error.context["report"]["execution_profile"]`
when structured SQL Server failure context is available. Rust reads it from
`MssqlWriteFailureContext::report().execution_profile()` on
`MssqlQueryPhase`, `MssqlWritePhase`, or `MssqlBatchSchemaValidation` errors.
A failure before the profile consumer can be installed has no profile.

### Export A One-Output Write Trace

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

The returned report also contains the relative model under
`report["operation_timeline"]`. Detailed SQL Server failure contexts retain the
partial failed timeline under
`error.context["report"]["operation_timeline"]`, although `trace_path` is only
written after a successful call. Rust callers can inspect
`MssqlWriteReport::operation_timeline()` and export the same document with
`MssqlWriteReport::to_trace_event_json_value()`.

Trace serialization happens after the SQL Server write succeeds. If the call
raises `OSError` for `trace_path`, treat the database write as completed and do
not blindly retry an append or create-and-load operation.

The returned value uses the shared
[execution profile model](../reference/api.md#execution-profile-model),
including its Delta provider snapshots and redaction rules. The same immutable
terminal profile supplies the bounded
[`query_execution_profile_terminal`](#inspect-terminal-execution-profiles)
event; the event is not a copy of the returned operator list and can arrive
before later SQL Server work finishes. Neither surface includes plan display
text, raw SQL, row values, paths, URLs, credentials, storage options, headers,
or byte ranges.

## Inspect Returned Write-All Cache Diagnostics

Auto-cached `write_all` calls record cache lifecycle timings whether detailed
operator profiling is enabled or disabled. On success, read the executed alias
reports under `report["cache"]["aliases"]`. Each attempted alias has one of
these statuses:

- `materialized_and_restored` means cache setup, installation, and required
  restoration completed. Outputs may still contain failures.
- `failed` means one cache lifecycle phase failed. `failed_phase` identifies
  the primary failed cache phase for that alias.

The `selected` status is plan-shaped metadata, such as a dry-run cache plan.
It was not attempted and therefore omits `phase_timings` and `failed_phase`.

Every attempted alias has these eight timings in order:

| Phase | Boundary |
| --- | --- |
| `cache_alias_dataframe_resolution` | Resolve the selected registered alias with `SessionContext::table`. |
| `cache_alias_physical_planning` | Create one DataFusion physical plan for the alias. |
| `cache_alias_stream_setup` | Create every partition stream and install progress and terminal observation. It does not poll the streams. |
| `cache_alias_execute_collect` | Poll and collect the partition streams concurrently, restore partition order, and complete deterministic task cancellation cleanup after an error. |
| `cache_alias_memtable_build` | Build the `MemTable` and convert it to the cached table provider. |
| `cache_alias_materialization_total` | Measure resolution through successful `MemTable` construction. This contains the first five phases. |
| `cache_alias_install` | Replace the registered alias with the cached provider, including immediate restoration if cached registration fails after deregistration. |
| `cache_alias_restore` | Remove the cached provider and restore the original provider after output execution or failure cleanup. |

The materialization total overlaps its five leaf phases. It excludes cache
installation, output execution, and cache restoration. Output execution occurs
between `cache_alias_install` and `cache_alias_restore` but is not itself a
cache phase. Do not add the phase durations and interpret the result as wall
time.

On a materialization failure, both the causal leaf and
`cache_alias_materialization_total` are `failed`. Later unstarted phases are
`not_started` with reason `prior_failure`. Install and restore are marked
`completed` or `failed` only when attempted. No elapsed time is invented for
an unstarted phase.

Progress and no-progress calls use the same explicit default cache path. It
preserves the physical plan schema, partition count, partition order, and
batches produced by DataFusion's default memory cache. A configured custom
DataFusion cache factory is not silently bypassed. The call fails with a
redacted planning error before materialization because an arbitrary custom
factory cannot provide the physical-plan ownership required by these
diagnostics.

### Read A Cache Failure

Python exposes a cache orchestration failure as `DeltaFunnelError` with
`phase="write_all_cache"`, `kind="write_all_cache_failed"`, and this context:

```python
failure = error.context
attempted_aliases = failure["aliases"]
primary_table_id = failure["primary_failed_alias_table_id"]
completed_workflow = failure["workflow"]
```

`aliases` contains each attempted alias exactly once in cache-selection order.
Aliases are installed in that order and restored in reverse installation
order. If a later alias fails, earlier installed aliases retain their final
restore timings. If restoration also fails after another primary error, the
alias records its restore failure without replacing the original error. The
array is empty only when failure happens before the first selected alias
attempt starts.

`primary_failed_alias_table_id` identifies the alias whose cache phase caused
the primary failure. It is `None` when the primary failure happened outside an
alias phase, such as output workflow setup or execution. `workflow` is present
only when output execution completed and later cache restoration failed, so
completed output reports are not lost.

Rust callers match `DeltaFunnelError::WriteAllCache { failure, source }` and
read `WriteAllCacheFailure::aliases()`,
`primary_failed_alias_table_id()`, and `workflow()`. `source` retains the
original primary error and its source chain.

These lifecycle timings describe cache orchestration boundaries. A detailed
operator profile is separately opt-in and describes work inside one cache
physical plan. On a normal Python result, read it from
`report["cache"]["aliases"][i]["execution_profile"]`. On a cache failure, read
the same alias-owned field from `error.context["aliases"][i]`. Rust callers use
`WriteAllCacheAliasReport::execution_profile()` on aliases from either
`WriteAllCacheReport::CacheAliases` or `WriteAllCacheFailure::aliases()`.

Disabled profiling and pre-plan failures store `None`. Dry-run `selected`
aliases omit the field. An available cache profile has
`scope="write_all_cache_alias"` and describes only alias materialization, not
later reads from the cached table or output queries. Its terminal outcome is
unchanged by later `MemTable`, install, output, or restore failures. Output
profiles remain separate with `scope="mssql_output"`.

Every available cache profile supplies exactly one
[`query_execution_profile_terminal`](#inspect-terminal-execution-profiles)
event from the same immutable terminal result. The event is a live bounded
summary, not another report-owned profile. Operator work can overlap across the
cache phase boundaries above, so operator durations and lifecycle durations are
not additive. See
[multi-output SQL Server profiling](../reference/api.md#multi-output-sql-server-profiling)
for normal and failure navigation and the
[execution profile model](../reference/api.md#execution-profile-model) for the
operator-level contract.

## What Not To Share

Do not include these values in public issues, chat, logs, or pasted reports:

- SQL Server connection strings
- passwords, access keys, secret keys, session tokens, or SAS tokens
- raw SQL unless it has been intentionally reviewed and sanitized
- row values or sample records from production data
- credential-bearing URLs, including query strings, fragments, and userinfo
- raw dependency debug output

Prefer the structured JSON report from `to_json_value()`. It is designed to
preserve report semantics while avoiding default exposure of raw SQL,
connection strings, storage option values, and row values.

## Bug Report Checklist

Include the smallest safe set of facts that explains where the workload failed:

- DeltaFunnel crate version or commit
- whether the run was dry-run or execute mode
- validation mode: `disabled`, `validate_if_possible`, or `require`
- workflow counts and output names
- source report sections for affected sources
- failed output `failure.error` and `failure.context`, if present
- `phase_timings` for the workflow and failed output
- `batch_shaping`, `write_stats`, `validation_status`, `partial_write_possible`,
  and `cleanup` for SQL Server write failures
- tracing logs for `delta_funnel`, `arrow_tiberius`, and
  `tiberius_raw_bulk::protocol`

For SQL Server engine analysis, use SQL Server tooling such as DMVs, Extended
Events, Query Store, or separate profiling. DeltaFunnel reports do not replace
SQL Server's own execution diagnostics.
