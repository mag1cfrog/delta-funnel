# Diagnostics Reference

This reference defines Delta Funnel terminal tracing events, returned operation
diagnostics, stream outcomes, and write-all cache lifecycle fields. For
task-oriented steps, use
[Export and inspect execution profiles](../advanced/execution-profiling.md) or
[Troubleshoot a failed run](../advanced/tracing-and-diagnostics.md).

## Inspect terminal Parquet I/O

`delta_provider_parquet_io_summary` is one bounded terminal `DEBUG` event on the
`delta_funnel` tracing target. It records one aggregate provider snapshot
instead of per-request or per-range details. The default Rust filter admits
INFO events, so enable it with `delta_funnel=debug`.

For Python, both the Rust filter and the selected Python logger and handler must
admit DEBUG records. `DELTAFUNNEL_LOG` and the filter passed to `init_logging()`
do not change Python logging levels. See
[Python logging](../advanced/python-logging.md) for a complete configuration
example.

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

## Inspect terminal execution profiles

`query_execution_profile_terminal` is one bounded `DEBUG` summary on the
`delta_funnel` tracing target for an execution that registered a detailed
profile consumer. Profiling is opt-in. When `ExecutionProfileMode` remains at
its default `Disabled` value, Delta Funnel does not allocate a profile result,
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
[execution profile model](execution-profile.md#execution-profile-model) for the
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

## Inspect returned write-all cache diagnostics

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

Detailed write-all traces position the seven non-overlapping cache actions on
the root wall clock. The aggregate `cache_alias_materialization_total` remains
in the report but is not duplicated as another trace span over its five child
actions.

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

### Read a cache failure

Python exposes a cache orchestration failure as `DeltaFunnelError` with
`phase="write_all_cache"`, `kind="write_all_cache_failed"`, and this context:

```python
failure = error.context
attempted_aliases = failure["aliases"]
primary_table_id = failure["primary_failed_alias_table_id"]
completed_workflow = failure["workflow"]
partial_timeline = failure["operation_timeline"]
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
`primary_failed_alias_table_id()`, `workflow()`, and `operation_timeline()`.
The partial timeline is present when detailed profiling was enabled and uses a
failed root status. `source` retains the original primary error and its source
chain.

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
[multi-output API](api.md#session-write-all)
for normal and failure navigation and the
[execution profile model](execution-profile.md#execution-profile-model) for the
operator-level contract.

## Related guides

- [Troubleshoot a failed run](../advanced/tracing-and-diagnostics.md)
- [Export and inspect execution profiles](../advanced/execution-profiling.md)
- [Python logging](../advanced/python-logging.md)
