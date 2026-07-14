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
