# Failure Reports And Safe Tracing

Use this guide when a real DeltaFunnel workload fails and you need to collect
diagnostics without exposing credentials, raw SQL, or row values.

DeltaFunnel library code emits reports and tracing spans/events. It does not
install a global tracing subscriber. Applications, tests, and future language
bindings own subscriber setup.

## Run A Dry-Run Preflight

Start with a dry run before executing a write. Dry runs plan the source, query,
target schema, target lifecycle, and output shape without contacting SQL Server,
starting row production, constructing a bulk writer, or writing rows.

For fuller source/provider metrics, enable scan-summary collection and call the
scan-summary dry-run method:

```rust
use delta_funnel::{
    DeltaFunnelRuntime, DeltaFunnelSession, DryRunScanSummaryMode, SessionOptions,
    ValidationOptions,
};

let session_options = SessionOptions::new().with_validation_options(
    ValidationOptions::new()
        .with_dry_run_scan_summary_mode(DryRunScanSummaryMode::ExhaustScanMetadata),
);

let mut session = DeltaFunnelSession::new(session_options)?;
let runtime = DeltaFunnelRuntime::new()?;

// Register sources, plan SQL, and build OutputWritePlan values here.

let report = runtime.dry_run_all_to_mssql_with_scan_summary(&session, &outputs)?;
let report_json = report.to_json_value();
```

Collect these dry-run sections for a failure report:

- `status`, `output_count`, and `phase_timings`
- `sources`, including protocol, file count, usage status, and provider stats
- each output's `status`, `target_table`, `load_mode`, schema counts, row-count
  evidence, and `validation_status`
- `dry_run` booleans proving that SQL Server, row production, table lifecycle,
  and bulk writer work did not start

## Run Execute Mode With Validation

Use execute mode only after the dry-run report looks correct. Target validation
is controlled by `TargetValidationMode`:

- `validate_if_possible` is the default. DeltaFunnel runs target-side row-count
  validation when the selected workflow supports it.
- `disabled` skips target-side validation.
- `require` fails when target-side validation cannot be completed.

```rust
use delta_funnel::{
    DeltaFunnelRuntime, DeltaFunnelSession, SessionOptions, TargetValidationMode,
    ValidationOptions,
};

let session_options = SessionOptions::new().with_validation_options(
    ValidationOptions::new()
        .with_target_validation_mode(TargetValidationMode::Require),
);

let mut session = DeltaFunnelSession::new(session_options)?;
let runtime = DeltaFunnelRuntime::new()?;

// Register sources, plan SQL, and build Execute OutputWritePlan values here.

let report = runtime.write_all(&session, &outputs)?;
let report_json = report.to_json_value();
```

Validation currently proves row-count facts reported by the workflow. It is not
full data equality validation, checksum validation, ordering validation, or a
SQL Server performance profile.

## Report Field Reference

Counts carry both a `kind` and a `value`. Do not read `value` without checking
`kind`.

| Field family | Kinds | How to read it |
| --- | --- | --- |
| `RowCount` | `exact`, `estimated`, `partial`, `unavailable` | `exact` proves the count for that scope. `estimated` comes from metadata or planning. `partial` is an observed prefix from a failed or incomplete path. `unavailable` has no numeric value. |
| `FileCount` | `exact`, `estimated`, `unavailable`, `skipped`, `not_executed` | `skipped` means DeltaFunnel intentionally avoided the count. `not_executed` means the workflow step that would count files never ran. |

Statuses also carry stable `kind` strings and optional `reason` strings:

| Status | Kinds | Notes |
| --- | --- | --- |
| `WorkflowStatus` | `success`, `partial_success`, `failure`, `skipped`, `no_op` | Dry-run workflow reports use this shape. Execute multi-output reports expose workflow counts and per-output status instead. |
| `OutputStatus` | `planned`, `succeeded`, `failed`, `skipped`, `dry_run_planned`, `validation_failed` | A validation failure nests a `validation` status. |
| `PhaseStatus` | `completed`, `failed`, `skipped`, `not_started`, `unavailable` | Phase timings include `elapsed_micros` only when measured. |
| `ValidationStatus` | `disabled`, `passed`, `failed`, `skipped`, `unavailable`, `required_but_failed` | `required_but_failed` means the caller required validation and DeltaFunnel could not prove a pass. |

Common reason strings include `validation_disabled`, `dry_run`,
`capability_unavailable`, `permission_unavailable`, `prior_failure`,
`unsupported_load_mode`, `missing_target_access`,
`missing_exact_output_rows`, `cost_avoidance`, `not_executed`, and
`failure_before_validation`.

Source reports contain sanitized `source_uri` and `protocol.table_uri` fields.
They also report source usage, file-count evidence, provider scheduling, and
provider read stats. Provider stats can be absent when the report stayed on the
metadata-only path, when a provider capability was unavailable, or when the
workflow failed before scan metadata could be collected.

Batch shaping reports compare input batches/rows from the selected output stream
with output batches/rows after DeltaFunnel shapes data for SQL Server. Write
stats report rows and batches accepted by the SQL Server write path.

## Read The Failure Report

When a workflow fails, start at the highest-level report and then drill down:

- `workflow` or workflow-level counts show how many outputs succeeded, failed,
  or were skipped.
- failed outputs include `failure.error` and, when available, structured
  `failure.context`.
- `failure.context.phase` identifies the write phase that failed, such as
  `connect`, `prepare_target_lifecycle`, `initialize_writer`,
  `poll_batch_stream`, `validate_batch_schema`, `write_batch`, `finalize`,
  `validation`, or `cleanup`.
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

Enable tracing in the application or test harness that calls DeltaFunnel. Use
target filters that include DeltaFunnel workflow events, Arrow writer events,
and raw bulk protocol events:

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
- `arrow_tiberius` for Arrow-to-SQL Server writer lifecycle events
- `tiberius_raw_bulk::protocol` for sanitized raw bulk protocol events

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
