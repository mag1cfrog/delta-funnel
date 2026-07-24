# Dry Runs, Validation, And Reports

Use dry runs to validate a plan before writing rows to SQL Server. Use execute
reports to confirm what the workflow wrote and whether target validation
succeeded.

The examples below continue from the [Python quickstart](python-api-walkthrough.md):
`daily_orders` is a lazy table created from SQL.

## Single-output dry run

```python
dry_run_report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
    dry_run=True,
)
```

Dry-run calls do not contact SQL Server, produce rows, construct a bulk writer,
or change a target table. They check source planning, target identity,
lifecycle choices, and output shape.

## Configure execute validation

Target validation has three modes:

- `validate_if_possible` is the default. Delta Funnel validates target-side
  row counts when the selected workflow supports it.
- `disabled` skips target-side validation.
- `require` fails when target-side validation cannot be completed.

For Python, select the mode when creating the session used by the workflow:

```python
from deltafunnel import Session

session = Session(
    default_mssql_connection_string=connection_string,
    validation_options={"target_validation_mode": "require"},
)
```

For Rust, configure `ValidationOptions` on the session:

```rust
use delta_funnel::{
    DeltaFunnelSession, SessionOptions, TargetValidationMode, ValidationOptions,
};

let session_options = SessionOptions::new().with_validation_options(
    ValidationOptions::new()
        .with_target_validation_mode(TargetValidationMode::Require),
);
let session = DeltaFunnelSession::new(session_options)?;
```

Validation proves row-count facts reported by the workflow. It is not full data
equality, checksum, ordering, or SQL Server performance validation.

## Execute reports

Execute calls return report dictionaries too:

```python
report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
)
```

Python returns reports as dictionaries. Rust report types provide
`to_json_value()` when a JSON-compatible representation is needed.

## Read report values

Counts carry both a `kind` and a `value`. Check `kind` before using `value`:

| Field family | Kinds | How to read it |
| --- | --- | --- |
| `RowCount` | `exact`, `estimated`, `partial`, `unavailable` | `exact` proves the count for that scope. `estimated` comes from metadata or planning. `partial` is an observed prefix from a failed or incomplete path. `unavailable` has no numeric value. |
| `FileCount` | `exact`, `estimated`, `unavailable`, `skipped`, `not_executed` | `skipped` means Delta Funnel intentionally avoided the count. `not_executed` means the workflow step that would count files never ran. |

Statuses carry stable `kind` strings and optional `reason` strings:

| Status | Kinds | Notes |
| --- | --- | --- |
| `WorkflowStatus` | `success`, `partial_success`, `failure`, `skipped`, `no_op` | Dry-run workflow reports use this shape. Execute multi-output reports expose workflow counts and per-output status instead. |
| `OutputStatus` | `planned`, `succeeded`, `failed`, `skipped`, `dry_run_planned`, `validation_failed` | A validation failure nests a `validation` status. |
| `PhaseStatus` | `completed`, `failed`, `skipped`, `not_started`, `unavailable` | Phase timings include `elapsed_micros` only when measured. |
| `ValidationStatus` | `disabled`, `passed`, `failed`, `skipped`, `unavailable`, `required_but_failed` | `required_but_failed` means the caller required validation and Delta Funnel could not prove a pass. |

Common reason strings include `validation_disabled`, `dry_run`,
`capability_unavailable`, `permission_unavailable`, `prior_failure`,
`unsupported_load_mode`, `missing_target_access`,
`missing_exact_output_rows`, `cost_avoidance`, `not_executed`, and
`failure_before_validation`.

Source reports can include sanitized source and protocol facts, file-count
evidence, provider scheduling, and provider read statistics. Batch-shaping
reports compare rows and batches before and after SQL Server shaping. Write
statistics report rows and batches accepted by the SQL Server write path.

## Collect detailed source statistics in Rust

The default `metadata_only` dry-run mode avoids DataFusion physical planning.
Choose `exhaust_scan_metadata` when a Rust workflow also needs provider scan
statistics and fuller source file-count evidence:

```rust
use delta_funnel::{
    DeltaFunnelRuntime, DeltaFunnelSession, DryRunScanSummaryMode, SessionOptions,
    ValidationOptions,
};

let session_options = SessionOptions::new().with_validation_options(
    ValidationOptions::new()
        .with_dry_run_scan_summary_mode(DryRunScanSummaryMode::ExhaustScanMetadata),
);

let session = DeltaFunnelSession::new(session_options)?;
let runtime = DeltaFunnelRuntime::new()?;

// Register sources, plan SQL, and build dry-run OutputWritePlan values here.

let report = runtime.dry_run_all_to_mssql_with_scan_summary(&session, &outputs)?;
let report_json = report.to_json_value();
```

This mode can perform extra Delta metadata and DataFusion physical-planning
work, but it still stops before row production and SQL Server work.

For multi-output dry runs, shared caching, and partial failure reports, see
[Multiple outputs and shared caching](advanced/multiple-outputs.md).

For interpreting failures and collecting safe diagnostics, see
[Troubleshoot a failed run](advanced/tracing-and-diagnostics.md).

For application diagnostics, see [Python logging](advanced/python-logging.md).
