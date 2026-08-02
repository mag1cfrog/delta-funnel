# API Reference

Use this page to look up public entry points and exact Python signatures. For
examples, start with the [Python quickstart](../python-api-walkthrough.md) or
[Rust quickstart](../rust-quickstart.md).

## Find an Entry Point

| Goal | Python entry point |
| --- | --- |
| Configure a workflow | [`Session`](#session) |
| Register a Delta source | [`Session.delta_lake`](#session-delta-lake) |
| Build a lazy SQL query | [`Session.table_from_sql`](#session-table-from-sql) |
| Preview rows | [`Table.preview`](#table-preview) or [`Table.show`](#table-show) |
| Write one SQL Server output | [`Table.write_to_mssql`](#table-write-to-mssql) |
| Define one of several outputs | [`Table.to_mssql`](#table-to-mssql) |
| Write several outputs | [`Session.write_all`](#session-write-all) |
| Profile several outputs without SQL Server I/O | [`Session.write_all_for_stream_benchmark`](#session-write-all-for-stream-benchmark) |
| Enable Python logging | [`init_logging`](#init-logging) |
| Enable Perfetto diagnostics | [`init_perfetto_diagnostics`](#init-perfetto-diagnostics) |
| Enable exact execution profiling | Pass `execution_profile=True` to an operation |
| Configure a ranked native CPU profile | [`RankedProfileConfig`](#ranked-profile-config) |
| Handle a Delta Funnel failure | [`DeltaFunnelError`](#delta-funnel-error) |

## Rust

The published Rust API reference is
[docs.rs/delta-funnel](https://docs.rs/delta-funnel).

Build the reference for the checked-out version with:

```bash
cargo doc -p delta-funnel --open
```

## Python

The package and import name are `deltafunnel`. The typed public surface is
recorded in
[`deltafunnel.pyi`](https://github.com/mag1cfrog/delta-funnel/blob/main/crates/delta-funnel-python/deltafunnel.pyi).

### Values and Type Aliases

```python
__version__: str

LoadMode: TypeAlias = Literal["append_existing", "create_and_load", "replace"]
WriteAllCacheMode: TypeAlias = Literal["auto", "explicit", "disabled"]
Report: TypeAlias = dict[str, object]
Options: TypeAlias = Mapping[str, object]

class WriteAllExecutionOptions(TypedDict, total=False):
    cache_mode: WriteAllCacheMode
    cache_aliases: Sequence[str]
```

Reports are JSON-compatible Python dictionaries. See
[Dry runs and reports](../dry-runs-reports.md) for report interpretation.

### Functions

<a id="init-logging"></a>
#### `init_logging`

```python
def init_logging(
    filter: str | None = None,
    logger: str = "deltafunnel",
) -> bool
```

Installs the Python logging bridge for Rust tracing events. It returns `True`
when it installs the process-wide subscriber and `False` when a subscriber is
already set. See [Python logging](../advanced/python-logging.md) for filters,
record fields, and setup.

<a id="init-perfetto-diagnostics"></a>
#### `init_perfetto_diagnostics`

```python
def init_perfetto_diagnostics(
    filter: str | None = None,
    logger: str = "deltafunnel",
    wait_timeout_seconds: float = 10.0,
) -> bool
```

Installs the Python logging bridge and Perfetto profiling layer, then waits for
a capture to attach. The function is available only in builds compiled with
Perfetto diagnostics. Use it for an advanced whole-process capture.
`RankedProfileConfig` manages activation automatically for operation-scoped
HTML reports. See
[Perfetto diagnostics](../contributing/profiling-perfetto.md) for build and
capture steps.

### Exceptions

<a id="delta-funnel-error"></a>
#### `DeltaFunnelError`

```python
class DeltaFunnelError(Exception):
    phase: str
    kind: str
    message: str
    context: object | None
```

Delta Funnel raises this exception for configuration, planning, and workflow
failures. `phase` identifies the failed operation area, `kind` is a stable
machine-readable category, `message` is safe for display, and `context`
contains structured details when available.

Errors raised after an operation finishes can also expose
`deltafunnel_operation_status`, `deltafunnel_operation_error`, or
`deltafunnel_operation_report`. Inspect these fields before retrying a write.
See [Troubleshoot a failed run](../advanced/tracing-and-diagnostics.md).

### Classes

<a id="ranked-profile-config"></a>
#### `RankedProfileConfig`

```python
class RankedProfileConfig:
    report_path: Path
    sample_hz: Literal[100, 1000]
    artifact_path: Path | None
    max_operator_activity_spans: int

    def __init__(
        self,
        report_path: str | PathLike[str],
        *,
        sample_hz: Literal[100, 1000] = 1000,
        artifact_path: str | PathLike[str] | None = None,
        max_operator_activity_spans: int = 100_000,
    ) -> None
```

Configures one operation-scoped ranked HTML report. Set `artifact_path` to
retain the same model as a `.dfprofile` file for later HTML or terminal
inspection. The configuration is immutable. Delta Funnel creates missing
parent directories. Use 1000 Hz for short operations and 100 Hz for longer
captures. `max_operator_activity_spans` is the positive per-operation limit
for detailed DataFusion activity spans. Increase it from its 100,000 default
only when capture health reports an activity truncation marker.

Only one ranked profile can be active in a process. The type is importable from
every wheel, but using it requires a diagnostics-enabled Linux build. It does
not enable the exact profile, and dry runs reject it. See
[Generate a ranked HTML report](../advanced/execution-profiling.md#generate-an-operation-scoped-ranked-html-report).

<a id="session"></a>
#### `Session`

```python
class Session:
    def __init__(
        self,
        *,
        default_mssql_connection_string: str | None = None,
        target_partitions: int | None = None,
        output_batch_size: int | None = None,
        provider_scan_options: Options | None = None,
        validation_options: Options | None = None,
        schema_options: Options | None = None,
    ) -> None
```

A session owns registered sources, lazy SQL tables, runtime configuration, and
SQL Server defaults.

| Parameter | Meaning |
| --- | --- |
| `default_mssql_connection_string` | Default ADO-style connection string for outputs that do not provide one. |
| `target_partitions` | Positive DataFusion execution partition target. `None` preserves the DataFusion default. |
| `output_batch_size` | Positive target row count for output batches. `None` preserves the DataFusion default. |
| `provider_scan_options` | Delta scan concurrency, buffering, and Parquet metadata prefetch overrides. |
| `validation_options` | Target validation and dry-run scan-summary behavior. |
| `schema_options` | Arrow-to-SQL Server type mapping policies. |

##### Session option mappings

All option mappings reject unknown keys.

`provider_scan_options` accepts:

| Key | Accepted value | Default |
| --- | --- | --- |
| `max_concurrent_file_reads_per_scan` | Positive integer | Automatic: resolved scan partition target multiplied by the per-partition limit |
| `max_concurrent_file_reads_per_partition` | Positive integer | `3` |
| `output_buffer_capacity_per_partition` | Positive integer | `1` |
| `native_async_prefetch_file_count_per_partition` | Non-negative integer; `0` is fully lazy | `2` |
| `parquet_metadata_size_hint` | Positive Parquet file-tail size in bytes; omitted disables footer metadata prefetch | Disabled |

See [Provider read scheduling](../internals/provider-read-scheduling.md#execution-options)
for the execution boundaries controlled by these values.

`validation_options` accepts:

| Key | Accepted value | Default |
| --- | --- | --- |
| `target_validation_mode` | `"disabled"`, `"validate_if_possible"`, or `"require"` | `"validate_if_possible"` |
| `dry_run_scan_summary_mode` | `"metadata_only"` or `"exhaust_scan_metadata"` | `"metadata_only"` |
| `require_successful_planning` | Boolean | `True` |

See [Dry runs and reports](../dry-runs-reports.md) for the behavior and cost of
the validation and scan-summary modes.

`schema_options` accepts:

| Key | Accepted value | Default |
| --- | --- | --- |
| `string_policy` | `"nvarchar_max"`, `"observed_nvarchar"`, or `{"nvarchar": N}` with positive `N` | `"nvarchar_max"` |
| `binary_policy` | `"varbinary_max"`, `"observed_varbinary"`, or `{"varbinary": N}` with positive `N` | `"varbinary_max"` |
| `timezone_policy` | `"reject"`, `"datetimeoffset"`, or `"normalize_utc_datetime2"` | `"reject"` |
| `timestamp_policy` | `"datetime"`, `"datetime2"`, or `{"datetime2": P}` with `P` from `0` through `7` | `"datetime2"` with precision `7` |
| `nanosecond_policy` | `"reject_non_100ns"`, `"round_to_100ns"`, or `"truncate_to_100ns"` | `"reject_non_100ns"` |
| `uint64_policy` | `"reject"`, `"decimal20_0"`, or `"checked_bigint"` | `"reject"` |
| `decimal_policy` | `"reject_negative_scale"` or `"normalize_negative_scale"` | `"reject_negative_scale"` |
| `decimal256_policy` | `"checked_downcast"` or `"reject"` | `"checked_downcast"` |
| `float_policy` | `"reject_non_finite"` | `"reject_non_finite"` |
| `date64_policy` | `"reject_non_midnight"` or `"timestamp_datetime2"` | `"reject_non_midnight"` |

The bounded string and binary forms choose an explicit SQL Server length;
the `observed_*` forms infer a bounded length from observed values.
`normalize_utc_datetime2` converts timezone-aware values to UTC before using
the timezone-free timestamp target. The `checked_*` policies accept values
only when the target representation can hold them. Normalization, rounding,
and truncation policies perform the conversion named by the value.

<a id="session-delta-lake"></a>
##### `Session.delta_lake`

```python
@overload
def delta_lake(
    self,
    source_uri: str,
    *,
    version: int | None = None,
    storage_options: Mapping[str, str] | None = None,
    name: str,
    progress: bool | None = None,
) -> Table

@overload
def delta_lake(
    self,
    source_uri: str,
    *,
    version: int | None = None,
    storage_options: Mapping[str, str] | None = None,
    name: None = None,
    progress: bool | None = None,
) -> PendingDeltaSource
```

With `name`, loads and registers the Delta source immediately. Without `name`,
returns an unregistered `PendingDeltaSource`; call its `alias` method to load
and register it. `version` selects a Delta snapshot. `storage_options` must map
string keys to string values. See [Private S3 sources](../advanced/private-s3.md)
for documented AWS keys.

<a id="session-table-from-sql"></a>
##### `Session.table_from_sql`

```python
def table_from_sql(self, sql: str) -> Table
```

Builds a lazy table from one read-only DataFusion SQL statement. It plans the
query but does not read rows.

<a id="multi-output-sql-server-profiling"></a>
<a id="session-write-all"></a>
##### `Session.write_all`

```python
def write_all(
    self,
    outputs: Sequence[MssqlOutputSpec],
    *,
    options: WriteAllExecutionOptions | None = None,
    dry_run: bool | None = None,
    progress: bool | None = None,
    execution_profile: bool = False,
    ranked_profile: RankedProfileConfig | None = None,
) -> Report
```

Plans or executes several SQL Server output specs in order and returns one
report. Every spec must come from the same session.

- `dry_run=True` plans without writing and rejects `options`,
  `execution_profile`, and `ranked_profile`.
- `options={"cache_mode": "auto"}` enables eligible shared-work caching.
- `options={"cache_mode": "explicit", "cache_aliases": ["prepared", "export"]}`
  caches exactly those registered derived aliases in dependency order. The two
  keys must be supplied together, and the selection must not skip a registered
  derived alias between a selected alias and a later selected alias or output.
- `options={"cache_mode": "disabled"}` disables shared-work caching.
- `execution_profile=True` attaches exact profiles to attempted outputs and executed
  cache aliases.
- `ranked_profile` writes an operation-scoped ranked HTML report.

See [Multiple outputs and shared caching](../advanced/multiple-outputs.md) for
workflow examples and
[Inspect write-all profiles](../advanced/execution-profiling.md#inspect-write-all-profiles)
for profile ownership.

<a id="session-write-all-for-stream-benchmark"></a>
##### `Session.write_all_for_stream_benchmark`

```python
def write_all_for_stream_benchmark(
    self,
    outputs: Sequence[MssqlOutputSpec],
    *,
    options: WriteAllExecutionOptions | None = None,
    execution_profile: bool = False,
    ranked_profile: RankedProfileConfig | None = None,
) -> Report
```

Executes and fully drains several output streams without contacting SQL Server.
The method preserves normal output planning, shared-cache work, DataFusion
execution, row counting, and batch schema validation. It skips SQL Server
lifecycle work, bulk writes, target validation, and cleanup. Output specs must
still resolve valid target and connection configuration for planning.
Produced counts are available through `output_row_count` and `batch_shaping`;
`write_stats` remains zero because the benchmark writes no rows.

Use this method only to benchmark query and stream execution without SQL Server
I/O. It does not predict end-to-end write performance.

<a id="pending-delta-source"></a>
#### `PendingDeltaSource`

Returned by `Session.delta_lake(...)` when `name` is omitted. The source is not
available to SQL until it is registered.

<a id="pending-delta-source-alias"></a>
##### `PendingDeltaSource.alias`

```python
def alias(
    self,
    name: str,
    *,
    progress: bool | None = None,
) -> Table
```

Loads and registers the pending source under `name`, then returns a `Table`.
The `progress` value applies to this registration call.

<a id="table"></a>
#### `Table`

A lazy Delta source or SQL-derived query associated with its owning session.

<a id="table-alias"></a>
##### `Table.alias`

```python
def alias(self, name: str) -> Table
```

Registers the lazy SQL-derived table under `name` so later SQL can reference
it, then returns the registered table.

<a id="table-preview"></a>
##### `Table.preview`

```python
def preview(
    self,
    limit: int = 20,
    *,
    progress: bool | None = None,
    execution_profile: bool = False,
    ranked_profile: RankedProfileConfig | None = None,
) -> Preview
```

Executes a bounded query and returns a rendered `Preview`. Phase timings are
always included. `execution_profile=True` enables the returned exact profile.
`ranked_profile` writes a sampled native CPU report without implicitly enabling
the exact profile. The method reads rows but does not contact SQL Server.

<a id="table-show"></a>
##### `Table.show`

```python
def show(
    self,
    limit: int = 20,
    *,
    progress: bool | None = None,
) -> None
```

Executes the same bounded query as `preview` and prints its text form to Python
stdout. It does not retain the `Preview` or enable exact execution profiling.

<a id="table-to-mssql"></a>
##### `Table.to_mssql`

```python
def to_mssql(
    self,
    *,
    schema: str,
    table: str,
    load_mode: LoadMode,
    name: str | None = None,
    connection_string: str | None = None,
) -> MssqlOutputSpec
```

Builds an output specification without planning or writing rows. `name`
defaults to the target table name and identifies the output in `write_all`
reports. `connection_string` overrides the session default for this output.

<a id="one-output-sql-server-profiling"></a>
<a id="table-write-to-mssql"></a>
##### `Table.write_to_mssql`

```python
def write_to_mssql(
    self,
    *,
    schema: str,
    table: str,
    load_mode: LoadMode,
    dry_run: bool | None = None,
    name: str | None = None,
    connection_string: str | None = None,
    progress: bool | None = None,
    execution_profile: bool = False,
    ranked_profile: RankedProfileConfig | None = None,
) -> Report
```

Plans or executes one SQL Server output and returns a report.

- `dry_run=True` plans without writing and rejects `execution_profile` and
  `ranked_profile`.
- `execution_profile=True` attaches `execution_profile` to an execute report.
- `ranked_profile` writes an operation-scoped ranked HTML report.
- `connection_string` overrides the session default for this call.

If ranked report export fails after SQL Server succeeds, the raised
exception reports `deltafunnel_operation_status="completed"` and contains the
sanitized report in `deltafunnel_operation_report`. Do not treat the export
error as evidence that a write is safe to retry.

See [SQL Server writes](../sql-server.md) for load modes and
[Inspect returned SQL Server output diagnostics](../advanced/execution-profiling.md#inspect-returned-sql-server-output-diagnostics)
for profile details.

<a id="preview"></a>
#### `Preview`

```python
class Preview:
    text: str
    html: str
    phase_timings: list[dict[str, object]]
    execution_profile: dict[str, object] | None

    def __str__(self) -> str
    def _repr_html_(self) -> str
```

`text` is the plain-text table. `html` backs notebook display.
`phase_timings` is always populated. `execution_profile` is populated when
`Table.preview` receives `execution_profile=True`.

<a id="mssql-output-spec"></a>
#### `MssqlOutputSpec`

An opaque output specification created by `Table.to_mssql(...)` and consumed
by `Session.write_all(...)`. It retains its owning session, lazy table, output
name, target, load mode, and optional connection override.

## Related Reference

- [Execution profile reference](execution-profile.md) defines the returned
  profile schema, metric mapping, labels, and redaction rules.
- [Diagnostics reference](diagnostics.md) defines tracing events, operation
  phase timings, stream outcomes, and cache lifecycle fields.
- [Progress displays](../progress.md) defines automatic, forced, and disabled
  progress behavior.

## Moved Reference Sections

These anchors preserve links to sections that moved out of this page.

<a id="execution-profile-model"></a>
[Execution profile model](execution-profile.md#execution-profile-model)

<a id="profile-schema"></a>
[Profile schema](execution-profile.md#profile-schema)

<a id="raw-and-aggregated-metrics"></a>
[Raw and aggregated metrics](execution-profile.md#raw-and-aggregated-metrics)

<a id="labels-and-redaction"></a>
[Labels and redaction](execution-profile.md#labels-and-redaction)

<a id="delta-provider-snapshots"></a>
[Delta provider snapshots](execution-profile.md#delta-provider-snapshots)
