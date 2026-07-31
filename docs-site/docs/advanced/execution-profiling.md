# Export and Inspect Execution Profiles

Delta Funnel provides two operation-scoped profiling models:

| Goal | Configuration | Output |
| --- | --- | --- |
| Collect exact DataFusion operator metrics | `execution_profile=True` | Returned profile |
| Rank sampled native CPU functions | `RankedProfileConfig` | Interactive HTML and optional `.dfprofile` artifact |

Both options work with `Table.preview`, `Table.write_to_mssql`, and
`Session.write_all`. You can enable either one or both for the same operation.
Neither is enabled by default.

Exact execution profiling works in normal wheels. Ranked profiling requires a
diagnostics-enabled Linux wheel and the setup in
[Set up Perfetto diagnostics for Python](../contributing/profiling-perfetto.md).

## Generate an operation-scoped ranked HTML report

Use 1000 Hz for a short investigation and 100 Hz for a longer or
lower-volume capture:

```python
from deltafunnel import RankedProfileConfig

preview = table.preview(
    limit=100_000,
    ranked_profile=RankedProfileConfig(
        "target/profiles/preview.profile.html",
        sample_hz=1000,
        artifact_path="target/profiles/preview.dfprofile",
    ),
)
```

The report is scoped to this operation. Open the self-contained HTML file in a
browser.

`artifact_path` is optional. Set it to keep the validated ranked model used by
both HTML and the terminal inspector. The larger raw Perfetto capture is
discarded:

```bash
delta-funnel-perfetto inspect target/profiles/preview.dfprofile
delta-funnel-perfetto report target/profiles/preview.dfprofile \
  --output target/profiles/preview-again.profile.html
```

Use the same configuration for a single SQL Server write:

```python
report = table.write_to_mssql(
    schema="dbo",
    table="orders",
    load_mode="append_existing",
    ranked_profile=RankedProfileConfig(
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
    ranked_profile=RankedProfileConfig(
        "target/profiles/write-all.profile.html",
        sample_hz=100,
    ),
)
```

Exact durations and sampled CPU counts use different units. Function self and
inclusive values are statistical on-CPU samples, not wall-clock durations.
See [Choose a Delta Funnel profiling method](../contributing/profiling.md) for
measurement guidance and screenshots.

Only one ranked profile can be active in a Python process. Ranked profiling is
not available for dry runs. If an operation fails, Delta Funnel preserves that
error while still attempting to finish the report.

## Inspect returned preview diagnostics

Use exact execution profiling when you need DataFusion operator metrics
alongside the always-available phase timings, without native CPU sampling:

```python
preview = table.preview(limit=20, execution_profile=True)

for timing in preview.phase_timings:
    print(timing["phase_name"], timing["status"], timing["elapsed_micros"])

profile = preview.execution_profile
```

`phase_timings` is available for every executed preview. The returned
`execution_profile` is populated only when you pass `execution_profile=True`.
A failure before physical planning completes cannot produce an operator
profile.

The returned profile follows the
[execution profile schema](../reference/execution-profile.md#profile-schema).
Operator metrics are cumulative and may overlap, so do not add them together
and call the result wall time.

## Inspect returned SQL Server output diagnostics

Enable exact profiling on a single write:

```python
report = table.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
    execution_profile=True,
)

profile = report["execution_profile"]
phase_timings = report["phase_timings"]
```

The profile describes DataFusion query execution. `phase_timings` describes
the SQL Server planning, stream consumption, writer finalization, validation,
swap, and cleanup phases. Each timing has an elapsed duration but no start
offset. A failed call can retain a completed query profile when the failure
happened later in the SQL Server lifecycle.

### Inspect write-all profiles

Enable exact profiling for every attempted output and executed cache alias:

```python
report = session.write_all(
    outputs,
    options={"cache_mode": "auto"},
    execution_profile=True,
)
```

Profiles remain attached to the output or cache alias that produced them. An
operation that fails before its profile observer is installed has no profile.
Skipped outputs are not attempted and also have no profile. See
[Multiple outputs and shared caching](multiple-outputs.md) for report
navigation.

### Profile write-all without SQL Server I/O

Use the stream benchmark path to execute full output queries and shared-cache
work without opening SQL Server connections:

```python
report = session.write_all_for_stream_benchmark(
    outputs,
    options={"cache_mode": "auto"},
    execution_profile=True,
    ranked_profile=RankedProfileConfig(
        "target/profiles/write-all-stream.profile.html",
        sample_hz=1000,
        artifact_path="target/profiles/write-all-stream.dfprofile",
    ),
)
```

The benchmark drains every output batch and retains row counts, schema checks,
cache materialization, exact profiles, and the operation-scoped ranked profile.
It skips SQL Server lifecycle work, target validation, bulk encoding, writes,
and cleanup, so use regular `write_all` when measuring end-to-end behavior.

## Combine exact and ranked profiling

Pass both configurations when you need returned operator metrics and ranked
native CPU functions from the same operation:

```python
preview = table.preview(
    limit=100_000,
    execution_profile=True,
    ranked_profile=RankedProfileConfig(
        "target/profiles/preview.profile.html",
        sample_hz=1000,
        artifact_path="target/profiles/preview.dfprofile",
    ),
)
```

The ranked report and artifact paths must name different files. This also
applies when symlinks or hard links make them refer to the same file. Ranked
profiling does not enable the returned exact execution profile.

## Migrate from the earlier Python API

The profiling API changed before 1.0. Replace earlier calls as follows:

| Earlier call | Replacement |
| --- | --- |
| `profile=True` | `execution_profile=True` |
| `trace_path="trace.json"` | No direct replacement; use a ranked report for interactive diagnosis |
| `ExecutionProfileConfig()` | `execution_profile=True` |
| `ExecutionProfileConfig(chrome_trace_path="trace.json")` | `execution_profile=True` for returned exact metrics, or `RankedProfileConfig(...)` for an interactive report |
| `preview.export_trace("trace.json")` | No direct replacement |
| `ProfilerConfig("report.html")` | `RankedProfileConfig("report.html")` |
| `artifact_output="profile.dfprofile"` | `artifact_path="profile.dfprofile"` |
| `profiler=config` | `ranked_profile=config` |
| `options={"profile": True}` | Top-level `execution_profile=True` |

The old names are not compatibility aliases. Python rejects them instead of
silently selecting a different profiling model.

## Related reference

- [Python API reference](../reference/api.md) defines exact signatures and
  configuration fields.
- [Execution profile reference](../reference/execution-profile.md) defines
  returned operator metrics and redaction.
- [Diagnostics reference](../reference/diagnostics.md) defines operation
  phase timings and terminal events.
- [Perfetto diagnostics runbook](../contributing/profiling-perfetto.md) covers
  diagnostics-wheel setup, symbols, CLI inspection, and advanced raw capture.
