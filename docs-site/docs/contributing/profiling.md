# Choose a Delta Funnel Profiling Method

Use this guide to choose between exact semantic timelines and sampled native
call stacks when diagnosing Python-driven Delta Funnel workloads on Linux.
Stable semantic JSON works with normal Python builds. Samply is intended for
experienced developers working from a source checkout. Perfetto diagnostics
can use either the Linux x86_64 diagnostics wheel published from `main` to
TestPyPI or a local source build. Stable PyPI wheels do not include the optional
Perfetto producer.

## Choose the diagnostic mode

| Goal | Mode |
| --- | --- |
| Inspect exact operation, phase, query, worker, and operator timing | Stable semantic JSON export |
| Drill from one Python operation into ranked semantic work and native functions | [Operation-scoped ranked HTML](../advanced/execution-profiling.md#generate-an-operation-scoped-ranked-html-report) |
| Find native Rust CPU hotspots and source lines with the smallest capture | [Samply](profiling-samply.md) |
| Capture several operations or a whole Python process | [Manual Perfetto capture](profiling-perfetto.md#advanced-capture-a-whole-python-process) |
| Correlate a workload expected to run for up to ten minutes | [Standard streaming Perfetto](profiling-perfetto.md#record-a-longer-workload) |
| Add scheduler and wakeup context to a short standard capture | [Deep-system Perfetto](profiling-perfetto.md#add-scheduler-context) |

Start with operation-scoped ranked HTML for one preview or write. Use manual
short capture when the scope must include several operations, and streaming
mode when the expected duration exceeds two minutes. Operation-scoped and
manual short captures sample native call stacks at 1000 Hz by default;
streaming and deep-system modes default to 100 Hz. These are statistical
samples, so nearby runs can have different sample counts. On Linux, native
sampling is on-CPU only. Time blocked on I/O, locks, or sleep is absent from
the sampled stacks; use deep-system mode only when scheduler context is needed.

See the [profiling validation report](profiling-validation-report.md) for the
canonical 13.4M-row performance comparison, 10-minute streaming result, and
production correctness matrix behind these recommendations.

## Export the stable semantic timeline

Use stable semantic JSON when the question is about exact wall-clock phase,
query, worker, or operator ordering and native function stacks are not needed.
This path requires no diagnostic build or external capture process.

For a preview, enable detailed profiling and export the returned timeline:

```python
preview = table.preview(limit=100_000, profile=True)
preview.export_trace("preview-trace.json")
```

For a single SQL Server write, pass `profile=True` and `trace_path` to the
execute call. For `write_all`, pass `options={"profile": True}` and
`trace_path`. See the exact preview, write, and write-all examples in
[Export and inspect execution profiles](../advanced/execution-profiling.md#inspect-returned-preview-diagnostics).

Open the JSON with VizTracer's viewer or any compatible Chrome Trace Event
viewer:

```bash
vizviewer preview-trace.json
```

The export contains exact semantic wall-clock intervals. Operator lifecycle
bars can include waiting, and parallel intervals can overlap. Do not add their
durations and interpret the sum as elapsed wall time.
