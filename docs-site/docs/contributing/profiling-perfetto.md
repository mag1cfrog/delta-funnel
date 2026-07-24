# Record and Inspect a Unified Perfetto Trace

This is the default path for profiling a real Python workload. It installs the
diagnostics wheel with uv, records a short trace, checks the result, and leaves
one `.pftrace` file. Inspect that file as a bounded terminal hierarchy, generate
a self-contained ranked HTML report, or open the raw timeline in Perfetto UI.

Perfetto diagnostics are intended for occasional local investigation, not
continuous collection. The TestPyPI wheel supports CPython 3.10 or newer on
Linux x86_64 with glibc 2.28 or newer.

## 1. Prepare the Linux host

Install matching `tracebox` and `trace_processor_shell` binaries from the same
Perfetto release, plus Linux `perf`. The workflow is verified with Perfetto
v57.2. Put all three commands on `PATH` and check them once:

```sh
command -v tracebox trace_processor_shell perf timeout
tracebox --version
trace_processor_shell --version
perf stat --all-cpus --event cpu-clock -- sleep 0.1 >/dev/null
```

If the final command reports a permission error, this temporary development
machine setting lasts until reboot:

```sh
echo '-1' | sudo tee /proc/sys/kernel/perf_event_paranoid
```

This loosens system-wide performance-event access. Do not use it on a shared or
production host without approval. The capture command repeats the permission
and Perfetto readiness checks before starting the workload.

## 2. Install the diagnostics wheel with uv

Merge this configuration into the workload project's `pyproject.toml`. Keep
its existing dependencies and index settings:

```toml
[project]
dependencies = [
    "deltafunnel>=0.0.0.dev0",
]

[[tool.uv.index]]
name = "delta-funnel-testpypi"
url = "https://test.pypi.org/simple"
explicit = true

[tool.uv.sources]
deltafunnel = { index = "delta-funnel-testpypi" }
```

Only `deltafunnel` comes from TestPyPI. Its dependencies and the rest of the
project continue to resolve from the default PyPI index.

Sync the environment and verify the installed diagnostics CLI:

```sh
uv sync --upgrade-package deltafunnel

uv run delta-funnel-perfetto --help
uv run delta-funnel-perfetto inspect --help
```

Locate the packaged capture command:

```sh
environment_python="$(uv run python -c 'import sys; print(sys.executable)')"
perfetto_assets="$(uv run python -c \
  'from importlib.resources import files; print(files("deltafunnel") / "perfetto")')"
capture_workload="$perfetto_assets/capture-workload"
test -x "$capture_workload"

uv run python -c \
  'import deltafunnel; print("Delta Funnel diagnostics", deltafunnel.__version__)'
```

The generated `uv.lock` records the exact diagnostics version and TestPyPI
source. Keep it with the capture when reproducibility matters.

## 3. Activate diagnostics in the workload

Add this before `init_logging()` and before any preview or write operation:

```python
import deltafunnel

if not deltafunnel.init_perfetto_diagnostics():
    raise RuntimeError("another tracing subscriber is already installed")
```

Activation is process-wide. Every later Delta Funnel operation in that Python
process can appear in the trace.

## 4. Record the workload

Run one command from the workload project root. Use a new output name for each
capture because existing trace files are never overwritten:

```sh
"$capture_workload" \
  --output target/perfetto-captures/query.pftrace \
  -- "$environment_python" path/to/workload.py
```

The command starts Perfetto, waits until all data sources are ready, runs the
workload, stops Perfetto, and checks the saved trace. A successful run ends
with output like:

```text
workload_status=0 tracebox_status=0 health_status=0 sample_hz=1000 trace=target/perfetto-captures/query.pftrace
```

`health_status=0` means the printed health row reported
`capture_complete=1`. The command always exits with the workload's own status.
A later capture or health failure cannot turn a successful database write into
a failed workload. Never retry a write only because diagnostics failed.

Short mode defaults to 1000 Hz. Pass `--sample-hz 100` when lower capture
volume matters more than resolving short native work. The explicit override
accepts only `100` or `1000` and works with every mode. At 1000 Hz the capture
tool also drains the kernel sampling buffers more often to avoid losing short
bursts.

## 5. Inspect ranked results in the terminal

Start with a bounded one-shot view of the operation roots:

```sh
uv run delta-funnel-perfetto inspect \
  target/perfetto-captures/query.pftrace
```

Each semantic row reports an exact wall-clock duration and an
`id=semantic:ID` identity. Select a row to show its immediate children and up
to two lower levels:

```sh
uv run delta-funnel-perfetto inspect \
  target/perfetto-captures/query.pftrace \
  --semantic 42 \
  --depth 2 \
  --limit 30
```

Selected semantic nodes also show their sampled native function roots. Use the
printed `function:SEMANTIC_ID:FUNCTION_ID` identity to descend into one sampled
call path:

```sh
uv run delta-funnel-perfetto inspect \
  target/perfetto-captures/query.pftrace \
  --function 42:7 \
  --sort inclusive-cpu \
  --depth 2
```

Semantic `duration_ns` and `operation_wall_percent` values are exact wall-clock
measurements. Function `self_cpu_samples`, `inclusive_cpu_samples`, and their
percentages are statistical on-CPU samples. Do not compare their numeric values
as if they used the same unit.

Use interactive mode when an agent or human needs to navigate repeatedly
without reloading and aggregating the trace:

```sh
uv run delta-funnel-perfetto inspect \
  target/perfetto-captures/query.pftrace \
  --interactive
```

Enter `help` to list the line-oriented commands. The main navigation commands
are:

```text
open semantic:ID
open function:SEMANTIC_ID:FUNCTION_ID
up
root
sort duration
sort inclusive-cpu
filter TEXT
clear
limit N
quit
```

`open` accepts an exact immediate-child identity printed by the current view.
This prevents a short identity from accidentally selecting another node.
Every interactive response ends with `-- end --`, so an agent can consume the
session without terminal-screen parsing.

## 6. Generate a ranked HTML report

Generate a self-contained interactive report beside the trace:

```sh
uv run delta-funnel-perfetto report \
  target/perfetto-captures/query.pftrace
```

The default output is
`target/perfetto-captures/query.profile.html`. Choose another destination with
`--output`:

```sh
uv run delta-funnel-perfetto report \
  target/perfetto-captures/query.pftrace \
  --output target/perfetto-captures/query-report.html
```

Open the HTML file in a browser. It uses the same ranked semantic and function
data model as the terminal inspector. The raw trace is not embedded in the
report and is not modified.

## 7. Inspect the raw timeline in Perfetto UI

Open the `.pftrace` file in [Perfetto UI](https://ui.perfetto.dev/). Expand the
`Delta Funnel diagnostics` process to read the exact hierarchy from top to
bottom:

```text
Operation
  Phases
  Query
    Worker
      Operator and lower-level activity
  Output owner
    Output execution stages
```

To isolate one operation, click the funnel-shaped track filter and paste its
exact token, including the closing bracket, such as
`op-00000000000000000003]`. The closing bracket prevents a shorter numeric ID
from matching a longer one. Use the same technique with a worker token such as
`w-00000000000000000001]` when a query contains many parallel workers. Expand
the remaining parent tracks to keep the relevant ancestry in view.

[![Full Perfetto timeline filtered to one SQL Server write_all operation](../assets/perfetto-semantic-hierarchy.png)](../assets/perfetto-semantic-hierarchy.png)

The full viewport above shows a real SQL Server `write_all` operation. Each
output owner contains an end-to-end `Execute output` parent. Its children show
query setup and the actual SQL Server lifecycle, including connection, target
preparation, streaming writes, writer finalization, and validation, on the
same wall-clock ruler.

Drag across an output owner or worker track to select the time range you want
to investigate. Temporarily clear the name filter, check
`Process callstacks cpu-clock`, and then reapply the exact operation or worker
filter. Open `Current Selection`, choose `Perf sample flamegraph`, and keep
`Top Down` selected. The semantic tracks show exact wall-clock intervals; the
flame graph shows statistical on-CPU native samples from the same selected
interval.

[![Perfetto Top Down native flame graph](../assets/perfetto-native-flamegraph.png)](../assets/perfetto-native-flamegraph.png)

The blue markers and shaded region above delimit the selected 3.26-second
interval. The lower panel follows the native stack from the runtime into Delta
Funnel and DataFusion. Click either screenshot to open the complete UI at full
size.

The repository example takes about 6 seconds and produced about 12 MB during
validation. Hardware, workload, and symbols change both values.

## Advanced Perfetto options

### Record a longer workload

Use streaming mode when the workload is expected to run for more than two
minutes and up to ten minutes:

```sh
"$capture_workload" \
  --mode streaming \
  --output target/perfetto-captures/query-streaming.pftrace \
  -- "$environment_python" path/to/workload.py
```

Streaming periodically drains its buffers, has a 12-minute safety timeout, and
caps the saved file at 512 MiB. High event volume can reach the cap sooner.
Missing tail time in an incomplete trace is unknown activity, not zero
activity. Streaming defaults to 100 Hz; explicitly selecting 1000 Hz can reach
the file cap much sooner.

### Add scheduler context

Use deep-system mode only when the question requires scheduler and wakeup
evidence. It defaults to 100 Hz and requires tracefs access:

```sh
test -r /sys/kernel/tracing/events/sched/sched_switch/id
test -w /sys/kernel/tracing/tracing_on

"$capture_workload" \
  --mode deep-system \
  --output target/perfetto-captures/query-deep-system.pftrace \
  -- "$environment_python" path/to/workload.py
```

Grant tracefs access through the host's normal access-management process. Do
not run the workload or tracebox with `sudo`. Deep-system mode uses more memory,
creates larger traces, and adds overhead, so it is not the default.

### Build from source for line-level symbols

The TestPyPI wheel retains native function names but omits large DWARF line
tables. Build from a source checkout when source lines are required:

```sh
python3 -m venv target/python-perfetto-venv
source target/python-perfetto-venv/bin/activate
maturin develop --locked --profile profiling \
  --features perfetto-profile \
  --manifest-path crates/delta-funnel-python/Cargo.toml

environment_python="$VIRTUAL_ENV/bin/python"
perfetto_assets="$PWD/tools/perfetto"
capture_workload="$perfetto_assets/capture-workload"
```

Then use the same activation and capture steps above. The `profiling` profile
keeps optimizations and line-table debug information. Normal builds and stable
PyPI wheels remain Perfetto-free.

### Interpret capture health

The capture command prints the complete machine-readable health row. The most
important fields are:

- `capture_complete=1`: exact semantic data was complete and finalization was
  observed.
- `semantic_complete=1`: operation roots, identities, nesting, and semantic
  buffers passed their checks.
- `perf_samples_skipped` and `perf_sample_without_callsite_count`: nonzero
  values reduce native sampling confidence but do not erase exact semantics.
- `truncation_marker_count`: the documented per-operation activity budget was
  reached. This is not buffer loss. Detailed child spans stop, while Perfetto
  retains task-root contexts for native sample attribution.
- `saved_file_bytes`: the factual file size.

An incomplete trace may still contain useful retained intervals. Do not assume
anything about omitted time. The short mode preserves its beginning; streaming
mode can retain different intervals as buffers drain and wrap.

### Troubleshoot activation

`init_perfetto_diagnostics()` returns `False` when another global tracing
subscriber is already installed. Start a fresh Python process and activate
Perfetto first.

A `DeltaFunnelError` with phase `perfetto_diagnostics` includes a stable `kind`:

```text
not_available
invalid_logger
invalid_wait_timeout
producer_initialization_failed
capture_timeout
capture_unavailable
```

If `delta-funnel-perfetto` reports that the build does not include Perfetto
diagnostics, uv resolved a stable PyPI wheel instead of the TestPyPI
diagnostics wheel. If uv cannot find the diagnostics wheel, confirm CPython
3.10 or newer, Linux x86_64, and glibc 2.28 or newer. Use `uv pip show
deltafunnel` and `uv run python -c 'import sys; print(sys.executable)'` to
confirm the installed version, source, and active environment.

## Keep capture data local

A `.pftrace` file can contain process names, command lines, library paths,
function names, timing, and system activity. Store it in a private local
directory. Perfetto UI processes a local file locally unless the user chooses
its upload or share action. Review the trace and follow the workload's data
handling policy before any upload.

See the [profiling validation report](profiling-validation-report.md) for the
correctness matrix, performance measurements, buffer sizes, and production
decision evidence behind this workflow.
