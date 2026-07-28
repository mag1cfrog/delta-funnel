# Set Up Perfetto Diagnostics for Python

This guide installs the diagnostics wheel, Perfetto tools, and native
symbolizer needed to generate an operation-scoped ranked HTML report directly
from `Table.preview`, `Table.write_to_mssql`, or `Session.write_all`.

Perfetto diagnostics are intended for occasional local investigation, not
continuous collection. The TestPyPI wheel supports CPython 3.10 or newer on
Linux x86_64 with glibc 2.28 or newer.

## 1. Prepare the Linux host

Install `llvm-symbolizer` 13 or newer and `debuginfod-find` for the host
distribution.

Fedora:

```sh
sudo dnf install llvm elfutils-debuginfod-client
```

Ubuntu or Debian:

```sh
sudo apt update
sudo apt install llvm debuginfod
```

If the distribution's default `llvm` package is older than LLVM 13, install a
newer supported LLVM package and make its `llvm-symbolizer` available under
that unversioned command name on `PATH`.

On another glibc Linux distribution, install the packages that provide an LLVM
13 or newer `llvm-symbolizer` and the `debuginfod-find` command. The
diagnostics wheel does not currently support macOS, Windows, or musl Linux.

Download the official Trace Processor and traceconv launchers. Saving the Trace
Processor launcher as `trace_processor_shell` matches the command Delta Funnel
uses:

```sh
mkdir -p ~/.local/bin

curl -fL https://get.perfetto.dev/trace_processor \
  -o ~/.local/bin/trace_processor_shell
curl -fL https://get.perfetto.dev/traceconv \
  -o ~/.local/bin/traceconv

chmod +x \
  ~/.local/bin/trace_processor_shell \
  ~/.local/bin/traceconv
export PATH="$HOME/.local/bin:$PATH"

trace_processor_shell --help >/dev/null
traceconv --help 2>&1 | grep -q symbolize
llvm-symbolizer --output-style=JSON </dev/null >/dev/null
```

The launchers download and cache the matching native binaries on first use.
Delta Funnel has been verified with Perfetto v57.2.

Allow native call-stack sampling on a development machine:

```sh
sudo sysctl kernel.perf_event_paranoid=-1
```

This loosens system-wide performance-event access. Do not use it on a shared or
production host without approval. The temporary setting lasts until reboot.
Do not run the Python workload or `tracebox` with `sudo`.

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

Sync the environment and verify the installed package:

```sh
uv sync --upgrade-package deltafunnel

uv run python -c \
  'from deltafunnel import RankedProfileConfig; print(RankedProfileConfig("profile.html"))'

perfetto_assets="$(uv run python -c \
  'from importlib.resources import files; print(files("deltafunnel") / "perfetto")')"
"$perfetto_assets/delta-funnel-tracebox" --version
```

For a 1000 Hz profile, select the official debuginfod service for the host
distribution:

```sh
# Fedora
export DEBUGINFOD_URLS=https://debuginfod.fedoraproject.org/

# Ubuntu
export DEBUGINFOD_URLS=https://debuginfod.ubuntu.com/

# Debian
export DEBUGINFOD_URLS=https://debuginfod.debian.net/
```

Run only the matching `export` command. Then cache the exact glibc and CPython
debuginfo used by the workload:

```sh
python_executable="$(uv run python -c 'import sys; print(sys.executable)')"
python_library="$(uv run python -c \
  'import pathlib, sysconfig; print(pathlib.Path(sysconfig.get_config_var("LIBDIR")) / sysconfig.get_config_var("LDLIBRARY"))')"
libc_library="$(ldd "$python_executable" | awk '$1 == "libc.so.6" { print $3; exit }')"
test -n "$libc_library"

debuginfod-find debuginfo "$libc_library" >/dev/null
if llvm-readelf -n "$python_library" | grep -q "Build ID:"; then
  debuginfod-find debuginfo "$python_library" >/dev/null
fi
```

This downloads matching debug files into the user's local debuginfod cache. It
does not upload the trace. `debuginfod-find` reads each binary's Build ID and
fetches the matching debug file, so no manual Build ID lookup is needed. Run
these commands once on each machine and again after upgrading glibc or the
Python runtime used by the workload. Delta Funnel searches the cache and
`/usr/lib/debug` automatically when `traceconv` symbolizes a 1000 Hz operation
profile.

Perfetto's offline symbol lookup requires a Build ID even when the Python
runtime contains unstripped symbols. If the runtime has no Build ID, use the
distribution's Python or another diagnostics-capable Python build before
recording a 1000 Hz profile.

On another distribution, use its official or organization-provided
`DEBUGINFOD_URLS`. If no debuginfod service is available, install matching
debug packages under `/usr/lib/debug` instead.

The generated `uv.lock` records the exact diagnostics version and TestPyPI
source. Keep it with the report when reproducibility matters.

The wheel includes a small launcher for Delta Funnel's pinned Perfetto v57.2
tracebox build. The launcher downloads and verifies the 1 MB release archive
on first use, then caches the native binary. That tracebox manages `traced`,
`traced_probes`, and `traced_perf` for each capture; do not start separate
daemons.

## 3. Generate one operation-scoped report

Follow
[Generate an operation-scoped ranked HTML report](../advanced/execution-profiling.md#generate-an-operation-scoped-ranked-html-report).
The Python operation starts and stops its own capture, enables the required
subscriber, and writes the final interactive HTML report. No separate capture
command or `init_perfetto_diagnostics()` call is needed.

## Advanced: capture a whole Python process

Use the manual path only when one operation scope is insufficient, when
several operations must be correlated, or when you need the raw Perfetto
timeline or scheduler context. For one operation, retain a `.dfprofile`
artifact and use the terminal inspector without a whole-process capture. A
whole-process capture includes every enabled Delta Funnel operation between
activation and process exit.

The whole-process command also requires `perf` and the util-linux `setsid`
command. Install the packages for the host distribution:

```sh
# Fedora
sudo dnf install perf util-linux-core

# Ubuntu
sudo apt install linux-tools-generic util-linux

# Debian
sudo apt install linux-perf util-linux
```

Run only the matching install command, then verify both executables:

```sh
perf --version
setsid --help >/dev/null
```

Locate the packaged capture command and verify the diagnostics CLI:

```sh
environment_python="$(uv run python -c 'import sys; print(sys.executable)')"
perfetto_assets="$(uv run python -c \
  'from importlib.resources import files; print(files("deltafunnel") / "perfetto")')"
capture_workload="$perfetto_assets/capture-workload"
test -x "$capture_workload"

uv run delta-funnel-perfetto --help
uv run delta-funnel-perfetto inspect --help
```

### Activate diagnostics in the workload

Add this before `init_logging()` and before any preview or write operation:

```python
import deltafunnel

if not deltafunnel.init_perfetto_diagnostics():
    raise RuntimeError("another tracing subscriber is already installed")
```

Activation is process-wide. Every later Delta Funnel operation in that Python
process can appear in the trace.

### Record the workload

Run one command from the workload project root. Use a new output name for each
capture because existing trace and report files are never overwritten:

```sh
"$capture_workload" \
  --output target/perfetto-captures/query.pftrace \
  -- "$environment_python" path/to/workload.py
```

The command starts Perfetto, waits until all data sources are ready, runs the
workload, finalizes and checks the saved trace, then generates the sibling
ranked report. A successful run creates:

```text
target/perfetto-captures/query.pftrace
target/perfetto-captures/query.profile.html
```

It ends with one machine-readable status line:

```text
workload_status=0 tracebox_status=0 health_status=0 report_status=0 sample_hz=1000 trace=target/perfetto-captures/query.pftrace report=target/perfetto-captures/query.profile.html
```

`workload_status` is the workload's actual result. `tracebox_status`,
`health_status`, and `report_status` independently describe capture
finalization, raw-trace health, and report generation. Status `0` means that
stage succeeded, while `125` means it did not run. The `report` field appears
only after a complete non-empty report is present.

On ordinary completion, the command exits with `workload_status`, even when
later diagnostics fail. Argument and setup failures return before the workload,
while an interrupted wrapper returns its signal status. The machine-readable
fields are authoritative once capture starts.

A capture, health, or report failure cannot turn a successful database write
into a failed workload. The warning tells automated callers not to retry a
completed write. Keep the raw trace for diagnosis and inspect the nonzero
diagnostics status instead.

Short mode defaults to 1000 Hz. Pass `--sample-hz 100` when lower capture
volume matters more than resolving short native work. The explicit override
accepts only `100` or `1000` and works with every mode. At 1000 Hz the capture
tool also drains the kernel sampling buffers more often to avoid losing short
bursts.

Delta Funnel's pinned tracebox expands Perfetto's callchain queue so 1000 Hz
reports can sample every CPU allowed by the Python process affinity. The report
summary records how many CPUs actually contributed samples.

### Inspect ranked results in the terminal

Materialize the shared ranked model once when an agent or script will inspect a
whole-process capture repeatedly:

```sh
uv run delta-funnel-perfetto report \
  target/perfetto-captures/query.pftrace \
  --output target/perfetto-captures/query.profile.html \
  --artifact-output target/perfetto-captures/query.dfprofile
```

`inspect` accepts the `.dfprofile` artifact, not a raw trace, and never starts
Trace Processor. Use `report --artifact-output` once to convert an older raw
trace.

Start with a bounded one-shot view of the operation roots:

```sh
uv run delta-funnel-perfetto inspect \
  target/perfetto-captures/query.dfprofile
```

Each semantic row reports an exact wall-clock duration and an
`id=semantic:ID` identity. Select a row to show its immediate children and up
to two lower levels:

```sh
uv run delta-funnel-perfetto inspect \
  target/perfetto-captures/query.dfprofile \
  --semantic 42 \
  --depth 2 \
  --limit 30
```

Selected semantic nodes also show their sampled native function roots. Use the
printed `function:SEMANTIC_ID:FUNCTION_ID` identity to descend into one sampled
call path:

```sh
uv run delta-funnel-perfetto inspect \
  target/perfetto-captures/query.dfprofile \
  --function 42:7 \
  --sort inclusive-cpu \
  --depth 2
```

Semantic `duration_ns` and `operation_wall_percent` values are exact wall-clock
measurements. Function `self_cpu_samples`, `inclusive_cpu_samples`, and their
percentages are statistical on-CPU samples. Do not compare their numeric values
as if they used the same unit.

Function rows use compact names such as `BlockingTask::poll`. HTML reports show
the complete native symbol on hover. Pass `--full-symbols` to `inspect` when
terminal output needs the complete symbol.

HTML and terminal reports also compact zero-self single-child call chains by
default. This changes only the view; the complete captured tree remains in the
report model. Select **Show all native frames** in HTML or pass `--all-frames`
to `inspect` to restore every frame.

Use interactive mode when an agent or human needs to navigate repeatedly
without reloading and aggregating the trace:

```sh
uv run delta-funnel-perfetto inspect \
  target/perfetto-captures/query.dfprofile \
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

### Regenerate a ranked HTML report

Normal whole-process capture already generates the sibling HTML report. Use
the standalone command only to regenerate an older raw trace or create another
report destination:

```sh
uv run delta-funnel-perfetto report \
  target/perfetto-captures/query.pftrace \
  --output target/perfetto-captures/query-rerun.profile.html \
  --no-clobber
```

Without `--output`, the destination is
`target/perfetto-captures/query.profile.html`. The standalone command replaces
its output by default. Pass `--no-clobber` to preserve an existing report.

Open the HTML file in a browser. It uses the same ranked semantic and function
data model as the terminal inspector. The raw trace is not embedded in the
report and is not modified.

### Inspect the raw timeline in Perfetto UI

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

Each output owner contains an end-to-end `Execute output` parent. Its children
show query setup and the SQL Server lifecycle on the same wall-clock ruler.

Drag across an output owner or worker track to select the time range you want
to investigate. Temporarily clear the name filter, check
`Process callstacks cpu-clock`, and then reapply the exact operation or worker
filter. Open `Current Selection`, choose `Perf sample flamegraph`, and keep
`Top Down` selected. The semantic tracks show exact wall-clock intervals; the
flame graph shows statistical on-CPU native samples from the same selected
interval.

## Whole-process capture options

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
RUSTFLAGS='-C force-frame-pointers=yes' \
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
