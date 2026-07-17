# Local Development

Use this guide when developing Delta Funnel itself. Run all commands from the
repository root.

## Prerequisites

- Rust toolchain matching the workspace `rust-version`
- Python 3.10 or newer
- `maturin` when building the Python wheel

## Get the source

```bash
git clone https://github.com/mag1cfrog/delta-funnel.git
cd delta-funnel
```

Install `maturin` if it is not already available:

```bash
python -m pip install "maturin>=1.11,<2"
```

## Check the Rust workspace

```bash
cargo check --workspace
```

## Build and smoke-test the Python wheel

```bash
cargo xtask python-package-check
```

This command builds the `deltafunnel` wheel, checks the typing marker files,
installs the wheel into a clean virtual environment, imports `deltafunnel`, and
constructs `Session()`.

## Run the SQL Server integration tests

SQL Server tests are opt-in and managed by xtask:

```bash
cargo xtask sqlserver-test
```

The runner can start a local SQL Server container, create the test database,
run Rust and Python write tests, and remove the container when it exits.

See [SQL Server integration tests](sql-server-tests.md) for container runtime,
existing server, and individual suite options.

## Profile Python-driven Rust execution with Samply

Use Samply when you need sampled call stacks below Delta Funnel's semantic
operation timeline. Samply can show CPython native, PyO3, Delta Funnel,
DataFusion, Delta Kernel, Arrow, Parquet, and Tokio frames in one profile. Use
the [tracing and diagnostics](../advanced/tracing-and-diagnostics.md) workflow
instead when you need exact phase boundaries, logical worker identities,
operation metrics, or wall-clock ordering.

This workflow is for local development. It does not add a profiler to the
Delta Funnel runtime.

### Install Samply and allow Linux performance events

Install Samply from crates.io:

```bash
cargo install --locked samply
samply --version
```

On Linux, check whether unprivileged processes may read performance events:

```bash
cat /proc/sys/kernel/perf_event_paranoid
```

If the value is greater than `1`, grant access until the next reboot:

```bash
echo '1' | sudo tee /proc/sys/kernel/perf_event_paranoid
```

If `samply record` still fails with `mmap failed: Operation not permitted`,
increase the locked performance-event memory limit until the next reboot:

```bash
sudo sysctl kernel.perf_event_mlock_kb=2048
```

These settings loosen system-wide performance-event access. Use them only on
a development machine where that access is acceptable.

### Build an optimized extension with symbols

Create and activate an isolated environment so that Python cannot import a
previously installed Delta Funnel wheel:

```bash
python3 -m venv target/samply-venv
source target/samply-venv/bin/activate
```

Build the extension with the workspace's `profiling` Cargo profile:

```bash
maturin develop \
  --locked \
  --profile profiling \
  --manifest-path crates/delta-funnel-python/Cargo.toml
```

The `profiling` profile inherits all release optimizations and adds line-table
debug information. It does not change the normal release profile.

Confirm that Python resolves the extension from the isolated environment:

```bash
python -c \
  'import deltafunnel.deltafunnel as native; print(native.__file__)'
```

The printed path must be under `target/samply-venv`. On Linux, also confirm
that the extension has debug information and has not been stripped:

```bash
native_path=$(python -c \
  'import deltafunnel.deltafunnel as native; print(native.__file__)')
file "$native_path"
```

The output should contain `with debug_info, not stripped`.

### Record and reopen a profile

Run Samply directly against the isolated Python interpreter and a
representative workload:

```bash
samply record \
  target/samply-venv/bin/python \
  path/to/workload.py
```

Samply starts the Python process, samples its native operating-system threads,
then opens Firefox Profiler. Prefer one representative invocation that runs
for a few seconds. Repeating a short script creates extra processes and
runtime threads that make the profile harder to read.

Use the repository progress smoke test to check the build and recording
plumbing with deterministic local data:

```bash
PYTHONPATH=crates/delta-funnel-python/tests \
  samply record \
  target/samply-venv/bin/python \
  crates/delta-funnel-python/tests/progress_smoke.py before
```

The smoke test is intentionally short. Use a real workload for performance
analysis so that the default 1000 Hz sampling rate collects enough samples.

Save the recording when you want to reopen it later:

```bash
samply record \
  --save-only \
  --output target/delta-funnel-samply.json.gz \
  target/samply-venv/bin/python \
  path/to/workload.py

samply load target/delta-funnel-samply.json.gz
```

Keep the symbolized extension and local Cargo sources available while
`samply load` is running. Its local server supplies symbols and source
locations to Firefox Profiler. Profiles can contain paths, process arguments,
and workload details, so do not upload one unless it is safe to share.

### Read the profile

Start with these views and filters:

1. Select the `python` track to follow the Python to PyO3 to Delta Funnel call
   path and synchronous planning work.
2. Select a busy `delta-funnel-ru` track to inspect Tokio and DataFusion
   execution. These are operating-system threads, not Delta Funnel logical
   worker identities.
3. Click the track-count button and use `Only display tracks that match a
   certain text` to narrow the track list to `python` or `delta-funnel-ru`.
4. In Call Tree or Flame Graph, use `Filter stacks` with terms such as
   `delta_funnel::`, `datafusion`, `delta_kernel`, `parquet`, `arrow_`,
   `tokio::`, or `pyo3`.
5. Select a frame to see its source path and line. Frames marked `(inlined)`
   preserve compiler inline information.
6. Use Stack Chart for a time-ordered view of sampled stacks and Flame Graph
   for aggregate hot paths.

The percentages are statistical CPU-time estimates. Short functions may not
be sampled, and small differences should be confirmed with repeated runs. On
Linux, Samply currently collects on-CPU samples only. Time blocked on IO,
locks, or scheduling can contribute to wall time without appearing as a hot
stack.

Do not add an exact tracing span only to expose a function that Samply already
resolves. Add semantic instrumentation when the question requires exact
wall-clock phase boundaries, off-CPU waits, logical worker correlation, or
domain-specific counters. The operation timeline and Samply answer different
questions and can be used together.

### Verified Linux result

The symbolized extension was built and imported with Python 3.12 and 3.14 on
Fedora Linux 43 x86-64 with Linux 6.19 and Rust 1.97. The inspected recording
used Python 3.12 and Samply 0.13.1. It resolved CPython and PyO3 native symbols,
plus function names, source lines, and inline frames for Delta Funnel,
DataFusion, Delta Kernel, Arrow, Parquet, and Tokio code. The CPython to Rust
transition and Rust worker stacks unwound without enabling frame pointers. One
libc entry remained generic, but the native call chains were intact.

The short Python validation confirms the mixed-language call stacks, but its
process startup cost is not a representative overhead measurement. The
repeatable 13,394,789-row
[Delta scan benchmark](scan-benchmarks.md#compare-samply-with-detailed-profiling)
compares Samply with both disabled controls and detailed operation profiling
on the same optimized binary.
