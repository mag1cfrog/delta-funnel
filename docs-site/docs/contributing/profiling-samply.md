# Profile Native CPU with Samply

Use Samply when native Rust functions and source lines matter more than exact
Delta Funnel phase boundaries. Samply can show CPython, PyO3, Delta Funnel,
DataFusion, Delta Kernel, Arrow, Parquet, and Tokio frames in one profile. It
records operating-system threads rather than Delta Funnel logical workers.

Samply is a standalone local development tool. It does not add a profiler to
Delta Funnel or combine its samples with the semantic timeline.

## Install Samply and allow performance events

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

If `samply record` still reports `mmap failed: Operation not permitted`,
increase the locked performance-event memory limit until the next reboot:

```bash
sudo sysctl kernel.perf_event_mlock_kb=2048
```

These settings loosen system-wide performance-event access. Use them only on
a development machine where that access is acceptable.

## Build an optimized extension with symbols

Create an isolated environment so Python cannot import an older installed
Delta Funnel wheel:

```bash
python3 -m venv target/samply-venv
source target/samply-venv/bin/activate
maturin develop \
  --locked \
  --profile profiling \
  --manifest-path crates/delta-funnel-python/Cargo.toml
```

The `profiling` profile keeps release optimizations and adds line-table debug
information. Confirm that the extension comes from the isolated environment
and has not been stripped:

```bash
native_path=$(python -c \
  'import deltafunnel.deltafunnel as native; print(native.__file__)')
test "${native_path#"$PWD/target/samply-venv/"}" != "$native_path"
file "$native_path"
```

The `file` output should contain `with debug_info, not stripped`.

## Record and reopen a profile

Run Samply directly against the isolated Python interpreter and one
representative workload:

```bash
samply record \
  target/samply-venv/bin/python \
  path/to/workload.py
```

Prefer an invocation that runs for at least a few seconds. Repeating a very
short script creates extra processes and runtime threads without producing a
representative application profile.

Use the repository progress smoke test only to verify build and recording
plumbing:

```bash
PYTHONPATH=crates/delta-funnel-python/tests \
  samply record \
  target/samply-venv/bin/python \
  crates/delta-funnel-python/tests/progress_smoke.py before
```

Save a recording when it must be reopened later:

```bash
samply record \
  --save-only \
  --output target/delta-funnel-samply.json.gz \
  target/samply-venv/bin/python \
  path/to/workload.py

samply load target/delta-funnel-samply.json.gz
```

Keep the symbolized extension and local Cargo sources available while
`samply load` runs. Its local server supplies symbols and source locations to
Firefox Profiler.

## Inspect Samply stacks

Start with these views and filters:

1. Select the `python` track for the Python to PyO3 to Delta Funnel call path
   and synchronous planning work.
2. Select a busy `delta-funnel-ru` track for Tokio and DataFusion execution.
3. Use the track-count filter to narrow tracks to `python` or
   `delta-funnel-ru`.
4. Filter Call Tree or Flame Graph stacks with `delta_funnel::`, `datafusion`,
   `delta_kernel`, `parquet`, `arrow_`, `tokio::`, or `pyo3`.
5. Select a frame to inspect its source path and line.
6. Use Stack Chart for time-ordered samples and Flame Graph for aggregate hot
   paths.

Percentages are statistical CPU-time estimates. Short functions may not be
sampled, and small differences need repeated runs. On Linux, Samply currently
collects on-CPU samples only. Time blocked on I/O, locks, or scheduling can
contribute to wall time without appearing as a hot stack.

Do not add an exact semantic span only to expose a function that Samply already
resolves. Add semantic instrumentation when the question requires exact phase
boundaries, off-CPU waits, logical worker correlation, or domain-specific
counters.
