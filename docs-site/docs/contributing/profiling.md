# Profile Delta Funnel Workloads

Use this guide to choose between exact semantic timelines and sampled native
call stacks when diagnosing Python-driven Delta Funnel workloads on Linux.
Stable semantic JSON works with normal Python builds. The Samply and Perfetto
workflows are intended for experienced developers working from a source
checkout. Published Python wheels do not include the optional Perfetto
producer.

## Choose the diagnostic mode

| Goal | Mode |
| --- | --- |
| Inspect exact operation, phase, query, worker, and operator timing | Stable semantic JSON export |
| Find native Rust CPU hotspots and source lines with the smallest capture | Samply |
| Correlate a brief workload with sampled native Rust stacks | Standard short Perfetto |
| Correlate a workload expected to run for up to ten minutes | Standard streaming Perfetto |
| Add scheduler and wakeup context to a short standard capture | Deep-system Perfetto |

Use the short standard mode for a brief workload and the streaming standard
mode when the expected duration exceeds two minutes. Both modes record exact
begin and end timestamps as semantic Track Events. Their 100 Hz native call
stacks are statistical samples, so nearby runs can have different sample
counts. On Linux, native sampling is on-CPU only. Time blocked on I/O, locks,
or sleep is absent from the sampled stacks; use the deep-system mode only when
scheduler context is needed.

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
[Tracing and diagnostics](../advanced/tracing-and-diagnostics.md#inspect-returned-preview-diagnostics).

Open the JSON with VizTracer's viewer or any compatible Chrome Trace Event
viewer:

```bash
vizviewer preview-trace.json
```

The export contains exact semantic wall-clock intervals. Operator lifecycle
bars can include waiting, and parallel intervals can overlap. Do not add their
durations and interpret the sum as elapsed wall time.

## Profile native CPU with Samply

Use Samply when native Rust functions and source lines matter more than exact
Delta Funnel phase boundaries. Samply can show CPython, PyO3, Delta Funnel,
DataFusion, Delta Kernel, Arrow, Parquet, and Tokio frames in one profile. It
records operating-system threads rather than Delta Funnel logical workers.

Samply is a standalone local development tool. It does not add a profiler to
Delta Funnel or combine its samples with the semantic timeline.

### Install Samply and allow performance events

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

### Build an optimized extension with symbols

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

### Record and reopen a profile

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

### Inspect Samply stacks

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

## Prepare Perfetto diagnostics

Perfetto capture is for occasional diagnostics, not continuous or unattended
collection. Use the short configuration for brief captures. Use the bounded
streaming configuration for a workload expected to run for more than two
minutes and up to ten minutes. Streaming has a 12-minute safety timeout and a
512 MiB saved-file cap, but high event volume can reach that cap sooner. See
[#522](https://github.com/mag1cfrog/delta-funnel/issues/522) for the historical
decision evidence and
[#527](https://github.com/mag1cfrog/delta-funnel/issues/527) for the bounded
streaming design and validation contract.

Run this workflow from the repository root. Install these tools and make them
available on `PATH`:

- `maturin`
- Perfetto `tracebox`
- Perfetto `trace_processor_shell`
- Linux `perf`
- GNU `timeout`

The workflow is verified with matching Perfetto v57.2 tools. Record the version
output when sharing a health summary:

```sh
test "$(uname -s)" = Linux
command -v maturin tracebox trace_processor_shell perf timeout
maturin --version
tracebox --version
trace_processor_shell --version
tracebox --help | grep -F -- '--system-sockets'
tracebox --system-sockets --query >/dev/null
cat /proc/sys/kernel/perf_event_paranoid
cat /proc/sys/kernel/kptr_restrict
perf stat --all-cpus --event cpu-clock -- sleep 0.1 >/dev/null
```

The `perf stat` command exercises the same per-CPU software clock used by the
checked-in Perfetto configs. It is the authoritative permission preflight;
`tracebox --notify-fd` can report ready even when the kernel later rejects
`perf_event_open`. The final `tracebox` readiness check below independently
verifies the producer and configured data sources. Either check must fail
before the workload starts.

If the host policy does not already allow the capture, this temporary setting
lasts until reboot:

```sh
echo '-1' | sudo tee /proc/sys/kernel/perf_event_paranoid
```

For a persistent setting, review the security impact with the system owner,
then use the host's normal sysctl management. A conventional Linux setup is:

```sh
printf 'kernel.perf_event_paranoid=-1\n' | \
  sudo tee /etc/sysctl.d/90-delta-funnel-perfetto.conf
sudo sysctl --system
```

These commands show every privileged action. Do not lower the setting on a
shared or production host without approval. Use the least permissive host
policy that passes the real readiness check.

## Build a diagnostics-enabled Python extension

From the repository root, create and activate a dedicated virtual environment:

```sh
python -m venv target/python-perfetto-venv
source target/python-perfetto-venv/bin/activate
maturin develop --locked --profile profiling \
  --features perfetto-profile \
  --manifest-path crates/delta-funnel-python/Cargo.toml
ln -sf python \
  target/python-perfetto-venv/bin/delta-funnel-perfetto-preview
```

The `profiling` profile preserves information needed to symbolize native call
stacks. The `perfetto-profile` feature is opt-in so normal builds and published
wheels do not link the Perfetto SDK. The virtual-environment symlink gives the
example a unique process command line without breaking Python's environment
discovery. Keep this exact unstripped diagnostic extension available while
inspecting the trace. Optimized and inlined Rust code can still collapse or
move frames even when symbols are present. A restrictive `kernel.kptr_restrict`
setting can leave kernel frames as raw addresses, but it does not prevent local
user-space Rust symbolization or make the capture unhealthy.

## Start the external capture

Choose exactly one configuration. The short mode preserves the beginning of a
brief capture. The streaming mode periodically drains ring buffers to a bounded
file and is intended for a workload expected to run for more than two minutes:

```bash
# Short standard capture:
capture_config=tools/perfetto/delta-funnel-standard.pbtx
capture_path=target/perfetto-captures/python-preview.pftrace
configured_file_cap_bytes=
```

```bash
# Bounded streaming standard capture:
capture_config=tools/perfetto/delta-funnel-standard-streaming.pbtx
capture_path=target/perfetto-captures/python-preview-streaming.pftrace
configured_file_cap_bytes=536870912
```

Preflight the selected new local output path, then start `tracebox` as a child
of the controlling Bash shell. The readiness FIFO lets `tracebox` reject
unavailable data sources before Python runs:

```bash
capture_dir="${capture_path%/*}"
ready_fifo="$capture_dir/tracebox-ready"
ready_timeout_seconds=15

mkdir -p "$capture_dir"
test -w "$capture_dir"
test ! -e "$capture_path"
rm -f "$ready_fifo"
mkfifo "$ready_fifo"

tracebox --txt --system-sockets --no-clobber \
  --notify-fd 3 \
  --config "$capture_config" \
  --out "$capture_path" \
  3>"$ready_fifo" &
trace_pid=$!

if readiness_raw="$(timeout "$ready_timeout_seconds" \
  od -An -tu1 -N1 "$ready_fifo")"; then
  readiness="$(tr -d '[:space:]' <<<"$readiness_raw")"
else
  readiness=
fi
rm -f "$ready_fifo"
if test "$readiness" != 0; then
  kill -TERM "$trace_pid" 2>/dev/null || true
  if wait "$trace_pid"; then
    tracebox_status=0
  else
    tracebox_status=$?
  fi
  unset trace_pid
  printf 'tracebox readiness failed: status=%s\n' \
    "$tracebox_status" >&2
fi
test "$readiness" = 0
```

Both standard configurations scope native sampling to the
`delta-funnel-perfetto-preview` process token used below. It samples native call
stacks at 100 Hz while preserving exact Delta Funnel semantic spans and process
metadata in separate buffers. If the final `test` fails, tracebox has already
been stopped and waited for. Fix the tool, socket, permission, timeout, or
output error. Do not run the workload. The explicit readiness timeout also
bounds data-source failures that do not produce a notification byte.

The short standard buffers are 128 MiB for semantic events, 64 MiB for native
samples, and 4 MiB for process metadata. `DISCARD` preserves the beginning of a
short capture but drops new packets after a buffer fills, so a saturated buffer
has an incomplete tail.

The streaming standard buffers are 64 MiB for semantic events and 64 MiB for
native samples plus process metadata. `RING_BUFFER` retains recent packets
between five-second file writes. The 512 MiB cap bounds the saved file, while
the 12-minute duration is only a safety stop. Neither value guarantees ten
minutes of retention: a workload with many short operations can fill the file
sooner. Streaming intentionally excludes scheduler tracing. Use deep-system
mode separately when scheduler evidence is required.

The streaming config asks the Perfetto v57.2 tracing service to deflate the
saved trace. Stock Perfetto UI and Trace Processor open the compressed file
directly. Compression preserves every captured packet and reduces disk usage at
the cost of additional tracing-service CPU. The short configurations remain
uncompressed so their startup and finalization behavior does not change.

## Activate diagnostics and run a preview

Call `init_perfetto_diagnostics()` once, before `init_logging()` and before any
preview or write operation:

```bash
if test -n "${trace_pid:-}" && kill -0 "$trace_pid" 2>/dev/null; then
  if target/python-perfetto-venv/bin/delta-funnel-perfetto-preview \
    examples/perfetto_preview.py; then
    workload_status=0
  else
    workload_status=$?
  fi
else
  printf 'tracebox is not ready; workload was not started\n' >&2
  workload_status=125
fi
```

The repository-owned example generates data in memory, exercises planning and
parallel preview execution, and prints only a completion message. It exits
nonzero if diagnostics are unavailable or not ready, and it never starts or
stops `tracebox` itself.

`DELTAFUNNEL_LOG` and the function's `filter` and `logger` arguments configure
the Python logging side of the combined subscriber. They do not disable the
`delta_funnel.profile` events sent to Perfetto.

Before running a different workload, keep the same process token and call
`init_perfetto_diagnostics()` before any other tracing or Delta Funnel setup.
Activation is process-wide: every Delta Funnel operation in that Python process
after activation can appear in the trace. Concurrent unrelated operations are
not isolated automatically.

## Stop the capture and preserve both results

After Python exits, stop the child `tracebox` and preserve its result separately
from the workload result:

```bash
if test -n "${trace_pid:-}"; then
  kill -TERM "$trace_pid" 2>/dev/null || true
  if wait "$trace_pid"; then
    tracebox_status=0
  else
    tracebox_status=$?
  fi
else
  tracebox_status="${tracebox_status:-125}"
fi
printf 'workload_status=%s tracebox_status=%s\n' \
  "$workload_status" "$tracebox_status"
```

For an interrupted target, run the same `kill` and `wait` commands before
leaving the shell. Keep the partial `.pftrace`; it may contain the only useful
diagnostic evidence. A successful workload remains successful if tracebox
shutdown, file writing, the health query, or the UI later fails. In particular,
never retry a database write because capture finalization failed.

In streaming mode, reaching the saved-file cap can stop tracebox before the
workload exits. The target process continues independently. The `kill` command
above is harmless when the child has already exited, and `wait` still collects
its status. A zero `tracebox_status` only reports a clean process exit. It does
not prove that the full workload interval was retained or finalized.

## Check capture health

Run the checked-in health command before interpreting the trace. Pass the
configured cap for streaming mode so the row records both the configured bound
and the factual saved size:

```bash
if test -n "$configured_file_cap_bytes"; then
  tools/perfetto/capture-health \
    "$capture_path" "$configured_file_cap_bytes"
else
  tools/perfetto/capture-health "$capture_path"
fi
```

`capture_complete` must be 1 before treating the file as a complete capture.
`semantic_complete` reports exact Delta Funnel event health independently from
the statistical sample counts. Nonzero `perf_samples_skipped` or
`perf_sample_without_callsite_count` values reduce sampling confidence but do
not automatically make exact semantic data incomplete. A normal
`truncation_marker_count` records the documented per-operation activity budget
and is not buffer loss. `finalization_observed` is 1 only when the trace contains
Perfetto's `tracing_disabled` lifecycle marker. TraceStats in a streaming trace
are periodic snapshots, so `flush_failure_count` reports observed failures but
is not a separate final-flush attestation. Use the flush, semantic, and
finalization fields together when deciding whether the available evidence is
complete.

`configured_file_cap_bytes` and `saved_file_bytes` are facts, not a stop-reason
heuristic. Do not infer that the cap was or was not reached from their
proximity. If `finalization_observed` is 0, the available file may have stopped
because of the cap, a write failure, or hard termination. Perfetto does not
provide enough retained evidence here to distinguish those cases reliably.

An incomplete streaming trace still contains packets drained before output
stopped. Treat missing tail time as unknown, not as zero activity. Asynchronous
data sources can stop at slightly different boundaries, and ring-buffer
overwrite can remove older packets that were not drained in time. Exact spans
and native samples that passed the health checks remain useful for their
retained interval, but they do not describe the omitted interval.

## Inspect the semantic hierarchy and native stacks

Open `$capture_path` in
[Perfetto UI](https://ui.perfetto.dev/). The `Delta Funnel diagnostics` process
track contains this hierarchy:

```text
Operation
  Phases
  Query
    Worker
      Operator and lower-level activity
```

Select the operation's time range, then use the `Perf sample flamegraph` panel
in `Top Down` mode to move from sampled native entry points into deeper Rust
functions. Use an exact worker identity such as
`w-00000000000000000001]` in the track-name filter when isolating one logical
worker. The closing bracket prevents worker 1 from also matching worker 10 or
worker 14.

Perfetto may report a small number of skipped performance samples. That affects
sample density, not the exact semantic spans. Treat a high skipped-sample count
or lost semantic packets as an unhealthy capture.

The generated example takes about 6 seconds and produced about 12 MB during the
v57.2 validation run. Hardware and symbols change both values. The short
standard mode allocates 196 MiB of service buffers, while streaming allocates
128 MiB. The diagnostics-enabled target additionally requests a bounded 32 MiB
producer shared-memory buffer to absorb semantic event bursts. These are
in-memory buffer allocations, not expected file sizes. Always check the factual
saved size and health row.

## Use deep-system mode only for scheduler questions

Deep-system mode additionally requires read and write access to tracefs. Check
that access before starting tracebox:

```sh
test -r /sys/kernel/tracing/events/sched/sched_switch/id
test -w /sys/kernel/tracing/tracing_on
```

If either check fails, use the host's normal access-management process to grant
the current user tracefs access. Do not run tracebox or the Python workload with
`sudo` as a workaround. Then replace the `capture_config` and `capture_path`
assignments in the start step:

```bash
capture_config=tools/perfetto/delta-funnel-deep-system.pbtx
capture_path=target/perfetto-captures/python-preview-deep-system.pftrace
configured_file_cap_bytes=
```

Deep-system mode adds a separate 256 MiB compact scheduler buffer, for a total
allocation of 452 MiB. Confirm that scheduler tracks are present in Perfetto UI
before relying on them; the canonical health row intentionally contains only
fields shared by short and streaming captures. Scheduler events can
substantially increase file size and system overhead, so do not use this mode
as the default.

## Keep capture data local

The `.pftrace` can contain process names, command lines, library paths, function
names, timing, and system activity. Store it under the local ignored `target`
directory and open it directly in the stock UI. Perfetto processes a local file
locally unless you explicitly use its upload or share action. Review the trace
before any upload and follow the data-handling policy for the workload.

## Activation results and errors

`init_perfetto_diagnostics()` has one-shot process-wide behavior:

- `True` means the combined Python logging and Perfetto subscriber was installed
  and an external capture was ready before the function returned.
- `False` means another global tracing subscriber was already installed. The
  function does not replace it. Start a fresh Python process and call
  `init_perfetto_diagnostics()` first.
- A `DeltaFunnelError` with phase `perfetto_diagnostics` reports an unavailable
  build, invalid argument, producer failure, capture timeout, or unavailable
  capture through its structured `kind` field.

The stable diagnostic error kinds are:

```text
not_available
invalid_logger
invalid_wait_timeout
producer_initialization_failed
capture_timeout
capture_unavailable
```

An invalid logging filter remains a configuration error with kind
`invalid_logging_filter`.

Python activates the producer and subscriber, but it does not start, stop, or
finalize `tracebox`. Once activation succeeds, later capture-service problems
cannot change a successful preview or database write into a Python operation
failure. Always stop and validate the external trace separately.
