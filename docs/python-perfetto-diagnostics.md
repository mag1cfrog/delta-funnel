# Capture Python Perfetto Diagnostics

Use this workflow to capture Delta Funnel's exact semantic operation hierarchy
and sampled native Rust call stacks in one local Perfetto trace. It is intended
for experienced developers diagnosing short Python-driven workloads on Linux.
Published Python wheels do not include the Perfetto producer.

This workflow has a **Limited go** status. The checked-in configurations use a
two-minute safety timeout and bounded `DISCARD` buffers. They are suitable for
occasional short captures, not unattended or 10-minute profiling. See
[#522](https://github.com/mag1cfrog/delta-funnel/issues/522) for the historical
decision evidence and
[#527](https://github.com/mag1cfrog/delta-funnel/issues/527) for the planned
long-capture work.

## Choose the diagnostic mode

| Goal | Mode |
| --- | --- |
| Inspect exact operation, phase, query, worker, and operator timing | Stable semantic JSON export |
| Find native Rust CPU hotspots and source lines with the smallest capture | Samply |
| Correlate exact semantic timing with sampled native Rust stacks | Standard Perfetto |
| Add scheduler and wakeup context to a short standard capture | Deep-system Perfetto |

Use the standard Perfetto mode by default for this guide. Semantic Track Events
record exact begin and end timestamps. The 100 Hz native call stacks are
statistical samples, so nearby runs can have different sample counts. On Linux,
native sampling is on-CPU only. Time blocked on I/O, locks, or sleep is absent
from the sampled stacks; use the deep-system mode only when scheduler context is
needed.

## Prerequisites

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

Preflight a new local output path, then start `tracebox` as a child of the
controlling Bash shell. The readiness FIFO lets `tracebox` reject unavailable
data sources before Python runs:

```bash
capture_config=tools/perfetto/delta-funnel-standard.pbtx
capture_path=target/perfetto-captures/python-preview.pftrace
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

The standard configuration scopes native sampling to the
`delta-funnel-perfetto-preview` process token used below. It samples native call
stacks at 100 Hz while preserving exact Delta Funnel semantic spans and process
metadata in separate buffers. If the final `test` fails, tracebox has already
been stopped and waited for. Fix the tool, socket, permission, timeout, or
output error. Do not run the workload. The explicit readiness timeout also
bounds data-source failures that do not produce a notification byte.

The standard buffers are 128 MiB for semantic events, 64 MiB for native
samples, and 4 MiB for process metadata. `DISCARD` preserves the beginning of a
short capture but drops new packets after a buffer fills, so a saturated buffer
has an incomplete tail.

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

## Check capture health

Run the checked-in Trace Processor health query and report the saved file size
before interpreting the trace:

```sh
trace_processor_shell query \
  -f tools/perfetto/short-capture-health.sql \
  "$capture_path"
stat --format='trace_file_bytes=%s' "$capture_path"
```

`semantic_health` must be `complete`. Any incomplete operation root, truncation
marker, missing canonical field, crossing semantic slice, buffer loss, or flush
failure makes the semantic capture incomplete. Nonzero `data_source_loss_events`,
`skipped_samples`, or `unwind_errors` values are visible evidence of reduced
source coverage and must be reported. The system timebase can produce
`samples_without_call_sites` for non-target work, so that count is evidence
rather than an automatic failure. A
`trace_finalization_status` of `not_reported` means the trace did not expose a
dedicated final-flush result; use the separate flush and semantic completeness
fields instead. For the standard config, `scheduler_rows` must be `0`.

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
v57.2 validation run. Hardware and symbols change both values. The total
standard buffer allocation is 196 MiB, so check the saved size and health row
instead of assuming a fixed output size.

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
```

Deep-system mode adds a separate 256 MiB compact scheduler buffer, for a total
allocation of 452 MiB. Its health row should have nonzero `scheduler_rows`.
Scheduler events can substantially increase file size and system overhead, so
do not use this mode as the default.

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
