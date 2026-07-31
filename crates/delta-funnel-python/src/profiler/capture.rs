//! Current-process Perfetto capture and ranked report generation.

use std::{
    collections::BTreeSet,
    env,
    ffi::{OsStr, OsString},
    fs, io,
    io::{Read, Write},
    num::NonZeroU64,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use super::DEFAULT_MAX_OPERATOR_ACTIVITY_SPANS;
use crate::yama::authorize_tracebox;
#[cfg(test)]
use crate::yama::ptrace_scope_is_relational;

const TRACEBOX_READY_TIMEOUT: Duration = Duration::from_secs(15);
const CAPTURE_ENABLE_TIMEOUT: Duration = Duration::from_secs(10);
const TRACEBOX_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const TRACEBOX_STOP_POLL_INTERVAL: Duration = Duration::from_millis(10);
// ponytail: cap automatic sizing at 1 GiB; expose buffer sizing only if healthy
// captures exceed it.
const MAX_SEMANTIC_BUFFER_SIZE_KB: u64 = 1024 * 1024;
const BYTES_PER_ADDITIONAL_ACTIVITY_SPAN: u64 = 512;
const TRACEBOX_START_GATE: &str = concat!(
    "import os,sys;",
    "os.read(0,1) or sys.exit(70);",
    "fd=os.open(os.devnull,os.O_RDONLY);",
    "os.dup2(fd,0);",
    "os.close(fd);",
    "os.execvp(sys.argv[2],sys.argv[2:])",
);
const SHORT_CAPTURE_CONFIG: &str = include_str!(concat!(
    env!("OUT_DIR"),
    "/perfetto/delta-funnel-standard.pbtx"
));
const STREAMING_CAPTURE_CONFIG: &str = include_str!(concat!(
    env!("OUT_DIR"),
    "/perfetto/delta-funnel-standard-streaming.pbtx"
));
static OPERATION_PROFILE_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Debug)]
pub(super) struct ProfilerFailure {
    pub(super) kind: &'static str,
    pub(super) message: String,
}

impl ProfilerFailure {
    fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

pub(super) struct OperationCapture {
    output: PathBuf,
    artifact_output: Option<PathBuf>,
    trace: PathBuf,
    directory: tempfile::TempDir,
    symbolize: bool,
    tracebox: ManagedTracebox,
    reservation: Option<ProfileReservation>,
    scope: delta_funnel::perfetto_profile::OperationCaptureScope,
}

impl OperationCapture {
    pub(super) fn start(
        output: PathBuf,
        artifact_output: Option<PathBuf>,
        sample_hz: u16,
        max_operator_activity_spans: NonZeroU64,
        tracebox: PathBuf,
    ) -> Result<Self, ProfilerFailure> {
        let reservation = ProfileReservation::acquire()?;
        let scope = delta_funnel::perfetto_profile::OperationCaptureScope::allocate()
            .map(|scope| scope.with_max_operator_activity_spans(max_operator_activity_spans))
            .ok_or_else(|| {
                ProfilerFailure::new(
                    "capture_scope_unavailable",
                    "operation profile scope identity could not be allocated",
                )
            })?;
        let output = prepare_output_path(&output)?;
        let artifact_output = artifact_output
            .as_deref()
            .map(prepare_output_path)
            .transpose()?;
        let parent = output.parent().ok_or_else(|| {
            ProfilerFailure::new(
                "output_unavailable",
                "profile output path has no parent directory",
            )
        })?;
        preflight_tracebox(tracebox.as_os_str())?;
        preflight_trace_processor()?;
        if sample_hz == 1000 {
            preflight_traceconv()?;
            preflight_llvm_symbolizer()?;
        }
        let directory = tempfile::Builder::new()
            .prefix(".delta-funnel-profile.")
            .tempdir_in(parent)
            .map_err(|_| {
                ProfilerFailure::new(
                    "temporary_directory_failed",
                    "temporary profile directory could not be created",
                )
            })?;
        let trace = directory.path().join("operation.pftrace");
        let config_path = directory.path().join("capture.pbtx");
        let config = current_process_capture_config(sample_hz, max_operator_activity_spans)?;
        fs::write(&config_path, config).map_err(|_| {
            ProfilerFailure::new(
                "capture_config_failed",
                "temporary Perfetto capture config could not be written",
            )
        })?;
        let python = env::current_exe().map_err(|_| {
            ProfilerFailure::new(
                "tracebox_start_failed",
                "Python tracebox start gate could not be located",
            )
        })?;
        let tracebox = start_tracebox_with(
            python.as_os_str(),
            tracebox.as_os_str(),
            &config_path,
            &trace,
            |pid| authorize_tracebox(pid).map_err(profiler_failure_from_ptrace),
            delta_funnel::perfetto_profile::initialize_perfetto,
            delta_funnel::perfetto_profile::wait_for_capture,
        )?;
        crate::perfetto_diagnostics::refresh_perfetto_capture_filter();
        Ok(Self {
            output,
            artifact_output,
            trace,
            directory,
            symbolize: sample_hz == 1000,
            tracebox,
            reservation: Some(reservation),
            scope,
        })
    }

    pub(super) fn in_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        self.scope.in_scope(operation)
    }

    pub(super) fn finish(mut self) -> Result<(), ProfilerFailure> {
        let stop_result = self.tracebox.stop();
        crate::perfetto_diagnostics::refresh_perfetto_capture_filter();
        drop(self.reservation.take());
        stop_result?;
        let report_trace = if self.symbolize {
            symbolize_trace(&self.trace, self.directory.path())?
        } else {
            self.trace.clone()
        };
        delta_funnel::perfetto_profile::generate_operation_ranked_profile_outputs(
            &report_trace,
            &self.output,
            self.artifact_output.as_deref(),
            &self.scope,
        )
        .map(|_| ())
        .map_err(|error| ProfilerFailure::new(error.kind(), error.to_string()))
    }
}

impl Drop for OperationCapture {
    fn drop(&mut self) {
        if self.tracebox.has_child() {
            let _ = self.tracebox.stop();
            crate::perfetto_diagnostics::refresh_perfetto_capture_filter();
        }
    }
}

#[derive(Debug)]
struct ProfileReservation;

impl ProfileReservation {
    fn acquire() -> Result<Self, ProfilerFailure> {
        OPERATION_PROFILE_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self)
            .map_err(|_| {
                ProfilerFailure::new(
                    "profile_already_active",
                    "another operation profile is already active in this process",
                )
            })
    }
}

impl Drop for ProfileReservation {
    fn drop(&mut self) {
        OPERATION_PROFILE_ACTIVE.store(false, Ordering::Release);
    }
}

#[derive(Debug)]
struct ManagedTracebox {
    child: Option<Child>,
}

impl ManagedTracebox {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn id(&self) -> Result<u32, ProfilerFailure> {
        self.child.as_ref().map(Child::id).ok_or_else(|| {
            ProfilerFailure::new(
                "tracebox_start_failed",
                "Perfetto tracebox process was not available",
            )
        })
    }

    fn child_mut(&mut self) -> Result<&mut Child, ProfilerFailure> {
        self.child.as_mut().ok_or_else(|| {
            ProfilerFailure::new(
                "tracebox_start_failed",
                "Perfetto tracebox process was not available",
            )
        })
    }

    fn release_start_gate(&mut self) -> Result<(), ProfilerFailure> {
        let mut gate = self.child_mut()?.stdin.take().ok_or_else(|| {
            ProfilerFailure::new(
                "tracebox_start_failed",
                "Perfetto tracebox start gate was not available",
            )
        })?;
        gate.write_all(b"\n").map_err(|_| {
            ProfilerFailure::new(
                "tracebox_start_failed",
                "Perfetto tracebox start gate could not be released",
            )
        })
    }

    const fn has_child(&self) -> bool {
        self.child.is_some()
    }

    fn stop(&mut self) -> Result<(), ProfilerFailure> {
        let child = self.child.take().ok_or_else(|| {
            ProfilerFailure::new(
                "capture_stop_failed",
                "Perfetto capture process was not available",
            )
        })?;
        stop_tracebox(child)
    }
}

impl Drop for ManagedTracebox {
    fn drop(&mut self) {
        if let Some(child) = self.child.take() {
            let _ = stop_tracebox(child);
        }
    }
}

fn profiler_failure_from_ptrace(error: crate::yama::PtraceAuthorizationError) -> ProfilerFailure {
    ProfilerFailure::new(error.kind, error.message)
}

fn prepare_output_path(output: &Path) -> Result<PathBuf, ProfilerFailure> {
    let output = std::path::absolute(output).map_err(|_| {
        ProfilerFailure::new(
            "output_unavailable",
            "profile output path could not be resolved",
        )
    })?;
    let parent = output.parent().ok_or_else(|| {
        ProfilerFailure::new(
            "output_unavailable",
            "profile output path has no parent directory",
        )
    })?;
    fs::create_dir_all(parent).map_err(|_| {
        ProfilerFailure::new(
            "output_unavailable",
            "profile output directory could not be created",
        )
    })?;
    match fs::metadata(&output) {
        Ok(metadata) if !metadata.is_file() => {
            return Err(ProfilerFailure::new(
                "output_unavailable",
                "profile output path is not a file",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(ProfilerFailure::new(
                "output_unavailable",
                "profile output path could not be inspected",
            ));
        }
    }
    tempfile::NamedTempFile::new_in(parent).map_err(|_| {
        ProfilerFailure::new(
            "output_unavailable",
            "profile output directory is not writable",
        )
    })?;
    Ok(output)
}

fn current_process_capture_config(
    sample_hz: u16,
    max_operator_activity_spans: NonZeroU64,
) -> Result<String, ProfilerFailure> {
    let (template, read_period_ms, ring_buffer_pages, unwind_mode, base_semantic_buffer_size_kb) =
        match sample_hz {
            100 => (
                STREAMING_CAPTURE_CONFIG,
                100,
                256,
                "UNWIND_DWARF",
                65_536_u64,
            ),
            1000 => (
                SHORT_CAPTURE_CONFIG,
                10,
                512,
                "UNWIND_KERNEL_FRAME_POINTER",
                131_072_u64,
            ),
            _ => {
                return Err(ProfilerFailure::new(
                    "invalid_sample_frequency",
                    "profile sample frequency must be 100 or 1000 Hz",
                ));
            }
        };
    let additional_buffer_size_kb = max_operator_activity_spans
        .get()
        .saturating_sub(DEFAULT_MAX_OPERATOR_ACTIVITY_SPANS)
        .saturating_mul(BYTES_PER_ADDITIONAL_ACTIVITY_SPAN)
        .div_ceil(1024);
    let semantic_buffer_size_kb = base_semantic_buffer_size_kb
        .saturating_add(additional_buffer_size_kb)
        .min(MAX_SEMANTIC_BUFFER_SIZE_KB);
    [
        ("frequency: 100", format!("frequency: {sample_hz}")),
        (
            "ring_buffer_pages: 256",
            format!("ring_buffer_pages: {ring_buffer_pages}"),
        ),
        (
            "ring_buffer_read_period_ms: 100",
            format!("ring_buffer_read_period_ms: {read_period_ms}"),
        ),
        (
            "user_frames: UNWIND_DWARF",
            format!("user_frames: {unwind_mode}"),
        ),
        (
            "target_cmdline: \"delta-funnel-perfetto-preview\"",
            format!("target_pid: {}", std::process::id()),
        ),
        (
            if sample_hz == 100 {
                "size_kb: 65536  # semantic activity buffer"
            } else {
                "size_kb: 131072  # semantic activity buffer"
            },
            format!("size_kb: {semantic_buffer_size_kb}  # semantic activity buffer"),
        ),
    ]
    .into_iter()
    .try_fold(template.to_owned(), |config, (from, to)| {
        replace_one_config_value(config, from, &to)
    })
}

fn replace_one_config_value(
    config: String,
    from: &str,
    to: &str,
) -> Result<String, ProfilerFailure> {
    if config.matches(from).count() != 1 {
        return Err(ProfilerFailure::new(
            "invalid_capture_config",
            "packaged Perfetto capture config is invalid",
        ));
    }
    Ok(config.replacen(from, to, 1))
}

fn preflight_tracebox(program: &OsStr) -> Result<(), ProfilerFailure> {
    let status = Command::new(program)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| {
            let kind = if error.kind() == io::ErrorKind::NotFound {
                "tracebox_unavailable"
            } else {
                "tracebox_start_failed"
            };
            ProfilerFailure::new(kind, "Perfetto tracebox could not be started")
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(ProfilerFailure::new(
            "tracebox_start_failed",
            "Perfetto tracebox preflight failed",
        ))
    }
}

fn preflight_trace_processor() -> Result<(), ProfilerFailure> {
    let program =
        env::var_os("TRACE_PROCESSOR_SHELL").unwrap_or_else(|| "trace_processor_shell".into());
    let status = Command::new(program)
        .arg("--help")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| {
            let kind = if error.kind() == io::ErrorKind::NotFound {
                "trace_processor_unavailable"
            } else {
                "trace_processor_start_failed"
            };
            ProfilerFailure::new(kind, "Perfetto Trace Processor could not be started")
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(ProfilerFailure::new(
            "trace_processor_start_failed",
            "Perfetto Trace Processor preflight failed",
        ))
    }
}

fn preflight_traceconv() -> Result<(), ProfilerFailure> {
    let program = env::var_os("TRACECONV").unwrap_or_else(|| "traceconv".into());
    let output = Command::new(program)
        .arg("--help")
        .stdin(Stdio::null())
        .output()
        .map_err(|error| {
            let kind = if error.kind() == io::ErrorKind::NotFound {
                "traceconv_unavailable"
            } else {
                "traceconv_start_failed"
            };
            ProfilerFailure::new(kind, "Perfetto traceconv could not be started")
        })?;
    if [&output.stdout, &output.stderr].into_iter().any(|help| {
        help.windows(b" symbolize".len())
            .any(|line| line == b" symbolize")
    }) {
        Ok(())
    } else {
        Err(ProfilerFailure::new(
            "traceconv_start_failed",
            "Perfetto traceconv preflight failed",
        ))
    }
}

fn preflight_llvm_symbolizer() -> Result<(), ProfilerFailure> {
    preflight_llvm_symbolizer_with(OsStr::new("llvm-symbolizer"))
}

fn preflight_llvm_symbolizer_with(program: &OsStr) -> Result<(), ProfilerFailure> {
    Command::new(program)
        .arg("--output-style=JSON")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| {
            let kind = if error.kind() == io::ErrorKind::NotFound {
                "llvm_symbolizer_unavailable"
            } else {
                "llvm_symbolizer_start_failed"
            };
            ProfilerFailure::new(
                kind,
                format!("llvm-symbolizer could not be started: {error}"),
            )
        })
        .and_then(|status| {
            status.success().then_some(()).ok_or_else(|| {
                ProfilerFailure::new(
                    "llvm_symbolizer_start_failed",
                    "llvm-symbolizer does not support JSON output required by Perfetto",
                )
            })
        })
}

fn symbolize_trace(trace: &Path, directory: &Path) -> Result<PathBuf, ProfilerFailure> {
    let binary_directory = directory.join("symbol-binaries");
    fs::create_dir(&binary_directory).map_err(|_| {
        ProfilerFailure::new(
            "symbolization_failed",
            "temporary symbol directory could not be created",
        )
    })?;
    let maps = fs::read_to_string("/proc/self/maps").map_err(|_| {
        ProfilerFailure::new(
            "symbolization_failed",
            "process mappings could not be read for symbolization",
        )
    })?;
    let mut linked = 0_usize;
    for path in mapped_file_paths(&maps) {
        if !path.is_file() {
            continue;
        }
        let link = binary_directory.join(linked.to_string());
        if std::os::unix::fs::symlink(&path, link).is_ok() {
            linked += 1;
        }
    }
    if linked == 0 {
        return Err(ProfilerFailure::new(
            "symbolization_failed",
            "no mapped binaries were available for symbolization",
        ));
    }

    let symbol_search_path = symbol_search_path(&binary_directory)?;
    let symbols = directory.join("symbols.pftrace");
    let program = env::var_os("TRACECONV").unwrap_or_else(|| "traceconv".into());
    let status = Command::new(program)
        .args(["symbolize"])
        .arg(trace)
        .arg(&symbols)
        .env("PERFETTO_BINARY_PATH", symbol_search_path)
        .env("PERFETTO_SYMBOLIZER_MODE", "index")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|_| {
            ProfilerFailure::new(
                "symbolization_failed",
                "Perfetto trace symbolization could not be started",
            )
        })?;
    if !status.success() {
        return Err(ProfilerFailure::new(
            "symbolization_failed",
            "Perfetto trace symbolization failed",
        ));
    }

    let symbolized = directory.join("symbolized.pftrace");
    let mut output = fs::File::create(&symbolized).map_err(|_| {
        ProfilerFailure::new(
            "symbolization_failed",
            "symbolized trace could not be created",
        )
    })?;
    for input in [trace, symbols.as_path()] {
        let mut input = fs::File::open(input).map_err(|_| {
            ProfilerFailure::new(
                "symbolization_failed",
                "symbolized trace input could not be read",
            )
        })?;
        io::copy(&mut input, &mut output).map_err(|_| {
            ProfilerFailure::new(
                "symbolization_failed",
                "symbolized trace could not be written",
            )
        })?;
    }
    Ok(symbolized)
}

fn symbol_search_path(binary_directory: &Path) -> Result<OsString, ProfilerFailure> {
    let mut paths = vec![binary_directory.to_owned()];
    let system_debug = PathBuf::from("/usr/lib/debug");
    if system_debug.is_dir() {
        paths.push(system_debug);
    }
    let debuginfod_cache = debuginfod_cache_path(
        env::var_os("DEBUGINFOD_CACHE_PATH").map(PathBuf::from),
        env::var_os("XDG_CACHE_HOME").map(PathBuf::from),
        env::var_os("HOME").map(PathBuf::from),
    );
    if let Some(cache) = debuginfod_cache.filter(|path| path.is_dir()) {
        paths.push(cache);
    }
    env::join_paths(paths).map_err(|_| {
        ProfilerFailure::new(
            "symbolization_failed",
            "symbol search paths could not be configured",
        )
    })
}

fn debuginfod_cache_path(
    explicit: Option<PathBuf>,
    xdg_cache_home: Option<PathBuf>,
    home: Option<PathBuf>,
) -> Option<PathBuf> {
    explicit
        .or_else(|| {
            home.as_ref()
                .map(|path| path.join(".debuginfod_client_cache"))
                .filter(|path| path.is_dir())
        })
        .or_else(|| xdg_cache_home.map(|path| path.join("debuginfod_client")))
        .or_else(|| home.map(|path| path.join(".cache/debuginfod_client")))
}

fn mapped_file_paths(maps: &str) -> impl Iterator<Item = PathBuf> + '_ {
    maps.lines()
        .filter_map(|line| line.get(line.find('/')?..))
        .map(|path| path.strip_suffix(" (deleted)").unwrap_or(path))
        .map(PathBuf::from)
        .collect::<BTreeSet<_>>()
        .into_iter()
}

fn start_tracebox_with(
    python: &OsStr,
    tracebox: &OsStr,
    config: &Path,
    trace: &Path,
    authorize: impl FnOnce(u32) -> Result<(), ProfilerFailure>,
    initialize: impl FnOnce() -> io::Result<()>,
    wait_for_capture: impl FnOnce(Duration) -> io::Result<()>,
) -> Result<ManagedTracebox, ProfilerFailure> {
    // The Python process keeps this PID stable across exec while preventing
    // tracebox from inspecting the parent before Yama authorizes that exact PID.
    let child = Command::new(python)
        .args([
            "-I",
            "-S",
            "-c",
            TRACEBOX_START_GATE,
            "delta-funnel-tracebox-gate",
        ])
        .arg(tracebox)
        .args([
            "--txt",
            "--system-sockets",
            "--no-clobber",
            "--notify-fd",
            "1",
            "--config",
        ])
        .arg(config)
        .arg("--out")
        .arg(trace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| {
            ProfilerFailure::new(
                "tracebox_start_failed",
                "Perfetto tracebox could not be started",
            )
        })?;
    let mut tracebox = ManagedTracebox::new(child);
    authorize(tracebox.id()?)?;
    tracebox.release_start_gate()?;
    wait_for_tracebox_readiness(tracebox.child_mut()?)?;
    if initialize().is_err() {
        return Err(ProfilerFailure::new(
            "producer_initialization_failed",
            "Perfetto producer could not be initialized",
        ));
    }
    if wait_for_capture(CAPTURE_ENABLE_TIMEOUT).is_err() {
        return Err(ProfilerFailure::new(
            "capture_unavailable",
            "Perfetto capture did not enable Delta Funnel profiling",
        ));
    }
    Ok(tracebox)
}

fn wait_for_tracebox_readiness(child: &mut Child) -> Result<(), ProfilerFailure> {
    let mut stdout = child.stdout.take().ok_or_else(|| {
        ProfilerFailure::new(
            "tracebox_start_failed",
            "Perfetto tracebox readiness channel was not available",
        )
    })?;
    let (sender, receiver) = mpsc::sync_channel(1);
    let reader = thread::spawn(move || {
        let mut readiness = [0_u8];
        let result = stdout.read_exact(&mut readiness).map(|()| readiness[0]);
        let _ = sender.send(result);
    });
    let result = receiver.recv_timeout(TRACEBOX_READY_TIMEOUT);
    if result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = reader.join();
    match result {
        Ok(Ok(0)) => Ok(()),
        Ok(Ok(_)) | Ok(Err(_)) => Err(ProfilerFailure::new(
            "tracebox_start_failed",
            "Perfetto tracebox reported a startup failure",
        )),
        Err(_) => Err(ProfilerFailure::new(
            "tracebox_timeout",
            "Perfetto tracebox did not become ready within 15 seconds",
        )),
    }
}

fn stop_tracebox(child: Child) -> Result<(), ProfilerFailure> {
    stop_tracebox_with_timeout(child, TRACEBOX_STOP_TIMEOUT)
}

fn stop_tracebox_with_timeout(
    mut child: Child,
    stop_timeout: Duration,
) -> Result<(), ProfilerFailure> {
    let result = try_stop_tracebox(&mut child, stop_timeout);
    if result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    result
}

fn try_stop_tracebox(child: &mut Child, stop_timeout: Duration) -> Result<(), ProfilerFailure> {
    let initial_status = child.try_wait().map_err(|_| {
        ProfilerFailure::new(
            "capture_stop_failed",
            "Perfetto capture status could not be read",
        )
    })?;
    let status = match initial_status {
        Some(status) => status,
        None => {
            terminate_child(child)?;
            wait_for_tracebox_exit(child, stop_timeout)?
        }
    };
    if status.success() {
        Ok(())
    } else {
        Err(ProfilerFailure::new(
            "capture_failed",
            "Perfetto capture process failed",
        ))
    }
}

fn wait_for_tracebox_exit(
    child: &mut Child,
    stop_timeout: Duration,
) -> Result<std::process::ExitStatus, ProfilerFailure> {
    let deadline = Instant::now() + stop_timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(TRACEBOX_STOP_POLL_INTERVAL);
            }
            Ok(None) => {
                return Err(ProfilerFailure::new(
                    "capture_stop_timeout",
                    "Perfetto capture did not stop within 5 seconds",
                ));
            }
            Err(_) => {
                return Err(ProfilerFailure::new(
                    "capture_stop_failed",
                    "Perfetto capture status could not be read",
                ));
            }
        }
    }
}

fn terminate_child(child: &mut Child) -> Result<(), ProfilerFailure> {
    // SAFETY: Child::id returns the live child PID and kill does not retain the pointer.
    let result = unsafe { libc::kill(child.id().cast_signed(), libc::SIGTERM) };
    if result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(ProfilerFailure::new(
            "capture_stop_failed",
            "Perfetto capture process could not be stopped",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::{fs, os::unix::fs::PermissionsExt, sync::Mutex};

    #[cfg(unix)]
    static TRACEBOX_TEST_LOCK: Mutex<()> = Mutex::new(());
    #[cfg(unix)]
    const FAKE_TRACEBOX: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fake_tracebox.sh");
    #[cfg(unix)]
    const PYTHON: &str = "python3";

    #[test]
    fn current_process_config_scopes_samples_and_applies_both_supported_rates() {
        for (sample_hz, read_period_ms, ring_buffer_pages, unwind_mode) in [
            (100, 100, 256, "UNWIND_DWARF"),
            (1000, 10, 512, "UNWIND_KERNEL_FRAME_POINTER"),
        ] {
            let config = current_process_capture_config(
                sample_hz,
                NonZeroU64::new(DEFAULT_MAX_OPERATOR_ACTIVITY_SPANS)
                    .expect("the default activity span limit must be positive"),
            )
            .expect("the packaged config must support operation capture");
            assert!(config.contains(&format!("frequency: {sample_hz}")));
            assert!(config.contains(&format!("ring_buffer_read_period_ms: {read_period_ms}")));
            assert!(config.contains(&format!("ring_buffer_pages: {ring_buffer_pages}")));
            assert!(config.contains(&format!("user_frames: {unwind_mode}")));
            assert!(config.contains(&format!("target_pid: {}", std::process::id())));
            assert!(!config.contains("target_cmdline:"));
            assert_eq!(config.contains("write_into_file: true"), sample_hz == 100);
            assert!(!config.contains("target_cpu:"));
        }

        for (maximum_spans, expected_buffer_size_kb) in
            [(1, 65_536), (500_000, 265_536), (u64::MAX, 1_048_576)]
        {
            let config = current_process_capture_config(
                100,
                NonZeroU64::new(maximum_spans).expect("the activity span limit must be positive"),
            )
            .expect("the packaged streaming config must support the activity span limit");
            assert!(config.contains(&format!(
                "size_kb: {expected_buffer_size_kb}  # semantic activity buffer"
            )));
        }

        let first = ProfileReservation::acquire().expect("the first profile must reserve capture");
        let second = ProfileReservation::acquire()
            .expect_err("a concurrent profile must not mix operations");
        assert_eq!(second.kind, "profile_already_active");
        drop(first);
        drop(ProfileReservation::acquire().expect("the released reservation must be reusable"));
    }

    #[test]
    fn mapped_file_paths_are_unique_and_preserve_spaces() {
        let maps = "\
1000-2000 r-xp 00000000 00:00 0 /tmp/a library.so
2000-3000 r--p 00000000 00:00 0 /tmp/a library.so
3000-4000 r-xp 00000000 00:00 0 /tmp/deleted.so (deleted)
4000-5000 rw-p 00000000 00:00 0 [heap]
";

        assert_eq!(
            mapped_file_paths(maps).collect::<Vec<_>>(),
            vec![
                PathBuf::from("/tmp/a library.so"),
                PathBuf::from("/tmp/deleted.so"),
            ]
        );
    }

    #[test]
    fn debuginfod_cache_path_matches_elfutils_precedence() {
        let temporary = tempfile::tempdir().expect("temporary directory should be created");
        let home = temporary.path().join("home");
        let xdg_cache_home = temporary.path().join("xdg");
        let explicit = temporary.path().join("explicit");
        let legacy = home.join(".debuginfod_client_cache");
        fs::create_dir_all(&legacy).expect("legacy cache should be created");

        assert_eq!(
            debuginfod_cache_path(
                Some(explicit.clone()),
                Some(xdg_cache_home.clone()),
                Some(home.clone()),
            ),
            Some(explicit),
        );
        assert_eq!(
            debuginfod_cache_path(None, Some(xdg_cache_home.clone()), Some(home.clone())),
            Some(legacy.clone()),
        );

        fs::remove_dir(&legacy).expect("legacy cache should be removed");
        assert_eq!(
            debuginfod_cache_path(None, Some(xdg_cache_home.clone()), Some(home.clone())),
            Some(xdg_cache_home.join("debuginfod_client")),
        );
        assert_eq!(
            debuginfod_cache_path(None, None, Some(home.clone())),
            Some(home.join(".cache/debuginfod_client")),
        );
    }

    #[cfg(unix)]
    #[test]
    fn llvm_symbolizer_preflight_requires_json_output() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let missing = directory.path().join("missing-symbolizer");
        let error = preflight_llvm_symbolizer_with(missing.as_os_str())
            .expect_err("a missing executable must fail preflight");
        assert_eq!(error.kind, "llvm_symbolizer_unavailable");
        assert!(
            error
                .message
                .starts_with("llvm-symbolizer could not be started: ")
        );

        let version_only = directory.path().join("version-only-symbolizer");
        fs::write(&version_only, "#!/bin/sh\n[ \"${1:-}\" = --version ]\n")?;
        let mut permissions = fs::metadata(&version_only)?.permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&version_only, permissions.clone())?;

        assert!(
            Command::new(&version_only)
                .arg("--version")
                .status()?
                .success()
        );
        let error = preflight_llvm_symbolizer_with(version_only.as_os_str())
            .expect_err("version output alone must not satisfy Perfetto");
        assert_eq!(error.kind, "llvm_symbolizer_start_failed");

        let json_capable = directory.path().join("json-capable-symbolizer");
        fs::write(
            &json_capable,
            "#!/bin/sh\n[ \"${1:-}\" = --output-style=JSON ]\n",
        )?;
        fs::set_permissions(&json_capable, permissions)?;
        preflight_llvm_symbolizer_with(json_capable.as_os_str()).map_err(profiler_test_error)
    }

    #[cfg(unix)]
    #[test]
    fn tracebox_lifecycle_authorizes_only_the_managed_child() -> io::Result<()> {
        let _tracebox_test_guard = TRACEBOX_TEST_LOCK
            .lock()
            .map_err(|_| io::Error::other("tracebox test lock was poisoned"))?;
        let directory = tempfile::tempdir()?;
        let output = directory.path().join("reports/profile.html");
        let output = prepare_output_path(&output).map_err(profiler_test_error)?;
        let parent = output
            .parent()
            .expect("an absolute output file path must have a parent");
        assert!(parent.is_dir());

        let config = directory.path().join("capture.pbtx");
        fs::write(&config, "test config")?;
        let trace = directory.path().join("capture.pftrace");

        preflight_tracebox(OsStr::new(FAKE_TRACEBOX)).map_err(profiler_test_error)?;
        let mut gate_observed = false;
        let mut tracebox = start_tracebox_with(
            OsStr::new(PYTHON),
            OsStr::new(FAKE_TRACEBOX),
            &config,
            &trace,
            |pid| {
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    let command_line = fs::read(format!("/proc/{pid}/cmdline"))
                        .expect("the gated child command line must be readable");
                    if command_line
                        .split(|byte| *byte == 0)
                        .any(|argument| argument == b"delta-funnel-tracebox-gate")
                    {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "tracebox must remain behind the start gate until authorization"
                    );
                    thread::sleep(Duration::from_millis(1));
                }
                gate_observed = true;
                authorize_tracebox(pid).map_err(profiler_failure_from_ptrace)
            },
            || Ok(()),
            |_| Ok(()),
        )
        .map_err(profiler_test_error)?;
        assert!(gate_observed);
        if ptrace_scope_is_relational()
            .map_err(|error| profiler_test_error(profiler_failure_from_ptrace(error)))?
        {
            assert!(
                !Command::new("sh")
                    .args(["-c", ": <\"/proc/$PPID/mem\""])
                    .stderr(Stdio::null())
                    .status()?
                    .success(),
                "the Yama exception must not authorize an unrelated sibling"
            );
        }
        tracebox.stop().map_err(profiler_test_error)?;
        assert!(!tracebox.has_child());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn managed_tracebox_is_reaped_when_initialization_fails() -> io::Result<()> {
        let _tracebox_test_guard = TRACEBOX_TEST_LOCK
            .lock()
            .map_err(|_| io::Error::other("tracebox test lock was poisoned"))?;
        let directory = tempfile::tempdir()?;
        let config = directory.path().join("capture.pbtx");
        fs::write(&config, "test config")?;
        let trace = directory.path().join("capture.pftrace");

        let error = start_tracebox_with(
            OsStr::new(PYTHON),
            OsStr::new(FAKE_TRACEBOX),
            &config,
            &trace,
            |pid| authorize_tracebox(pid).map_err(profiler_failure_from_ptrace),
            || Err(io::Error::other("injected initialization failure")),
            |_| Ok(()),
        )
        .expect_err("producer initialization must fail");
        assert_eq!(
            error.kind, "producer_initialization_failed",
            "{}",
            error.message
        );

        let pid = fs::read_to_string(trace.with_extension("pid"))?
            .trim()
            .parse::<i32>()
            .map_err(io::Error::other)?;
        // SAFETY: signal 0 only checks whether the captured numeric PID still exists.
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn blocked_tracebox_is_reaped_when_authorization_fails() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let config = directory.path().join("capture.pbtx");
        fs::write(&config, "test config")?;
        let trace = directory.path().join("capture.pftrace");
        let mut child_pid = None;
        let mut initialize_called = false;
        let mut capture_wait_called = false;

        let error = start_tracebox_with(
            OsStr::new(PYTHON),
            OsStr::new(FAKE_TRACEBOX),
            &config,
            &trace,
            |pid| {
                child_pid = Some(pid);
                Err(ProfilerFailure::new(
                    "ptrace_permission_failed",
                    "injected authorization failure",
                ))
            },
            || {
                initialize_called = true;
                Ok(())
            },
            |_| {
                capture_wait_called = true;
                Ok(())
            },
        )
        .expect_err("tracebox authorization must fail");
        assert_eq!(error.kind, "ptrace_permission_failed");
        assert!(!initialize_called);
        assert!(!capture_wait_called);
        assert!(!trace.with_extension("pid").exists());

        let pid = child_pid.expect("the gated child PID must be captured");
        // SAFETY: signal 0 only checks whether the captured numeric PID still exists.
        assert_eq!(unsafe { libc::kill(pid.cast_signed(), 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn tracebox_is_reaped_when_graceful_shutdown_fails() -> io::Result<()> {
        let mut child = Command::new("sh")
            .args(["-c", "trap '' TERM; printf '\\000'; while :; do :; done"])
            .stdout(Stdio::piped())
            .spawn()?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("the test child must expose stdout"))?;
        let mut ready = [0_u8];
        stdout.read_exact(&mut ready)?;
        let pid = child.id();

        let error = stop_tracebox_with_timeout(child, Duration::ZERO)
            .expect_err("an unresponsive tracebox must time out");

        assert_eq!(error.kind, "capture_stop_timeout");
        // SAFETY: signal 0 only checks whether the captured numeric PID still exists.
        assert_eq!(unsafe { libc::kill(pid.cast_signed(), 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
        Ok(())
    }

    #[test]
    fn prepares_relative_output_as_an_absolute_path() -> io::Result<()> {
        let directory = tempfile::tempdir_in(".")?;
        let directory_name = directory
            .path()
            .file_name()
            .ok_or_else(|| io::Error::other("the temporary directory must have a name"))?;
        let relative_output = Path::new(directory_name).join("reports/profile.html");

        let output = prepare_output_path(&relative_output).map_err(profiler_test_error)?;

        assert!(output.is_absolute());
        assert!(output.ends_with(&relative_output));
        Ok(())
    }

    fn profiler_test_error(error: ProfilerFailure) -> io::Error {
        io::Error::other(error.message)
    }
}
