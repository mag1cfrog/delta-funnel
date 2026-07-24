//! Current-process Perfetto capture and ranked report generation.

use std::{
    env,
    ffi::OsStr,
    fs, io,
    io::Read,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

const TRACEBOX_READY_TIMEOUT: Duration = Duration::from_secs(15);
const CAPTURE_ENABLE_TIMEOUT: Duration = Duration::from_secs(10);
const TRACEBOX_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const TRACEBOX_STOP_POLL_INTERVAL: Duration = Duration::from_millis(10);
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
    trace: PathBuf,
    _directory: tempfile::TempDir,
    child: Option<Child>,
    reservation: Option<ProfileReservation>,
    scope: delta_funnel::perfetto_profile::OperationCaptureScope,
}

impl OperationCapture {
    pub(super) fn start(output: PathBuf, sample_hz: u16) -> Result<Self, ProfilerFailure> {
        let reservation = ProfileReservation::acquire()?;
        let scope =
            delta_funnel::perfetto_profile::OperationCaptureScope::allocate().ok_or_else(|| {
                ProfilerFailure::new(
                    "capture_scope_unavailable",
                    "operation profile scope identity could not be allocated",
                )
            })?;
        let output = prepare_output_path(&output)?;
        let parent = output.parent().ok_or_else(|| {
            ProfilerFailure::new(
                "output_unavailable",
                "profile output path has no parent directory",
            )
        })?;
        preflight_trace_processor()?;
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
        let config = current_process_capture_config(sample_hz)?;
        fs::write(&config_path, config).map_err(|_| {
            ProfilerFailure::new(
                "capture_config_failed",
                "temporary Perfetto capture config could not be written",
            )
        })?;
        let tracebox = env::var_os("TRACEBOX").unwrap_or_else(|| "tracebox".into());
        let child = start_tracebox_with(
            &tracebox,
            &config_path,
            &trace,
            delta_funnel::perfetto_profile::initialize_perfetto,
            delta_funnel::perfetto_profile::wait_for_capture,
        )?;
        crate::perfetto_diagnostics::refresh_perfetto_capture_filter();
        Ok(Self {
            output,
            trace,
            _directory: directory,
            child: Some(child),
            reservation: Some(reservation),
            scope,
        })
    }

    pub(super) fn in_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        self.scope.in_scope(operation)
    }

    pub(super) fn finish(mut self) -> Result<(), ProfilerFailure> {
        let child = self.child.take().ok_or_else(|| {
            ProfilerFailure::new(
                "capture_stop_failed",
                "Perfetto capture process was not available",
            )
        })?;
        let stop_result = stop_tracebox(child);
        crate::perfetto_diagnostics::refresh_perfetto_capture_filter();
        drop(self.reservation.take());
        stop_result?;
        delta_funnel::perfetto_profile::generate_operation_ranked_profile_report(
            &self.trace,
            &self.output,
            &self.scope,
        )
        .map(|_| ())
        .map_err(|error| ProfilerFailure::new(error.kind(), error.to_string()))
    }
}

impl Drop for OperationCapture {
    fn drop(&mut self) {
        if let Some(child) = self.child.take() {
            let _ = stop_tracebox(child);
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

fn current_process_capture_config(sample_hz: u16) -> Result<String, ProfilerFailure> {
    let (template, read_period_ms, ring_buffer_pages) = match sample_hz {
        100 => (STREAMING_CAPTURE_CONFIG, 100, 256),
        1000 => (SHORT_CAPTURE_CONFIG, 10, 512),
        _ => {
            return Err(ProfilerFailure::new(
                "invalid_sample_frequency",
                "profile sample frequency must be 100 or 1000 Hz",
            ));
        }
    };
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
            "target_cmdline: \"delta-funnel-perfetto-preview\"",
            format!("target_pid: {}", std::process::id()),
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

fn start_tracebox_with(
    tracebox: &OsStr,
    config: &Path,
    trace: &Path,
    initialize: impl FnOnce() -> io::Result<()>,
    wait_for_capture: impl FnOnce(Duration) -> io::Result<()>,
) -> Result<Child, ProfilerFailure> {
    let mut child = Command::new(tracebox)
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
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| {
            let kind = if error.kind() == io::ErrorKind::NotFound {
                "tracebox_unavailable"
            } else {
                "tracebox_start_failed"
            };
            ProfilerFailure::new(kind, "Perfetto tracebox could not be started")
        })?;
    let readiness = wait_for_tracebox_readiness(&mut child);
    if let Err(error) = readiness {
        let _ = stop_tracebox(child);
        return Err(error);
    }
    if initialize().is_err() {
        let _ = stop_tracebox(child);
        return Err(ProfilerFailure::new(
            "producer_initialization_failed",
            "Perfetto producer could not be initialized",
        ));
    }
    if wait_for_capture(CAPTURE_ENABLE_TIMEOUT).is_err() {
        let _ = stop_tracebox(child);
        return Err(ProfilerFailure::new(
            "capture_unavailable",
            "Perfetto capture did not enable Delta Funnel profiling",
        ));
    }
    Ok(child)
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

fn stop_tracebox(mut child: Child) -> Result<(), ProfilerFailure> {
    let initial_status = child.try_wait().map_err(|_| {
        ProfilerFailure::new(
            "capture_stop_failed",
            "Perfetto capture status could not be read",
        )
    })?;
    let status = match initial_status {
        Some(status) => status,
        None => {
            terminate_child(&mut child)?;
            wait_for_tracebox_exit(&mut child)?
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

fn wait_for_tracebox_exit(child: &mut Child) -> Result<std::process::ExitStatus, ProfilerFailure> {
    let deadline = Instant::now() + TRACEBOX_STOP_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(TRACEBOX_STOP_POLL_INTERVAL);
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
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
    use std::{fs, os::unix::fs::PermissionsExt};

    #[test]
    fn current_process_config_scopes_samples_and_applies_both_supported_rates() {
        for (sample_hz, read_period_ms, ring_buffer_pages) in [(100, 100, 256), (1000, 10, 512)] {
            let config = current_process_capture_config(sample_hz)
                .expect("the packaged config must support operation capture");
            assert!(config.contains(&format!("frequency: {sample_hz}")));
            assert!(config.contains(&format!("ring_buffer_read_period_ms: {read_period_ms}")));
            assert!(config.contains(&format!("ring_buffer_pages: {ring_buffer_pages}")));
            assert!(config.contains(&format!("target_pid: {}", std::process::id())));
            assert!(!config.contains("target_cmdline:"));
            assert_eq!(config.contains("write_into_file: true"), sample_hz == 100);
        }

        let first = ProfileReservation::acquire().expect("the first profile must reserve capture");
        let second = ProfileReservation::acquire()
            .expect_err("a concurrent profile must not mix operations");
        assert_eq!(second.kind, "profile_already_active");
        drop(first);
        drop(ProfileReservation::acquire().expect("the released reservation must be reusable"));
    }

    #[cfg(unix)]
    #[test]
    fn tracebox_readiness_and_shutdown_use_one_managed_child() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let output = directory.path().join("reports/profile.html");
        let output = prepare_output_path(&output).map_err(profiler_test_error)?;
        let parent = output
            .parent()
            .expect("an absolute output file path must have a parent");
        assert!(parent.is_dir());

        let script = directory.path().join("tracebox");
        fs::write(
            &script,
            "#!/bin/sh\ntrap 'exit 0' TERM\nprintf '\\000'\nwhile :; do :; done\n",
        )?;
        let mut permissions = fs::metadata(&script)?.permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&script, permissions)?;
        let config = directory.path().join("capture.pbtx");
        fs::write(&config, "test config")?;
        let trace = directory.path().join("capture.pftrace");

        let child = start_tracebox_with(script.as_os_str(), &config, &trace, || Ok(()), |_| Ok(()))
            .map_err(profiler_test_error)?;
        stop_tracebox(child).map_err(profiler_test_error)
    }

    #[test]
    fn output_path_is_frozen_before_the_profiled_operation_can_change_directory() -> io::Result<()>
    {
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
