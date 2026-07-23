use std::ffi::OsStr;
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;

use super::report_cli::{RankedReportFailure, RankedReportFailurePhase};

// ponytail: This is above the largest validated compact aggregate (95.3 MiB).
// Raise it only with a larger production aggregate and memory evidence.
const MAX_QUERY_OUTPUT_BYTES: usize = 256 * 1024 * 1024;

pub(super) fn run_trace_processor_query(
    input: &Path,
    sql: &[u8],
) -> Result<Vec<u8>, RankedReportFailure> {
    let program =
        std::env::var_os("TRACE_PROCESSOR_SHELL").unwrap_or_else(|| "trace_processor_shell".into());
    run_trace_processor_query_with(&program, input, sql, MAX_QUERY_OUTPUT_BYTES)
}

fn run_trace_processor_query_with(
    program: &OsStr,
    input: &Path,
    sql: &[u8],
    output_limit: usize,
) -> Result<Vec<u8>, RankedReportFailure> {
    let mut child = Command::new(program)
        .args([
            OsStr::new("query"),
            OsStr::new("--query-file"),
            OsStr::new("-"),
        ])
        .arg(input)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| {
            let kind = if error.kind() == io::ErrorKind::NotFound {
                "unavailable"
            } else {
                "start_failed"
            };
            RankedReportFailure::new(
                RankedReportFailurePhase::TraceProcessor,
                kind,
                "trace processor could not be started",
            )
        })?;

    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(RankedReportFailure::new(
            RankedReportFailurePhase::TraceProcessor,
            "start_failed",
            "trace processor stdout was not available",
        ));
    };
    let stdout_reader = thread::spawn(move || read_bounded(stdout, output_limit));

    let write_result = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("trace processor stdin was not available"))
        .and_then(|mut stdin| stdin.write_all(sql));
    if write_result.is_err() {
        let _ = child.kill();
    }
    let status_result = child.wait();
    let stdout_result = join_reader(stdout_reader);

    write_result.map_err(|_| {
        RankedReportFailure::new(
            RankedReportFailurePhase::TraceProcessor,
            "input_failed",
            "trace processor query input failed",
        )
    })?;
    let status = status_result.map_err(|_| {
        RankedReportFailure::new(
            RankedReportFailurePhase::TraceProcessor,
            "wait_failed",
            "trace processor status could not be read",
        )
    })?;
    let stdout = stdout_result?;
    if !status.success() {
        return Err(RankedReportFailure::new(
            RankedReportFailurePhase::TraceProcessor,
            "execution_failed",
            format!("trace processor exited with status {status}"),
        ));
    }
    if stdout.exceeded_limit {
        return Err(RankedReportFailure::new(
            RankedReportFailurePhase::Query,
            "result_too_large",
            "trace processor query output exceeded the report limit",
        ));
    }
    Ok(stdout.bytes)
}

struct BoundedBytes {
    bytes: Vec<u8>,
    exceeded_limit: bool,
}

fn read_bounded(mut reader: impl Read, limit: usize) -> io::Result<BoundedBytes> {
    let mut bytes = Vec::new();
    let mut exceeded_limit = false;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let retained = count.min(limit.saturating_sub(bytes.len()));
        bytes.extend_from_slice(&buffer[..retained]);
        exceeded_limit |= retained != count;
    }
    Ok(BoundedBytes {
        bytes,
        exceeded_limit,
    })
}

fn join_reader(
    reader: thread::JoinHandle<io::Result<BoundedBytes>>,
) -> Result<BoundedBytes, RankedReportFailure> {
    reader
        .join()
        .map_err(|_| {
            RankedReportFailure::new(
                RankedReportFailurePhase::TraceProcessor,
                "output_failed",
                "trace processor output reader failed",
            )
        })?
        .map_err(|_| {
            RankedReportFailure::new(
                RankedReportFailurePhase::TraceProcessor,
                "output_failed",
                "trace processor output could not be read",
            )
        })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn bounds_output_and_maps_process_failures_without_exposing_stderr() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("capture.pftrace");
        fs::write(&input, b"trace")?;

        let success = script(
            directory.path(),
            "success",
            "cat >/dev/null\nprintf 'result'",
        )?;
        assert_eq!(
            run_trace_processor_query_with(success.as_os_str(), &input, b"SELECT 1;", 16)
                .map_err(|error| io::Error::other(error.to_string()))?,
            b"result"
        );

        let oversized = script(
            directory.path(),
            "oversized",
            "cat >/dev/null\ndd if=/dev/zero bs=65536 count=2 2>/dev/null",
        )?;
        let error = run_trace_processor_query_with(oversized.as_os_str(), &input, b"SELECT 1;", 4)
            .expect_err("oversized output should fail");
        assert_eq!(error.phase(), RankedReportFailurePhase::Query);
        assert_eq!(error.kind(), "result_too_large");

        let failed = script(
            directory.path(),
            "failed",
            "cat >/dev/null\nprintf 'private stderr' >&2\nexit 7",
        )?;
        let error = run_trace_processor_query_with(failed.as_os_str(), &input, b"SELECT 1;", 16)
            .expect_err("nonzero exit should fail");
        assert_eq!(error.phase(), RankedReportFailurePhase::TraceProcessor);
        assert_eq!(error.kind(), "execution_failed");
        assert!(!error.machine_line().contains("private stderr"));
        Ok(())
    }

    fn script(directory: &Path, name: &str, body: &str) -> io::Result<std::path::PathBuf> {
        let path = directory.join(name);
        fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}\n"))?;
        let mut permissions = fs::metadata(&path)?.permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&path, permissions)?;
        Ok(path)
    }
}
