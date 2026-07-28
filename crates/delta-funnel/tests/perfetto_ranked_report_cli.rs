//! End-to-end coverage for the opt-in ranked report CLI.

#![cfg(all(feature = "perfetto-profile", unix))]

#[cfg(target_os = "linux")]
use std::ffi::CString;
use std::fs;
#[cfg(target_os = "linux")]
use std::fs::File;
use std::io;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use delta_funnel::perfetto_profile::{
    OperationCaptureScope, generate_operation_ranked_profile_outputs,
};

const OPERATION_OUTPUTS_CHILD: &str = "DELTA_FUNNEL_TEST_OPERATION_OUTPUTS_CHILD";

#[test]
fn generates_a_ranked_report_and_artifact_with_one_healthy_trace_query()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("capture.pftrace");
    let output = directory.path().join("capture.profile.html");
    let artifact = directory.path().join("capture.dfprofile");
    let aggregate = directory.path().join("aggregate.csv");
    let trace_processor = directory.path().join("trace_processor_shell");
    let input_bytes = b"\x0a\x00";
    fs::write(&input, input_bytes)?;
    fs::write(&aggregate, aggregate_output())?;
    write_executable(
        &trace_processor,
        "#!/bin/sh\n\
         set -eu\n\
         query=$(cat)\n\
         case \"$query\" in\n\
           *delta_funnel_capture_health_input*'CREATE PERFETTO TABLE delta_funnel_report_selection AS'*'CREATE PERFETTO TABLE delta_funnel_capture_health AS'*record_kind*) ;;\n\
           *) exit 65 ;;\n\
         esac\n\
         cat \"$DELTA_FUNNEL_TEST_AGGREGATE\"\n",
    )?;

    let result = Command::new(env!("CARGO_BIN_EXE_delta-funnel-perfetto"))
        .arg("report")
        .arg(&input)
        .arg("--output")
        .arg(&output)
        .arg("--artifact-output")
        .arg(&artifact)
        .env("TRACE_PROCESSOR_SHELL", &trace_processor)
        .env("DELTA_FUNNEL_TEST_AGGREGATE", &aggregate)
        .output()?;
    assert!(
        result.status.success(),
        "report failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(fs::read(&input)?, input_bytes);
    let html = fs::read_to_string(output)?;
    assert!(html.starts_with("<!doctype html>"));
    assert!(html.contains("Delta Funnel preview"));
    assert!(html.contains("Function metrics are sampled on-CPU observations"));
    assert!(!html.contains("http://"));
    assert!(!html.contains("https://"));
    assert!(artifact.is_file());
    let inspect = Command::new(env!("CARGO_BIN_EXE_delta-funnel-perfetto"))
        .arg("inspect")
        .arg(&artifact)
        .env(
            "TRACE_PROCESSOR_SHELL",
            directory.path().join("unavailable"),
        )
        .output()?;
    assert!(
        inspect.status.success(),
        "artifact inspection failed: {}",
        String::from_utf8_lossy(&inspect.stderr)
    );
    assert!(String::from_utf8(inspect.stdout)?.contains("Delta Funnel preview"));

    #[cfg(target_os = "linux")]
    {
        let failed_output = directory.path().join("stdout-failure.profile.html");
        let result = Command::new(env!("CARGO_BIN_EXE_delta-funnel-perfetto"))
            .arg("report")
            .arg(&input)
            .arg("--output")
            .arg(&failed_output)
            .env("TRACE_PROCESSOR_SHELL", &trace_processor)
            .env("DELTA_FUNNEL_TEST_AGGREGATE", &aggregate)
            .stdout(Stdio::from(File::options().write(true).open("/dev/full")?))
            .output()?;
        assert_eq!(result.status.code(), Some(73));
        let failure: serde_json::Value = serde_json::from_slice(&result.stderr)?;
        assert_eq!(failure["phase"], "output");
        assert_eq!(failure["kind"], "terminal_write_failed");
        assert!(failed_output.is_file());
    }
    Ok(())
}

#[test]
fn generates_operation_html_and_artifact_from_one_aggregate()
-> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os(OPERATION_OUTPUTS_CHILD).is_some() {
        let input = required_path("DELTA_FUNNEL_TEST_OPERATION_INPUT")?;
        let output = required_path("DELTA_FUNNEL_TEST_OPERATION_HTML")?;
        let artifact = required_path("DELTA_FUNNEL_TEST_OPERATION_ARTIFACT")?;
        let scope = OperationCaptureScope::allocate().ok_or("operation scope unavailable")?;
        generate_operation_ranked_profile_outputs(&input, &output, Some(&artifact), &scope)?;
        return Ok(());
    }

    let directory = tempfile::tempdir()?;
    let input = directory.path().join("operation.pftrace");
    let output = directory.path().join("operation.profile.html");
    let artifact = directory.path().join("operation.dfprofile");
    let rerendered = directory.path().join("rerendered.profile.html");
    let aggregate = directory.path().join("aggregate.csv");
    let trace_processor = directory.path().join("trace_processor_shell");
    let input_bytes = b"\x0a\x00";
    fs::write(&input, input_bytes)?;
    fs::write(&aggregate, aggregate_output())?;
    write_executable(
        &trace_processor,
        "#!/bin/sh\n\
         set -eu\n\
         query=$(cat)\n\
         case \"$query\" in\n\
           *delta_funnel_capture_health_input*'CREATE PERFETTO TABLE delta_funnel_report_selection AS'*'CREATE PERFETTO TABLE delta_funnel_capture_health AS'*record_kind*) ;;\n\
           *) exit 65 ;;\n\
         esac\n\
         cat \"$DELTA_FUNNEL_TEST_AGGREGATE\"\n",
    )?;

    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "generates_operation_html_and_artifact_from_one_aggregate",
        ])
        .env(OPERATION_OUTPUTS_CHILD, "1")
        .env("DELTA_FUNNEL_TEST_OPERATION_INPUT", &input)
        .env("DELTA_FUNNEL_TEST_OPERATION_HTML", &output)
        .env("DELTA_FUNNEL_TEST_OPERATION_ARTIFACT", &artifact)
        .env("TRACE_PROCESSOR_SHELL", &trace_processor)
        .env("DELTA_FUNNEL_TEST_AGGREGATE", &aggregate);
    let child = output_with_timeout(command, Duration::from_secs(10))?;
    assert!(
        child.status.success(),
        "operation output child failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    assert_eq!(fs::read(&input)?, input_bytes);
    assert!(output.is_file());
    assert!(artifact.is_file());

    let mut command = Command::new(env!("CARGO_BIN_EXE_delta-funnel-perfetto"));
    command
        .arg("report")
        .arg(&artifact)
        .arg("--output")
        .arg(&rerendered)
        .env(
            "TRACE_PROCESSOR_SHELL",
            directory.path().join("unavailable"),
        );
    let report = output_with_timeout(command, Duration::from_secs(10))?;
    assert!(
        report.status.success(),
        "artifact rerender failed: {}",
        String::from_utf8_lossy(&report.stderr)
    );
    assert_eq!(fs::read(output)?, fs::read(rerendered)?);
    Ok(())
}

#[test]
fn reads_artifacts_without_trace_processor() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let input = directory
        .path()
        .join("artifact-with-trace-extension.pftrace");
    let output = directory.path().join("artifact.profile.html");
    fs::write(
        &input,
        include_bytes!("../src/perfetto_profile/testdata/ranked_profile_v1.dfprofile"),
    )?;
    let missing_trace_processor = directory.path().join("does-not-exist");

    let report = Command::new(env!("CARGO_BIN_EXE_delta-funnel-perfetto"))
        .arg("report")
        .arg(&input)
        .arg("--output")
        .arg(&output)
        .env("TRACE_PROCESSOR_SHELL", &missing_trace_processor)
        .output()?;
    assert!(
        report.status.success(),
        "artifact report failed: {}",
        String::from_utf8_lossy(&report.stderr)
    );
    let html = fs::read_to_string(output)?;
    assert!(html.contains("Delta Funnel preview"));

    let inspect = Command::new(env!("CARGO_BIN_EXE_delta-funnel-perfetto"))
        .arg("inspect")
        .arg(&input)
        .env("TRACE_PROCESSOR_SHELL", missing_trace_processor)
        .output()?;
    assert!(
        inspect.status.success(),
        "artifact inspection failed: {}",
        String::from_utf8_lossy(&inspect.stderr)
    );
    assert!(String::from_utf8(inspect.stdout)?.contains("Delta Funnel preview"));

    Ok(())
}

#[test]
fn rejects_malicious_artifacts_without_trace_processor() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let fixture =
        include_bytes!("../src/perfetto_profile/testdata/ranked_profile_v1.dfprofile").to_vec();
    let mut cases = Vec::new();

    let mut invalid_magic = fixture.clone();
    invalid_magic[0] ^= 1;
    cases.push(("invalid-magic", invalid_magic, "invalid_artifact_magic"));

    cases.push((
        "truncated-header",
        fixture[..32].to_vec(),
        "truncated_artifact",
    ));

    let mut unsupported_version = fixture.clone();
    unsupported_version[8..10].copy_from_slice(&2_u16.to_le_bytes());
    cases.push((
        "unsupported-version",
        unsupported_version,
        "unsupported_artifact_version",
    ));

    let mut trailing = fixture.clone();
    trailing.push(0);
    cases.push(("trailing-byte", trailing, "trailing_artifact_bytes"));

    let mut invalid_archive = fixture;
    invalid_archive.pop();
    let payload_length = u64::try_from(invalid_archive.len() - 64)?;
    invalid_archive[32..40].copy_from_slice(&payload_length.to_le_bytes());
    cases.push(("invalid-archive", invalid_archive, "invalid_archive"));

    let mut invalid_profile =
        include_bytes!("../src/perfetto_profile/testdata/ranked_profile_v1.dfprofile").to_vec();
    let semantic_kind = invalid_profile
        .windows(b"operation".len())
        .position(|window| window == b"operation")
        .ok_or("fixture does not contain its semantic kind")?;
    invalid_profile[semantic_kind + b"operation".len() - 1] = b'x';
    cases.push(("invalid-profile", invalid_profile, "invalid_ranked_profile"));

    let marker = directory.path().join("trace-processor-started");
    let trace_processor = directory.path().join("trace_processor_shell");
    write_executable(
        &trace_processor,
        "#!/bin/sh\n\
         set -eu\n\
         : >\"$DELTA_FUNNEL_TRACE_PROCESSOR_MARKER\"\n\
         exit 99\n",
    )?;

    for (name, bytes, expected_kind) in cases {
        let input = directory.path().join(format!("{name}.dfprofile"));
        fs::write(&input, bytes)?;
        for command in ["inspect", "report"] {
            let report = directory.path().join(format!("{name}.profile.html"));
            let mut process = Command::new(env!("CARGO_BIN_EXE_delta-funnel-perfetto"));
            process.arg(command).arg(&input);
            if command == "report" {
                process.arg("--output").arg(&report);
            }
            let result = process
                .env("TRACE_PROCESSOR_SHELL", &trace_processor)
                .env("DELTA_FUNNEL_TRACE_PROCESSOR_MARKER", &marker)
                .output()?;
            assert_eq!(
                result.status.code(),
                Some(66),
                "{command} accepted malicious case {name}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            let failure: serde_json::Value = serde_json::from_slice(&result.stderr)?;
            assert_eq!(failure["phase"], "input", "{command} case {name}");
            assert_eq!(failure["kind"], expected_kind, "{command} case {name}");
            assert!(!report.exists(), "{command} case {name} created output");
            assert!(
                !marker.exists(),
                "{command} case {name} started Trace Processor"
            );
        }
    }
    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn rejects_named_pipe_inputs_without_hanging() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("blocking.dfprofile");
    let input_c = CString::new(input.as_os_str().as_bytes())?;
    if unsafe { libc::mkfifo(input_c.as_ptr(), 0o600) } != 0 {
        return Err(io::Error::last_os_error().into());
    }

    let marker = directory.path().join("trace-processor-started");
    let trace_processor = directory.path().join("trace_processor_shell");
    write_executable(
        &trace_processor,
        "#!/bin/sh\n\
         set -eu\n\
         : >\"$DELTA_FUNNEL_TRACE_PROCESSOR_MARKER\"\n\
         exit 99\n",
    )?;

    for command_name in ["inspect", "report"] {
        let report = directory
            .path()
            .join(format!("{command_name}.profile.html"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_delta-funnel-perfetto"));
        command.arg(command_name).arg(&input);
        if command_name == "report" {
            command.arg("--output").arg(&report);
        }
        command
            .env("TRACE_PROCESSOR_SHELL", &trace_processor)
            .env("DELTA_FUNNEL_TRACE_PROCESSOR_MARKER", &marker);
        let result = output_with_timeout(command, Duration::from_secs(2))?;
        assert_eq!(result.status.code(), Some(66));
        let failure: serde_json::Value = serde_json::from_slice(&result.stderr)?;
        assert_eq!(failure["phase"], "input");
        assert_eq!(failure["kind"], "not_file");
        assert!(!report.exists());
        assert!(!marker.exists());
    }
    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn reports_help_output_failures_with_stable_diagnostics() -> Result<(), Box<dyn std::error::Error>>
{
    let result = Command::new(env!("CARGO_BIN_EXE_delta-funnel-perfetto"))
        .arg("--help")
        .stdout(Stdio::from(File::options().write(true).open("/dev/full")?))
        .output()?;
    assert_eq!(result.status.code(), Some(73));
    let failure: serde_json::Value = serde_json::from_slice(&result.stderr)?;
    assert_eq!(failure["phase"], "output");
    assert_eq!(failure["kind"], "terminal_write_failed");
    Ok(())
}

fn output_with_timeout(
    mut command: Command,
    timeout: Duration,
) -> Result<Output, Box<dyn std::error::Error>> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let deadline = Instant::now() + timeout;
    while child.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(io::ErrorKind::TimedOut, "command did not exit").into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(child.wait_with_output()?)
}

fn write_executable(path: &Path, contents: &str) -> io::Result<()> {
    fs::write(path, contents)?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions)
}

fn required_path(name: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{name} is not set").into())
}

fn aggregate_output() -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    let records = [
        serde_json::json!({
            "record_kind": "metadata",
            "record": {
                "capture_complete": true,
                "semantic_complete": true,
                "finalization_observed": true,
                "incomplete_operation_root_count": 0,
                "truncation_marker_count": 0,
                "missing_identity_field_count": 0,
                "missing_terminal_result_count": 0,
                "crossing_worker_slice_count": 0,
                "crossing_planning_activity_slice_count": 0,
                "crossing_execution_activity_slice_count": 0,
                "invalid_planning_activity_hierarchy_count": 0,
                "invalid_execution_activity_hierarchy_count": 0,
                "perf_sample_without_callsite_count": 0,
                "perf_samples_skipped": 0,
                "buffer_loss_count": 0,
                "data_source_loss_count": 0,
                "flush_failure_count": 0,
                "schema_version": 3,
                "sample_frequency_hz": 1000,
                "sampled_cpu_count": 1,
                "exact_time_unit": "nanoseconds",
                "sample_unit": "samples",
                "eligible_sample_count": 1,
                "direct_sample_count": 1,
                "ambiguous_sample_count": 0,
                "unattributed_sample_count": 0,
                "resolved_function_sample_count": 0,
                "unresolved_function_sample_count": 0,
                "unwind_error_sample_count": 0,
                "missing_callstack_sample_count": 1,
                "trace_profiler_dropped_sample_count": 0,
                "audit_error_count": 0,
            },
        }),
        serde_json::json!({
            "record_kind": "semantic",
            "record": {
                "semantic_id": 1,
                "parent_semantic_id": null,
                "operation_id": 1,
                "name": "Delta Funnel preview",
                "semantic_kind": "operation",
                "operation_kind": "preview",
                "stage_category": null,
                "stage_name": null,
                "activity": null,
                "start_ns": 10,
                "end_ns": 20,
                "duration_ns": 10,
                "time_semantics": "wall_clock",
                "result": "ok",
                "is_complete": true,
                "query_execution_id": null,
                "query_scope": null,
                "query_owner": null,
                "worker_lane_id": null,
                "worker_kind": null,
                "node_id": null,
                "parent_node_id": null,
                "operator_partition": null,
                "execution_stream_id": null,
                "stage_owner_id": null,
                "direct_sample_count": 1,
                "inclusive_sample_count": 0,
                "resolved_function_sample_count": 0,
                "unresolved_function_sample_count": 0,
                "unwind_error_sample_count": 0,
                "missing_callstack_sample_count": 1,
            },
        }),
    ];
    let mut output = String::from("\"record_hex\"\n");
    for record in records {
        output.push('"');
        for byte in record.to_string().bytes() {
            output.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
            output.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
        }
        output.push_str("\"\n");
    }
    output
}
