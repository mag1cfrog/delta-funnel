//! End-to-end coverage for the opt-in ranked report CLI.

#![cfg(all(feature = "perfetto-profile", unix))]

use std::fs;
#[cfg(target_os = "linux")]
use std::fs::File;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
#[cfg(target_os = "linux")]
use std::process::Stdio;

#[test]
fn generates_a_ranked_report_with_one_healthy_trace_query() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("capture.pftrace");
    let output = directory.path().join("capture.profile.html");
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
           *delta_funnel_capture_health_input*'CREATE PERFETTO TABLE delta_funnel_capture_health AS'*record_kind*) ;;\n\
           *) exit 65 ;;\n\
         esac\n\
         cat \"$DELTA_FUNNEL_TEST_AGGREGATE\"\n",
    )?;

    let result = Command::new(env!("CARGO_BIN_EXE_delta-funnel-perfetto"))
        .arg("report")
        .arg(&input)
        .arg("--output")
        .arg(&output)
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

fn write_executable(path: &Path, contents: &str) -> io::Result<()> {
    fs::write(path, contents)?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions)
}

fn aggregate_output() -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    let records = [
        serde_json::json!({
            "record_kind": "metadata",
            "record": {
                "capture_complete": true,
                "semantic_complete": true,
                "schema_version": 1,
                "sample_frequency_hz": 1000,
                "exact_time_unit": "nanoseconds",
                "sample_unit": "samples",
                "eligible_sample_count": 1,
                "direct_sample_count": 1,
                "ambiguous_sample_count": 0,
                "unattributed_sample_count": 0,
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
            },
        }),
        serde_json::json!({
            "record_kind": "function_self",
            "record": {
                "semantic_id": 1,
                "function_id": -1,
                "self_sample_count": 1,
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
