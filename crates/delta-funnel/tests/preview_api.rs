//! Compile-time coverage for the public bounded-preview value model.

use delta_funnel::{
    DeltaFunnelRuntime, DeltaFunnelSession, ExecutionProfileMode, PhaseTimingReport,
    PreviewOptions, QueryExecutionProfile, SessionOptions, TablePreview,
    progress::ProgressReporter,
};

#[test]
fn preview_options_and_result_accessors_are_exported_from_the_crate_root() {
    let options =
        PreviewOptions::new(20).with_execution_profile_mode(ExecutionProfileMode::Detailed);

    let _: fn(&PreviewOptions) -> usize = PreviewOptions::limit;
    let _: fn(&PreviewOptions) -> ExecutionProfileMode = PreviewOptions::execution_profile_mode;
    let _: for<'a> fn(&'a TablePreview) -> &'a [PhaseTimingReport] = TablePreview::phase_timings;
    let _: for<'a> fn(&'a TablePreview) -> Option<&'a QueryExecutionProfile> =
        TablePreview::execution_profile;

    assert_eq!(options.limit(), 20);
    assert_eq!(
        options.execution_profile_mode(),
        ExecutionProfileMode::Detailed
    );
}

#[tokio::test]
async fn option_bearing_session_route_is_public() -> Result<(), Box<dyn std::error::Error>> {
    let mut async_session = DeltaFunnelSession::new(SessionOptions::default())?;
    let async_table = async_session.table_from_sql("select 1 as id").await?;
    let async_preview = async_session
        .preview_table_with_options(&async_table, PreviewOptions::new(1))
        .await?;
    assert_eq!(async_preview.execution_profile(), None);
    Ok(())
}

#[test]
fn option_bearing_runtime_routes_are_public() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = DeltaFunnelRuntime::new()?;
    let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
    let table = runtime.table_from_sql(&mut session, "select 1 as id")?;
    let options =
        PreviewOptions::new(1).with_execution_profile_mode(ExecutionProfileMode::Detailed);
    let preview = runtime.preview_table_with_options(&session, &table, options)?;
    assert!(preview.execution_profile().is_some());

    let preview = runtime.preview_table_with_options_and_progress(
        &session,
        &table,
        options,
        ProgressReporter::new(|_| {}),
    )?;
    assert!(preview.execution_profile().is_some());
    Ok(())
}
