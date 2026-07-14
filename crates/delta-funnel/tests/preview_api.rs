//! Compile-time coverage for the public bounded-preview value model.

use delta_funnel::{
    ExecutionProfileMode, PhaseTimingReport, PreviewOptions, QueryExecutionProfile, TablePreview,
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
