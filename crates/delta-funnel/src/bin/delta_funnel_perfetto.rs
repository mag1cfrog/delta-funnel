//! Generates Delta Funnel Perfetto diagnostics reports.

fn main() {
    std::process::exit(delta_funnel::perfetto_profile::run_perfetto_diagnostics_cli());
}
