import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { matchesGlob } from "node:path";

const { diagnostics } = JSON.parse(
  readFileSync(".github/path-filters.json", "utf8"),
);
const selectsDiagnostics = (file) =>
  diagnostics.some((pattern) => matchesGlob(file, pattern));

for (const file of [
  "crates/delta-funnel/src/sql_server/execution/sink.rs",
  "crates/delta-funnel/src/table_formats/delta/read.rs",
  "crates/delta-funnel-python/src/session.rs",
  "crates/delta-funnel-python/src/logging.rs",
  "crates/delta-funnel/src/query_engine/datafusion.rs",
  "crates/delta-funnel/src/query_engine/datafusion/execution/metered_object_store.rs",
]) {
  assert.equal(selectsDiagnostics(file), false, `${file} selected diagnostics`);
}

for (const file of [
  "Cargo.toml",
  "Cargo.lock",
  "crates/delta-funnel/Cargo.toml",
  "crates/delta-funnel/src/perfetto_profile.rs",
  "crates/delta-funnel/src/perfetto_profile/report_cli.rs",
  "crates/delta-funnel/src/profiling.rs",
  "crates/delta-funnel/src/query_engine/datafusion/execution_profile.rs",
  "crates/delta-funnel/src/query_engine/datafusion/operator_activity.rs",
  "crates/delta-funnel/src/query_engine/datafusion/operator_activity/task_tracing.rs",
  "crates/delta-funnel/src/query_engine/datafusion/planning_activity.rs",
  "crates/delta-funnel/src/query_engine/datafusion/profiled_execution.rs",
  "crates/delta-funnel/src/query_engine/datafusion/profiled_object_store.rs",
  "crates/delta-funnel-python/build.rs",
  "crates/delta-funnel-python/pyproject.toml",
  "crates/delta-funnel-python/src/perfetto_diagnostics.rs",
  "tools/perfetto/delta-funnel-standard.pbtx",
  ".github/workflows/testpypi-diagnostics-build.yml",
]) {
  assert.equal(
    selectsDiagnostics(file),
    true,
    `${file} skipped diagnostics`,
  );
}
