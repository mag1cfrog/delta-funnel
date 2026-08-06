#!/bin/sh
set -eu

source_sha=46f23d8fbec7effe3806bf7507a0b43b91a594ec
document=docs/delta-arrow-reader-extraction-baseline.md
repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT HUP INT TERM

git cat-file -e "$source_sha^{commit}"

{
    git ls-tree -r --name-only "$source_sha" -- \
        crates/delta-funnel/src/table_formats/delta \
        crates/delta-funnel/src/query_engine/datafusion/catalog \
        crates/delta-funnel/src/query_engine/datafusion/planning \
        crates/delta-funnel/src/query_engine/datafusion/execution
    sed -n '/^PATHS$/,/^END$/p' <<'PATH_LIST' | sed '1d;$d'
PATHS
Cargo.toml
crates/delta-funnel-python/Cargo.toml
crates/delta-funnel-python/deltafunnel.pyi
crates/delta-funnel-python/src/logging.rs
crates/delta-funnel-python/src/progress.rs
crates/delta-funnel-python/src/session.rs
crates/delta-funnel/Cargo.toml
crates/delta-funnel/src/bin/delta_scan_partition_bench.rs
crates/delta-funnel/src/error.rs
crates/delta-funnel/src/lib.rs
crates/delta-funnel/src/observability.rs
crates/delta-funnel/src/orchestrator/runtime.rs
crates/delta-funnel/src/orchestrator/session.rs
crates/delta-funnel/src/orchestrator/session/dry_run_report.rs
crates/delta-funnel/src/orchestrator/session/options.rs
crates/delta-funnel/src/orchestrator/session/query_handoff.rs
crates/delta-funnel/src/orchestrator/session/registry/derived.rs
crates/delta-funnel/src/orchestrator/session/registry/lineage.rs
crates/delta-funnel/src/orchestrator/session/registry/source.rs
crates/delta-funnel/src/orchestrator/session/source_report.rs
crates/delta-funnel/src/orchestrator/session/sql_server_workflows/output.rs
crates/delta-funnel/src/orchestrator/session/sql_server_workflows/write_all/cache_alias.rs
crates/delta-funnel/src/orchestrator/session/sql_server_workflows/write_all/cache_plan.rs
crates/delta-funnel/src/orchestrator/session/sql_server_workflows/write_all/cached_stream.rs
crates/delta-funnel/src/orchestrator/session/sql_server_workflows/write_all/request.rs
crates/delta-funnel/src/progress.rs
crates/delta-funnel/src/query_engine.rs
crates/delta-funnel/src/query_engine/datafusion.rs
crates/delta-funnel/src/query_engine/datafusion/catalog.rs
crates/delta-funnel/src/query_engine/datafusion/execution.rs
crates/delta-funnel/src/query_engine/datafusion/planning.rs
crates/delta-funnel/src/report.rs
crates/delta-funnel/src/report/delta.rs
crates/delta-funnel/src/report/delta/protocol.rs
crates/delta-funnel/src/report/delta/source.rs
crates/delta-funnel/src/report/execution_profile.rs
crates/delta-funnel/src/report/json.rs
crates/delta-funnel/src/table_formats.rs
crates/delta-funnel/src/table_formats/delta.rs
docs/dependency-alignment.md
docs/dynamic-partition-pruning-investigation.md
docs/failure-reports-and-tracing.md
docs/native-async-backend-benchmark-notes.md
docs/provider-read-scheduling.md
docs/scan-partition-benchmark.md
docs/scan-partition-target-policy.md
docs-site/docs/advanced/multiple-outputs.md
docs-site/docs/advanced/python-logging.md
docs-site/docs/advanced/tracing-and-diagnostics.md
docs-site/docs/concepts.md
docs-site/docs/contributing/profiling-samply.md
docs-site/docs/contributing/profiling-validation-reproduction.md
docs-site/docs/contributing/scan-benchmarks.md
docs-site/docs/index.md
docs-site/docs/internals/datafusion-delta.md
docs-site/docs/internals/provider-read-scheduling.md
docs-site/docs/internals/scan-partition-planning.md
docs-site/docs/progress.md
docs-site/docs/python-api-walkthrough.md
docs-site/docs/reference/api.md
docs-site/docs/reference/diagnostics.md
docs-site/docs/reference/execution-profile.md
END
PATH_LIST
} | LC_ALL=C sort -u >"$tmpdir/required-paths"

awk '
    /<!-- reader-ownership-map:start -->/ { found_start = 1; capture = 1; next }
    /<!-- reader-ownership-map:end -->/ { found_end = 1; capture = 0; next }
    capture && /^[0-9a-f]/ { print }
    END {
        if (!found_start || !found_end) {
            exit 1
        }
    }
' "$document" >"$tmpdir/recorded"

if ! awk '
    NF != 4 ||
    length($1) != 40 ||
    $1 !~ /^[0-9a-f]+$/ ||
    $2 !~ /^[0-9]+$/ ||
    $3 !~ /^(standalone_core|standalone_datafusion|delta_funnel_integration|delta_funnel_only|shared_fixture_to_replace)$/ {
        exit 1
    }
' "$tmpdir/recorded"; then
    echo "invalid reader ownership inventory row" >&2
    exit 1
fi

cut -d ' ' -f4 "$tmpdir/recorded" | LC_ALL=C sort >"$tmpdir/recorded-paths"
diff -u "$tmpdir/required-paths" "$tmpdir/recorded-paths"

while read -r recorded_blob recorded_lines owner filepath; do
    actual_blob=$(git rev-parse "$source_sha:$filepath")
    actual_lines=$(git show "$source_sha:$filepath" | wc -l | tr -d ' ')

    if [ "$recorded_blob" != "$actual_blob" ] || [ "$recorded_lines" != "$actual_lines" ]; then
        echo "inventory mismatch: $filepath" >&2
        exit 1
    fi
done <"$tmpdir/recorded"

actual_digest=$(
    awk '{print $1 " " $4}' "$tmpdir/recorded" |
        LC_ALL=C sort |
        sha256sum |
        awk '{print $1}'
)
recorded_digest=$(
    sed -n 's/^Inventory digest.*: `\([0-9a-f]*\)`\.$/\1/p' "$document"
)

if [ "$recorded_digest" != "$actual_digest" ]; then
    echo "inventory digest mismatch" >&2
    exit 1
fi

printf 'verified %s reader ownership entries at %s\n' "$(wc -l <"$tmpdir/recorded" | tr -d ' ')" "$source_sha"
