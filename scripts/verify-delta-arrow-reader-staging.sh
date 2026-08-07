#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"

crate_manifest=crates/delta-arrow-reader/Cargo.toml
release_config=release-plz.toml
release_workflow=.github/workflows/release-plz.yml

for expected in \
    'name = "delta-arrow-reader"' \
    'version = "0.1.0"' \
    'edition = "2024"' \
    'rust-version = "1.88"' \
    'license = "Apache-2.0"' \
    'publish = false'
do
    grep -Fx "$expected" "$crate_manifest" >/dev/null
done

if grep -Eq 'workspace[[:space:]]*=[[:space:]]*true|delta[-_]funnel' "$crate_manifest"; then
    echo "temporary package inherits workspace metadata or depends on Delta Funnel" >&2
    exit 1
fi

grep -Fx '    "crates/delta-arrow-reader",' Cargo.toml >/dev/null

workspace_release=$(
    sed -n '/^\[workspace\]$/,/^\[/p' "$release_config" |
        sed -n 's/^release = \(.*\)$/\1/p'
)
if [ "$workspace_release" != false ]; then
    echo "release-plz workspace default must remain disabled" >&2
    exit 1
fi

selected_packages=$(
    awk '
        function emit() {
            if (name != "" && release == "true") {
                print name
            }
        }
        /^\[\[package\]\]$/ {
            emit()
            name = ""
            release = ""
            next
        }
        /^name = "/ {
            name = $0
            sub(/^name = "/, "", name)
            sub(/"$/, "", name)
            next
        }
        /^release = / {
            release = $3
        }
        END {
            emit()
        }
    ' "$release_config" | LC_ALL=C sort
)
expected_packages=$(printf '%s\n' delta-funnel delta-funnel-python)
if [ "$selected_packages" != "$expected_packages" ]; then
    echo "unexpected release-plz package selection" >&2
    printf 'expected:\n%s\nactual:\n%s\n' "$expected_packages" "$selected_packages" >&2
    exit 1
fi

release_condition=$(sed -n '/^  release:$/,/^    runs-on:/p' "$release_workflow")
printf '%s\n' "$release_condition" |
    grep -F "github.ref == 'refs/heads/main' &&" >/dev/null

push_branches=$(sed -n '/^  push:$/,/^  workflow_dispatch:/p' "$release_workflow")
printf '%s\n' "$push_branches" | grep -Fx '      - main' >/dev/null

if grep -F 'delta-arrow-reader' "$release_config" CHANGELOG.md >/dev/null ||
    test -n "$(git tag --list '*delta-arrow-reader*')"; then
    echo "Delta Funnel release artifacts mention the temporary package" >&2
    exit 1
fi

printf 'verified delta-arrow-reader staging and release isolation\n'
