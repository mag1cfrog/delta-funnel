# delta-arrow-reader

This is the temporary staging crate for extracting Delta Funnel's read-only
Delta Lake to Arrow implementation. It currently contains the reader foundation,
snapshot and protocol loading, deletion-vector handling, predicates, scan
planning, partition grouping, and backend-neutral scheduling. It is not
published or used by Delta Funnel production code.

The crate exists only on the
[`refactor/delta-arrow-reader-staging`](https://github.com/mag1cfrog/delta-funnel/tree/refactor/delta-arrow-reader-staging)
branch. [Issue #460](https://github.com/mag1cfrog/delta-funnel/issues/460)
owns this scaffold, and
[issue #447](https://github.com/mag1cfrog/delta-funnel/issues/447) owns the
extraction lifecycle.

No public table-loading or scan-stream API exists yet. The NativeAsync and
OfficialKernel file readers, direct API, and DataFusion integration belong to
later #447-family issues.

## Validate the staging crate

Run these commands from the Delta Funnel repository root:

```console
cargo fmt --all -- --check
cargo check -p delta-arrow-reader --no-default-features --all-targets
cargo check -p delta-arrow-reader --all-features --all-targets
cargo clippy -p delta-arrow-reader --all-targets --all-features -- -D warnings
cargo test -p delta-arrow-reader --no-default-features
cargo test -p delta-arrow-reader --all-features
RUSTDOCFLAGS="-D warnings" cargo doc -p delta-arrow-reader --all-features --no-deps
cargo package -p delta-arrow-reader --allow-dirty
git diff --check
```
