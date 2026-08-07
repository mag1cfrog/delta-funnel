# delta-arrow-reader

This is the temporary staging crate for extracting Delta Funnel's read-only
Delta Lake to Arrow implementation. It has no reader API yet and is not
published.

The crate exists only on the
[`refactor/delta-arrow-reader-staging`](https://github.com/mag1cfrog/delta-funnel/tree/refactor/delta-arrow-reader-staging)
branch. [Issue #460](https://github.com/mag1cfrog/delta-funnel/issues/460)
owns this scaffold, and
[issue #447](https://github.com/mag1cfrog/delta-funnel/issues/447) owns the
extraction lifecycle.

## Validate the scaffold

Run these commands from the Delta Funnel repository root:

```console
cargo fmt --all -- --check
cargo check -p delta-arrow-reader --all-targets
cargo clippy -p delta-arrow-reader --all-targets -- -D warnings
cargo test -p delta-arrow-reader
RUSTDOCFLAGS="-D warnings" cargo doc -p delta-arrow-reader --no-deps
cargo package -p delta-arrow-reader --allow-dirty
git diff --check
```

Reader features, dependencies, behavior, and public APIs belong to later
#447-family issues.
