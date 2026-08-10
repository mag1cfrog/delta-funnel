# delta-arrow-reader

This is the temporary staging crate for extracting Delta Funnel's read-only
Delta Lake to Arrow implementation. It contains the reader foundation, snapshot
and protocol loading, deletion-vector handling, predicates, scan planning,
partition grouping, backend-neutral scheduling, NativeAsync and OfficialKernel
file executors, the public DataFusion-independent streaming reader, and an
optional public DataFusion provider with registration, filtering, execution,
dynamic partition pruning, and metrics support. It is not published or used by
Delta Funnel production code.

The crate exists only on the
[`refactor/delta-arrow-reader-staging`](https://github.com/mag1cfrog/delta-funnel/tree/refactor/delta-arrow-reader-staging)
branch. [Issue #460](https://github.com/mag1cfrog/delta-funnel/issues/460)
owns this scaffold, and
[issue #447](https://github.com/mag1cfrog/delta-funnel/issues/447) owns the
extraction lifecycle.

## Read a table without SQL

The caller owns the Tokio runtime and pulls batches from the returned stream.
The reader does not collect the full result in memory.

```rust,no_run
use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTableBuilder};
use futures_util::TryStreamExt;

# async fn read_table() -> Result<(), Box<dyn std::error::Error>> {
let table = DeltaTableBuilder::new("/tmp/example-delta-table")
    .load_async()
    .await?;
let scan = table
    .scan()
    .with_projection(vec!["id".into(), "name".into()])
    .with_predicate(DeltaPredicate::Compare {
        column: "id".into(),
        op: DeltaComparison::GtEq,
        value: DeltaScalar::Int64(10),
    })
    .with_limit(100)
    .build()
    .await?;
let mut batches = scan.execute().await?;

while let Some(batch) = batches.try_next().await? {
    println!("rows={}", batch.num_rows());
}
# Ok(())
# }
```

Run the deterministic local end-to-end example, which creates its own Delta
table and reads it through only the public API:

```console
cargo test -p delta-arrow-reader --test direct_reader local_end_to_end_example_reads_without_sql -- --exact --nocapture
```

## Validate the staging crate

Run these commands from the Delta Funnel repository root:

```console
cargo fmt --all -- --check
cargo check -p delta-arrow-reader --no-default-features --all-targets
cargo check -p delta-arrow-reader --all-features --all-targets
cargo clippy -p delta-arrow-reader --all-targets --all-features -- -D warnings
cargo test -p delta-arrow-reader --no-default-features
cargo test -p delta-arrow-reader --no-default-features --features official-kernel
cargo test -p delta-arrow-reader --features official-kernel
cargo test -p delta-arrow-reader --all-features
RUSTDOCFLAGS="-D warnings" cargo doc -p delta-arrow-reader --all-features --no-deps
cargo package -p delta-arrow-reader --allow-dirty
git diff --check
```
