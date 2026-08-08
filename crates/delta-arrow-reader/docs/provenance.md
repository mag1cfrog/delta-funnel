# Provenance

## Staging source

- Source repository: <https://github.com/mag1cfrog/delta-funnel>
- Frozen reader source SHA:
  `e2650427ff5e2e1a7a4e5ef9eaf30969b217fec3`
- Baseline artifact: `docs/delta-arrow-reader-extraction-baseline.md`
- Staging branch base SHA:
  `89c7c87d6e2eaf27283eae581f9e2a11821fb5f3`
- Temporary branch: `refactor/delta-arrow-reader-staging`
- Temporary crate path: `crates/delta-arrow-reader`
- Identity and inventory check: `2026-08-07T02:00:13Z` UTC

The governing issues are
[#447](https://github.com/mag1cfrog/delta-funnel/issues/447),
[#459](https://github.com/mag1cfrog/delta-funnel/issues/459), and
[#460](https://github.com/mag1cfrog/delta-funnel/issues/460).

## Independent handoff

[Issue #486](https://github.com/mag1cfrog/delta-funnel/issues/486) will export
one later frozen staging SHA into the independent repository and become the
final repository and provenance authority. This scaffold does not reserve or
publish the candidate package.

## Path mapping

| Delta Funnel source path | Staged crate path | Owning issue |
| --- | --- | --- |
| `crates/delta-funnel/src/table_formats/delta/uri.rs` | `crates/delta-arrow-reader/src/uri.rs` | #462 |
| `crates/delta-funnel/src/table_formats/delta/kernel.rs` | `crates/delta-arrow-reader/src/kernel.rs` | #462 |
| `crates/delta-funnel/src/table_formats/delta/snapshot.rs` | `crates/delta-arrow-reader/src/snapshot.rs` | #462 |
| `crates/delta-funnel/src/table_formats/delta/protocol.rs` | `crates/delta-arrow-reader/src/protocol.rs` | #462 |
| `crates/delta-funnel/src/table_formats/delta/deletion_vector.rs` | `crates/delta-arrow-reader/src/deletion_vector.rs` | #477 |
| DV metadata portions of `crates/delta-funnel/src/table_formats/delta.rs` | `crates/delta-arrow-reader/src/kernel.rs` and `src/deletion_vector.rs` | #477 |
| DV masking portions of `crates/delta-funnel/src/query_engine/datafusion/execution/file_reader.rs` | `crates/delta-arrow-reader/src/deletion_vector.rs` | #477 |
| DV coordinate portions of `crates/delta-funnel/src/query_engine/datafusion/execution/native_async_reader.rs` | `crates/delta-arrow-reader/src/deletion_vector.rs` | #477 |
| DV counter portions of `crates/delta-funnel/src/query_engine/datafusion/execution/read_stats.rs` | `crates/delta-arrow-reader/src/metrics.rs` | #477 |
| DataFusion predicate-adapter portions of `crates/delta-funnel/src/table_formats/delta/kernel.rs` | `crates/delta-arrow-reader/src/predicate.rs` and `src/kernel.rs` | #484 |

## Test migration

- #477 ports the focused deletion-vector metadata, payload, coordinate,
  masking, redaction, and metric assertions into the staged crate.
- Scan-metadata preservation remains with #463 because it requires the
  unpartitioned task planner that does not exist yet.
- Parquet/backend/provider integration assertions remain in Delta Funnel for
  #464, #465, #482, and #483. Their movable coordinate and masking assertions
  are already covered here.
- #484 adapts the focused comparison, scalar, Boolean, null, and Kernel
  conversion assertions into the staged crate, then adds schema-validation,
  three-valued residual, pruning-parity, concurrency, and redaction coverage.
- Empty binary values remain accepted, while negative-scale decimal and
  signed-zero float predicates fall back to residual-only when Kernel cannot
  preserve their logical meaning. These #484 contract differences are covered
  locally rather than retaining the old adapter outcomes.
- DataFusion expression translation, including `IN`, `BETWEEN`, casts, and
  qualified-expression rejection, remains with #467. Scan-level pruning and
  residual integration remain with #463 and #466 because those layers do not
  exist in the staged crate yet.
