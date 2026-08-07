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
