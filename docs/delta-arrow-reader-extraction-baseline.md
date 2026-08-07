# Delta Lake-to-Arrow reader extraction baseline

This is the evidence artifact required by [issue #459](https://github.com/mag1cfrog/delta-funnel/issues/459) in the [#447 extraction family](https://github.com/mag1cfrog/delta-funnel/issues/447). The linked issues remain the requirements authority. This document records evidence only.

## Source identity

- Source commit: `e2650427ff5e2e1a7a4e5ef9eaf30969b217fec3` from Delta Funnel's default `main` branch.
- Package identity: workspace package `delta-funnel` version `0.5.0`.
- Release identity: tag `delta-funnel-v0.5.0` at `29c7cabf02451fbfa330f9db95eb1f9fc65c8f2e`; the package remains version `0.5.0`, and the source commit is the later benchmark-only merge for PR #640.
- Required reader changes: the source commit contains #631 (`a3060716`), #633 (`dfed15da`), #635 (`d4b55edc`), #639 (`d57775fb`), and the #459 benchmark prerequisite in PR #640 (`e2650427`).
- Inventory capture: `2026-08-06T15:36:25Z` UTC.
- Rust toolchain: `rustc 1.97.0 (2d8144b78 2026-07-07)`, host `x86_64-unknown-linux-gnu`, LLVM `22.1.6`.
- Host: `Linux x86_64`, available parallelism `16`.
- Benchmark build profile: Cargo `release`.
- Comparison rule: later extraction PRs use the source commit above unless #459 is explicitly reopened and updated before #462 begins.

The active extraction source is the v0.5.0 state above. The #561 TestPyPI `0.4.2.dev` package is historical performance-control evidence only. No v0.4.x or v0.3.x package, tag, report schema, or profiling artifact is an extraction source.

### Related work at capture

| Authority | State at capture | Final merged source where applicable |
| --- | --- | --- |
| [#443](https://github.com/mag1cfrog/delta-funnel/issues/443) | closed, completed | children #448 and #449 below |
| [#448](https://github.com/mag1cfrog/delta-funnel/issues/448) | closed, completed | PR #489, `8478bc7c4e781ef5c7c2e9119e1d93c08e6ec904` |
| [#449](https://github.com/mag1cfrog/delta-funnel/issues/449) | closed, completed | PR #490, `e97173db644eb10c6f57be741a6429bca441f660` |
| [#445](https://github.com/mag1cfrog/delta-funnel/issues/445) | closed, completed | final closeout PR #499, `20b217ec405512d4926c9a2f04853002818d46c1` |
| [#545](https://github.com/mag1cfrog/delta-funnel/issues/545) | closed, completed | child-owned PRs; #612 is the retained exact-profile authority |
| [#560](https://github.com/mag1cfrog/delta-funnel/issues/560) | closed, completed | PR #578, `a24edbad9b72c8c74d38500d640c1be2416cfb47` |
| [#561](https://github.com/mag1cfrog/delta-funnel/issues/561) | closed, not planned after evidence found no justified optimization child | no closing PR; accepted result is included in the source commit |
| [#612](https://github.com/mag1cfrog/delta-funnel/issues/612) | closed, completed | PR #613, `2689ba21713bb1ff360dc6ed423af2617acf5f75` |
| [#640](https://github.com/mag1cfrog/delta-funnel/pull/640) | closed, completed benchmark prerequisite for #459 | `e2650427ff5e2e1a7a4e5ef9eaf30969b217fec3` |
| [#447](https://github.com/mag1cfrog/delta-funnel/issues/447) and its 26 native children | open, implementation-ready | #459 is the first child and the family parent is the membership authority |

There were no open pull requests in the repository at capture. The planned open work touching the inventory is the #447 family recorded by its parent; it is not competing source drift.

## Reader ownership map

Each entry is `git_blob line_count final_owner repository_path`. Line counts are review-sizing evidence only. The owner meanings are:

- `standalone_core`: query-engine-neutral reader code or documentation moves to the standalone crate.
- `standalone_datafusion`: DataFusion adapter code, tests, benchmarks, or documentation moves with the optional DataFusion surface.
- `delta_funnel_integration`: Delta Funnel keeps the file and later adapts its reader-facing portions to the published crate.
- `delta_funnel_only`: Delta Funnel keeps the file as an application, reporting, profiling, progress, or Python concern.
- `shared_fixture_to_replace`: later fixture work must copy or deterministically recreate the truth without retaining a shared production dependency.

Inventory entries: `93`.

<!-- reader-ownership-map:start -->
```text
ccb975ddc2f06021ec6b26efffa5fad91fe7f373 23 delta_funnel_integration Cargo.toml
2d2c41b8eaaa182ed4b53df270f1648ad1324560 38 delta_funnel_integration crates/delta-funnel-python/Cargo.toml
95279048fc6809ea0c4dab915f874e49b6a6ef8c 171 delta_funnel_integration crates/delta-funnel-python/deltafunnel.pyi
5863066994bfac6d85f2fc51dfee4572641e726b 783 delta_funnel_only crates/delta-funnel-python/src/logging.rs
7b8732c9e0e53bfcbdb998c73267032203ea268c 2994 delta_funnel_only crates/delta-funnel-python/src/progress.rs
0ff7e9f00915c3c265c9dc64166bd320697edaed 4848 delta_funnel_integration crates/delta-funnel-python/src/session.rs
cd30cbde2c8ba6efb37f765cda258d18e5ff15c8 79 delta_funnel_integration crates/delta-funnel/Cargo.toml
52284e22a5deb0cddce4aa7257012468dc88f25e 9415 delta_funnel_only crates/delta-funnel/src/bin/delta_scan_partition_bench.rs
8d9f81687958575d86f2ea9e9fa7740a7e6cfef0 1043 delta_funnel_integration crates/delta-funnel/src/error.rs
9961345ab58573c4d211b8977d77eb05fc08c5af 187 delta_funnel_integration crates/delta-funnel/src/lib.rs
f4c153f7dba545bb7a9492b375fba792709be1bd 2182 delta_funnel_only crates/delta-funnel/src/observability.rs
14a3a437464e2947b78f7f64b5eb3e56dad331de 776 delta_funnel_integration crates/delta-funnel/src/orchestrator/runtime.rs
cdfafdb2100e417b6d61ffc09cb50961612527ef 102 delta_funnel_integration crates/delta-funnel/src/orchestrator/session.rs
8da6e649faa40453f2c9ebc0489d22fdb686454b 1314 delta_funnel_only crates/delta-funnel/src/orchestrator/session/dry_run_report.rs
35e7e81b86e7f70b53a764d8c1c2b54b0b11a02a 305 delta_funnel_integration crates/delta-funnel/src/orchestrator/session/options.rs
c85e94649279c73a48c6a30d875341da6bbf6bab 3576 delta_funnel_integration crates/delta-funnel/src/orchestrator/session/query_handoff.rs
c83e535b3da3f950aa47bc1cac0960a2e7e25f3c 859 delta_funnel_only crates/delta-funnel/src/orchestrator/session/registry/derived.rs
e3df85948a28fb721e732190f55c5413f54246a1 764 delta_funnel_only crates/delta-funnel/src/orchestrator/session/registry/lineage.rs
fb3a3da2031a3dd12457adcf9b4fef8f003ea2d2 865 delta_funnel_integration crates/delta-funnel/src/orchestrator/session/registry/source.rs
22ec50cc2c5e90b855d4a813fd43fbdec043386d 318 delta_funnel_only crates/delta-funnel/src/orchestrator/session/source_report.rs
94fae3174497fd41dcdc5249cef6018f0f29a4dd 2654 delta_funnel_only crates/delta-funnel/src/orchestrator/session/sql_server_workflows/output.rs
4d54b6ccfd637e1a17df6860ffa209f92fefddf6 2749 delta_funnel_only crates/delta-funnel/src/orchestrator/session/sql_server_workflows/write_all/cache_alias.rs
b8d4232a0d88b60fa49159a886e42f4c18798831 1352 delta_funnel_only crates/delta-funnel/src/orchestrator/session/sql_server_workflows/write_all/cache_plan.rs
08cee16cbe849f31e3a3df1062a8d5bacbcc26e7 1240 delta_funnel_only crates/delta-funnel/src/orchestrator/session/sql_server_workflows/write_all/cached_stream.rs
0c897a99d8df2e9229c401cce0aa3efddc68f92d 3935 delta_funnel_only crates/delta-funnel/src/orchestrator/session/sql_server_workflows/write_all/request.rs
55fcc8354ddee9c616cb6b5a9897f727b848b188 830 delta_funnel_only crates/delta-funnel/src/progress.rs
a0a256c1a1e54fb6a1581a1509455eeff7eae5a6 15 delta_funnel_integration crates/delta-funnel/src/query_engine.rs
f2c413a78e6b01789099a3d0b548bad5ed09c57d 872 delta_funnel_integration crates/delta-funnel/src/query_engine/datafusion.rs
6e92824b2e2099b9f99e1129792861cce2f3cd53 4 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/catalog.rs
82bbdaae6cce68ec07b6d52dd222bf9ee4bb193f 578 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/catalog/provider.rs
75f5567cf46bf8eb6ceb183195f45efe06c18460 14392 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/catalog/provider_tests.rs
7b35de637cb6009b547fde2ea213b3b3b9d771f1 808 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/catalog/registration.rs
38df9ed0b18c45f5f25d40da2a901df8e9f855a9 16 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/execution.rs
8144fff83b493a3aeb30322903638ffa79def974 715 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/execution/async_scheduler.rs
c90e698e06ff75ab21119a1847c89fc41e18670b 356 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/execution/environment.rs
234b372bb6cb792760649cb691cc0ff6f8dad75f 739 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/execution/file_reader.rs
c9ab9e24075bfe1dc817b1b3746c3f83cc00c034 659 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/execution/metered_object_store.rs
2b6679bdaa577096d7354b5006d82a77719ef503 4898 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/execution/native_async_reader.rs
aaf8b18cb180466cd4998dd5b8bc5537142fffd0 354 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/execution/native_async_row_group_pruning.rs
7303bf070fdee9546d3232d2dc65b53e7380015b 7040 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/execution/planning_exec.rs
1e8b6fe493d3f6e5dfdfb10e9d7c541de2ca4166 645 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/execution/read_stats.rs
a40fd55a39686fc5ee2708f5b9ce2594acb0a2d1 67 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/execution/reader_backend.rs
306d5c46e611a6a14e3183b638f753a80ad08173 1125 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/execution/scheduling.rs
a82cbb62320f1d928d6e035e1a20b04f49d13999 10 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning.rs
841c8ae010b3c888d8fcf0ed0bb3690dea6a16af 423 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning/dynamic_filters.rs
09bda3b52491215e6f2cc8604a720ecb798f31e5 665 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning/dynamic_partition_pruning.rs
6af7870f36484f79f57b10be698df42c44ad214e 310 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning/file_task.rs
676164becffbbb42a9512e318096dee0cef82a6e 865 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning/file_task_partition.rs
29276e3a4a7a6bd4157492a9598101a924bf71f5 1732 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning/filters.rs
ad6d509279c04fa0ebacd813327308f011764fa8 716 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning/filters/analysis.rs
f6e8c2249cb1b0620811a9432f27e5c19878899a 2995 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning/filters/partition_pushdown.rs
9171d84f89eaf7af8736290a25aa47f7f370a480 385 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning/filters/stats_pushdown.rs
39113dbde4c4b3528ab6dac818fa5cae76197a01 993 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning/partition_target.rs
2db8455cf23e34c04249c274f52bbccdfe73b337 351 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning/projection.rs
0c5ff115caf8331b3743a4dbc32ff7741a206078 1055 standalone_datafusion crates/delta-funnel/src/query_engine/datafusion/planning/scan_plan.rs
03f4f2539214b3dab418641f010b3670bc16c950 2070 delta_funnel_only crates/delta-funnel/src/report.rs
800a87377445661e76fe0f832fe2853772b71b89 5 delta_funnel_only crates/delta-funnel/src/report/delta.rs
ede443814c19aa7ed2878974e1119f2714755843 20 delta_funnel_only crates/delta-funnel/src/report/delta/protocol.rs
dda8062dfd9abf191908f61d8799cbe6388a4355 308 delta_funnel_only crates/delta-funnel/src/report/delta/source.rs
1912da52955fa8ebc0336205d2a26ad64bfcf5dc 557 delta_funnel_only crates/delta-funnel/src/report/execution_profile.rs
873d461d321e06bd9d6616b20c300bac6d037275 1628 delta_funnel_only crates/delta-funnel/src/report/json.rs
b9648579a946a95e171c5ed2bb42ad07d7022a3c 37 delta_funnel_integration crates/delta-funnel/src/table_formats.rs
32925e7815ef15e9f3e5a64bd1617c73778ad17c 7095 standalone_core crates/delta-funnel/src/table_formats/delta.rs
07e0eae3aea64c468c784ecbc5f7d74e9c80a058 874 standalone_core crates/delta-funnel/src/table_formats/delta/deletion_vector.rs
01f726bac64d368a79e40ca32920132505589742 1248 standalone_core crates/delta-funnel/src/table_formats/delta/kernel.rs
795ae78b68d319a3f482236d96360f462e18c202 539 standalone_core crates/delta-funnel/src/table_formats/delta/protocol.rs
4961b6fc1d816e93bb0005d31217a9822af385ac 600 standalone_core crates/delta-funnel/src/table_formats/delta/read.rs
188bcee2aca8c136dbdfac7937f82806eb6141b5 674 standalone_core crates/delta-funnel/src/table_formats/delta/snapshot.rs
ed424ad1f50857cc44bacdeaf7abe86fdd3a4940 2515 shared_fixture_to_replace crates/delta-funnel/src/table_formats/delta/test_support.rs
6a48ef149199d4e79bdb036aec4ce0281a800261 171 standalone_core crates/delta-funnel/src/table_formats/delta/uri.rs
bd0b60d0e53258ffe647ec6eedf485dcad04fad3 113 delta_funnel_only docs-site/docs/advanced/multiple-outputs.md
b39b98302e20d7e611b1135991258fd859ec4074 77 delta_funnel_only docs-site/docs/advanced/python-logging.md
4cf5a053bca49b4b6b49bac87115846128039dfe 150 delta_funnel_only docs-site/docs/advanced/tracing-and-diagnostics.md
43c67e93865d7f0c1f429369a63c286da69b59ff 40 delta_funnel_integration docs-site/docs/concepts.md
a1651f985f3b2ec09b2cdcd5c6dd4707ff163ad0 133 delta_funnel_only docs-site/docs/contributing/profiling-samply.md
5f4e20132cff11d02955815e6b7ba83398dbad99 186 delta_funnel_only docs-site/docs/contributing/profiling-validation-reproduction.md
967174b73d91b678747966d006c445e15a54788e 224 standalone_datafusion docs-site/docs/contributing/scan-benchmarks.md
474c8d54b52b6989d9346fc3a1d8af57a35cde19 86 delta_funnel_integration docs-site/docs/index.md
349ef34bd865e6b31f61771ebc962ecf2bba39cd 25 standalone_datafusion docs-site/docs/internals/datafusion-delta.md
a2ddfd5e990143c308557bead7e2cab68c37e446 224 standalone_datafusion docs-site/docs/internals/provider-read-scheduling.md
f8aa31c4e71e95d55c936da457f7fe30f5f7c28f 91 standalone_datafusion docs-site/docs/internals/scan-partition-planning.md
d8f51f69cf410ac1763b286c7ea88b20f51407e0 188 delta_funnel_only docs-site/docs/progress.md
2b33b94b706a05f8bbb5e907c0bed5a62e630da0 104 delta_funnel_integration docs-site/docs/python-api-walkthrough.md
89714ee36c3ebcc5fef0163091fcf86f8a8b0305 541 delta_funnel_integration docs-site/docs/reference/api.md
e86119f169b85d2579623514276f991099ca424e 237 delta_funnel_only docs-site/docs/reference/diagnostics.md
76e1f53b10a7c7e1bd0aa12d5db35b68679c2978 149 delta_funnel_only docs-site/docs/reference/execution-profile.md
f1e5f2d49412529b24b4f6912412a8c1be6face0 4 delta_funnel_only docs/dependency-alignment.md
65df72c135eb2cb06d88e408d07e64d96d2c78ae 4 standalone_datafusion docs/dynamic-partition-pruning-investigation.md
d88ea7d6ac594dd776cf16a70f539d2702f7648e 10 delta_funnel_only docs/failure-reports-and-tracing.md
353d3c9361409d7466a15894ce2aaff884d9a2b3 7 standalone_datafusion docs/native-async-backend-benchmark-notes.md
14e023c0d53b3de1994fa8a6adb5bb7e5ffae753 4 standalone_datafusion docs/provider-read-scheduling.md
909e0ca7dff09a1d400dde453dcea73944c09889 4 standalone_datafusion docs/scan-partition-benchmark.md
a5d74f6c7cee2c2bb183c2fb4bcd29e2572aa7b1 4 standalone_datafusion docs/scan-partition-target-policy.md
```
<!-- reader-ownership-map:end -->

Inventory digest (SHA-256 of sorted `git_blob repository_path` lines): `7b7f406f0713c6414a36d28cd45a761e036cea027267ed2c28dfb73dd8233db7`.

Reproduce this section with:

```console
scripts/verify-delta-arrow-reader-extraction-baseline.sh
```

## Public API migration inventory

The published 0.5.0 rustdoc is an exact signature reference because `delta-funnel-v0.5.0` and the frozen source commit have identical trees. The migration is intentionally breaking: standalone-owned exports are removed from the `delta_funnel` root, while genuine Delta Funnel orchestration, reporting, session, and error behavior remains integration. No compatibility alias, wrapper, or re-export is permitted. Canonical destinations are fixed by [#461](https://github.com/mag1cfrog/delta-funnel/issues/461), [#466](https://github.com/mag1cfrog/delta-funnel/issues/466), [#478](https://github.com/mag1cfrog/delta-funnel/issues/478), [#480](https://github.com/mag1cfrog/delta-funnel/issues/480), and [#468](https://github.com/mag1cfrog/delta-funnel/issues/468).

<!-- public-compatibility:start -->

| Crate-root item | Required treatment | Destination or retained responsibility | Affected callers | Migration test/documentation owner | Exact 0.5.0 signature source |
| --- | --- | --- | --- | --- | --- |
| `DeltaFunnelError` | `delta_funnel_owned_integration` | Remains `delta_funnel::DeltaFunnelError`; maps stable standalone error kinds at product boundaries. | Rust and Python error consumers | #474 boundary tests; #475 API docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/error/enum.DeltaFunnelError.html) |
| `DeltaProtocolReport` | `delta_funnel_owned_integration` | Remains `delta_funnel::DeltaProtocolReport`; owns sanitized product reporting. | Reports, progress, Python | #474 report tests; #475 API docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaProtocolReport.html) |
| `DeltaProviderSchedulingReport` | `delta_funnel_owned_integration` | Remains `delta_funnel::DeltaProviderSchedulingReport`; serializes product scheduling policy. | Reports, profiles, Python | #474 report tests; #475 diagnostics docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaProviderSchedulingReport.html) |
| `DeltaSourceReport` | `delta_funnel_owned_integration` | Remains `delta_funnel::DeltaSourceReport`; combines usage, protocol, scheduling, and metrics. | Reports, workflows, Python | #474 report tests; #475 diagnostics docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaSourceReport.html) |
| `SourceUsageStatus` | `delta_funnel_owned_integration` | Remains `delta_funnel::SourceUsageStatus`; owns product source-usage state. | Reports and workflows | #474 report tests; #475 diagnostics docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/enum.SourceUsageStatus.html) |
| `DeltaSourceConfig` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaTableBuilder`; Delta Funnel separately applies source naming and environment policy. | Rust loading, session registry, Python | #474 shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaSourceConfig.html) |
| `DeltaStorageOptions` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaStorageOptions` | Rust loading and Python options | #474 compile/shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/type.DeltaStorageOptions.html) |
| `PlannedDeltaSource` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaTable` | Rust loading, provider registration, reports | #474 compile/shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.PlannedDeltaSource.html) |
| `ProtocolPreflight` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaProtocolInfo` plus `DeltaTable::validate_protocol` | Rust loading, registration, reports | #474 shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.ProtocolPreflight.html) |
| `load_delta_source` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaTableBuilder::load` | Rust loading, tests, benchmark | #474 shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.load_delta_source.html) |
| `load_delta_sources` | `replace_with_standalone_import` | Repeated `delta_arrow_reader::DeltaTableBuilder::load`; Delta Funnel owns the multi-source loop. | Rust multi-source orchestration | #474 shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.load_delta_sources.html) |
| `load_delta_source_with_tracing` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaTableBuilder::load`; standalone owns bounded load tracing. | Session registry, progress, benchmark | #474 telemetry shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.load_delta_source_with_tracing.html) |
| `preflight_delta_protocol` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaTable::validate_protocol` | Rust reader and provider tests | #474 shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.preflight_delta_protocol.html) |
| `preflight_delta_sources` | `replace_with_standalone_import` | Repeated `delta_arrow_reader::DeltaTable::validate_protocol`; Delta Funnel owns the multi-source loop. | Rust multi-source orchestration | #474 shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.preflight_delta_sources.html) |
| `preflight_delta_protocol_with_tracing` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaTable::validate_protocol`; standalone owns bounded validation tracing. | Session registry, progress, benchmark | #474 telemetry shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.preflight_delta_protocol_with_tracing.html) |
| `DeltaProviderReadStatsSnapshot` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaDataFusionMetricsSnapshot` plus its nested `DeltaReadMetricsSnapshot` | Reports, profiles, progress, benchmark | #474 metrics shadow tests; #475 diagnostics docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaProviderReadStatsSnapshot.html) |
| `DeltaProviderReaderBackend` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaReaderBackend` | Rust provider callers and benchmark | #474 backend shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/enum.DeltaProviderReaderBackend.html) |
| `DeltaProviderScanExecutionOptions` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaReaderExecutionOptions` | Session, Python adapter, benchmark | #474 option shadow tests; #475 Rust/Python docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaProviderScanExecutionOptions.html) |
| `DeltaScanPartitionTargetDiagnosticInput` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaScanPartitionTargetDiagnosticInput` | Benchmark diagnostic path | #474 compile tests; #475 benchmark docs | exact hidden declaration below |
| `DeltaScanPartitionTargetDiagnosticOutput` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaScanPartitionTargetDiagnosticOutput` | Benchmark diagnostic path | #474 compile tests; #475 benchmark docs | exact hidden declaration below |
| `DeltaScanPartitionTargetDiagnosticSource` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaScanPartitionTargetDiagnosticSource` | Benchmark diagnostic path | #474 compile tests; #475 benchmark docs | exact hidden declaration below |
| `DeltaScanPartitionTargetLocalEnvironmentDiagnostic` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaScanPartitionTargetLocalEnvironmentDiagnostic` | Benchmark host probe | #474 compile tests; #475 benchmark docs | exact hidden declaration below |
| `DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus` | Benchmark host probe | #474 compile tests; #475 benchmark docs | exact hidden declaration below |
| `DeltaTableProviderConfig` | `replace_with_standalone_import` | `delta_arrow_reader::DeltaDataFusionScanOptions` plus `DeltaTable` | Session registry and provider tests | #474 registration shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaTableProviderConfig.html) |
| `QueryOptions` | `delta_funnel_owned_integration` | Remains `delta_funnel::QueryOptions`; owns application session policy. | Rust and Python sessions | #474 session tests; #475 API docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.QueryOptions.html) |
| `RegisteredDeltaSource` | `replace_with_standalone_import` | `delta_arrow_reader::RegisteredDeltaTable` | Session registry, reports, tests | #474 registration shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.RegisteredDeltaSource.html) |
| `RegisteredDeltaSources` | `delta_funnel_owned_integration` | Remains `delta_funnel::RegisteredDeltaSources`; owns all-or-nothing multi-source results over canonical registered tables. | Session registry, reports, tests | #474 registration shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.RegisteredDeltaSources.html) |
| `collect_delta_provider_read_stats` | `replace_with_standalone_import` | `delta_arrow_reader::collect_delta_datafusion_metrics` | Reports, profiles, progress, benchmark | #474 metrics shadow tests; #475 diagnostics docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.collect_delta_provider_read_stats.html) |
| `datafusion_query_output_stream` | `delta_funnel_owned_integration` | Remains `delta_funnel::datafusion_query_output_stream`; owns application output handoff. | Workflows and query handoff | #474 workflow tests; #475 API docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.datafusion_query_output_stream.html) |
| `datafusion_session_config` | `delta_funnel_owned_integration` | Remains `delta_funnel::datafusion_session_config`; owns application session configuration. | Rust and Python sessions | #474 session tests; #475 API docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.datafusion_session_config.html) |
| `datafusion_session_context` | `delta_funnel_owned_integration` | Remains `delta_funnel::datafusion_session_context`; owns application session construction. | Rust and Python sessions | #474 session tests; #475 API docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.datafusion_session_context.html) |
| `delta_scan_partition_target_local_environment_diagnostic` | `replace_with_standalone_import` | `delta_arrow_reader::delta_scan_partition_target_local_environment_diagnostic` | Benchmark host probe | #474 compile tests; #475 benchmark docs | exact hidden declaration below |
| `derive_delta_scan_partition_target_diagnostic` | `replace_with_standalone_import` | `delta_arrow_reader::derive_delta_scan_partition_target_diagnostic` | Benchmark synthetic/host paths | #474 compile tests; #475 benchmark docs | exact hidden declaration below |
| `register_delta_sources` | `delta_funnel_owned_integration` | Remains `delta_funnel::register_delta_sources`; owns all-or-nothing multi-source orchestration over `delta_arrow_reader::register_delta_table`. | Session registry, tests, benchmark | #474 registration shadow tests; #475 migration docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.register_delta_sources.html) |
| `register_delta_sources_with_scan_execution_options` | `delta_funnel_owned_integration` | Remains `delta_funnel::register_delta_sources_with_scan_execution_options`; adapts product options then calls `delta_arrow_reader::register_delta_table`. | Session registry, Python, benchmark | #474 option/registration shadow tests; #475 Rust/Python docs | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.register_delta_sources_with_scan_execution_options.html) |

<!-- public-compatibility:end -->

The diagnostic surface is `#[doc(hidden)]`, so these exact declarations replace unavailable rustdoc pages:

```rust
pub struct DeltaScanPartitionTargetDiagnosticInput {
    pub explicit_target_partitions: Option<usize>,
    pub datafusion_target_partitions: Option<usize>,
    pub available_parallelism: Option<usize>,
    pub available_memory_bytes: Option<u64>,
    pub unix_soft_file_descriptor_limit: Option<u64>,
    pub min_default_partitions: usize,
    pub parallelism_multiplier: usize,
    pub file_descriptors_per_partition: usize,
    pub available_memory_bytes_per_partition: u64,
}

pub struct DeltaScanPartitionTargetDiagnosticOutput {
    pub target_partitions: usize,
    pub source: DeltaScanPartitionTargetDiagnosticSource,
    pub explicit_target_partitions: Option<usize>,
    pub datafusion_target_partitions: Option<usize>,
    pub available_parallelism: Option<usize>,
    pub datafusion_target_cap: Option<usize>,
    pub unix_file_descriptor_cap: Option<usize>,
    pub memory_cap: Option<usize>,
}

pub struct DeltaScanPartitionTargetLocalEnvironmentDiagnostic {
    pub policy_input: DeltaScanPartitionTargetDiagnosticInput,
    pub memory_total_bytes: Option<u64>,
    pub memory_available_bytes: Option<u64>,
    pub unix_soft_file_descriptor_limit: Option<u64>,
    pub unix_soft_file_descriptor_limit_status:
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus,
}

pub enum DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus {
    Unsupported,
    Unknown,
    Finite,
    Unlimited,
}

pub enum DeltaScanPartitionTargetDiagnosticSource {
    ExplicitOverride,
    AvailableParallelismFallback,
    StaticFallback,
}

pub fn derive_delta_scan_partition_target_diagnostic(
    input: DeltaScanPartitionTargetDiagnosticInput,
) -> Result<DeltaScanPartitionTargetDiagnosticOutput, DeltaFunnelError>;

pub fn delta_scan_partition_target_local_environment_diagnostic()
    -> DeltaScanPartitionTargetLocalEnvironmentDiagnostic;
```

## Error and report inventory

### Reader-used DeltaFunnelError variants

The existing `DeltaFunnelError` envelope, display contract, and Python mapping remain Delta Funnel-owned. Rows marked `standalone_error_mapped_at_integration` replace the reader error with its stable standalone category, then add only the listed sanitized Delta Funnel context at a real product boundary.

| Variant | Exact fields | Context ownership after cutover |
| --- | --- | --- |
| `Config` | `message: String` | `delta_funnel_owned` |
| `InvalidSourceName` | `name: String, reason: &'static str` | `delta_funnel_owned` |
| `DuplicateSourceName` | `name: String` | `delta_funnel_owned` |
| `InvalidSourceUri` | `reason: &'static str` | `standalone_error_mapped_at_integration` |
| `DeltaSourceEngine` | `reason: &'static str` | `standalone_error_mapped_at_integration` |
| `DeltaSnapshotLoad` | `reason: String` | `standalone_error_mapped_at_integration` |
| `DeltaProtocolCompatibility` | `source_name: String, table_uri: String, snapshot_version: u64, reason: String` | `standalone_error_mapped_at_integration` |
| `DeltaSourceSchema` | `source_name: String, table_uri: String, reason: String` | `standalone_error_mapped_at_integration` |
| `DataFusionRegistration` | `source_name: String, table_uri: String, reason: String` | `delta_funnel_owned` |
| `DeltaScanProjection` | `source_name: String, table_uri: String, reason: String` | `standalone_error_mapped_at_integration` |
| `DeltaScanFilter` | `source_name: String, table_uri: String, reason: String` | `standalone_error_mapped_at_integration` |
| `DeltaScanConstruction` | `source_name: String, table_uri: String, source: Box<delta_kernel::Error>` | `standalone_error_mapped_at_integration` |
| `DeltaScanMetadataExpansion` | `source_name: String, table_uri: String, snapshot_version: u64, source: Box<delta_kernel::Error>` | `standalone_error_mapped_at_integration` |
| `DeltaScanFileTaskPlanning` | `source_name: String, table_uri: String, snapshot_version: u64, path: String, reason: String` | `standalone_error_mapped_at_integration` |
| `DeltaScanFileTaskPartitionPlanning` | `source_name: String, table_uri: String, snapshot_version: u64, reason: String` | `standalone_error_mapped_at_integration` |
| `DeltaScanFileRead` | `source_name: String, table_uri: String, snapshot_version: u64, path: String, phase: DeltaScanFileReadPhase, source: Box<delta_kernel::Error>` | `standalone_error_mapped_at_integration` |
| `DeltaScanDeletionVector` | `source_name: String, table_uri: String, snapshot_version: u64, path: String, phase: DeltaScanDeletionVectorPhase, source: Box<delta_kernel::Error>` | `standalone_error_mapped_at_integration` |

The two public phase enums used above are:

- [`DeltaScanFileReadPhase`](https://docs.rs/delta-funnel/0.5.0/delta_funnel/error/enum.DeltaScanFileReadPhase.html): `TableUriParsing`, `FileMetadataConversion`, `FilePathResolution`, `ObjectStoreEngineConstruction`, `ParquetReadSetup`, `ParquetBatchRead`, `RowIndexGeneration`, `PredicateEvaluation`, `ArrowConversion`, `TransformApplication`, `UnsupportedReadMode`, `DeletionVectorPredicateRejection`, and `DeletionVectorMasking`.
- [`DeltaScanDeletionVectorPhase`](https://docs.rs/delta-funnel/0.5.0/delta_funnel/error/enum.DeltaScanDeletionVectorPhase.html): `TableUriParsing`, `ObjectStoreEngineConstruction`, `DescriptorAccess`, `PayloadRead`, `SelectionVectorLengthMismatch`, and `SelectionVectorExhaustion`.

### Report and JSON fields

A `DeltaSourceReport` serializes reader information at `sources[]`. An exact execution profile embeds the same provider snapshot at `operators[].delta_provider_read_stats`.

Protocol fields:

```text
source_name
table_uri
snapshot_version
min_reader_version
min_writer_version
reader_features
writer_features
```

Scheduling fields:

<!-- scheduling-json-fields:start -->
```text
query_target_partitions
reader_backend
max_concurrent_file_reads_per_scan
max_concurrent_file_reads_per_partition
output_buffer_capacity_per_partition
native_async_prefetch_file_count_per_partition
parquet_metadata_size_hint
parquet_full_file_read_threshold
```
<!-- scheduling-json-fields:end -->

Provider read-stat fields:

```text
source_name
snapshot_version
reader_backend
scan_metadata_exhausted
scan_partitions_planned
files_planned
approximate_files_filtered_during_planning
estimated_rows
estimated_bytes
parquet_data_file_range_get_operations
parquet_data_file_full_get_operations
parquet_data_file_bytes_received
parquet_data_file_opened_bytes
datafusion_output_batch_size
scan_partitions_started
scan_partitions_completed
files_started
files_completed
dynamic_partition_files_pruned
dynamic_partition_files_kept
dynamic_filters_received
dynamic_filters_accepted
dynamic_filters_unsupported
dynamic_filter_snapshots
dynamic_partition_files_not_pruned_missing_metadata
dynamic_partition_files_not_pruned_unsupported_expression
batches_produced
rows_produced
deletion_vector_payloads_loaded
deletion_vectors_applied
deletion_vector_rows_deleted
deletion_vector_failures
deletion_vector_rejections
```

The terminal `delta_provider_parquet_io_summary` event authorizes only `source_name` after sanitization, `snapshot_version`, `reader_backend`, `outcome`, `metrics_available`, and the four Parquet counters when available. Reports additionally authorize the validated source name, sanitized URI, protocol versions/features, numeric scheduling/read metrics, usage state, reason codes, and phase names/status. They do not authorize storage options, credentials, URI userinfo/query/fragment, file paths, object paths, concrete ranges, headers, SQL, row values, or dependency error text.

Existing focused redaction coverage includes `error.rs` reader error display tests; `source_load_errors_do_not_expose_secret_bearing_uri`; `snapshot_load_cause_redacts_secret_bearing_uris`; `snapshot_errors_do_not_expose_secret_bearing_uri`; `protocol_report_sanitizes_uri_context`; `compatibility_error_display_redacts_uri_credentials`; `provider_io_summary_sanitizes_hostile_source_names`; JSON `assert_no_secret_or_raw_sql_text`; execution-profile provider-stat redaction tests; and Python source repr/progress tests.

### Rust and Python execution options

Rust-only backend selection defaults to `DeltaProviderReaderBackend::NativeAsync`. `OfficialKernel` remains available through Rust. Python `ProviderScanOptions` does not expose a backend selector.

<!-- provider-scan-options:start -->

| Python key | Rust field and default | Accepted Python value | Validation and meaning |
| --- | --- | --- | --- |
| `max_concurrent_file_reads_per_scan` | same field, `None` | `int`; omission preserves `None` | Positive when supplied; `None` means derive scan-wide capacity. |
| `max_concurrent_file_reads_per_partition` | same field, `3` | `int` | Positive. |
| `output_buffer_capacity_per_partition` | same field, `1` | `int` | Positive. |
| `native_async_prefetch_file_count_per_partition` | same field, `2` | `int` | Non-negative; zero disables prefetch. |
| `parquet_metadata_size_hint` | same field, `Some(65_536)` | `int \| None` | Positive when present; `None` disables metadata prefetch. |
| `parquet_full_file_read_threshold` | same field, `None` | `int \| None` | Positive when present; `None` disables full-file buffering. |

<!-- provider-scan-options:end -->

Python rejects booleans as integers, negative or oversized integers during `usize` extraction, zero for every positive-only field, and unknown keys before value conversion. Each Python key maps directly to the same Rust field, then the complete Rust `validate()` method runs. Both Parquet controls appear once in the Python type stub and once in scheduling JSON.

## Dependency and feature baseline

Workspace edition is `2024`; MSRV is `1.88`. The lockfile-resolved workspace feature union at capture is:

```text
arrow 58.3.0: arrow-csv,arrow-ipc,arrow-json,canonical_extension_types,chrono-tz,csv,default,ffi,ipc,json,prettyprint
arrow-schema 58.3.0: bitflags,canonical_extension_types,ffi
parquet 58.3.0: arrow,arrow-array,arrow-buffer,arrow-data,arrow-ipc,arrow-schema,arrow-select,async,base64,brotli,default,flate2,flate2-zlib-rs,futures,lz4,lz4_flex,object_store,simdutf8,snap,tokio,zstd
datafusion 54.1.0: datafusion-sql,sql,sqlparser
object_store 0.13.2: aws,azure,base64,cloud,default,form_urlencoded,fs,gcp,http,http-body-util,httparse,hyper,md-5,quick-xml,rand,reqwest,ring,rustls-pki-types,serde,serde_json,serde_urlencoded,tokio,walkdir
tokio 1.52.3: bytes,default,fs,io-util,libc,macros,mio,net,rt,rt-multi-thread,socket2,sync,time,tokio-macros,windows-sys
delta_kernel 0.25.0: arrow,arrow-58,arrow-conversion,arrow-expression,default,default-engine-base,internal-api,need-arrow,reqwest
delta_kernel_default_engine 0.25.0: arrow,arrow-58,native-tls
futures 0.3.32: alloc,async-await,default,executor,futures-executor,std
futures-util 0.3.32: alloc,async-await,async-await-macro,channel,default,futures-channel,futures-io,futures-macro,futures-sink,io,memchr,sink,slab,std
tracing 0.1.44: attributes,default,log,std,tracing-attributes
```

The direct manifest disables defaults for DataFusion and selects `sql`; disables defaults for `delta_kernel` and selects `arrow,internal-api`; disables defaults for `delta_kernel_default_engine` and selects `arrow,native-tls`; enables Parquet `async,object_store`; and enables Tokio `net,rt-multi-thread,sync,time`. The default engine's `native-tls` path resolves `native-tls 0.2.18` and `hyper-tls 0.6.0`. `rustls 0.23.40` is also present through the object-store dependency graph, not through a Delta Kernel rustls feature.

The accepted #560 composition is one private `Arc<DeltaKernelEngineContext>` per loaded source. It owns the normalized table URL, one shared `Arc<dyn ObjectStore>`, and one `Arc<dyn delta_kernel::Engine + Send + Sync>` built with `DefaultEngineBuilder`. The loaded snapshot, scan planning, OfficialKernel reader, NativeAsync reader, and deletion-vector reader share that context; none reconstructs credentials, object stores, or engines. Source loading is the single construction-failure boundary.

Published default-engine handlers own snapshot, JSON/checkpoint, scan-metadata, schema/protocol, Kernel evaluation, and OfficialKernel data-file behavior. NativeAsync alone performs direct Parquet reads over a clone of the same base store. Delta Funnel neither copies nor replaces upstream handlers or the upstream-selected default executor. The context stays alive through every snapshot, plan, reader, evaluator, deletion-vector operation, and stream reference, then drops with the last `Arc`; there is no global, static, thread-local, URI cache, or process-lifetime owner.

NativeAsync uses the caller-owned DataFusion/Tokio runtime and async Parquet/object-store futures. OfficialKernel iteration runs on DataFusion's bounded `RecordBatchReceiverStreamBuilder::spawn_blocking` boundary. The reader creates no Tokio runtime, calls no `Runtime::block_on`, adds no custom executor, and has no global engine cache.

The compact dependency proof commands are:

```console
cargo tree -p delta-funnel --edges normal --depth 1
cargo metadata --format-version 1 --locked
cargo tree -p delta-funnel -d
```

The resolved graph contains one compatibility-critical universe: Arrow/Parquet `58.3.0`, DataFusion `54.1.0`, object_store `0.13.2`, and Delta Kernel/default-engine `0.25.0`. No duplicate of those packages exists. Unrelated transitive packages do not cross the public Arrow/DataFusion/object-store boundary.

## Correctness baseline

The production tree under test is the frozen source commit. This baseline branch changes only this document and its verifier. Focused results captured on `2026-08-06` UTC were:

| Command | Result | Coverage |
| --- | --- | --- |
| `cargo test --locked -p delta-funnel --all-features table_formats::delta` | pass: 176, fail: 0 | Snapshot/protocol/schema, projection/filter/pruning, deletion vectors, storage, and reader redaction. |
| `cargo test --locked -p delta-funnel --all-features query_engine::datafusion` | pass: 635, fail: 0 | Provider registration/execution, partition targets, longest-file-first grouping, scheduling, both Parquet controls, cancellation/backpressure, and DataFusion public behavior. |
| `cargo test --locked -p delta-funnel --all-features report` | pass: 201, fail: 0, ignored: 3 opt-in external trace tests | #448/#449 metrics, JSON, terminal summaries, source reports, and #612 exact profiles. |
| `cargo test --locked -p delta-funnel --all-features perfetto_profile` | pass: 81, fail: 0, ignored: 3 opt-in external trace tests | Current #545 ranked artifact, report, terminal, sanitization, and bounded-output behavior. |
| `cargo test --locked -p delta-funnel --all-features --bin delta_scan_partition_bench` | pass: 71, fail: 0 | Benchmark parsing, fixture generation, CSV schema, storage profiles, and summary math. |
| `cargo test --locked -p delta-funnel-python --all-features` | pass: 192, fail: 0, ignored: 9 SQL Server integration tests owned by `cargo xtask sqlserver-test` | Python option parsing/defaults/validation, API stubs, reports, progress, and redaction. |

The normal repository gates also passed:

```console
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
git diff --check
```

The full workspace test result was 1,471 Delta Funnel library tests passed with three documented opt-in external-trace tests ignored; 71 benchmark tests, 21 Rust integration tests, 192 Python binding tests, nine xtask tests, and one doctest passed. Nine Python SQL Server tests remained explicitly ignored because their owning command requires the external SQL Server lane. There were no failures and no unexplained accepted failures.

## Controlled performance and I/O baseline

### Supported diagnostic capture

The release binary was built with `Cargo.lock` SHA-256 `be88d1fac0c3ac6ee221138c702c0bd10b0a9e21cc597819ea95968099948a8b`. Capture used benchmark CSV schema 22, seed 0, two warm-up repetitions, five measured repetitions, and scheduling profile `prefetch_2_ap_target_scan_3x`: 16 scan partitions, scan-wide capacity 48, per-partition capacity 3, output buffer 1, and NativeAsync file prefetch 2. OfficialKernel ignores both Parquet controls and reports its four I/O counters as unavailable. Capture completed on an otherwise idle host at `2026-08-06T15:38:27Z` UTC.

The clean-checkout command pattern is:

```bash
git checkout --detach e2650427ff5e2e1a7a4e5ef9eaf30969b217fec3
cargo build --locked --release -p delta-funnel --bin delta_scan_partition_bench
run_case() {
  name=$1 workload=$2 query=$3 backend=$4 storage=$5
  metadata_hint=$6 full_read_threshold=$7
  for repetitions in 2 5; do
    target/release/delta_scan_partition_bench \
      --mode provider-exec --seed 0 \
      --provider-exec-storage-profile "$storage" \
      --provider-exec-workload "$workload" \
      --provider-exec-query "$query" \
      --provider-exec-backend "$backend" \
      --provider-exec-scheduling-profile prefetch_2_ap_target_scan_3x \
      --provider-exec-parquet-metadata-size-hint "$metadata_hint" \
      --provider-exec-parquet-full-file-read-threshold "$full_read_threshold" \
      --provider-exec-repetitions "$repetitions" \
      --output "target/issue-459-${name}-${repetitions}.csv"
  done
}
run_case local-native-full provider_few_larger_files full_rows native_async local 65536 disabled
run_case local-native-projection provider_few_larger_files project_id native_async local 65536 disabled
run_case local-official-full provider_few_larger_files full_rows official_kernel local 65536 disabled
run_case local-native-pruned-unequal provider_many_unequal_files filter_tail_ids native_async local 65536 disabled
run_case local-native-many-small provider_many_small_files project_id native_async local 65536 disabled
run_case local-native-metadata-disabled provider_few_larger_files full_rows native_async local disabled disabled
run_case local-native-metadata-undersized provider_few_larger_files full_rows native_async local 8 disabled
run_case local-native-full-read-eligible provider_few_larger_files full_rows native_async local 65536 1000000
run_case local-native-full-read-ineligible provider_few_larger_files full_rows native_async local 65536 1000
run_case local-native-dv provider_few_larger_files_sparse_dv project_id native_async local 65536 disabled
run_case local-official-dv provider_few_larger_files_sparse_dv project_id official_kernel local 65536 disabled
run_case throttled-native-full provider_few_larger_files full_rows native_async s3_throttled 65536 disabled
```

For each call, repetition count 2 is warm-up and count 5 is measured. Fixture throughput is `data_file_bytes / total_micros_p50`; it is a stable whole-fixture diagnostic, not observed object-store traffic. Source rows per second and every deterministic observation below come directly from the schema-22 measured row. Slash-separated columns are ordered exactly as named.

<!-- controlled-benchmark-results:start -->
| Case | p50 us | Fixture MiB/s | Source rows/s | Files planned / started | Rows / batches | Metadata hint / full threshold | Range / full GETs | Bytes received / opened | DV loaded / applied / deleted |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `local-native-dv` | 1349 | 579.54 | 24290585 | 4 / 4 | 32756 / 4 | 65536 / disabled | 8 / 0 | 446708 / 819772 | 4 / 4 / 12 |
| `local-native-full` | 913 | 856.29 | 35890470 | 4 / 4 | 32768 / 4 | 65536 / disabled | 8 / 0 | 1078950 / 819772 | 0 / 0 / 0 |
| `local-native-full-read-eligible` | 811 | 963.99 | 40404438 | 4 / 4 | 32768 / 4 | 65536 / 1000000 | 0 / 4 | 819772 / 819772 | 0 / 0 / 0 |
| `local-native-full-read-ineligible` | 916 | 853.49 | 35772925 | 4 / 4 | 32768 / 4 | 65536 / 1000 | 8 / 0 | 1078950 / 819772 | 0 / 0 / 0 |
| `local-native-many-small` | 1853 | 121.71 | 4420939 | 64 / 64 | 8192 / 64 | 65536 / disabled | 128 / 0 | 278921 / 236489 | 0 / 0 / 0 |
| `local-native-metadata-disabled` | 911 | 858.17 | 35969264 | 4 / 4 | 32768 / 4 | disabled / disabled | 12 / 0 | 819343 / 819772 | 0 / 0 / 0 |
| `local-native-metadata-undersized` | 983 | 795.32 | 33334689 | 4 / 4 | 32768 / 4 | 8 / disabled | 12 / 0 | 819343 / 819772 | 0 / 0 / 0 |
| `local-native-projection` | 725 | 1078.34 | 45197241 | 4 / 4 | 32768 / 4 | 65536 / disabled | 8 / 0 | 446708 / 819772 | 0 / 0 / 0 |
| `local-native-pruned-unequal` | 1440 | 1235.69 | 50488888 | 32 / 32 | 68608 / 32 | 65536 / disabled | 64 / 0 | 1001608 / 1748159 | 0 / 0 / 0 |
| `local-official-dv` | 970 | 805.97 | 33781443 | 4 / 4 | 32756 / 36 | 65536 / disabled | unavailable / unavailable | unavailable / unavailable | 4 / 4 / 12 |
| `local-official-full` | 1395 | 560.43 | 23489605 | 4 / 4 | 32768 / 36 | 65536 / disabled | unavailable / unavailable | unavailable / unavailable | 0 / 0 / 0 |
| `throttled-native-full` | 97793 | 7.99 | 335075 | 4 / 4 | 32768 / 4 | 65536 / disabled | 8 / 0 | 1078950 / 819772 | 0 / 0 / 0 |
<!-- controlled-benchmark-results:end -->

The full and projection rows separate column materialization cost. The unequal fixture places 4,096 rows in its first 32 small files; `filter_tail_ids` excludes those files during planning, then schedules 32 retained files containing eight 8,192-row files and 24 128-row files through the accepted longest-file-first/lightest-partition algorithm. Metadata prefetch default, disabled, and 8-byte undersized modes produce 8, 12, and 12 range GETs respectively. A 1,000,000-byte full-file threshold admits all four larger files and produces four full GETs with no range GETs; a 1,000-byte threshold admits none. The local NativeAsync rows retain metered request and byte observations, while the throttled row exercises the delayed localhost object-store profile.

### Fixture provenance and portability gate

The provider-exec fixtures are generated locally by `crates/delta-funnel/src/bin/delta_scan_partition_bench.rs`, frozen blob `52284e22a5deb0cddce4aa7257012468dc88f25e`, using Arrow/Parquet 58.3.0 from the locked graph. Their source and generated output are covered by the repository's Apache-2.0 license. They contain deterministic synthetic values and no external or private data.

<!-- benchmark-fixture-fingerprints:start -->
| Fixture | Shape | Canonical content fingerprint |
| --- | --- | --- |
| `provider_few_larger_files` | 4 files by 8,192 rows | `fnv1a64:a3f6509701b2a6fc` |
| `provider_many_small_files` | 64 files by 128 rows | `fnv1a64:05a1a9efa301e8be` |
| `provider_many_unequal_files` | 32 initial 128-row files, then 8 8,192-row and 24 128-row files | `fnv1a64:e29235befe1d61e3` |
| `provider_few_larger_files_sparse_dv` | 4 files by 8,192 rows with three deleted indexes per file | `fnv1a64:e1509da31486f25a` |
<!-- benchmark-fixture-fingerprints:end -->

The fingerprint is FNV-1a 64 over each sorted repository-relative generated path, path length, file size, and file bytes. Every warm-up and measured generation reproduced the same value for its fixture. `--provider-exec-retain-fixtures` keeps a generated Delta table beneath the selected temporary root when an extraction child needs to copy or independently inspect it; the default still removes it. The standalone DataFusion fixture path is to copy this Apache-2.0 deterministic recipe without a production dependency and require the same fingerprints before accepting post-extraction comparisons. This clean-checkout recipe, portable ownership, and repeated identical output pass the fixture gate without committing generated data.

### Resolved frozen-harness gaps

PR #640 resolved the five previously blocking harness gaps in the single frozen source:

1. `full_rows` provides a full-column provider scan.
2. Both Parquet controls accept explicit positive byte values or `disabled`.
3. Schema-22 CSV records the exact settings and all four #448 request/byte counters.
4. `provider_many_unequal_files` provides a pruned, size-skewed scheduling case.
5. Every generated comparison fixture emits a stable canonical content fingerprint and supports opt-in retention.

The controlled performance and fixture portability gates are complete at the source SHA above. Later extraction work must compare against this one frozen baseline and must not substitute a newer benchmark or source commit.

### Committed-artifact redaction

Only this compact document and its verifier are committed. Raw CSV, trace, build, and test logs remain uncommitted under `target/`. The verifier rejects a home-directory path, credential-bearing URI, common secret assignment, or private-key marker. The committed artifact contains no username, private hostname, environment value, credential, table URI, object-store path, connection string, SQL literal, row value, or raw dependency error.
