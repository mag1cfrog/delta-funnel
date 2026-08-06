# Delta Lake-to-Arrow reader extraction baseline

This is the evidence artifact required by [issue #459](https://github.com/mag1cfrog/delta-funnel/issues/459) in the [#447 extraction family](https://github.com/mag1cfrog/delta-funnel/issues/447). The linked issues remain the requirements authority. This document records evidence only.

## Source identity

- Source commit: `46f23d8fbec7effe3806bf7507a0b43b91a594ec` from Delta Funnel's default `main` branch.
- Package identity: workspace package `delta-funnel` version `0.5.0`.
- Release identity: tag `delta-funnel-v0.5.0` at `29c7cabf02451fbfa330f9db95eb1f9fc65c8f2e`; the source commit is the immediately following release-PR merge commit.
- Required reader changes: the source commit contains #631 (`a3060716`), #633 (`dfed15da`), #635 (`d4b55edc`), and #639 (`d57775fb`).
- Inventory capture: `2026-08-06T11:04:33Z` UTC.
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
89a65aa4c5dc5fa12715c89e157ed8102e43a49b 8995 delta_funnel_only crates/delta-funnel/src/bin/delta_scan_partition_bench.rs
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

Inventory digest (SHA-256 of sorted `git_blob repository_path` lines): `a9c7f6fff06cc2f1a4b32589ff46cd531a411d969e6dea47fc0f6f075a296866`.

Reproduce this section with:

```console
scripts/verify-delta-arrow-reader-extraction-baseline.sh
```
