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

## Public compatibility inventory

The published 0.5.0 rustdoc is an exact signature reference because `delta-funnel-v0.5.0` and the frozen source commit have identical trees. #474 requires existing Delta Funnel public definitions to remain the source-compatible facade. Therefore no row requires a breaking change and no row uses `reexport_standalone_type`.

<!-- public-compatibility:start -->

| Crate-root item | Required cutover treatment | Exact 0.5.0 signature source |
| --- | --- | --- |
| `DeltaFunnelError` | `delta_funnel_owned_unchanged` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/error/enum.DeltaFunnelError.html) |
| `DeltaProtocolReport` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaProtocolReport.html) |
| `DeltaProviderSchedulingReport` | `delta_funnel_owned_unchanged` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaProviderSchedulingReport.html) |
| `DeltaSourceReport` | `delta_funnel_owned_unchanged` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaSourceReport.html) |
| `SourceUsageStatus` | `delta_funnel_owned_unchanged` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/enum.SourceUsageStatus.html) |
| `DeltaSourceConfig` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaSourceConfig.html) |
| `DeltaStorageOptions` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/type.DeltaStorageOptions.html) |
| `PlannedDeltaSource` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.PlannedDeltaSource.html) |
| `ProtocolPreflight` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.ProtocolPreflight.html) |
| `load_delta_source` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.load_delta_source.html) |
| `load_delta_sources` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.load_delta_sources.html) |
| `load_delta_source_with_tracing` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.load_delta_source_with_tracing.html) |
| `preflight_delta_protocol` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.preflight_delta_protocol.html) |
| `preflight_delta_sources` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.preflight_delta_sources.html) |
| `preflight_delta_protocol_with_tracing` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.preflight_delta_protocol_with_tracing.html) |
| `DeltaProviderReadStatsSnapshot` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaProviderReadStatsSnapshot.html) |
| `DeltaProviderReaderBackend` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/enum.DeltaProviderReaderBackend.html) |
| `DeltaProviderScanExecutionOptions` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaProviderScanExecutionOptions.html) |
| `DeltaScanPartitionTargetDiagnosticInput` | `delta_funnel_compatibility_wrapper` | exact hidden declaration below |
| `DeltaScanPartitionTargetDiagnosticOutput` | `delta_funnel_compatibility_wrapper` | exact hidden declaration below |
| `DeltaScanPartitionTargetDiagnosticSource` | `delta_funnel_compatibility_wrapper` | exact hidden declaration below |
| `DeltaScanPartitionTargetLocalEnvironmentDiagnostic` | `delta_funnel_compatibility_wrapper` | exact hidden declaration below |
| `DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus` | `delta_funnel_compatibility_wrapper` | exact hidden declaration below |
| `DeltaTableProviderConfig` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.DeltaTableProviderConfig.html) |
| `QueryOptions` | `delta_funnel_owned_unchanged` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.QueryOptions.html) |
| `RegisteredDeltaSource` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.RegisteredDeltaSource.html) |
| `RegisteredDeltaSources` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/struct.RegisteredDeltaSources.html) |
| `collect_delta_provider_read_stats` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.collect_delta_provider_read_stats.html) |
| `datafusion_query_output_stream` | `delta_funnel_owned_unchanged` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.datafusion_query_output_stream.html) |
| `datafusion_session_config` | `delta_funnel_owned_unchanged` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.datafusion_session_config.html) |
| `datafusion_session_context` | `delta_funnel_owned_unchanged` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.datafusion_session_context.html) |
| `delta_scan_partition_target_local_environment_diagnostic` | `delta_funnel_compatibility_wrapper` | exact hidden declaration below |
| `derive_delta_scan_partition_target_diagnostic` | `delta_funnel_compatibility_wrapper` | exact hidden declaration below |
| `register_delta_sources` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.register_delta_sources.html) |
| `register_delta_sources_with_scan_execution_options` | `delta_funnel_compatibility_wrapper` | [rustdoc](https://docs.rs/delta-funnel/0.5.0/delta_funnel/fn.register_delta_sources_with_scan_execution_options.html) |

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

The existing `DeltaFunnelError` envelope, display contract, and Python mapping remain Delta Funnel-owned. Rows marked `reader_context_in_delta_funnel_wrapper` preserve the listed context while mapping the standalone error category through #474.

| Variant | Exact fields | Context ownership after cutover |
| --- | --- | --- |
| `Config` | `message: String` | `delta_funnel_owned` |
| `InvalidSourceName` | `name: String, reason: &'static str` | `delta_funnel_owned` |
| `DuplicateSourceName` | `name: String` | `delta_funnel_owned` |
| `InvalidSourceUri` | `reason: &'static str` | `reader_context_in_delta_funnel_wrapper` |
| `DeltaSourceEngine` | `reason: &'static str` | `reader_context_in_delta_funnel_wrapper` |
| `DeltaSnapshotLoad` | `reason: String` | `reader_context_in_delta_funnel_wrapper` |
| `DeltaProtocolCompatibility` | `source_name: String, table_uri: String, snapshot_version: u64, reason: String` | `reader_context_in_delta_funnel_wrapper` |
| `DeltaSourceSchema` | `source_name: String, table_uri: String, reason: String` | `reader_context_in_delta_funnel_wrapper` |
| `DataFusionRegistration` | `source_name: String, table_uri: String, reason: String` | `delta_funnel_owned` |
| `DeltaScanProjection` | `source_name: String, table_uri: String, reason: String` | `reader_context_in_delta_funnel_wrapper` |
| `DeltaScanFilter` | `source_name: String, table_uri: String, reason: String` | `reader_context_in_delta_funnel_wrapper` |
| `DeltaScanConstruction` | `source_name: String, table_uri: String, source: Box<delta_kernel::Error>` | `reader_context_in_delta_funnel_wrapper` |
| `DeltaScanMetadataExpansion` | `source_name: String, table_uri: String, snapshot_version: u64, source: Box<delta_kernel::Error>` | `reader_context_in_delta_funnel_wrapper` |
| `DeltaScanFileTaskPlanning` | `source_name: String, table_uri: String, snapshot_version: u64, path: String, reason: String` | `reader_context_in_delta_funnel_wrapper` |
| `DeltaScanFileTaskPartitionPlanning` | `source_name: String, table_uri: String, snapshot_version: u64, reason: String` | `reader_context_in_delta_funnel_wrapper` |
| `DeltaScanFileRead` | `source_name: String, table_uri: String, snapshot_version: u64, path: String, phase: DeltaScanFileReadPhase, source: Box<delta_kernel::Error>` | `reader_context_in_delta_funnel_wrapper` |
| `DeltaScanDeletionVector` | `source_name: String, table_uri: String, snapshot_version: u64, path: String, phase: DeltaScanDeletionVectorPhase, source: Box<delta_kernel::Error>` | `reader_context_in_delta_funnel_wrapper` |

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
