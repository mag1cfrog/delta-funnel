# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.0](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.5.0...delta-funnel-v0.6.0) - 2026-08-12

### Added

- configure Delta file repartitioning from Python ([#670](https://github.com/mag1cfrog/delta-funnel/pull/670))
- configure Delta provider view types ([#669](https://github.com/mag1cfrog/delta-funnel/pull/669))
- add typed provider scan options ([#671](https://github.com/mag1cfrog/delta-funnel/pull/671))

### Other

- bump delta arrow reader to 0.1.3 ([#668](https://github.com/mag1cfrog/delta-funnel/pull/668))
- remove the internal Delta reader
- cut over to delta arrow reader ([#665](https://github.com/mag1cfrog/delta-funnel/pull/665))
- prepare published reader migration ([#664](https://github.com/mag1cfrog/delta-funnel/pull/664))
- complete issue 459 benchmark prerequisites ([#640](https://github.com/mag1cfrog/delta-funnel/pull/640))

### Added

- expose the Delta reader view-type policy through Rust and Python session options, with ordinary Arrow string and binary arrays as the Delta Funnel default

## [0.5.0](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.4.1...delta-funnel-v0.5.0) - 2026-08-05

### Added

- expose bounded ASCII varchar output policy in Python ([#638](https://github.com/mag1cfrog/delta-funnel/pull/638))
- support explicit dependency cache chains for multi-output writes ([#632](https://github.com/mag1cfrog/delta-funnel/pull/632))
- add Parquet metadata size hint ([#631](https://github.com/mag1cfrog/delta-funnel/pull/631))
- make ranked profile activity limit configurable ([#627](https://github.com/mag1cfrog/delta-funnel/pull/627))
- expose stream-only write-all benchmark ([#626](https://github.com/mag1cfrog/delta-funnel/pull/626))

### Fixed

- validate cached replay schemas against output plans ([#630](https://github.com/mag1cfrog/delta-funnel/pull/630))
- improve replanned schema mismatch diagnostics
- authorize whole-workload Perfetto captures under Yama ([#625](https://github.com/mag1cfrog/delta-funnel/pull/625))
- harden scoped Yama capture startup ([#624](https://github.com/mag1cfrog/delta-funnel/pull/624))
- authorize operation-scoped Perfetto captures under Yama ([#622](https://github.com/mag1cfrog/delta-funnel/pull/622))

### Other

- reduce small Parquet read latency with optional full-file buffering ([#639](https://github.com/mag1cfrog/delta-funnel/pull/639))
- speed up SQL Server replacement loads with safe bulk-load table locking ([#637](https://github.com/mag1cfrog/delta-funnel/pull/637))
- use arrow-sql-server dependency ([#636](https://github.com/mag1cfrog/delta-funnel/pull/636))
- reduce Parquet read latency with default metadata prefetch ([#635](https://github.com/mag1cfrog/delta-funnel/pull/635))
- balance Delta scan file partitions ([#633](https://github.com/mag1cfrog/delta-funnel/pull/633))

### Added

- configure the per-operation ranked-profile activity span limit from Python
- scale the ranked-profile semantic buffer for higher activity span limits

### Changed

- [**breaking**] replace the `arrow-tiberius` dependency with `arrow-sql-server`; Rust integrations and custom tracing filters that name the dependency should use `arrow_sql_server` instead of `arrow_tiberius`

### Fixed

- authorize operation-scoped diagnostics captures on Linux hosts using Yama restricted ptrace

## [0.4.1](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.4.0...delta-funnel-v0.4.1) - 2026-07-28

### Added

- export reusable ranked profile artifacts while generating trace reports

### Fixed

- report unknown Python provider scan options accurately

### Other

- inspect reusable ranked profile artifacts without reprocessing raw traces

## [0.4.0](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.3.3...delta-funnel-v0.4.0) - 2026-07-28

### Added

- add opt-in ranked native CPU profiling to diagnostics-enabled Linux builds for Python previews, SQL Server writes, and `write_all`, with self-contained interactive HTML reports and optional reusable `.dfprofile` artifacts ([#581](https://github.com/mag1cfrog/delta-funnel/pull/581), [#606](https://github.com/mag1cfrog/delta-funnel/pull/606))
- add `delta-funnel-perfetto` commands for terminal inspection, HTML generation, and bounded 100 or 1000 Hz whole-process capture with health checks and native symbolization ([#532](https://github.com/mag1cfrog/delta-funnel/pull/532), [#535](https://github.com/mag1cfrog/delta-funnel/pull/535), [#566](https://github.com/mag1cfrog/delta-funnel/pull/566), [#595](https://github.com/mag1cfrog/delta-funnel/pull/595))
- attribute sampled native call trees to exact semantic phases across DataFusion, Delta Lake, S3, and SQL Server work, with readable call-chain compaction and explicit incomplete, ambiguous, unresolved, and missing-sample states ([#545](https://github.com/mag1cfrog/delta-funnel/issues/545))

### Changed

- [**breaking**] replace Python `profile=True` and `write_all(options={"profile": True})` with the top-level `execution_profile=True` option for returned DataFusion operator metrics; remove `Preview.export_trace()` and use `RankedProfileConfig` for interactive diagnostics instead ([#611](https://github.com/mag1cfrog/delta-funnel/pull/611), [#613](https://github.com/mag1cfrog/delta-funnel/pull/613))
- [**breaking**] remove Rust `QueryExecutionProfile::to_trace_event_json_value()` and the legacy handcrafted Chrome Trace timeline export; exact execution profiles now remain focused on typed operator metrics ([#613](https://github.com/mag1cfrog/delta-funnel/pull/613))

### Fixed

- return a structured `DeltaFunnelError` instead of `PanicException` when concurrent Python code attempts unsupported `Session` catalog mutation ([#580](https://github.com/mag1cfrog/delta-funnel/pull/580))

## [0.3.3](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.3.2...delta-funnel-v0.3.3) - 2026-07-16

### Added

- export detailed preview execution profiles for interactive analysis in VizTracer and Perfetto ([#506](https://github.com/mag1cfrog/delta-funnel/pull/506))

## [0.3.2](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.3.1...delta-funnel-v0.3.2) - 2026-07-16

### Fixed

- prevent preview failures when Delta queries filter on columns not included in the result ([#504](https://github.com/mag1cfrog/delta-funnel/pull/504))

## [0.3.1](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.3.0...delta-funnel-v0.3.1) - 2026-07-15

### Added

- support reading Delta tables with v2 checkpoints, type widening, and vacuum protocol checks ([#500](https://github.com/mag1cfrog/delta-funnel/pull/500))
- improve Python progress displays with animated spinners for work without known totals and compact bars for measurable progress ([#502](https://github.com/mag1cfrog/delta-funnel/pull/502))

### Fixed

- show millisecond-precision progress timing ([#503](https://github.com/mag1cfrog/delta-funnel/pull/503))

## [0.3.0](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.2.1...delta-funnel-v0.3.0) - 2026-07-15

### Added

- report detailed DataFusion execution profiles for write_all cache alias materialization ([#498](https://github.com/mag1cfrog/delta-funnel/pull/498))
- expose complete cache alias lifecycle timings and structured failure diagnostics for SQL Server write_all ([#497](https://github.com/mag1cfrog/delta-funnel/pull/497))
- add opt-in per-output DataFusion profiling for SQL Server write-all workflows in Rust and Python ([#496](https://github.com/mag1cfrog/delta-funnel/pull/496))
- add end-to-end SQL Server output profiling with query phase timings, DataFusion operator metrics, and Python support ([#495](https://github.com/mag1cfrog/delta-funnel/pull/495))
- add end-to-end preview phase timings, optional DataFusion operator profiles, and structured failure diagnostics for Rust and Python ([#494](https://github.com/mag1cfrog/delta-funnel/pull/494))
- emit terminal query execution profiles with DataFusion operator metrics and Delta I/O statistics for successful, failed, and cancelled workflows ([#493](https://github.com/mag1cfrog/delta-funnel/pull/493))
- add deterministic DataFusion execution profiles with redacted operator metrics and Delta read statistics ([#491](https://github.com/mag1cfrog/delta-funnel/pull/491))
- add terminal Parquet I/O tracing summaries for successful, failed, and cancelled Delta provider scans ([#490](https://github.com/mag1cfrog/delta-funnel/pull/490))
- report actual Parquet requests and bytes read by the NativeAsync Delta reader ([#489](https://github.com/mag1cfrog/delta-funnel/pull/489))
- show live progress while Delta Lake sources are being registered
- show live query execution and Delta file progress for Python table previews
- show consolidated progress across planning, shared caching, and SQL Server writes for multi-output workflows
- show live Delta file scanning, pruning, and SQL Server write progress in Python terminals and notebooks
- add typed lifecycle progress reporting for single SQL Server writes and dry runs
- add Python table preview and notebook display for lazy queries
- add a phase-aligned wide export benchmark for provider execution
- show automatic terminal and Jupyter progress for Python SQL Server writes and dry runs

### Other

- finish profiling family closeout ([#499](https://github.com/mag1cfrog/delta-funnel/pull/499))
- Docs/431 reorganize site navigation ([#488](https://github.com/mag1cfrog/delta-funnel/pull/488))
- Feat/431 simplify write all progress ([#487](https://github.com/mag1cfrog/delta-funnel/pull/487))
- validate interactive Python progress across terminal and notebook workflows ([#456](https://github.com/mag1cfrog/delta-funnel/pull/456))
- replace stale crate overview with complete Rust quickstart ([#430](https://github.com/mag1cfrog/delta-funnel/pull/430))
- remove documentation coupling from Rust workspace tests ([#429](https://github.com/mag1cfrog/delta-funnel/pull/429))
- make README and docs homepage friendlier with preview-first quickstarts
- improve the public landing pages with clearer Delta Lake to SQL Server positioning
- modernize PyO3 extension module builds ([#492](https://github.com/mag1cfrog/delta-funnel/pull/492))

## [0.2.1](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.2.0...delta-funnel-v0.2.1) - 2026-07-07

### Added

- add a default provider-exec benchmark shortcut with JSONL phase tracing

## [0.2.0](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.6...delta-funnel-v0.2.0) - 2026-07-07

### Added

- replace SQL Server write options with direct backend selection

### Fixed

- match SQL Server datetime rounding when writing timestamp columns as datetime
- support non-nullable Timestamp(ns) columns mapped to SQL Server datetime
- keep partition filters out of native row predicates ([#413](https://github.com/mag1cfrog/delta-funnel/pull/413))

### Other

- update arrow-tiberius adapter to 0.2.0 ([#416](https://github.com/mag1cfrog/delta-funnel/pull/416))

## [0.1.6](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.5...delta-funnel-v0.1.6) - 2026-07-04

### Fixed

- expose SQL Server timestamp policy

## [0.1.5](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.4...delta-funnel-v0.1.5) - 2026-07-03

### Fixed

- allow SQL Server replace writes to create missing targets

## [0.1.4](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.3...delta-funnel-v0.1.4) - 2026-07-03

### Fixed

- accept Utf8View SQL Server string batches

## [0.1.3](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.2...delta-funnel-v0.1.3) - 2026-07-03

### Fixed

- expose SQL Server batch validation diagnostics in Python errors and reports

## [0.1.2](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.1...delta-funnel-v0.1.2) - 2026-07-02

### Added

- load AWS env for S3 sources ([#393](https://github.com/mag1cfrog/delta-funnel/pull/393))
- implement SQL Server replace mode

### Fixed

- add S3 credential diagnostics ([#389](https://github.com/mag1cfrog/delta-funnel/pull/389))
- support native async leaf casts

### Other

- lock down S3 storage options contract ([#385](https://github.com/mag1cfrog/delta-funnel/pull/385))
- clarify Python S3 credential contract ([#384](https://github.com/mag1cfrog/delta-funnel/pull/384))
- Seo/search indexing ([#376](https://github.com/mag1cfrog/delta-funnel/pull/376))

## [0.1.1](https://github.com/mag1cfrog/delta-funnel/compare/v0.1.0...v0.1.1) - 2026-07-02

### Added

- bridge python logging to tracing

### Fixed

- preserve delta snapshot load cause
- avoid python attach panic in logging bridge
- expose snapshot load cause to python

### Other

- polish public docs landing page ([#359](https://github.com/mag1cfrog/delta-funnel/pull/359))
- isolate python logging bridge global test
- harden python logging bridge
