# Dynamic Partition Pruning Investigation

Issue #27 asks whether DeltaFunnel should support runtime dynamic partition
pruning for the Delta DataFusion provider. This note records the investigation
against the current workspace dependency set.

## Decision

Dynamic partition pruning is feasible, but it is not a one-PR implementation.
The right next step after this investigation is a small implementation slice
that teaches the Delta physical scan plan to receive DataFusion physical
dynamic filters and use them only for safe file-task skipping during execution.

The full feature should be split because it crosses DataFusion physical
optimization, Delta scan metadata, provider file scheduling, metrics,
cancellation, and correctness around residual filters, deletion vectors, and
physical-to-logical transforms.

## DataFusion Version

The workspace uses DataFusion 53.1.0.

Relevant DataFusion 53.1.0 behavior:

- Dynamic filter pushdown is enabled by default through
  `optimizer.enable_dynamic_filter_pushdown`, plus separate join, TopK, and
  aggregate toggles.
- Hash join dynamic filtering also has build-side size and distinct-row
  thresholds for choosing `InList` pushdown versus hash-table lookup
  expressions.
- The recommended physical optimizer list runs an early `FilterPushdown` rule
  for normal physical filter pushdown and a later post-optimization
  `FilterPushdown` rule for dynamic filters whose expressions can reference
  physical plan state.
- `HashJoinExec` creates a `DynamicFilterPhysicalExpr` for inner joins when
  join dynamic filter pushdown is enabled, and pushes that dynamic filter to
  the right-side probe input during the post-optimization phase.
- `ExecutionPlan` has generic physical pushdown hooks:
  `gather_filters_for_pushdown` and `handle_child_pushdown_result`.
- Built-in file scans receive pushed physical filters through
  `DataSourceExec`, then `DataSource::try_pushdown_filters`, then the concrete
  file source.
- DataFusion's pruning predicate path snapshots `DynamicFilterPhysicalExpr`
  values before pruning, so a scan can use the current dynamic expression as a
  static predicate at a specific point in execution.

This means custom execution plans can participate in the mechanism, but they
must opt in by implementing the physical pushdown hooks. It is not enough to
support logical `TableProvider::supports_filters_pushdown`.

The built-in file scan path is not strictly required. It is the reference
implementation and already wires dynamic filters through `DataSourceExec` and
file sources, but the generic `ExecutionPlan` hooks are available to custom
plans. DeltaFunnel can keep its current provider execution path if
`DeltaScanPlanningExec` implements the same physical hook contract.

## Source Checkpoints

The investigation checked these local source boundaries:

- `crates/delta-funnel/Cargo.toml`: DataFusion 53.1.0 dependency.
- `crates/delta-funnel/src/query_engine/datafusion/catalog/provider.rs`:
  logical `TableProvider` pushdown and scan construction.
- `crates/delta-funnel/src/query_engine/datafusion/planning/filters.rs`:
  static logical pushdown decisions and residual policy.
- `crates/delta-funnel/src/query_engine/datafusion/planning/scan_plan.rs`:
  static Delta kernel scan planning and file-task partition planning.
- `crates/delta-funnel/src/query_engine/datafusion/execution/planning_exec.rs`:
  custom Delta physical scan execution plan.
- DataFusion `datafusion-physical-plan/src/execution_plan.rs`: physical
  pushdown hooks.
- DataFusion `datafusion-physical-plan/src/joins/hash_join/exec.rs`: join
  dynamic filter creation and pushdown.
- DataFusion `datafusion-physical-optimizer/src/optimizer.rs`: early and late
  filter pushdown rule ordering.
- DataFusion `datafusion-datasource/src/source.rs` and
  `datafusion-datasource/src/file_scan_config.rs`: built-in file scan
  pushdown path.
- DataFusion `datafusion-pruning/src/pruning_predicate.rs`: dynamic filter
  snapshotting for pruning.

## Current Delta Provider Path

DeltaFunnel currently performs static provider pushdown at the logical provider
boundary:

1. `DeltaTableProvider::supports_filters_pushdown` classifies DataFusion
   logical filters.
2. `DeltaTableProvider::scan` builds a `ProviderScanPlan` from the logical
   projection and pushed logical filters.
3. Static partition and stats predicates are converted to Delta kernel
   predicates for metadata pruning.
4. `ProviderScanPlan::plan_file_task_partitions` expands Delta scan metadata
   and groups selected files into provider file-task partitions.
5. `DeltaTableProvider::scan` returns a custom leaf `DeltaScanPlanningExec`.
6. `DeltaScanPlanningExec::execute` reads the planned file tasks through the
   selected provider reader backend.

That is a good shape for static pushdown, but it is not enough for runtime
dynamic partition pruning. The dynamic filter does not exist when
`TableProvider::scan` builds the initial Delta kernel scan and file-task plan.
DataFusion creates and pushes dynamic physical filters later, during physical
optimization.

`DeltaScanPlanningExec` currently does not override
`gather_filters_for_pushdown` or `handle_child_pushdown_result`. As a result,
DataFusion can create a join dynamic filter, but the Delta scan does not retain
it as a scan consumer.

## Feasible Integration Shape

The safest DeltaFunnel integration is execution-time pruning inside the custom
physical scan plan.

The implementation should:

1. Add a field to `DeltaScanPlanningExec` for pushed physical filters.
2. Override `ExecutionPlan::handle_child_pushdown_result` on
   `DeltaScanPlanningExec`.
3. During the post-optimization phase, accept compatible dynamic physical
   filters and return an updated scan node that owns those filters.
4. Preserve the original static scan plan and file-task partition plan.
5. At execution time, snapshot each dynamic filter and apply it to not-yet-started
   file tasks using only file metadata already present in `DeltaScanFileTask`.
6. Keep DataFusion residual filters unless the provider has separately proven
   exact handling.

This should initially skip whole file tasks only. It should not try to rewrite
the initial Delta kernel scan after planning, split Parquet row groups, or
cancel already emitted rows.

## Pruning Boundary

Dynamic values arrive too late to affect the current pre-execution Delta kernel
metadata expansion. That is acceptable for a first implementation.

The first supported boundary should be:

- Static pruning: `TableProvider::scan` and Delta kernel scan planning.
- Dynamic pruning: `DeltaScanPlanningExec::execute`, before scheduling each
  not-yet-started file task.

This preserves the current provider architecture. It also keeps the scan from
waiting for dynamic filters during physical planning, where waiting would be
incorrect and could deadlock.

## Correctness Rules

The implementation must keep these rules:

- Dynamic pruning must only skip files that are proven unable to produce rows
  for the current snapshot of the dynamic filter.
- Dynamic filters must be treated as opportunistic. If the filter is not ready,
  unsupported, incomplete, or cannot be evaluated against file metadata, the
  provider must read the file.
- A filter that arrives after a file task starts must not invalidate that file's
  already emitted rows.
- The first implementation should not cancel in-flight file reads for dynamic
  pruning. It may only avoid scheduling future file tasks.
- Residual filters must remain in DataFusion unless DeltaFunnel proves exact
  provider enforcement for the full predicate.
- Deletion vectors must still be applied for every file that is read.
- Physical-to-logical transforms and partition value materialization must still
  run before rows reach DataFusion.
- Null partition values and missing file statistics must be conservative.
- Waiting for dynamic filters must not deadlock the plan or starve downstream
  polling.
- Existing scan-wide and per-partition read concurrency bounds must remain in
  force.

## Expression Scope

The first implementation should only accept dynamic filters that can be mapped
to metadata available before reading a file:

- Partition column equality, membership, null checks, and simple comparisons
  where DeltaFunnel already has safe partition semantics.
- Data-column filters only when file statistics can prove exclusion and the
  existing stats pushdown policy accepts the expression family.

Unsupported expressions should be counted and ignored for provider pruning.
They should not fail the query.

Dynamic filters are physical expressions, while the existing static pushdown
policy works with logical `Expr`. The implementation should not reuse the
logical converter blindly. It needs a small physical-expression adapter or a
metadata pruning helper that can operate on snapshotted physical expressions.

Empty or contradictory dynamic filters should be useful but conservative. If a
snapshot proves no file task can match, the provider may skip not-yet-started
tasks and record skipped metrics. If that proof cannot be made from available
metadata, the provider must read the task and leave residual filtering to
DataFusion.

Updated dynamic filters should be treated as newer snapshots, not as a reason
to revisit already-started work. Each file admission can use the best available
snapshot at that point.

## Issue Question Answers

| Question | Answer |
| --- | --- |
| Which DataFusion rules create dynamic filters? | Dynamic filters are created by physical plan operators such as `HashJoinExec`, TopK, and aggregate execution. They are pushed by the late post-optimization `FilterPushdown` rule. |
| Which config enables them? | `optimizer.enable_dynamic_filter_pushdown` is enabled by default and coordinates the join, TopK, and aggregate toggles. Join dynamic filtering also uses hash join InList size and row thresholds. |
| Which provider methods are needed? | `DeltaScanPlanningExec` must implement physical `ExecutionPlan` pushdown hooks, primarily `handle_child_pushdown_result`, and preserve accepted filters in an updated scan node. |
| Is the built-in file scan path required? | No. It is the reference path, but custom non-file `ExecutionPlan` nodes can participate through the generic physical pushdown hooks. |
| Can dynamic filters be snapshotted for pruning? | Yes. DataFusion dynamic physical expressions can be snapshotted, and DataFusion's pruning predicate path already snapshots them before pruning. DeltaFunnel still needs a physical-expression metadata adapter. |
| Can dynamic filters prune before metadata expansion? | Not with the current provider path. The first safe boundary is execution scheduling after static metadata expansion and file-task grouping. |
| What about incomplete filters? | Treat them as opportunistic. If no safe snapshot is available, read the file task. |
| What about empty or contradictory filters? | Skip not-yet-started file tasks only when the current snapshot proves exclusion from file metadata. Otherwise read and preserve residual filtering. |
| What about updated filters after scheduling starts? | Use the newest snapshot for future file admissions only. Do not invalidate emitted rows or cancel already started reads in the first implementation. |
| How should metrics and dry-run reports work? | Metrics should distinguish received, accepted, unsupported, snapshotted, skipped, already-started, missing-metadata, and too-late dynamic filters. Dry-run or benchmark reporting can consume the same counters once implementation exists. |

## Scheduling Behavior

The provider scheduler should evaluate dynamic pruning at the last cheap point
before file admission:

- For `native_async`, evaluate before acquiring file permits and before opening
  the file stream.
- For `official_kernel`, evaluate before the synchronous file handoff starts.
- In bounded native async prefetch mode, a prefetched or opened file counts as
  already started and should not be dynamically skipped by the first
  implementation.
- If the output stream is dropped, cancellation behavior should remain the same
  as today: stop scheduling future work and release permits.

The first implementation should avoid waiting for filter completion by default.
It can snapshot the current dynamic expression before each file admission. A
later issue can investigate bounded waiting if evidence shows that early scans
often outrun useful dynamic filters.

## Metrics

Add provider-owned metrics that distinguish dynamic pruning from existing scan
planning counters:

- `dynamic_filters_received`
- `dynamic_filters_accepted`
- `dynamic_filters_unsupported`
- `dynamic_filter_snapshots`
- `dynamic_files_skipped_before_scheduling`
- `dynamic_files_started_before_filter`
- `dynamic_files_not_pruned_missing_metadata`
- `dynamic_files_not_pruned_unsupported_expression`
- `dynamic_filters_completed`
- `dynamic_filters_too_late`

Existing read stats should continue to report planned partitions, started
partitions, files read, batches, rows, deletion-vector counts, and backend.

## Follow-Up Issues

Recommended implementation slices:

1. Physical hook and plan plumbing.
   Teach `DeltaScanPlanningExec` to accept physical pushed filters and preserve
   them across `with_new_children` and `reset_state` as needed.
2. Metadata pruning adapter.
   Snapshot dynamic physical expressions and evaluate conservative file-task
   exclusion against partition values and available file statistics.
3. Scheduler integration.
   Skip not-yet-started file tasks before file admission for both reader
   backends, without changing existing cancellation or concurrency bounds.
4. Metrics and dry-run reporting.
   Add dynamic pruning counters and expose them in provider read stats and
   benchmark output if applicable.
5. End-to-end dynamic pruning tests.
   Cover joins, late filters, empty and contradictory filters, multiple
   partition columns, null partition values, residual filters, deletion-vector
   tables or explicit DV rejection, cancellation, and metrics.

## Non-Goals For First Implementation

- Re-planning the initial Delta kernel scan after dynamic values arrive.
- Waiting indefinitely for dynamic filters before scanning.
- Cancelling already started file reads based only on a later dynamic filter.
- Row-group or page-level dynamic pruning.
- Claiming ordering, distribution, or join execution behavior outside
  DataFusion's normal plan.
- Replacing DeltaFunnel's provider execution path with DataFusion's built-in
  Parquet file source.

## Close Criteria For Issue #27

Issue #27 can close after this investigation is reviewed and follow-up
implementation issues exist for the accepted slices. The implementation itself
should remain outside #27 so the investigation can stay small and reviewable.
