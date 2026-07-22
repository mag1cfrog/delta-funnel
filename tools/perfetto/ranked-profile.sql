-- Build the exact semantic hierarchy from an unmodified raw trace.
--
-- tools/perfetto/sample-correlation.sql must run first. This query does not
-- create presentation intervals or modify the input trace.
INCLUDE PERFETTO MODULE linux.perf.samples;
INCLUDE PERFETTO MODULE graphs.search;

CREATE PERFETTO TABLE delta_funnel_ranked_semantics AS
SELECT
  slice.id AS semantic_id,
  slice.parent_id AS raw_slice_parent_id,
  slice.track_id,
  track.parent_id AS track_parent_id,
  slice.depth AS raw_depth,
  slice.name,
  slice.ts,
  CASE WHEN slice.dur >= 0 THEN slice.ts + slice.dur END AS end_ts,
  CASE WHEN slice.dur >= 0 THEN slice.dur END AS duration_ns,
  CASE
    WHEN slice.dur >= 0 THEN slice.ts + slice.dur
    ELSE trace_end()
  END AS analysis_end_ts,
  slice.dur >= 0 AS is_complete,
  extract_arg(slice.arg_set_id, 'debug.operation_id') AS operation_id,
  slice.id IN (
    SELECT operation_slice_id FROM delta_funnel_profile_operations
  ) AS is_operation_root,
  extract_arg(slice.arg_set_id, 'debug.operation_kind') AS operation_kind,
  extract_arg(slice.arg_set_id, 'debug.stage_category') AS stage_category,
  extract_arg(slice.arg_set_id, 'debug.stage_name') AS stage_name,
  extract_arg(slice.arg_set_id, 'debug.time_semantics') AS time_semantics,
  extract_arg(slice.arg_set_id, 'debug.result') AS result,
  extract_arg(slice.arg_set_id, 'debug.query_execution_id') AS query_execution_id,
  extract_arg(slice.arg_set_id, 'debug.worker_lane_id') AS worker_lane_id,
  extract_arg(slice.arg_set_id, 'debug.node_id') AS node_id,
  extract_arg(slice.arg_set_id, 'debug.parent_node_id') AS parent_node_id,
  extract_arg(slice.arg_set_id, 'debug.operator_partition') AS operator_partition,
  extract_arg(slice.arg_set_id, 'debug.execution_stream_id') AS execution_stream_id,
  extract_arg(slice.arg_set_id, 'debug.stage_owner_id') AS stage_owner_id,
  extract_arg(slice.arg_set_id, 'debug.activity') AS activity
FROM slice
JOIN track ON track.id = slice.track_id
WHERE slice.category = 'delta_funnel.profile'
  AND extract_arg(slice.arg_set_id, 'debug.operation_id') IS NOT NULL
  AND extract_arg(slice.arg_set_id, 'debug.time_semantics') IN (
    'wall_clock',
    'lifecycle'
  );

CREATE PERFETTO TABLE delta_funnel_ranked_semantic_slice_ancestors AS
WITH RECURSIVE ancestors(semantic_id, ancestor_slice_id, distance) AS (
  SELECT semantic_id, raw_slice_parent_id, 1
  FROM delta_funnel_ranked_semantics
  WHERE raw_slice_parent_id IS NOT NULL

  UNION ALL

  SELECT ancestors.semantic_id, parent.parent_id, ancestors.distance + 1
  FROM ancestors
  JOIN slice AS parent ON parent.id = ancestors.ancestor_slice_id
  WHERE parent.parent_id IS NOT NULL
)
SELECT * FROM ancestors;

CREATE PERFETTO TABLE delta_funnel_ranked_semantic_track_ancestors AS
WITH RECURSIVE ancestors(semantic_id, ancestor_track_id, distance) AS (
  SELECT semantic_id, track_parent_id, 1
  FROM delta_funnel_ranked_semantics
  WHERE track_parent_id IS NOT NULL

  UNION ALL

  SELECT ancestors.semantic_id, parent.parent_id, ancestors.distance + 1
  FROM ancestors
  JOIN track AS parent ON parent.id = ancestors.ancestor_track_id
  WHERE parent.parent_id IS NOT NULL
)
SELECT * FROM ancestors;

CREATE PERFETTO TABLE delta_funnel_ranked_semantic_parent_candidates AS
-- Explicit slice ancestry is the strongest emitted relationship.
SELECT
  child.semantic_id,
  parent.semantic_id AS parent_semantic_id,
  0 AS relation_order,
  ancestor.distance,
  parent.raw_depth AS parent_depth,
  parent.analysis_end_ts - parent.ts AS parent_duration_ns
FROM delta_funnel_ranked_semantic_slice_ancestors AS ancestor
JOIN delta_funnel_ranked_semantics AS child
  ON child.semantic_id = ancestor.semantic_id
JOIN delta_funnel_ranked_semantics AS parent
  ON parent.semantic_id = ancestor.ancestor_slice_id
  AND parent.operation_id = child.operation_id

UNION ALL

-- Stage spans can live on owner tracks while their children live on query or
-- stream tracks. Strict containment and compatible identities bridge only
-- that emitted cross-track relationship.
SELECT
  child.semantic_id,
  parent.semantic_id AS parent_semantic_id,
  2 AS relation_order,
  0 AS distance,
  parent.raw_depth AS parent_depth,
  parent.analysis_end_ts - parent.ts AS parent_duration_ns
FROM delta_funnel_ranked_semantics AS child
JOIN delta_funnel_ranked_semantics AS parent
  ON parent.operation_id = child.operation_id
  AND parent.semantic_id != child.semantic_id
  AND parent.stage_category IS NOT NULL
  AND child.ts >= parent.ts
  AND child.analysis_end_ts <= parent.analysis_end_ts
  AND (
    child.ts > parent.ts
    OR child.analysis_end_ts < parent.analysis_end_ts
  )
  AND (
    parent.query_execution_id IS NULL
    OR child.query_execution_id IS NULL
    OR parent.query_execution_id = child.query_execution_id
  )
  AND (
    parent.worker_lane_id IS NULL
    OR child.worker_lane_id IS NULL
    OR parent.worker_lane_id = child.worker_lane_id
  )
  AND (
    parent.node_id IS NULL
    OR child.node_id IS NULL
    OR parent.node_id = child.node_id
  )
  AND (
    parent.parent_node_id IS NULL
    OR child.parent_node_id IS NULL
    OR parent.parent_node_id = child.parent_node_id
  )
  AND (
    parent.operator_partition IS NULL
    OR child.operator_partition IS NULL
    OR parent.operator_partition = child.operator_partition
  )
  AND (
    parent.execution_stream_id IS NULL
    OR child.execution_stream_id IS NULL
    OR parent.execution_stream_id = child.execution_stream_id
  )
  AND (
    parent.stage_owner_id IS NULL
    OR child.stage_owner_id IS NULL
    OR parent.stage_owner_id = child.stage_owner_id
  )

UNION ALL

-- A containing non-root track ancestor is stronger than inferred stage
-- containment. The operation root remains the final fallback.
SELECT
  child.semantic_id,
  parent.semantic_id AS parent_semantic_id,
  CASE WHEN parent.is_operation_root THEN 3 ELSE 1 END AS relation_order,
  ancestor.distance,
  parent.raw_depth AS parent_depth,
  parent.analysis_end_ts - parent.ts AS parent_duration_ns
FROM delta_funnel_ranked_semantic_track_ancestors AS ancestor
JOIN delta_funnel_ranked_semantics AS child
  ON child.semantic_id = ancestor.semantic_id
JOIN delta_funnel_ranked_semantics AS parent
  ON parent.track_id = ancestor.ancestor_track_id
  AND parent.operation_id = child.operation_id
  AND child.ts >= parent.ts
  AND child.analysis_end_ts <= parent.analysis_end_ts;

CREATE PERFETTO TABLE delta_funnel_ranked_semantic_parent_rankings AS
SELECT
  *,
  row_number() OVER (
    PARTITION BY semantic_id
    ORDER BY
      relation_order,
      distance,
      parent_duration_ns,
      parent_depth DESC,
      parent_semantic_id
  ) AS candidate_rank
FROM delta_funnel_ranked_semantic_parent_candidates;

CREATE PERFETTO TABLE delta_funnel_ranked_semantic_parents AS
SELECT
  semantic.semantic_id,
  selected.parent_semantic_id,
  coalesce(ties.tie_count, 0) AS best_candidate_count
FROM delta_funnel_ranked_semantics AS semantic
LEFT JOIN delta_funnel_ranked_semantic_parent_rankings AS selected
  ON selected.semantic_id = semantic.semantic_id
  AND selected.candidate_rank = 1
LEFT JOIN (
  SELECT candidate.semantic_id, count(*) AS tie_count
  FROM delta_funnel_ranked_semantic_parent_rankings AS candidate
  JOIN delta_funnel_ranked_semantic_parent_rankings AS selected
    ON selected.semantic_id = candidate.semantic_id
    AND selected.candidate_rank = 1
    AND candidate.relation_order = selected.relation_order
    AND candidate.distance = selected.distance
    AND candidate.parent_depth = selected.parent_depth
    AND candidate.parent_duration_ns = selected.parent_duration_ns
  GROUP BY candidate.semantic_id
) AS ties USING (semantic_id);

CREATE PERFETTO TABLE delta_funnel_ranked_semantic_lineage AS
WITH RECURSIVE lineage(descendant_semantic_id, ancestor_semantic_id) AS (
  SELECT semantic_id, semantic_id
  FROM delta_funnel_ranked_semantics

  UNION

  SELECT lineage.descendant_semantic_id, parent.parent_semantic_id
  FROM lineage
  JOIN delta_funnel_ranked_semantic_parents AS parent
    ON parent.semantic_id = lineage.ancestor_semantic_id
  WHERE parent.parent_semantic_id IS NOT NULL
)
SELECT * FROM lineage;

CREATE PERFETTO TABLE delta_funnel_ranked_semantic_depths AS
SELECT
  descendant_semantic_id AS semantic_id,
  count(*) - 1 AS semantic_depth
FROM delta_funnel_ranked_semantic_lineage
GROUP BY descendant_semantic_id;

CREATE PERFETTO TABLE delta_funnel_ranked_semantic_cycles AS
WITH RECURSIVE ancestry(start_semantic_id, semantic_id) AS (
  SELECT semantic_id, parent_semantic_id
  FROM delta_funnel_ranked_semantic_parents
  WHERE parent_semantic_id IS NOT NULL

  UNION

  SELECT ancestry.start_semantic_id, parent.parent_semantic_id
  FROM ancestry
  JOIN delta_funnel_ranked_semantic_parents AS parent
    ON parent.semantic_id = ancestry.semantic_id
  WHERE parent.parent_semantic_id IS NOT NULL
)
SELECT DISTINCT start_semantic_id AS semantic_id
FROM ancestry
WHERE start_semantic_id = semantic_id;

CREATE PERFETTO TABLE delta_funnel_ranked_semantic_audit AS
WITH metrics AS (
SELECT
  (SELECT count(*) FROM delta_funnel_ranked_semantics) AS semantic_count,
  (SELECT count(*) FROM delta_funnel_profile_operations) AS profile_operation_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_semantics
    WHERE is_operation_root
  ) AS operation_root_count,
  (
    SELECT count(*)
    FROM (
      SELECT operation.operation_id
      FROM delta_funnel_profile_operations AS operation
      LEFT JOIN delta_funnel_ranked_semantics AS root
        ON root.operation_id = operation.operation_id
        AND root.is_operation_root
      GROUP BY operation.operation_id
      HAVING count(root.semantic_id) != 1
    )
  ) AS operation_root_error_count,
  (
    SELECT count(*) = 0
    FROM delta_funnel_profile_operations
  ) AS missing_operation_error_count,
  (
    SELECT count(*)
    FROM (
      SELECT semantic_id
      FROM delta_funnel_ranked_semantics
      GROUP BY semantic_id
      HAVING count(*) != 1
    )
  ) AS duplicate_semantic_identity_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_semantics AS semantic
    JOIN delta_funnel_ranked_semantic_parents AS parent USING (semantic_id)
    WHERE NOT semantic.is_operation_root
      AND parent.parent_semantic_id IS NULL
  ) AS missing_parent_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_semantic_parents
    WHERE best_candidate_count > 1
  ) AS ambiguous_parent_count,
  (SELECT count(*) FROM delta_funnel_ranked_semantic_cycles)
    AS semantic_cycle_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_semantics
    WHERE NOT is_complete
  ) AS incomplete_semantic_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_semantics
    WHERE NOT is_complete
      AND (end_ts IS NOT NULL OR duration_ns IS NOT NULL)
  ) AS fabricated_completion_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_semantics
    WHERE is_complete
      AND (
        end_ts IS NULL
        OR duration_ns IS NULL
        OR duration_ns < 0
        OR end_ts < ts
      )
  ) AS invalid_interval_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_semantic_parents AS edge
    JOIN delta_funnel_ranked_semantics AS child
      ON child.semantic_id = edge.semantic_id
    JOIN delta_funnel_ranked_semantics AS parent
      ON parent.semantic_id = edge.parent_semantic_id
    WHERE child.operation_id != parent.operation_id
  ) AS cross_operation_parent_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_semantic_parents AS edge
    JOIN delta_funnel_ranked_semantics AS child
      ON child.semantic_id = edge.semantic_id
    JOIN delta_funnel_ranked_semantics AS parent
      ON parent.semantic_id = edge.parent_semantic_id
    WHERE child.ts < parent.ts
      OR child.analysis_end_ts > parent.analysis_end_ts
  ) AS invalid_parent_interval_count
)
SELECT
  *,
  missing_operation_error_count
    + operation_root_error_count
    + duplicate_semantic_identity_count
    + missing_parent_count
    + ambiguous_parent_count
    + semantic_cycle_count
    + fabricated_completion_count
    + invalid_interval_count
    + cross_operation_parent_count
    + invalid_parent_interval_count AS audit_error_count
FROM metrics;

CREATE PERFETTO TABLE delta_funnel_ranked_sample_semantic_candidates AS
SELECT
  sample.sample_id,
  semantic.semantic_id,
  depth.semantic_depth
FROM delta_funnel_sample_correlation AS sample
JOIN delta_funnel_ranked_semantics AS semantic
  ON semantic.operation_id = sample.operation_id
  AND sample.ts >= semantic.ts
  AND sample.ts < semantic.analysis_end_ts
  AND (
    semantic.query_execution_id IS NULL
    OR semantic.query_execution_id = sample.query_execution_id
  )
  AND (
    semantic.worker_lane_id IS NULL
    OR semantic.worker_lane_id = sample.worker_lane_id
  )
  AND (
    semantic.node_id IS NULL
    OR semantic.node_id = sample.node_id
  )
  AND (
    semantic.parent_node_id IS NULL
    OR semantic.parent_node_id = sample.parent_node_id
  )
  AND (
    semantic.operator_partition IS NULL
    OR semantic.operator_partition = sample.operator_partition
  )
  AND (
    semantic.execution_stream_id IS NULL
    OR semantic.execution_stream_id = sample.execution_stream_id
  )
  AND (
    semantic.stage_owner_id IS NULL
    OR semantic.stage_owner_id = sample.stage_owner_id
  )
JOIN delta_funnel_ranked_semantic_depths AS depth USING (semantic_id)
WHERE sample.attribution != 'ambiguous';

CREATE PERFETTO TABLE delta_funnel_ranked_sample_candidate_rankings AS
SELECT
  *,
  row_number() OVER (
    PARTITION BY sample_id
    ORDER BY semantic_depth DESC, semantic_id
  ) AS candidate_rank
FROM delta_funnel_ranked_sample_semantic_candidates;

-- The deepest candidate is unique only when every other compatible candidate
-- is one of its real ancestors. Parallel branches remain ambiguous.
CREATE PERFETTO TABLE delta_funnel_ranked_sample_candidate_conflicts AS
SELECT candidate.sample_id, count(*) AS conflict_count
FROM delta_funnel_ranked_sample_semantic_candidates AS candidate
JOIN delta_funnel_ranked_sample_candidate_rankings AS selected
  ON selected.sample_id = candidate.sample_id
  AND selected.candidate_rank = 1
LEFT JOIN delta_funnel_ranked_semantic_lineage AS lineage
  ON lineage.descendant_semantic_id = selected.semantic_id
  AND lineage.ancestor_semantic_id = candidate.semantic_id
WHERE lineage.ancestor_semantic_id IS NULL
GROUP BY candidate.sample_id;

CREATE PERFETTO TABLE delta_funnel_ranked_sample_ownership AS
WITH best AS (
  SELECT
    selected.sample_id,
    selected.semantic_id,
    coalesce(conflict.conflict_count, 0) AS conflict_count
  FROM delta_funnel_ranked_sample_candidate_rankings AS selected
  LEFT JOIN delta_funnel_ranked_sample_candidate_conflicts AS conflict
    USING (sample_id)
  WHERE selected.candidate_rank = 1
)
SELECT
  sample.sample_id,
  sample.ts,
  sample.utid,
  sample.callsite_id,
  sample.unwind_error,
  CASE
    WHEN sample.attribution = 'ambiguous' THEN 'ambiguous'
    WHEN best.conflict_count = 0
      AND best.semantic_id IS NOT NULL THEN 'direct'
    WHEN best.conflict_count > 0 THEN 'ambiguous'
    ELSE 'unattributed'
  END AS attribution,
  sample.operation_id,
  CASE WHEN best.conflict_count = 0 THEN best.semantic_id END AS semantic_id,
  coalesce(best.conflict_count, 0) AS conflicting_candidate_count
FROM delta_funnel_sample_correlation AS sample
LEFT JOIN best USING (sample_id);

CREATE PERFETTO TABLE delta_funnel_ranked_semantic_sample_counts AS
WITH
  direct AS (
    SELECT semantic_id, count(*) AS direct_sample_count
    FROM delta_funnel_ranked_sample_ownership
    WHERE attribution = 'direct'
    GROUP BY semantic_id
  ),
  inclusive AS (
    SELECT
      lineage.ancestor_semantic_id AS semantic_id,
      sum(direct.direct_sample_count) AS inclusive_sample_count
    FROM delta_funnel_ranked_semantic_lineage AS lineage
    JOIN direct ON direct.semantic_id = lineage.descendant_semantic_id
    GROUP BY lineage.ancestor_semantic_id
  )
SELECT
  semantic.semantic_id,
  coalesce(direct.direct_sample_count, 0) AS direct_sample_count,
  coalesce(inclusive.inclusive_sample_count, 0) AS inclusive_sample_count
FROM delta_funnel_ranked_semantics AS semantic
LEFT JOIN direct USING (semantic_id)
LEFT JOIN inclusive USING (semantic_id);

CREATE PERFETTO TABLE delta_funnel_ranked_attribution_audit AS
WITH metrics AS (
SELECT
  (SELECT audit_error_count FROM delta_funnel_sample_correlation_audit)
    AS correlation_audit_error_count,
  (SELECT audit_error_count FROM delta_funnel_ranked_semantic_audit)
    AS semantic_audit_error_count,
  count(*) AS eligible_sample_count,
  coalesce(sum(attribution = 'direct'), 0) AS direct_sample_count,
  coalesce(sum(attribution = 'ambiguous'), 0) AS ambiguous_sample_count,
  coalesce(sum(attribution = 'unattributed'), 0) AS unattributed_sample_count,
  count(*) - count(DISTINCT sample_id) AS duplicate_sample_ownership_count,
  coalesce(sum(
    attribution = 'direct' AND semantic_id IS NULL
  ), 0) AS missing_direct_owner_count,
  coalesce(sum(
    attribution != 'direct' AND semantic_id IS NOT NULL
  ), 0) AS invalid_non_direct_owner_count,
  count(*) - coalesce(sum(
    attribution IN ('direct', 'ambiguous', 'unattributed')
  ), 0) AS attribution_conservation_error_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_semantics AS root
    JOIN delta_funnel_ranked_semantic_sample_counts AS sample_count
      USING (semantic_id)
    LEFT JOIN (
      SELECT operation_id, count(*) AS direct_sample_count
      FROM delta_funnel_ranked_sample_ownership
      WHERE attribution = 'direct'
      GROUP BY operation_id
    ) AS operation_direct USING (operation_id)
    WHERE root.is_operation_root
      AND sample_count.inclusive_sample_count
        != coalesce(operation_direct.direct_sample_count, 0)
  ) AS inclusive_conservation_error_count,
  (
    SELECT abs(
      coalesce(sum(direct_sample_count), 0)
      - (
        SELECT count(*)
        FROM delta_funnel_ranked_sample_ownership
        WHERE attribution = 'direct'
      )
    )
    FROM delta_funnel_ranked_semantic_sample_counts
  ) AS direct_count_reconciliation_error_count
FROM delta_funnel_ranked_sample_ownership
)
SELECT
  *,
  correlation_audit_error_count
    + semantic_audit_error_count
    + duplicate_sample_ownership_count
    + missing_direct_owner_count
    + invalid_non_direct_owner_count
    + attribution_conservation_error_count
    + inclusive_conservation_error_count
    + direct_count_reconciliation_error_count AS audit_error_count
FROM metrics;

CREATE PERFETTO TABLE delta_funnel_ranked_usable_function_samples AS
SELECT sample.*
FROM delta_funnel_ranked_sample_ownership AS sample
WHERE sample.attribution = 'direct'
  AND sample.callsite_id IS NOT NULL
  AND sample.unwind_error IS NULL;

CREATE PERFETTO TABLE delta_funnel_ranked_expanded_callstacks AS
SELECT
  stack.id AS function_id,
  stack.parent_id AS parent_function_id,
  stack.callsite_id,
  stack.name,
  stack.mapping_name,
  stack.source_file,
  stack.line_number,
  stack.is_leaf_function_in_callsite_frame AS is_leaf
FROM _callstacks_for_stack_profile_samples!((
  SELECT DISTINCT callsite_id
  FROM delta_funnel_ranked_usable_function_samples
)) AS stack;

CREATE PERFETTO TABLE delta_funnel_ranked_callsite_leaves AS
SELECT
  callsite_id,
  count(*) AS leaf_count,
  min(function_id) AS function_id
FROM delta_funnel_ranked_expanded_callstacks
WHERE is_leaf
GROUP BY callsite_id;

CREATE PERFETTO TABLE delta_funnel_ranked_function_sample_ownership AS
SELECT
  sample.sample_id,
  sample.semantic_id,
  sample.callsite_id,
  CASE
    WHEN sample.callsite_id IS NOT NULL
      AND sample.unwind_error IS NULL
      AND leaf.leaf_count = 1 THEN leaf.function_id
    ELSE -1
  END AS function_id,
  CASE
    WHEN sample.callsite_id IS NOT NULL
      AND sample.unwind_error IS NULL
      AND leaf.leaf_count = 1 THEN 'resolved'
    ELSE 'unresolved'
  END AS resolution
FROM delta_funnel_ranked_sample_ownership AS sample
LEFT JOIN delta_funnel_ranked_callsite_leaves AS leaf USING (callsite_id)
WHERE sample.attribution = 'direct';

-- These are the same Perfetto macros behind cpu_profiling_summary_tree.
-- Repeated callsite rows preserve sample multiplicity for reconciliation.
CREATE PERFETTO TABLE delta_funnel_ranked_official_function_frames AS
SELECT *
FROM _callstacks_for_callsites!((
  SELECT callsite_id
  FROM delta_funnel_ranked_function_sample_ownership
  WHERE resolution = 'resolved'
));

CREATE PERFETTO TABLE delta_funnel_ranked_official_function_summary AS
SELECT *
FROM _callstacks_self_to_cumulative!((
  SELECT id, parent_id, self_count
  FROM delta_funnel_ranked_official_function_frames
));

CREATE PERFETTO TABLE delta_funnel_ranked_function_self_counts AS
SELECT semantic_id, function_id, count(*) AS self_sample_count
FROM delta_funnel_ranked_function_sample_ownership
GROUP BY semantic_id, function_id;

CREATE PERFETTO TABLE delta_funnel_ranked_function_roots AS
SELECT
  semantic_id,
  function_id,
  self_sample_count,
  coalesce((
    SELECT max(id)
    FROM delta_funnel_ranked_official_function_frames
  ), -1) + row_number() OVER (
    ORDER BY semantic_id, function_id
  ) AS root_node_id
FROM delta_funnel_ranked_function_self_counts
WHERE function_id != -1;

CREATE PERFETTO TABLE delta_funnel_ranked_function_graph AS
SELECT
  id AS source_node_id,
  parent_id AS dest_node_id,
  1 AS edge_weight
FROM delta_funnel_ranked_official_function_frames
WHERE parent_id IS NOT NULL

UNION ALL

SELECT root_node_id, function_id, 1
FROM delta_funnel_ranked_function_roots;

-- A synthetic root represents each semantic/function self-count pair. The
-- native graph scan preserves that root while walking toward stack roots.
CREATE PERFETTO TABLE delta_funnel_ranked_function_ancestry AS
SELECT *
FROM graph_reachable_weight_bounded_dfs!((
  SELECT source_node_id, dest_node_id, edge_weight
  FROM delta_funnel_ranked_function_graph
), (
  SELECT
    root_node_id,
    (SELECT count(*) + 1 FROM delta_funnel_ranked_official_function_frames)
      AS root_target_weight
  FROM delta_funnel_ranked_function_roots
), 0);

CREATE PERFETTO TABLE delta_funnel_ranked_function_inclusive_counts AS
SELECT
  root.semantic_id,
  ancestry.node_id AS function_id,
  sum(root.self_sample_count) AS inclusive_sample_count
FROM delta_funnel_ranked_function_ancestry AS ancestry
JOIN delta_funnel_ranked_function_roots AS root USING (root_node_id)
JOIN delta_funnel_ranked_official_function_frames AS frame
  ON frame.id = ancestry.node_id
GROUP BY root.semantic_id, ancestry.node_id

UNION ALL

SELECT semantic_id, function_id, self_sample_count
FROM delta_funnel_ranked_function_self_counts
WHERE function_id = -1;

-- Aggregate metadata never contains unrestricted source or module paths.
CREATE PERFETTO TABLE delta_funnel_ranked_function_metadata AS
WITH RECURSIVE
  module_names(function_id, value, depth) AS (
    SELECT id, replace(mapping_name, char(92), '/'), 0
    FROM delta_funnel_ranked_official_function_frames

    UNION ALL

    SELECT function_id, substr(value, instr(value, '/') + 1), depth + 1
    FROM module_names
    WHERE instr(value, '/') > 0
      AND depth < 64
  ),
  source_names(function_id, value, depth) AS (
    SELECT id, replace(source_file, char(92), '/'), 0
    FROM delta_funnel_ranked_official_function_frames

    UNION ALL

    SELECT function_id, substr(value, instr(value, '/') + 1), depth + 1
    FROM source_names
    WHERE instr(value, '/') > 0
      AND depth < 64
  ),
  modules AS (
    SELECT
      function_id,
      CASE
        WHEN value GLOB '[A-Za-z]:*' THEN NULL
        ELSE nullif(substr(value, 1, 255), '')
      END AS module_name
    FROM module_names
    WHERE instr(value, '/') = 0
  ),
  sources AS (
    SELECT
      function_id,
      CASE
        WHEN value GLOB '[A-Za-z]:*' THEN NULL
        ELSE nullif(substr(value, 1, 255), '')
      END AS source_file
    FROM source_names
    WHERE instr(value, '/') = 0
  )
SELECT
  frame.id AS function_id,
  frame.parent_id AS parent_function_id,
  coalesce(nullif(substr(frame.name, 1, 512), ''), '[unresolved]') AS name,
  module.module_name,
  source.source_file,
  frame.line_number
FROM delta_funnel_ranked_official_function_frames AS frame
LEFT JOIN modules AS module ON module.function_id = frame.id
LEFT JOIN sources AS source ON source.function_id = frame.id;

CREATE PERFETTO TABLE delta_funnel_ranked_function_aggregates AS
SELECT
  inclusive.semantic_id,
  inclusive.function_id,
  metadata.parent_function_id,
  metadata.name,
  metadata.module_name,
  metadata.source_file,
  metadata.line_number,
  coalesce(self.self_sample_count, 0) AS self_sample_count,
  inclusive.inclusive_sample_count
FROM delta_funnel_ranked_function_inclusive_counts AS inclusive
JOIN delta_funnel_ranked_function_metadata AS metadata USING (function_id)
LEFT JOIN delta_funnel_ranked_function_self_counts AS self
  USING (semantic_id, function_id)

UNION ALL

SELECT
  self.semantic_id,
  self.function_id,
  NULL AS parent_function_id,
  '[native stack unavailable]' AS name,
  NULL AS module_name,
  NULL AS source_file,
  NULL AS line_number,
  self.self_sample_count,
  self.self_sample_count AS inclusive_sample_count
FROM delta_funnel_ranked_function_self_counts AS self
WHERE self.function_id = -1;

CREATE PERFETTO TABLE delta_funnel_ranked_function_cycles AS
WITH RECURSIVE ancestry(
  semantic_id,
  start_function_id,
  function_id
) AS (
  SELECT semantic_id, function_id, parent_function_id
  FROM delta_funnel_ranked_function_aggregates
  WHERE parent_function_id IS NOT NULL

  UNION

  SELECT
    ancestry.semantic_id,
    ancestry.start_function_id,
    parent.parent_function_id
  FROM ancestry
  JOIN delta_funnel_ranked_function_aggregates AS parent
    ON parent.semantic_id = ancestry.semantic_id
    AND parent.function_id = ancestry.function_id
  WHERE parent.parent_function_id IS NOT NULL
)
SELECT DISTINCT semantic_id, start_function_id AS function_id
FROM ancestry
WHERE start_function_id = function_id;

CREATE PERFETTO TABLE delta_funnel_ranked_function_reconciliation AS
WITH actual AS (
  SELECT
    function_id,
    sum(self_sample_count) AS self_sample_count,
    sum(inclusive_sample_count) AS cumulative_sample_count
  FROM delta_funnel_ranked_function_aggregates
  WHERE function_id != -1
  GROUP BY function_id
)
SELECT
  official.id AS function_id,
  official.self_count AS official_self_sample_count,
  summary.cumulative_count AS official_cumulative_sample_count,
  coalesce(actual.self_sample_count, 0) AS actual_self_sample_count,
  coalesce(actual.cumulative_sample_count, 0) AS actual_cumulative_sample_count
FROM delta_funnel_ranked_official_function_frames AS official
JOIN delta_funnel_ranked_official_function_summary AS summary USING (id)
LEFT JOIN actual ON actual.function_id = official.id;

CREATE PERFETTO TABLE delta_funnel_ranked_function_audit AS
WITH metrics AS (
SELECT
  (SELECT audit_error_count FROM delta_funnel_ranked_attribution_audit)
    AS attribution_audit_error_count,
  (SELECT count(*) FROM delta_funnel_ranked_function_aggregates)
    AS aggregate_function_node_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_function_aggregates
    WHERE name IN ('[unresolved]', '[native stack unavailable]')
  ) AS unresolved_function_node_count,
  (SELECT count(*) FROM delta_funnel_ranked_function_sample_ownership)
    AS function_sample_count,
  coalesce((
    SELECT sum(resolution = 'resolved')
    FROM delta_funnel_ranked_function_sample_ownership
  ), 0) AS resolved_function_sample_count,
  coalesce((
    SELECT sum(resolution = 'unresolved')
    FROM delta_funnel_ranked_function_sample_ownership
  ), 0) AS unresolved_function_sample_count,
  (
    SELECT count(*)
    FROM (
      SELECT semantic_id, function_id
      FROM delta_funnel_ranked_function_aggregates
      GROUP BY semantic_id, function_id
      HAVING count(*) != 1
    )
  ) AS duplicate_function_identity_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_function_aggregates AS child
    LEFT JOIN delta_funnel_ranked_function_aggregates AS parent
      ON parent.semantic_id = child.semantic_id
      AND parent.function_id = child.parent_function_id
    WHERE child.parent_function_id IS NOT NULL
      AND parent.function_id IS NULL
  ) AS missing_function_parent_count,
  (SELECT count(*) FROM delta_funnel_ranked_function_cycles)
    AS function_cycle_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_function_reconciliation
    WHERE official_self_sample_count != actual_self_sample_count
      OR official_cumulative_sample_count != actual_cumulative_sample_count
  ) AS official_summary_mismatch_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_function_aggregates
    WHERE coalesce(module_name, '') GLOB '*[\\/]*'
      OR coalesce(source_file, '') GLOB '*[\\/]*'
      OR coalesce(module_name, '') GLOB '[A-Za-z]:*'
      OR coalesce(source_file, '') GLOB '[A-Za-z]:*'
  ) AS unsafe_path_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_usable_function_samples AS sample
    LEFT JOIN delta_funnel_ranked_callsite_leaves AS leaf USING (callsite_id)
    WHERE coalesce(leaf.leaf_count, 0) != 1
  ) AS invalid_leaf_mapping_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_sample_ownership
    WHERE attribution = 'direct'
  ) - (
    SELECT count(*)
    FROM delta_funnel_ranked_function_sample_ownership
  ) AS function_sample_conservation_error_count,
  (
    SELECT count(*)
    FROM delta_funnel_ranked_function_sample_ownership
  ) - coalesce((
    SELECT sum(self_sample_count)
    FROM delta_funnel_ranked_function_self_counts
  ), 0) AS function_self_conservation_error_count
)
SELECT
  *,
  attribution_audit_error_count
    + duplicate_function_identity_count
    + missing_function_parent_count
    + function_cycle_count
    + official_summary_mismatch_count
    + unsafe_path_count
    + invalid_leaf_mapping_count
    + function_sample_conservation_error_count
    + function_self_conservation_error_count AS audit_error_count
FROM metrics;
