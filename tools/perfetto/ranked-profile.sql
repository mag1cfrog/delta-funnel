-- Complete the validation-only function rollup and aggregate audit after
-- ranked_profile_base.sql. Production reports use the compact Rust fold.
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

CREATE PERFETTO INDEX delta_funnel_ranked_function_inclusive_lookup
ON delta_funnel_ranked_function_inclusive_counts(semantic_id, function_id);

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

CREATE PERFETTO INDEX delta_funnel_ranked_function_metadata_lookup
ON delta_funnel_ranked_function_metadata(function_id);

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

CREATE PERFETTO INDEX delta_funnel_ranked_function_aggregate_identity
ON delta_funnel_ranked_function_aggregates(semantic_id, function_id);

CREATE PERFETTO TABLE delta_funnel_ranked_function_cycles AS
WITH RECURSIVE ancestry(
  start_function_id,
  function_id
) AS (
  SELECT id, parent_id
  FROM (
    SELECT DISTINCT id, parent_id
    FROM delta_funnel_ranked_official_function_frames
  )
  WHERE parent_id IS NOT NULL

  UNION

  SELECT
    ancestry.start_function_id,
    parent.parent_function_id
  FROM ancestry
  JOIN (
    SELECT DISTINCT id, parent_id AS parent_function_id
    FROM delta_funnel_ranked_official_function_frames
  ) AS parent ON parent.id = ancestry.function_id
  WHERE parent.parent_function_id IS NOT NULL
)
SELECT DISTINCT start_function_id AS function_id
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

CREATE PERFETTO TABLE delta_funnel_ranked_semantic_aggregates AS
SELECT
  semantic.semantic_id,
  parent.parent_semantic_id,
  semantic.operation_id,
  semantic.name,
  CASE
    WHEN semantic.is_operation_root THEN 'operation'
    WHEN semantic.stage_category IS NOT NULL THEN 'stage'
    WHEN semantic.activity IS NOT NULL THEN 'activity'
    WHEN semantic.name IN ('Planning', 'Execution', 'Finalization') THEN 'phase'
    WHEN semantic.name = 'DataFusion query' THEN 'query'
    WHEN semantic.name = 'DataFusion query planning' THEN 'query_planning'
    ELSE 'semantic'
  END AS semantic_kind,
  semantic.operation_kind,
  semantic.stage_category,
  semantic.stage_name,
  semantic.activity,
  semantic.ts AS start_ns,
  semantic.end_ts AS end_ns,
  semantic.duration_ns,
  semantic.time_semantics,
  semantic.result,
  semantic.is_complete,
  semantic.query_execution_id,
  semantic.query_scope,
  semantic.query_owner,
  semantic.worker_lane_id,
  semantic.worker_kind,
  semantic.node_id,
  semantic.parent_node_id,
  semantic.operator_partition,
  semantic.execution_stream_id,
  semantic.stage_owner_id,
  sample_count.direct_sample_count,
  sample_count.inclusive_sample_count
FROM delta_funnel_ranked_semantics AS semantic
JOIN delta_funnel_ranked_semantic_parents AS parent USING (semantic_id)
JOIN delta_funnel_ranked_semantic_sample_counts AS sample_count USING (semantic_id);

-- Ambiguous and unattributed samples have no unique semantic owner, so their
-- conservation totals remain profile-scoped rather than attached arbitrarily.
CREATE PERFETTO TABLE delta_funnel_ranked_coverage_aggregate AS
SELECT
  'profile' AS scope,
  count(*) AS eligible_sample_count,
  coalesce(sum(attribution = 'direct'), 0) AS direct_sample_count,
  coalesce(sum(attribution = 'ambiguous'), 0) AS ambiguous_sample_count,
  coalesce(sum(attribution = 'unattributed'), 0) AS unattributed_sample_count
FROM delta_funnel_ranked_sample_ownership;

-- Measure the exact UTF-8 JSON Lines representation without emitting it or
-- copying raw samples into the aggregate contract.
CREATE PERFETTO TABLE delta_funnel_ranked_aggregate_size AS
SELECT
  (
    SELECT coalesce(sum(length(CAST(json_object(
      'kind', 'semantic',
      'semantic_id', semantic_id,
      'parent_semantic_id', parent_semantic_id,
      'operation_id', operation_id,
      'name', name,
      'semantic_kind', semantic_kind,
      'operation_kind', operation_kind,
      'stage_category', stage_category,
      'stage_name', stage_name,
      'activity', activity,
      'start_ns', start_ns,
      'end_ns', end_ns,
      'duration_ns', duration_ns,
      'time_semantics', time_semantics,
      'result', result,
      'is_complete', is_complete,
      'query_execution_id', query_execution_id,
      'query_scope', query_scope,
      'query_owner', query_owner,
      'worker_lane_id', worker_lane_id,
      'worker_kind', worker_kind,
      'node_id', node_id,
      'parent_node_id', parent_node_id,
      'operator_partition', operator_partition,
      'execution_stream_id', execution_stream_id,
      'stage_owner_id', stage_owner_id,
      'direct_sample_count', direct_sample_count,
      'inclusive_sample_count', inclusive_sample_count
    ) AS BLOB))) + count(*), 0)
    FROM delta_funnel_ranked_semantic_aggregates
  ) AS semantic_aggregate_json_bytes,
  (
    SELECT coalesce(sum(length(CAST(json_object(
      'kind', 'function',
      'semantic_id', semantic_id,
      'function_id', function_id,
      'parent_function_id', parent_function_id,
      'name', name,
      'module_name', module_name,
      'source_file', source_file,
      'line_number', line_number,
      'self_sample_count', self_sample_count,
      'inclusive_sample_count', inclusive_sample_count
    ) AS BLOB))) + count(*), 0)
    FROM delta_funnel_ranked_function_aggregates
  ) AS function_aggregate_json_bytes,
  (
    SELECT coalesce(sum(length(CAST(json_object(
      'kind', 'coverage',
      'scope', scope,
      'eligible_sample_count', eligible_sample_count,
      'direct_sample_count', direct_sample_count,
      'ambiguous_sample_count', ambiguous_sample_count,
      'unattributed_sample_count', unattributed_sample_count
    ) AS BLOB))) + count(*), 0)
    FROM delta_funnel_ranked_coverage_aggregate
  ) AS coverage_aggregate_json_bytes;

CREATE PERFETTO TABLE delta_funnel_ranked_aggregate_audit AS
SELECT
  semantic.semantic_count,
  function.aggregate_function_node_count,
  function.unresolved_function_node_count,
  (
    SELECT profile_process_sample_count
    FROM delta_funnel_sample_correlation_audit
  ) AS profile_process_sample_count,
  attribution.eligible_sample_count,
  attribution.direct_sample_count,
  (
    SELECT coalesce(sum(inclusive_sample_count), 0)
    FROM delta_funnel_ranked_semantic_sample_counts
  ) AS inclusive_semantic_sample_count,
  attribution.ambiguous_sample_count,
  attribution.unattributed_sample_count,
  function.resolved_function_sample_count,
  function.unresolved_function_sample_count,
  function.official_summary_mismatch_count,
  size.semantic_aggregate_json_bytes,
  size.function_aggregate_json_bytes,
  size.coverage_aggregate_json_bytes,
  size.semantic_aggregate_json_bytes
    + size.function_aggregate_json_bytes
    + size.coverage_aggregate_json_bytes AS aggregate_json_bytes,
  function.audit_error_count
    + (
      SELECT count(*) != semantic.semantic_count
      FROM delta_funnel_ranked_semantic_aggregates
    )
    + (
      SELECT count(*) != 1
      FROM delta_funnel_ranked_coverage_aggregate
    ) AS audit_error_count
FROM delta_funnel_ranked_semantic_audit AS semantic
CROSS JOIN delta_funnel_ranked_attribution_audit AS attribution
CROSS JOIN delta_funnel_ranked_function_audit AS function
CROSS JOIN delta_funnel_ranked_aggregate_size AS size;
