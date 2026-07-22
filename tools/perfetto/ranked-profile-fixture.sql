INCLUDE PERFETTO MODULE graphs.search;

CREATE PERFETTO TABLE fixture_semantics(
  id LONG,
  parent_id LONG,
  operation_id LONG
) AS
SELECT column1 AS id, column2 AS parent_id, column3 AS operation_id
FROM (VALUES
  (1, NULL, 1),
  (2, 1, 1),
  (3, 99, 1),
  (4, 5, 1),
  (5, 4, 1),
  (6, 1, 2));

CREATE PERFETTO TABLE fixture_semantic_cycles AS
WITH RECURSIVE ancestry(start_id, id) AS (
  SELECT id, parent_id
  FROM fixture_semantics
  WHERE parent_id IS NOT NULL

  UNION

  SELECT ancestry.start_id, parent.parent_id
  FROM ancestry
  JOIN fixture_semantics AS parent ON parent.id = ancestry.id
  WHERE parent.parent_id IS NOT NULL
)
SELECT DISTINCT start_id AS id
FROM ancestry
WHERE start_id = id;

CREATE PERFETTO TABLE fixture_semantic_edges(
  child_id LONG,
  parent_id LONG
) AS
SELECT column1 AS child_id, column2 AS parent_id
FROM (VALUES
  (2, 1),
  (7, 1),
  (7, 2));

CREATE PERFETTO TABLE fixture_attribution_semantics(
  id LONG,
  parent_id LONG
) AS
SELECT column1 AS id, column2 AS parent_id
FROM (VALUES
  (10, NULL),
  (11, 10),
  (12, 10),
  (13, 11));

CREATE PERFETTO TABLE fixture_attribution_lineage AS
WITH RECURSIVE lineage(descendant_id, ancestor_id) AS (
  SELECT id, id
  FROM fixture_attribution_semantics

  UNION

  SELECT lineage.descendant_id, parent.parent_id
  FROM lineage
  JOIN fixture_attribution_semantics AS parent
    ON parent.id = lineage.ancestor_id
  WHERE parent.parent_id IS NOT NULL
)
SELECT * FROM lineage;

CREATE PERFETTO TABLE fixture_sample_candidates(
  sample_id LONG,
  semantic_id LONG,
  semantic_depth LONG
) AS
SELECT column1 AS sample_id, column2 AS semantic_id, column3 AS semantic_depth
FROM (VALUES
  (1, 10, 0),
  (1, 11, 1),
  (1, 13, 2),
  (2, 11, 1),
  (2, 12, 1),
  (4, 10, 0),
  (4, 11, 1));

CREATE PERFETTO TABLE fixture_candidate_rankings AS
SELECT
  *,
  row_number() OVER (
    PARTITION BY sample_id
    ORDER BY semantic_depth DESC, semantic_id
  ) AS candidate_rank
FROM fixture_sample_candidates;

CREATE PERFETTO TABLE fixture_candidate_conflicts AS
SELECT candidate.sample_id, count(*) AS conflict_count
FROM fixture_sample_candidates AS candidate
JOIN fixture_candidate_rankings AS selected
  ON selected.sample_id = candidate.sample_id
  AND selected.candidate_rank = 1
LEFT JOIN fixture_attribution_lineage AS lineage
  ON lineage.descendant_id = selected.semantic_id
  AND lineage.ancestor_id = candidate.semantic_id
WHERE lineage.ancestor_id IS NULL
GROUP BY candidate.sample_id;

CREATE PERFETTO TABLE fixture_sample_ownership AS
WITH samples(sample_id) AS (
  VALUES (1), (2), (3), (4)
)
SELECT
  sample.sample_id,
  CASE
    WHEN selected.semantic_id IS NULL THEN 'unattributed'
    WHEN coalesce(conflict.conflict_count, 0) > 0 THEN 'ambiguous'
    ELSE 'direct'
  END AS attribution,
  CASE
    WHEN coalesce(conflict.conflict_count, 0) = 0 THEN selected.semantic_id
  END AS semantic_id
FROM samples AS sample
LEFT JOIN fixture_candidate_rankings AS selected
  ON selected.sample_id = sample.sample_id
  AND selected.candidate_rank = 1
LEFT JOIN fixture_candidate_conflicts AS conflict USING (sample_id);

CREATE PERFETTO TABLE fixture_operation_resolution(
  sample_id LONG,
  operation_count LONG,
  context_leaf_count LONG,
  expected STRING
) AS
SELECT
  column1 AS sample_id,
  column2 AS operation_count,
  column3 AS context_leaf_count,
  column4 AS expected
FROM (VALUES
  (1, 2, 1, 'direct'),
  (2, 2, 0, 'ambiguous'),
  (3, 1, 0, 'unattributed'),
  (4, 1, 2, 'ambiguous'),
  (5, 1, 1, 'direct'));

CREATE PERFETTO TABLE fixture_function_frames(
  id LONG,
  parent_id LONG
) AS
SELECT column1 AS id, column2 AS parent_id
FROM (VALUES
  (0, NULL),
  (1, 0),
  (2, 0),
  (3, 4),
  (4, 3));

CREATE PERFETTO TABLE fixture_function_cycles AS
WITH RECURSIVE ancestry(start_function_id, function_id) AS (
  SELECT id, parent_id
  FROM fixture_function_frames
  WHERE parent_id IS NOT NULL

  UNION

  SELECT ancestry.start_function_id, parent.parent_id
  FROM ancestry
  JOIN fixture_function_frames AS parent
    ON parent.id = ancestry.function_id
  WHERE parent.parent_id IS NOT NULL
)
SELECT DISTINCT start_function_id AS function_id
FROM ancestry
WHERE start_function_id = function_id;

CREATE PERFETTO TABLE fixture_function_self_counts(
  semantic_id LONG,
  function_id LONG,
  self_sample_count LONG
) AS
SELECT
  column1 AS semantic_id,
  column2 AS function_id,
  column3 AS self_sample_count
FROM (VALUES
  (100, -1, 1),
  (100, 1, 2),
  (100, 2, 3),
  (101, 1, 1));

CREATE PERFETTO TABLE fixture_function_roots AS
SELECT
  semantic_id,
  function_id,
  self_sample_count,
  coalesce((SELECT max(id) FROM fixture_function_frames), -1)
    + row_number() OVER (ORDER BY semantic_id, function_id) AS root_node_id
FROM fixture_function_self_counts
WHERE function_id != -1;

CREATE PERFETTO TABLE fixture_function_graph AS
SELECT id AS source_node_id, parent_id AS dest_node_id, 1 AS edge_weight
FROM fixture_function_frames
WHERE parent_id IS NOT NULL

UNION ALL

SELECT root_node_id, function_id, 1
FROM fixture_function_roots;

CREATE PERFETTO TABLE fixture_function_ancestry AS
SELECT *
FROM graph_reachable_weight_bounded_dfs!((
  SELECT source_node_id, dest_node_id, edge_weight
  FROM fixture_function_graph
), (
  SELECT
    root_node_id,
    (SELECT count(*) + 1 FROM fixture_function_frames) AS root_target_weight
  FROM fixture_function_roots
), 0);

CREATE PERFETTO TABLE fixture_function_inclusive_counts AS
SELECT
  root.semantic_id,
  ancestry.node_id AS function_id,
  sum(root.self_sample_count) AS inclusive_sample_count
FROM fixture_function_ancestry AS ancestry
JOIN fixture_function_roots AS root USING (root_node_id)
JOIN fixture_function_frames AS frame ON frame.id = ancestry.node_id
GROUP BY root.semantic_id, ancestry.node_id

UNION ALL

SELECT semantic_id, function_id, self_sample_count
FROM fixture_function_self_counts
WHERE function_id = -1;

CREATE PERFETTO TABLE fixture_expected_function_counts(
  semantic_id LONG,
  function_id LONG,
  inclusive_sample_count LONG
) AS
SELECT
  column1 AS semantic_id,
  column2 AS function_id,
  column3 AS inclusive_sample_count
FROM (VALUES
  (100, -1, 1),
  (100, 0, 5),
  (100, 1, 2),
  (100, 2, 3),
  (101, 0, 1),
  (101, 1, 1));

WITH checks AS (
  SELECT
    (
      SELECT count(*)
      FROM fixture_semantics AS child
      LEFT JOIN fixture_semantics AS parent ON parent.id = child.parent_id
      WHERE child.parent_id IS NOT NULL
        AND parent.id IS NULL
    ) != 1 AS missing_parent_error,
    (SELECT count(*) FROM fixture_semantic_cycles) != 2 AS cycle_error,
    (
      SELECT count(*)
      FROM fixture_semantics AS child
      JOIN fixture_semantics AS parent ON parent.id = child.parent_id
      WHERE child.operation_id != parent.operation_id
    ) != 1 AS cross_operation_error,
    (
      SELECT count(*)
      FROM (
        SELECT child_id
        FROM fixture_semantic_edges
        GROUP BY child_id
        HAVING count(*) > 1
      )
    ) != 1 AS multiple_parent_error,
    coalesce((
      SELECT sum(attribution = 'direct') FROM fixture_sample_ownership
    ), 0) != 2 AS direct_attribution_error,
    coalesce((
      SELECT sum(attribution = 'ambiguous') FROM fixture_sample_ownership
    ), 0) != 1 AS ambiguous_attribution_error,
    coalesce((
      SELECT sum(attribution = 'unattributed') FROM fixture_sample_ownership
    ), 0) != 1 AS unattributed_error,
    (
      SELECT count(*)
      FROM fixture_sample_ownership
      WHERE (sample_id = 1 AND semantic_id != 13)
        OR (sample_id = 2 AND attribution != 'ambiguous')
        OR (sample_id = 3 AND attribution != 'unattributed')
        OR (sample_id = 4 AND semantic_id != 11)
    ) AS owner_selection_error,
    (
      SELECT count(*) FROM fixture_sample_ownership
    ) != 4 AS attribution_conservation_error,
    (
      SELECT count(*)
      FROM fixture_operation_resolution
      WHERE delta_funnel_sample_attribution(
        operation_count,
        context_leaf_count
      ) != expected
    ) AS operation_resolution_error,
    (
      SELECT count(*)
      FROM fixture_expected_function_counts AS expected
      LEFT JOIN fixture_function_inclusive_counts AS actual
        USING (semantic_id, function_id, inclusive_sample_count)
      WHERE actual.function_id IS NULL
    ) + (
      SELECT count(*)
      FROM fixture_function_inclusive_counts AS actual
      LEFT JOIN fixture_expected_function_counts AS expected
        USING (semantic_id, function_id, inclusive_sample_count)
      WHERE expected.function_id IS NULL
    ) AS function_rollup_error,
    (SELECT count(*) FROM fixture_function_cycles) != 2
      AS function_cycle_error,
    (
      SELECT sum(self_sample_count) FROM fixture_function_self_counts
    ) != 7 AS function_self_conservation_error,
    (
      SELECT count(*)
      FROM fixture_function_self_counts
      WHERE function_id = -1
    ) != 1 AS unresolved_bucket_error,
    (
      SELECT audit_error_count = 0
      FROM delta_funnel_ranked_aggregate_audit
    ) AS empty_profile_audit_error
)
SELECT
  *,
  missing_parent_error
    + cycle_error
    + cross_operation_error
    + multiple_parent_error
    + direct_attribution_error
    + ambiguous_attribution_error
    + unattributed_error
    + owner_selection_error
    + attribution_conservation_error
    + operation_resolution_error
    + function_rollup_error
    + function_cycle_error
    + function_self_conservation_error
    + unresolved_bucket_error
    + empty_profile_audit_error AS fixture_error_count
FROM checks;
