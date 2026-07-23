-- Correlate target-process native samples with active Delta Funnel contexts.
CREATE PERFETTO TABLE delta_funnel_profile_operations AS
SELECT
  s.id AS operation_slice_id,
  s.name AS operation_name,
  s.ts,
  CASE WHEN s.dur >= 0 THEN s.ts + s.dur ELSE trace_end() END AS end_ts,
  extract_arg(s.arg_set_id, 'debug.operation_id') AS operation_id,
  pt.upid
FROM slice AS s
JOIN process_track AS pt ON pt.id = s.track_id
WHERE s.category = 'delta_funnel.profile'
  AND s.name IN (
    'Delta Funnel preview',
    'Delta Funnel SQL Server write',
    'Delta Funnel SQL Server write_all'
  )
  AND extract_arg(s.arg_set_id, 'debug.operation_id') IS NOT NULL;

-- Task contexts emit their complete identity once. Later entries carry only
-- this stable ID, so resolve it process-wide rather than assuming one thread.
-- Keep this small point-lookup table in SQLite. Its B-tree index makes the
-- much larger context table's identity join efficient.
CREATE TABLE delta_funnel_profile_context_identities AS
SELECT
  identity_thread.upid,
  extract_arg(s.arg_set_id, 'debug.profile_context_id') AS profile_context_id,
  coalesce(extract_arg(s.arg_set_id, 'debug.context_name'), s.name) AS name,
  extract_arg(s.arg_set_id, 'debug.operation_id') AS operation_id,
  extract_arg(s.arg_set_id, 'debug.query_execution_id') AS query_execution_id,
  extract_arg(s.arg_set_id, 'debug.worker_lane_id') AS worker_lane_id,
  extract_arg(s.arg_set_id, 'debug.node_id') AS node_id,
  extract_arg(s.arg_set_id, 'debug.parent_node_id') AS parent_node_id,
  extract_arg(s.arg_set_id, 'debug.operator_partition') AS operator_partition,
  extract_arg(s.arg_set_id, 'debug.execution_stream_id') AS execution_stream_id,
  extract_arg(s.arg_set_id, 'debug.stage_owner_id') AS stage_owner_id,
  extract_arg(s.arg_set_id, 'debug.activity') AS activity
FROM slice AS s
JOIN thread_track AS identity_track ON identity_track.id = s.track_id
JOIN thread AS identity_thread ON identity_thread.utid = identity_track.utid
WHERE s.category = 'delta_funnel.profile.context'
  AND extract_arg(s.arg_set_id, 'debug.profile_context_id') IS NOT NULL
  AND extract_arg(s.arg_set_id, 'debug.operation_id') IS NOT NULL;

CREATE INDEX delta_funnel_profile_context_identity_lookup
ON delta_funnel_profile_context_identities(upid, profile_context_id);

CREATE PERFETTO TABLE delta_funnel_profile_contexts AS
SELECT
  context.id AS context_id,
  context.parent_id,
  coalesce(identity.name, context.name) AS name,
  context.ts,
  CASE WHEN context.dur >= 0 THEN context.ts + context.dur ELSE trace_end() END AS end_ts,
  context.depth,
  context_thread.upid,
  context_track.utid,
  extract_arg(context.arg_set_id, 'debug.profile_context_id') AS profile_context_id,
  coalesce(
    identity.operation_id,
    extract_arg(context.arg_set_id, 'debug.operation_id')
  ) AS operation_id,
  coalesce(
    identity.query_execution_id,
    extract_arg(context.arg_set_id, 'debug.query_execution_id')
  ) AS query_execution_id,
  coalesce(
    identity.worker_lane_id,
    extract_arg(context.arg_set_id, 'debug.worker_lane_id')
  ) AS worker_lane_id,
  coalesce(identity.node_id, extract_arg(context.arg_set_id, 'debug.node_id')) AS node_id,
  coalesce(
    identity.parent_node_id,
    extract_arg(context.arg_set_id, 'debug.parent_node_id')
  ) AS parent_node_id,
  coalesce(
    identity.operator_partition,
    extract_arg(context.arg_set_id, 'debug.operator_partition')
  ) AS operator_partition,
  coalesce(
    identity.execution_stream_id,
    extract_arg(context.arg_set_id, 'debug.execution_stream_id')
  ) AS execution_stream_id,
  coalesce(
    identity.stage_owner_id,
    extract_arg(context.arg_set_id, 'debug.stage_owner_id')
  ) AS stage_owner_id,
  coalesce(identity.activity, extract_arg(context.arg_set_id, 'debug.activity')) AS activity
FROM slice AS context
JOIN thread_track AS context_track ON context_track.id = context.track_id
JOIN thread AS context_thread ON context_thread.utid = context_track.utid
LEFT JOIN delta_funnel_profile_context_identities AS identity
  ON identity.upid = context_thread.upid
  AND identity.profile_context_id = extract_arg(
      context.arg_set_id,
      'debug.profile_context_id'
    )
WHERE context.category = 'delta_funnel.profile.context';

CREATE PERFETTO TABLE delta_funnel_profile_process_samples AS
SELECT sample.*
FROM perf_sample AS sample
JOIN thread USING (utid)
WHERE thread.upid IN (
  SELECT DISTINCT upid FROM delta_funnel_profile_operations
);

CREATE PERFETTO TABLE delta_funnel_operation_samples AS
SELECT
  sample.id AS sample_id,
  sample.ts,
  sample.utid,
  sample.callsite_id,
  sample.unwind_error,
  operation.operation_slice_id,
  operation.operation_id
FROM delta_funnel_profile_process_samples AS sample
JOIN thread USING (utid)
JOIN delta_funnel_profile_operations AS operation
  ON operation.upid = thread.upid
  AND sample.ts >= operation.ts
  AND sample.ts < operation.end_ts;

CREATE PERFETTO INDEX delta_funnel_operation_sample_lookup
ON delta_funnel_operation_samples(sample_id, operation_id);

CREATE PERFETTO TABLE delta_funnel_sample_context_matches AS
SELECT
  sample.sample_id,
  context.*
FROM delta_funnel_operation_samples AS sample
JOIN delta_funnel_profile_contexts AS context
  ON context.operation_id = sample.operation_id
  AND context.utid = sample.utid
  AND sample.ts >= context.ts
  AND sample.ts < context.end_ts;

CREATE PERFETTO INDEX delta_funnel_sample_context_match_lookup
ON delta_funnel_sample_context_matches(sample_id, depth);

-- Perfetto depth includes any intervening thread slices. Selecting the
-- greatest active depth is safer than requiring profile contexts to be direct
-- parent and child slices.
CREATE PERFETTO TABLE delta_funnel_sample_context_leaves AS
SELECT candidate.*
FROM delta_funnel_sample_context_matches AS candidate
WHERE candidate.depth = (
  SELECT max(active.depth)
  FROM delta_funnel_sample_context_matches AS active
  WHERE active.sample_id = candidate.sample_id
);

CREATE PERFETTO INDEX delta_funnel_sample_context_leaf_lookup
ON delta_funnel_sample_context_leaves(sample_id, operation_id);

CREATE PERFETTO FUNCTION delta_funnel_sample_attribution(
  operation_count LONG,
  context_leaf_count LONG
)
RETURNS STRING AS
SELECT CASE
  WHEN $context_leaf_count = 1 THEN 'direct'
  WHEN $operation_count = 1 AND $context_leaf_count = 0 THEN 'unattributed'
  ELSE 'ambiguous'
END;

CREATE PERFETTO TABLE delta_funnel_sample_resolution AS
WITH counts AS (
  SELECT
    sample.sample_id,
    count(DISTINCT sample.operation_slice_id) AS operation_count,
    count(DISTINCT leaf.context_id) AS context_leaf_count
  FROM delta_funnel_operation_samples AS sample
  LEFT JOIN delta_funnel_sample_context_leaves AS leaf
    USING (sample_id, operation_id)
  GROUP BY sample.sample_id
)
SELECT
  sample.sample_id,
  min(sample.ts) AS ts,
  min(sample.utid) AS utid,
  min(sample.callsite_id) AS callsite_id,
  min(sample.unwind_error) AS unwind_error,
  delta_funnel_sample_attribution(
    counts.operation_count,
    counts.context_leaf_count
  ) AS attribution,
  CASE
    WHEN counts.context_leaf_count = 1 THEN min(leaf.operation_id)
    WHEN counts.operation_count = 1
      AND counts.context_leaf_count = 0 THEN min(sample.operation_id)
  END AS operation_id,
  CASE
    WHEN counts.context_leaf_count = 1 THEN min(leaf.context_id)
  END AS context_leaf_id,
  counts.operation_count,
  counts.context_leaf_count
FROM delta_funnel_operation_samples AS sample
JOIN counts USING (sample_id)
LEFT JOIN delta_funnel_sample_context_leaves AS leaf
  USING (sample_id, operation_id)
GROUP BY sample.sample_id;

CREATE PERFETTO TABLE delta_funnel_sample_correlation AS
SELECT
  sample.*,
  leaf.name AS context_leaf_name,
  leaf.query_execution_id,
  leaf.worker_lane_id,
  leaf.node_id,
  leaf.parent_node_id,
  leaf.operator_partition,
  leaf.execution_stream_id,
  leaf.stage_owner_id,
  leaf.activity
FROM delta_funnel_sample_resolution AS sample
LEFT JOIN delta_funnel_sample_context_leaves AS leaf
  ON leaf.sample_id = sample.sample_id
  AND leaf.context_id = sample.context_leaf_id;

-- One row is the reviewable correlation contract for this raw trace. Keeping
-- it as a table lets later aggregate queries reuse the definitions above.
CREATE PERFETTO TABLE delta_funnel_sample_correlation_audit AS
WITH audit AS (
SELECT
  (SELECT count(*) FROM delta_funnel_profile_operations) AS operation_count,
  (SELECT count(*) FROM delta_funnel_profile_process_samples)
    AS profile_process_sample_count,
  count(*) AS eligible_sample_count,
  (SELECT count(*) FROM delta_funnel_profile_process_samples) - count(*)
    AS outside_operation_sample_count,
  coalesce(sum(attribution = 'direct'), 0) AS direct_sample_count,
  coalesce(sum(attribution = 'ambiguous'), 0) AS ambiguous_sample_count,
  coalesce(sum(attribution = 'unattributed'), 0) AS unattributed_sample_count,
  count(*) - coalesce(sum(
    attribution IN ('direct', 'ambiguous', 'unattributed')
  ), 0) AS attribution_conservation_error_count,
  coalesce(sum(callsite_id IS NULL OR unwind_error IS NOT NULL), 0)
    AS native_stack_unavailable_count,
  (
    SELECT count(*)
    FROM (
      SELECT upid, profile_context_id
      FROM delta_funnel_profile_context_identities
      GROUP BY upid, profile_context_id
      HAVING count(*) != 1
    )
  ) AS duplicate_context_identity_count,
  (
    SELECT count(*)
    FROM delta_funnel_profile_contexts
    WHERE profile_context_id IS NOT NULL
      AND operation_id IS NULL
  ) AS unresolved_context_identity_count,
  (
    SELECT count(*) = 0
    FROM delta_funnel_profile_operations
  ) AS missing_operation_error_count
FROM delta_funnel_sample_correlation
)
SELECT
  *,
  attribution_conservation_error_count
    + duplicate_context_identity_count
    + missing_operation_error_count
    + unresolved_context_identity_count AS audit_error_count
FROM audit;
