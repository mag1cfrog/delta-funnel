-- Exact semantic stages used by tools/perfetto/semantic-parity.
WITH semantic_slices AS (
  SELECT
    s.id,
    s.parent_id,
    s.name,
    s.ts,
    s.dur,
    extract_arg(s.arg_set_id, 'debug.operation_id') AS operation_id,
    extract_arg(s.arg_set_id, 'debug.query_execution_id') AS query_execution_id,
    extract_arg(s.arg_set_id, 'debug.query_scope') AS query_scope,
    extract_arg(s.arg_set_id, 'debug.query_owner') AS query_owner,
    extract_arg(s.arg_set_id, 'debug.operator_partition') AS operator_partition,
    extract_arg(s.arg_set_id, 'debug.execution_stream_id') AS execution_stream_id,
    extract_arg(s.arg_set_id, 'debug.activity') AS activity,
    extract_arg(s.arg_set_id, 'debug.planning_activity_name')
      AS planning_activity_name,
    extract_arg(s.arg_set_id, 'debug.execution_activity_name')
      AS execution_activity_name,
    extract_arg(s.arg_set_id, 'debug.operation_kind') AS operation_kind,
    extract_arg(s.arg_set_id, 'debug.stage_name') AS stage_name,
    extract_arg(s.arg_set_id, 'debug.stage_category') AS stage_category,
    extract_arg(s.arg_set_id, 'debug.stage_owner_id') AS stage_owner_id,
    extract_arg(s.arg_set_id, 'debug.result') AS result,
    extract_arg(s.arg_set_id, 'debug.time_semantics') AS time_semantics
  FROM slice AS s
  WHERE s.category = 'delta_funnel.profile'
),
classified AS (
  SELECT
    *,
    CASE
      WHEN name = 'Delta Funnel preview' THEN 'operation'
      WHEN stage_name IS NOT NULL THEN 'stage'
      WHEN planning_activity_name IS NOT NULL THEN 'planning_activity'
      WHEN execution_activity_name IS NOT NULL THEN 'execution_activity'
      WHEN name = 'DataFusion query planning' THEN 'context'
      WHEN name IN ('Planning', 'Execution', 'Finalization') THEN 'context'
    END AS semantic_kind
  FROM semantic_slices
)
SELECT
  id,
  parent_id,
  semantic_kind,
  name,
  CASE semantic_kind
    WHEN 'operation' THEN 'delta_funnel.operation'
    WHEN 'stage' THEN stage_category
    WHEN 'planning_activity' THEN 'datafusion.planning.activity'
    WHEN 'execution_activity' THEN 'datafusion.execution.activity'
  END AS category,
  ts,
  dur,
  operation_id,
  query_execution_id,
  query_scope,
  query_owner,
  operator_partition,
  execution_stream_id,
  activity,
  operation_kind,
  stage_owner_id,
  result,
  time_semantics
FROM classified
WHERE semantic_kind IS NOT NULL
ORDER BY ts, id;
