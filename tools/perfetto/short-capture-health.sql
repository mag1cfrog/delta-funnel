-- One-row health summary for the externally managed short capture workflow.
WITH
semantic_slices AS (
  SELECT
    s.*,
    extract_arg(s.arg_set_id, 'debug.operation_id') AS operation_id,
    extract_arg(s.arg_set_id, 'debug.query_execution_id') AS query_execution_id,
    extract_arg(s.arg_set_id, 'debug.query_scope') AS query_scope,
    extract_arg(s.arg_set_id, 'debug.worker_lane_id') AS worker_lane_id,
    extract_arg(s.arg_set_id, 'debug.worker_kind') AS worker_kind,
    extract_arg(s.arg_set_id, 'debug.node_id') AS node_id,
    extract_arg(s.arg_set_id, 'debug.operator_partition') AS operator_partition,
    extract_arg(s.arg_set_id, 'debug.execution_stream_id') AS execution_stream_id,
    extract_arg(s.arg_set_id, 'debug.activity') AS activity,
    extract_arg(s.arg_set_id, 'debug.time_semantics') AS time_semantics,
    extract_arg(s.arg_set_id, 'debug.result') AS result
  FROM slice AS s
  WHERE s.category = 'delta_funnel.profile'
),
classified_slices AS (
  SELECT
    *,
    CASE
      WHEN name IN (
        'Delta Funnel preview',
        'Delta Funnel SQL Server write',
        'Delta Funnel SQL Server write_all'
      ) THEN 'operation'
      WHEN name IN ('Planning', 'Execution', 'Finalization') THEN 'phase'
      WHEN name = 'DataFusion query' THEN 'query'
      WHEN name = 'DataFusion query planning' THEN 'query_planning'
      WHEN worker_lane_id IS NOT NULL THEN 'operator'
    END AS semantic_kind
  FROM semantic_slices
),
missing_fields AS (
  SELECT
    coalesce(sum(
      CASE semantic_kind
        WHEN 'operation' THEN
          (operation_id IS NULL) + (time_semantics IS NULL) + (result IS NULL)
        WHEN 'phase' THEN
          (operation_id IS NULL) + (time_semantics IS NULL) + (result IS NULL)
        WHEN 'query' THEN
          (operation_id IS NULL) + (query_execution_id IS NULL) +
          (query_scope IS NULL) + (time_semantics IS NULL)
        WHEN 'query_planning' THEN
          (operation_id IS NULL) + (query_execution_id IS NULL) +
          (query_scope IS NULL) + (time_semantics IS NULL) + (result IS NULL)
        WHEN 'operator' THEN
          (operation_id IS NULL) + (query_execution_id IS NULL) +
          (query_scope IS NULL) + (worker_lane_id IS NULL) +
          (worker_kind IS NULL) + (node_id IS NULL) +
          (operator_partition IS NULL) + (execution_stream_id IS NULL) +
          (activity IS NULL) + (time_semantics IS NULL) + (result IS NULL)
        ELSE 0
      END
    ), 0) AS missing_canonical_field_values
  FROM classified_slices
),
ordered_slices AS (
  SELECT
    id,
    ts,
    dur,
    track_id,
    depth,
    max(ts + dur) OVER (
      PARTITION BY track_id, depth
      ORDER BY ts, id
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS previous_end
  FROM semantic_slices
  WHERE dur > 0
),
crossing_slice_ids AS (
  SELECT id
  FROM ordered_slices
  WHERE previous_end > ts
  UNION
  SELECT child.id
  FROM semantic_slices AS child
  JOIN semantic_slices AS parent ON parent.id = child.parent_id
  WHERE child.dur > 0
    AND parent.dur > 0
    AND child.ts + child.dur > parent.ts + parent.dur
),
profile_counts AS (
  SELECT
    coalesce(sum(semantic_kind = 'operation'), 0) AS operation_roots,
    coalesce(sum(semantic_kind = 'operation' AND dur < 0), 0)
      AS incomplete_operation_roots,
    coalesce(sum(semantic_kind = 'phase'), 0) AS phase_slices,
    coalesce(sum(semantic_kind = 'query'), 0) AS queries,
    count(DISTINCT CASE
      WHEN semantic_kind = 'operator' THEN
        printf('%lld:%lld:%lld', operation_id, query_execution_id, worker_lane_id)
    END) AS workers,
    coalesce(sum(semantic_kind = 'operator'), 0) AS operator_slices,
    coalesce(sum(name = 'Operator activity trace truncated'), 0) AS truncation_markers,
    coalesce(sum(dur < 0), 0) AS incomplete_semantic_slices
  FROM classified_slices
),
sample_counts AS (
  SELECT
    count(*) AS native_samples,
    count(callsite_id) AS samples_with_call_sites,
    count(*) - count(callsite_id) AS samples_without_call_sites,
    coalesce(sum(unwind_error IS NOT NULL), 0) AS unwind_errors,
    count(DISTINCT CASE WHEN callsite_id IS NOT NULL THEN t.upid END)
      AS sampled_processes,
    round(
      (max(CASE WHEN callsite_id IS NOT NULL THEN ts END) -
       min(CASE WHEN callsite_id IS NOT NULL THEN ts END)) / 1000000.0,
      3
    ) AS sampled_process_span_ms
  FROM perf_sample AS s
  LEFT JOIN thread AS t USING (utid)
),
stat_counts AS (
  SELECT
    coalesce(sum(CASE
      WHEN name GLOB 'traced_buf_*' AND severity = 'data_loss' THEN value
      ELSE 0
    END), 0) AS buffer_loss_events,
    coalesce(sum(CASE
      WHEN severity = 'data_loss'
        AND name NOT GLOB 'traced_buf_*'
        AND name NOT IN ('traced_flushes_failed', 'traced_final_flush_failed')
      THEN value
      ELSE 0
    END), 0) AS data_source_loss_events,
    coalesce(sum(CASE
      WHEN name IN ('traced_flushes_failed', 'traced_final_flush_failed') THEN value
      ELSE 0
    END), 0) AS flush_failures,
    coalesce(sum(CASE WHEN name = 'traced_flushes_succeeded' THEN value ELSE 0 END), 0)
      AS successful_flushes,
    coalesce(sum(CASE WHEN name = 'perf_samples_skipped' THEN value ELSE 0 END), 0)
      AS skipped_samples,
    coalesce(sum(CASE WHEN name = 'traced_final_flush_succeeded' THEN value ELSE 0 END), 0)
      AS final_flush_succeeded,
    coalesce(sum(CASE WHEN name = 'traced_final_flush_failed' THEN value ELSE 0 END), 0)
      AS final_flush_failed
  FROM stats
)
SELECT
  CASE
    WHEN operation_roots > 0
      AND incomplete_operation_roots = 0
      AND truncation_markers = 0
      AND missing_canonical_field_values = 0
      AND incomplete_semantic_slices = 0
      AND (SELECT count(*) FROM crossing_slice_ids) = 0
      AND buffer_loss_events = 0
      AND flush_failures = 0
    THEN 'complete'
    ELSE 'incomplete'
  END AS semantic_health,
  operation_roots,
  incomplete_operation_roots,
  phase_slices,
  queries,
  workers,
  operator_slices,
  truncation_markers,
  missing_canonical_field_values,
  incomplete_semantic_slices,
  (SELECT count(*) FROM crossing_slice_ids) AS crossing_semantic_slices,
  (SELECT count(*) FROM sched) AS scheduler_rows,
  native_samples,
  samples_with_call_sites,
  samples_without_call_sites,
  unwind_errors,
  skipped_samples,
  sampled_processes,
  sampled_process_span_ms,
  buffer_loss_events,
  data_source_loss_events,
  flush_failures,
  successful_flushes,
  CASE
    WHEN final_flush_failed > 0 THEN 'failed'
    WHEN final_flush_succeeded > 0 THEN 'succeeded'
    ELSE 'not_reported'
  END AS trace_finalization_status
FROM profile_counts, missing_fields, sample_counts, stat_counts;
