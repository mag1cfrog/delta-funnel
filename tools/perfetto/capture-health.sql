-- One-row health summary for short and streaming Delta Funnel captures.
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
      WHEN worker_lane_id IS NOT NULL
        OR worker_kind IS NOT NULL
        OR node_id IS NOT NULL
        OR operator_partition IS NOT NULL
        OR execution_stream_id IS NOT NULL
      THEN 'operator'
      WHEN name IN (
        'Delta Funnel preview',
        'Delta Funnel SQL Server write',
        'Delta Funnel SQL Server write_all'
      ) THEN 'operation'
      WHEN name IN ('Planning', 'Execution', 'Finalization') THEN 'phase'
      WHEN name = 'DataFusion query' THEN 'query'
      WHEN name = 'DataFusion query planning' THEN 'query_planning'
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
    ), 0) AS missing_canonical_field_count
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
  FROM classified_slices
  WHERE semantic_kind = 'operator' AND dur > 0
),
crossing_worker_slice_ids AS (
  SELECT id
  FROM ordered_slices
  WHERE previous_end > ts
  UNION
  SELECT child.id
  FROM classified_slices AS child
  JOIN classified_slices AS parent ON parent.id = child.parent_id
  WHERE child.semantic_kind = 'operator'
    AND parent.semantic_kind = 'operator'
    AND child.dur > 0
    AND parent.dur > 0
    AND child.ts + child.dur > parent.ts + parent.dur
),
profile_counts AS (
  SELECT
    coalesce(sum(semantic_kind = 'operation'), 0) AS operation_root_count,
    coalesce(sum(semantic_kind = 'operation' AND dur < 0), 0)
      AS incomplete_operation_root_count,
    coalesce(sum(semantic_kind = 'operator'), 0) AS operator_slice_count,
    coalesce(sum(name = 'Operator activity trace truncated'), 0)
      AS truncation_marker_count
  FROM classified_slices
),
sample_counts AS (
  SELECT
    count(*) AS perf_sample_count,
    count(*) - count(callsite_id) AS perf_sample_without_callsite_count
  FROM perf_sample
),
lifecycle_counts AS (
  SELECT coalesce(sum(name = 'tracing_disabled_ns'), 0) AS tracing_disabled_count
  FROM metadata
),
stat_counts AS (
  SELECT
    coalesce(sum(CASE
      WHEN name GLOB 'traced_buf_*' AND severity = 'data_loss' THEN value
      ELSE 0
    END), 0) AS buffer_loss_count,
    -- Every checked-in config reserves buffer 0 for exact semantic events.
    coalesce(sum(CASE
      WHEN name GLOB 'traced_buf_*'
        AND severity = 'data_loss'
        AND idx = 0
      THEN value
      ELSE 0
    END), 0) AS semantic_buffer_loss_count,
    coalesce(sum(CASE
      WHEN severity = 'data_loss'
        AND name NOT GLOB 'traced_buf_*'
        AND name NOT IN (
          'perf_samples_skipped',
          'traced_flushes_failed',
          'traced_final_flush_failed'
        )
      THEN value
      ELSE 0
    END), 0) AS data_source_loss_count,
    coalesce(sum(CASE
      WHEN name = 'traced_flushes_failed' THEN value
      ELSE 0
    END), 0) AS failed_flush_count,
    coalesce(sum(CASE
      WHEN name = 'perf_samples_skipped' THEN value
      ELSE 0
    END), 0) AS perf_samples_skipped,
    coalesce(sum(CASE
      WHEN name = 'traced_final_flush_failed' THEN value
      ELSE 0
    END), 0) AS final_flush_failed
  FROM stats
),
trace_metrics AS (
  SELECT round((end_ts - start_ts) / 1000000000.0, 6) AS trace_duration_seconds
  FROM trace_bounds
),
health_values AS (
  SELECT
    operation_root_count > 0
      AND incomplete_operation_root_count = 0
      AND missing_canonical_field_count = 0
      AND (SELECT count(*) FROM crossing_worker_slice_ids) = 0
      AND semantic_buffer_loss_count = 0
      AS semantic_complete,
    tracing_disabled_count > 0 AS finalization_observed,
    max(failed_flush_count, final_flush_failed) AS flush_failure_count,
    operation_root_count,
    incomplete_operation_root_count,
    operator_slice_count,
    truncation_marker_count,
    missing_canonical_field_count,
    (SELECT count(*) FROM crossing_worker_slice_ids)
      AS crossing_worker_slice_count,
    perf_sample_count,
    perf_sample_without_callsite_count,
    perf_samples_skipped,
    buffer_loss_count,
    data_source_loss_count,
    trace_duration_seconds
  FROM
    profile_counts,
    missing_fields,
    sample_counts,
    lifecycle_counts,
    stat_counts,
    trace_metrics
)
SELECT
  semantic_complete
    AND finalization_observed
    AND flush_failure_count = 0
    AS capture_complete,
  semantic_complete,
  operation_root_count,
  incomplete_operation_root_count,
  operator_slice_count,
  truncation_marker_count,
  missing_canonical_field_count,
  crossing_worker_slice_count,
  perf_sample_count,
  perf_sample_without_callsite_count,
  perf_samples_skipped,
  buffer_loss_count,
  data_source_loss_count,
  flush_failure_count,
  finalization_observed,
  trace_duration_seconds,
  configured_file_cap_bytes,
  saved_file_bytes
FROM health_values, delta_funnel_capture_health_input;
