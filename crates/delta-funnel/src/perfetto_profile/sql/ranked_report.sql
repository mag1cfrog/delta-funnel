-- Emit only the compact records required by the Rust report fold.
WITH
  trace_config AS (
    SELECT substr(str_value, instr(str_value, 'name: "linux.perf"')) AS value
    FROM metadata
    WHERE name = 'trace_config_pbtxt'
    LIMIT 1
  ),
  sample_config AS (
    SELECT CAST(substr(
      value,
      instr(value, 'frequency:') + length('frequency:')
    ) AS INTEGER) AS sample_frequency_hz
    FROM trace_config
    WHERE instr(value, 'frequency:') > 0
  ),
  records(record_order, primary_id, secondary_id, record_hex) AS (
    SELECT
      0,
      0,
      0,
      hex(json_object(
        'record_kind', 'metadata',
        'schema_version', 1,
        'sample_frequency_hz', sample_config.sample_frequency_hz,
        'exact_time_unit', 'nanoseconds',
        'sample_unit', 'samples',
        'eligible_sample_count', coverage.eligible_sample_count,
        'direct_sample_count', coverage.direct_sample_count,
        'ambiguous_sample_count', coverage.ambiguous_sample_count,
        'unattributed_sample_count', coverage.unattributed_sample_count,
        'audit_error_count', audit.audit_error_count
      ))
    FROM sample_config
    CROSS JOIN (
      SELECT
        count(*) AS eligible_sample_count,
        coalesce(sum(attribution = 'direct'), 0) AS direct_sample_count,
        coalesce(sum(attribution = 'ambiguous'), 0) AS ambiguous_sample_count,
        coalesce(sum(attribution = 'unattributed'), 0)
          AS unattributed_sample_count
      FROM delta_funnel_ranked_sample_ownership
    ) AS coverage
    CROSS JOIN delta_funnel_ranked_attribution_audit AS audit

    UNION ALL

    SELECT
      1,
      semantic.operation_id,
      semantic.semantic_id,
      hex(json_object(
        'record_kind', 'semantic',
        'semantic_id', semantic.semantic_id,
        'parent_semantic_id', parent.parent_semantic_id,
        'operation_id', semantic.operation_id,
        'name', semantic.name,
        'semantic_kind', CASE
          WHEN semantic.is_operation_root THEN 'operation'
          WHEN semantic.stage_category IS NOT NULL THEN 'stage'
          WHEN semantic.activity IS NOT NULL THEN 'activity'
          WHEN semantic.name IN ('Planning', 'Execution', 'Finalization') THEN 'phase'
          WHEN semantic.name = 'DataFusion query' THEN 'query'
          WHEN semantic.name = 'DataFusion query planning' THEN 'query_planning'
          ELSE 'semantic'
        END,
        'operation_kind', semantic.operation_kind,
        'stage_category', semantic.stage_category,
        'stage_name', semantic.stage_name,
        'activity', semantic.activity,
        'start_ns', semantic.ts,
        'end_ns', semantic.end_ts,
        'duration_ns', semantic.duration_ns,
        'time_semantics', semantic.time_semantics,
        'result', semantic.result,
        'is_complete', semantic.is_complete,
        'query_execution_id', semantic.query_execution_id,
        'query_scope', semantic.query_scope,
        'query_owner', semantic.query_owner,
        'worker_lane_id', semantic.worker_lane_id,
        'worker_kind', semantic.worker_kind,
        'node_id', semantic.node_id,
        'parent_node_id', semantic.parent_node_id,
        'operator_partition', semantic.operator_partition,
        'execution_stream_id', semantic.execution_stream_id,
        'stage_owner_id', semantic.stage_owner_id,
        'direct_sample_count', sample_count.direct_sample_count
      ))
    FROM delta_funnel_ranked_semantics AS semantic
    JOIN delta_funnel_ranked_semantic_parents AS parent USING (semantic_id)
    JOIN delta_funnel_ranked_semantic_sample_counts AS sample_count USING (semantic_id)

    UNION ALL

    SELECT
      2,
      metadata.function_id,
      0,
      hex(json_object(
        'record_kind', 'frame',
        'function_id', metadata.function_id,
        'parent_function_id', metadata.parent_function_id,
        'name', metadata.name,
        'module_name', metadata.module_name,
        'source_file', metadata.source_file,
        'line_number', metadata.line_number,
        'official_self_sample_count', frame.self_count,
        'official_inclusive_sample_count', summary.cumulative_count
      ))
    FROM delta_funnel_ranked_function_metadata AS metadata
    JOIN delta_funnel_ranked_official_function_frames AS frame
      ON frame.id = metadata.function_id
    JOIN delta_funnel_ranked_official_function_summary AS summary
      ON summary.id = metadata.function_id

    UNION ALL

    SELECT
      3,
      self.semantic_id,
      self.function_id,
      hex(json_object(
        'record_kind', 'function_self',
        'semantic_id', self.semantic_id,
        'function_id', self.function_id,
        'self_sample_count', self.self_sample_count
      ))
    FROM delta_funnel_ranked_function_self_counts AS self
  )
SELECT record_hex
FROM records
ORDER BY record_order, primary_id, secondary_id;
