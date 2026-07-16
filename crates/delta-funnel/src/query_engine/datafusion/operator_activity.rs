//! Wall-clock activity spans for finalized DataFusion physical plans.

use std::{
    any::Any,
    collections::HashMap,
    fmt,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    common::Result as DataFusionResult,
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
        SendableRecordBatchStream, metrics::MetricsSet,
    },
};
use futures_util::Stream;
use serde_json::Value;

use crate::{
    report::{OperationTimelineRecorder, OperationTimelineSpanRecorder},
    usize_to_u64_saturating,
};

const OPERATOR_ACTIVITY_CATEGORY: &str = "datafusion.operator.activity";
const MAX_OPERATOR_ACTIVITY_SPANS: u64 = 100_000;

#[derive(Debug, Clone)]
struct OperatorActivityRecorder {
    timeline: OperationTimelineRecorder,
    maximum_spans: u64,
    remaining_spans: Arc<AtomicU64>,
    truncation_reported: Arc<AtomicBool>,
}

impl OperatorActivityRecorder {
    fn new(timeline: OperationTimelineRecorder) -> Self {
        Self::with_max_spans(timeline, MAX_OPERATOR_ACTIVITY_SPANS)
    }

    fn with_max_spans(timeline: OperationTimelineRecorder, maximum_spans: u64) -> Self {
        Self {
            timeline,
            maximum_spans,
            remaining_spans: Arc::new(AtomicU64::new(maximum_spans)),
            truncation_reported: Arc::new(AtomicBool::new(false)),
        }
    }

    fn start_span(
        &self,
        operator_name: &str,
        node_id: u64,
        parent_node_id: Option<u64>,
        partition: usize,
        activity: &'static str,
    ) -> Option<OperationTimelineSpanRecorder> {
        if self
            .remaining_spans
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_err()
        {
            self.report_truncation();
            return None;
        }

        Some(
            self.timeline
                .start_span(
                    operator_name,
                    OPERATOR_ACTIVITY_CATEGORY,
                    activity_track_name(partition),
                )
                .with_attribute("node_id", Value::from(node_id))
                .with_attribute(
                    "parent_node_id",
                    parent_node_id.map_or(Value::Null, Value::from),
                )
                .with_attribute("partition", Value::from(usize_to_u64_saturating(partition)))
                .with_attribute("activity", Value::String(activity.to_owned())),
        )
    }

    fn report_truncation(&self) {
        if !self.truncation_reported.swap(true, Ordering::Relaxed) {
            self.timeline
                .start_span(
                    "Operator activity trace truncated",
                    OPERATOR_ACTIVITY_CATEGORY,
                    "DataFusion operator activity",
                )
                .with_attribute("maximum_spans", Value::from(self.maximum_spans))
                .completed();
        }
    }
}

fn activity_track_name(partition: usize) -> String {
    let thread = std::thread::current();
    let thread_id = format!("{:?}", thread.id());
    let worker = thread
        .name()
        .map_or_else(|| thread_id.clone(), |name| format!("{name} ({thread_id})"));
    format!("DataFusion partition {partition} / worker {worker}")
}

/// Adds transparent execute and poll instrumentation to one finalized plan.
pub(super) fn instrument_query_execution_plan(
    root: Arc<dyn ExecutionPlan>,
    timeline: OperationTimelineRecorder,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let activity = OperatorActivityRecorder::new(timeline);
    let mut next_node_id = 0;
    let mut instrumented = HashMap::new();
    instrument_query_execution_node(root, None, &mut next_node_id, &mut instrumented, &activity)
}

fn instrument_query_execution_node(
    plan: Arc<dyn ExecutionPlan>,
    parent_node_id: Option<u64>,
    next_node_id: &mut u64,
    instrumented: &mut HashMap<usize, Arc<dyn ExecutionPlan>>,
    activity: &OperatorActivityRecorder,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let identity = plan_identity(&plan);
    if let Some(plan) = instrumented.get(&identity) {
        return Ok(Arc::clone(plan));
    }

    let node_id = *next_node_id;
    *next_node_id = next_node_id.saturating_add(1);
    let children = plan
        .children()
        .into_iter()
        .map(Arc::clone)
        .map(|child| {
            instrument_query_execution_node(
                child,
                Some(node_id),
                next_node_id,
                instrumented,
                activity,
            )
        })
        .collect::<DataFusionResult<Vec<_>>>()?;
    let inner = plan.with_new_children(children)?;
    let plan: Arc<dyn ExecutionPlan> = Arc::new(ProfiledOperatorExec {
        inner,
        node_id,
        parent_node_id,
        activity: activity.clone(),
    });
    instrumented.insert(identity, Arc::clone(&plan));
    Ok(plan)
}

fn plan_identity(plan: &Arc<dyn ExecutionPlan>) -> usize {
    Arc::as_ptr(plan) as *const () as usize
}

pub(super) fn unprofiled_execution_plan(plan: &dyn ExecutionPlan) -> &dyn ExecutionPlan {
    plan.as_any()
        .downcast_ref::<ProfiledOperatorExec>()
        .map_or(plan, |profiled| profiled.inner.as_ref())
}

#[derive(Debug)]
struct ProfiledOperatorExec {
    inner: Arc<dyn ExecutionPlan>,
    node_id: u64,
    parent_node_id: Option<u64>,
    activity: OperatorActivityRecorder,
}

impl DisplayAs for ProfiledOperatorExec {
    fn fmt_as(
        &self,
        display_type: DisplayFormatType,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        self.inner.fmt_as(display_type, formatter)
    }
}

impl ExecutionPlan for ProfiledOperatorExec {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let inner = Arc::clone(&self.inner).with_new_children(children)?;
        Ok(Arc::new(Self {
            inner,
            node_id: self.node_id,
            parent_node_id: self.parent_node_id,
            activity: self.activity.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let span = self.activity.start_span(
            self.name(),
            self.node_id,
            self.parent_node_id,
            partition,
            "execute",
        );
        let result = self.inner.execute(partition, context);
        if let Some(span) = span {
            let span = span.with_attribute(
                "result",
                Value::String(if result.is_ok() { "stream" } else { "error" }.to_owned()),
            );
            if result.is_ok() {
                span.completed();
            } else {
                span.failed();
            }
        }
        result.map(|inner| {
            Box::pin(ProfiledRecordBatchStream {
                schema: inner.schema(),
                inner,
                operator_name: self.name().to_owned(),
                node_id: self.node_id,
                parent_node_id: self.parent_node_id,
                partition,
                activity: self.activity.clone(),
            }) as SendableRecordBatchStream
        })
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }
}

struct ProfiledRecordBatchStream {
    schema: SchemaRef,
    inner: SendableRecordBatchStream,
    operator_name: String,
    node_id: u64,
    parent_node_id: Option<u64>,
    partition: usize,
    activity: OperatorActivityRecorder,
}

impl Stream for ProfiledRecordBatchStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let span = self.activity.start_span(
            &self.operator_name,
            self.node_id,
            self.parent_node_id,
            self.partition,
            "poll_next",
        );
        let poll = self.inner.as_mut().poll_next(context);
        if let Some(span) = span {
            let (result, failed) = match &poll {
                Poll::Pending => ("pending", false),
                Poll::Ready(Some(Ok(_))) => ("batch", false),
                Poll::Ready(Some(Err(_))) => ("error", true),
                Poll::Ready(None) => ("eof", false),
            };
            let span = span.with_attribute("result", Value::String(result.to_owned()));
            if failed {
                span.failed();
            } else {
                span.completed();
            }
        }
        poll
    }
}

impl RecordBatchStream for ProfiledRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use datafusion::{physical_plan::collect, prelude::SessionContext};

    use crate::{
        QueryExecutionOutcome, QueryExecutionScope, TimelineSpanStatus, TimelineSpanTimeSemantics,
        query_engine::datafusion::execution_profile::collect_query_execution_profile,
    };

    use super::*;

    #[test]
    fn activity_limit_records_one_visible_truncation_marker() {
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::with_max_spans(timeline.clone(), 1);

        activity
            .start_span("FilterExec", 0, None, 0, "poll_next")
            .expect("first activity should fit")
            .completed();
        assert!(
            activity
                .start_span("FilterExec", 0, None, 0, "poll_next")
                .is_none()
        );
        assert!(
            activity
                .start_span("FilterExec", 0, None, 0, "poll_next")
                .is_none()
        );

        let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
        assert_eq!(
            timeline
                .spans()
                .iter()
                .filter(|span| span.name() == "Operator activity trace truncated")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn instrumented_plan_preserves_results_and_records_nested_activity()
    -> Result<(), Box<dyn Error>> {
        let context = SessionContext::new();
        let dataframe = context
            .sql("select 1 as id union all select 2 as id")
            .await?;
        let task_context = Arc::new(dataframe.task_ctx());
        let plan = dataframe.create_physical_plan().await?;
        let timeline = OperationTimelineRecorder::start();
        let plan = instrument_query_execution_plan(plan, timeline.clone())?;

        let batches = collect(Arc::clone(&plan), task_context).await?;

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let profile = collect_query_execution_profile(
            &plan,
            QueryExecutionScope::Preview,
            QueryExecutionOutcome::Success,
            2,
            None,
        );
        let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
        let spans = timeline
            .spans()
            .iter()
            .filter(|span| span.category() == OPERATOR_ACTIVITY_CATEGORY)
            .collect::<Vec<_>>();
        assert!(!spans.is_empty());
        assert!(spans.iter().all(|span| {
            let Some(partition) = span.attributes()["partition"].as_u64() else {
                return false;
            };
            span.time_semantics() == TimelineSpanTimeSemantics::WallClock
                && span
                    .track_name()
                    .starts_with(&format!("DataFusion partition {partition} / worker "))
                && matches!(
                    span.attributes()["activity"].as_str(),
                    Some("execute" | "poll_next")
                )
        }));
        for span in &spans {
            let node_id = span.attributes()["node_id"]
                .as_u64()
                .ok_or("expected activity node ID")?;
            let operator = profile
                .operators()
                .iter()
                .find(|operator| operator.node_id() == node_id)
                .ok_or("expected matching profile operator")?;
            assert_eq!(span.name(), operator.operator_name());
            assert_eq!(
                span.attributes()["parent_node_id"].as_u64(),
                operator.parent_node_id()
            );
        }
        assert!(spans.iter().any(|outer| {
            let outer_end = outer
                .start_offset_micros()
                .saturating_add(outer.duration_micros());
            spans.iter().any(|inner| {
                let inner_end = inner
                    .start_offset_micros()
                    .saturating_add(inner.duration_micros());
                outer.id() != inner.id()
                    && outer.track_name() == inner.track_name()
                    && outer.start_offset_micros() <= inner.start_offset_micros()
                    && outer_end >= inner_end
            })
        }));

        Ok(())
    }
}
