//! Nested wall-clock activity for DataFusion physical planning.

use std::future::Future;

use tracing::Instrument;

use super::QueryTraceIdentity;

pub(crate) async fn with_query_planning_activity<F, T, E>(
    identity: QueryTraceIdentity,
    future: F,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let process_span = process_query_planning_span(&identity);
    let result = future.instrument(process_span.span.clone()).await;
    process_span.finish(if result.is_ok() { "ok" } else { "error" });
    result
}

fn process_query_planning_span(identity: &QueryTraceIdentity) -> ProcessPlanningSpan {
    let parent = identity.process_root_span();
    let span = tracing::trace_span!(
        target: crate::profiling::PROFILE_TARGET,
        parent: parent,
        "DataFusion query planning",
        operation_id = identity.operation_id(),
        query_execution_id = identity.query_execution_id(),
        query_scope = identity.query_scope().as_str(),
        query_owner = identity.query_owner(),
        result = tracing::field::Empty,
        time_semantics = "wall_clock",
    );
    ProcessPlanningSpan {
        span,
        result_recorded: false,
    }
}

struct ProcessPlanningSpan {
    span: tracing::Span,
    result_recorded: bool,
}

impl ProcessPlanningSpan {
    fn finish(mut self, result: &'static str) {
        self.span.record("result", result);
        self.result_recorded = true;
    }
}

impl Drop for ProcessPlanningSpan {
    fn drop(&mut self) {
        if !self.result_recorded {
            self.span.record("result", "cancelled");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use datafusion::prelude::SessionContext;

    use crate::{
        QueryExecutionScope, observability::test_capture::TracingCapture,
        profiling::OperationTraceContext,
        query_engine::datafusion::test_support::register_fixture_source,
    };

    use super::*;

    #[tokio::test]
    async fn planning_activity_records_identity_and_success() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let context =
            OperationTraceContext::start_for_test(true).expect("profile tracing should start");
        let identity = QueryTraceIdentity::new(
            context.clone(),
            QueryExecutionScope::Preview,
            Some("orders"),
        )
        .expect("query identity should be available");

        let result: Result<(), &'static str> =
            with_query_planning_activity(identity, async { Ok(()) }).await;
        assert_eq!(result, Ok(()));
        context.record_process_result("ok");
        drop(context);

        let spans = capture.captured().spans();
        let planning = spans
            .iter()
            .find(|span| span.name == "DataFusion query planning")
            .expect("query planning should be captured");
        assert_eq!(planning.fields["query_scope"], "preview");
        assert_eq!(planning.fields["query_owner"], "orders");
        assert_eq!(planning.fields["result"], "ok");
    }

    #[tokio::test]
    async fn planning_activity_records_failures() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let context =
            OperationTraceContext::start_for_test(true).expect("profile tracing should start");
        let identity = QueryTraceIdentity::new(context.clone(), QueryExecutionScope::Preview, None)
            .expect("query identity should be available");

        let result =
            with_query_planning_activity(identity, async { Err::<(), _>("planning failed") }).await;
        assert_eq!(result, Err("planning failed"));
        drop(context);

        let spans = capture.captured().spans();
        let planning = spans
            .iter()
            .find(|span| span.name == "DataFusion query planning")
            .expect("query planning should be captured");
        assert_eq!(planning.fields["result"], "error");
    }

    #[tokio::test]
    async fn standalone_delta_planning_spans_remain_under_query_planning()
    -> Result<(), Box<dyn Error>> {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let context = SessionContext::new();
        let _table = register_fixture_source(&context, "orders", "planning-spans")?;
        let dataframe = context.sql("select * from orders").await?;
        let operation =
            OperationTraceContext::start_for_test(true).ok_or("profile tracing should start")?;
        let identity = QueryTraceIdentity::new(
            operation.clone(),
            QueryExecutionScope::Preview,
            Some("orders"),
        )
        .ok_or("query identity should be available")?;

        with_query_planning_activity(identity, dataframe.create_physical_plan()).await?;
        operation.record_process_result("ok");
        drop(operation);

        let spans = capture.captured().spans();
        let planning = spans
            .iter()
            .find(|span| span.name == "DataFusion query planning")
            .ok_or("query planning span was not captured")?;
        for name in [
            "Delta scan planning",
            "Delta projection planning",
            "Delta filter planning",
            "Delta Kernel scan construction",
            "Delta scan metadata expansion",
            "Delta file task partitioning",
            "Delta partition target selection",
            "Delta scan execution setup",
        ] {
            let span = spans
                .iter()
                .find(|span| span.target == "delta_arrow_reader::profile" && span.name == name)
                .ok_or_else(|| format!("{name} span was not captured"))?;
            let mut parent_id = span.parent_id;
            while parent_id.is_some() && parent_id != Some(planning.id) {
                parent_id = spans
                    .iter()
                    .find(|parent| Some(parent.id) == parent_id)
                    .and_then(|parent| parent.parent_id);
            }
            assert_eq!(parent_id, Some(planning.id), "{name}");
            assert!(span.closed, "{name}");
        }
        Ok(())
    }
}
