//! Profiled DataFusion object-store transport.

use std::future::Future;

#[cfg(feature = "perfetto-profile")]
use tracing::Instrument;

#[cfg(feature = "perfetto-profile")]
use super::operator_activity::current_datafusion_object_store_transport_span;

#[cfg(feature = "perfetto-profile")]
pub(super) async fn await_object_store_transport<F>(request: F) -> F::Output
where
    F: Future,
{
    match current_datafusion_object_store_transport_span() {
        Some(span) => request.instrument(span).await,
        None => request.await,
    }
}

#[cfg(not(feature = "perfetto-profile"))]
pub(super) async fn await_object_store_transport<F>(request: F) -> F::Output
where
    F: Future,
{
    request.await
}

#[cfg(test)]
mod tests {
    use super::await_object_store_transport;

    #[tokio::test]
    async fn transport_without_an_active_task_preserves_the_output() {
        assert_eq!(await_object_store_transport(async { 42 }).await, 42);
    }
}
