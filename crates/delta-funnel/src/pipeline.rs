//! Query output pipeline helpers.

mod batch_handoff;

pub(crate) use batch_handoff::validate_nonzero_usize_option;
pub use batch_handoff::{
    BatchHandoffError, BatchHandoffOutcome, BatchHandoffStats, BatchPipelinePhase,
    RecordBatchConsumer, handoff_datafusion_query_output, handoff_record_batch_stream,
};
