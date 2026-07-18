//! Shared opt-in Perfetto producer and semantic track adapter for development binaries.

#![allow(
    missing_docs,
    reason = "the Perfetto SDK macro generates undocumented public helpers"
)]

use std::io;
use std::time::{Duration, Instant};

use perfetto_sdk::producer::{Backends, Producer, ProducerInitArgsBuilder};
use perfetto_sdk::protos::trace::track_event::track_descriptor::{
    TrackDescriptorChildTracksOrdering, TrackDescriptorFieldNumber,
    TrackDescriptorSiblingMergeBehavior,
};
use perfetto_sdk::track_event::{
    EventContext, TrackEvent, TrackEventProtoField, TrackEventProtoTrack, TrackEventTrack,
};
use perfetto_sdk::{track_event_categories, track_event_category_enabled};

mod profile_layer;

pub(crate) use profile_layer::{PROFILE_TARGET, PerfettoProfileLayer};

const CATEGORY: &str = "delta_funnel.perfetto_spike";
const CAPTURE_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

track_event_categories! {
    pub mod delta_funnel_perfetto {
        (
            "delta_funnel.perfetto_spike",
            "Delta Funnel Perfetto capability spike",
            []
        ),
    }
}

pub(crate) use delta_funnel_perfetto as perfetto_te_ns;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SemanticTrack {
    pub(crate) name: String,
    pub(crate) uuid: u64,
    pub(crate) parent_uuid: u64,
    pub(crate) sibling_order_rank: u64,
}

impl SemanticTrack {
    fn new(name: String, id: u64, parent_uuid: u64, sibling_order_rank: u64) -> Self {
        let uuid = TrackEventTrack::named_track_uuid(&name, id, parent_uuid);
        Self {
            name,
            uuid,
            parent_uuid,
            sibling_order_rank,
        }
    }

    pub(crate) fn set_on(&self, context: &mut EventContext) {
        let fields = [
            TrackEventProtoField::VarInt(
                TrackDescriptorFieldNumber::ParentUuid as u32,
                self.parent_uuid,
            ),
            TrackEventProtoField::Cstr(TrackDescriptorFieldNumber::Name as u32, &self.name),
            TrackEventProtoField::VarInt(
                TrackDescriptorFieldNumber::DisallowMergingWithSystemTracks as u32,
                1,
            ),
            TrackEventProtoField::VarInt(
                TrackDescriptorFieldNumber::ChildOrdering as u32,
                u64::from(u32::from(TrackDescriptorChildTracksOrdering::Explicit)),
            ),
            TrackEventProtoField::VarInt(
                TrackDescriptorFieldNumber::SiblingOrderRank as u32,
                self.sibling_order_rank,
            ),
            TrackEventProtoField::VarInt(
                TrackDescriptorFieldNumber::SiblingMergeBehavior as u32,
                u64::from(u32::from(
                    TrackDescriptorSiblingMergeBehavior::SiblingMergeBehaviorNone,
                )),
            ),
        ];
        context.set_proto_track(&TrackEventProtoTrack {
            uuid: self.uuid,
            fields: &fields,
        });
    }
}

pub(crate) fn diagnostics_track(process_uuid: u64) -> SemanticTrack {
    SemanticTrack::new("Delta Funnel diagnostics".to_owned(), 0, process_uuid, 10)
}

pub(crate) fn operation_track(operation_id: u64, diagnostics_uuid: u64) -> SemanticTrack {
    SemanticTrack::new(
        format!("Operation [{}]", operation_token(operation_id)),
        operation_id,
        diagnostics_uuid,
        operation_id,
    )
}

pub(crate) fn phase_track(operation_id: u64, operation_uuid: u64) -> SemanticTrack {
    SemanticTrack::new(
        format!("Operation [{}] / phases", operation_token(operation_id)),
        0,
        operation_uuid,
        10,
    )
}

pub(crate) fn query_track(
    operation_id: u64,
    query_execution_id: u64,
    operation_uuid: u64,
) -> SemanticTrack {
    SemanticTrack::new(
        format!(
            "Operation [{}] / query [{}]",
            operation_token(operation_id),
            query_token(query_execution_id)
        ),
        query_execution_id,
        operation_uuid,
        20_u64.saturating_add(query_execution_id),
    )
}

pub(crate) fn worker_track(
    operation_id: u64,
    query_execution_id: u64,
    worker_lane_id: u64,
    query_uuid: u64,
    sibling_order_rank: u64,
) -> SemanticTrack {
    SemanticTrack::new(
        format!(
            "Operation [{}] / query [{}] / worker [{}]",
            operation_token(operation_id),
            query_token(query_execution_id),
            worker_token(worker_lane_id)
        ),
        worker_lane_id,
        query_uuid,
        sibling_order_rank,
    )
}

pub(crate) fn operation_token(id: u64) -> String {
    format!("op-{id:020}")
}

pub(crate) fn query_token(id: u64) -> String {
    format!("q-{id:020}")
}

pub(crate) fn worker_token(id: u64) -> String {
    format!("w-{id:020}")
}

pub(crate) fn initialize_perfetto() -> io::Result<()> {
    let producer_args = ProducerInitArgsBuilder::new().backends(Backends::SYSTEM);
    Producer::init(producer_args.build());
    TrackEvent::init();
    perfetto_te_ns::register().map_err(|error| {
        io::Error::other(format!("failed to register Perfetto category: {error}"))
    })?;
    Ok(())
}

pub(crate) fn wait_for_capture() -> io::Result<()> {
    let deadline = Instant::now() + CAPTURE_WAIT_TIMEOUT;
    while !track_event_category_enabled!("delta_funnel.perfetto_spike") {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "Perfetto category {CATEGORY:?} was not enabled within {CAPTURE_WAIT_TIMEOUT:?}"
                ),
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}
