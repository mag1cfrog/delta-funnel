use std::fs::File;
use std::io::{Read, Write};
use std::mem;
use std::ops::Range;
use std::path::Path;

use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

use super::ranked_report::{
    ArchivedRankedProfileDocument, RANKED_PROFILE_SCHEMA_VERSION, RankedProfileDocument,
};
use super::report_cli::{RankedReportFailure, RankedReportFailurePhase};
use super::report_html::ExistingOutputPolicy;

const MAGIC: [u8; 8] = *b"DFPROF\0\0";
const ARTIFACT_VERSION: u16 = 1;
const HEADER_LENGTH: usize = 64;
// Version 1 means rkyv 0.8, aligned little-endian primitives, and 32-bit
// relative pointers. Cargo features pin each format-affecting choice.
const RKYV_FORMAT_CONFIG: u32 = 1;
// The largest production fixture is 51 MiB. This leaves ten times that
// measured headroom while bounding allocation before parsing untrusted data.
const MAX_ARTIFACT_BYTES: usize = 512 * 1024 * 1024;

const ARTIFACT_VERSION_RANGE: Range<usize> = 8..10;
const HEADER_LENGTH_RANGE: Range<usize> = 10..12;
const SCHEMA_VERSION_RANGE: Range<usize> = 12..16;
const RKYV_CONFIG_RANGE: Range<usize> = 16..20;
const RESERVED_WORD_RANGE: Range<usize> = 20..24;
const PAYLOAD_OFFSET_RANGE: Range<usize> = 24..32;
const PAYLOAD_LENGTH_RANGE: Range<usize> = 32..40;
const RESERVED_TAIL_RANGE: Range<usize> = 40..HEADER_LENGTH;

#[derive(Debug)]
pub(super) struct RankedProfileArtifact {
    bytes: AlignedVec<16>,
    payload: Range<usize>,
}

impl RankedProfileArtifact {
    pub(super) fn document(&self) -> Result<&ArchivedRankedProfileDocument, RankedReportFailure> {
        rkyv::access::<ArchivedRankedProfileDocument, RkyvError>(&self.bytes[self.payload.clone()])
            .map_err(|_| {
                input_failure("invalid_archive", "artifact payload is not a valid archive")
            })
    }

    pub(super) fn into_document(self) -> Result<RankedProfileDocument, RankedReportFailure> {
        rkyv::deserialize::<RankedProfileDocument, RkyvError>(self.document()?).map_err(|_| {
            input_failure(
                "artifact_deserialize_failed",
                "artifact payload could not be materialized",
            )
        })
    }
}

pub(super) fn has_ranked_profile_artifact_magic(input: &Path) -> Result<bool, RankedReportFailure> {
    let mut file = File::open(input)
        .map_err(|_| input_failure("input_unreadable", "profile input could not be read"))?;
    let mut magic = [0_u8; MAGIC.len()];
    match file.read_exact(&mut magic) {
        Ok(()) => Ok(magic == MAGIC),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(_) => Err(input_failure(
            "input_unreadable",
            "profile input could not be read",
        )),
    }
}

pub(super) fn read_ranked_profile_artifact(
    input: &Path,
) -> Result<RankedProfileArtifact, RankedReportFailure> {
    let mut file = File::open(input)
        .map_err(|_| input_failure("artifact_unreadable", "artifact could not be read"))?;
    let file_length = file
        .metadata()
        .map_err(|_| input_failure("artifact_unreadable", "artifact could not be inspected"))?
        .len();
    let file_length = usize::try_from(file_length).map_err(|_| artifact_too_large())?;
    if file_length > MAX_ARTIFACT_BYTES {
        return Err(artifact_too_large());
    }
    if file_length < HEADER_LENGTH {
        return Err(input_failure(
            "truncated_artifact",
            "artifact is shorter than its fixed header",
        ));
    }

    let mut bytes = AlignedVec::<16>::with_capacity(file_length);
    bytes.resize(file_length, 0);
    file.read_exact(bytes.as_mut_slice())
        .map_err(|_| input_failure("artifact_unreadable", "artifact could not be read"))?;
    let mut trailing_byte = [0_u8; 1];
    if file
        .read(&mut trailing_byte)
        .map_err(|_| input_failure("artifact_unreadable", "artifact could not be read"))?
        != 0
    {
        return Err(input_failure(
            "artifact_changed_while_reading",
            "artifact changed while it was being read",
        ));
    }
    let payload = parse_header(&bytes)?;
    let artifact = RankedProfileArtifact { bytes, payload };
    artifact.document()?.validate().map_err(|error| {
        RankedReportFailure::new(
            RankedReportFailurePhase::AggregateValidation,
            "invalid_ranked_profile",
            error.to_string(),
        )
    })?;
    Ok(artifact)
}

pub(super) fn write_ranked_profile_artifact(
    output: &Path,
    document: &RankedProfileDocument,
    existing_output: ExistingOutputPolicy,
) -> Result<(), RankedReportFailure> {
    document.validate().map_err(|error| {
        RankedReportFailure::new(
            RankedReportFailurePhase::AggregateValidation,
            "invalid_ranked_profile",
            error.to_string(),
        )
    })?;
    let payload = rkyv::to_bytes::<RkyvError>(document).map_err(|_| {
        RankedReportFailure::new(
            RankedReportFailurePhase::Serialization,
            "artifact_serialize_failed",
            "ranked profile artifact could not be serialized",
        )
    })?;
    HEADER_LENGTH
        .checked_add(payload.len())
        .filter(|length| *length <= MAX_ARTIFACT_BYTES)
        .ok_or_else(serialized_artifact_too_large)?;
    let payload_length =
        u64::try_from(payload.len()).map_err(|_| serialized_artifact_too_large())?;
    let header = encode_header(payload_length);

    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(|_| {
        output_failure(
            "create_parent_failed",
            "artifact output directory could not be created",
        )
    })?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|_| {
        output_failure(
            "create_temporary_failed",
            "temporary artifact file could not be created",
        )
    })?;
    temporary
        .write_all(&header)
        .and_then(|()| temporary.write_all(&payload))
        .map_err(|_| {
            output_failure(
                "write_failed",
                "temporary artifact file could not be written",
            )
        })?;
    let persisted = match existing_output {
        ExistingOutputPolicy::Replace => temporary.persist(output),
        ExistingOutputPolicy::Preserve => temporary.persist_noclobber(output),
    };
    persisted.map_err(|_| {
        output_failure(
            "persist_failed",
            "completed artifact could not be persisted",
        )
    })?;
    Ok(())
}

fn encode_header(payload_length: u64) -> [u8; HEADER_LENGTH] {
    let mut header = [0_u8; HEADER_LENGTH];
    header[..MAGIC.len()].copy_from_slice(&MAGIC);
    header[ARTIFACT_VERSION_RANGE].copy_from_slice(&ARTIFACT_VERSION.to_le_bytes());
    header[HEADER_LENGTH_RANGE].copy_from_slice(&(HEADER_LENGTH as u16).to_le_bytes());
    header[SCHEMA_VERSION_RANGE].copy_from_slice(&RANKED_PROFILE_SCHEMA_VERSION.to_le_bytes());
    header[RKYV_CONFIG_RANGE].copy_from_slice(&RKYV_FORMAT_CONFIG.to_le_bytes());
    header[PAYLOAD_OFFSET_RANGE].copy_from_slice(&(HEADER_LENGTH as u64).to_le_bytes());
    header[PAYLOAD_LENGTH_RANGE].copy_from_slice(&payload_length.to_le_bytes());
    header
}

fn parse_header(bytes: &[u8]) -> Result<Range<usize>, RankedReportFailure> {
    if bytes.get(..MAGIC.len()) != Some(MAGIC.as_slice()) {
        return Err(input_failure(
            "invalid_artifact_magic",
            "input is not a Delta Funnel ranked profile artifact",
        ));
    }
    let artifact_version = read_u16(bytes, ARTIFACT_VERSION_RANGE.clone());
    if artifact_version != ARTIFACT_VERSION {
        return Err(input_failure(
            "unsupported_artifact_version",
            "artifact format version is not supported",
        ));
    }
    if usize::from(read_u16(bytes, HEADER_LENGTH_RANGE.clone())) != HEADER_LENGTH {
        return Err(input_failure(
            "invalid_artifact_header",
            "artifact header length is invalid",
        ));
    }
    if read_u32(bytes, SCHEMA_VERSION_RANGE.clone()) != RANKED_PROFILE_SCHEMA_VERSION {
        return Err(input_failure(
            "unsupported_ranked_schema",
            "ranked profile schema version is not supported",
        ));
    }
    if read_u32(bytes, RKYV_CONFIG_RANGE.clone()) != RKYV_FORMAT_CONFIG {
        return Err(input_failure(
            "unsupported_rkyv_config",
            "artifact rkyv format configuration is not supported",
        ));
    }
    if bytes[RESERVED_WORD_RANGE]
        .iter()
        .chain(bytes[RESERVED_TAIL_RANGE].iter())
        .any(|byte| *byte != 0)
    {
        return Err(input_failure(
            "invalid_artifact_header",
            "artifact header has nonzero reserved bytes",
        ));
    }

    let payload_offset =
        usize::try_from(read_u64(bytes, PAYLOAD_OFFSET_RANGE.clone())).map_err(|_| {
            input_failure(
                "artifact_length_overflow",
                "artifact payload offset does not fit this platform",
            )
        })?;
    if payload_offset % mem::align_of::<ArchivedRankedProfileDocument>() != 0 {
        return Err(input_failure(
            "invalid_payload_alignment",
            "artifact payload is not correctly aligned",
        ));
    }
    if payload_offset != HEADER_LENGTH {
        return Err(input_failure(
            "invalid_payload_offset",
            "artifact payload offset is invalid",
        ));
    }
    let payload_length =
        usize::try_from(read_u64(bytes, PAYLOAD_LENGTH_RANGE.clone())).map_err(|_| {
            input_failure(
                "artifact_length_overflow",
                "artifact payload length does not fit this platform",
            )
        })?;
    let expected_length = payload_offset.checked_add(payload_length).ok_or_else(|| {
        input_failure(
            "artifact_length_overflow",
            "artifact payload range overflows",
        )
    })?;
    if expected_length > MAX_ARTIFACT_BYTES {
        return Err(artifact_too_large());
    }
    if expected_length > bytes.len() {
        return Err(input_failure(
            "truncated_artifact",
            "artifact payload is truncated",
        ));
    }
    if expected_length < bytes.len() {
        return Err(input_failure(
            "trailing_artifact_bytes",
            "artifact has bytes after its declared payload",
        ));
    }
    Ok(payload_offset..expected_length)
}

fn read_u16(bytes: &[u8], range: Range<usize>) -> u16 {
    u16::from_le_bytes([bytes[range.start], bytes[range.start + 1]])
}

fn read_u32(bytes: &[u8], range: Range<usize>) -> u32 {
    u32::from_le_bytes([
        bytes[range.start],
        bytes[range.start + 1],
        bytes[range.start + 2],
        bytes[range.start + 3],
    ])
}

fn read_u64(bytes: &[u8], range: Range<usize>) -> u64 {
    u64::from_le_bytes([
        bytes[range.start],
        bytes[range.start + 1],
        bytes[range.start + 2],
        bytes[range.start + 3],
        bytes[range.start + 4],
        bytes[range.start + 5],
        bytes[range.start + 6],
        bytes[range.start + 7],
    ])
}

fn artifact_too_large() -> RankedReportFailure {
    input_failure(
        "artifact_too_large",
        "artifact exceeds the 512 MiB safety limit",
    )
}

fn serialized_artifact_too_large() -> RankedReportFailure {
    RankedReportFailure::new(
        RankedReportFailurePhase::Serialization,
        "artifact_too_large",
        "serialized artifact exceeds the 512 MiB safety limit",
    )
}

fn input_failure(kind: &'static str, message: impl Into<String>) -> RankedReportFailure {
    RankedReportFailure::new(RankedReportFailurePhase::Input, kind, message)
}

fn output_failure(kind: &'static str, message: &'static str) -> RankedReportFailure {
    RankedReportFailure::new(RankedReportFailurePhase::Output, kind, message)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::perfetto_profile::ranked_report::{RankedProfileMetadata, RankedSemantic};

    fn document() -> RankedProfileDocument {
        RankedProfileDocument {
            metadata: RankedProfileMetadata {
                capture_complete: true,
                semantic_complete: true,
                finalization_observed: true,
                incomplete_operation_root_count: 0,
                truncation_marker_count: 0,
                missing_identity_field_count: 0,
                missing_terminal_result_count: 0,
                crossing_worker_slice_count: 0,
                crossing_planning_activity_slice_count: 0,
                crossing_execution_activity_slice_count: 0,
                invalid_planning_activity_hierarchy_count: 0,
                invalid_execution_activity_hierarchy_count: 0,
                perf_sample_without_callsite_count: 0,
                perf_samples_skipped: 0,
                buffer_loss_count: 0,
                data_source_loss_count: 0,
                flush_failure_count: 0,
                schema_version: RANKED_PROFILE_SCHEMA_VERSION,
                sample_frequency_hz: 1_000,
                sampled_cpu_count: 1,
                exact_time_unit: "nanoseconds".to_owned(),
                sample_unit: "samples".to_owned(),
                eligible_sample_count: 0,
                direct_sample_count: 0,
                ambiguous_sample_count: 0,
                unattributed_sample_count: 0,
                resolved_function_sample_count: 0,
                unresolved_function_sample_count: 0,
                unwind_error_sample_count: 0,
                missing_callstack_sample_count: 0,
                trace_profiler_dropped_sample_count: 0,
            },
            semantics: vec![RankedSemantic {
                semantic_id: 1,
                parent_semantic_id: None,
                operation_id: 1,
                name: "Delta Funnel preview".to_owned(),
                semantic_kind: "operation".to_owned(),
                operation_kind: Some("preview".to_owned()),
                stage_category: None,
                stage_name: None,
                activity: None,
                start_ns: 10,
                end_ns: Some(20),
                duration_ns: Some(10),
                time_semantics: "wall_clock".to_owned(),
                result: Some("ok".to_owned()),
                is_complete: true,
                query_execution_id: None,
                query_scope: None,
                query_owner: None,
                worker_lane_id: None,
                worker_kind: None,
                node_id: None,
                parent_node_id: None,
                operator_partition: None,
                execution_stream_id: None,
                stage_owner_id: None,
                direct_sample_count: 0,
                inclusive_sample_count: 0,
                resolved_function_sample_count: 0,
                unresolved_function_sample_count: 0,
                unwind_error_sample_count: 0,
                missing_callstack_sample_count: 0,
            }],
            functions: Vec::new(),
        }
    }

    fn artifact_bytes(document: &RankedProfileDocument) -> Vec<u8> {
        let payload =
            rkyv::to_bytes::<RkyvError>(document).expect("test document should serialize");
        let mut bytes = encode_header(payload.len() as u64).to_vec();
        bytes.extend_from_slice(&payload);
        bytes
    }

    fn failure_kind(bytes: &[u8]) -> &'static str {
        let directory = tempfile::tempdir().expect("test directory should be created");
        let input = directory.path().join("profile.dfprofile");
        fs::write(&input, bytes).expect("test artifact should be written");
        read_ranked_profile_artifact(&input)
            .expect_err("test artifact should be rejected")
            .kind()
    }

    #[test]
    fn round_trips_validated_artifact_with_atomic_output_policies() {
        let directory = tempfile::tempdir().expect("test directory should be created");
        let output = directory.path().join("nested/profile.dfprofile");
        let document = document();

        write_ranked_profile_artifact(&output, &document, ExistingOutputPolicy::Replace)
            .expect("artifact should be written");
        let artifact = read_ranked_profile_artifact(&output).expect("artifact should be read");
        let archived = artifact.document().expect("artifact should remain valid");
        assert_eq!(archived.metadata.sample_frequency_hz.to_native(), 1_000);
        assert_eq!(archived.semantics[0].name.as_str(), "Delta Funnel preview");
        assert_eq!(
            read_ranked_profile_artifact(&output)
                .expect("artifact should be read")
                .into_document()
                .expect("artifact should deserialize"),
            document
        );

        let original = fs::read(&output).expect("artifact should be readable");
        let error =
            write_ranked_profile_artifact(&output, &document, ExistingOutputPolicy::Preserve)
                .expect_err("preserve policy should reject an existing artifact");
        assert_eq!(error.kind(), "persist_failed");
        assert_eq!(
            fs::read(&output).expect("artifact should remain readable"),
            original
        );
        assert_eq!(
            fs::read_dir(output.parent().expect("output should have a parent"))
                .expect("output directory should be readable")
                .count(),
            1
        );

        let mut invalid = document;
        invalid.semantics[0].semantic_kind = "phase".to_owned();
        let error = write_ranked_profile_artifact(&output, &invalid, ExistingOutputPolicy::Replace)
            .expect_err("invalid document should be rejected before output");
        assert_eq!(error.kind(), "invalid_ranked_profile");
        assert_eq!(
            fs::read(&output).expect("artifact should remain readable"),
            original
        );
    }

    #[test]
    fn reads_the_version_one_golden_fixture() {
        let directory = tempfile::tempdir().expect("test directory should be created");
        let input = directory.path().join("golden.dfprofile");
        fs::write(
            &input,
            include_bytes!("testdata/ranked_profile_v1.dfprofile"),
        )
        .expect("golden fixture should be copied");

        let artifact = read_ranked_profile_artifact(&input).expect("golden fixture should load");
        let document = artifact
            .document()
            .expect("golden fixture should remain valid");
        assert_eq!(document.metadata.schema_version.to_native(), 3);
        assert_eq!(document.semantics[0].name.as_str(), "Delta Funnel preview");
    }

    #[test]
    fn rejects_invalid_artifact_envelopes_before_archived_access() {
        let valid = artifact_bytes(&document());
        let mut cases = Vec::new();

        let mut invalid_magic = valid.clone();
        invalid_magic[0] ^= 1;
        cases.push((invalid_magic, "invalid_artifact_magic"));

        let mut unsupported_artifact = valid.clone();
        unsupported_artifact[ARTIFACT_VERSION_RANGE]
            .copy_from_slice(&(ARTIFACT_VERSION + 1).to_le_bytes());
        cases.push((unsupported_artifact, "unsupported_artifact_version"));

        let mut invalid_header_length = valid.clone();
        invalid_header_length[HEADER_LENGTH_RANGE].copy_from_slice(&32_u16.to_le_bytes());
        cases.push((invalid_header_length, "invalid_artifact_header"));

        let mut unsupported_schema = valid.clone();
        unsupported_schema[SCHEMA_VERSION_RANGE]
            .copy_from_slice(&(RANKED_PROFILE_SCHEMA_VERSION + 1).to_le_bytes());
        cases.push((unsupported_schema, "unsupported_ranked_schema"));

        let mut unsupported_config = valid.clone();
        unsupported_config[RKYV_CONFIG_RANGE]
            .copy_from_slice(&(RKYV_FORMAT_CONFIG + 1).to_le_bytes());
        cases.push((unsupported_config, "unsupported_rkyv_config"));

        let mut reserved = valid.clone();
        reserved[RESERVED_TAIL_RANGE.start] = 1;
        cases.push((reserved, "invalid_artifact_header"));

        let mut unaligned = valid.clone();
        unaligned[PAYLOAD_OFFSET_RANGE].copy_from_slice(&65_u64.to_le_bytes());
        cases.push((unaligned, "invalid_payload_alignment"));

        let mut invalid_offset = valid.clone();
        invalid_offset[PAYLOAD_OFFSET_RANGE].copy_from_slice(&80_u64.to_le_bytes());
        cases.push((invalid_offset, "invalid_payload_offset"));

        let mut truncated = valid.clone();
        let payload_length = read_u64(&truncated, PAYLOAD_LENGTH_RANGE.clone());
        truncated[PAYLOAD_LENGTH_RANGE].copy_from_slice(&(payload_length + 1).to_le_bytes());
        cases.push((truncated, "truncated_artifact"));

        let mut trailing = valid.clone();
        trailing.push(0);
        cases.push((trailing, "trailing_artifact_bytes"));

        let mut too_large = valid;
        too_large[PAYLOAD_LENGTH_RANGE].copy_from_slice(&(MAX_ARTIFACT_BYTES as u64).to_le_bytes());
        cases.push((too_large, "artifact_too_large"));

        cases.push((vec![0; HEADER_LENGTH - 1], "truncated_artifact"));

        for (bytes, expected_kind) in cases {
            assert_eq!(failure_kind(&bytes), expected_kind);
        }
    }

    #[test]
    fn bytechecks_and_semantically_validates_archived_documents() {
        let mut invalid_archive = artifact_bytes(&document());
        invalid_archive.truncate(invalid_archive.len() - 1);
        let payload_length = (invalid_archive.len() - HEADER_LENGTH) as u64;
        invalid_archive[PAYLOAD_LENGTH_RANGE].copy_from_slice(&payload_length.to_le_bytes());
        assert_eq!(failure_kind(&invalid_archive), "invalid_archive");

        let mut invalid_document = document();
        invalid_document.semantics[0].semantic_kind = "phase".to_owned();
        let expected = invalid_document
            .validate()
            .expect_err("test document should be invalid")
            .to_string();
        let directory = tempfile::tempdir().expect("test directory should be created");
        let input = directory.path().join("invalid.dfprofile");
        fs::write(&input, artifact_bytes(&invalid_document))
            .expect("invalid test artifact should be written");
        let error = read_ranked_profile_artifact(&input)
            .expect_err("invalid semantic document should be rejected");
        assert_eq!(error.kind(), "invalid_ranked_profile");
        assert_eq!(error.to_string(), expected);
    }
}
