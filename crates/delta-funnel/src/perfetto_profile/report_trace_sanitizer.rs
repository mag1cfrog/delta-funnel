use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use tempfile::NamedTempFile;

use super::report_cli::{RankedReportFailure, RankedReportFailurePhase};

const TRACE_PACKET_TAG: u64 = 1 << 3 | 2;
const PERF_SAMPLE_FIELD: u64 = 66;
const COMPRESSED_PACKETS_FIELD: u64 = 50;
const ZSTD_COMPRESSED_PACKETS_FIELD: u64 = 133;
const SAMPLE_SKIPPED_REASON_FIELD: u64 = 18;
const PROFILER_SKIP_NOT_IN_SCOPE: u64 = 4;
const LEGACY_COMPOSED_SEQUENCE_ID: u64 = 4_000_000_000;
const LEGACY_UNIFIED_CATEGORY_PREFIX: &[u8] = b"delta_funnel.unified.";
const COMPRESSED_CHUNK_BYTES: usize = 384 * 1024;
// ponytail: This matches the existing production-tested parser bound. Raise it
// only when a valid Perfetto trace demonstrates a larger individual packet.
const MAX_TRACE_PACKET_BYTES: usize = 64 * 1024 * 1024;
const MAX_DECOMPRESSED_CHUNK_BYTES: u64 = 64 * 1024 * 1024;

pub(super) fn sanitize_trace(input: &Path) -> Result<NamedTempFile, RankedReportFailure> {
    sanitize_trace_inner(input).map_err(sanitize_failure)
}

fn sanitize_failure(error: io::Error) -> RankedReportFailure {
    if error
        .get_ref()
        .is_some_and(|source| source.is::<LegacyComposedTrace>())
    {
        return RankedReportFailure::new(
            RankedReportFailurePhase::Health,
            "legacy_composed_trace",
            "legacy composed traces are unsupported; generate the ranked report from the raw capture",
        );
    }
    match error.kind() {
        io::ErrorKind::InvalidData => RankedReportFailure::new(
            RankedReportFailurePhase::Input,
            "malformed_trace",
            "input trace could not be prepared for ranked analysis",
        ),
        _ => RankedReportFailure::new(
            RankedReportFailurePhase::Input,
            "sanitize_failed",
            "input trace could not be prepared for ranked analysis",
        ),
    }
}

fn sanitize_trace_inner(input: &Path) -> io::Result<NamedTempFile> {
    let mut sanitized = NamedTempFile::new()?;
    {
        let input = BufReader::new(File::open(input)?);
        let output = BufWriter::new(sanitized.as_file_mut());
        let mut writer = SanitizedTraceWriter::new(output);
        sanitize_packets(input, &mut writer, false)?;
        writer.finish()?;
    }
    Ok(sanitized)
}

fn sanitize_packets(
    mut input: impl Read,
    output: &mut SanitizedTraceWriter<impl Write>,
    inside_compressed_chunk: bool,
) -> io::Result<()> {
    while let Some(packet) = read_packet(&mut input)? {
        match inspect_packet(&packet)? {
            PacketAction::Keep => output.write_packet(&packet)?,
            PacketAction::KeepUncompressed => output.write_uncompressed_packet(&packet)?,
            PacketAction::Drop => {}
            PacketAction::Deflate(compressed) if !inside_compressed_chunk => {
                let decoder = ZlibDecoder::new(compressed);
                sanitize_compressed_packets(decoder, output)?;
            }
            PacketAction::Zstd(compressed) if !inside_compressed_chunk => {
                let decoder = zstd::stream::read::Decoder::new(compressed)?;
                sanitize_compressed_packets(decoder, output)?;
            }
            PacketAction::Deflate(_) | PacketAction::Zstd(_) => {
                output.write_uncompressed_packet(&packet)?;
            }
        }
    }
    Ok(())
}

fn sanitize_compressed_packets(
    decoder: impl Read,
    output: &mut SanitizedTraceWriter<impl Write>,
) -> io::Result<()> {
    let mut limited = decoder.take(MAX_DECOMPRESSED_CHUNK_BYTES.saturating_add(1));
    sanitize_packets(&mut limited, output, true)?;
    if limited.limit() == 0 {
        return Err(invalid_data(
            "decompressed packet chunk exceeds the 64 MiB limit",
        ));
    }
    Ok(())
}

enum PacketAction<'a> {
    Keep,
    KeepUncompressed,
    Drop,
    Deflate(&'a [u8]),
    Zstd(&'a [u8]),
}

fn inspect_packet(packet: &[u8]) -> io::Result<PacketAction<'_>> {
    let mut offset = 0;
    let mut field_count = 0;
    let mut compressed = None;
    let mut perf_sample = None;
    let mut only_skip_envelope_fields = true;
    let mut is_legacy_sequence = false;
    let mut has_legacy_category = false;
    while offset < packet.len() {
        let field = next_field(packet, &mut offset)?;
        field_count += 1;
        match field.number {
            COMPRESSED_PACKETS_FIELD => {
                let Some(bytes) = field.delimited else {
                    return Ok(PacketAction::Keep);
                };
                if compressed.replace(PacketAction::Deflate(bytes)).is_some() {
                    return Ok(PacketAction::KeepUncompressed);
                }
            }
            ZSTD_COMPRESSED_PACKETS_FIELD => {
                let Some(bytes) = field.delimited else {
                    return Ok(PacketAction::Keep);
                };
                if compressed.replace(PacketAction::Zstd(bytes)).is_some() {
                    return Ok(PacketAction::KeepUncompressed);
                }
            }
            PERF_SAMPLE_FIELD => {
                let Some(bytes) = field.delimited else {
                    only_skip_envelope_fields = false;
                    continue;
                };
                if perf_sample.replace(bytes).is_some() {
                    only_skip_envelope_fields = false;
                }
            }
            10 => {
                is_legacy_sequence |= field.varint == Some(LEGACY_COMPOSED_SEQUENCE_ID);
            }
            11 => {
                has_legacy_category |= field.delimited.is_some_and(|event| {
                    event
                        .windows(LEGACY_UNIFIED_CATEGORY_PREFIX.len())
                        .any(|window| window == LEGACY_UNIFIED_CATEGORY_PREFIX)
                });
                only_skip_envelope_fields = false;
            }
            3 | 8 | 13 | 42 | 58 | 79 | 87 => {}
            _ => only_skip_envelope_fields = false,
        }
    }

    if is_legacy_sequence && has_legacy_category {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            LegacyComposedTrace,
        ));
    }
    if let Some(compressed) = compressed {
        return if field_count == 1 {
            Ok(compressed)
        } else {
            Ok(PacketAction::KeepUncompressed)
        };
    }
    if only_skip_envelope_fields && perf_sample.is_some_and(is_not_in_scope_sample) {
        return Ok(PacketAction::Drop);
    }
    Ok(PacketAction::Keep)
}

#[derive(Debug)]
struct LegacyComposedTrace;

impl fmt::Display for LegacyComposedTrace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("legacy composed trace")
    }
}

impl std::error::Error for LegacyComposedTrace {}

fn is_not_in_scope_sample(sample: &[u8]) -> bool {
    let mut offset = 0;
    let mut skipped_reason = None;
    while offset < sample.len() {
        let Ok(field) = next_field(sample, &mut offset) else {
            return false;
        };
        match field.number {
            SAMPLE_SKIPPED_REASON_FIELD => {
                let Some(value) = field.varint else {
                    return false;
                };
                if skipped_reason.replace(value).is_some() {
                    return false;
                }
            }
            1 | 2 | 3 | 5 | 6 | 7 if field.varint.is_some() => {}
            _ => return false,
        }
    }
    skipped_reason == Some(PROFILER_SKIP_NOT_IN_SCOPE)
}

struct Field<'a> {
    number: u64,
    varint: Option<u64>,
    delimited: Option<&'a [u8]>,
}

fn next_field<'a>(data: &'a [u8], offset: &mut usize) -> io::Result<Field<'a>> {
    let tag = decode_varint(data, offset)?;
    let number = tag >> 3;
    let wire_type = tag & 7;
    if number == 0 || number > 0x1fff_ffff {
        return Err(invalid_data("protobuf field number is invalid"));
    }
    let mut field = Field {
        number,
        varint: None,
        delimited: None,
    };
    match wire_type {
        0 => field.varint = Some(decode_varint(data, offset)?),
        1 => skip_bytes(data, offset, 8)?,
        2 => {
            let length = usize::try_from(decode_varint(data, offset)?)
                .map_err(|_| invalid_data("protobuf field length exceeds usize"))?;
            let end = offset
                .checked_add(length)
                .filter(|end| *end <= data.len())
                .ok_or_else(|| invalid_data("protobuf field is truncated"))?;
            field.delimited = Some(&data[*offset..end]);
            *offset = end;
        }
        5 => skip_bytes(data, offset, 4)?,
        _ => return Err(invalid_data("protobuf wire type is unsupported")),
    }
    Ok(field)
}

fn skip_bytes(data: &[u8], offset: &mut usize, length: usize) -> io::Result<()> {
    *offset = offset
        .checked_add(length)
        .filter(|end| *end <= data.len())
        .ok_or_else(|| invalid_data("protobuf fixed-width field is truncated"))?;
    Ok(())
}

fn read_packet(input: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
    let Some(tag) = read_varint(input)? else {
        return Ok(None);
    };
    if tag != TRACE_PACKET_TAG {
        return Err(invalid_data(
            "trace contains an unexpected outer protobuf field",
        ));
    }
    let length =
        read_varint(input)?.ok_or_else(|| invalid_data("trace packet length is truncated"))?;
    let length =
        usize::try_from(length).map_err(|_| invalid_data("trace packet length exceeds usize"))?;
    if length > MAX_TRACE_PACKET_BYTES {
        return Err(invalid_data("trace packet exceeds the 64 MiB limit"));
    }
    let mut packet = vec![0; length];
    input
        .read_exact(&mut packet)
        .map_err(|error| map_truncated(error, "trace packet is truncated"))?;
    Ok(Some(packet))
}

fn read_varint(input: &mut impl Read) -> io::Result<Option<u64>> {
    let mut value = 0_u64;
    for index in 0..10 {
        let mut byte = [0_u8];
        match input.read_exact(&mut byte) {
            Ok(()) => {}
            Err(error) if index == 0 && error.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(error) => return Err(map_truncated(error, "protobuf varint is truncated")),
        }
        if index == 9 && byte[0] > 1 {
            return Err(invalid_data("protobuf varint exceeds 64 bits"));
        }
        value |= u64::from(byte[0] & 0x7f) << (index * 7);
        if byte[0] < 0x80 {
            return Ok(Some(value));
        }
    }
    Err(invalid_data("protobuf varint exceeds 64 bits"))
}

fn decode_varint(data: &[u8], offset: &mut usize) -> io::Result<u64> {
    let mut value = 0_u64;
    for index in 0..10 {
        let byte = *data
            .get(*offset)
            .ok_or_else(|| invalid_data("protobuf varint is truncated"))?;
        *offset += 1;
        if index == 9 && byte > 1 {
            return Err(invalid_data("protobuf varint exceeds 64 bits"));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte < 0x80 {
            return Ok(value);
        }
    }
    Err(invalid_data("protobuf varint exceeds 64 bits"))
}

struct SanitizedTraceWriter<W> {
    output: W,
    chunk: Vec<u8>,
}

impl<W: Write> SanitizedTraceWriter<W> {
    fn new(output: W) -> Self {
        Self {
            output,
            chunk: Vec::with_capacity(COMPRESSED_CHUNK_BYTES),
        }
    }

    fn write_packet(&mut self, packet: &[u8]) -> io::Result<()> {
        let framed_length = trace_packet_framed_length(packet.len())?;
        if framed_length > COMPRESSED_CHUNK_BYTES {
            self.flush_chunk()?;
            return write_trace_packet(&mut self.output, packet);
        }
        if self.chunk.len().saturating_add(framed_length) > COMPRESSED_CHUNK_BYTES {
            self.flush_chunk()?;
        }
        append_trace_packet(&mut self.chunk, packet);
        Ok(())
    }

    fn write_uncompressed_packet(&mut self, packet: &[u8]) -> io::Result<()> {
        self.flush_chunk()?;
        write_trace_packet(&mut self.output, packet)
    }

    fn flush_chunk(&mut self) -> io::Result<()> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&self.chunk)?;
        let compressed = encoder.finish()?;
        let mut wrapper = Vec::with_capacity(compressed.len().saturating_add(16));
        encode_varint(COMPRESSED_PACKETS_FIELD << 3 | 2, &mut wrapper);
        encode_varint(compressed.len() as u64, &mut wrapper);
        wrapper.extend_from_slice(&compressed);
        write_trace_packet(&mut self.output, &wrapper)?;
        self.chunk.clear();
        Ok(())
    }

    fn finish(mut self) -> io::Result<W> {
        self.flush_chunk()?;
        self.output.flush()?;
        Ok(self.output)
    }
}

fn trace_packet_framed_length(packet_length: usize) -> io::Result<usize> {
    1_usize
        .checked_add(varint_length(packet_length as u64))
        .and_then(|length| length.checked_add(packet_length))
        .ok_or_else(|| invalid_data("trace packet framed length overflowed"))
}

fn write_trace_packet(mut output: impl Write, packet: &[u8]) -> io::Result<()> {
    let mut prefix = [0_u8; 11];
    prefix[0] = TRACE_PACKET_TAG as u8;
    let length_bytes = encode_varint_into(packet.len() as u64, &mut prefix[1..]);
    output.write_all(&prefix[..1 + length_bytes])?;
    output.write_all(packet)
}

fn append_trace_packet(output: &mut Vec<u8>, packet: &[u8]) {
    output.push(TRACE_PACKET_TAG as u8);
    encode_varint(packet.len() as u64, output);
    output.extend_from_slice(packet);
}

fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn encode_varint_into(mut value: u64, output: &mut [u8]) -> usize {
    let mut length = 0;
    while value >= 0x80 {
        output[length] = (value as u8 & 0x7f) | 0x80;
        value >>= 7;
        length += 1;
    }
    output[length] = value as u8;
    length + 1
}

fn varint_length(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 0x80 {
        value >>= 7;
        length += 1;
    }
    length
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn map_truncated(error: io::Error, message: &'static str) -> io::Error {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        invalid_data(message)
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use flate2::write::ZlibEncoder;

    use super::*;

    #[test]
    fn filters_only_not_in_scope_samples_across_supported_compression() -> io::Result<()> {
        let plain_kept = packet_with_field(1000, b"plain");
        let dropped = perf_sample_packet(Some(PROFILER_SKIP_NOT_IN_SCOPE), false, false);
        let unwind_failure = perf_sample_packet(Some(2), false, false);
        let valid_sample = perf_sample_packet(None, true, false);
        let combined_payload = perf_sample_packet(Some(PROFILER_SKIP_NOT_IN_SCOPE), false, true);
        let deflated = compressed_packet(
            &trace(&[dropped.clone(), unwind_failure.clone()]),
            CompressionKind::Deflate,
        )?;
        let zstd = compressed_packet(
            &trace(&[valid_sample.clone(), dropped]),
            CompressionKind::Zstd,
        )?;
        let input = trace(&[plain_kept.clone(), deflated, combined_payload.clone(), zstd]);

        let mut writer = SanitizedTraceWriter::new(Vec::new());
        sanitize_packets(Cursor::new(input), &mut writer, false)?;
        let output = writer.finish()?;

        assert_eq!(
            expand_packets(&output)?,
            [plain_kept, unwind_failure, combined_payload, valid_sample]
        );
        Ok(())
    }

    #[test]
    fn rejects_oversized_packets_before_allocating_the_payload() {
        let mut input = vec![TRACE_PACKET_TAG as u8];
        encode_varint(
            u64::try_from(MAX_TRACE_PACKET_BYTES).expect("limit should fit u64") + 1,
            &mut input,
        );
        let mut writer = SanitizedTraceWriter::new(Vec::new());

        let error = sanitize_packets(Cursor::new(input), &mut writer, false)
            .expect_err("oversized packet should fail");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("64 MiB"));
    }

    #[test]
    fn rejects_only_the_retired_composer_identity() -> io::Result<()> {
        let legacy = legacy_composed_packet(LEGACY_COMPOSED_SEQUENCE_ID, true);
        let current_sequence = legacy_composed_packet(1, true);
        let unrelated_category = legacy_composed_packet(LEGACY_COMPOSED_SEQUENCE_ID, false);

        let error = inspect_packet(&legacy)
            .err()
            .ok_or_else(|| io::Error::other("legacy composed packet should fail"))?;
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(matches!(
            inspect_packet(&current_sequence),
            Ok(PacketAction::Keep)
        ));
        assert!(matches!(
            inspect_packet(&unrelated_category),
            Ok(PacketAction::Keep)
        ));

        let failure = sanitize_failure(error);
        assert_eq!(failure.phase(), RankedReportFailurePhase::Health);
        assert_eq!(failure.kind(), "legacy_composed_trace");
        Ok(())
    }

    fn perf_sample_packet(
        skipped_reason: Option<u64>,
        callstack: bool,
        track_event: bool,
    ) -> Vec<u8> {
        let mut sample = Vec::new();
        append_varint_field(&mut sample, 1, 0);
        append_varint_field(&mut sample, 2, 42);
        append_varint_field(&mut sample, 3, 43);
        append_varint_field(&mut sample, 5, 2);
        append_varint_field(&mut sample, 6, 1);
        if callstack {
            append_varint_field(&mut sample, 4, 99);
        }
        if let Some(reason) = skipped_reason {
            append_varint_field(&mut sample, SAMPLE_SKIPPED_REASON_FIELD, reason);
        }

        let mut packet = Vec::new();
        append_varint_field(&mut packet, 13, 2);
        append_varint_field(&mut packet, 8, 100);
        append_length_delimited_field(&mut packet, PERF_SAMPLE_FIELD, &sample);
        append_varint_field(&mut packet, 3, 1000);
        append_varint_field(&mut packet, 10, 1);
        append_varint_field(&mut packet, 79, 1000);
        if track_event {
            append_length_delimited_field(&mut packet, 11, b"also meaningful");
        }
        packet
    }

    fn packet_with_field(field_number: u64, value: &[u8]) -> Vec<u8> {
        let mut packet = Vec::new();
        append_length_delimited_field(&mut packet, field_number, value);
        packet
    }

    fn legacy_composed_packet(sequence_id: u64, legacy_category: bool) -> Vec<u8> {
        let mut event = Vec::new();
        append_length_delimited_field(
            &mut event,
            22,
            if legacy_category {
                b"delta_funnel.unified.native_sample"
            } else {
                b"delta_funnel.profile"
            },
        );
        let mut packet = Vec::new();
        append_varint_field(&mut packet, 10, sequence_id);
        append_length_delimited_field(&mut packet, 11, &event);
        packet
    }

    enum CompressionKind {
        Deflate,
        Zstd,
    }

    fn compressed_packet(trace: &[u8], kind: CompressionKind) -> io::Result<Vec<u8>> {
        let (field_number, compressed) = match kind {
            CompressionKind::Deflate => {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(trace)?;
                (COMPRESSED_PACKETS_FIELD, encoder.finish()?)
            }
            CompressionKind::Zstd => (
                ZSTD_COMPRESSED_PACKETS_FIELD,
                zstd::stream::encode_all(trace, 0)?,
            ),
        };
        Ok(packet_with_field(field_number, &compressed))
    }

    fn expand_packets(trace: &[u8]) -> io::Result<Vec<Vec<u8>>> {
        let mut packets = Vec::new();
        expand_packets_into(Cursor::new(trace), &mut packets)?;
        Ok(packets)
    }

    fn expand_packets_into(mut trace: impl Read, packets: &mut Vec<Vec<u8>>) -> io::Result<()> {
        while let Some(packet) = read_packet(&mut trace)? {
            match inspect_packet(&packet)? {
                PacketAction::Deflate(compressed) => {
                    expand_packets_into(ZlibDecoder::new(compressed), packets)?;
                }
                PacketAction::Zstd(compressed) => {
                    expand_packets_into(zstd::stream::read::Decoder::new(compressed)?, packets)?;
                }
                _ => packets.push(packet),
            }
        }
        Ok(())
    }

    fn trace(packets: &[Vec<u8>]) -> Vec<u8> {
        let mut trace = Vec::new();
        for packet in packets {
            append_trace_packet(&mut trace, packet);
        }
        trace
    }

    fn append_varint_field(output: &mut Vec<u8>, field_number: u64, value: u64) {
        encode_varint(field_number << 3, output);
        encode_varint(value, output);
    }

    fn append_length_delimited_field(output: &mut Vec<u8>, field_number: u64, value: &[u8]) {
        encode_varint(field_number << 3 | 2, output);
        encode_varint(value.len() as u64, output);
        output.extend_from_slice(value);
    }
}
