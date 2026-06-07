use std::cmp::Ordering;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime};
use datafusion::arrow::datatypes::{DataType, TimeUnit};

const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// Logical value family used when evaluating serialized Delta partition metadata.
///
/// Delta stores partition values as text in add-file metadata. Exact provider
/// pruning still has to evaluate those raw strings through the column's logical
/// type. This enum is the small central switch that additional logical
/// partition metadata types should extend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PartitionMetadataValueKind {
    String,
    SignedInteger { min: i64, max: i64 },
    Boolean,
    Date,
    Decimal { precision: u8, scale: i8 },
    Float32,
    Float64,
    TimestampUtc,
    Binary,
}

impl PartitionMetadataValueKind {
    pub(super) fn from_supported_data_type(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => Some(Self::String),
            DataType::Int8 => Some(Self::SignedInteger {
                min: i64::from(i8::MIN),
                max: i64::from(i8::MAX),
            }),
            DataType::Int16 => Some(Self::SignedInteger {
                min: i64::from(i16::MIN),
                max: i64::from(i16::MAX),
            }),
            DataType::Int32 => Some(Self::SignedInteger {
                min: i64::from(i32::MIN),
                max: i64::from(i32::MAX),
            }),
            DataType::Int64 => Some(Self::SignedInteger {
                min: i64::MIN,
                max: i64::MAX,
            }),
            DataType::Boolean => Some(Self::Boolean),
            DataType::Date32 => Some(Self::Date),
            DataType::Decimal128(precision, scale)
                if *precision <= 38 && 0 <= *scale && *scale <= *precision as i8 =>
            {
                Some(Self::Decimal {
                    precision: *precision,
                    scale: *scale,
                })
            }
            DataType::Float32 => Some(Self::Float32),
            DataType::Float64 => Some(Self::Float64),
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone))
                if timezone.as_ref() == "UTC" =>
            {
                Some(Self::TimestampUtc)
            }
            DataType::Binary => Some(Self::Binary),
            _ => None,
        }
    }

    pub(super) fn is_boolean(self) -> bool {
        matches!(self, Self::Boolean)
    }

    pub(super) fn supports_ordering(self) -> bool {
        match self {
            Self::String
            | Self::SignedInteger { .. }
            | Self::Date
            | Self::Decimal { .. }
            | Self::Float32
            | Self::Float64
            | Self::TimestampUtc => true,
            Self::Boolean | Self::Binary => false,
        }
    }

    pub(super) fn supports_between(self) -> bool {
        match self {
            Self::String
            | Self::SignedInteger { .. }
            | Self::Date
            | Self::Decimal { .. }
            | Self::Float32
            | Self::Float64
            | Self::TimestampUtc => true,
            Self::Boolean | Self::Binary => false,
        }
    }

    pub(super) fn parse_raw(self, raw_value: &str) -> Option<PartitionScalar> {
        match self {
            Self::String => Some(PartitionScalar::String(raw_value.to_owned())),
            Self::SignedInteger { min, max } => raw_value
                .parse::<i64>()
                .ok()
                .filter(|value| min <= *value && *value <= max)
                .map(PartitionScalar::SignedInteger),
            Self::Boolean => raw_value.parse::<bool>().ok().map(PartitionScalar::Boolean),
            Self::Date => parse_delta_date(raw_value).map(PartitionScalar::Date),
            Self::Decimal { precision, scale } => {
                parse_delta_decimal(raw_value, precision, scale).map(PartitionScalar::Decimal)
            }
            Self::Float32 => parse_float32(raw_value).map(PartitionScalar::Float32),
            Self::Float64 => parse_float64(raw_value).map(PartitionScalar::Float64),
            Self::TimestampUtc => {
                parse_delta_timestamp_utc(raw_value).map(PartitionScalar::TimestampUtc)
            }
            Self::Binary => None,
        }
    }

    pub(super) fn normalize_decimal_literal(
        self,
        value: i128,
        precision: u8,
        scale: i8,
    ) -> Option<PartitionScalar> {
        let Self::Decimal {
            precision: column_precision,
            scale: column_scale,
        } = self
        else {
            return None;
        };

        normalize_decimal_partition_literal(value, precision, scale, column_precision, column_scale)
            .map(PartitionScalar::Decimal)
    }
}

fn parse_delta_date(raw_value: &str) -> Option<i32> {
    if raw_value.len() != 10 {
        return None;
    }
    if !raw_value
        .bytes()
        .enumerate()
        .all(|(index, byte)| matches!((index, byte), (4 | 7, b'-') | (_, b'0'..=b'9')))
    {
        return None;
    }

    NaiveDate::parse_from_str(raw_value, "%Y-%m-%d")
        .ok()
        .map(|date| date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE)
}

fn parse_delta_timestamp_utc(raw_value: &str) -> Option<i64> {
    if raw_value.contains('T') {
        let timestamp = DateTime::parse_from_rfc3339(raw_value).ok()?;
        if timestamp.offset().local_minus_utc() != 0 {
            return None;
        }
        microsecond_exact_timestamp(timestamp.timestamp(), timestamp.timestamp_subsec_nanos())
    } else {
        let timestamp = NaiveDateTime::parse_from_str(raw_value, "%Y-%m-%d %H:%M:%S%.f").ok()?;
        microsecond_exact_timestamp(
            timestamp.and_utc().timestamp(),
            timestamp.and_utc().timestamp_subsec_nanos(),
        )
    }
}

fn microsecond_exact_timestamp(seconds: i64, nanos: u32) -> Option<i64> {
    if !nanos.is_multiple_of(1_000) {
        return None;
    }

    seconds
        .checked_mul(1_000_000)?
        .checked_add(i64::from(nanos / 1_000))
}

fn parse_delta_decimal(raw_value: &str, precision: u8, scale: i8) -> Option<i128> {
    let scale = u8::try_from(scale).ok()?;
    if precision == 0 || 38 < precision || precision < scale {
        return None;
    }
    let (mantissa, exponent) = match raw_value.find(['e', 'E']) {
        Some(index) => {
            let (mantissa, exponent) = raw_value.split_at(index);
            if mantissa.is_empty() || exponent.len() == 1 || exponent[1..].contains(['e', 'E']) {
                return None;
            }
            (mantissa, exponent[1..].parse::<i32>().ok()?)
        }
        None => (raw_value, 0),
    };
    let (negative, unsigned) = mantissa
        .strip_prefix('-')
        .map_or((false, mantissa), |value| (true, value));
    if unsigned.is_empty() {
        return None;
    }
    if unsigned.ends_with('.') {
        return None;
    }
    let mut parts = unsigned.split('.');
    let integer = parts.next()?;
    let fraction = parts.next().unwrap_or("");
    if parts.next().is_some()
        || integer.is_empty()
        || (scale as usize) < fraction.len()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let fraction_len = i32::try_from(fraction.len()).ok()?;
    let shift = i32::from(scale)
        .checked_add(exponent)?
        .checked_sub(fraction_len)?;
    let shift = u8::try_from(shift).ok()?;
    let digits = format!("{integer}{fraction}");
    let significant_digits = digits.trim_start_matches('0');
    if significant_digits.is_empty() {
        return Some(0);
    }
    if (precision as usize) < significant_digits.len() + usize::from(shift) {
        return None;
    }

    let value = significant_digits
        .parse::<i128>()
        .ok()?
        .checked_mul(pow10(shift)?)?;
    if negative {
        value.checked_neg()
    } else {
        Some(value)
    }
}

pub(crate) fn normalize_decimal_partition_literal(
    value: i128,
    precision: u8,
    scale: i8,
    column_precision: u8,
    column_scale: i8,
) -> Option<i128> {
    let scale = u8::try_from(scale).ok()?;
    let column_scale = u8::try_from(column_scale).ok()?;
    if precision == 0
        || 38 < precision
        || precision < scale
        || column_precision == 0
        || 38 < column_precision
        || column_precision < column_scale
        || !decimal_precision_fits(value, precision)?
    {
        return None;
    }

    let normalized = match scale.cmp(&column_scale) {
        Ordering::Less => value.checked_mul(pow10(column_scale - scale)?)?,
        Ordering::Equal => value,
        Ordering::Greater => {
            let divisor = pow10(scale - column_scale)?;
            if value % divisor != 0 {
                return None;
            }
            value.checked_div(divisor)?
        }
    };

    decimal_precision_fits(normalized, column_precision)?.then_some(normalized)
}

fn decimal_precision_fits(value: i128, precision: u8) -> Option<bool> {
    let mut value = value.checked_abs()?;
    let mut digits = 0;
    while value != 0 {
        digits += 1;
        value /= 10;
    }

    Some(digits <= usize::from(precision))
}

fn pow10(exponent: u8) -> Option<i128> {
    (0..exponent).try_fold(1_i128, |value, _| value.checked_mul(10))
}

fn parse_float32(raw_value: &str) -> Option<u32> {
    match raw_value {
        "NaN" => Some(f32::NAN.to_bits()),
        "Infinity" => Some(f32::INFINITY.to_bits()),
        "-Infinity" => Some(f32::NEG_INFINITY.to_bits()),
        _ => raw_value
            .parse::<f32>()
            .ok()
            .filter(|value| value.is_finite())
            .map(f32::to_bits),
    }
}

fn parse_float64(raw_value: &str) -> Option<u64> {
    match raw_value {
        "NaN" => Some(f64::NAN.to_bits()),
        "Infinity" => Some(f64::INFINITY.to_bits()),
        "-Infinity" => Some(f64::NEG_INFINITY.to_bits()),
        _ => raw_value
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(f64::to_bits),
    }
}

/// Typed literal or parsed raw partition metadata value.
///
/// The evaluator compares values only after both sides have been converted into
/// this representation, so raw string ordering is never reused for typed
/// partition values.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum PartitionScalar {
    String(String),
    SignedInteger(i64),
    Boolean(bool),
    Date(i32),
    Decimal(i128),
    Float32(u32),
    Float64(u64),
    TimestampUtc(i64),
}

impl PartitionScalar {
    pub(super) fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::String(left), Self::String(right)) => Some(left.cmp(right)),
            (Self::SignedInteger(left), Self::SignedInteger(right)) => Some(left.cmp(right)),
            (Self::Date(left), Self::Date(right)) => Some(left.cmp(right)),
            (Self::Decimal(left), Self::Decimal(right)) => Some(left.cmp(right)),
            (Self::Float32(left), Self::Float32(right)) => {
                Some(f32::from_bits(*left).total_cmp(&f32::from_bits(*right)))
            }
            (Self::Float64(left), Self::Float64(right)) => {
                Some(f64::from_bits(*left).total_cmp(&f64::from_bits(*right)))
            }
            (Self::TimestampUtc(left), Self::TimestampUtc(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};

    use super::*;

    #[test]
    fn value_kind_tracks_supported_logical_types() {
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Utf8),
            Some(PartitionMetadataValueKind::String)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::LargeUtf8),
            Some(PartitionMetadataValueKind::String)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Int8),
            Some(PartitionMetadataValueKind::SignedInteger {
                min: i64::from(i8::MIN),
                max: i64::from(i8::MAX),
            })
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Int16),
            Some(PartitionMetadataValueKind::SignedInteger {
                min: i64::from(i16::MIN),
                max: i64::from(i16::MAX),
            })
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Int32),
            Some(PartitionMetadataValueKind::SignedInteger {
                min: i64::from(i32::MIN),
                max: i64::from(i32::MAX),
            })
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Int64),
            Some(PartitionMetadataValueKind::SignedInteger {
                min: i64::MIN,
                max: i64::MAX,
            })
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Boolean),
            Some(PartitionMetadataValueKind::Boolean)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Date32),
            Some(PartitionMetadataValueKind::Date)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Decimal128(10, 2)),
            Some(PartitionMetadataValueKind::Decimal {
                precision: 10,
                scale: 2
            })
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Decimal128(38, 18)),
            Some(PartitionMetadataValueKind::Decimal {
                precision: 38,
                scale: 18
            })
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Decimal128(10, -1)),
            None
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Decimal128(10, 11)),
            None
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Float32),
            Some(PartitionMetadataValueKind::Float32)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Float64),
            Some(PartitionMetadataValueKind::Float64)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            Some(PartitionMetadataValueKind::TimestampUtc)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Binary),
            Some(PartitionMetadataValueKind::Binary)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into())
            )),
            None
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("America/Phoenix".into())
            )),
            None
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                None
            )),
            None
        );
    }

    #[test]
    fn string_value_kind_preserves_raw_partition_text() {
        assert_eq!(
            PartitionMetadataValueKind::String.parse_raw(""),
            Some(PartitionScalar::String(String::new()))
        );
        assert_eq!(
            PartitionMetadataValueKind::String.parse_raw("us-west"),
            Some(PartitionScalar::String("us-west".to_owned()))
        );
    }

    #[test]
    fn integer_value_kind_parses_signed_base10_text_with_width_bounds() {
        let byte_kind = PartitionMetadataValueKind::SignedInteger {
            min: i64::from(i8::MIN),
            max: i64::from(i8::MAX),
        };
        let int_kind = PartitionMetadataValueKind::SignedInteger {
            min: i64::from(i32::MIN),
            max: i64::from(i32::MAX),
        };

        assert_eq!(
            byte_kind.parse_raw("-128"),
            Some(PartitionScalar::SignedInteger(-128))
        );
        assert_eq!(
            byte_kind.parse_raw("127"),
            Some(PartitionScalar::SignedInteger(127))
        );
        assert_eq!(byte_kind.parse_raw("-129"), None);
        assert_eq!(byte_kind.parse_raw("128"), None);
        assert_eq!(byte_kind.parse_raw(""), None);
        assert_eq!(byte_kind.parse_raw("not-an-integer"), None);
        assert_eq!(
            int_kind.parse_raw("2147483647"),
            Some(PartitionScalar::SignedInteger(2147483647))
        );
        assert_eq!(int_kind.parse_raw("2147483648"), None);
    }

    #[test]
    fn boolean_value_kind_parses_lowercase_delta_metadata_text() {
        assert_eq!(
            PartitionMetadataValueKind::Boolean.parse_raw("true"),
            Some(PartitionScalar::Boolean(true))
        );
        assert_eq!(
            PartitionMetadataValueKind::Boolean.parse_raw("false"),
            Some(PartitionScalar::Boolean(false))
        );
        assert_eq!(PartitionMetadataValueKind::Boolean.parse_raw("TRUE"), None);
        assert_eq!(PartitionMetadataValueKind::Boolean.parse_raw("False"), None);
        assert_eq!(PartitionMetadataValueKind::Boolean.parse_raw(""), None);
        assert_eq!(
            PartitionMetadataValueKind::Boolean.parse_raw("not-a-boolean"),
            None
        );
        assert!(!PartitionMetadataValueKind::Boolean.supports_ordering());
    }

    #[test]
    fn date_value_kind_parses_delta_metadata_text_to_date32_days() {
        assert_eq!(
            PartitionMetadataValueKind::Date.parse_raw("1970-01-01"),
            Some(PartitionScalar::Date(0))
        );
        assert_eq!(
            PartitionMetadataValueKind::Date.parse_raw("1969-12-31"),
            Some(PartitionScalar::Date(-1))
        );
        assert_eq!(
            PartitionMetadataValueKind::Date.parse_raw("2026-01-01"),
            Some(PartitionScalar::Date(20_454))
        );
        assert_eq!(
            PartitionMetadataValueKind::Date.parse_raw("2024-02-29"),
            Some(PartitionScalar::Date(19_782))
        );
        assert_eq!(PartitionMetadataValueKind::Date.parse_raw(""), None);
        assert_eq!(
            PartitionMetadataValueKind::Date.parse_raw("not-a-date"),
            None
        );
        assert_eq!(
            PartitionMetadataValueKind::Date.parse_raw("2026-02-29"),
            None
        );
        assert_eq!(
            PartitionMetadataValueKind::Date.parse_raw("2026-1-01"),
            None
        );
        assert_eq!(
            PartitionMetadataValueKind::Date.parse_raw("2026-01-1"),
            None
        );
        assert_eq!(
            PartitionMetadataValueKind::Date.parse_raw("2026-01-01T00:00:00"),
            None
        );
        assert!(PartitionMetadataValueKind::Date.supports_ordering());
    }

    #[test]
    fn timestamp_value_kind_parses_utc_metadata_text_to_microseconds() {
        assert_eq!(
            PartitionMetadataValueKind::TimestampUtc.parse_raw("1970-01-01T00:00:00Z"),
            Some(PartitionScalar::TimestampUtc(0))
        );
        assert_eq!(
            PartitionMetadataValueKind::TimestampUtc.parse_raw("2026-01-01T00:00:00.123456Z"),
            Some(PartitionScalar::TimestampUtc(1_767_225_600_123_456))
        );
        assert_eq!(
            PartitionMetadataValueKind::TimestampUtc.parse_raw("2026-01-01 00:00:00.123456"),
            Some(PartitionScalar::TimestampUtc(1_767_225_600_123_456))
        );
        assert_eq!(
            PartitionMetadataValueKind::TimestampUtc.parse_raw("1969-12-31 23:59:59.999999"),
            Some(PartitionScalar::TimestampUtc(-1))
        );
        assert_eq!(
            PartitionMetadataValueKind::TimestampUtc.parse_raw("2026-01-01T00:00:00+00:00"),
            Some(PartitionScalar::TimestampUtc(1_767_225_600_000_000))
        );
        assert_eq!(
            PartitionMetadataValueKind::TimestampUtc.parse_raw("2026-01-01T00:00:00+01:00"),
            None
        );
        assert_eq!(
            PartitionMetadataValueKind::TimestampUtc.parse_raw("2026-01-01T00:00:00.123456789Z"),
            None
        );
        assert_eq!(
            PartitionMetadataValueKind::TimestampUtc.parse_raw("2026-01-01 00:00:00.123456789"),
            None
        );
        assert_eq!(
            PartitionMetadataValueKind::TimestampUtc.parse_raw("2026-01-01"),
            None
        );
        assert_eq!(PartitionMetadataValueKind::TimestampUtc.parse_raw(""), None);
        assert!(PartitionMetadataValueKind::TimestampUtc.supports_ordering());
        assert!(PartitionMetadataValueKind::TimestampUtc.supports_between());
    }

    #[test]
    fn binary_value_kind_is_promoted_only_for_null_checks() {
        assert_eq!(PartitionMetadataValueKind::Binary.parse_raw("hello"), None);
        assert_eq!(PartitionMetadataValueKind::Binary.parse_raw(""), None);
        assert!(!PartitionMetadataValueKind::Binary.supports_ordering());
        assert!(!PartitionMetadataValueKind::Binary.supports_between());
    }

    #[test]
    fn decimal_value_kind_parses_delta_metadata_text_to_scaled_i128() {
        let decimal = PartitionMetadataValueKind::Decimal {
            precision: 10,
            scale: 2,
        };
        let high_precision_decimal = PartitionMetadataValueKind::Decimal {
            precision: 38,
            scale: 18,
        };

        assert_eq!(
            decimal.parse_raw("123.45"),
            Some(PartitionScalar::Decimal(12_345))
        );
        assert_eq!(
            decimal.parse_raw("-1.23"),
            Some(PartitionScalar::Decimal(-123))
        );
        assert_eq!(decimal.parse_raw("0.00"), Some(PartitionScalar::Decimal(0)));
        assert_eq!(
            decimal.parse_raw("1.0"),
            Some(PartitionScalar::Decimal(100))
        );
        assert_eq!(decimal.parse_raw("1"), Some(PartitionScalar::Decimal(100)));
        assert_eq!(
            decimal.parse_raw("12345678.90"),
            Some(PartitionScalar::Decimal(1_234_567_890))
        );
        assert_eq!(
            high_precision_decimal.parse_raw("1.230000000000000000"),
            Some(PartitionScalar::Decimal(1_230_000_000_000_000_000))
        );
        assert_eq!(
            high_precision_decimal.parse_raw("0E-18"),
            Some(PartitionScalar::Decimal(0))
        );
        assert_eq!(
            high_precision_decimal.parse_raw("1.23E-16"),
            Some(PartitionScalar::Decimal(123))
        );
        assert_eq!(
            decimal.parse_raw("-1.23E+2"),
            Some(PartitionScalar::Decimal(-12_300))
        );
        assert_eq!(decimal.parse_raw("123456789.01"), None);
        assert_eq!(decimal.parse_raw("1.234"), None);
        assert_eq!(decimal.parse_raw("1.234E-1"), None);
        assert_eq!(decimal.parse_raw("1E39"), None);
        assert_eq!(decimal.parse_raw("1E2147483647"), None);
        assert_eq!(decimal.parse_raw("1E-2147483648"), None);
        assert_eq!(decimal.parse_raw(""), None);
        assert_eq!(decimal.parse_raw("-"), None);
        assert_eq!(decimal.parse_raw(".12"), None);
        assert_eq!(decimal.parse_raw("1."), None);
        assert_eq!(decimal.parse_raw("+1.23"), None);
        assert_eq!(decimal.parse_raw("1EE2"), None);
        assert_eq!(decimal.parse_raw("not-a-decimal"), None);
        assert!(decimal.supports_ordering());
        assert!(decimal.supports_between());
    }

    #[test]
    fn floating_value_kinds_parse_delta_metadata_text_to_bits() {
        assert_eq!(
            PartitionMetadataValueKind::Float32.parse_raw("1.5"),
            Some(PartitionScalar::Float32(1.5_f32.to_bits()))
        );
        assert_eq!(
            PartitionMetadataValueKind::Float32.parse_raw("-0.0"),
            Some(PartitionScalar::Float32((-0.0_f32).to_bits()))
        );
        assert_eq!(
            PartitionMetadataValueKind::Float32.parse_raw("0.0"),
            Some(PartitionScalar::Float32(0.0_f32.to_bits()))
        );
        assert_ne!(
            PartitionMetadataValueKind::Float32.parse_raw("-0.0"),
            PartitionMetadataValueKind::Float32.parse_raw("0.0")
        );
        assert_eq!(
            PartitionMetadataValueKind::Float64.parse_raw("-2.25"),
            Some(PartitionScalar::Float64((-2.25_f64).to_bits()))
        );
        assert_eq!(
            PartitionMetadataValueKind::Float64.parse_raw("1.25E-2"),
            Some(PartitionScalar::Float64(1.25E-2_f64.to_bits()))
        );
        assert_eq!(
            PartitionMetadataValueKind::Float32.parse_raw("Infinity"),
            Some(PartitionScalar::Float32(f32::INFINITY.to_bits()))
        );
        assert_eq!(
            PartitionMetadataValueKind::Float32.parse_raw("-Infinity"),
            Some(PartitionScalar::Float32(f32::NEG_INFINITY.to_bits()))
        );
        assert!(
            matches!(
                PartitionMetadataValueKind::Float32.parse_raw("NaN"),
                Some(PartitionScalar::Float32(value)) if f32::from_bits(value).is_nan()
            ),
            "raw NaN should stay comparable instead of becoming metadata null"
        );
        assert_eq!(
            PartitionMetadataValueKind::Float64.parse_raw("Infinity"),
            Some(PartitionScalar::Float64(f64::INFINITY.to_bits()))
        );
        assert_eq!(
            PartitionMetadataValueKind::Float64.parse_raw("-Infinity"),
            Some(PartitionScalar::Float64(f64::NEG_INFINITY.to_bits()))
        );
        assert!(
            matches!(
                PartitionMetadataValueKind::Float64.parse_raw("NaN"),
                Some(PartitionScalar::Float64(value)) if f64::from_bits(value).is_nan()
            ),
            "raw NaN should stay comparable instead of becoming metadata null"
        );

        for raw_value in ["", "not-a-float", "inf", "-inf", "nan"] {
            assert_eq!(
                PartitionMetadataValueKind::Float32.parse_raw(raw_value),
                None
            );
            assert_eq!(
                PartitionMetadataValueKind::Float64.parse_raw(raw_value),
                None
            );
        }
        assert!(PartitionMetadataValueKind::Float32.supports_ordering());
        assert!(PartitionMetadataValueKind::Float32.supports_between());
        assert!(PartitionMetadataValueKind::Float64.supports_ordering());
        assert!(PartitionMetadataValueKind::Float64.supports_between());
    }

    #[test]
    fn floating_scalars_compare_with_total_ordering() {
        let negative_zero = PartitionScalar::Float32((-0.0_f32).to_bits());
        let positive_zero = PartitionScalar::Float32(0.0_f32.to_bits());
        let positive_one = PartitionScalar::Float32(1.0_f32.to_bits());
        let positive_infinity = PartitionScalar::Float32(f32::INFINITY.to_bits());
        let positive_nan = PartitionScalar::Float32(f32::NAN.to_bits());
        let negative_infinity = PartitionScalar::Float64(f64::NEG_INFINITY.to_bits());
        let negative_double = PartitionScalar::Float64((-2.25_f64).to_bits());
        let positive_double = PartitionScalar::Float64(0.0_f64.to_bits());

        assert_eq!(
            negative_infinity.compare(&negative_double),
            Some(Ordering::Less)
        );
        assert_eq!(negative_zero.compare(&positive_zero), Some(Ordering::Less));
        assert_eq!(
            positive_zero.compare(&negative_zero),
            Some(Ordering::Greater)
        );
        assert_eq!(positive_zero.compare(&positive_one), Some(Ordering::Less));
        assert_eq!(
            negative_double.compare(&positive_double),
            Some(Ordering::Less)
        );
        assert_eq!(
            positive_one.compare(&positive_infinity),
            Some(Ordering::Less)
        );
        assert_eq!(
            positive_infinity.compare(&positive_nan),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn decimal_literal_normalization_rescales_only_when_exact_and_in_bounds() {
        let decimal = PartitionMetadataValueKind::Decimal {
            precision: 10,
            scale: 2,
        };

        assert_eq!(
            decimal.normalize_decimal_literal(12_345, 10, 2),
            Some(PartitionScalar::Decimal(12_345))
        );
        assert_eq!(
            decimal.normalize_decimal_literal(1_234, 10, 1),
            Some(PartitionScalar::Decimal(12_340))
        );
        assert_eq!(
            decimal.normalize_decimal_literal(123_450, 12, 3),
            Some(PartitionScalar::Decimal(12_345))
        );
        assert_eq!(decimal.normalize_decimal_literal(12_346, 10, 3), None);
        assert_eq!(
            decimal.normalize_decimal_literal(12_345_678_901, 11, 2),
            None
        );
        assert_eq!(decimal.normalize_decimal_literal(12_345, 10, -1), None);
        assert_eq!(decimal.normalize_decimal_literal(12_345, 39, 2), None);
    }
}
