use std::cmp::Ordering;

use chrono::{Datelike, NaiveDate};
use datafusion::arrow::datatypes::DataType;

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
            _ => None,
        }
    }

    pub(super) fn is_boolean(self) -> bool {
        matches!(self, Self::Boolean)
    }

    pub(super) fn supports_ordering(self) -> bool {
        match self {
            Self::String | Self::SignedInteger { .. } | Self::Date | Self::Decimal { .. } => true,
            Self::Boolean => false,
        }
    }

    pub(super) fn supports_between(self) -> bool {
        match self {
            Self::String | Self::SignedInteger { .. } | Self::Date | Self::Decimal { .. } => true,
            Self::Boolean => false,
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

    let shift = i32::from(scale) + exponent - fraction.len() as i32;
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
}

impl PartitionScalar {
    pub(super) fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::String(left), Self::String(right)) => Some(left.cmp(right)),
            (Self::SignedInteger(left), Self::SignedInteger(right)) => Some(left.cmp(right)),
            (Self::Date(left), Self::Date(right)) => Some(left.cmp(right)),
            (Self::Decimal(left), Self::Decimal(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;

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
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Float64),
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
