//! Delta source registration name validation.

use std::collections::HashSet;

use crate::DeltaFunnelError;

const EMPTY_NAME: &str = "source names must not be empty";
const INVALID_FIRST_CHARACTER: &str = "source names must start with an ASCII letter or underscore";
const INVALID_CHARACTER: &str =
    "source names may contain only ASCII letters, digits, and underscores";

/// Validates Delta source names before registration.
///
/// Source names are DataFusion table names for the MVP. They intentionally use
/// a simple unquoted identifier subset: ASCII letters, digits, and underscores,
/// with a letter or underscore as the first character. Duplicate checks are
/// case-insensitive so unquoted SQL references cannot become ambiguous.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::InvalidSourceName`] for the first invalid name,
/// or [`DeltaFunnelError::DuplicateSourceName`] for the first case-insensitive
/// duplicate.
pub(crate) fn validate_delta_source_names<I, S>(names: I) -> Result<(), DeltaFunnelError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut seen = HashSet::new();

    for name in names {
        let name = name.as_ref();
        validate_source_name(name)?;

        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(DeltaFunnelError::DuplicateSourceName {
                name: name.to_owned(),
            });
        }
    }

    Ok(())
}

fn validate_source_name(name: &str) -> Result<(), DeltaFunnelError> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(invalid_source_name(name, EMPTY_NAME));
    };

    if !is_valid_first_character(first) {
        return Err(invalid_source_name(name, INVALID_FIRST_CHARACTER));
    }

    if chars.all(is_valid_following_character) {
        Ok(())
    } else {
        Err(invalid_source_name(name, INVALID_CHARACTER))
    }
}

fn invalid_source_name(name: &str, reason: &'static str) -> DeltaFunnelError {
    DeltaFunnelError::InvalidSourceName {
        name: name.to_owned(),
        reason,
    }
}

fn is_valid_first_character(value: char) -> bool {
    value == '_' || value.is_ascii_alphabetic()
}

fn is_valid_following_character(value: char) -> bool {
    value == '_' || value.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::validate_delta_source_names;
    use crate::DeltaFunnelError;

    fn assert_invalid_name(input: &str, expected_reason: &'static str) {
        let result = validate_delta_source_names([input]);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::InvalidSourceName { name, reason })
                if name == input && reason == expected_reason
        ));
    }

    #[test]
    fn accepts_simple_unquoted_identifiers() -> Result<(), DeltaFunnelError> {
        validate_delta_source_names(["orders", "_customers", "Regions_2026", "line_items"])?;

        Ok(())
    }

    #[test]
    fn rejects_empty_source_names() {
        assert_invalid_name("", "source names must not be empty");
    }

    #[test]
    fn rejects_source_names_that_start_with_a_digit() {
        assert_invalid_name(
            "2026_orders",
            "source names must start with an ASCII letter or underscore",
        );
    }

    #[test]
    fn rejects_names_that_need_quoting_or_qualification() {
        for (name, reason) in [
            (
                "orders.latest",
                "source names may contain only ASCII letters, digits, and underscores",
            ),
            (
                "line-items",
                "source names may contain only ASCII letters, digits, and underscores",
            ),
            (
                "line items",
                "source names may contain only ASCII letters, digits, and underscores",
            ),
            (
                "\"orders\"",
                "source names must start with an ASCII letter or underscore",
            ),
            (
                "orders$",
                "source names may contain only ASCII letters, digits, and underscores",
            ),
            (
                "ordérs",
                "source names may contain only ASCII letters, digits, and underscores",
            ),
        ] {
            assert_invalid_name(name, reason);
        }
    }

    #[test]
    fn rejects_case_insensitive_duplicates() {
        let result = validate_delta_source_names(["orders", "customers", "Orders"]);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "Orders"
        ));
    }

    #[test]
    fn invalid_name_wins_before_duplicate_detection() {
        let result = validate_delta_source_names(["orders", "orders.latest", "Orders"]);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::InvalidSourceName { name, reason })
                if name == "orders.latest"
                    && reason == "source names may contain only ASCII letters, digits, and underscores"
        ));
    }
}
