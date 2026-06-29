//! Redaction helpers for user-facing display strings.

/// Escapes caller-provided text before it is displayed in logs, errors, and reports.
#[must_use]
pub(crate) fn sanitize_text_for_display(text: &str) -> String {
    text.chars().flat_map(char::escape_default).collect()
}

/// Sanitizes a URI for display in logs, errors, and reports.
#[must_use]
pub fn sanitize_uri_for_display(uri: &str) -> String {
    let uri = strip_fragment_and_query(uri);
    let uri = strip_userinfo(uri);

    sanitize_text_for_display(&uri)
}

/// Removes URI query strings and fragments before a URI is displayed.
///
/// Query strings commonly carry credential-bearing storage options such as
/// presigned URL signatures, SAS tokens, access tokens, or debugging flags that
/// should not appear in logs or Python-facing errors. URI fragments are less
/// common for Delta table locations, but callers can still pass local
/// client-side state such as `#version=42` or accidental secret material such
/// as `#access_token=...`. Neither part is needed to identify the table in a
/// sanitized error message.
fn strip_fragment_and_query(uri: &str) -> &str {
    uri.find(['?', '#']).map_or(uri, |index| &uri[..index])
}

/// Removes URI userinfo credentials before a URI is displayed.
///
/// Userinfo is the `user[:password]@` portion inside a URI authority, for
/// example `https://alice:secret@example.com/table` or
/// `s3://user:password@example.com/table`. It is uncommon for normal Delta
/// object-store paths, but it can appear in copied HTTP(S), FTP-like, proxy, or
/// test URLs. Keeping only the scheme, host, and path gives useful source
/// context without exposing embedded usernames or passwords.
fn strip_userinfo(uri: &str) -> String {
    let Some(scheme_end) = uri.find("://") else {
        return uri.to_owned();
    };
    let authority_start = scheme_end + 3;
    let authority_end = uri[authority_start..]
        .find('/')
        .map_or(uri.len(), |relative_end| authority_start + relative_end);
    let authority = &uri[authority_start..authority_end];
    let Some(userinfo_end) = authority.rfind('@') else {
        return uri.to_owned();
    };

    format!(
        "{}{}{}",
        &uri[..authority_start],
        &authority[userinfo_end + 1..],
        &uri[authority_end..]
    )
}

#[cfg(test)]
mod tests {
    use super::sanitize_uri_for_display;

    #[test]
    fn uri_display_redacts_query_fragment_and_userinfo() {
        let display =
            sanitize_uri_for_display("s3://user:password@example.com/table?token=secret#debug");

        assert_eq!(display, "s3://example.com/table");
    }

    #[test]
    fn uri_display_redacts_fragment_without_query() {
        let display = sanitize_uri_for_display("https://example.com/table#access_token=secret");

        assert_eq!(display, "https://example.com/table");
    }

    #[test]
    fn uri_display_preserves_at_signs_outside_authority() {
        let display = sanitize_uri_for_display("s3://bucket/path@name/table?token=secret");

        assert_eq!(display, "s3://bucket/path@name/table");
    }

    #[test]
    fn uri_display_escapes_control_characters() {
        let display = sanitize_uri_for_display("s3://bucket/table\nname");

        assert_eq!(display, r"s3://bucket/table\nname");
    }
}
