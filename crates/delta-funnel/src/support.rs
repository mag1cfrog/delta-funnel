//! Cross-cutting crate-internal support helpers.

mod redaction;

pub(crate) use redaction::{sanitize_text_for_display, sanitize_uri_for_display};
