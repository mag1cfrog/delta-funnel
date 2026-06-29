//! Cross-cutting crate-internal support helpers.

mod redaction;

pub(crate) use redaction::sanitize_text_for_display;
pub use redaction::sanitize_uri_for_display;
