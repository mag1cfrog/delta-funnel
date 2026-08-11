//! Table-format source policy.

use std::collections::HashSet;

use delta_arrow_reader::DeltaStorageOptions;
use delta_kernel::object_store::aws::AmazonS3ConfigKey;
use url::Url;

mod name;

#[cfg(test)]
#[path = "table_formats/test_support.rs"]
mod test_support;

pub(crate) use name::validate_table_source_names;
#[cfg(test)]
pub(crate) use test_support::RealParquetDeltaTable;

/// Caller-provided configuration for one named Delta source.
pub struct DeltaSourceConfig {
    /// DataFusion table name that will identify this source.
    pub name: String,
    /// Caller-provided Delta table location.
    pub table_uri: String,
    /// Optional fixed Delta table version.
    pub version: Option<u64>,
    /// Source-local options forwarded to Delta object-store construction.
    pub storage_options: DeltaStorageOptions,
}

impl DeltaSourceConfig {
    /// Builds a Delta source config with no fixed version or storage options.
    #[must_use]
    pub fn new(name: impl Into<String>, table_uri: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table_uri: table_uri.into(),
            version: None,
            storage_options: DeltaStorageOptions::default(),
        }
    }

    /// Sets an optional fixed Delta table version.
    #[must_use]
    pub fn with_version(mut self, version: Option<u64>) -> Self {
        self.version = version;
        self
    }

    /// Sets source-local storage options.
    #[must_use]
    pub fn with_storage_options(mut self, storage_options: DeltaStorageOptions) -> Self {
        self.storage_options = storage_options;
        self
    }

    pub(crate) fn apply_environment_storage_options(&mut self) {
        self.storage_options = effective_storage_options_for_source_from_env(
            &self.table_uri,
            std::mem::take(&mut self.storage_options),
            std::env::vars(),
        );
    }

    pub(crate) fn s3_auth_mode_hint(&self) -> Option<S3AuthModeHint> {
        s3_auth_mode_hint_for_source(&self.table_uri, &self.storage_options)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum S3AuthModeHint {
    ExplicitStatic,
    ExplicitWebIdentity,
    ExplicitContainer,
    ImplicitProviderChain,
    OtherExplicit,
}

impl S3AuthModeHint {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitStatic => "explicit_static",
            Self::ExplicitWebIdentity => "explicit_web_identity",
            Self::ExplicitContainer => "explicit_container",
            Self::ImplicitProviderChain => "implicit_provider_chain",
            Self::OtherExplicit => "other_explicit",
        }
    }
}

fn effective_storage_options_for_source_from_env<I>(
    table_uri: &str,
    storage_options: DeltaStorageOptions,
    env: I,
) -> DeltaStorageOptions
where
    I: IntoIterator<Item = (String, String)>,
{
    if s3_auth_mode_hint_for_source(table_uri, &storage_options).is_none() {
        return storage_options;
    }

    let caller_s3_keys = storage_options
        .keys()
        .filter_map(|key| s3_option_precedence_key(key))
        .collect::<HashSet<_>>();
    let mut effective = env
        .into_iter()
        .filter(|(key, _)| key.starts_with("AWS_"))
        .filter(|(key, _)| match s3_option_precedence_key(key) {
            Some(key) => !caller_s3_keys.contains(&key),
            None => true,
        })
        .collect::<DeltaStorageOptions>();
    effective.extend(storage_options);
    effective
}

fn s3_option_precedence_key(key: &str) -> Option<String> {
    let key = key.to_ascii_lowercase().parse::<AmazonS3ConfigKey>().ok()?;
    Some(match key {
        AmazonS3ConfigKey::DefaultRegion | AmazonS3ConfigKey::Region => "aws_region".to_owned(),
        _ => key.as_ref().to_owned(),
    })
}

fn s3_auth_mode_hint_for_source(
    table_uri: &str,
    storage_options: &DeltaStorageOptions,
) -> Option<S3AuthModeHint> {
    let table_url = Url::parse(table_uri).ok()?;
    if !is_s3_compatible_uri(table_url.scheme(), table_url.host_str()) {
        return None;
    }

    Some(classify_s3_auth_mode(storage_options))
}

fn is_s3_compatible_uri(scheme: &str, host: Option<&str>) -> bool {
    match (scheme, host) {
        ("s3" | "s3a", Some(_)) => true,
        ("https", Some(host)) => {
            let host = host.to_ascii_lowercase();
            host.ends_with("amazonaws.com") || host.ends_with("r2.cloudflarestorage.com")
        }
        _ => false,
    }
}

fn classify_s3_auth_mode(storage_options: &DeltaStorageOptions) -> S3AuthModeHint {
    let mut access_key = false;
    let mut secret_key = false;
    let mut web_identity_token = false;
    let mut role_arn = false;
    let mut container_relative_uri = false;
    let mut container_full_uri = false;
    let mut container_token_file = false;
    let mut auth_option = false;

    for key in storage_options.keys() {
        match key.to_ascii_lowercase().as_str() {
            "aws_access_key_id" | "access_key_id" => {
                access_key = true;
                auth_option = true;
            }
            "aws_secret_access_key" | "secret_access_key" => {
                secret_key = true;
                auth_option = true;
            }
            "aws_session_token" | "aws_token" | "session_token" | "token" => {
                auth_option = true;
            }
            "aws_web_identity_token_file" | "web_identity_token_file" => {
                web_identity_token = true;
                auth_option = true;
            }
            "aws_role_arn" | "role_arn" => {
                role_arn = true;
                auth_option = true;
            }
            "aws_role_session_name"
            | "role_session_name"
            | "aws_endpoint_url_sts"
            | "endpoint_url_sts" => auth_option = true,
            "aws_container_credentials_relative_uri" | "container_credentials_relative_uri" => {
                container_relative_uri = true;
                auth_option = true;
            }
            "aws_container_credentials_full_uri" | "container_credentials_full_uri" => {
                container_full_uri = true;
                auth_option = true;
            }
            "aws_container_authorization_token_file" | "container_authorization_token_file" => {
                container_token_file = true;
                auth_option = true;
            }
            "aws_imdsv1_fallback"
            | "imdsv1_fallback"
            | "aws_metadata_endpoint"
            | "metadata_endpoint"
            | "aws_unsigned_payload"
            | "unsigned_payload"
            | "aws_skip_signature"
            | "skip_signature" => auth_option = true,
            _ => {}
        }
    }

    if access_key && secret_key {
        S3AuthModeHint::ExplicitStatic
    } else if web_identity_token && role_arn {
        S3AuthModeHint::ExplicitWebIdentity
    } else if container_relative_uri || (container_full_uri && container_token_file) {
        S3AuthModeHint::ExplicitContainer
    } else if auth_option {
        S3AuthModeHint::OtherExplicit
    } else {
        S3AuthModeHint::ImplicitProviderChain
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage_options(entries: &[(&str, &str)]) -> DeltaStorageOptions {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn env_vars(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn s3_sources_merge_aws_environment_options() {
        for table_uri in [
            "s3://bucket/root",
            "s3a://bucket/root",
            "https://s3.us-east-1.amazonaws.com/bucket/root",
            "https://bucket.s3.us-east-1.amazonaws.com/root",
            "https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket/root",
        ] {
            let effective = effective_storage_options_for_source_from_env(
                table_uri,
                DeltaStorageOptions::default(),
                env_vars(&[
                    ("AWS_ACCESS_KEY_ID", "env-access"),
                    ("AWS_SECRET_ACCESS_KEY", "env-secret"),
                    ("AWS_REGION", "us-east-1"),
                    ("AWS_CUSTOM_FUTURE_OPTION", "future"),
                    ("NOT_AWS", "ignored"),
                ]),
            );

            assert_eq!(
                effective.get("AWS_ACCESS_KEY_ID").map(String::as_str),
                Some("env-access")
            );
            assert_eq!(
                effective.get("AWS_SECRET_ACCESS_KEY").map(String::as_str),
                Some("env-secret")
            );
            assert_eq!(
                effective.get("AWS_REGION").map(String::as_str),
                Some("us-east-1")
            );
            assert_eq!(
                effective
                    .get("AWS_CUSTOM_FUTURE_OPTION")
                    .map(String::as_str),
                Some("future")
            );
            assert!(!effective.contains_key("NOT_AWS"));
        }
    }

    #[test]
    fn explicit_s3_options_override_equivalent_environment_options() {
        let effective = effective_storage_options_for_source_from_env(
            "s3://bucket/root",
            storage_options(&[
                ("aws_access_key_id", "caller-access"),
                ("AWS_SECRET_ACCESS_KEY", "caller-secret"),
                ("region", "caller-region"),
            ]),
            env_vars(&[
                ("AWS_ACCESS_KEY_ID", "env-access"),
                ("AWS_SECRET_ACCESS_KEY", "env-secret"),
                ("AWS_REGION", "env-region"),
                ("AWS_CUSTOM_FUTURE_OPTION", "future"),
            ]),
        );

        assert_eq!(
            effective.get("aws_access_key_id").map(String::as_str),
            Some("caller-access")
        );
        assert_eq!(
            effective.get("AWS_SECRET_ACCESS_KEY").map(String::as_str),
            Some("caller-secret")
        );
        assert_eq!(
            effective.get("region").map(String::as_str),
            Some("caller-region")
        );
        assert_eq!(
            effective
                .get("AWS_CUSTOM_FUTURE_OPTION")
                .map(String::as_str),
            Some("future")
        );
        assert!(!effective.contains_key("AWS_ACCESS_KEY_ID"));
        assert!(!effective.contains_key("AWS_REGION"));
    }

    #[test]
    fn non_s3_sources_do_not_merge_aws_environment_options() {
        let caller = storage_options(&[("authorization", "caller-token")]);
        let effective = effective_storage_options_for_source_from_env(
            "file:///tmp/table",
            caller.clone(),
            env_vars(&[("AWS_ACCESS_KEY_ID", "env-access")]),
        );

        assert_eq!(effective, caller);
    }

    #[test]
    fn s3_auth_hint_detects_supported_uris() {
        for table_uri in [
            "s3://bucket/table",
            "s3a://bucket/table",
            "https://s3.us-east-1.amazonaws.com/bucket/table",
            "https://bucket.s3.us-east-1.amazonaws.com/table",
            "https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket/table",
        ] {
            assert_eq!(
                s3_auth_mode_hint_for_source(table_uri, &DeltaStorageOptions::default()),
                Some(S3AuthModeHint::ImplicitProviderChain)
            );
        }

        for table_uri in ["file:///tmp/table", "https://example.com/table"] {
            assert_eq!(
                s3_auth_mode_hint_for_source(table_uri, &DeltaStorageOptions::default()),
                None
            );
        }
    }

    #[test]
    fn s3_auth_hint_classifies_explicit_credentials() {
        let cases = [
            (
                storage_options(&[
                    ("AWS_ACCESS_KEY_ID", "access"),
                    ("AWS_SECRET_ACCESS_KEY", "secret"),
                ]),
                S3AuthModeHint::ExplicitStatic,
            ),
            (
                storage_options(&[
                    ("aws_web_identity_token_file", "/token"),
                    ("aws_role_arn", "arn:aws:iam::123456789012:role/Test"),
                ]),
                S3AuthModeHint::ExplicitWebIdentity,
            ),
            (
                storage_options(&[("aws_container_credentials_relative_uri", "/credentials")]),
                S3AuthModeHint::ExplicitContainer,
            ),
            (
                storage_options(&[("AWS_SESSION_TOKEN", "partial")]),
                S3AuthModeHint::OtherExplicit,
            ),
            (
                storage_options(&[("AWS_REGION", "us-east-1")]),
                S3AuthModeHint::ImplicitProviderChain,
            ),
        ];

        for (options, expected) in cases {
            assert_eq!(
                s3_auth_mode_hint_for_source("s3://bucket/table", &options),
                Some(expected)
            );
        }
    }
}
