//! Delta source snapshot loading.

use crate::{
    DeltaFunnelError,
    error::{DeltaSnapshotLoadSnafu, DeltaSourceEngineSnafu, InvalidSourceUriSnafu},
    support::{sanitize_text_for_display, sanitize_uri_for_display},
};

use super::DeltaStorageOptions;
use super::kernel::{
    DefaultEngineBuilder, Snapshot, SnapshotRef, Version, store_from_url_opts, try_parse_uri,
};
use super::uri::normalize_delta_table_uri;

const ENGINE_CONSTRUCTION_FAILED: &str = "object store engine could not be constructed";
const SNAPSHOT_LOAD_FAILED: &str = "snapshot could not be loaded";
const S3_IMPLICIT_CREDENTIAL_HINT: &str = "S3 credential hint: no explicit S3 credentials were supplied through storage_options; local shells may need explicit AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, optional AWS_SESSION_TOKEN, and AWS_REGION.";

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

/// Loaded Delta table snapshot state.
///
/// This is intentionally narrower than a named source config. It proves and
/// owns the source-side state that later protocol and DataFusion provider
/// slices can consume without reloading the snapshot.
pub(crate) struct LoadedDeltaTableSnapshot {
    table_uri: String,
    snapshot: SnapshotRef,
}

impl LoadedDeltaTableSnapshot {
    /// Normalized Delta table URI used to load the snapshot.
    #[must_use]
    pub(crate) fn table_uri(&self) -> &str {
        &self.table_uri
    }

    /// Loaded Delta table version.
    #[must_use]
    pub(crate) fn version(&self) -> Version {
        self.kernel_snapshot().version()
    }

    pub(crate) fn kernel_snapshot(&self) -> &SnapshotRef {
        &self.snapshot
    }
}

struct DeltaKernelEngine {
    inner: Box<dyn delta_kernel::Engine + Send + Sync>,
}

impl DeltaKernelEngine {
    fn build(
        table_uri: &str,
        storage_options: &DeltaStorageOptions,
    ) -> Result<Self, DeltaFunnelError> {
        let table_url = match try_parse_uri(table_uri) {
            Ok(table_url) => table_url,
            Err(_) => {
                return InvalidSourceUriSnafu {
                    reason: "normalized table URI could not be parsed",
                }
                .fail();
            }
        };
        let store = match store_from_url_opts(
            &table_url,
            storage_options
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        ) {
            Ok(store) => store,
            Err(_) => {
                return DeltaSourceEngineSnafu {
                    reason: ENGINE_CONSTRUCTION_FAILED,
                }
                .fail();
            }
        };

        Ok(Self {
            inner: Box::new(DefaultEngineBuilder::new(store).build()),
        })
    }

    fn as_kernel_engine(&self) -> &dyn delta_kernel::Engine {
        self.inner.as_ref()
    }
}

/// Loads the latest or requested snapshot for a Delta table URI.
///
/// The table URI is normalized through [`normalize_delta_table_uri`] before
/// engine construction and snapshot loading.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::InvalidSourceUri`] when the table URI cannot be
/// normalized, [`DeltaFunnelError::DeltaSourceEngine`] when the object-store
/// backed default engine cannot be constructed, or
/// [`DeltaFunnelError::DeltaSnapshotLoad`] when `delta_kernel` cannot load the
/// requested snapshot.
pub(crate) fn load_delta_table_snapshot(
    table_uri: impl AsRef<str>,
    version: Option<Version>,
    storage_options: &DeltaStorageOptions,
) -> Result<LoadedDeltaTableSnapshot, DeltaFunnelError> {
    let table_uri = normalize_delta_table_uri(table_uri)?;
    let s3_auth_mode_hint = s3_auth_mode_hint_for_source(&table_uri, storage_options);
    let engine = DeltaKernelEngine::build(&table_uri, storage_options)?;

    let mut builder = Snapshot::builder_for(&table_uri);
    if let Some(version) = version {
        builder = builder.at_version(version);
    }

    let snapshot = match builder.build(engine.as_kernel_engine()) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return DeltaSnapshotLoadSnafu {
                reason: snapshot_load_failed_reason(&error.to_string(), s3_auth_mode_hint),
            }
            .fail();
        }
    };

    Ok(LoadedDeltaTableSnapshot {
        table_uri,
        snapshot,
    })
}

pub(crate) fn s3_auth_mode_hint_for_source(
    table_uri: &str,
    storage_options: &DeltaStorageOptions,
) -> Option<S3AuthModeHint> {
    let table_url = try_parse_uri(table_uri).ok()?;
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
    let mut has_access_key_id = false;
    let mut has_secret_access_key = false;
    let mut has_web_identity_token_file = false;
    let mut has_role_arn = false;
    let mut has_container_credentials_uri = false;
    let mut has_auth_related_option = false;

    for key in storage_options.keys() {
        match key.to_ascii_lowercase().as_str() {
            "aws_access_key_id" | "access_key_id" => {
                has_access_key_id = true;
                has_auth_related_option = true;
            }
            "aws_secret_access_key" | "secret_access_key" => {
                has_secret_access_key = true;
                has_auth_related_option = true;
            }
            "aws_session_token" | "aws_token" | "session_token" | "token" => {
                has_auth_related_option = true;
            }
            "aws_web_identity_token_file" | "web_identity_token_file" => {
                has_web_identity_token_file = true;
                has_auth_related_option = true;
            }
            "aws_role_arn" | "role_arn" => {
                has_role_arn = true;
                has_auth_related_option = true;
            }
            "aws_role_session_name"
            | "role_session_name"
            | "aws_endpoint_url_sts"
            | "endpoint_url_sts" => {
                has_auth_related_option = true;
            }
            "aws_container_credentials_relative_uri"
            | "container_credentials_relative_uri"
            | "aws_container_credentials_full_uri"
            | "container_credentials_full_uri" => {
                has_container_credentials_uri = true;
                has_auth_related_option = true;
            }
            "aws_container_authorization_token_file"
            | "container_authorization_token_file"
            | "aws_skip_signature"
            | "skip_signature" => {
                has_auth_related_option = true;
            }
            _ => {}
        }
    }

    if has_access_key_id && has_secret_access_key {
        S3AuthModeHint::ExplicitStatic
    } else if has_web_identity_token_file && has_role_arn {
        S3AuthModeHint::ExplicitWebIdentity
    } else if has_container_credentials_uri {
        S3AuthModeHint::ExplicitContainer
    } else if has_auth_related_option {
        S3AuthModeHint::OtherExplicit
    } else {
        S3AuthModeHint::ImplicitProviderChain
    }
}

fn snapshot_load_failed_reason(cause: &str, s3_auth_mode_hint: Option<S3AuthModeHint>) -> String {
    let mut reason = format!(
        "{SNAPSHOT_LOAD_FAILED}: {}",
        sanitize_snapshot_load_cause(cause)
    );

    if s3_auth_mode_hint == Some(S3AuthModeHint::ImplicitProviderChain) {
        reason.push(' ');
        reason.push_str(S3_IMPLICIT_CREDENTIAL_HINT);
    }

    reason
}

fn sanitize_snapshot_load_cause(cause: &str) -> String {
    cause
        .split_whitespace()
        .map(|token| {
            if token.contains("://") {
                sanitize_uri_for_display(token)
            } else {
                sanitize_text_for_display(token)
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        DeltaStorageOptions, S3_IMPLICIT_CREDENTIAL_HINT, S3AuthModeHint, SNAPSHOT_LOAD_FAILED,
        load_delta_table_snapshot, s3_auth_mode_hint_for_source, snapshot_load_failed_reason,
    };
    use crate::DeltaFunnelError;

    struct DeltaLogTable {
        path: PathBuf,
    }

    struct TestDir {
        path: PathBuf,
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl DeltaLogTable {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-funnel-snapshot-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{PROTOCOL_JSON}\n{METADATA_JSON}\n"),
            )?;
            fs::write(
                log_path.join("00000000000000000001.json"),
                format!("{}\n", add_json("part-00001.parquet")),
            )?;

            Ok(Self { path })
        }
    }

    impl TestDir {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-funnel-broken-snapshot-tests")
                .join(unique_name(name)?);
            fs::create_dir_all(&path)?;

            Ok(Self { path })
        }
    }

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;

    fn add_json(path: &str) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true}}}}"#
        )
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

        Ok(format!("{}-{}-{nanos}", std::process::id(), name))
    }

    fn empty_storage_options() -> DeltaStorageOptions {
        DeltaStorageOptions::default()
    }

    fn storage_options(entries: &[(&str, &str)]) -> DeltaStorageOptions {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn loads_latest_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("latest")?;
        let loaded = load_delta_table_snapshot(
            table.path.to_string_lossy(),
            None,
            &empty_storage_options(),
        )?;

        assert_eq!(loaded.version(), 1);
        assert!(loaded.table_uri().starts_with("file://"));
        assert!(loaded.table_uri().ends_with('/'));

        Ok(())
    }

    #[test]
    fn loads_fixed_snapshot_version() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("fixed")?;
        let loaded = load_delta_table_snapshot(
            table.path.to_string_lossy(),
            Some(0),
            &empty_storage_options(),
        )?;

        assert_eq!(loaded.version(), 0);

        Ok(())
    }

    #[test]
    fn rejects_missing_fixed_snapshot_version() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("missing-version")?;
        let result = load_delta_table_snapshot(
            table.path.to_string_lossy(),
            Some(2),
            &empty_storage_options(),
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSnapshotLoad { .. })
        ));

        Ok(())
    }

    #[test]
    fn snapshot_load_error_includes_dependency_cause() -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::new("empty-table-cause")?;
        let result =
            load_delta_table_snapshot(dir.path.to_string_lossy(), None, &empty_storage_options());
        let reason = match result {
            Err(DeltaFunnelError::DeltaSnapshotLoad { reason }) => reason,
            _ => return Err("expected snapshot load error".into()),
        };

        assert!(reason.starts_with(&format!("{SNAPSHOT_LOAD_FAILED}: ")));
        assert_ne!(reason, SNAPSHOT_LOAD_FAILED);
        assert!(!reason.contains('\n'));

        Ok(())
    }

    #[test]
    fn snapshot_load_cause_redacts_secret_bearing_uris() {
        let reason = snapshot_load_failed_reason(
            "failed to read s3://user:password@example.com/table?token=secret#debug\nretry",
            None,
        );

        assert!(reason.contains("s3://example.com/table"));
        assert!(!reason.contains("user"));
        assert!(!reason.contains("password"));
        assert!(!reason.contains("token"));
        assert!(!reason.contains("secret"));
        assert!(!reason.contains('\n'));
    }

    #[test]
    fn s3_auth_mode_hint_detects_s3_compatible_source_uris() {
        for table_uri in [
            "s3://bucket/table",
            "s3a://bucket/table",
            "https://s3.us-east-1.amazonaws.com/bucket/table",
            "https://bucket.s3.us-east-1.amazonaws.com/table",
            "https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket/table",
        ] {
            assert_eq!(
                s3_auth_mode_hint_for_source(table_uri, &empty_storage_options()),
                Some(S3AuthModeHint::ImplicitProviderChain),
                "{table_uri}"
            );
        }

        for table_uri in ["file:///tmp/table", "https://example.com/table"] {
            assert_eq!(
                s3_auth_mode_hint_for_source(table_uri, &empty_storage_options()),
                None,
                "{table_uri}"
            );
        }
    }

    #[test]
    fn s3_auth_mode_hint_classifies_explicit_auth_configuration() {
        let cases = [
            (
                storage_options(&[
                    ("AWS_ACCESS_KEY_ID", "access"),
                    ("AWS_SECRET_ACCESS_KEY", "secret"),
                    ("AWS_REGION", "us-east-1"),
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

    #[test]
    fn s3_implicit_snapshot_load_hint_is_appended_only_for_implicit_auth() {
        let implicit = snapshot_load_failed_reason(
            "failed to read _delta_log",
            Some(S3AuthModeHint::ImplicitProviderChain),
        );
        let explicit = snapshot_load_failed_reason(
            "failed to read _delta_log",
            Some(S3AuthModeHint::ExplicitStatic),
        );
        let non_s3 = snapshot_load_failed_reason("failed to read _delta_log", None);

        assert!(implicit.contains(S3_IMPLICIT_CREDENTIAL_HINT));
        assert!(!explicit.contains(S3_IMPLICIT_CREDENTIAL_HINT));
        assert!(!non_s3.contains(S3_IMPLICIT_CREDENTIAL_HINT));
    }

    #[test]
    fn rejects_unsupported_object_store_scheme() {
        let result =
            load_delta_table_snapshot("ftp://example.com/table", None, &empty_storage_options());

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSourceEngine { .. })
        ));
    }

    #[test]
    fn rejects_existing_empty_directory_as_snapshot_load_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::new("empty-table")?;
        let result =
            load_delta_table_snapshot(dir.path.to_string_lossy(), None, &empty_storage_options());

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSnapshotLoad { .. })
        ));

        Ok(())
    }

    #[test]
    fn rejects_malformed_commit_json_as_snapshot_load_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::new("malformed-json")?;
        let log_path = dir.path.join("_delta_log");
        fs::create_dir_all(&log_path)?;
        fs::write(log_path.join("00000000000000000000.json"), "{not json\n")?;

        let result =
            load_delta_table_snapshot(dir.path.to_string_lossy(), None, &empty_storage_options());

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSnapshotLoad { .. })
        ));

        Ok(())
    }

    #[test]
    fn rejects_commit_without_protocol_or_metadata_as_snapshot_load_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::new("missing-protocol-metadata")?;
        let log_path = dir.path.join("_delta_log");
        fs::create_dir_all(&log_path)?;
        fs::write(
            log_path.join("00000000000000000000.json"),
            format!("{}\n", add_json("part-00000.parquet")),
        )?;

        let result =
            load_delta_table_snapshot(dir.path.to_string_lossy(), None, &empty_storage_options());

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSnapshotLoad { .. })
        ));

        Ok(())
    }

    #[test]
    fn rejects_regular_file_as_invalid_source_uri() -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::new("regular-file-parent")?;
        let file_path = dir.path.join("not-a-directory");
        fs::write(&file_path, "not a table")?;

        let result =
            load_delta_table_snapshot(file_path.to_string_lossy(), None, &empty_storage_options());

        assert!(matches!(
            result,
            Err(DeltaFunnelError::InvalidSourceUri { .. })
        ));

        Ok(())
    }

    #[test]
    fn snapshot_errors_do_not_expose_secret_bearing_uri() {
        let result = load_delta_table_snapshot(
            "ftp://user:password@example.com/table",
            None,
            &empty_storage_options(),
        );
        let error = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();

        assert!(!error.contains("user"));
        assert!(!error.contains("password"));
        assert!(!error.contains("example.com"));
        assert!(!error.contains("ftp://"));
    }
}
