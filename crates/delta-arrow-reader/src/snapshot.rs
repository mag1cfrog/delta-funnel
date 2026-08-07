//! Immutable Delta snapshot loading.

use std::sync::Arc;

use snafu::ResultExt;

use crate::{
    DeltaProtocolInfo, DeltaReaderError, DeltaSnapshotSelection, DeltaStorageOptions,
    error::{SnapshotLoadSnafu, StorageInitializationSnafu},
    kernel::{DeltaKernelEngineContext, KernelSnapshot},
    uri::normalize_delta_table_uri,
};

#[derive(Clone)]
pub(crate) struct LoadedDeltaTableSnapshot {
    snapshot: KernelSnapshot,
    protocol_info: DeltaProtocolInfo,
    engine_context: Arc<DeltaKernelEngineContext>,
}

#[allow(dead_code)]
impl LoadedDeltaTableSnapshot {
    pub(crate) fn table_uri(&self) -> &str {
        self.engine_context.table_url().as_str()
    }

    pub(crate) fn version(&self) -> u64 {
        self.snapshot.version()
    }

    pub(crate) fn protocol_info(&self) -> &DeltaProtocolInfo {
        &self.protocol_info
    }

    pub(crate) fn engine_context(&self) -> &Arc<DeltaKernelEngineContext> {
        &self.engine_context
    }
}

pub(crate) fn load_delta_table_snapshot_blocking(
    table_uri: &str,
    storage_options: &DeltaStorageOptions,
    selection: DeltaSnapshotSelection,
) -> Result<LoadedDeltaTableSnapshot, DeltaReaderError> {
    let table_url = normalize_delta_table_uri(table_uri)?;
    let s3_auth_mode_hint = s3_auth_mode_hint_for_source(&table_url, storage_options);
    let engine_context = DeltaKernelEngineContext::build(table_url, storage_options)
        .boxed()
        .context(StorageInitializationSnafu {
            reason: "storage_initialization_failed",
        })?;
    let engine_context = Arc::new(engine_context);
    let version = match selection {
        DeltaSnapshotSelection::Latest => None,
        DeltaSnapshotSelection::Version(version) => Some(version),
    };
    let snapshot = engine_context
        .load_snapshot(version)
        .boxed()
        .context(SnapshotLoadSnafu {
            reason: snapshot_load_failed_reason(s3_auth_mode_hint),
        })?;
    let protocol_info = DeltaProtocolInfo::from_snapshot(&snapshot);

    Ok(LoadedDeltaTableSnapshot {
        snapshot,
        protocol_info,
        engine_context,
    })
}

#[allow(dead_code)]
/// Offloads the one blocking Kernel load to the caller's Tokio runtime.
///
/// Dropping the returned future cancels result delivery. A blocking load that
/// already started may still finish before its owned context is dropped.
pub(crate) async fn load_delta_table_snapshot_async(
    table_uri: String,
    storage_options: DeltaStorageOptions,
    selection: DeltaSnapshotSelection,
) -> Result<LoadedDeltaTableSnapshot, DeltaReaderError> {
    tokio::task::spawn_blocking(move || {
        load_delta_table_snapshot_blocking(&table_uri, &storage_options, selection)
    })
    .await
    .boxed()
    .context(SnapshotLoadSnafu {
        reason: "snapshot_load_task_failed",
    })?
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum S3AuthModeHint {
    ExplicitStatic,
    ExplicitWebIdentity,
    ExplicitContainer,
    ImplicitProviderChain,
    OtherExplicit,
}

fn s3_auth_mode_hint_for_source(
    table_url: &url::Url,
    storage_options: &DeltaStorageOptions,
) -> Option<S3AuthModeHint> {
    is_s3_compatible(table_url).then(|| classify_s3_auth_mode(storage_options))
}

fn is_s3_compatible(table_url: &url::Url) -> bool {
    match (table_url.scheme(), table_url.host_str()) {
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
    let mut has_container_credentials_relative_uri = false;
    let mut has_container_credentials_full_uri = false;
    let mut has_container_authorization_token_file = false;
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
            "aws_container_credentials_relative_uri" | "container_credentials_relative_uri" => {
                has_container_credentials_relative_uri = true;
                has_auth_related_option = true;
            }
            "aws_container_credentials_full_uri" | "container_credentials_full_uri" => {
                has_container_credentials_full_uri = true;
                has_auth_related_option = true;
            }
            "aws_container_authorization_token_file" | "container_authorization_token_file" => {
                has_container_authorization_token_file = true;
                has_auth_related_option = true;
            }
            "aws_imdsv1_fallback"
            | "imdsv1_fallback"
            | "aws_metadata_endpoint"
            | "metadata_endpoint"
            | "aws_unsigned_payload"
            | "unsigned_payload"
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
    } else if has_container_credentials_relative_uri
        || (has_container_credentials_full_uri && has_container_authorization_token_file)
    {
        S3AuthModeHint::ExplicitContainer
    } else if has_auth_related_option {
        S3AuthModeHint::OtherExplicit
    } else {
        S3AuthModeHint::ImplicitProviderChain
    }
}

fn snapshot_load_failed_reason(s3_auth_mode_hint: Option<S3AuthModeHint>) -> &'static str {
    if s3_auth_mode_hint == Some(S3AuthModeHint::ImplicitProviderChain) {
        "snapshot_load_failed_with_implicit_s3_credentials"
    } else {
        "snapshot_load_failed"
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as _,
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use futures_util::future;

    use super::{
        S3AuthModeHint, load_delta_table_snapshot_async, load_delta_table_snapshot_blocking,
        s3_auth_mode_hint_for_source, snapshot_load_failed_reason,
    };
    use crate::{
        DeltaReaderError, DeltaReaderPhase, DeltaSnapshotSelection, DeltaStorageOptions,
        kernel::is_kernel_error,
    };

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const METADATA_JSON: &str = r#"{"metaData":{"id":"delta-arrow-reader-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;

    struct DeltaLogTable(PathBuf);

    impl DeltaLogTable {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-arrow-reader-snapshot-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{PROTOCOL_JSON}\n{METADATA_JSON}\n"),
            )?;
            fs::write(
                log_path.join("00000000000000000001.json"),
                r#"{"commitInfo":{"timestamp":1587968586000}}"#,
            )?;
            Ok(Self(path))
        }
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(format!("{}-{name}-{nanos}", std::process::id()))
    }

    fn storage_options(entries: &[(&str, &str)]) -> DeltaStorageOptions {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn loads_latest_and_fixed_snapshots_into_shared_immutable_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("blocking")?;
        let table_uri = table.0.to_string_lossy();
        let latest = load_delta_table_snapshot_blocking(
            &table_uri,
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        let fixed = load_delta_table_snapshot_blocking(
            &table_uri,
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Version(0),
        )?;
        let cloned = latest.clone();

        assert_eq!(latest.version(), 1);
        assert_eq!(fixed.version(), 0);
        assert!(latest.table_uri().starts_with("file://"));
        assert!(Arc::ptr_eq(
            latest.engine_context(),
            cloned.engine_context()
        ));
        assert!(Arc::ptr_eq(
            &latest.engine_context().object_store(),
            &cloned.engine_context().object_store()
        ));
        Ok(())
    }

    #[test]
    fn corrupt_snapshot_preserves_the_kernel_source_without_dependency_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("corrupt")?;
        fs::write(
            table.0.join("_delta_log/00000000000000000001.json"),
            "{not json\n",
        )?;
        let result = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        );
        let error = match result {
            Ok(_) => panic!("corrupt snapshot should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, DeltaReaderError::SnapshotLoad { .. }));
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=snapshot error=snapshot_load reason=snapshot_load_failed"
        );
        assert!(is_kernel_error(error.source().expect("Kernel source")));
        Ok(())
    }

    #[test]
    fn snapshot_failures_are_redacted_and_preserve_the_kernel_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("missing-version")?;
        let result = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Version(2),
        );
        let error = match result {
            Ok(_) => panic!("missing version should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, DeltaReaderError::SnapshotLoad { .. }));
        assert_eq!(error.phase(), DeltaReaderPhase::Snapshot);
        assert_eq!(error.as_str(), "snapshot_load");
        assert!(!error.to_string().contains(&table.0.to_string_lossy()[..]));
        assert!(is_kernel_error(error.source().expect("Kernel source")));
        Ok(())
    }

    #[test]
    fn unsupported_store_preserves_its_source_without_uri_disclosure() {
        let result = load_delta_table_snapshot_blocking(
            "ftp://user:password@example.com/table",
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        );
        let error = match result {
            Ok(_) => panic!("unsupported store should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            DeltaReaderError::StorageInitialization { .. }
        ));
        assert_eq!(error.phase(), DeltaReaderPhase::Storage);
        assert!(!error.to_string().contains("user"));
        assert!(!error.to_string().contains("password"));
        assert!(!error.to_string().contains("example.com"));
        assert!(is_kernel_error(error.source().expect("Kernel source")));
    }

    #[test]
    fn s3_auth_mode_hint_preserves_the_existing_classification()
    -> Result<(), Box<dyn std::error::Error>> {
        let table_url = url::Url::parse("s3://bucket/table")?;
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
                s3_auth_mode_hint_for_source(&table_url, &options),
                Some(expected)
            );
        }

        assert_eq!(
            s3_auth_mode_hint_for_source(
                &url::Url::parse("https://example.com/table")?,
                &DeltaStorageOptions::new()
            ),
            None
        );
        assert_eq!(
            snapshot_load_failed_reason(Some(S3AuthModeHint::ImplicitProviderChain)),
            "snapshot_load_failed_with_implicit_s3_credentials"
        );
        assert_eq!(
            snapshot_load_failed_reason(Some(S3AuthModeHint::ExplicitStatic)),
            "snapshot_load_failed"
        );
        Ok(())
    }

    #[test]
    fn async_loads_are_independent_and_use_the_same_blocking_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("async")?;
        let table_uri = table.0.to_string_lossy().into_owned();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let (first, second) = runtime.block_on(future::join(
            load_delta_table_snapshot_async(
                table_uri.clone(),
                DeltaStorageOptions::new(),
                DeltaSnapshotSelection::Latest,
            ),
            load_delta_table_snapshot_async(
                table_uri,
                DeltaStorageOptions::new(),
                DeltaSnapshotSelection::Version(0),
            ),
        ));
        let first = first?;
        let second = second?;

        assert_eq!(first.version(), 1);
        assert_eq!(second.version(), 0);
        assert!(!Arc::ptr_eq(
            first.engine_context(),
            second.engine_context()
        ));
        Ok(())
    }
}
