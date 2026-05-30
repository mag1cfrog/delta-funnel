//! Delta source table URI normalization.

use crate::DeltaFunnelError;
use crate::delta_kernel_adapter::try_parse_uri;

const INVALID_TABLE_URI: &str = "table location could not be parsed or normalized";

/// Normalizes a Delta table URI for later snapshot loading.
///
/// This uses the official `delta_kernel` URI handling path so bare local paths
/// are canonicalized to `file://` URLs and remote object-store URLs keep the
/// same URL semantics that snapshot loading will use. Relative local paths are
/// resolved against the process current directory by the `delta_kernel`
/// canonicalization path.
///
/// # Errors
///
/// Returns [`DeltaFunnelError::InvalidSourceUri`] when the table location
/// cannot be parsed or normalized by `delta_kernel`.
pub(crate) fn normalize_delta_table_uri(
    table_uri: impl AsRef<str>,
) -> Result<String, DeltaFunnelError> {
    let table_url = try_parse_uri(table_uri).map_err(|_| DeltaFunnelError::InvalidSourceUri {
        reason: INVALID_TABLE_URI,
    })?;

    Ok(table_url.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::normalize_delta_table_uri;
    use crate::DeltaFunnelError;

    struct TestDir {
        path: PathBuf,
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl TestDir {
        fn absolute(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let mut path = std::env::temp_dir();
            path.push(unique_name(name)?);
            fs::create_dir_all(&path)?;

            Ok(Self { path })
        }

        fn relative(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-funnel-uri-tests")
                .join(unique_name(name)?);
            fs::create_dir_all(&path)?;

            Ok(Self { path })
        }
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

        Ok(format!("{}-{}-{nanos}", std::process::id(), name))
    }

    #[test]
    fn normalizes_absolute_local_paths_to_file_urls() -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::absolute("absolute")?;
        let normalized = normalize_delta_table_uri(dir.path.to_string_lossy())?;

        assert!(normalized.starts_with("file://"));
        assert!(normalized.ends_with('/'));

        Ok(())
    }

    #[test]
    fn normalizes_relative_local_paths_to_file_urls() -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::relative("relative")?;
        let normalized = normalize_delta_table_uri(dir.path.to_string_lossy())?;
        let normalized_url = crate::delta_kernel_adapter::try_parse_uri(&normalized)?;
        let normalized_path = normalized_url.to_file_path().map_err(|()| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "normalized file URL could not be converted to a local path",
            )
        })?;
        let expected_path = fs::canonicalize(&dir.path)?;
        let current_dir = fs::canonicalize(std::env::current_dir()?)?;

        assert!(normalized.starts_with("file://"));
        assert!(normalized.ends_with('/'));
        assert_eq!(normalized_path, expected_path);
        assert!(expected_path.starts_with(current_dir));

        Ok(())
    }

    #[test]
    fn normalizes_file_urls_idempotently() -> Result<(), Box<dyn std::error::Error>> {
        let dir = TestDir::absolute("file-url")?;
        let normalized = normalize_delta_table_uri(dir.path.to_string_lossy())?;

        assert_eq!(normalize_delta_table_uri(&normalized)?, normalized);

        Ok(())
    }

    #[test]
    fn normalizes_object_store_urls_without_constructing_a_store()
    -> Result<(), Box<dyn std::error::Error>> {
        let normalized = normalize_delta_table_uri("s3://bucket/path/to/table")?;

        assert_eq!(normalized, "s3://bucket/path/to/table/");

        Ok(())
    }

    #[test]
    fn rejects_missing_local_paths() -> Result<(), Box<dyn std::error::Error>> {
        let mut missing = std::env::temp_dir();
        missing.push("delta-funnel-missing-table-path");
        missing.push(unique_name("missing")?);

        let result = normalize_delta_table_uri(missing.to_string_lossy());

        assert!(matches!(
            result,
            Err(DeltaFunnelError::InvalidSourceUri { .. })
        ));

        Ok(())
    }
}
