//! Delta table URI normalization.

use crate::{DeltaReaderError, error::InvalidTableUriSnafu, kernel::parse_uri};

/// Normalizes a Delta table URI for snapshot loading.
pub(crate) fn normalize_delta_table_uri(table_uri: &str) -> Result<url::Url, DeltaReaderError> {
    if table_uri.trim().is_empty() {
        return InvalidTableUriSnafu {
            reason: "empty_table_uri",
        }
        .fail();
    }

    parse_uri(table_uri).map_err(|_| {
        InvalidTableUriSnafu {
            reason: "invalid_table_uri",
        }
        .build()
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::normalize_delta_table_uri;
    use crate::DeltaReaderPhase;

    struct TestDir(PathBuf);

    impl TestDir {
        fn absolute(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = std::env::temp_dir().join(unique_name(name)?);
            fs::create_dir_all(&path)?;
            Ok(Self(path))
        }

        fn relative(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-arrow-reader-uri-tests")
                .join(unique_name(name)?);
            fs::create_dir_all(&path)?;
            Ok(Self(path))
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(format!("{}-{name}-{nanos}", std::process::id()))
    }

    #[test]
    fn normalizes_absolute_and_relative_local_paths() -> Result<(), Box<dyn std::error::Error>> {
        let absolute = TestDir::absolute("absolute")?;
        let relative = TestDir::relative("relative")?;

        let absolute_uri = normalize_delta_table_uri(&absolute.0.to_string_lossy())?;
        let relative_uri = normalize_delta_table_uri(&relative.0.to_string_lossy())?;
        let relative_path = relative_uri
            .to_file_path()
            .map_err(|()| std::io::Error::other("expected a local file URI"))?;

        assert!(absolute_uri.as_str().starts_with("file://"));
        assert!(absolute_uri.as_str().ends_with('/'));
        assert_eq!(relative_path, fs::canonicalize(&relative.0)?);
        assert_eq!(
            normalize_delta_table_uri(absolute_uri.as_str())?,
            absolute_uri
        );
        Ok(())
    }

    #[test]
    fn preserves_remote_url_semantics_without_opening_a_store()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            normalize_delta_table_uri("s3://bucket/path/to/table")?.as_str(),
            "s3://bucket/path/to/table/"
        );
        Ok(())
    }

    #[test]
    fn rejects_empty_missing_and_hostile_locations_without_disclosure()
    -> Result<(), Box<dyn std::error::Error>> {
        let missing = std::env::temp_dir()
            .join("sensitive-missing-table")
            .join(unique_name("missing")?);
        let parent = TestDir::absolute("regular-file")?;
        let regular_file = parent.0.join("not-a-directory");
        fs::write(&regular_file, "not a table")?;

        for (table_uri, expected_reason) in [
            ("", "empty_table_uri"),
            (" \t\n", "empty_table_uri"),
            (&missing.to_string_lossy(), "invalid_table_uri"),
            (&regular_file.to_string_lossy(), "invalid_table_uri"),
            ("s3://secret-user:secret-password@[", "invalid_table_uri"),
        ] {
            let error = normalize_delta_table_uri(table_uri).expect_err("URI should be rejected");
            assert_eq!(error.as_str(), "invalid_table_uri");
            assert_eq!(error.phase(), DeltaReaderPhase::TableUri);
            assert!(error.to_string().contains(expected_reason));
            assert!(!error.to_string().contains("secret"));
            assert!(!format!("{error:?}").contains("secret"));
        }

        Ok(())
    }
}
