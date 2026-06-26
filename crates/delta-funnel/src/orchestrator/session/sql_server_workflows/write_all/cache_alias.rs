use std::{fmt, sync::Arc};

use datafusion::{datasource::TableProvider, prelude::SessionContext};

use crate::{DeltaFunnelError, support::sanitize_text_for_display};

use super::super::super::{
    DeltaFunnelSession, LazyTable,
    errors::{mssql_scoped_cache_alias_error, unknown_cached_alias_error},
};
use super::MssqlDerivedCacheAliasPlan;

/// Active replacement of one registered derived alias with a cached provider.
///
/// The original provider is owned by this scope until `restore` is awaited.
/// Callers must not rely on `Drop` for restoration.
#[allow(dead_code)]
pub(crate) struct MssqlScopedCacheAliasReplacement<'a> {
    context: &'a SessionContext,
    table_id: u64,
    alias_name: String,
    original_provider: Option<Arc<dyn TableProvider>>,
}

#[allow(dead_code)]
impl<'a> MssqlScopedCacheAliasReplacement<'a> {
    pub(super) fn new(
        context: &'a SessionContext,
        table_id: u64,
        alias_name: String,
        original_provider: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            context,
            table_id,
            alias_name,
            original_provider: Some(original_provider),
        }
    }

    #[cfg(test)]
    pub(super) fn broken_for_test(
        context: &'a SessionContext,
        table_id: u64,
        alias_name: String,
    ) -> Self {
        Self {
            context,
            table_id,
            alias_name,
            original_provider: None,
        }
    }

    /// Returns the session table id for the replaced alias.
    #[must_use]
    pub(crate) const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the registered alias name currently backed by the cached provider.
    #[must_use]
    pub(crate) fn alias_name(&self) -> &str {
        &self.alias_name
    }

    /// Restores the original provider under the alias and consumes the scope.
    ///
    /// This method transitions the catalog from "alias points at cached
    /// provider" back to "alias points at the original provider". It is async
    /// by design so later cache cleanup can remain awaitable even if DataFusion
    /// changes or additional async cleanup is needed.
    ///
    /// Callers should await this method on both success and error paths that
    /// leave the scoped replacement active.
    pub(crate) async fn restore(
        mut self,
    ) -> Result<MssqlScopedCacheAliasRestoration, DeltaFunnelError> {
        let Some(original_provider) = self.original_provider.take() else {
            return Err(mssql_scoped_cache_alias_error(
                "restore",
                &self.alias_name,
                "original provider was already restored",
            ));
        };

        let removed_cached = self
            .context
            .deregister_table(self.alias_name.as_str())
            .map_err(|error| {
                mssql_scoped_cache_alias_error("restore_deregister", &self.alias_name, error)
            })?;

        self.context
            .register_table(self.alias_name.as_str(), original_provider)
            .map_err(|error| {
                mssql_scoped_cache_alias_error("restore_register", &self.alias_name, error)
            })?;

        Ok(MssqlScopedCacheAliasRestoration {
            table_id: self.table_id,
            alias_name: self.alias_name,
            cached_alias_was_present: removed_cached.is_some(),
        })
    }
}

/// Report returned after a scoped cache alias replacement restores the original alias.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct MssqlScopedCacheAliasRestoration {
    table_id: u64,
    alias_name: String,
    cached_alias_was_present: bool,
}

#[allow(dead_code)]
impl MssqlScopedCacheAliasRestoration {
    /// Returns the restored session table id.
    #[must_use]
    pub(crate) const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the restored alias name.
    #[must_use]
    pub(crate) fn alias_name(&self) -> &str {
        &self.alias_name
    }

    /// Returns whether a cached alias was present when restoration started.
    #[must_use]
    pub(crate) const fn cached_alias_was_present(&self) -> bool {
        self.cached_alias_was_present
    }
}

impl DeltaFunnelSession {
    /// Materializes one registered derived alias and temporarily replaces that alias.
    ///
    /// The method leaves the original catalog alias active while DataFusion
    /// builds the cache, then swaps the catalog entry to the cached provider.
    /// The returned scope owns the original provider and must be restored with
    /// `MssqlScopedCacheAliasReplacement::restore`.
    ///
    /// This is intentionally a one-alias primitive. It does not choose cache
    /// candidates, replan downstream SQL, or execute any outputs.
    #[allow(dead_code)]
    pub(crate) async fn replace_registered_derived_alias_with_cache(
        &self,
        table: &LazyTable,
    ) -> Result<MssqlScopedCacheAliasReplacement<'_>, DeltaFunnelError> {
        let registered = self.registered_derived_for_scoped_cache_alias(table)?;
        let table_id = registered.table().id();
        let alias_name = registered.name().to_owned();

        let cached_provider = self
            .context
            .table(alias_name.as_str())
            .await
            .map_err(|error| mssql_scoped_cache_alias_error("resolve", alias_name.as_str(), error))?
            .cache()
            .await
            .map_err(|error| {
                mssql_scoped_cache_alias_error("materialize", alias_name.as_str(), error)
            })?
            .into_view();

        let original_provider =
            self.install_scoped_cache_alias_provider(alias_name.as_str(), cached_provider)?;

        Ok(MssqlScopedCacheAliasReplacement::new(
            &self.context,
            table_id,
            alias_name,
            original_provider,
        ))
    }

    pub(super) async fn replace_mssql_cache_aliases(
        &self,
        cache_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<Vec<MssqlScopedCacheAliasReplacement<'_>>, DeltaFunnelError> {
        let mut replacements = Vec::new();

        for cache_alias in cache_aliases {
            let table = self
                .registered_derived_table_by_id(cache_alias.table_id())
                .ok_or_else(|| unknown_cached_alias_error(cache_alias))?
                .table()
                .clone();

            match self
                .replace_registered_derived_alias_with_cache(&table)
                .await
            {
                Ok(replacement) => replacements.push(replacement),
                Err(error) => {
                    return Err(restore_mssql_cache_aliases_after_error(error, replacements).await);
                }
            }
        }

        Ok(replacements)
    }

    /// Swaps a catalog alias from its original provider to a cached provider.
    ///
    /// On success, the alias points at `cached_provider` and the original
    /// provider is returned to the caller for later restoration. If registering
    /// the cached provider fails after the original provider has been removed,
    /// this helper attempts to put the original provider back before returning
    /// the error.
    fn install_scoped_cache_alias_provider(
        &self,
        alias_name: &str,
        cached_provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, DeltaFunnelError> {
        let original_provider = self
            .context
            .deregister_table(alias_name)
            .map_err(|error| mssql_scoped_cache_alias_error("deregister", alias_name, error))?
            .ok_or_else(|| {
                mssql_scoped_cache_alias_error(
                    "deregister",
                    alias_name,
                    "registered alias was missing from the catalog",
                )
            })?;

        if let Err(register_error) = self.context.register_table(alias_name, cached_provider) {
            return Err(self.restore_original_after_cached_register_failure(
                alias_name,
                original_provider,
                register_error,
            ));
        }

        Ok(original_provider)
    }

    /// Restores the original provider after cached-provider registration fails.
    ///
    /// This helper is used only for the narrow failure window where the
    /// original provider has already been deregistered but the cached provider
    /// could not be registered. The returned error reports the cached register
    /// failure and, if restoration also fails, includes that cleanup failure.
    pub(super) fn restore_original_after_cached_register_failure(
        &self,
        alias_name: &str,
        original_provider: Arc<dyn TableProvider>,
        register_error: impl fmt::Display,
    ) -> DeltaFunnelError {
        let restore_result = self.context.register_table(alias_name, original_provider);
        let message = match restore_result {
            Ok(_) => format!(
                "failed to register cached provider for alias `{}`: {}",
                sanitize_text_for_display(alias_name),
                sanitize_text_for_display(&register_error.to_string())
            ),
            Err(restore_error) => format!(
                "failed to register cached provider for alias `{}`: {}; also failed to restore original provider: {}",
                sanitize_text_for_display(alias_name),
                sanitize_text_for_display(&register_error.to_string()),
                sanitize_text_for_display(&restore_error.to_string())
            ),
        };

        DeltaFunnelError::MssqlWorkflowPlanning { message }
    }
}

pub(super) async fn restore_mssql_cache_aliases(
    replacements: Vec<MssqlScopedCacheAliasReplacement<'_>>,
) -> Result<Vec<MssqlScopedCacheAliasRestoration>, DeltaFunnelError> {
    let mut restorations = Vec::new();
    let mut first_error = None;

    for replacement in replacements.into_iter().rev() {
        match replacement.restore().await {
            Ok(restoration) => restorations.push(restoration),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(restorations),
    }
}

pub(super) async fn restore_mssql_cache_aliases_after_error(
    error: DeltaFunnelError,
    replacements: Vec<MssqlScopedCacheAliasReplacement<'_>>,
) -> DeltaFunnelError {
    match restore_mssql_cache_aliases(replacements).await {
        Ok(_restorations) => error,
        Err(restore_error) => cache_error_with_restore_error(error, restore_error),
    }
}

pub(super) fn cache_error_with_restore_error(
    error: DeltaFunnelError,
    restore_error: DeltaFunnelError,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWorkflowPlanning {
        message: format!(
            "write_all auto cache failed: {}; also failed to restore cache aliases: {}",
            sanitize_text_for_display(&error.to_string()),
            sanitize_text_for_display(&restore_error.to_string())
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    use crate::{
        DeltaFunnelError,
        orchestrator::session::{
            DeltaFunnelSession, SessionOptions,
            test_support::{marker_values_from_batches, scan_counting_marker_region_provider},
        },
    };

    #[tokio::test]
    async fn scoped_cache_alias_replacement_materializes_cache_and_restores_original_provider()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);

        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;

        assert_eq!(replacement.table_id(), big.id());
        assert_eq!(replacement.alias_name(), "big");
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let direct_cached_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_cached_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let restoration = replacement.restore().await?;

        assert_eq!(restoration.table_id(), big.id());
        assert_eq!(restoration.alias_name(), "big");
        assert!(restoration.cached_alias_was_present());

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_replacement_restores_original_after_cached_register_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;
        let original_provider = session
            .context()
            .deregister_table("big")?
            .ok_or("expected original provider")?;

        let error = session.restore_original_after_cached_register_failure(
            "big",
            original_provider,
            "injected cached register failure",
        );

        assert!(matches!(
            &error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("failed to register cached provider")
                    && message.contains("injected cached register failure")
        ));

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_replacement_explicit_restore_cleans_up_after_later_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let later_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated downstream planning failure".to_owned(),
        };
        let restoration = replacement.restore().await?;

        assert!(matches!(
            later_error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("simulated downstream planning failure")
        ));
        assert_eq!(restoration.alias_name(), "big");

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_restore_reinstalls_original_when_cached_alias_is_missing()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let removed_cached = session.context().deregister_table("big")?;
        assert!(removed_cached.is_some());

        let restoration = replacement.restore().await?;

        assert_eq!(restoration.alias_name(), "big");
        assert!(!restoration.cached_alias_was_present());

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test]
    fn cache_error_with_restore_error_preserves_both_contexts() {
        let primary_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated output workflow failure".to_owned(),
        };
        let restore_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated restore failure for alias big".to_owned(),
        };

        let error = cache_error_with_restore_error(primary_error, restore_error);

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("write_all auto cache failed")
                    && message.contains("simulated output workflow failure")
                    && message.contains("also failed to restore cache aliases")
                    && message.contains("simulated restore failure for alias big")
        ));
    }

    #[tokio::test]
    async fn restore_mssql_cache_aliases_after_error_preserves_broken_restore_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let broken_replacement = MssqlScopedCacheAliasReplacement::broken_for_test(
            session.context(),
            42,
            "big".to_owned(),
        );
        let primary_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated cached workflow failure".to_owned(),
        };

        let error =
            restore_mssql_cache_aliases_after_error(primary_error, vec![broken_replacement]).await;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("write_all auto cache failed")
                    && message.contains("simulated cached workflow failure")
                    && message.contains("also failed to restore cache aliases")
                    && message.contains("scoped MSSQL cache alias restore failed")
                    && message.contains("big")
                    && message.contains("original provider was already restored")
        ));
        Ok(())
    }
}
