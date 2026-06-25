use std::fmt;

use crate::{
    BatchPipelinePhase, DeltaFunnelError, SqlTablePhase, support::sanitize_text_for_display,
};

use super::{LazyTable, mssql::MssqlDerivedCacheAliasPlan};

pub(super) fn sql_table_error<T>(
    phase: SqlTablePhase,
    message: impl Into<String>,
) -> Result<T, DeltaFunnelError> {
    Err(DeltaFunnelError::SqlTable {
        phase,
        message: message.into(),
    })
}

pub(super) fn unknown_lazy_table_error(table: &LazyTable) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWorkflowPlanning {
        message: format!(
            "lazy table `{}` is not registered in this session",
            sanitize_text_for_display(table.name())
        ),
    }
}

pub(super) fn unknown_cached_alias_error(alias: &MssqlDerivedCacheAliasPlan) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWorkflowPlanning {
        message: format!(
            "cached alias `{}` is not registered in this session",
            sanitize_text_for_display(alias.alias())
        ),
    }
}

pub(super) fn cached_output_stream_setup_error(
    output_name: &str,
    message: impl fmt::Display,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWorkflowPlanning {
        message: format!(
            "cached output stream setup failed for `{}`: {}",
            sanitize_text_for_display(output_name),
            sanitize_text_for_display(&message.to_string())
        ),
    }
}

pub(super) fn mssql_scoped_cache_alias_error(
    phase: &'static str,
    alias_name: &str,
    error: impl fmt::Display,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWorkflowPlanning {
        message: format!(
            "scoped MSSQL cache alias {phase} failed for `{}`: {}",
            sanitize_text_for_display(alias_name),
            sanitize_text_for_display(&error.to_string())
        ),
    }
}

pub(super) fn datafusion_handoff_setup_error(
    option: &'static str,
    error: impl fmt::Display,
) -> DeltaFunnelError {
    DeltaFunnelError::BatchPipeline {
        phase: BatchPipelinePhase::HandoffSetup,
        option,
        message: error.to_string(),
    }
}
