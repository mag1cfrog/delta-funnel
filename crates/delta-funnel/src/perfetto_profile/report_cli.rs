use std::env;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::iter;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;

use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Args, Parser, Subcommand};

use super::report_terminal::{InspectSelection, InspectSort, TerminalInspectError};

const DEFAULT_INSPECT_LIMIT: u16 = 20;
const MAX_INSPECT_LIMIT: u16 = 200;
const MAX_INSPECT_DEPTH: u16 = 32;
const MAX_FILTER_CHARS: usize = 128;

#[derive(Debug, Parser, PartialEq, Eq)]
#[command(
    name = "delta-funnel-perfetto",
    about = "Generate and inspect Delta Funnel Perfetto diagnostics",
    disable_version_flag = true
)]
struct PerfettoCli {
    #[command(subcommand)]
    command: PerfettoCommand,
}

#[derive(Debug, PartialEq, Eq, Subcommand)]
enum PerfettoCommand {
    /// Generate a ranked HTML report from a raw trace.
    Report(RankedReportArgs),

    /// Inspect ranked profiling data in the terminal.
    Inspect(InspectArgs),
}

#[derive(Args, Debug, PartialEq, Eq)]
struct RankedReportArgs {
    /// Raw Perfetto trace to analyze.
    #[arg(value_name = "INPUT.pftrace")]
    input: PathBuf,

    /// Report destination. Defaults to INPUT.profile.html.
    #[arg(long, value_name = "OUTPUT.profile.html", allow_hyphen_values = true)]
    output: Option<PathBuf>,
}

#[derive(Args, Debug, PartialEq, Eq)]
struct InspectArgs {
    /// Raw Perfetto trace to inspect.
    #[arg(value_name = "INPUT.pftrace")]
    input: PathBuf,

    /// Maximum number of rows to display.
    #[arg(
        long,
        default_value_t = DEFAULT_INSPECT_LIMIT,
        value_parser = clap::value_parser!(u16).range(1..=i64::from(MAX_INSPECT_LIMIT))
    )]
    limit: u16,

    /// Select one semantic node by its exact numeric identity.
    #[arg(long, value_name = "ID")]
    semantic: Option<i64>,

    /// Select one function callsite by semantic and function identity.
    #[arg(
        long,
        value_name = "SEMANTIC_ID:FUNCTION_ID",
        conflicts_with = "semantic",
        allow_hyphen_values = true
    )]
    function: Option<FunctionSelector>,

    /// Retain matching rows and their contextual ancestors.
    #[arg(long, value_name = "TEXT", allow_hyphen_values = true)]
    filter: Option<FilterText>,

    /// Sort sibling rows by the selected metric.
    #[arg(long, value_enum)]
    sort: Option<InspectSort>,

    /// Maximum descendant depth from the active context.
    #[arg(
        long,
        value_parser = clap::value_parser!(u16).range(0..=i64::from(MAX_INSPECT_DEPTH))
    )]
    depth: Option<u16>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FunctionSelector {
    semantic_id: i64,
    function_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FilterText(String);

impl FromStr for FilterText {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() || value.chars().count() > MAX_FILTER_CHARS {
            return Err("filter must contain between 1 and 128 characters");
        }
        Ok(Self(value.to_owned()))
    }
}

impl FromStr for FunctionSelector {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (semantic_id, function_id) = value
            .split_once(':')
            .ok_or("function identity must contain one colon")?;
        Ok(Self {
            semantic_id: semantic_id
                .parse()
                .map_err(|_| "semantic function owner must be a signed integer")?,
            function_id: function_id
                .parse()
                .map_err(|_| "function ID must be a signed integer")?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CliArgumentError {
    MissingCommand,
    UnknownCommand,
    MissingInput,
    MissingOutputValue,
    DuplicateOutput,
    MultipleInputs,
    InvalidLimit,
    InvalidDepth,
    InvalidSemanticId,
    InvalidFunctionId,
    InvalidFilter,
    InvalidSort,
    IncompatibleSelectors,
    IncompatibleSort,
    DuplicateOption,
    UnknownOption,
}

impl fmt::Display for CliArgumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::MissingCommand => "a diagnostics command is required",
            Self::UnknownCommand => "unknown diagnostics command",
            Self::MissingInput => "an input trace path is required",
            Self::MissingOutputValue => "--output requires a path",
            Self::DuplicateOutput => "--output may be specified only once",
            Self::MultipleInputs => "only one input trace path may be provided",
            Self::InvalidLimit => "limit must be between 1 and 200",
            Self::InvalidDepth => "depth must be between 0 and 32",
            Self::InvalidSemanticId => "semantic ID must be a signed integer",
            Self::InvalidFunctionId => {
                "function ID must use SEMANTIC_ID:FUNCTION_ID signed integers"
            }
            Self::InvalidFilter => "filter must contain between 1 and 128 characters",
            Self::InvalidSort => "sort must be duration, inclusive-cpu, self-cpu, or name",
            Self::IncompatibleSelectors => "--semantic and --function cannot be used together",
            Self::IncompatibleSort => "function callsites cannot be sorted by exact duration",
            Self::DuplicateOption => "option may be specified only once",
            Self::UnknownOption => "unknown option",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for CliArgumentError {}

impl CliArgumentError {
    const fn kind(self) -> &'static str {
        match self {
            Self::MissingCommand => "missing_command",
            Self::UnknownCommand => "unknown_command",
            Self::MissingInput => "missing_input",
            Self::MissingOutputValue => "missing_output_value",
            Self::DuplicateOutput => "duplicate_output",
            Self::MultipleInputs => "multiple_inputs",
            Self::InvalidLimit => "invalid_limit",
            Self::InvalidDepth => "invalid_depth",
            Self::InvalidSemanticId => "invalid_semantic_id",
            Self::InvalidFunctionId => "invalid_function_id",
            Self::InvalidFilter => "invalid_filter",
            Self::InvalidSort => "invalid_sort",
            Self::IncompatibleSelectors => "incompatible_selectors",
            Self::IncompatibleSort => "incompatible_sort",
            Self::DuplicateOption => "duplicate_option",
            Self::UnknownOption => "unknown_option",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RankedReportFailurePhase {
    Argument,
    Input,
    Health,
    TraceProcessor,
    Query,
    AggregateValidation,
    Serialization,
    Output,
}

impl RankedReportFailurePhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Argument => "argument",
            Self::Input => "input",
            Self::Health => "health",
            Self::TraceProcessor => "trace_processor",
            Self::Query => "query",
            Self::AggregateValidation => "aggregate_validation",
            Self::Serialization => "serialization",
            Self::Output => "output",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RankedReportFailure {
    phase: RankedReportFailurePhase,
    kind: &'static str,
    message: String,
}

impl RankedReportFailure {
    pub(super) fn new(
        phase: RankedReportFailurePhase,
        kind: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            phase,
            kind,
            message: message.into(),
        }
    }

    pub fn phase(&self) -> RankedReportFailurePhase {
        self.phase
    }

    #[cfg(test)]
    pub fn kind(&self) -> &'static str {
        self.kind
    }

    pub fn machine_line(&self) -> String {
        serde_json::json!({
            "phase": self.phase.as_str(),
            "kind": self.kind,
            "message": self.message,
        })
        .to_string()
    }
}

impl fmt::Display for RankedReportFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RankedReportFailure {}

impl From<CliArgumentError> for RankedReportFailure {
    fn from(error: CliArgumentError) -> Self {
        Self::new(
            RankedReportFailurePhase::Argument,
            error.kind(),
            error.to_string(),
        )
    }
}

impl From<TerminalInspectError> for RankedReportFailure {
    fn from(error: TerminalInspectError) -> Self {
        Self::new(
            RankedReportFailurePhase::Argument,
            error.kind(),
            error.to_string(),
        )
    }
}

#[derive(Debug)]
pub(super) enum RankedReportPathError {
    InputUnreadable(io::Error),
    InputNotFile,
    OutputHasNoFileName,
    OutputNotFile,
    OutputParentNotDirectory,
    OutputInspection(io::Error),
    InputOutputAlias,
}

impl fmt::Display for RankedReportPathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InputUnreadable(error) => {
                write!(formatter, "input trace is not readable: {error}")
            }
            Self::InputNotFile => formatter.write_str("input trace is not a file"),
            Self::OutputHasNoFileName => formatter.write_str("output path has no file name"),
            Self::OutputNotFile => formatter.write_str("existing output is not a file"),
            Self::OutputParentNotDirectory => {
                formatter.write_str("output parent path is not a directory")
            }
            Self::OutputInspection(error) => {
                write!(formatter, "output path could not be inspected: {error}")
            }
            Self::InputOutputAlias => {
                formatter.write_str("input trace and output resolve to the same file")
            }
        }
    }
}

impl std::error::Error for RankedReportPathError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InputUnreadable(error) | Self::OutputInspection(error) => Some(error),
            _ => None,
        }
    }
}

impl From<RankedReportPathError> for RankedReportFailure {
    fn from(error: RankedReportPathError) -> Self {
        let (phase, kind) = match &error {
            RankedReportPathError::InputUnreadable(_) => {
                (RankedReportFailurePhase::Input, "unreadable")
            }
            RankedReportPathError::InputNotFile => (RankedReportFailurePhase::Input, "not_file"),
            RankedReportPathError::OutputHasNoFileName => {
                (RankedReportFailurePhase::Output, "missing_file_name")
            }
            RankedReportPathError::OutputNotFile => (RankedReportFailurePhase::Output, "not_file"),
            RankedReportPathError::OutputParentNotDirectory => {
                (RankedReportFailurePhase::Output, "parent_not_directory")
            }
            RankedReportPathError::OutputInspection(_) => {
                (RankedReportFailurePhase::Output, "inspection_failed")
            }
            RankedReportPathError::InputOutputAlias => {
                (RankedReportFailurePhase::Output, "aliases_input")
            }
        };
        Self::new(phase, kind, error.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RankedReportPaths {
    pub(super) input: PathBuf,
    pub(super) output: PathBuf,
}

/// Runs the bundled Perfetto diagnostics command-line interface.
///
/// Returns the process exit code without terminating the caller.
pub fn run_perfetto_diagnostics_cli() -> i32 {
    run_perfetto_diagnostics_cli_with(env::args_os().skip(1))
}

fn run_perfetto_diagnostics_cli_with(args: impl IntoIterator<Item = OsString>) -> i32 {
    let args = args.into_iter().collect::<Vec<_>>();
    match PerfettoCli::try_parse_from(
        iter::once(OsString::from("delta-funnel-perfetto")).chain(args.iter().cloned()),
    ) {
        Ok(PerfettoCli {
            command: PerfettoCommand::Report(args),
        }) => run_report_command(args),
        Ok(PerfettoCli {
            command: PerfettoCommand::Inspect(args),
        }) => run_inspect_command(args),
        Err(error) if matches!(error.kind(), ErrorKind::DisplayHelp) => {
            if error.print().is_ok() {
                0
            } else {
                70
            }
        }
        Err(error) => emit_failure(cli_failure(&args, &error)),
    }
}

fn run_inspect_command(args: InspectArgs) -> i32 {
    let selection = if let Some(function) = args.function {
        InspectSelection::Function {
            semantic_id: function.semantic_id,
            function_id: function.function_id,
        }
    } else if let Some(semantic_id) = args.semantic {
        InspectSelection::Semantic(semantic_id)
    } else {
        InspectSelection::Root
    };
    let sort = args.sort.unwrap_or(match selection {
        InspectSelection::Function { .. } => InspectSort::InclusiveCpu,
        InspectSelection::Root | InspectSelection::Semantic(_) => InspectSort::Duration,
    });
    if matches!(selection, InspectSelection::Function { .. }) && sort == InspectSort::Duration {
        return emit_failure(CliArgumentError::IncompatibleSort.into());
    }
    let input = match preflight_ranked_profile_input(&args.input) {
        Ok(input) => input,
        Err(error) => return emit_failure(error.into()),
    };
    match super::load_ranked_profile(&input) {
        Ok(document) => match super::render_terminal_view(
            &document,
            selection,
            sort,
            args.filter.as_ref().map(|filter| filter.0.as_str()),
            usize::from(args.limit),
            args.depth.map_or_else(
                || usize::from(selection != InspectSelection::Root),
                usize::from,
            ),
        ) {
            Ok(output) => {
                print!("{output}");
                0
            }
            Err(error) => emit_failure(error.into()),
        },
        Err(error) => emit_failure(error),
    }
}

fn run_report_command(args: RankedReportArgs) -> i32 {
    let output = args
        .output
        .unwrap_or_else(|| default_report_path(&args.input));
    match super::generate_ranked_profile_report(&args.input, &output) {
        Ok(output) => {
            println!("wrote {}", output.display());
            0
        }
        Err(error) => emit_failure(error),
    }
}

fn classify_cli_error(args: &[OsString], error: &clap::Error) -> CliArgumentError {
    let Some(command) = args.first() else {
        return CliArgumentError::MissingCommand;
    };
    if command != "report" && command != "inspect" {
        return CliArgumentError::UnknownCommand;
    }
    if command == "report"
        && matches!(
            args.last().and_then(|argument| argument.to_str()),
            Some("--output")
        )
    {
        return CliArgumentError::MissingOutputValue;
    }
    match error.kind() {
        ErrorKind::MissingRequiredArgument => CliArgumentError::MissingInput,
        ErrorKind::ArgumentConflict if command == "report" => CliArgumentError::DuplicateOutput,
        ErrorKind::ArgumentConflict if is_selector_conflict(error) => {
            CliArgumentError::IncompatibleSelectors
        }
        ErrorKind::ArgumentConflict => CliArgumentError::DuplicateOption,
        ErrorKind::TooManyValues => CliArgumentError::MultipleInputs,
        ErrorKind::ValueValidation
            if matches!(
                error.get(ContextKind::InvalidArg),
                Some(ContextValue::String(argument)) if argument.starts_with("--limit")
            ) =>
        {
            CliArgumentError::InvalidLimit
        }
        ErrorKind::ValueValidation
            if matches!(
                error.get(ContextKind::InvalidArg),
                Some(ContextValue::String(argument)) if argument.starts_with("--depth")
            ) =>
        {
            CliArgumentError::InvalidDepth
        }
        ErrorKind::ValueValidation
            if matches!(
                error.get(ContextKind::InvalidArg),
                Some(ContextValue::String(argument)) if argument.starts_with("--semantic")
            ) =>
        {
            CliArgumentError::InvalidSemanticId
        }
        ErrorKind::ValueValidation
            if matches!(
                error.get(ContextKind::InvalidArg),
                Some(ContextValue::String(argument)) if argument.starts_with("--function")
            ) =>
        {
            CliArgumentError::InvalidFunctionId
        }
        ErrorKind::ValueValidation
            if matches!(
                error.get(ContextKind::InvalidArg),
                Some(ContextValue::String(argument)) if argument.starts_with("--filter")
            ) =>
        {
            CliArgumentError::InvalidFilter
        }
        ErrorKind::InvalidValue
            if matches!(
                error.get(ContextKind::InvalidArg),
                Some(ContextValue::String(argument)) if argument.starts_with("--sort")
            ) =>
        {
            CliArgumentError::InvalidSort
        }
        ErrorKind::UnknownArgument => match error.get(ContextKind::InvalidArg) {
            Some(ContextValue::String(argument)) if !argument.starts_with('-') => {
                CliArgumentError::MultipleInputs
            }
            _ => CliArgumentError::UnknownOption,
        },
        _ => CliArgumentError::UnknownOption,
    }
}

fn is_selector_conflict(error: &clap::Error) -> bool {
    let arguments = [
        error.get(ContextKind::InvalidArg),
        error.get(ContextKind::PriorArg),
    ];
    let contains_semantic = arguments.into_iter().flatten().any(|value| {
        matches!(value, ContextValue::String(argument) if argument.starts_with("--semantic"))
    });
    let contains_function = arguments.into_iter().flatten().any(|value| {
        matches!(value, ContextValue::String(argument) if argument.starts_with("--function"))
    });
    contains_semantic && contains_function
}

fn cli_failure(args: &[OsString], error: &clap::Error) -> RankedReportFailure {
    let argument_error = classify_cli_error(args, error);
    let message = clap_suggestion(error).map_or_else(
        || argument_error.to_string(),
        |suggestion| format!("{argument_error}; did you mean {suggestion}?"),
    );
    RankedReportFailure::new(
        RankedReportFailurePhase::Argument,
        argument_error.kind(),
        message,
    )
}

fn clap_suggestion(error: &clap::Error) -> Option<String> {
    [ContextKind::SuggestedArg, ContextKind::SuggestedSubcommand]
        .into_iter()
        .find_map(|kind| {
            let suggestion = match error.get(kind)? {
                ContextValue::String(suggestion) => suggestion.clone(),
                ContextValue::Strings(suggestions) => suggestions.first()?.clone(),
                _ => return None,
            };
            (suggestion.len() <= 64 && suggestion.is_ascii()).then_some(suggestion)
        })
}

fn emit_failure(error: RankedReportFailure) -> i32 {
    eprintln!("{}", error.machine_line());
    failure_exit_code(error.phase())
}

fn failure_exit_code(phase: RankedReportFailurePhase) -> i32 {
    match phase {
        RankedReportFailurePhase::Argument => 64,
        RankedReportFailurePhase::Health
        | RankedReportFailurePhase::Query
        | RankedReportFailurePhase::AggregateValidation => 65,
        RankedReportFailurePhase::Input => 66,
        RankedReportFailurePhase::TraceProcessor => 69,
        RankedReportFailurePhase::Serialization => 70,
        RankedReportFailurePhase::Output => 73,
    }
}

pub(super) fn preflight_ranked_report_paths(
    input: &Path,
    output: &Path,
) -> Result<RankedReportPaths, RankedReportPathError> {
    let input = preflight_ranked_profile_input(input)?;
    let output = absolute_path(output).map_err(RankedReportPathError::OutputInspection)?;
    if output.file_name().is_none() {
        return Err(RankedReportPathError::OutputHasNoFileName);
    }
    inspect_output_path(&output)?;
    let output_identity =
        resolve_output_identity(&output).map_err(RankedReportPathError::OutputInspection)?;
    match same_file::is_same_file(&input, &output_identity) {
        Ok(true) => return Err(RankedReportPathError::InputOutputAlias),
        Ok(false) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(RankedReportPathError::OutputInspection(error)),
    }
    Ok(RankedReportPaths { input, output })
}

pub(super) fn preflight_ranked_profile_input(
    input: &Path,
) -> Result<PathBuf, RankedReportPathError> {
    let input_file = File::open(input).map_err(RankedReportPathError::InputUnreadable)?;
    if !input_file
        .metadata()
        .map_err(RankedReportPathError::InputUnreadable)?
        .is_file()
    {
        return Err(RankedReportPathError::InputNotFile);
    }
    let input = input
        .canonicalize()
        .map_err(RankedReportPathError::InputUnreadable)?;
    Ok(input)
}

fn resolve_output_identity(path: &Path) -> io::Result<PathBuf> {
    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            Component::RootDir => resolved.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Normal(segment) => {
                resolved.push(segment);
                match resolved.canonicalize() {
                    Ok(canonical) => resolved = canonical,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(resolved)
}

fn absolute_path(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn inspect_output_path(output: &Path) -> Result<(), RankedReportPathError> {
    match fs::metadata(output) {
        Ok(metadata) if !metadata.is_file() => return Err(RankedReportPathError::OutputNotFile),
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) if error.kind() == io::ErrorKind::NotADirectory => {
            return Err(RankedReportPathError::OutputParentNotDirectory);
        }
        Err(error) => return Err(RankedReportPathError::OutputInspection(error)),
    }

    let mut ancestor = output
        .parent()
        .ok_or(RankedReportPathError::OutputHasNoFileName)?;
    loop {
        match fs::metadata(ancestor) {
            Ok(metadata) if metadata.is_dir() => return Ok(()),
            Ok(_) => return Err(RankedReportPathError::OutputParentNotDirectory),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                ancestor = ancestor
                    .parent()
                    .ok_or(RankedReportPathError::OutputParentNotDirectory)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotADirectory => {
                return Err(RankedReportPathError::OutputParentNotDirectory);
            }
            Err(error) => return Err(RankedReportPathError::OutputInspection(error)),
        }
    }
}

fn default_report_path(input: &Path) -> PathBuf {
    input.with_extension("profile.html")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parses_report_arguments_and_generates_help() -> Result<(), Box<dyn std::error::Error>> {
        let root_help = PerfettoCli::try_parse_from(["delta-funnel-perfetto", "--help"])
            .err()
            .ok_or("root help should stop parsing")?;
        assert_eq!(root_help.kind(), ErrorKind::DisplayHelp);
        let root_help = root_help.to_string();
        assert!(root_help.contains("Generate and inspect Delta Funnel Perfetto diagnostics"));
        assert!(root_help.contains("report"));
        assert!(root_help.contains("inspect"));

        let report_help =
            PerfettoCli::try_parse_from(["delta-funnel-perfetto", "report", "--help"])
                .err()
                .ok_or("report help should stop parsing")?;
        assert_eq!(report_help.kind(), ErrorKind::DisplayHelp);
        let report_help = report_help.to_string();
        assert!(report_help.contains("INPUT.pftrace"));
        assert!(report_help.contains("--output <OUTPUT.profile.html>"));

        let default_output =
            PerfettoCli::try_parse_from(["delta-funnel-perfetto", "report", "capture.pftrace"])?;
        assert_eq!(
            default_output,
            PerfettoCli {
                command: PerfettoCommand::Report(RankedReportArgs {
                    input: PathBuf::from("capture.pftrace"),
                    output: None,
                }),
            }
        );

        let explicit_output = PerfettoCli::try_parse_from([
            "delta-funnel-perfetto",
            "report",
            "--output",
            "reports/capture.html",
            "traces/capture",
        ])?;
        assert_eq!(
            explicit_output,
            PerfettoCli {
                command: PerfettoCommand::Report(RankedReportArgs {
                    input: PathBuf::from("traces/capture"),
                    output: Some(PathBuf::from("reports/capture.html")),
                }),
            }
        );
        assert_eq!(
            default_report_path(Path::new("capture.pftrace")),
            PathBuf::from("capture.profile.html")
        );
        Ok(())
    }

    #[test]
    fn parses_inspect_arguments_and_generates_help() -> Result<(), Box<dyn std::error::Error>> {
        let help = PerfettoCli::try_parse_from(["delta-funnel-perfetto", "inspect", "--help"])
            .err()
            .ok_or("inspect help should stop parsing")?;
        assert_eq!(help.kind(), ErrorKind::DisplayHelp);
        let help = help.to_string();
        assert!(help.contains("INPUT.pftrace"));
        assert!(help.contains("--limit <LIMIT>"));
        assert!(help.contains("--semantic <ID>"));
        assert!(help.contains("--function <SEMANTIC_ID:FUNCTION_ID>"));
        assert!(help.contains("--filter <TEXT>"));
        assert!(help.contains("--sort <SORT>"));
        assert!(help.contains("--depth <DEPTH>"));

        assert_eq!(
            PerfettoCli::try_parse_from([
                "delta-funnel-perfetto",
                "inspect",
                "capture.pftrace",
                "--limit",
                "7",
                "--semantic",
                "42",
                "--sort",
                "self-cpu",
                "--filter",
                "scan",
                "--depth",
                "3",
            ])?,
            PerfettoCli {
                command: PerfettoCommand::Inspect(InspectArgs {
                    input: PathBuf::from("capture.pftrace"),
                    limit: 7,
                    semantic: Some(42),
                    function: None,
                    filter: Some(FilterText("scan".to_owned())),
                    sort: Some(InspectSort::SelfCpu),
                    depth: Some(3),
                }),
            }
        );
        assert_eq!(
            PerfettoCli::try_parse_from([
                "delta-funnel-perfetto",
                "inspect",
                "capture.pftrace",
                "--function",
                "42:7",
            ])?,
            PerfettoCli {
                command: PerfettoCommand::Inspect(InspectArgs {
                    input: PathBuf::from("capture.pftrace"),
                    limit: DEFAULT_INSPECT_LIMIT,
                    semantic: None,
                    function: Some(FunctionSelector {
                        semantic_id: 42,
                        function_id: 7,
                    }),
                    filter: None,
                    sort: None,
                    depth: None,
                }),
            }
        );
        Ok(())
    }

    #[test]
    fn dispatches_commands_and_maps_failure_phases_to_exit_codes() {
        assert_eq!(
            run_perfetto_diagnostics_cli_with([OsString::from("--help")]),
            0
        );
        assert_eq!(
            run_perfetto_diagnostics_cli_with([OsString::from("report"), OsString::from("--help")]),
            0
        );
        assert_eq!(
            run_perfetto_diagnostics_cli_with([
                OsString::from("inspect"),
                OsString::from("--help")
            ]),
            0
        );
        assert_eq!(
            run_inspect_command(InspectArgs {
                input: PathBuf::from("missing.pftrace"),
                limit: DEFAULT_INSPECT_LIMIT,
                semantic: None,
                function: Some(FunctionSelector {
                    semantic_id: 1,
                    function_id: 2,
                }),
                filter: None,
                sort: Some(InspectSort::Duration),
                depth: None,
            }),
            64
        );
        assert_eq!(
            run_perfetto_diagnostics_cli_with([OsString::from("unknown")]),
            64
        );
        assert_eq!(failure_exit_code(RankedReportFailurePhase::Health), 65);
        assert_eq!(failure_exit_code(RankedReportFailurePhase::Input), 66);
        assert_eq!(
            failure_exit_code(RankedReportFailurePhase::TraceProcessor),
            69
        );
        assert_eq!(
            failure_exit_code(RankedReportFailurePhase::Serialization),
            70
        );
        assert_eq!(failure_exit_code(RankedReportFailurePhase::Output), 73);
    }

    #[test]
    fn rejects_invalid_argument_shapes_with_stable_kinds() -> Result<(), Box<dyn std::error::Error>>
    {
        for (args, expected) in [
            (vec![], CliArgumentError::MissingCommand),
            (
                vec![OsString::from("unknown")],
                CliArgumentError::UnknownCommand,
            ),
            (
                vec![OsString::from("report")],
                CliArgumentError::MissingInput,
            ),
            (
                vec![OsString::from("report"), OsString::from("--output")],
                CliArgumentError::MissingOutputValue,
            ),
            (
                vec![
                    OsString::from("report"),
                    OsString::from("trace.pftrace"),
                    OsString::from("--output"),
                    OsString::from("first.html"),
                    OsString::from("--output"),
                    OsString::from("second.html"),
                ],
                CliArgumentError::DuplicateOutput,
            ),
            (
                vec![
                    OsString::from("report"),
                    OsString::from("first.pftrace"),
                    OsString::from("second.pftrace"),
                ],
                CliArgumentError::MultipleInputs,
            ),
            (
                vec![OsString::from("report"), OsString::from("--unknown")],
                CliArgumentError::UnknownOption,
            ),
            (
                vec![OsString::from("inspect")],
                CliArgumentError::MissingInput,
            ),
            (
                vec![
                    OsString::from("inspect"),
                    OsString::from("capture.pftrace"),
                    OsString::from("--limit"),
                    OsString::from("0"),
                ],
                CliArgumentError::InvalidLimit,
            ),
            (
                vec![
                    OsString::from("inspect"),
                    OsString::from("capture.pftrace"),
                    OsString::from("--depth"),
                    OsString::from("33"),
                ],
                CliArgumentError::InvalidDepth,
            ),
            (
                vec![
                    OsString::from("inspect"),
                    OsString::from("capture.pftrace"),
                    OsString::from("--semantic"),
                    OsString::from("worker-1"),
                ],
                CliArgumentError::InvalidSemanticId,
            ),
            (
                vec![
                    OsString::from("inspect"),
                    OsString::from("capture.pftrace"),
                    OsString::from("--function"),
                    OsString::from("42"),
                ],
                CliArgumentError::InvalidFunctionId,
            ),
            (
                vec![
                    OsString::from("inspect"),
                    OsString::from("capture.pftrace"),
                    OsString::from("--filter"),
                    OsString::new(),
                ],
                CliArgumentError::InvalidFilter,
            ),
            (
                vec![
                    OsString::from("inspect"),
                    OsString::from("capture.pftrace"),
                    OsString::from("--sort"),
                    OsString::from("cpu"),
                ],
                CliArgumentError::InvalidSort,
            ),
            (
                vec![
                    OsString::from("inspect"),
                    OsString::from("capture.pftrace"),
                    OsString::from("--semantic"),
                    OsString::from("42"),
                    OsString::from("--function"),
                    OsString::from("42:7"),
                ],
                CliArgumentError::IncompatibleSelectors,
            ),
            (
                vec![
                    OsString::from("inspect"),
                    OsString::from("capture.pftrace"),
                    OsString::from("--limit"),
                    OsString::from("10"),
                    OsString::from("--limit"),
                    OsString::from("20"),
                ],
                CliArgumentError::DuplicateOption,
            ),
        ] {
            let error = PerfettoCli::try_parse_from(
                iter::once(OsString::from("delta-funnel-perfetto")).chain(args.iter().cloned()),
            )
            .err()
            .ok_or("invalid arguments should fail")?;
            assert_eq!(classify_cli_error(&args, &error), expected);
        }
        Ok(())
    }

    #[test]
    fn failures_expose_stable_machine_readable_fields() -> Result<(), Box<dyn std::error::Error>> {
        let argument_failure = RankedReportFailure::from(CliArgumentError::MissingInput);
        assert_eq!(argument_failure.phase(), RankedReportFailurePhase::Argument);
        assert_eq!(argument_failure.kind(), "missing_input");

        let failure = RankedReportFailure::new(
            RankedReportFailurePhase::TraceProcessor,
            "execution_failed",
            "first line\nsecond line",
        );
        let value: serde_json::Value =
            serde_json::from_str(&failure.machine_line()).expect("failure should be valid JSON");
        assert_eq!(value["phase"], "trace_processor");
        assert_eq!(value["kind"], "execution_failed");
        assert_eq!(value["message"], "first line\nsecond line");

        let path_failure = RankedReportFailure::from(RankedReportPathError::InputOutputAlias);
        assert_eq!(path_failure.phase(), RankedReportFailurePhase::Output);
        assert_eq!(path_failure.kind(), "aliases_input");

        let typo = vec![OsString::from("reprot")];
        let typo_error = PerfettoCli::try_parse_from(["delta-funnel-perfetto", "reprot"])
            .err()
            .ok_or("misspelled command should fail")?;
        let typo_failure = cli_failure(&typo, &typo_error);
        assert_eq!(typo_failure.kind(), "unknown_command");
        assert!(typo_failure.to_string().contains("did you mean report?"));
        Ok(())
    }

    #[test]
    fn preflights_readable_input_and_non_mutating_output_paths() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("capture.pftrace");
        File::create(&input)?.write_all(b"trace")?;
        let missing_parent = directory.path().join("reports/nested");
        let output = missing_parent.join("capture.profile.html");

        let paths = preflight_ranked_report_paths(&input, &output)
            .map_err(|error| io::Error::other(error.to_string()))?;
        assert_eq!(paths.input, input.canonicalize()?);
        assert_eq!(paths.output, output);
        assert!(!missing_parent.exists());
        Ok(())
    }

    #[test]
    fn rejects_non_files_and_every_existing_input_alias() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("capture.pftrace");
        File::create(&input)?.write_all(b"trace")?;

        assert!(matches!(
            preflight_ranked_report_paths(directory.path(), &directory.path().join("report.html")),
            Err(RankedReportPathError::InputNotFile)
        ));
        assert!(matches!(
            preflight_ranked_report_paths(&input, &input),
            Err(RankedReportPathError::InputOutputAlias)
        ));

        let hard_link = directory.path().join("hard-link.pftrace");
        fs::hard_link(&input, &hard_link)?;
        assert!(matches!(
            preflight_ranked_report_paths(&input, &hard_link),
            Err(RankedReportPathError::InputOutputAlias)
        ));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn preserves_symlink_parent_semantics_when_detecting_aliases() -> io::Result<()> {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir()?;
        let real_parent = directory.path().join("real/child");
        fs::create_dir_all(&real_parent)?;
        let input = directory.path().join("real/capture.pftrace");
        File::create(&input)?.write_all(b"trace")?;
        let link = directory.path().join("link");
        symlink(&real_parent, &link)?;
        let output = link.join("../capture.pftrace");

        assert!(matches!(
            preflight_ranked_report_paths(&input, &output),
            Err(RankedReportPathError::InputOutputAlias)
        ));
        Ok(())
    }

    #[test]
    fn rejects_input_alias_through_a_missing_parent_and_dot_dot() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("capture.pftrace");
        File::create(&input)?.write_all(b"trace")?;
        let missing_parent = directory.path().join("missing");
        let output = missing_parent.join("../capture.pftrace");

        assert!(matches!(
            preflight_ranked_report_paths(&input, &output),
            Err(RankedReportPathError::InputOutputAlias)
        ));
        assert_eq!(fs::read(&input)?, b"trace");
        assert!(!missing_parent.exists());
        Ok(())
    }

    #[test]
    fn rejects_invalid_output_shapes() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("capture.pftrace");
        File::create(&input)?.write_all(b"trace")?;

        assert!(matches!(
            preflight_ranked_report_paths(&input, Path::new(std::path::MAIN_SEPARATOR_STR)),
            Err(RankedReportPathError::OutputHasNoFileName)
        ));
        assert!(matches!(
            preflight_ranked_report_paths(&input, directory.path()),
            Err(RankedReportPathError::OutputHasNoFileName)
                | Err(RankedReportPathError::OutputNotFile)
        ));
        let parent_file = directory.path().join("parent-file");
        File::create(&parent_file)?;
        assert!(matches!(
            preflight_ranked_report_paths(&input, &parent_file.join("report.html")),
            Err(RankedReportPathError::OutputParentNotDirectory)
        ));
        Ok(())
    }
}
