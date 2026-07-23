use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufRead, IsTerminal, Write};
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
const MAX_INTERACTIVE_COMMAND_BYTES: usize = 1024;
const INTERACTIVE_END_MARKER: &str = "-- end --\n";

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

    /// Keep the loaded profile open for line-oriented commands.
    #[arg(long)]
    interactive: bool,
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

struct InspectState {
    selection: InspectSelection,
    sort: InspectSort,
    filter: Option<String>,
    limit: usize,
    depth: Option<usize>,
}

struct InspectNavigation {
    semantic_parents: HashMap<i64, Option<i64>>,
    function_parents: HashMap<(i64, i64), Option<i64>>,
}

impl InspectNavigation {
    fn new(document: &super::ranked_report::RankedProfileDocument) -> Self {
        Self {
            semantic_parents: document
                .semantics
                .iter()
                .map(|semantic| (semantic.semantic_id, semantic.parent_semantic_id))
                .collect(),
            function_parents: document
                .functions
                .iter()
                .map(|function| {
                    (
                        (function.semantic_id, function.function_id),
                        function.parent_function_id,
                    )
                })
                .collect(),
        }
    }

    fn open_semantic(
        &self,
        selection: InspectSelection,
        semantic_id: i64,
    ) -> Result<InspectSelection, &'static str> {
        let parent = match selection {
            InspectSelection::Root => None,
            InspectSelection::Semantic(parent) => Some(parent),
            InspectSelection::Function { .. } => {
                return Err("semantic target is not an immediate child");
            }
        };
        if self.semantic_parents.get(&semantic_id).copied() != Some(parent) {
            return Err("semantic target is not an immediate child");
        }
        Ok(InspectSelection::Semantic(semantic_id))
    }

    fn open_function(
        &self,
        selection: InspectSelection,
        semantic_id: i64,
        function_id: i64,
    ) -> Result<InspectSelection, &'static str> {
        let parent = match selection {
            InspectSelection::Semantic(owner) if owner == semantic_id => None,
            InspectSelection::Function {
                semantic_id: owner,
                function_id: parent,
            } if owner == semantic_id => Some(parent),
            InspectSelection::Root
            | InspectSelection::Semantic(_)
            | InspectSelection::Function { .. } => {
                return Err("function target is not an immediate child");
            }
        };
        if self
            .function_parents
            .get(&(semantic_id, function_id))
            .copied()
            != Some(parent)
        {
            return Err("function target is not an immediate child");
        }
        Ok(InspectSelection::Function {
            semantic_id,
            function_id,
        })
    }

    fn up(&self, selection: InspectSelection) -> Result<InspectSelection, &'static str> {
        match selection {
            InspectSelection::Root => Err("already at operation roots"),
            InspectSelection::Semantic(semantic_id) => self
                .semantic_parents
                .get(&semantic_id)
                .copied()
                .map(|parent| parent.map_or(InspectSelection::Root, InspectSelection::Semantic))
                .ok_or("current semantic selection does not exist"),
            InspectSelection::Function {
                semantic_id,
                function_id,
            } => self
                .function_parents
                .get(&(semantic_id, function_id))
                .copied()
                .map(|parent| {
                    parent.map_or(InspectSelection::Semantic(semantic_id), |function_id| {
                        InspectSelection::Function {
                            semantic_id,
                            function_id,
                        }
                    })
                })
                .ok_or("current function selection does not exist"),
        }
    }
}

impl InspectState {
    fn render(
        &self,
        document: &super::ranked_report::RankedProfileDocument,
    ) -> Result<String, TerminalInspectError> {
        super::render_terminal_view(
            document,
            self.selection,
            self.sort,
            self.filter.as_deref(),
            self.limit,
            self.depth
                .unwrap_or_else(|| usize::from(self.selection != InspectSelection::Root)),
        )
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
    let mut state = InspectState {
        selection,
        sort,
        filter: args.filter.map(|filter| filter.0),
        limit: usize::from(args.limit),
        depth: args.depth.map(usize::from),
    };
    let input = match preflight_ranked_profile_input(&args.input) {
        Ok(input) => input,
        Err(error) => return emit_failure(error.into()),
    };
    match super::load_ranked_profile(&input) {
        Ok(document) if args.interactive => {
            let stdin = io::stdin();
            let stdout = io::stdout();
            let prompt = stdin.is_terminal() && stdout.is_terminal();
            let mut input = stdin.lock();
            let mut output = stdout.lock();
            let mut error = io::stderr().lock();
            match run_interactive_session(
                &document,
                &mut state,
                &mut input,
                &mut output,
                &mut error,
                prompt,
            ) {
                Ok(()) => 0,
                Err(error) => emit_failure(error),
            }
        }
        Ok(document) => match state.render(&document) {
            Ok(output) => {
                let mut stdout = io::stdout().lock();
                match stdout
                    .write_all(output.as_bytes())
                    .and_then(|()| stdout.flush())
                {
                    Ok(()) => 0,
                    Err(_) => emit_failure(terminal_output_failure()),
                }
            }
            Err(error) => emit_failure(error.into()),
        },
        Err(error) => emit_failure(error),
    }
}

fn run_interactive_session(
    document: &super::ranked_report::RankedProfileDocument,
    state: &mut InspectState,
    input: &mut impl BufRead,
    output: &mut impl Write,
    error: &mut impl Write,
    prompt: bool,
) -> Result<(), RankedReportFailure> {
    let navigation = InspectNavigation::new(document);
    let initial = state.render(document).map_err(RankedReportFailure::from)?;
    write_interactive_response(output, &initial)?;
    let mut line = Vec::with_capacity(MAX_INTERACTIVE_COMMAND_BYTES);
    loop {
        if prompt {
            output
                .write_all(b"profile> ")
                .and_then(|()| output.flush())
                .map_err(|_| terminal_output_failure())?;
        }
        match read_interactive_line(input, &mut line).map_err(|_| interactive_input_failure())? {
            InteractiveLine::Eof => return Ok(()),
            InteractiveLine::Invalid(message) => {
                write_interactive_error(error, message)?;
                write_interactive_response(output, "")?;
            }
            InteractiveLine::Command(command) => {
                match run_interactive_command(document, &navigation, state, command.trim()) {
                    Ok(InteractiveCommandResult::Output(response)) => {
                        write_interactive_response(output, &response)?;
                    }
                    Ok(InteractiveCommandResult::Quit) => {
                        write_interactive_response(output, "")?;
                        return Ok(());
                    }
                    Err(message) => {
                        write_interactive_error(error, message)?;
                        write_interactive_response(output, "")?;
                    }
                }
            }
        }
    }
}

enum InteractiveCommandResult {
    Output(String),
    Quit,
}

fn run_interactive_command(
    document: &super::ranked_report::RankedProfileDocument,
    navigation: &InspectNavigation,
    state: &mut InspectState,
    command: &str,
) -> Result<InteractiveCommandResult, &'static str> {
    match command {
        "show" => state
            .render(document)
            .map(InteractiveCommandResult::Output)
            .map_err(|_| "current profile selection does not exist"),
        "up" => {
            state.selection = navigation.up(state.selection)?;
            render_interactive_selection(document, state)
        }
        "root" => {
            state.selection = InspectSelection::Root;
            render_interactive_selection(document, state)
        }
        "help" => Ok(InteractiveCommandResult::Output(
            "commands: show, open semantic:ID, open function:SEMANTIC_ID:FUNCTION_ID, up, root, sort METRIC, limit N, help, quit\n"
                .to_owned(),
        )),
        "quit" => Ok(InteractiveCommandResult::Quit),
        _ => {
            if command == "sort" || command.starts_with("sort ") {
                let value = command
                    .strip_prefix("sort ")
                    .ok_or("invalid sort; expected sort METRIC")?;
                let sort = match value {
                    "duration" => InspectSort::Duration,
                    "inclusive-cpu" => InspectSort::InclusiveCpu,
                    "self-cpu" => InspectSort::SelfCpu,
                    "name" => InspectSort::Name,
                    _ => {
                        return Err(
                            "invalid sort; expected duration, inclusive-cpu, self-cpu, or name",
                        );
                    }
                };
                if sort == InspectSort::Duration
                    && matches!(state.selection, InspectSelection::Function { .. })
                {
                    return Err("function callsites cannot be sorted by exact duration");
                }
                state.sort = sort;
                return render_interactive_selection(document, state);
            }
            if command == "limit" || command.starts_with("limit ") {
                let value = command
                    .strip_prefix("limit ")
                    .ok_or("invalid limit; expected limit N")?;
                let limit = value
                    .parse::<u16>()
                    .ok()
                    .filter(|limit| (1..=MAX_INSPECT_LIMIT).contains(limit))
                    .ok_or("limit must be between 1 and 200")?;
                state.limit = usize::from(limit);
                return render_interactive_selection(document, state);
            }
            let target = command
                .strip_prefix("open ")
                .ok_or("unknown interactive command")?;
            if let Some(semantic_id) = target.strip_prefix("semantic:") {
                let semantic_id = semantic_id
                    .parse()
                    .map_err(|_| "invalid semantic identity; expected semantic:ID")?;
                state.selection = navigation.open_semantic(state.selection, semantic_id)?;
            } else if let Some(function) = target.strip_prefix("function:") {
                let function = function.parse::<FunctionSelector>().map_err(|_| {
                    "invalid function identity; expected function:SEMANTIC_ID:FUNCTION_ID"
                })?;
                state.selection = navigation.open_function(
                    state.selection,
                    function.semantic_id,
                    function.function_id,
                )?;
                if state.sort == InspectSort::Duration {
                    state.sort = InspectSort::InclusiveCpu;
                }
            } else {
                return Err(
                    "invalid open target; expected semantic:ID or function:SEMANTIC_ID:FUNCTION_ID",
                );
            }
            render_interactive_selection(document, state)
        }
    }
}

fn render_interactive_selection(
    document: &super::ranked_report::RankedProfileDocument,
    state: &InspectState,
) -> Result<InteractiveCommandResult, &'static str> {
    state
        .render(document)
        .map(InteractiveCommandResult::Output)
        .map_err(|_| "current profile selection does not exist")
}

enum InteractiveLine {
    Eof,
    Command(String),
    Invalid(&'static str),
}

fn read_interactive_line(
    input: &mut impl BufRead,
    line: &mut Vec<u8>,
) -> io::Result<InteractiveLine> {
    line.clear();
    let mut read_any = false;
    let mut exceeded_limit = false;
    loop {
        let available = input.fill_buf()?;
        if available.is_empty() {
            break;
        }
        read_any = true;
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let payload = newline.unwrap_or(consumed);
        if !exceeded_limit {
            let retained = payload.min(
                MAX_INTERACTIVE_COMMAND_BYTES
                    .saturating_add(1)
                    .saturating_sub(line.len()),
            );
            line.extend_from_slice(&available[..retained]);
            exceeded_limit |= retained != payload || line.len() > MAX_INTERACTIVE_COMMAND_BYTES;
        }
        input.consume(consumed);
        if newline.is_some() {
            break;
        }
    }
    if !read_any {
        return Ok(InteractiveLine::Eof);
    }
    if exceeded_limit {
        return Ok(InteractiveLine::Invalid(
            "interactive command exceeds the 1024-byte limit",
        ));
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    match std::str::from_utf8(line) {
        Ok(line) => Ok(InteractiveLine::Command(line.to_owned())),
        Err(_) => Ok(InteractiveLine::Invalid(
            "interactive command must be valid UTF-8",
        )),
    }
}

fn write_interactive_response(
    output: &mut impl Write,
    response: &str,
) -> Result<(), RankedReportFailure> {
    output
        .write_all(response.as_bytes())
        .and_then(|()| {
            if !response.is_empty() && !response.ends_with('\n') {
                output.write_all(b"\n")?;
            }
            output.write_all(INTERACTIVE_END_MARKER.as_bytes())
        })
        .and_then(|()| output.flush())
        .map_err(|_| terminal_output_failure())
}

fn write_interactive_error(
    error: &mut impl Write,
    message: &str,
) -> Result<(), RankedReportFailure> {
    writeln!(error, "error: {message}")
        .and_then(|()| error.flush())
        .map_err(|_| terminal_output_failure())
}

fn interactive_input_failure() -> RankedReportFailure {
    RankedReportFailure::new(
        RankedReportFailurePhase::Input,
        "interactive_read_failed",
        "interactive command input could not be read",
    )
}

fn terminal_output_failure() -> RankedReportFailure {
    RankedReportFailure::new(
        RankedReportFailurePhase::Output,
        "terminal_write_failed",
        "terminal output could not be written",
    )
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

    fn interactive_document() -> super::super::ranked_report::RankedProfileDocument {
        use super::super::ranked_report::{
            RankedFunction, RankedProfileDocument, RankedProfileMetadata,
        };

        RankedProfileDocument {
            metadata: RankedProfileMetadata {
                schema_version: 1,
                sample_frequency_hz: 100,
                exact_time_unit: "nanoseconds".to_owned(),
                sample_unit: "samples".to_owned(),
                eligible_sample_count: 0,
                direct_sample_count: 0,
                ambiguous_sample_count: 0,
                unattributed_sample_count: 0,
            },
            semantics: vec![
                interactive_semantic(1, None, "operation"),
                interactive_semantic(2, Some(1), "planning"),
                interactive_semantic(3, Some(2), "metadata"),
            ],
            functions: vec![
                RankedFunction {
                    semantic_id: 1,
                    function_id: 10,
                    parent_function_id: None,
                    name: "root_function".to_owned(),
                    module_name: None,
                    source_file: None,
                    line_number: None,
                    self_sample_count: 1,
                    inclusive_sample_count: 2,
                },
                RankedFunction {
                    semantic_id: 1,
                    function_id: 11,
                    parent_function_id: Some(10),
                    name: "child_function".to_owned(),
                    module_name: None,
                    source_file: None,
                    line_number: None,
                    self_sample_count: 1,
                    inclusive_sample_count: 1,
                },
                RankedFunction {
                    semantic_id: 2,
                    function_id: 20,
                    parent_function_id: None,
                    name: "other_semantic_function".to_owned(),
                    module_name: None,
                    source_file: None,
                    line_number: None,
                    self_sample_count: 1,
                    inclusive_sample_count: 1,
                },
            ],
        }
    }

    fn interactive_semantic(
        semantic_id: i64,
        parent_semantic_id: Option<i64>,
        name: &str,
    ) -> super::super::ranked_report::RankedSemantic {
        super::super::ranked_report::RankedSemantic {
            semantic_id,
            parent_semantic_id,
            operation_id: 1,
            name: name.to_owned(),
            semantic_kind: name.to_owned(),
            operation_kind: Some("preview".to_owned()),
            stage_category: None,
            stage_name: None,
            activity: None,
            start_ns: 0,
            end_ns: Some(10),
            duration_ns: Some(10),
            time_semantics: "wall_clock".to_owned(),
            result: Some("completed".to_owned()),
            is_complete: true,
            query_execution_id: None,
            query_scope: None,
            query_owner: None,
            worker_lane_id: None,
            worker_kind: None,
            node_id: None,
            parent_node_id: None,
            operator_partition: None,
            execution_stream_id: None,
            stage_owner_id: None,
            direct_sample_count: 2,
            inclusive_sample_count: 4,
        }
    }

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
        assert!(help.contains("--interactive"));
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
                "--interactive",
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
                    interactive: true,
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
                    interactive: false,
                }),
            }
        );
        Ok(())
    }

    #[test]
    fn interactive_session_is_line_oriented_bounded_and_recoverable() {
        let document = interactive_document();
        let mut state = InspectState {
            selection: InspectSelection::Root,
            sort: InspectSort::Duration,
            filter: None,
            limit: 20,
            depth: None,
        };
        let mut commands = vec![b'a'; MAX_INTERACTIVE_COMMAND_BYTES + 1];
        commands.extend_from_slice(b"\nshow\nunknown\nhelp\nquit\n");
        let mut input = io::Cursor::new(commands);
        let mut output = Vec::new();
        let mut error = Vec::new();

        run_interactive_session(
            &document,
            &mut state,
            &mut input,
            &mut output,
            &mut error,
            false,
        )
        .expect("recoverable commands should preserve the session");

        let output = String::from_utf8(output).expect("output should be UTF-8");
        let error = String::from_utf8(error).expect("errors should be UTF-8");
        assert_eq!(output.matches(INTERACTIVE_END_MARKER).count(), 6);
        assert_eq!(output.matches("context: operation-roots").count(), 2);
        assert!(output.contains("open semantic:ID"));
        assert!(!output.contains("profile> "));
        assert!(error.contains("interactive command exceeds the 1024-byte limit"));
        assert!(error.contains("unknown interactive command"));
    }

    #[test]
    fn interactive_navigation_requires_exact_immediate_children() {
        let document = interactive_document();
        let mut state = InspectState {
            selection: InspectSelection::Root,
            sort: InspectSort::Duration,
            filter: None,
            limit: 20,
            depth: None,
        };
        let commands = b"open semantic:2\nopen semantic:1\nopen function:1:11\nopen function:2:20\nopen function:1:10\nopen function:1:11\nup\nup\nopen semantic:2\nroot\nup\nquit\n";
        let mut input = io::Cursor::new(commands);
        let mut output = Vec::new();
        let mut error = Vec::new();

        run_interactive_session(
            &document,
            &mut state,
            &mut input,
            &mut output,
            &mut error,
            false,
        )
        .expect("navigation errors should preserve the session");

        let output = String::from_utf8(output).expect("output should be UTF-8");
        let error = String::from_utf8(error).expect("errors should be UTF-8");
        assert_eq!(output.matches(INTERACTIVE_END_MARKER).count(), 13);
        assert_eq!(output.matches("context: operation-roots").count(), 2);
        assert_eq!(output.matches("context: semantic:1").count(), 2);
        assert_eq!(output.matches("context: function:1:10").count(), 2);
        assert_eq!(output.matches("context: function:1:11").count(), 1);
        assert_eq!(output.matches("context: semantic:2").count(), 1);
        assert_eq!(
            error
                .matches("semantic target is not an immediate child")
                .count(),
            1
        );
        assert_eq!(
            error
                .matches("function target is not an immediate child")
                .count(),
            2
        );
        assert!(error.contains("already at operation roots"));
    }

    #[test]
    fn interactive_sort_and_limit_validate_before_changing_state() {
        let document = interactive_document();
        let navigation = InspectNavigation::new(&document);
        let mut state = InspectState {
            selection: InspectSelection::Root,
            sort: InspectSort::Duration,
            filter: None,
            limit: 20,
            depth: None,
        };

        assert!(matches!(
            run_interactive_command(&document, &navigation, &mut state, "open semantic:1"),
            Ok(InteractiveCommandResult::Output(_))
        ));
        let Ok(InteractiveCommandResult::Output(output)) =
            run_interactive_command(&document, &navigation, &mut state, "limit 1")
        else {
            panic!("a valid limit should render the current view");
        };
        assert_eq!(state.limit, 1);
        assert!(output.contains("showing: 1 of 2; truncated: true"));
        assert!(matches!(
            run_interactive_command(&document, &navigation, &mut state, "limit 0"),
            Err("limit must be between 1 and 200")
        ));
        assert_eq!(state.limit, 1);

        let Ok(InteractiveCommandResult::Output(output)) =
            run_interactive_command(&document, &navigation, &mut state, "sort name")
        else {
            panic!("a valid sort should render the current view");
        };
        assert_eq!(state.sort, InspectSort::Name);
        assert!(output.contains("sort: name"));
        assert!(matches!(
            run_interactive_command(&document, &navigation, &mut state, "sort unknown"),
            Err("invalid sort; expected duration, inclusive-cpu, self-cpu, or name")
        ));
        assert_eq!(state.sort, InspectSort::Name);

        assert!(matches!(
            run_interactive_command(&document, &navigation, &mut state, "open function:1:10"),
            Ok(InteractiveCommandResult::Output(_))
        ));
        assert!(matches!(
            run_interactive_command(&document, &navigation, &mut state, "sort duration"),
            Err("function callsites cannot be sorted by exact duration")
        ));
        assert_eq!(state.sort, InspectSort::Name);
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
                interactive: false,
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
