use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RankedReportCliAction {
    Help,
    Generate { input: PathBuf, output: PathBuf },
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RankedReportArgumentError {
    MissingInput,
    MissingOutputValue,
    DuplicateOutput,
    MultipleInputs,
    UnknownOption,
}

impl fmt::Display for RankedReportArgumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::MissingInput => "an input trace path is required",
            Self::MissingOutputValue => "--output requires a path",
            Self::DuplicateOutput => "--output may be specified only once",
            Self::MultipleInputs => "only one input trace path may be provided",
            Self::UnknownOption => "unknown option",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RankedReportArgumentError {}

#[doc(hidden)]
#[derive(Debug)]
pub enum RankedReportPathError {
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

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RankedReportPaths {
    pub input: PathBuf,
    pub output: PathBuf,
}

#[doc(hidden)]
pub fn parse_ranked_report_args(
    args: impl IntoIterator<Item = OsString>,
) -> Result<RankedReportCliAction, RankedReportArgumentError> {
    let mut args = args.into_iter();
    let mut input = None;
    let mut output = None;
    while let Some(argument) = args.next() {
        if matches!(argument.to_str(), Some("-h" | "--help")) {
            return Ok(RankedReportCliAction::Help);
        }
        if argument == OsStr::new("--output") {
            if output.is_some() {
                return Err(RankedReportArgumentError::DuplicateOutput);
            }
            output = Some(PathBuf::from(
                args.next()
                    .ok_or(RankedReportArgumentError::MissingOutputValue)?,
            ));
        } else if argument.as_encoded_bytes().starts_with(b"-") {
            return Err(RankedReportArgumentError::UnknownOption);
        } else if input.replace(PathBuf::from(argument)).is_some() {
            return Err(RankedReportArgumentError::MultipleInputs);
        }
    }

    let input = input.ok_or(RankedReportArgumentError::MissingInput)?;
    let output = output.unwrap_or_else(|| default_report_path(&input));
    Ok(RankedReportCliAction::Generate { input, output })
}

#[doc(hidden)]
pub fn preflight_ranked_report_paths(
    input: &Path,
    output: &Path,
) -> Result<RankedReportPaths, RankedReportPathError> {
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
    let output = absolute_path(output).map_err(RankedReportPathError::OutputInspection)?;
    if output.file_name().is_none() {
        return Err(RankedReportPathError::OutputHasNoFileName);
    }
    inspect_output_path(&output)?;
    match same_file::is_same_file(&input, &output) {
        Ok(true) => return Err(RankedReportPathError::InputOutputAlias),
        Ok(false) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(RankedReportPathError::OutputInspection(error)),
    }
    Ok(RankedReportPaths { input, output })
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
    fn parses_help_explicit_output_and_default_sibling_output() {
        assert_eq!(
            parse_ranked_report_args([OsString::from("--help")]),
            Ok(RankedReportCliAction::Help)
        );
        assert_eq!(
            parse_ranked_report_args([OsString::from("capture.pftrace")]),
            Ok(RankedReportCliAction::Generate {
                input: PathBuf::from("capture.pftrace"),
                output: PathBuf::from("capture.profile.html"),
            })
        );
        assert_eq!(
            parse_ranked_report_args([
                OsString::from("--output"),
                OsString::from("reports/capture.html"),
                OsString::from("traces/capture"),
            ]),
            Ok(RankedReportCliAction::Generate {
                input: PathBuf::from("traces/capture"),
                output: PathBuf::from("reports/capture.html"),
            })
        );
    }

    #[test]
    fn rejects_invalid_argument_shapes() {
        for (args, expected) in [
            (vec![], RankedReportArgumentError::MissingInput),
            (
                vec![OsString::from("--output")],
                RankedReportArgumentError::MissingOutputValue,
            ),
            (
                vec![
                    OsString::from("trace.pftrace"),
                    OsString::from("--output"),
                    OsString::from("first.html"),
                    OsString::from("--output"),
                    OsString::from("second.html"),
                ],
                RankedReportArgumentError::DuplicateOutput,
            ),
            (
                vec![
                    OsString::from("first.pftrace"),
                    OsString::from("second.pftrace"),
                ],
                RankedReportArgumentError::MultipleInputs,
            ),
            (
                vec![OsString::from("--unknown")],
                RankedReportArgumentError::UnknownOption,
            ),
        ] {
            assert_eq!(parse_ranked_report_args(args), Err(expected));
        }
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
