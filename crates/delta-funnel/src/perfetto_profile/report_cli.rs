use std::ffi::{OsStr, OsString};
use std::fmt;
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

fn default_report_path(input: &Path) -> PathBuf {
    input.with_extension("profile.html")
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
