//! Repository maintenance tasks for DeltaFunnel.

use std::env;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::process::{Command, ExitCode};

mod sqlserver;

fn main() -> ExitCode {
    match run(env::args_os().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: impl IntoIterator<Item = OsString>) -> Result<(), XtaskError> {
    let args = args.into_iter().collect::<Vec<_>>();

    match args.first().and_then(|arg| arg.to_str()) {
        None | Some("-h" | "--help") => {
            print_top_level_help();
            Ok(())
        }
        Some("sqlserver-test") => {
            if args[1..].iter().any(|arg| arg == "-h" || arg == "--help") {
                print_sqlserver_test_help();
                return Ok(());
            }

            let options = SqlServerTestOptions::parse(&args[1..])?;
            run_sqlserver_tests(&options)
        }
        Some(command) => Err(XtaskError::UnknownCommand(command.to_owned())),
    }
}

fn run_sqlserver_tests(options: &SqlServerTestOptions) -> Result<(), XtaskError> {
    println!("sqlserver-test");

    if options.connection.connection_string.is_some() {
        println!("  existing connection: true");
        println!("  container runtime: <not used>");
    } else {
        println!("  existing connection: false");
        if let Some(runtime) = &options.connection.container_runtime {
            println!("  container runtime: {}", runtime.display());
        } else {
            println!("  container runtime: <auto>");
        }
        println!("  image: {}", options.connection.image);
    }

    println!("  keep container: {}", options.connection.keep_container);
    println!("  schema: {}", options.schema);

    let connection = options
        .connection
        .connect_or_start()
        .map_err(XtaskError::SqlServer)?;
    let test_connection_string =
        connection_string_with_database(&connection.connection_string, &connection.database);

    println!("  test database: {}", connection.database);

    let mut command = Command::new("cargo");
    command
        .arg("test")
        .arg("-p")
        .arg("delta-funnel")
        .arg("--test")
        .arg("mssql_direct_raw_bulk")
        .arg("--")
        .arg("--nocapture")
        .env(sqlserver::CONNECTION_STRING_ENV, test_connection_string)
        .env(sqlserver::TEST_SCHEMA_ENV, &options.schema);

    run_command(
        &mut command,
        "cargo test -p delta-funnel --test mssql_direct_raw_bulk",
    )?;
    Ok(())
}

fn print_top_level_help() {
    println!(
        "Usage:\n  cargo xtask <COMMAND> [OPTIONS]\n\nCommands:\n  sqlserver-test    Run SQL Server integration tests\n\nRun `cargo xtask <COMMAND> --help` for command-specific options."
    );
}

fn print_sqlserver_test_help() {
    println!(
        "Usage:\n  cargo xtask sqlserver-test [OPTIONS]\n\nOptions:\n  --container-runtime <PATH>  Container runtime executable, such as docker or podman\n  --connection-string <URL>   Use an existing SQL Server instead of a local container\n  --image <IMAGE>             SQL Server container image\n  --database <NAME>           Test database name\n  --schema <NAME>             Test schema name [default: dbo]\n  --keep-container            Keep the container after the task exits\n  -h, --help                  Print help"
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerTestOptions {
    connection: sqlserver::SqlServerConnectionOptions,
    schema: String,
}

impl Default for SqlServerTestOptions {
    fn default() -> Self {
        Self {
            connection: sqlserver::SqlServerConnectionOptions::integration_default(),
            schema: "dbo".to_owned(),
        }
    }
}

impl SqlServerTestOptions {
    fn parse(args: &[OsString]) -> Result<Self, XtaskError> {
        let mut options = Self::default();
        let mut index = 0;

        while index < args.len() {
            let arg = args[index]
                .to_str()
                .ok_or_else(|| XtaskError::InvalidUtf8Argument(args[index].clone()))?;

            match arg {
                "-h" | "--help" => {
                    print_sqlserver_test_help();
                    return Ok(options);
                }
                "--container-runtime" => {
                    options.connection.container_runtime =
                        Some(PathBuf::from(required_value(args, index)?));
                    index += 1;
                }
                "--connection-string" => {
                    options.connection.connection_string = Some(required_value(args, index)?);
                    index += 1;
                }
                "--image" => {
                    options.connection.image = required_value(args, index)?;
                    index += 1;
                }
                "--database" => {
                    options.connection.database = required_value(args, index)?;
                    index += 1;
                }
                "--schema" => {
                    options.schema = required_value(args, index)?;
                    index += 1;
                }
                "--keep-container" => {
                    options.connection.keep_container = true;
                }
                other => return Err(XtaskError::UnknownOption(other.to_owned())),
            }

            index += 1;
        }

        sqlserver::validate_database_name(&options.connection.database)
            .map_err(XtaskError::SqlServer)?;
        sqlserver::validate_schema_name(&options.schema).map_err(XtaskError::SqlServer)?;

        Ok(options)
    }
}

fn required_value(args: &[OsString], index: usize) -> Result<String, XtaskError> {
    let value = args
        .get(index + 1)
        .ok_or_else(|| XtaskError::MissingOptionValue(option_name(args, index)))?;

    value
        .to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| XtaskError::InvalidUtf8Argument(value.clone()))
}

fn option_name(args: &[OsString], index: usize) -> String {
    args.get(index)
        .and_then(|arg| arg.to_str())
        .unwrap_or("<unknown>")
        .to_owned()
}

fn run_command(command: &mut Command, description: &'static str) -> Result<(), XtaskError> {
    let status = command
        .status()
        .map_err(|source| XtaskError::CommandSpawn {
            description,
            source,
        })?;

    if status.success() {
        Ok(())
    } else {
        Err(XtaskError::CommandStatus {
            description,
            status,
        })
    }
}

fn connection_string_with_database(connection_string: &str, database: &str) -> String {
    if connection_string_contains_database(connection_string) {
        return connection_string.to_owned();
    }

    let separator = if connection_string.ends_with(';') {
        ""
    } else {
        ";"
    };
    format!("{connection_string}{separator}database={database}")
}

fn connection_string_contains_database(connection_string: &str) -> bool {
    connection_string.split(';').any(|part| {
        let key = part
            .split_once('=')
            .map(|(key, _value)| key.trim().to_ascii_lowercase());
        matches!(key.as_deref(), Some("database" | "initial catalog"))
    })
}

#[derive(Debug)]
enum XtaskError {
    UnknownCommand(String),
    UnknownOption(String),
    MissingOptionValue(String),
    InvalidUtf8Argument(OsString),
    CommandSpawn {
        description: &'static str,
        source: std::io::Error,
    },
    CommandStatus {
        description: &'static str,
        status: std::process::ExitStatus,
    },
    SqlServer(sqlserver::SqlServerError),
}

impl fmt::Display for XtaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCommand(command) => write!(f, "unknown command `{command}`"),
            Self::UnknownOption(option) => write!(f, "unknown option `{option}`"),
            Self::MissingOptionValue(option) => write!(f, "missing value for `{option}`"),
            Self::InvalidUtf8Argument(arg) => write!(f, "argument is not valid UTF-8: {arg:?}"),
            Self::CommandSpawn {
                description,
                source,
            } => {
                write!(f, "failed to {description}: {source}")
            }
            Self::CommandStatus {
                description,
                status,
            } => {
                write!(f, "{description} exited with {status}")
            }
            Self::SqlServer(source) => source.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::{SqlServerTestOptions, connection_string_with_database};

    #[test]
    fn parses_sqlserver_test_options() -> Result<(), String> {
        let args = [
            OsString::from("--container-runtime"),
            OsString::from("podman"),
            OsString::from("--database"),
            OsString::from("delta_funnel_it"),
            OsString::from("--schema"),
            OsString::from("dbo"),
            OsString::from("--keep-container"),
        ];

        let options = match SqlServerTestOptions::parse(&args) {
            Ok(options) => options,
            Err(error) => return Err(format!("expected options to parse: {error}")),
        };

        assert_eq!(options.connection.container_runtime, Some("podman".into()));
        assert_eq!(options.connection.database, "delta_funnel_it");
        assert_eq!(options.schema, "dbo");
        assert!(options.connection.keep_container);
        Ok(())
    }

    #[test]
    fn appends_database_when_connection_string_has_no_database() {
        let connection =
            connection_string_with_database("server=tcp:127.0.0.1,1433;user id=sa", "df_it");

        assert_eq!(
            connection,
            "server=tcp:127.0.0.1,1433;user id=sa;database=df_it"
        );
    }

    #[test]
    fn preserves_connection_string_with_existing_database() {
        let connection =
            connection_string_with_database("server=tcp:127.0.0.1,1433;database=df_it", "other");

        assert_eq!(connection, "server=tcp:127.0.0.1,1433;database=df_it");
    }
}
