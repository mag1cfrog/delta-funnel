//! Repository maintenance tasks for DeltaFunnel.

use std::env;
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

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
        Some("python-package-check") => {
            if args[1..].iter().any(|arg| arg == "-h" || arg == "--help") {
                print_python_package_check_help();
                return Ok(());
            }

            let options = PythonPackageCheckOptions::parse(&args[1..])?;
            run_python_package_check(&options)
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
        .env(sqlserver::CONNECTION_STRING_ENV, &test_connection_string)
        .env(sqlserver::TEST_SCHEMA_ENV, &options.schema);

    run_command(
        &mut command,
        "cargo test -p delta-funnel --test mssql_direct_raw_bulk",
    )?;

    let mut command = Command::new("cargo");
    command
        .arg("test")
        .arg("-p")
        .arg("delta-funnel-python")
        .arg("table_write_to_mssql_execute_writes")
        .arg("--")
        .arg("--ignored")
        .arg("--nocapture")
        .env(sqlserver::CONNECTION_STRING_ENV, &test_connection_string)
        .env(sqlserver::TEST_SCHEMA_ENV, &options.schema);

    run_command(
        &mut command,
        "cargo test -p delta-funnel-python table_write_to_mssql_execute_writes -- --ignored",
    )?;

    let mut command = Command::new("cargo");
    command
        .arg("test")
        .arg("-p")
        .arg("delta-funnel-python")
        .arg("write_all_execute_writes")
        .arg("--")
        .arg("--ignored")
        .arg("--nocapture")
        .env(sqlserver::CONNECTION_STRING_ENV, &test_connection_string)
        .env(sqlserver::TEST_SCHEMA_ENV, &options.schema);

    run_command(
        &mut command,
        "cargo test -p delta-funnel-python write_all_execute_writes -- --ignored",
    )?;
    Ok(())
}

fn run_python_package_check(options: &PythonPackageCheckOptions) -> Result<(), XtaskError> {
    println!("python-package-check");
    println!("  python: {}", options.python.display());

    let repo_root = repo_root();
    let python_crate = repo_root.join("crates").join("delta-funnel-python");
    let temp_parent = repo_root.join("target").join("xtask");
    let temp_dir = TempDir::create_in(&temp_parent, "python-package-check")?;
    let tool_tmp = temp_dir.path().join("tmp");
    let wheels_dir = temp_dir.path().join("wheels");
    fs::create_dir_all(&tool_tmp).map_err(|source| XtaskError::TempDir {
        path: tool_tmp.clone(),
        source,
    })?;
    fs::create_dir_all(&wheels_dir).map_err(|source| XtaskError::TempDir {
        path: wheels_dir.clone(),
        source,
    })?;
    println!("  work dir: {}", temp_dir.path().display());

    let mut command = Command::new("maturin");
    command
        .arg("build")
        .arg("--skip-auditwheel")
        .arg("--out")
        .arg(&wheels_dir)
        .current_dir(&python_crate)
        .env("TMPDIR", &tool_tmp);

    run_command(
        &mut command,
        "run maturin build --skip-auditwheel for the Python package",
    )?;

    let wheel = single_wheel_in(&wheels_dir)?;
    println!("  wheel: {}", wheel.display());

    let mut command = Command::new(&options.python);
    command
        .arg("-c")
        .arg(WHEEL_CONTENT_CHECK)
        .arg(&wheel)
        .current_dir(&repo_root)
        .env("TMPDIR", &tool_tmp);
    run_command(&mut command, "verify Python wheel typing metadata")?;

    let venv_dir = temp_dir.path().join("venv");
    let mut command = Command::new(&options.python);
    command
        .arg("-m")
        .arg("venv")
        .arg(&venv_dir)
        .env("TMPDIR", &tool_tmp);
    run_command(&mut command, "create clean Python virtualenv")?;

    let venv_python = venv_python(&venv_dir);
    let pip_cache = temp_dir.path().join("pip-cache");

    let mut command = Command::new(&venv_python);
    command
        .arg("-m")
        .arg("pip")
        .arg("install")
        .arg("--no-cache-dir")
        .arg(&wheel)
        .env("PIP_CACHE_DIR", &pip_cache)
        .env("TMPDIR", &tool_tmp);
    run_command(&mut command, "install Python wheel into clean virtualenv")?;

    for rich_requirement in [None, Some("rich==14.0.0")] {
        if let Some(rich_requirement) = rich_requirement {
            let mut command = Command::new(&venv_python);
            command
                .arg("-m")
                .arg("pip")
                .arg("install")
                .arg("--no-cache-dir")
                .arg(rich_requirement)
                .env("PIP_CACHE_DIR", &pip_cache)
                .env("TMPDIR", &tool_tmp);
            run_command(&mut command, "install minimum supported Rich release")?;
        }

        for logging_order in ["before", "after"] {
            let mut command = Command::new(&venv_python);
            command
                .arg("-c")
                .arg(PYTHON_PROGRESS_SMOKE)
                .arg(logging_order)
                .env("TMPDIR", &tool_tmp);
            run_command(
                &mut command,
                "smoke-test Python progress and logging isolation",
            )?;
        }
    }

    Ok(())
}

fn print_top_level_help() {
    println!(
        "Usage:\n  cargo xtask <COMMAND> [OPTIONS]\n\nCommands:\n  python-package-check    Build, install, and smoke-test the Python wheel\n  sqlserver-test          Run SQL Server integration tests\n\nRun `cargo xtask <COMMAND> --help` for command-specific options."
    );
}

fn print_sqlserver_test_help() {
    println!(
        "Usage:\n  cargo xtask sqlserver-test [OPTIONS]\n\nOptions:\n  --container-runtime <PATH>  Container runtime executable, such as docker or podman\n  --connection-string <URL>   Use an existing SQL Server instead of a local container\n  --image <IMAGE>             SQL Server container image\n  --database <NAME>           Test database name\n  --schema <NAME>             Test schema name [default: dbo]\n  --keep-container            Keep the container after the task exits\n  -h, --help                  Print help"
    );
}

fn print_python_package_check_help() {
    println!(
        "Usage:\n  cargo xtask python-package-check [OPTIONS]\n\nOptions:\n  --python <PATH>  Python executable used for venv and smoke checks [default: python3]\n  -h, --help       Print help"
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PythonPackageCheckOptions {
    python: PathBuf,
}

impl Default for PythonPackageCheckOptions {
    fn default() -> Self {
        Self {
            python: PathBuf::from("python3"),
        }
    }
}

impl PythonPackageCheckOptions {
    fn parse(args: &[OsString]) -> Result<Self, XtaskError> {
        let mut options = Self::default();
        let mut index = 0;

        while index < args.len() {
            let arg = args[index]
                .to_str()
                .ok_or_else(|| XtaskError::InvalidUtf8Argument(args[index].clone()))?;

            match arg {
                "-h" | "--help" => {
                    print_python_package_check_help();
                    return Ok(options);
                }
                "--python" => {
                    options.python = PathBuf::from(required_value(args, index)?);
                    index += 1;
                }
                other => return Err(XtaskError::UnknownOption(other.to_owned())),
            }

            index += 1;
        }

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

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn single_wheel_in(wheels_dir: &Path) -> Result<PathBuf, XtaskError> {
    let mut wheels = fs::read_dir(wheels_dir)
        .map_err(|source| XtaskError::ReadDir {
            path: wheels_dir.to_path_buf(),
            source,
        })?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("whl"))
        .collect::<Vec<_>>();
    wheels.sort();

    match wheels.as_slice() {
        [wheel] => Ok(wheel.clone()),
        [] => Err(XtaskError::WheelCount {
            dir: wheels_dir.to_path_buf(),
            count: 0,
        }),
        wheels => Err(XtaskError::WheelCount {
            dir: wheels_dir.to_path_buf(),
            count: wheels.len(),
        }),
    }
}

fn venv_python(venv_dir: &Path) -> PathBuf {
    if cfg!(windows) {
        venv_dir.join("Scripts").join("python.exe")
    } else {
        venv_dir.join("bin").join("python")
    }
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn create_in(parent: &Path, prefix: &str) -> Result<Self, XtaskError> {
        fs::create_dir_all(parent).map_err(|source| XtaskError::TempDir {
            path: parent.to_path_buf(),
            source,
        })?;
        let path = parent.join(format!("{prefix}-{}", unique_suffix()));
        fs::create_dir_all(&path).map_err(|source| XtaskError::TempDir {
            path: path.clone(),
            source,
        })?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!(
                "failed to clean up temp dir `{}`: {error}",
                self.path.display()
            );
        }
    }
}

fn unique_suffix() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{}-{millis}", std::process::id())
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
    ReadDir {
        path: PathBuf,
        source: std::io::Error,
    },
    TempDir {
        path: PathBuf,
        source: std::io::Error,
    },
    WheelCount {
        dir: PathBuf,
        count: usize,
    },
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
            Self::ReadDir { path, source } => {
                write!(f, "failed to read directory `{}`: {source}", path.display())
            }
            Self::TempDir { path, source } => {
                write!(
                    f,
                    "failed to create temp dir `{}`: {source}",
                    path.display()
                )
            }
            Self::WheelCount { dir, count } => {
                write!(
                    f,
                    "expected exactly one wheel in `{}`, found {count}",
                    dir.display()
                )
            }
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

const WHEEL_CONTENT_CHECK: &str = r#"
import email.parser
import sys
import zipfile
from pathlib import Path

required = {"deltafunnel/__init__.pyi", "deltafunnel/py.typed"}
wheel_path = Path(sys.argv[1])
expected_version = wheel_path.name.split("-")[1]

with zipfile.ZipFile(wheel_path) as wheel:
    names = set(wheel.namelist())
    metadata_files = [name for name in names if name.endswith(".dist-info/METADATA")]
    native_modules = [
        name
        for name in names
        if name.startswith("deltafunnel/") and name.endswith((".so", ".pyd"))
    ]

missing = sorted(required - names)
if missing:
    raise SystemExit("missing wheel entries: " + ", ".join(missing))
if len(metadata_files) != 1:
    raise SystemExit("expected exactly one METADATA file")
if not native_modules:
    raise SystemExit("missing native extension module")

with zipfile.ZipFile(wheel_path) as wheel:
    metadata = email.parser.Parser().parsestr(wheel.read(metadata_files[0]).decode())
if metadata["Name"] != "deltafunnel":
    raise SystemExit(f"unexpected package name: {metadata['Name']}")
if metadata["Version"] != expected_version:
    raise SystemExit(f"unexpected package version: {metadata['Version']}")
"#;

const PYTHON_PROGRESS_SMOKE: &str = r#"
import contextlib
import deltafunnel
import io
import os
import sys

session = deltafunnel.Session()
assert repr(session).startswith("deltafunnel.Session(")
table = session.table_from_sql("select 1 as id")
write_options = {
    "schema": "dbo",
    "table": "orders",
    "load_mode": "create_and_load",
    "dry_run": True,
    "connection_string": "server=tcp:sql.example.com;password=not-used",
}
environment = dict(os.environ)
logging_order = sys.argv[1]
if logging_order == "after":
    deltafunnel.init_logging()

automatic_output = io.StringIO()
with contextlib.redirect_stderr(automatic_output):
    report = table.write_to_mssql(**write_options)
assert report["run_mode"] == "dry_run"
assert automatic_output.getvalue() == ""

forced_output = io.StringIO()
with contextlib.redirect_stderr(forced_output):
    table.write_to_mssql(**write_options, progress=True)
assert "Completed" in forced_output.getvalue()
assert dict(os.environ) == environment
if logging_order == "before":
    deltafunnel.init_logging()
print(deltafunnel.__version__)
"#;

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, path::PathBuf};

    use super::{PythonPackageCheckOptions, SqlServerTestOptions, connection_string_with_database};

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
    fn parses_python_package_check_options() -> Result<(), String> {
        let args = [OsString::from("--python"), OsString::from("python")];

        let options = match PythonPackageCheckOptions::parse(&args) {
            Ok(options) => options,
            Err(error) => return Err(format!("expected options to parse: {error}")),
        };

        assert_eq!(options.python, PathBuf::from("python"));
        Ok(())
    }

    #[test]
    fn python_package_check_defaults_to_python3() {
        let options = PythonPackageCheckOptions::default();

        assert_eq!(options.python, PathBuf::from("python3"));
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
