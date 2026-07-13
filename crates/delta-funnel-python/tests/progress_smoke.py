import ast
import base64
import contextlib
import importlib.util
import inspect
import io
import json
import os
import sys
import tempfile
from pathlib import Path

import deltafunnel


for optional_module in ["IPython", "ipykernel", "ipywidgets", "jupyter_client"]:
    assert importlib.util.find_spec(optional_module) is None


def delta_table_uri(root):
    table = Path(root, "orders")
    log = table / "_delta_log"
    log.mkdir(parents=True)
    schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": False, "metadata": {}}
        ],
    }
    protocol = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
    metadata = {
        "metaData": {
            "id": "delta-funnel-progress-smoke",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps(schema),
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495,
        }
    }
    parquet = base64.b64decode(
        Path(__file__).with_name("progress_smoke.parquet.b64").read_text()
    )
    data_files = ["part-00000.parquet", "part-00001.parquet"]
    for data_file in data_files:
        (table / data_file).write_bytes(parquet)
    adds = [
        {
            "add": {
                "path": data_file,
                "partitionValues": {},
                "size": len(parquet),
                "modificationTime": 1587968586000,
                "dataChange": True,
            }
        }
        for data_file in data_files
    ]
    (log / "00000000000000000000.json").write_text(
        f"{json.dumps(protocol)}\n{json.dumps(metadata)}\n"
    )
    (log / "00000000000000000001.json").write_text(
        "".join(f"{json.dumps(add)}\n" for add in adds)
    )
    return table.resolve().as_uri()


def capture_stderr(action, *args, **kwargs):
    output = io.StringIO()
    with contextlib.redirect_stderr(output):
        result = action(*args, **kwargs)
    return result, output.getvalue()


def capture_deltafunnel_error(action, *args, **kwargs):
    output = io.StringIO()
    with contextlib.redirect_stderr(output):
        try:
            action(*args, **kwargs)
        except deltafunnel.DeltaFunnelError as error:
            return error, output.getvalue()
    raise AssertionError("DeltaFunnelError was not raised")


for method in [
    deltafunnel.Session.delta_lake,
    deltafunnel.PendingDeltaSource.alias,
    deltafunnel.Table.preview,
    deltafunnel.Table.show,
    deltafunnel.Table.write_to_mssql,
    deltafunnel.Session.write_all,
]:
    progress_parameter = inspect.signature(method).parameters["progress"]
    assert progress_parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert progress_parameter.default is None


stub = ast.parse(Path(deltafunnel.__file__).with_name("__init__.pyi").read_text())
expected_stub_methods = {
    "Session": {"delta_lake", "write_all"},
    "PendingDeltaSource": {"alias"},
    "Table": {"preview", "show", "write_to_mssql"},
}
checked_stub_methods = set()
for class_definition in [node for node in stub.body if isinstance(node, ast.ClassDef)]:
    expected_methods = expected_stub_methods.get(class_definition.name, set())
    for method in [
        node
        for node in class_definition.body
        if isinstance(node, ast.FunctionDef) and node.name in expected_methods
    ]:
        keyword_defaults = dict(
            zip(method.args.kwonlyargs, method.args.kw_defaults, strict=True)
        )
        progress_argument, progress_default = next(
            (argument, default)
            for argument, default in keyword_defaults.items()
            if argument.arg == "progress"
        )
        assert ast.unparse(progress_argument.annotation) == "bool | None"
        assert isinstance(progress_default, ast.Constant)
        assert progress_default.value is None
        checked_stub_methods.add((class_definition.name, method.name))

assert checked_stub_methods == {
    (class_name, method)
    for class_name, methods in expected_stub_methods.items()
    for method in methods
}


QUIET_PROGRESS_MODES = ({}, {"progress": None}, {"progress": False})


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

for progress_mode in QUIET_PROGRESS_MODES:
    report, output = capture_stderr(
        table.write_to_mssql, **write_options, **progress_mode
    )
    assert report["run_mode"] == "dry_run"
    assert output == ""

_, forced_output = capture_stderr(table.write_to_mssql, **write_options, progress=True)
assert "Completed" in forced_output

execute_options = {
    "schema": "dbo",
    "table": "orders",
    "load_mode": "append_existing",
}
for progress_mode in QUIET_PROGRESS_MODES:
    error, output = capture_deltafunnel_error(
        table.write_to_mssql, **execute_options, **progress_mode
    )
    assert error.kind == "missing_mssql_connection"
    assert output == ""

error, forced_output = capture_deltafunnel_error(
    table.write_to_mssql, **execute_options, progress=True
)
assert error.kind == "missing_mssql_connection"
assert "Failed" in forced_output

dry_run_output = table.to_mssql(
    schema="dbo",
    table="orders",
    load_mode="create_and_load",
    connection_string="server=tcp:sql.example.com;password=not-used",
)
for progress_mode in QUIET_PROGRESS_MODES:
    report, output = capture_stderr(
        session.write_all, [dry_run_output], dry_run=True, **progress_mode
    )
    assert report["run_mode"] == "dry_run"
    assert output == ""

_, forced_output = capture_stderr(
    session.write_all, [dry_run_output], dry_run=True, progress=True
)
assert "Completed" in forced_output

execute_output = table.to_mssql(
    schema="dbo",
    table="orders",
    load_mode="append_existing",
)
for progress_mode in QUIET_PROGRESS_MODES:
    error, output = capture_deltafunnel_error(
        session.write_all, [execute_output], **progress_mode
    )
    assert error.kind == "missing_mssql_connection"
    assert output == ""

error, forced_output = capture_deltafunnel_error(
    session.write_all, [execute_output], progress=True
)
assert error.kind == "missing_mssql_connection"
assert "Failed" in forced_output

preview_stdout = io.StringIO()
preview_stderr = io.StringIO()
with contextlib.redirect_stdout(preview_stdout), contextlib.redirect_stderr(
    preview_stderr
):
    preview = table.preview(progress=True)
assert "| id |" in preview.text
assert preview_stdout.getvalue() == ""
assert "Completed" in preview_stderr.getvalue()

for progress_mode in QUIET_PROGRESS_MODES:
    automatic_stdout = io.StringIO()
    automatic_stderr = io.StringIO()
    with contextlib.redirect_stdout(automatic_stdout), contextlib.redirect_stderr(
        automatic_stderr
    ):
        automatic_preview = table.preview(**progress_mode)
    assert "| id |" in automatic_preview.text
    assert automatic_stdout.getvalue() == ""
    assert automatic_stderr.getvalue() == ""

show_stdout = io.StringIO()
show_stderr = io.StringIO()
with contextlib.redirect_stdout(show_stdout), contextlib.redirect_stderr(show_stderr):
    table.show(progress=True)
assert "| id |" in show_stdout.getvalue()
assert "Completed" not in show_stdout.getvalue()
assert "Completed" in show_stderr.getvalue()

for progress_mode in QUIET_PROGRESS_MODES:
    quiet_stdout = io.StringIO()
    quiet_stderr = io.StringIO()
    with contextlib.redirect_stdout(quiet_stdout), contextlib.redirect_stderr(
        quiet_stderr
    ):
        table.show(**progress_mode)
    assert "| id |" in quiet_stdout.getvalue()
    assert quiet_stderr.getvalue() == ""

with tempfile.TemporaryDirectory() as temp_dir:
    source_uri = delta_table_uri(temp_dir)

    registration_output = io.StringIO()
    with contextlib.redirect_stderr(registration_output):
        orders = deltafunnel.Session().delta_lake(
            source_uri,
            name="orders",
            progress=True,
        )
    registration_text = registration_output.getvalue()
    assert "Completed" in registration_text
    assert source_uri not in registration_text

    preview, preview_output = capture_stderr(orders.preview, progress=True)
    assert "| id |" in preview.text
    assert "Delta files 2/2" in preview_output

    for progress_mode in QUIET_PROGRESS_MODES:
        pending_session = deltafunnel.Session()
        pending, pending_output = capture_stderr(
            pending_session.delta_lake, source_uri, progress=True
        )
        assert pending_output == ""
        assert "sources=[]" in repr(pending_session)
        _, alias_output = capture_stderr(
            pending.alias, "orders", **progress_mode
        )
        assert alias_output == ""
        assert 'sources=["orders"]' in repr(pending_session)

    alias_session = deltafunnel.Session()
    pending = alias_session.delta_lake(source_uri)
    alias_output = io.StringIO()
    with contextlib.redirect_stderr(alias_output):
        pending.alias("orders", progress=True)
    assert "Completed" in alias_output.getvalue()

    for progress_mode in QUIET_PROGRESS_MODES:
        automatic_output = io.StringIO()
        with contextlib.redirect_stderr(automatic_output):
            deltafunnel.Session().delta_lake(
                source_uri,
                name="orders",
                **progress_mode,
            )
        assert automatic_output.getvalue() == ""

assert dict(os.environ) == environment
if logging_order == "before":
    deltafunnel.init_logging()
print(deltafunnel.__version__)
