import contextlib
import inspect
import io
import json
import os
import sys
import tempfile
from pathlib import Path

import deltafunnel


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
    add = {
        "add": {
            "path": "part-00000.parquet",
            "partitionValues": {},
            "size": 0,
            "modificationTime": 1587968586000,
            "dataChange": True,
        }
    }
    (log / "00000000000000000000.json").write_text(
        f"{json.dumps(protocol)}\n{json.dumps(metadata)}\n"
    )
    (log / "00000000000000000001.json").write_text(f"{json.dumps(add)}\n")
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
        deltafunnel.Session().delta_lake(
            source_uri,
            name="orders",
            progress=True,
        )
    registration_text = registration_output.getvalue()
    assert "Completed" in registration_text
    assert source_uri not in registration_text

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
