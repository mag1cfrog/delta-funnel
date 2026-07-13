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


delta_lake_signature = inspect.signature(deltafunnel.Session.delta_lake)
alias_signature = inspect.signature(deltafunnel.PendingDeltaSource.alias)
assert delta_lake_signature.parameters["progress"].kind is inspect.Parameter.KEYWORD_ONLY
assert delta_lake_signature.parameters["progress"].default is None
assert alias_signature.parameters["progress"].kind is inspect.Parameter.KEYWORD_ONLY
assert alias_signature.parameters["progress"].default is None


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

preview_stdout = io.StringIO()
preview_stderr = io.StringIO()
with contextlib.redirect_stdout(preview_stdout), contextlib.redirect_stderr(
    preview_stderr
):
    preview = table.preview(progress=True)
assert "| id |" in preview.text
assert preview_stdout.getvalue() == ""
assert "Completed" in preview_stderr.getvalue()

for automatic in ({}, {"progress": None}, {"progress": False}):
    automatic_stdout = io.StringIO()
    automatic_stderr = io.StringIO()
    with contextlib.redirect_stdout(automatic_stdout), contextlib.redirect_stderr(
        automatic_stderr
    ):
        automatic_preview = table.preview(**automatic)
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

    pending_session = deltafunnel.Session()
    pending_output = io.StringIO()
    with contextlib.redirect_stderr(pending_output):
        pending = pending_session.delta_lake(source_uri, progress=True)
    assert pending_output.getvalue() == ""
    assert "sources=[]" in repr(pending_session)

    alias_output = io.StringIO()
    with contextlib.redirect_stderr(alias_output):
        pending.alias("orders", progress=False)
    assert alias_output.getvalue() == ""
    assert 'sources=["orders"]' in repr(pending_session)

    alias_session = deltafunnel.Session()
    pending = alias_session.delta_lake(source_uri)
    alias_output = io.StringIO()
    with contextlib.redirect_stderr(alias_output):
        pending.alias("orders", progress=True)
    assert "Completed" in alias_output.getvalue()

    for automatic in ({}, {"progress": None}, {"progress": False}):
        automatic_output = io.StringIO()
        with contextlib.redirect_stderr(automatic_output):
            deltafunnel.Session().delta_lake(
                source_uri,
                name="orders",
                **automatic,
            )
        assert automatic_output.getvalue() == ""

assert dict(os.environ) == environment
if logging_order == "before":
    deltafunnel.init_logging()
print(deltafunnel.__version__)
