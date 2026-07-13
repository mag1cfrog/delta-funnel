import contextlib
import io
import os
import sys

import deltafunnel


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

show_stdout = io.StringIO()
show_stderr = io.StringIO()
with contextlib.redirect_stdout(show_stdout), contextlib.redirect_stderr(show_stderr):
    table.show(progress=True)
assert "| id |" in show_stdout.getvalue()
assert "Completed" not in show_stdout.getvalue()
assert "Completed" in show_stderr.getvalue()
assert dict(os.environ) == environment
if logging_order == "before":
    deltafunnel.init_logging()
print(deltafunnel.__version__)
