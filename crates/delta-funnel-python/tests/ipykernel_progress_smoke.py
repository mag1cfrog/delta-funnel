import os
import sys

from jupyter_client import KernelManager


def execute(client, code):
    message_id = client.execute(code)
    messages = []
    while True:
        message = client.get_iopub_msg(timeout=30)
        if message["parent_header"].get("msg_id") != message_id:
            continue
        messages.append(message)
        if (
            message["msg_type"] == "status"
            and message["content"]["execution_state"] == "idle"
        ):
            return messages


manager = KernelManager(kernel_name=sys.argv[1])
client = None
try:
    manager.start_kernel()
    client = manager.client()
    client.start_channels()
    client.wait_for_ready(timeout=30)
    expected_executable = os.path.realpath(sys.executable)
    action_messages = execute(
        client,
        f"""
import deltafunnel
import os
import sys

assert os.path.realpath(sys.executable) == {expected_executable!r}
table = deltafunnel.Session().table_from_sql("select 1 as id")
try:
    table.write_to_mssql(
        schema="dbo",
        table="orders",
        load_mode="append_existing",
    )
except deltafunnel.DeltaFunnelError as error:
    assert error.kind == "missing_mssql_connection"
else:
    raise AssertionError("write unexpectedly succeeded")
print("ACTION_DONE")
""",
    )
    display_indexes = [
        index
        for index, message in enumerate(action_messages)
        if message["msg_type"] in {"display_data", "update_display_data"}
    ]
    action_done_index = next(
        index
        for index, message in enumerate(action_messages)
        if message["msg_type"] == "stream"
        and "ACTION_DONE" in message["content"]["text"]
    )
    assert display_indexes
    assert max(display_indexes) < action_done_index

    write_all_messages = execute(
        client,
        """
import deltafunnel

session = deltafunnel.Session()
outputs = [
    session.table_from_sql("select 1 as id").to_mssql(
        schema="dbo",
        table="west",
        load_mode="create_and_load",
        connection_string="server=tcp:sql.example.com;password=not-used",
    ),
    session.table_from_sql("select 2 as id").to_mssql(
        schema="dbo",
        table="east",
        load_mode="create_and_load",
        connection_string="server=tcp:sql.example.com;password=not-used",
    ),
]
report = session.write_all(outputs, dry_run=True, progress=True)
assert report["run_mode"] == "dry_run"
assert report["output_count"] == 2
print("WRITE_ALL_DONE")
""",
    )
    write_all_progress_indexes = [
        index
        for index, message in enumerate(write_all_messages)
        if message["msg_type"] in {"display_data", "update_display_data"}
    ]
    write_all_progress = [
        write_all_messages[index] for index in write_all_progress_indexes
    ]
    widget_views = [
        message
        for message in write_all_progress
        if "application/vnd.jupyter.widget-view+json"
        in message["content"].get("data", {})
    ]
    write_all_done_index = next(
        index
        for index, message in enumerate(write_all_messages)
        if message["msg_type"] == "stream"
        and "WRITE_ALL_DONE" in message["content"]["text"]
    )
    assert write_all_progress
    assert len(widget_views) == 1
    rendered_progress = "\n".join(
        value
        for message in write_all_progress
        for value in message["content"].get("data", {}).values()
        if isinstance(value, str)
    )
    assert "Output 1/2 - Planning output: west" in rendered_progress
    assert "Output 2/2 - Planning output: east" in rendered_progress
    assert "Completed" in rendered_progress
    assert max(write_all_progress_indexes) < write_all_done_index

    preview_messages = execute(
        client,
        """
import deltafunnel

table = deltafunnel.Session().table_from_sql("select 1 as id")
table.preview(progress=True)
""",
    )
    progress_indexes = [
        index
        for index, message in enumerate(preview_messages)
        if message["msg_type"] in {"display_data", "update_display_data"}
        and "deltafunnel-preview" not in message["content"].get("data", {}).get("text/html", "")
    ]
    preview_index = next(
        index
        for index, message in enumerate(preview_messages)
        if "deltafunnel-preview"
        in message["content"].get("data", {}).get("text/html", "")
    )
    assert progress_indexes
    assert max(progress_indexes) < preview_index

    show_messages = execute(
        client,
        """
import deltafunnel

table = deltafunnel.Session().table_from_sql("select 1 as id")
table.show(progress=True)
print("SHOW_DONE")
""",
    )
    show_progress_indexes = [
        index
        for index, message in enumerate(show_messages)
        if message["msg_type"] in {"display_data", "update_display_data"}
    ]
    show_table_index = next(
        index
        for index, message in enumerate(show_messages)
        if message["msg_type"] == "stream" and "| id |" in message["content"]["text"]
    )
    assert show_progress_indexes
    assert max(show_progress_indexes) < show_table_index

    registration_messages = execute(
        client,
        """
import deltafunnel
import tempfile

with tempfile.TemporaryDirectory() as table:
    try:
        deltafunnel.Session().delta_lake(table, name="orders")
    except deltafunnel.DeltaFunnelError as error:
        assert error.kind == "delta_snapshot_load"
    else:
        raise AssertionError("registration unexpectedly succeeded")
print("REGISTRATION_DONE")
""",
    )
    registration_progress_indexes = [
        index
        for index, message in enumerate(registration_messages)
        if message["msg_type"] in {"display_data", "update_display_data"}
    ]
    registration_done_index = next(
        index
        for index, message in enumerate(registration_messages)
        if message["msg_type"] == "stream"
        and "REGISTRATION_DONE" in message["content"]["text"]
    )
    assert registration_progress_indexes
    assert max(registration_progress_indexes) < registration_done_index

    pending_messages = execute(
        client,
        """
import deltafunnel

session = deltafunnel.Session()
pending = session.delta_lake(
    "file:///definitely-missing-delta-funnel-437",
    progress=True,
)
assert "PendingDeltaSource" in repr(pending)
assert "sources=[]" in repr(session)
print("PENDING_DONE")
""",
    )
    assert not any(
        message["msg_type"] in {"display_data", "update_display_data"}
        for message in pending_messages
    )
    assert any(
        message["msg_type"] == "stream"
        and "PENDING_DONE" in message["content"]["text"]
        for message in pending_messages
    )

    sentinel_messages = execute(client, 'print("SENTINEL")')
    assert not any(
        message["msg_type"] in {"display_data", "update_display_data"}
        for message in sentinel_messages
    )
    assert any(
        message["msg_type"] == "stream"
        and "SENTINEL" in message["content"]["text"]
        for message in sentinel_messages
    )
finally:
    try:
        if client is not None:
            client.stop_channels()
    finally:
        if manager.has_kernel:
            manager.shutdown_kernel(now=True)
