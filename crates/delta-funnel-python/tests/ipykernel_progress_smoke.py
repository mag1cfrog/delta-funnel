import os
import socket
import sys
import tempfile

from jupyter_client import KernelManager
from progress_fixture import delta_table_uri


DISPLAY_MESSAGE_TYPES = {"display_data", "update_display_data"}


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


def display_messages(messages):
    return [
        message for message in messages if message["msg_type"] in DISPLAY_MESSAGE_TYPES
    ]


def rendered_text(messages):
    return "\n".join(
        value
        for message in messages
        for value in message["content"].get("data", {}).values()
        if isinstance(value, str)
    )


def display_payload(messages):
    return repr([message["content"].get("data", {}) for message in messages])


manager = KernelManager(kernel_name=sys.argv[1])
fixture = tempfile.TemporaryDirectory()
# Reserve a local port without listening so SQL connection failure is deterministic.
unreachable_socket = socket.socket()
unreachable_socket.bind(("127.0.0.1", 0))
unreachable_port = unreachable_socket.getsockname()[1]
client = None
try:
    source_uri = delta_table_uri(fixture.name)
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
        if message["msg_type"] in DISPLAY_MESSAGE_TYPES
    ]
    action_done_index = next(
        index
        for index, message in enumerate(action_messages)
        if message["msg_type"] == "stream"
        and "ACTION_DONE" in message["content"]["text"]
    )
    assert display_indexes
    action_displays = display_messages(action_messages)
    action_progress = rendered_text(action_displays)
    assert "Failed" in action_progress
    assert "missing_mssql_connection" not in display_payload(action_displays)
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
        if message["msg_type"] in DISPLAY_MESSAGE_TYPES
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
    rendered_progress = rendered_text(write_all_progress)
    assert "Output 1/2 - Planning output: west" in rendered_progress
    assert "Output 2/2 - Planning output: east" in rendered_progress
    assert "Completed" in rendered_progress
    write_all_payload = display_payload(write_all_progress)
    assert "not-used" not in write_all_payload
    assert "sql.example.com" not in write_all_payload
    assert max(write_all_progress_indexes) < write_all_done_index

    partial_failure_messages = execute(
        client,
        f"""
import deltafunnel

session = deltafunnel.Session()
output = session.table_from_sql("select 1 as id").to_mssql(
    schema="dbo",
    table="unreachable_output",
    load_mode="create_and_load",
    connection_string=(
        "server=tcp:127.0.0.1,{unreachable_port};user id=sa;password=kernel-secret;"
        "TrustServerCertificate=true"
    ),
)
report = session.write_all([output], progress=True)
assert not report["all_succeeded"]
assert report["failed_count"] == 1
print("PARTIAL_FAILURE_DONE")
""",
    )
    partial_failure_progress = display_messages(partial_failure_messages)
    partial_failure_text = rendered_text(partial_failure_progress)
    assert "Completed with failures" in partial_failure_text
    partial_failure_payload = display_payload(partial_failure_progress)
    assert "kernel-secret" not in partial_failure_payload
    assert "127.0.0.1" not in partial_failure_payload
    assert "connection refused" not in partial_failure_payload.lower()
    assert any(
        message["msg_type"] == "stream"
        and "PARTIAL_FAILURE_DONE" in message["content"]["text"]
        for message in partial_failure_messages
    )

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
        if message["msg_type"] in DISPLAY_MESSAGE_TYPES
        and "deltafunnel-preview" not in message["content"].get("data", {}).get("text/html", "")
    ]
    preview_index = next(
        index
        for index, message in enumerate(preview_messages)
        if "deltafunnel-preview"
        in message["content"].get("data", {}).get("text/html", "")
    )
    assert progress_indexes
    assert "Completed" in rendered_text(
        [preview_messages[index] for index in progress_indexes]
    )
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
        if message["msg_type"] in DISPLAY_MESSAGE_TYPES
    ]
    show_table_index = next(
        index
        for index, message in enumerate(show_messages)
        if message["msg_type"] == "stream" and "| id |" in message["content"]["text"]
    )
    assert show_progress_indexes
    assert "Completed" in rendered_text(
        [show_messages[index] for index in show_progress_indexes]
    )
    assert max(show_progress_indexes) < show_table_index

    disabled_messages = execute(
        client,
        """
import deltafunnel

table = deltafunnel.Session().table_from_sql("select 1 as id")
preview = table.preview(progress=False)
assert "| id |" in preview.text
print("DISABLED_DONE")
""",
    )
    assert not display_messages(disabled_messages)
    assert any(
        message["msg_type"] == "stream"
        and "DISABLED_DONE" in message["content"]["text"]
        for message in disabled_messages
    )

    registration_messages = execute(
        client,
        """
import deltafunnel
import tempfile

with tempfile.TemporaryDirectory(prefix="delta-funnel-registration-secret-") as table:
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
        if message["msg_type"] in DISPLAY_MESSAGE_TYPES
    ]
    registration_done_index = next(
        index
        for index, message in enumerate(registration_messages)
        if message["msg_type"] == "stream"
        and "REGISTRATION_DONE" in message["content"]["text"]
    )
    assert registration_progress_indexes
    registration_progress = rendered_text(
        [registration_messages[index] for index in registration_progress_indexes]
    )
    assert "Failed" in registration_progress
    registration_payload = display_payload(
        [registration_messages[index] for index in registration_progress_indexes]
    )
    assert "delta-funnel-registration-secret" not in registration_payload
    assert "delta_snapshot_load" not in registration_payload
    assert max(registration_progress_indexes) < registration_done_index

    pending_messages = execute(
        client,
        """
import deltafunnel

session = deltafunnel.Session()
pending = session.delta_lake(
    "file:///definitely-missing-delta-funnel-438",
    progress=True,
)
assert "PendingDeltaSource" in repr(pending)
assert "sources=[]" in repr(session)
print("PENDING_DONE")
""",
    )
    assert not any(
        message["msg_type"] in DISPLAY_MESSAGE_TYPES
        for message in pending_messages
    )
    assert any(
        message["msg_type"] == "stream"
        and "PENDING_DONE" in message["content"]["text"]
        for message in pending_messages
    )

    registration_equivalence_messages = execute(
        client,
        f"""
import deltafunnel
from rich.progress import Progress

descriptions = []
original_update = Progress.update

def recording_update(self, *args, **kwargs):
    descriptions.append(kwargs.get("description"))
    return original_update(self, *args, **kwargs)

Progress.update = recording_update
try:
    named_session = deltafunnel.Session()
    named_session.delta_lake({source_uri!r}, name="orders", progress=True)
    named_descriptions = descriptions.copy()
    descriptions.clear()

    alias_session = deltafunnel.Session()
    pending = alias_session.delta_lake({source_uri!r}, progress=True)
    assert not descriptions
    pending.alias("orders", progress=True)
    alias_descriptions = descriptions.copy()
finally:
    Progress.update = original_update

assert named_descriptions == alias_descriptions
assert named_descriptions == [
    "Loading Delta metadata",
    "Validating Delta protocol",
    "Preparing Delta provider",
    "Registering Delta source",
    "Completed",
]
print("REGISTRATION_EQUIVALENT")
""",
    )
    registration_equivalence_progress = display_messages(
        registration_equivalence_messages
    )
    assert len(
        [
            message
            for message in registration_equivalence_progress
            if "application/vnd.jupyter.widget-view+json"
            in message["content"].get("data", {})
        ]
    ) == 2
    assert rendered_text(registration_equivalence_progress).count("Completed") >= 2
    assert source_uri not in display_payload(registration_equivalence_progress)
    assert any(
        message["msg_type"] == "stream"
        and "REGISTRATION_EQUIVALENT" in message["content"]["text"]
        for message in registration_equivalence_messages
    )

    interruption_messages = execute(
        client,
        """
import deltafunnel
import sys
from rich.progress import Progress

original_update = Progress.update

def interrupt_once(self, *args, **kwargs):
    Progress.update = original_update
    raise KeyboardInterrupt("renderer interrupted")

Progress.update = interrupt_once
try:
    table = deltafunnel.Session().table_from_sql("select 1 as id")
    table.preview(progress=True)
except KeyboardInterrupt as error:
    assert error.deltafunnel_operation_status == "completed"
else:
    raise AssertionError("renderer interruption was not raised")
finally:
    Progress.update = original_update
print("INTERRUPTION_DONE", file=sys.stderr)
""",
    )
    interruption_stderr = "".join(
        message["content"]["text"]
        for message in interruption_messages
        if message["msg_type"] == "stream"
        and message["content"].get("name") == "stderr"
    )
    assert "DeltaFunnel action status: completed" in interruption_stderr
    assert "INTERRUPTION_DONE" in interruption_stderr
    assert not any(
        message["msg_type"] == "error" for message in interruption_messages
    )

    sentinel_messages = execute(client, 'print("SENTINEL")')
    assert not any(
        message["msg_type"] in DISPLAY_MESSAGE_TYPES
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
        unreachable_socket.close()
        fixture.cleanup()
