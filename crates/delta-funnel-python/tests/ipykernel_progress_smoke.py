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
