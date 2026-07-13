"""Install the small fake Rich modules used by Rust progress tests."""

import sys
import threading
import types


owner_thread = threading.get_ident()


def record(value):
    if threading.get_ident() == owner_thread:
        records.append(value)


def maybe_fail(call):
    if threading.get_ident() != owner_thread:
        return
    if fail_call == call:
        raise failure
    if stop_also_interrupts and call == "stop":
        raise stop_failure


class Console:
    def __init__(self, **kwargs):
        record({"call": "console", **kwargs})
        maybe_fail("console")
        self.stderr = kwargs.get("stderr", False)
        maybe_fail("stderr_console" if self.stderr else "stdout_console")
        stream_interactive = stderr_interactive if self.stderr else stdout_interactive
        self.is_interactive = stream_interactive or kwargs.get("force_interactive", False)
        self.is_jupyter = jupyter


class Progress:
    def __init__(self, *columns, **kwargs):
        console = kwargs.get("console")
        record(
            {
                "call": "progress",
                "columns": len(columns),
                "console_stderr": None if console is None else console.stderr,
                "auto_refresh": kwargs.get("auto_refresh"),
                "transient": kwargs.get("transient"),
                "redirect_stdout": kwargs.get("redirect_stdout"),
                "redirect_stderr": kwargs.get("redirect_stderr"),
            }
        )
        maybe_fail("progress")

    def add_task(self, description, **kwargs):
        record(
            {
                "call": "add_task",
                "description": description,
                "total": kwargs.get("total"),
            }
        )
        maybe_fail("add_task")
        return 7

    def start(self):
        record({"call": "start"})
        maybe_fail("start")

    def update(self, task_id, **kwargs):
        record({"call": "update", "task_id": task_id, **kwargs})
        description = kwargs.get("description", "")
        terminal = any(
            description.startswith(label)
            for label in ("Completed", "Completed with failures", "Failed", "Cancelled")
        )
        maybe_fail("terminal" if terminal else "update")

    def stop(self):
        record({"call": "stop"})
        maybe_fail("stop")


rich = types.ModuleType("rich")
rich.__path__ = []
console_module = types.ModuleType("rich.console")
console_module.Console = Console
progress_module = types.ModuleType("rich.progress")
progress_module.Progress = Progress
progress_module.TimeElapsedColumn = object
progress_module.BarColumn = object
progress_module.TaskProgressColumn = object
progress_module.TextColumn = lambda *args, **kwargs: object()
rich.console = console_module
rich.progress = progress_module
sys.modules["rich"] = rich
sys.modules["rich.console"] = console_module
sys.modules["rich.progress"] = progress_module
