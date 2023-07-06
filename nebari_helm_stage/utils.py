import logging
import os
import re
import signal
import subprocess
import sys
import threading

logger = logging.getLogger(__name__)


# copied from Nebari utils
def run_subprocess_cmd(processargs, **kwargs):
    """Runs subprocess command with realtime stdout logging with optional line prefix."""
    if "prefix" in kwargs:
        line_prefix = f"[{kwargs['prefix']}]: ".encode("utf-8")
        kwargs.pop("prefix")
    else:
        line_prefix = b""

    timeout = 0
    if "timeout" in kwargs:
        timeout = kwargs.pop("timeout")  # in seconds

    strip_errors = kwargs.pop("strip_errors", False)

    process = subprocess.Popen(
        processargs,
        **kwargs,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )
    # Set timeout thread
    timeout_timer = None
    if timeout > 0:

        def kill_process():
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass  # Already finished

        timeout_timer = threading.Timer(timeout, kill_process)
        timeout_timer.start()

    for line in iter(lambda: process.stdout.readline(), b""):
        full_line = line_prefix + line
        if strip_errors:
            full_line = full_line.decode("utf-8")
            full_line = re.sub(
                r"\x1b\[31m", "", full_line
            )  # Remove red ANSI escape code
            full_line = full_line.encode("utf-8")

        sys.stdout.buffer.write(full_line)
        sys.stdout.flush()

    if timeout_timer is not None:
        timeout_timer.cancel()

    return process.wait(
        timeout=10
    )  # Should already have finished because we have drained stdout
