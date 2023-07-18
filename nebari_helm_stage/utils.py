import collections.abc
import logging
import os
import re
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Dict, Union
from urllib.parse import urlparse, urlunparse

from ruamel.yaml import YAML

logger = logging.getLogger(__name__)


def run_subprocess_cmd(processargs, suppress_output=False, **kwargs):
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

    output_lines = []

    for line in iter(lambda: process.stdout.readline(), b""):
        full_line = line_prefix + line
        if strip_errors:
            full_line = full_line.decode("utf-8")
            full_line = re.sub(
                r"\x1b\[31m", "", full_line
            )  # Remove red ANSI escape code
            full_line = full_line.encode("utf-8")

        output_lines.append(full_line.decode("utf-8"))

        if not suppress_output:
            sys.stdout.buffer.write(full_line)
            sys.stdout.flush()

    if timeout_timer is not None:
        timeout_timer.cancel()

    exit_code = process.wait(
        timeout=10
    )  # Should already have finished because we have drained stdout

    output_str = "".join(output_lines)

    return exit_code, output_str


def update_dict(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update_dict(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def update_yaml(overrides: Dict[str, Any], file_path: Union[str, Path]):
    yaml = YAML()

    with open(file_path, "r") as f:
        values = yaml.load(f)

    values = update_dict(values, overrides)

    with open(file_path, "w") as f:
        yaml.dump(values, f)


def populate_contents(
    base_directory: str,
    output_dir: str,
) -> Dict[str, str]:

    contents = {}

    for dirpath, dirnames, filenames in os.walk(base_directory):
        for filename in filenames:
            file_path = Path(dirpath) / filename
            try:
                with open(file_path, "r") as file:
                    file_content = file.read()

                final_path = str(
                    (
                        Path(output_dir) / file_path.relative_to(Path(base_directory))
                    ).absolute()
                )
                contents[final_path] = file_content
            except UnicodeDecodeError:
                logger.info(
                    f"{filename} is not a text file so it will not be included."
                )

    return contents
