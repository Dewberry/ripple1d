"""API Lofging module for Ripple1D."""

import inspect
import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone

from ripple1d.consts import SUPPRESS_LOGS
from ripple1d.ripple1d_logger import RippleLogFormatter

LOGS: dict[str, logging.Logger] = {}  # global that is modified by initialize_log()


def initialize_log(log_dir: str = "", log_level: int = logging.INFO) -> logging.Logger:
    """Initialize log with JSON-LD style formatting and throttled level for AWS libs.

    By default sends to StreamHandler (stdout/stderr), but can provide a filename to log to disk instead.
    """
    global LOGS

    filename = os.path.join(log_dir, f"{_get_log_filename_prefix()}-{_get_log_filename_suffix()}.jsonld")

    # If this log has already been initialized, just return it
    if filename in LOGS:
        return LOGS[filename]

    for module in SUPPRESS_LOGS:
        logging.getLogger(module).setLevel(logging.ERROR)

    log = logging.getLogger()
    log.setLevel(log_level)
    formatter = RippleLogFormatter()

    if filename:
        print(f"Initializing log file: {filename}")
        os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
        file_handler = logging.FileHandler(filename=filename)
        file_handler.setFormatter(formatter)
        log.addHandler(file_handler)
    else:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        log.addHandler(stream_handler)

    # Check again if the log has already been initialized, this time just before assigning the new log, to minimize
    # chance of race condition.
    if filename in LOGS:
        return LOGS[filename]

    LOGS[filename] = log
    return log


def _get_log_filename_prefix():
    """Return a string of the current UTC timestamp, to be used as the left-hand portion of the log file name."""
    return f"{datetime.now(tz=timezone.utc).isoformat().replace(':','-')}-ripple1d"


def _get_log_filename_suffix():
    stack_filenames = [frame.filename for frame in inspect.stack()]

    # Check for specific patterns in the stack filenames
    if any("huey" in filename for filename in stack_filenames):
        return "huey"
    elif any("flask" in filename for filename in stack_filenames):
        return "flask"
    else:
        raise ValueError(
            f"Could not determine if process invoked by huey or by flask. Stack filenames: {stack_filenames}"
        )
