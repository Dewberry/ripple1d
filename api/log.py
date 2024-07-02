"""Logging utilities supporting the huey + Flask REST API."""

from datetime import datetime, timezone
import inspect
import logging
import os

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
LOGS: dict[str, logging.RootLogger] = {}  # global that is modified by initialize_log()


def initialize_log() -> None:
    """Initialize log with json-style formatting and throttled level for AWS libs.
    By default sends to StreamHandler (stdout/stderr), but can provide a filename to log to disk instead."""
    global LOGS

    filename = os.path.join(LOG_DIR, f"{_get_log_filename_prefix()}-{_get_log_filename_suffix()}.log")

    # If this log has already been initialized, just return it
    if filename in LOGS:
        return LOGS[filename]

    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)

    log = logging.getLogger()
    log.setLevel(logging.INFO)
    formatter = logging.Formatter('{"time":"%(asctime)s", "level": "%(levelname)s", "message":%(message)s}')

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
    return f"{datetime.now(tz=timezone.utc).isoformat().replace(':','-')}-ripple"


def _get_log_filename_suffix():
    """Return a string indicating whether this function is being called from a huey consumer or from a Flask instance.
    The string is to be used as the right-hand portino of the log file name."""
    stack_filenames = [frame.filename for frame in inspect.stack()]
    if stack_filenames[-1].endswith("huey_consumer.py"):
        return "huey"
    for fn in stack_filenames:
        if "flask.exe" in fn:
            return "flask"
    raise ValueError(f"Could not determine if process invoked by huey or by flask. Stack filenames: {stack_filenames}")
