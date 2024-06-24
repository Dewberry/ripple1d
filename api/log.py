
from __future__ import annotations

import logging
import os

LOGS: dict[str, logging.RootLogger] = {}

def initialize_log(filename: str = None) -> None:
    """Initialize log with json-style formatting and throttled level for AWS libs.
    By default sends to StreamHandler (stdout/stderr), but can provide a filename to log to disk instead."""
    global LOGS

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
