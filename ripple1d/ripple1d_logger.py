"""Logging utility an setup."""

import inspect
import json
import logging
import time
import traceback
from logging.handlers import RotatingFileHandler

SUPPRESS_LOGS = ["boto3", "botocore", "geopandas", "fiona", "rasterio", "pyogrio"]
import inspect
import json
import logging
import traceback


def log_process(func):
    """Log time to run function (called by huey task)."""

    def wrapper(*args, **kwargs):
        if logging.getLogger().isEnabledFor(logging.INFO):
            start = time.time()
        result = func(*args, **kwargs)
        if logging.getLogger().isEnabledFor(logging.INFO):
            elapsed_time = time.time() - start
            logging.info(f"{kwargs.get('task_id')} | {func.__name__} | process time {elapsed_time:.2f} seconds")
        return result

    return wrapper


class RippleLogFormatter(logging.Formatter):
    """Format log messages as JSON-LD."""

    def __init__(
        self, datefmt="%Y-%m-%dT%H:%M:%SZ", include_traceback: bool = True, include_fields: list = None, **kwargs
    ):
        """Initialize the formatter with options."""
        super().__init__(datefmt=datefmt, **kwargs)
        self.include_traceback = include_traceback

        # List of valid field options
        self.include_fields_options = [
            "logger_name",
            "function_name",
            "line_number",
            "filename",
            "thread_name",
            "process_name",
            "process_id",
            "error",
            "traceback",
        ]

        # Validate and set include_fields
        if include_fields is None:
            self.include_fields = []
        else:
            self.include_fields = [field for field in include_fields if field in self.include_fields_options]

    def format(self, record):
        """Set up fine-grain format and logging options."""
        # Get the function name of the caller
        stack = inspect.stack()
        for frame_info in stack[1:]:
            if frame_info.function != "log":
                record.funcName = frame_info.function
                break

        # Basic log entry fields that are always included
        log_entry = {
            "@type": "RippleLogs",
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "msg": record.getMessage(),
        }

        # Conditionally add fields based on the include_fields list
        if "logger_name" in self.include_fields:
            log_entry["logger_name"] = record.name
        if "function_name" in self.include_fields:
            log_entry["function_name"] = record.funcName
        if "line_number" in self.include_fields:
            log_entry["line_number"] = record.lineno
        if "filename" in self.include_fields:
            log_entry["filename"] = record.pathname
        if "thread_name" in self.include_fields:
            log_entry["thread_name"] = record.threadName
        if "process_name" in self.include_fields:
            log_entry["process_name"] = record.processName
        if "process_id" in self.include_fields:
            log_entry["process_id"] = record.process
        if "error" in self.include_fields:
            log_entry["error"] = None
        if "traceback" in self.include_fields and record.exc_info and self.include_traceback:
            log_entry["error"] = str(record.exc_info[1])
            log_entry["traceback"] = traceback.format_exc()

        return json.dumps(log_entry)


def configure_logging(level, logfile: str = None, milliseconds: bool = False, verbose: bool = False):
    """Configure logging for ripple1d."""
    if milliseconds:
        datefmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    else:
        datefmt = "%Y-%m-%dT%H:%M:%SZ"

    if verbose:
        include_fields = [
            "logger_name",
            "function_name",
            "line_number",
            "filename",
            "thread_name",
            "process_name",
            "process_id",
        ]
    else:
        include_fields = []

    log_formatter = RippleLogFormatter(datefmt=datefmt, include_traceback=True, include_fields=include_fields)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    handlers = [console_handler]

    if logfile:
        file_handler = RotatingFileHandler(logfile, maxBytes=5000000, backupCount=5)
        file_handler.setFormatter(log_formatter)
        handlers.append(file_handler)

    for module in SUPPRESS_LOGS:
        logging.getLogger(module).setLevel(logging.WARNING)

    logging.basicConfig(
        level=level,
        handlers=handlers,
    )
