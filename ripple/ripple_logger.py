import logging
from logging.handlers import RotatingFileHandler


def configure_logging(level, logfile: str = None, milliseconds: bool = False, verbose: str = False):
    if milliseconds:
        datefmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    else:
        datefmt = "%Y-%m-%dT%H:%M:%SZ"

    if level == logging.DEBUG and verbose:
        log_formatter = logging.Formatter(
            '{"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s", \
             "path": "%(pathname)s", "file": "%(filename)s", "module": "%(module)s", \
             "function": "%(funcName)s", "line": %(lineno)d}',
            datefmt=datefmt,
        )
    elif level == logging.DEBUG:
        log_formatter = logging.Formatter(
            '{"level": "%(levelname)s", "msg": "%(message)s", \
             "file": "%(filename)s", "line": %(lineno)d}',
            datefmt=datefmt,
        )
    else:
        log_formatter = logging.Formatter(
            '{"time": "%(asctime)s" , "level": "%(levelname)s", "msg": "%(message)s"}', datefmt=datefmt
        )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    handlers = [console_handler]

    if logfile:
        file_handler = RotatingFileHandler(logfile, maxBytes=5000000, backupCount=5)
        file_handler.setFormatter(log_formatter)
        handlers.append(file_handler)

    logging.basicConfig(
        level=level,
        datefmt=datefmt,
        handlers=handlers,
    )
