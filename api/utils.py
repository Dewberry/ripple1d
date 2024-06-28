from functools import wraps
import inspect
import logging
import traceback
import typing


def tracerbacker(func: typing.Callable) -> str:
    """Call the wrapped function and ignore its returned value.
    If the function raised an exception, log the Python traceback and return it as a string.
    If the function did not raise an exception, return an empty string."""

    @wraps(func)  # this ensures the func name does not change in huey
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except:
            tb = traceback.format_exc()
            logging.error(tb)
            return tb
        else:
            return ""

    return wrapper


def get_unexpected_and_missing_args(func: callable, kwargs_provided: set) -> tuple[list, list]:
    """Inspect func and evaluate whether the provided kwargs will satisfy it, considering its required & optional args.
    Return a list of unexpected kwargs (provided, but not called for),
    and return a list of missing kwargs (required but not provided).
    WARNING: assumes the func does not use *args or **kwargs features."""
    argspec = inspect.getfullargspec(func)
    unexpected = kwargs_provided - set(argspec.args)
    if argspec.defaults:
        required = set(argspec.args[: -len(argspec.defaults)])
    else:
        required = set(argspec.args)
    missing = required - kwargs_provided
    return sorted(unexpected), sorted(missing)
