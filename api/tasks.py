import os
import typing

from huey import SqliteHuey

# from huey.api import Result, Task, CancelExecution
from huey.signals import (
    SIGNAL_EXECUTING,
    SIGNAL_COMPLETE,
    SIGNAL_ERROR,
    SIGNAL_LOCKED,
    SIGNAL_INTERRUPTED,
    SIGNAL_CANCELED,
    SIGNAL_REVOKED,
)

from api.log import LOG_DIR, initialize_log
from api.utils import tracerbacker


LOG = initialize_log()

# huey = SqliteHuey(filename="huey.db", results=True, strict_fifo=True)
huey = SqliteHuey(filename=os.path.join(LOG_DIR, "huey.db"), results=True)
huey.storage.sql(
    'create table if not exists "executing" ("task_id" text not null, primary key("task_id"))', commit=True
)


@huey.task()
@tracerbacker
def process(func: callable, kwargs: dict = {}):
    """Generic huey task that executes the provided func with provided kwargs, asynchronously."""
    func(**kwargs)


def noop():
    """Function that does nothing except log a message. For ping endpoint and testing."""
    LOG.info("This message is from the noop function")
    pass


def status(task_id: str) -> str:
    """For given task ID, return its current status.
    WARNING assumes function executed by task is wrapped by `tracerbacker`"""
    if _is_executing(task_id):
        return "running"
    if huey.is_revoked(task_id):
        return "dismissed"
    if task_id in _all_queued():
        return "accepted"
    # if traceback string is non-empty, it failed. if empty, it succeeded. if None, task is not finished, or ID DNE.
    tb = result(task_id)
    if not (tb is None or isinstance(tb, str)):
        raise TypeError(
            f"For task={task_id}, expected result to be a string or None, but got type={type(tb)}, value={tb}"
        )
    if tb is None:
        return "notfound"
    elif tb == "":
        return "successful"
    else:
        return "failed"


def result(task_id: str) -> typing.Any:
    """For given task ID, return value returned by that task's function.
    WARNING: huey.result() returns None if the task ID does not exist, or the task is not finished"""
    return huey.result(task_id, preserve=True)


def revoke(task_id: str) -> None:
    """For given task ID, revoke the task, then fetch and return its result.
    WARNING: the result will be None if the task ID does not exist, or the task is not finished."""
    huey.revoke_by_id(task_id)
    return result(task_id)


def _all_queued() -> list[str]:
    """Return list of task IDs that have been submitted but not yet started executing"""
    return [task.id for task in huey.pending()]


def _all_executing() -> list[str]:
    """Return list of task IDs that are executing"""
    rows = huey.storage.sql('select "task_id" from "executing"', results=True)
    return [r[0] for r in rows]


def _is_executing(task_id) -> bool:
    """Return Boolean result of whether the provided task ID is executing"""
    expression = 'select exists (select 1 from "executing" where "task_id"=?)'
    return bool(huey.storage.sql(expression, (task_id,), results=True)[0][0])


@huey.signal(SIGNAL_EXECUTING)
def _task_signal_executing(signal, task):
    """When task emits a signal indicating it is starting, add its ID to the "executing" table"""
    huey.storage.sql('insert into "executing" ("task_id") values (?)', (task.id,), True)


@huey.signal(SIGNAL_COMPLETE)
def _task_signal_complete(signal, task):
    """When task emits a signal indicating it has finished, remove its ID from the "executing" table"""
    huey.storage.sql('delete from "executing" where "task_id"=?', (task.id,), True)


@huey.signal(SIGNAL_ERROR, SIGNAL_LOCKED, SIGNAL_INTERRUPTED, SIGNAL_CANCELED, SIGNAL_REVOKED)
def _task_signal_error(signal, task, exc=None):
    """When task emits various failed/canceled signals, remove its ID from the "executing" table"""
    huey.storage.sql('delete from "executing" where "task_id"=?', (task.id,), True)
