"""huey instance for huey + Flask REST API (to be called by huey_consumer.py)."""

import json
import logging
import os
import time
import typing

from huey import SqliteHuey, signals
from huey.api import Result

from ripple1d.api.utils import tracerbacker
from ripple1d.ripple1d_logger import initialize_log

initialize_log()

huey = SqliteHuey(filename=os.path.join("huey.db"), results=True)

# Create custom table task_status
huey.storage.sql(
    """
    create table if not exists "task_status" (
        "task_id" text not null,
        "func_name" text not null,
        "func_kwargs" text not null,
        "huey_status" text not null,
        "ogc_status" text not null,
        "accept_time" timestamptz not null,
        "dismiss_time" timestamptz,
        "start_time" timestamptz,
        "finish_time" timestamptz,
        "status_time" timestamptz,
        "finish_duration_minutes" real,
        primary key("task_id")
    );
    """,
    commit=True,
)

# Create custom triggers to keep status_time field updated
huey.storage.sql(
    """
    create trigger if not exists "trg_task_status_insert" after insert on "task_status"
    begin
        update "task_status" set
            "status_time" = max(
            ifnull("accept_time", datetime(0)),
            ifnull("dismiss_time", datetime(0)),
            ifnull("start_time", datetime(0)),
            ifnull("finish_time", datetime(0))
        ),
            "finish_duration_minutes" = 1440.0 * (julianday("finish_time") - julianday("start_time"))
        where "task_id" = NEW."task_id";
    end;
    """,
    commit=True,
)
huey.storage.sql(
    """
    create trigger if not exists "trg_task_status_update" after update on "task_status"
    begin
        update "task_status" set
            "status_time" = max(
            ifnull("accept_time", datetime(0)),
            ifnull("dismiss_time", datetime(0)),
            ifnull("start_time", datetime(0)),
            ifnull("finish_time", datetime(0))
        ),
            "finish_duration_minutes" = 1440.0 * (julianday("finish_time") - julianday("start_time"))
        where "task_id" = NEW."task_id";
    end;
    """,
    commit=True,
)


def create_and_enqueue_task(func: typing.Callable, kwargs: dict = {}) -> Result:
    """Create a task instance, then add it to the custom table "task_status" before adding it to the queue.

    Return the Result object associated with the task, which is the same class type that would be returned by
    calling the @huey.task()-decorated function itself (in this case, `_process`).

    This is needed to avoid a race condition, this ensures that the task exists in the custom table before
    it starts executing.
    """
    task_instance = _process.s(func, kwargs)
    huey.storage.sql(
        """
    insert into "task_status"
        ("task_id", "func_name", "func_kwargs", "huey_status", "ogc_status", "accept_time")
    values
        (?, ?, ?, 'queued', 'accepted', datetime('now'))""",
        (task_instance.id, func.__name__, json.dumps(kwargs)),
        True,
    )
    return huey.enqueue(task_instance)


def revoke_task(task_id: str):
    """Revoke a task."""
    huey.revoke_by_id(task_id)
    expression = f"""
    update "task_status"
    set
        "ogc_status" = 'dismissed',
        "dismiss_time" = datetime('now')
    where "task_id" = ?
    """
    args = (task_id,)
    huey.storage.sql(expression, args, True)


def update_p_id(task_id: str, p_id: str):
    """Update p_id."""
    expression = f"""
    update "task_status"
    set
        "p_id" = '{p_id}'
    where "task_id" = ?
    """
    args = (task_id,)
    huey.storage.sql(expression, args, True)


def subprocess_caller(func: str, args: dict):
    """Call the specified function through a subprocess."""
    subprocess_args = [
        sys.executable,
        os.path.dirname(__file__).replace("api", "ops/endpoints.py"),
        func,
        json.dumps(args),
    ]
    r = subprocess.Popen(subprocess_args)
    update_p_id(args.get("task_id"), r.pid)

    r.wait()


def task_status(only_task_id: str | None) -> dict[str, dict]:
    """Return dictionary of tasks, where key is task ID and value is subdict (fields related to each task).

    If only_task_id is None, all tasks will be returned.
    If only_task_id is *not* None, only the provided task will be returned.
    """
    expression = """
        select
            "task_id",
            "func_name",
            "func_kwargs",
            "huey_status",
            "ogc_status",
            "accept_time",
            "dismiss_time",
            "start_time",
            "finish_time",
            "status_time",
            "finish_duration_minutes"
        from "task_status"
        order by "status_time" desc
        """
    results_raw = huey.storage.sql(expression, results=True)
    results_dict = {}
    for (
        task_id,
        func_name,
        func_kwargs,
        huey_status,
        ogc_status,
        accept_time,
        dismiss_time,
        start_time,
        finish_time,
        status_time,
        finish_duration_minutes,
    ) in results_raw:
        if only_task_id is not None and task_id != only_task_id:
            continue

        results_dict[task_id] = {
            "huey_status": huey_status,
            "func_name": func_name,
            "func_kwargs": func_kwargs,
            "ogc_status": ogc_status,
            "accept_time": accept_time,
            "dismiss_time": dismiss_time,
            "start_time": start_time,
            "finish_time": finish_time,
            "status_time": status_time,
            "finish_duration_minutes": finish_duration_minutes,
        }
    return results_dict


def huey_status(task_id: str) -> str:
    """For given task ID, return its current status in huey terms."""
    expression = """select "huey_status" from "task_status" where "task_id" = ?"""
    results = huey.storage.sql(expression, (task_id,), results=True)
    if not results:
        return "notfound"
    return results[0][0]


def ogc_status(task_id: str) -> str:
    """For given task ID, return its current status in OGC API terms.

    WARNING assumes function executed by task is wrapped by `tracerbacker`
    """
    expression = """select "ogc_status" from "task_status" where "task_id" = ?"""
    results = huey.storage.sql(expression, (task_id,), results=True)
    if not results:
        return "notfound"
    return results[0][0]


def noop(task_id: str = None):
    """Do nothing except log a message. For ping endpoint and testing."""
    logging.info(f"{task_id} | noop")
    pass


def sleep_some(sleep_time: int = 15, task_id: str = None):
    """Do nothing except log a message and then sleep for a while.  For testing."""
    logging.info(f"{task_id} | sleep_some")
    for i in range(0, sleep_time):
        time.sleep(1)
    return f"Slept for {sleep_time}"


@huey.task(context=True)
@tracerbacker
def _process(func: typing.Callable, kwargs: dict = {}, task=None):
    """Execute generic huey task that calls the provided func with provided kwargs, asynchronously.

    task expected for all funcitons in the ops module to pass through task id for logging
    """
    if task:
        kwargs["task_id"] = task.id
    return func(**kwargs)


@huey.signal()
def _handle_signals(signal, task, exc=None):
    """Update the status in the task_status table When task emits a signal."""
    logging.info(f"{signal} : {task.id}")
    match signal:
        case signals.SIGNAL_EXECUTING:
            time_field = "start_time"
            ogc_status = "running"

        case signals.SIGNAL_COMPLETE | signals.SIGNAL_ERROR:
            time_field = "finish_time"
            tracerbacker_return = huey.result(task.id, preserve=True)
            if tracerbacker_return is None:
                ogc_status = "notfound"
            else:
                if tracerbacker_return["err"] is None:
                    ogc_status = "successful"
                else:
                    ogc_status = "failed"

        case signals.SIGNAL_CANCELED | signals.SIGNAL_LOCKED | signals.SIGNAL_EXPIRED | signals.SIGNAL_INTERRUPTED:
            time_field = "dismiss_time"
            ogc_status = "dismissed"

        case signals.SIGNAL_REVOKED:
            # Set huey status, then short-circuit without setting "ogc_status" or time field.
            # OGC status and dismiss time field are handled at time of revoke call, rather than waiting
            # for the revoke signal which happens later.
            huey.storage.sql(
                """update "task_status" set huey_status = ? where "task_id" = ?""", (signal, task.id), True
            )

            return

        case _:  # e.g. SIGNAL_RETRYING, SIGNAL_SCHEDULED
            raise ValueError(f"Unhandled signal: {signal}")

    expression = f"""
        update "task_status"
        set
            "huey_status" = ?,
            "ogc_status" = ?,
            "{time_field}" = datetime('now')
        where "task_id" = ?
        """

    args = (signal, ogc_status, task.id)
    huey.storage.sql(expression, args, True)
