import json
import os
import time
import typing

from huey import SqliteHuey
from huey.api import Result
import huey.signals as sigs

from api.log import LOG_DIR, initialize_log
from api.utils import tracerbacker


LOG = initialize_log()

huey = SqliteHuey(filename=os.path.join(LOG_DIR, "huey.db"), results=True)

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
    it starts executing."""
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


def all_task_status() -> dict[str, dict]:
    """Return dictionary of tasks, where key is task ID and value is subdict (fields related to each task)"""
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
            "result": result_traceback(task_id),
        }
    return results_dict


def huey_status(task_id: str) -> str:
    """For given task ID, return its current status in huey terms"""
    expression = """select "huey_status" from "task_status" where "task_id" = ?"""
    results = huey.storage.sql(expression, (task_id,), results=True)
    if not results:
        return "notfound"
    return results[0][0]


def ogc_status(task_id: str) -> str:
    """For given task ID, return its current status in OGC API terms.
    WARNING assumes function executed by task is wrapped by `tracerbacker`"""
    expression = """select "ogc_status" from "task_status" where "task_id" = ?"""
    results = huey.storage.sql(expression, (task_id,), results=True)
    if not results:
        return "notfound"
    return results[0][0]


def result_traceback(task_id: str) -> typing.Any:
    """Assumes that @tracerbacker is being used.
    For given task ID, return the value returned by that task's function.
    If traceback is None, task is not finished or the ID does not exist.
    If traceback string is non-empty, it failed.
    If traceback string is empty, it succeeded."""
    tb = huey.result(task_id, preserve=True)
    if not (tb is None or isinstance(tb, str)):
        raise TypeError(
            f"For task={task_id}, expected result to be a string or None, but got type={type(tb)}, value={tb}"
        )
    return tb


def noop():
    """Function that does nothing except log a message. For ping endpoint and testing."""
    LOG.info("This message is from the noop function")
    pass


def sleep15():
    """Function that does nothing except log a message and then sleep for a while.  For testing."""
    LOG.info("This message is from the sleep15 function")
    time.sleep(15)


@huey.task()  # If needing the worker to know about its own task, then use `@huey.task(context=True)`` and add `task=None` to the task function definition.
@tracerbacker
def _process(func: typing.Callable, kwargs: dict = {}):
    """Generic huey task that executes the provided func with provided kwargs, asynchronously."""
    func(**kwargs)


@huey.signal()
def _handle_signals(signal, task, exc=None):
    """When task emits a signal, update its status in the task_status table"""
    match signal:

        case sigs.SIGNAL_EXECUTING:
            time_field = "start_time"
            ogc_status = "running"

        case sigs.SIGNAL_COMPLETE | sigs.SIGNAL_ERROR:
            time_field = "finish_time"
            tb = result_traceback(task.id)
            if tb is None:
                ogc_status = "notfound"
            elif tb == "":
                ogc_status = "successful"
            else:
                ogc_status = "failed"

        case sigs.SIGNAL_CANCELED | sigs.SIGNAL_LOCKED | sigs.SIGNAL_EXPIRED | sigs.SIGNAL_INTERRUPTED:
            time_field = "dismiss_time"
            ogc_status = "dismissed"

        case sigs.SIGNAL_REVOKED:
            # Set huey status, then short-circuit without setting "ogc_status" or time field.
            # OGC status and dismiss time field are handled at time of revoke call, rather than waiting for the revoke signal which happens later.
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
