"""huey instance for huey + Flask REST API (to be called by huey_consumer.py)."""

import json
import logging
import os
import subprocess
import sys
import time
import typing

import psutil
from huey import SqliteHuey, signals
from huey.api import Result

from ripple1d.ripple1d_logger import initialize_server_logger

initialize_server_logger()

huey = SqliteHuey(filename=os.path.join("jobs.db"), results=True)

# Create custom table task_status
huey.storage.sql(
    """
    create table if not exists "task_status" (
        "task_id" text not null,
        "p_id" text,
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
        "results" text,
        primary key("task_id")
    );
    """,
    commit=True,
)

# Create custom table task_logs
huey.storage.sql(
    """
    create table if not exists "task_logs" (
        "task_id" text not null,
        "stdout" text,
        "stderr" text,
        "results" text,
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


def revoke_task_by_pid(task_id: str):
    """Revoke a task by pid."""
    expression = """select "p_id" from "task_status" where "task_id" = ?"""
    args = (task_id,)
    pid = huey.storage.sql(expression, args, results=True)

    p = psutil.Process(int(pid[0][0]))
    p.terminate()

    expression = f"""
        update "task_status"
        set
            "ogc_status" = 'dismissed',
            "dismiss_time" = datetime('now')
        where "task_id" = ?
        """
    args = (task_id,)
    huey.storage.sql(expression, args, True)


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


def subprocess_caller(
    func: str, args: dict, task_id: str, log_dir: str = "", log_level: int = logging.INFO, timeout: int = None
):
    """Call the specified function through a subprocess."""
    subprocess_args = [
        sys.executable,
        "-u",
        os.path.dirname(__file__).replace("api", "ops/endpoints.py"),
        func,
        json.dumps(args),
    ]

    process = subprocess.Popen(subprocess_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    results = None
    update_p_id(task_id, process.pid)

    try:
        stdout_lines, stderr_output = process.communicate(timeout=timeout)
        logs = stdout_lines.splitlines()
        for i, line in enumerate(logs):
            if '"results"' in line:
                program_output = json.loads(line)
                results = json.dumps(program_output.get("results", None))
                del logs[i]

    except subprocess.TimeoutExpired:
        process.kill()
        stderr_output.append("Process timed out")
        raise TimeoutError(f"{task_id}: Process timed out")

    except Exception as e:
        stderr_output.append(str(e))
        raise SystemError(f"{task_id}: Exception occurred: {e}")

    exit_code = process.wait()

    if len(stderr_output) == 0:
        errors = None
    else:
        errors = str(stderr_output)

    huey.storage.sql(
        """
        insert into "task_logs"
            ("task_id", "stdout", "stderr", "results")
        values
            (?, ?, ?, ?)""",
        (task_id, str(logs), errors, results),
        True,
    )

    if exit_code == 15:
        huey.storage.sql(
            """update "task_status" set "ogc_status" = ? where "task_id" = ?""", ("dismissed", task_id), True
        )

    elif exit_code != 0:
        logging.debug(f"{task_id} exit code {exit_code}")
        huey.storage.sql("""update "task_status" set "ogc_status" = ? where "task_id" = ?""", ("failed", task_id), True)


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


def fetch_one_query(task_id: str, field: str, table: str) -> str:
    """Return a fetch one query given table and field given task_id."""
    expression = f"""select "{field}" from "{table}" where "task_id" = ?"""
    results = huey.storage.sql(expression, (task_id,), results=True)
    if not results:
        return "notfound"
    return results[0][0]


def huey_status(task_id: str) -> str:
    """For given task ID, return its current status in huey terms."""
    # expression = """select "huey_status" from "task_status" where "task_id" = ?"""
    # results = huey.storage.sql(expression, (task_id,), results=True)
    # if not results:
    #     return "notfound"
    # return results[0][0]
    return fetch_one_query(task_id, "huey_status", "task_status")


def fetch_ogc_status(task_id: str) -> str:
    """For given task ID, return its current status in OGC API terms."""
    # expression = """select "ogc_status" from "task_status" where "task_id" = ?"""
    # results = huey.storage.sql(expression, (task_id,), results=True)
    # if not results:
    #     return "notfound"
    # return results[0][0]
    return fetch_one_query(task_id, "ogc_status", "task_status")


def job_dismissed(task_id: str) -> bool:
    """For given task ID, return its current status in OGC API terms."""
    expression = """select "dismiss_time" from "task_status" where "task_id" = ?"""
    results = huey.storage.sql(expression, (task_id,), results=True)
    logging.debug(f"job_dismissed response = {results}")
    if results[0][0] == "None" or results[0][0] is None:
        return False
    return True


def task_results(task_id: str) -> str:
    """For given task ID, return its current status in OGC API terms."""
    expression = """select "results" from "task_logs" where "task_id" = ?"""
    results = huey.storage.sql(expression, (task_id,), results=True)
    if not results:
        return "notfound"
    elif results[0][0] == "failed":
        return "failed"


def noop(task_id: str = None):
    """Do nothing except log a message. For ping endpoint and testing."""
    logging.info(f"{task_id} | noop")
    pass


@huey.task(context=True)
def _process(func: typing.Callable, kwargs: dict = {}, task=None):
    """Execute generic huey task that calls the provided func with provided kwargs, asynchronously."""
    if task:
        task_id = task.id
    else:
        task_id = None
    return subprocess_caller(func.__name__, kwargs, task_id)


@huey.signal()
def _handle_signals(signal, task, exc=None):
    """Update the status in the task_status table When task emits a signal."""
    # logging.info(f"{signal} : {task.id}")
    task_status = signal
    match signal:
        case signals.SIGNAL_EXECUTING:
            time_field = "start_time"
            ogc_status = "running"

        case signals.SIGNAL_COMPLETE:
            time_field = "finish_time"
            if job_dismissed(task.id):
                task_status = "revoked"
                ogc_status = "dismissed"
                huey.storage.sql(
                    """update "task_status" set huey_status = ? where "task_id" = ?""", (task_status, task.id), True
                )
            elif fetch_ogc_status(task.id) == "failed":
                task_status = "completed"
                ogc_status = "failed"
            else:
                ogc_status = "successful"

        case signals.SIGNAL_ERROR:
            time_field = "finish_time"
            ogc_status = "failed"

        case signals.SIGNAL_LOCKED | signals.SIGNAL_EXPIRED:
            time_field = "finish_time"
            ogc_status = "failed"

        case signals.SIGNAL_CANCELED | signals.SIGNAL_INTERRUPTED:
            time_field = "dismiss_time"
            ogc_status = "dismissed"

        case signals.SIGNAL_REVOKED:
            # Set huey status, then short-circuit without setting "ogc_status" or time field.
            # OGC status and dismiss time field are handled at time of revoke call, rather than waiting
            # for the revoke signal which happens later.
            huey.storage.sql(
                """update "task_status" set huey_status = ? where "task_id" = ?""", (signal, task.id), True
            )
            ogc_status = "dismissed"

        case _:  # e.g. SIGNAL_RETRYING, SIGNAL_SCHEDULED
            raise ValueError(f"Unhandled signal: {signal}")

    if ogc_status == "dismissed":
        return

    expression = f"""
        update "task_status"
        set
            "huey_status" = ?,
            "ogc_status" = ?,
            "{time_field}" = datetime('now')
        where "task_id" = ?
        """

    args = (task_status, ogc_status, task.id)
    huey.storage.sql(expression, args, True)
