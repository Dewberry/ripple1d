"""Flask API."""

import time
import traceback
import typing
from http import HTTPStatus

from flask import Flask, Response, jsonify, request
from werkzeug.exceptions import BadRequest

from api import tasks
from api.utils import get_unexpected_and_missing_args
from ripple.ops.fim_lib import create_fim_lib
from ripple.ops.ras_run import (
    create_model_run_normal_depth,
    run_incremental_normal_depth,
    run_known_wse,
)
from ripple.ops.ras_terrain import create_ras_terrain
from ripple.ops.subset_gpkg import extract_submodel

app = Flask(__name__)


@app.route("/processes/extract_submodel/execution", methods=["POST"])
def process__extract_submodel():
    """Enqueue a task to create a new GeoPackage"""
    return enqueue_async_task(extract_submodel)


@app.route("/processes/create_ras_terrain/execution", methods=["POST"])
def process__create_ras_terrain():
    """Enqueue a task to create a new RAS terrain."""
    return enqueue_async_task(create_ras_terrain)


@app.route("/processes/create_model_run_normal_depth/execution", methods=["POST"])
def process__create_model_run_normal_depth():
    """Enqueue a task to calculate the initial normal depth."""
    return enqueue_async_task(create_model_run_normal_depth)


@app.route("/processes/run_incremental_normal_depth/execution", methods=["POST"])
def process__run_incremental_normal_depth():
    """Enqueue a task to calculate the incremental normal depth."""
    return enqueue_async_task(run_incremental_normal_depth)


@app.route("/processes/run_known_wse/execution", methods=["POST"])
def process__run_known_wse():
    """Enqueue a task to calculate the water surface elevation (WSE) based on known inputs."""
    return enqueue_async_task(run_known_wse)


@app.route("/processes/create_fim_lib/execution", methods=["POST"])
def process__create_fim_lib():
    """Enqueue a task to create a FIM library."""
    return enqueue_async_task(create_fim_lib)


@app.route("/ping", methods=["GET"])
def ping():
    """Check the health of the service."""
    return jsonify({"status": "healthy"}), HTTPStatus.OK


@app.route("/processes/test/execution", methods=["POST"])
def test():
    """Test the execution and monitoring of an asynchronous task."""
    response, http_status = enqueue_async_task(tasks.noop)
    if http_status != HTTPStatus.CREATED:
        return jsonify(response.json, HTTPStatus.INTERNAL_SERVER_ERROR)

    timeout_seconds = 10

    start = time.time()
    while time.time() - start < timeout_seconds:
        time.sleep(0.2)
        status = tasks.ogc_status(response.json["jobID"])
        if status == "failed":
            return jsonify({"status": "not healthy"}), HTTPStatus.INTERNAL_SERVER_ERROR
        if status == "successful":
            return jsonify({"status": "healthy"}), HTTPStatus.OK
    return (
        jsonify({"status": f"huey is busy or not active, ping timed out after {timeout_seconds} seconds"}),
        HTTPStatus.GATEWAY_TIMEOUT,
    )


@app.route("/processes/sleep/execution", methods=["POST"])
def process__sleep():
    """Enqueue a task that sleeps for 15 seconds."""
    return enqueue_async_task(tasks.sleep15)


@app.route("/jobs/<task_id>", methods=["GET"])
def get_one_job(task_id):
    """Retrieve OGC status and result for one job.

    Query parameters:
        tb: Choices are ['true', 'false']. Defaults to 'false'. If 'true', the job result's traceback will be included
            in the response, as key 'tb'.
    """
    include_traceback, problem = parse_request_param__bool(param_name="tb", default=False)
    if problem is not None:
        return problem

    task2metadata = tasks.task_status(only_task_id=task_id)

    if len(task2metadata) == 0:
        return jsonify({"type": "process", "detail": f"job ID not found: {task_id}"}), HTTPStatus.NOT_FOUND

    if len(task2metadata) > 1:
        return (
            jsonify({"type": "process", "detail": f"multiple ({len(task2metadata)}) records matched job ID {task_id}"}),
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    huey_metadata = task2metadata[task_id]
    return get_ogc_job_metadata_from_huey_metadata(task_id, huey_metadata, include_traceback), HTTPStatus.OK


@app.route("/jobs", methods=["GET"])
def get_all_jobs():
    """Retrieve OGC status and result for all jobs.

    Query parameters:
        tb: Choices are ['true', 'false']. Defaults to 'false'. If 'true', each job result's traceback will be included
            in the response, as key 'tb'.
    """
    include_traceback, problem = parse_request_param__bool(param_name="tb", default=False)
    if problem is not None:
        return problem

    task2metadata = tasks.task_status(only_task_id=None)
    jobs = [
        get_ogc_job_metadata_from_huey_metadata(task_id, huey_metadata, include_traceback)
        for task_id, huey_metadata in task2metadata.items()
    ]
    links = []
    ret = {"jobs": jobs, "links": links}
    return jsonify(ret), HTTPStatus.OK


def parse_request_param__bool(param_name: str, default: bool) -> tuple[bool, tuple]:
    """Get the parameter, assert it is true or false, and return the appropriate Python boolean value as well as a status tuple.

    If there is a problem, the status tuple has two elements: a response message, and a HTTP status to be returned by the endpoint.
    If there is not a problem, the status tuple is actually None."""
    arg_tb = request.args.get(param_name)
    if not arg_tb:
        return (default, None)
    elif arg_tb.lower() == "false":
        return (False, None)
    elif arg_tb.lower() == "true":
        return (True, None)
    else:
        return (
            None,
            (
                jsonify(
                    {
                        "type": "process",
                        "detail": f"query param 'tb' should be 'true' or 'false', but got: {repr(arg_tb)}",
                    }
                ),
                HTTPStatus.BAD_REQUEST,
            ),
        )


def get_ogc_job_metadata_from_huey_metadata(task_id: str, huey_metadata: dict, include_traceback: bool) -> dict:
    """Convert huey-style task status metadata into a OGC-style result dictionary."""
    huey_result = tasks.huey.result(task_id, preserve=True)
    if include_traceback is False and huey_result is not None:
        del huey_result["tb"]  # remove the traceback
    ogc_job_metadata = {
        "jobID": task_id,
        "updated": huey_metadata["status_time"],
        "status": huey_metadata["ogc_status"],
        "processID": huey_metadata["func_name"],
        "type": "process",
        "submitter": "",
        "result": huey_result,
    }
    return ogc_job_metadata


@app.route("/jobs/<task_id>", methods=["DELETE"])
def dismiss(task_id):
    """Dismiss a specific task by its ID."""
    try:
        ogc_status = tasks.ogc_status(task_id)
        if ogc_status == "notfound":
            return jsonify({"type": "process", "detail": f"job ID not found: {task_id}"}), HTTPStatus.NOT_FOUND
        elif ogc_status == "accepted":
            tasks.revoke_task(task_id)
            return jsonify({"type": "process", "detail": f"job ID dismissed: {task_id}"}), HTTPStatus.OK
        elif ogc_status == "dismissed":
            return jsonify({"type": "process", "detail": f"job ID dismissed: {task_id}"}), HTTPStatus.OK
        else:
            return (
                jsonify(
                    {
                        "type": "process",
                        "detail": f"failed to dismiss job ID {task_id} due to existing job status '{ogc_status}'",
                    }
                ),
                HTTPStatus.CONFLICT,
            )
    except Exception as e:
        return (
            jsonify({"type": "process", "detail": f"failed to dismiss job ID {task_id} due to internal server error"}),
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def enqueue_async_task(func: typing.Callable) -> tuple[Response, HTTPStatus]:
    """Start the execution of the provided func using kwargs from the request body. Assume body is a JSON dictionary."""
    try:
        kwargs = request.json  # can throw BadRequest when parsing body into json
        if not isinstance(kwargs, dict):
            raise BadRequest
    except BadRequest:
        return (
            jsonify({"type": "process", "detail": "could not parse body to json dict"}),
            HTTPStatus.BAD_REQUEST,
        )

    unexpected, missing = get_unexpected_and_missing_args(func, set(kwargs))
    if unexpected or missing:
        return (
            jsonify({"type": "process", "detail": f"unexpected args: {unexpected}, missing args: {missing}"}),
            HTTPStatus.BAD_REQUEST,
        )

    try:
        result = tasks.create_and_enqueue_task(func, kwargs)
    except:
        msg = f"unable to submit task: {traceback.format_exc()}"
        app.logger.error(msg)
        return jsonify({"type": "process", "detail": msg}), HTTPStatus.INTERNAL_SERVER_ERROR
    else:
        return (
            jsonify({"type": "process", "jobID": result.id, "status": "accepted"}),
            HTTPStatus.CREATED,
        )
