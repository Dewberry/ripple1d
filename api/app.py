"""Flask API."""

import time
import traceback
import typing
from http import HTTPStatus

from flask import Flask, Response, jsonify, request
from werkzeug.exceptions import BadRequest

from api import tasks
from api.utils import get_unexpected_and_missing_args
from ripple.ops.create_fim_lib import new_fim_lib
from ripple.ops.create_ras_terrain import new_ras_terrain
from ripple.ops.run_ras_model import (
    incremental_normal_depth,
    initial_normal_depth,
    known_wse,
)
from ripple.ops.subset_gpkg import extract_submodel

app = Flask(__name__)


@app.route("/processes/extract_submodel/execution", methods=["POST"])
def process__extract_submodel():
    """Enqueue a task to create a new GeoPackage"""
    return enqueue_async_task(extract_submodel)


@app.route("/processes/new_ras_terrain/execution", methods=["POST"])
def process__new_ras_terrain():
    """Enqueue a task to create a new RAS terrain."""
    return enqueue_async_task(new_ras_terrain)


@app.route("/processes/initial_normal_depth/execution", methods=["POST"])
def process__initial_normal_depth():
    """Enqueue a task to calculate the initial normal depth."""
    return enqueue_async_task(initial_normal_depth)


@app.route("/processes/incremental_normal_depth/execution", methods=["POST"])
def process__incremental_normal_depth():
    """Enqueue a task to calculate the incremental normal depth."""
    return enqueue_async_task(incremental_normal_depth)


@app.route("/processes/known_wse/execution", methods=["POST"])
def process__known_wse():
    """Enqueue a task to calculate the water surface elevation (WSE) based on known inputs."""
    return enqueue_async_task(known_wse)


@app.route("/processes/new_fim_lib/execution", methods=["POST"])
def process__new_fim_lib():
    """Enqueue a task to create a new FIM library."""
    return enqueue_async_task(new_fim_lib)


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
def task_status(task_id):
    """Retrieve the status of a specific task by its ID."""
    status = tasks.ogc_status(task_id)
    if status == "accepted":
        return jsonify({"type": "process", "jobID": task_id, "status": status}), HTTPStatus.OK
    if status == "running":
        return jsonify({"type": "process", "jobID": task_id, "status": status}), HTTPStatus.OK
    if status == "successful":
        return (
            jsonify({"type": "process", "jobID": task_id, "status": status, "detail": tasks.result_traceback(task_id)}),
            HTTPStatus.OK,
        )
    if status == "failed":
        return (
            jsonify({"type": "process", "jobID": task_id, "status": status, "detail": tasks.result_traceback(task_id)}),
            HTTPStatus.OK,
        )
    if status == "dismissed":
        return (
            jsonify({"type": "process", "jobID": task_id, "status": status, "detail": tasks.result_traceback(task_id)}),
            HTTPStatus.OK,
        )
    if status == "notfound":
        return jsonify({"type": "process", "detail": f"job ID not found: {task_id}"}), HTTPStatus.NOT_FOUND
    return jsonify({"type": "process", "detail": f"unexpected status: {status}"}), HTTPStatus.INTERNAL_SERVER_ERROR


@app.route("/jobs", methods=["GET"])
def all_task_status():
    """Retrieve the status of all tasks."""
    return jsonify(tasks.all_task_status(), HTTPStatus.OK)


@app.route("/jobs/<task_id>/results", methods=["GET"])
def task_result(task_id):
    """Retrieve the result of a specific task by its ID."""
    try:
        status = tasks.ogc_status(task_id)
        if status == "notfound":
            return jsonify({"type": "process", "detail": f"job ID not found: {task_id}"}), HTTPStatus.NOT_FOUND
        else:
            return jsonify({"type": "process", "detail": tasks.result_traceback(task_id)}), HTTPStatus.OK
    except:
        return jsonify({"type": "process", "detail": f"failed to fetch results"}), HTTPStatus.INTERNAL_SERVER_ERROR


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
