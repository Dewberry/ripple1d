from http import HTTPStatus
import time
import traceback
import typing

from flask import Flask, Response, jsonify, request
from werkzeug.exceptions import BadRequest

from ripple.ops.create_fim_lib import new_fim_lib
from ripple.ops.create_ras_terrain import new_ras_terrain
from ripple.ops.run_ras_model import initial_normal_depth, incremental_normal_depth, known_wse
from ripple.ops.subset_gpkg import new_gpkg

from api import tasks
from api.utils import get_unexpected_and_missing_args

app = Flask(__name__)


@app.route("/processes/new_gpkg/execution", methods=["POST"])
def process__new_gpkg():
    return enqueue_async_task(new_gpkg)


@app.route("/processes/new_ras_terrain/execution", methods=["POST"])
def process__new_ras_terrain():
    return enqueue_async_task(new_ras_terrain)


@app.route("/processes/initial_normal_depth/execution", methods=["POST"])
def process__initial_normal_depth():
    return enqueue_async_task(initial_normal_depth)


@app.route("/processes/incremental_normal_depth/execution", methods=["POST"])
def process__incremental_normal_depth():
    return enqueue_async_task(incremental_normal_depth)


@app.route("/processes/known_wse/execution", methods=["POST"])
def process__known_wse():
    return enqueue_async_task(known_wse)


@app.route("/processes/new_fim_lib/execution", methods=["POST"])
def process__new_fim_lib():
    return enqueue_async_task(new_fim_lib)


@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"status": "healthy"}), HTTPStatus.OK


@app.route("/processes/test/execution", methods=["POST"])
def test():
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
    return enqueue_async_task(tasks.sleep15)


@app.route("/jobs/<task_id>", methods=["GET"])
def task_status(task_id):
    # https://developer.ogc.org/api/processes/index.html#tag/Status
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
    return jsonify(tasks.all_task_status(), HTTPStatus.OK)


@app.route("/jobs/<task_id>/results", methods=["GET"])
def task_result(task_id):
    # https://developer.ogc.org/api/processes/index.html#tag/Result
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
    # https://developer.ogc.org/api/processes/index.html#tag/Dismiss
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
    """Start the execution of the provided func using kwargs from the request body (assume body is a JSON dictionary)"""
    # https://developer.ogc.org/api/processes/index.html#tag/Execute/operation/execute
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
