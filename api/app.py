from http import HTTPStatus
import time
import traceback

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
    return process_async_request(new_gpkg)


@app.route("/processes/new_ras_terrain/execution", methods=["POST"])
def process__new_ras_terrain():
    return process_async_request(new_ras_terrain)


@app.route("/processes/initial_normal_depth/execution", methods=["POST"])
def process__initial_normal_depth():
    return process_async_request(initial_normal_depth)


@app.route("/processes/incremental_normal_depth/execution", methods=["POST"])
def process__incremental_normal_depth():
    return process_async_request(incremental_normal_depth)


@app.route("/processes/known_wse/execution", methods=["POST"])
def process__known_wse():
    return process_async_request(known_wse)


@app.route("/processes/new_fim_lib/execution", methods=["POST"])
def process__new_fim_lib():
    return process_async_request(new_fim_lib)


@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"status": "healthy"}), HTTPStatus.OK


@app.route("/processes/test/execution", methods=["POST"])
def test():
    response, http_status = process_async_request(tasks.noop)
    if http_status != HTTPStatus.CREATED:
        return jsonify(response.json, HTTPStatus.INTERNAL_SERVER_ERROR)

    timeout_seconds = 10

    start = time.time()
    while time.time() - start < timeout_seconds:
        time.sleep(0.2)
        status = tasks.status(response.json["jobID"])
        if status == "failed":
            return jsonify({"status": "not healthy"}), HTTPStatus.INTERNAL_SERVER_ERROR
        if status == "successful":
            return jsonify({"status": "healthy"}), HTTPStatus.OK
    return (
        jsonify({"status": f"huey is busy or not active, ping timed out after {timeout_seconds} seconds"}),
        HTTPStatus.GATEWAY_TIMEOUT,
    )


@app.route("/jobs/<task_id>", methods=["GET"])
def task_status(task_id):
    # https://developer.ogc.org/api/processes/index.html#tag/Status
    status = tasks.status(task_id)
    if status == "accepted":
        return jsonify({"type": "process", "jobID": task_id, "status": status}), HTTPStatus.OK
    if status == "running":
        return jsonify({"type": "process", "jobID": task_id, "status": status}), HTTPStatus.OK
    if status == "successful":
        return (
            jsonify({"type": "process", "jobID": task_id, "status": status, "detail": tasks.result(task_id)}),
            HTTPStatus.OK,
        )
    if status == "failed":
        return (
            jsonify({"type": "process", "jobID": task_id, "status": status, "detail": tasks.result(task_id)}),
            HTTPStatus.OK,
        )
    if status == "dismissed":
        return (
            jsonify({"type": "process", "jobID": task_id, "status": status, "detail": tasks.result(task_id)}),
            HTTPStatus.OK,
        )
    if status == "notfound":
        return jsonify({"type": "process", "detail": f"job ID not found: {task_id}"}), HTTPStatus.NOT_FOUND
    return jsonify({"type": "process", "detail": f"unexpected status: {status}"}), HTTPStatus.INTERNAL_SERVER_ERROR


@app.route("/jobs_status_all", methods=["GET"])
def task_status_all():
    task2result = {
        task_id: {"status": tasks.status(task_id), "result": tasks.result(task_id)}
        for task_id in sorted(set(tasks._all_queued()) | set(tasks._all_executing()) | set(tasks.huey.all_results()))
    }
    return (jsonify(task2result), HTTPStatus.OK)


@app.route("/jobs/<task_id>/results", methods=["GET"])
def task_result(task_id):
    # https://developer.ogc.org/api/processes/index.html#tag/Result
    try:
        status = tasks.status(task_id)
        if status == "notfound":
            return jsonify({"type": "process", "detail": f"job ID not found: {task_id}"}), HTTPStatus.NOT_FOUND
        else:
            return jsonify({"type": "process", "detail": tasks.result(task_id)}), HTTPStatus.OK
    except:
        return jsonify({"type": "process", "detail": f"failed to fetch results"}), HTTPStatus.INTERNAL_SERVER_ERROR


@app.route("/jobs/<task_id>/dismiss", methods=["DELETE"])
def dismiss(task_id):
    # https://developer.ogc.org/api/processes/index.html#tag/Dismiss
    try:
        status = tasks.status(task_id)
        if status == "notfound":
            return jsonify({"type": "process", "detail": f"job ID not found: {task_id}"}), HTTPStatus.NOT_FOUND
        else:
            result = tasks.revoke(task_id)
            return jsonify({"type": "process", "detail": result}), HTTPStatus.OK
    except:
        return (
            jsonify({"type": "process", "detail": f"failed to dismiss job ID: {task_id}"}),
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def process_async_request(func: callable) -> tuple[Response, HTTPStatus]:
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
        # tasks.process must be a huey TaskWrapper (decorated by @huey.task). Returns a huey Result object immediately.
        result = tasks.process(func, kwargs)
    except:
        msg = f"unable to submit task: {traceback.format_exc()}"
        app.logger.error(msg)
        return jsonify({"type": "process", "detail": msg}), HTTPStatus.INTERNAL_SERVER_ERROR
    else:
        return (
            jsonify({"type": "process", "jobID": result.id, "status": "accepted"}),
            HTTPStatus.CREATED,
        )
