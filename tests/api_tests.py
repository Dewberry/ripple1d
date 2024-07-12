import json
import os
import time
import unittest

import pytest
import requests

from ripple.consts import RIPPLE_VERSION

TEST_DIR = os.path.dirname(__file__)

SOURCE_RAS_MODEL_DIRECTORY = os.path.join(TEST_DIR, "ras-data\\Baxter")
SUBMODELS_BASE_DIRECTORY = os.path.join(TEST_DIR, "ras-data\\Baxter\\submodels")
REACH_ID = "2823932"


def submit_job(process: str, payload: dict):
    headers = {"Content-Type": "application/json"}
    url = f"http://localhost/processes/{process}/execution"
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    return json.loads(response.text)


def wait_for_job(job_id: str):
    url = f"http://localhost/jobs/{job_id}"
    while True:
        response = requests.get(url)
        job_status = response.json().get("status")
        if job_status in ["successful", "failed"]:
            print(job_status)
            return job_status
        time.sleep(5)  # Wait for 10 seconds before checking again


def check_process(func):

    def wrapper(self, *args, **kwargs):
        process, payload = func(self)
        response = submit_job(process, payload)
        status = wait_for_job(response["jobID"])

        self.assertEqual(status, "successful")

    return wrapper


class TestApi(unittest.TestCase):

    @check_process
    def test1_extract_submodel(self):
        payload = {
            "source_model_directory": SOURCE_RAS_MODEL_DIRECTORY,
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "nwm_id": REACH_ID,
            "ripple_version": RIPPLE_VERSION,
        }
        process = "extract_submodel"

        return process, payload

    @check_process
    def test2_create_ras_terrain(self):
        payload = {"submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}"}
        process = "create_ras_terrain"
        return process, payload

    @check_process
    def test3_create_model_run_normal_depth(self):
        payload = {
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "plan_suffix": "ind",
            "num_of_discharges_for_initial_normal_depth_runs": 5,
            "show_ras": False,
        }
        process = "create_model_run_normal_depth"
        return process, payload

    @check_process
    def test4_run_incremental_normal_depth(self):
        payload = {
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "plan_suffix": "nd",
            "depth_increment": 1,
            "ras_version": "631",
            "show_ras": False,
        }
        process = "run_incremental_normal_depth"
        return process, payload

    @check_process
    def test5_run_known_wse(self):
        payload = {
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "plan_suffix": "kwse",
            "min_elevation": 60.0,
            "max_elevation": 62.0,
            "depth_increment": 1.0,
            "ras_version": "631",
            "show_ras": False,
        }
        process = "run_known_wse"
        return process, payload

    @check_process
    def test6_create_fim_lib(self):
        payload = {"submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}", "plans": ["nd", "kwse"]}
        process = "create_fim_lib"
        return process, payload
