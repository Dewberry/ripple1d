import json
import os
import subprocess
import time
import unittest

import pytest
import requests

from ripple.consts import RIPPLE_VERSION

TEST_DIR = os.path.dirname(__file__)

REACH_ID = "2823932"
SOURCE_RAS_MODEL_DIRECTORY = os.path.join(TEST_DIR, "ras-data\\Baxter")
SUBMODELS_BASE_DIRECTORY = os.path.join(TEST_DIR, "ras-data\\Baxter\\submodels")
GPKG_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.gpkg")
TERRAIN_HDF = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\Terrain\\{REACH_ID}.hdf")
TERRAIN_VRT = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\Terrain\\{REACH_ID}.vrt")
RAS_PROJECT_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.prj")
GEOM_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.g01")
PLAN1_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.p01")
FLOW1_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.f01")
RESULT1_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.r01")
PLAN2_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.p02")
FLOW2_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.f02")
RESULT2_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.r02")
PLAN3_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.p03")
FLOW3_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.f03")
RESULT3_FILE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.r03")
FIM_LIB_DB = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\fims\\{REACH_ID}.db")
DEPTH_GRIDS_ND = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\fims\\z_0_0")
DEPTH_GRIDS_KWSE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\fims\\z_60_0")
STAC_ITEM = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.stac.json")


def start_server():
    return subprocess.Popen(["python", "-m", "ripple_api", "start"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)


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

            return job_status
        time.sleep(5)  # Wait for 10 seconds before checking again


def check_process(func):

    def wrapper(self, *args, **kwargs):
        process, payload, files = func(self)
        response = submit_job(process, payload)
        status = wait_for_job(response["jobID"])

        self.assertEqual(status, "successful")
        for file in files:
            self.assertTrue(os.path.exists(file))

    return wrapper


class TestApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server_process = start_server()
        time.sleep(5)  # Give the server some time to start

    @check_process
    def test1_extract_submodel(self):
        payload = {
            "source_model_directory": SOURCE_RAS_MODEL_DIRECTORY,
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "nwm_id": REACH_ID,
            "ripple_version": RIPPLE_VERSION,
        }
        process = "extract_submodel"
        files = [GPKG_FILE]
        return process, payload, files

    @check_process
    def test2_create_ras_terrain(self):
        payload = {"submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}"}
        process = "create_ras_terrain"
        files = [TERRAIN_HDF, TERRAIN_VRT]
        return process, payload, files

    @check_process
    def test3_create_model_run_normal_depth(self):
        payload = {
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "plan_suffix": "ind",
            "num_of_discharges_for_initial_normal_depth_runs": 1,
            "show_ras": False,
        }
        process = "create_model_run_normal_depth"
        files = [RAS_PROJECT_FILE, GEOM_FILE, PLAN1_FILE, FLOW1_FILE, RESULT1_FILE]
        return process, payload, files

    @check_process
    def test4_run_incremental_normal_depth(self):
        payload = {
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "plan_suffix": "nd",
            "depth_increment": 3,
            "ras_version": "631",
            "show_ras": False,
        }
        process = "run_incremental_normal_depth"
        files = [PLAN2_FILE, FLOW2_FILE, RESULT2_FILE]
        return process, payload, files

    @check_process
    def test5_run_known_wse(self):
        payload = {
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "plan_suffix": "kwse",
            "min_elevation": 60.0,
            "max_elevation": 61.0,
            "depth_increment": 1.0,
            "ras_version": "631",
            "show_ras": False,
        }
        process = "run_known_wse"
        files = [PLAN3_FILE, FLOW3_FILE, RESULT3_FILE]
        return process, payload, files

    @check_process
    def test6_create_fim_lib(self):
        payload = {"submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}", "plans": ["nd", "kwse"]}
        process = "create_fim_lib"
        files = [FIM_LIB_DB, DEPTH_GRIDS_ND, DEPTH_GRIDS_KWSE]
        return process, payload, files

    @check_process
    def test7_nwm_reach_model_stac(self):
        payload = {
            "ras_project_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "ras_model_s3_prefix": "stac/test-data/fim_models/2823932/",
            "bucket": "fim",
            "ripple_version": RIPPLE_VERSION,
        }
        process = "fim_model_to_stac"
        files = [STAC_ITEM]
        return process, payload, files

    def test8_cleanup(self):
        # TODO: clean up the submodel directory
        pass

    def test9_shutdown(self):
        r = subprocess.run(["python", "-m", "ripple_api", "stop"])
        self.assertEqual(r.returncode, 0)
