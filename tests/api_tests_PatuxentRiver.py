import json
import os
import shutil
import subprocess
import time
import unittest

import pandas as pd
import pytest
import requests

import ripple1d
from ripple1d.ras import RasFlowText

TEST_DIR = os.path.dirname(__file__)

REACH_ID = "11906190"
SOURCE_RAS_MODEL_DIRECTORY = os.path.join(TEST_DIR, "ras-data\\PatuxentRiver")
SUBMODELS_BASE_DIRECTORY = os.path.join(TEST_DIR, "ras-data\\PatuxentRiver\\submodels")
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
DEPTH_GRIDS_ND = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\fims\\z_nd")
DEPTH_GRIDS_KWSE = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\fims\\z_130_0")
MODEL_STAC_ITEM = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\{REACH_ID}.model.stac.json")
FIM_LIB_STAC_ITEM = os.path.join(SUBMODELS_BASE_DIRECTORY, f"{REACH_ID}\\fims\\{REACH_ID}.fim_lib.stac.json")


def start_server():
    return subprocess.Popen(
        ["ripple1d", "start"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


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
        time.sleep(7)  # Wait for 10 seconds before checking again


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
        time.sleep(10)  # Give the server some time to start

    @check_process
    def test_a_extract_submodel(self):
        payload = {
            "source_model_directory": SOURCE_RAS_MODEL_DIRECTORY,
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "nwm_id": REACH_ID,
            "ripple_version": ripple1d.__version__,
        }
        process = "extract_submodel"
        files = [GPKG_FILE]
        return process, payload, files

    @check_process
    def test_b_create_ras_terrain(self):
        payload = {"submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}"}
        process = "create_ras_terrain"
        files = [TERRAIN_HDF, TERRAIN_VRT]
        return process, payload, files

    @check_process
    def test_c_create_model_run_normal_depth(self):
        payload = {
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "plan_suffix": "ind",
            "num_of_discharges_for_initial_normal_depth_runs": 5,
            "show_ras": False,
        }
        process = "create_model_run_normal_depth"
        files = [RAS_PROJECT_FILE, GEOM_FILE, PLAN1_FILE, FLOW1_FILE, RESULT1_FILE]
        return process, payload, files

    @check_process
    def test_d_run_incremental_normal_depth(self):
        payload = {
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "plan_suffix": "nd",
            "depth_increment": 1,
            "ras_version": "631",
            "show_ras": False,
        }
        process = "run_incremental_normal_depth"
        files = [PLAN2_FILE, FLOW2_FILE, RESULT2_FILE]
        return process, payload, files

    @check_process
    def test_e_run_known_wse(self):
        payload = {
            "submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "plan_suffix": "kwse",
            "min_elevation": 128,
            "max_elevation": 130,
            "depth_increment": 1.0,
            "ras_version": "631",
            "show_ras": False,
        }
        process = "run_known_wse"
        files = [PLAN3_FILE, FLOW3_FILE, RESULT3_FILE]
        return process, payload, files

    @check_process
    def test_f_create_fim_lib(self):
        payload = {"submodel_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}", "plans": ["nd", "kwse"]}
        process = "create_fim_lib"
        files = [FIM_LIB_DB, DEPTH_GRIDS_ND, DEPTH_GRIDS_KWSE]
        return process, payload, files

    def test_g_check_flows_are_equal(self):
        rf2 = pd.DataFrame(RasFlowText(FLOW2_FILE).flow_change_locations)
        rf3 = pd.DataFrame(RasFlowText(FLOW3_FILE).flow_change_locations)
        self.assertTrue(set(rf2["flows"].iloc[0]) == set(rf3["flows"].iloc[0]))

    @check_process
    def test_h_nwm_reach_model_stac(self):
        payload = {
            "ras_project_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "ras_model_s3_prefix": f"stac/test-data/nwm_reach_models/{REACH_ID}",
            "bucket": "fim",
        }
        process = "nwm_reach_model_stac"
        files = [MODEL_STAC_ITEM]
        return process, payload, files

    @check_process
    def test_i_fim_lib_stac(self):
        payload = {
            "ras_project_directory": f"{SUBMODELS_BASE_DIRECTORY}\\{REACH_ID}",
            "nwm_reach_id": REACH_ID,
            "s3_prefix": f"stac/test-data/fim_libs/{REACH_ID}",
            "bucket": "fim",
        }
        process = "fim_lib_stac"
        files = [FIM_LIB_STAC_ITEM]
        return process, payload, files

    def test_j_cleanup(self):
        shutil.rmtree(SUBMODELS_BASE_DIRECTORY)

    # def test_k_shutdown(self):
    #     r = subprocess.run(["ripple1d", "stop"])
    #     self.assertEqual(r.returncode, 0)