import json
import os
import shutil
import subprocess
import time
import unittest

import pandas as pd
import pytest
import requests

from ripple1d.ras import RasFlowText


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
    url = f"http://localhost/jobs/{job_id}?tb=true"
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


@pytest.mark.usefixtures("setup_data")
class TestApi(unittest.TestCase):
    # @classmethod
    # def setUpClass(cls):
    #     cls.server_process = start_server()
    #     time.sleep(10)  # Give the server some time to start

    @check_process
    def test_a_extract_submodel(self):
        payload = {
            "source_model_directory": self.SOURCE_RAS_MODEL_DIRECTORY,
            "submodel_directory": self.SUBMODELS_DIRECTORY,
            "nwm_id": self.REACH_ID,
        }
        process = "extract_submodel"
        files = [self.GPKG_FILE]
        return process, payload, files

    @check_process
    def test_b_create_ras_terrain(self):
        payload = {"submodel_directory": self.SUBMODELS_DIRECTORY}
        process = "create_ras_terrain"
        files = [self.TERRAIN_HDF, self.TERRAIN_VRT]
        return process, payload, files

    @check_process
    def test_c_create_model_run_normal_depth(self):
        payload = {
            "submodel_directory": self.SUBMODELS_DIRECTORY,
            "plan_suffix": "ind",
            "num_of_discharges_for_initial_normal_depth_runs": 2,
            "show_ras": False,
        }
        process = "create_model_run_normal_depth"
        files = [self.RAS_PROJECT_FILE, self.GEOM_FILE, self.PLAN1_FILE, self.FLOW1_FILE, self.RESULT1_FILE]
        return process, payload, files

    @check_process
    def test_d_run_incremental_normal_depth(self):
        payload = {
            "submodel_directory": self.SUBMODELS_DIRECTORY,
            "plan_suffix": "nd",
            "depth_increment": 3,
            "ras_version": "631",
            "show_ras": False,
        }
        process = "run_incremental_normal_depth"
        files = [self.PLAN2_FILE, self.FLOW2_FILE, self.RESULT2_FILE]
        return process, payload, files

    @check_process
    def test_e_run_known_wse(self):
        payload = {
            "submodel_directory": self.SUBMODELS_DIRECTORY,
            "plan_suffix": "kwse",
            "min_elevation": self.min_elevation + 40,
            "max_elevation": self.min_elevation + 41,
            "depth_increment": 1.0,
            "ras_version": "631",
            "show_ras": False,
        }
        process = "run_known_wse"
        files = [self.PLAN3_FILE, self.FLOW3_FILE, self.RESULT3_FILE]
        return process, payload, files

    @check_process
    def test_f_create_fim_lib(self):
        payload = {"submodel_directory": self.SUBMODELS_DIRECTORY, "plans": ["nd", "kwse"]}
        process = "create_fim_lib"
        files = [self.FIM_LIB_DB, self.DEPTH_GRIDS_ND, self.DEPTH_GRIDS_KWSE]
        return process, payload, files

    def test_g_check_flows_are_equal(self):
        rf2 = pd.DataFrame(RasFlowText(self.FLOW2_FILE).flow_change_locations)
        rf3 = pd.DataFrame(RasFlowText(self.FLOW3_FILE).flow_change_locations)
        self.assertTrue(len(set(rf3["flows"].iloc[0]) - set(rf2["flows"].iloc[0])) == 0)

    # @check_process
    # def test_h_nwm_reach_model_stac(self):
    #     payload = {"ras_project_directory": self.SUBMODELS_DIRECTORY}
    #     process = "nwm_reach_model_stac"
    #     files = [self.MODEL_STAC_ITEM]
    #     return process, payload, files

    # @check_process
    # def test_i_fim_lib_stac(self):
    #     payload = {"ras_project_directory": self.SUBMODELS_DIRECTORY, "nwm_reach_id": self.REACH_ID}
    #     process = "fim_lib_stac"
    #     files = [self.FIM_LIB_STAC_ITEM]
    #     return process, payload, files
