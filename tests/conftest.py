import os

import numpy as np
import pytest

TEST_DIR = os.path.dirname(__file__)


def pytest_addoption(parser):
    parser.addoption("--model", action="store", help="Test Source HEC-RAS Model", default="Baxter")
    parser.addoption("--reach_id", action="store", help="NWM Reach ID", default="2823932")
    parser.addoption(
        "--min_elevation", action="store", help="Min elevation of the most downstream cross section", default="0"
    )


@pytest.fixture
def setup_data(request):

    RAS_MODEL = request.config.getoption("--model")
    REACH_ID = request.config.getoption("--reach_id")
    MIN_ELEVATION = float(request.config.getoption("--min_elevation"))

    SOURCE_RAS_MODEL_DIRECTORY = os.path.join(TEST_DIR, f"ras-data\\{RAS_MODEL}")
    SUBMODELS_BASE_DIRECTORY = os.path.join(SOURCE_RAS_MODEL_DIRECTORY, "submodels")
    SUBMODELS_DIRECTORY = os.path.join(SUBMODELS_BASE_DIRECTORY, REACH_ID)
    request.cls.REACH_ID = RAS_MODEL
    request.cls.REACH_ID = REACH_ID

    request.cls.SOURCE_RAS_MODEL_DIRECTORY = SOURCE_RAS_MODEL_DIRECTORY
    request.cls.SUBMODELS_BASE_DIRECTORY = SUBMODELS_BASE_DIRECTORY
    request.cls.SUBMODELS_DIRECTORY = SUBMODELS_DIRECTORY
    request.cls.GPKG_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.gpkg")
    request.cls.SOURCE_GPKG_FILE = os.path.join(SOURCE_RAS_MODEL_DIRECTORY, f"{RAS_MODEL}.gpkg")
    request.cls.TERRAIN_HDF = os.path.join(SUBMODELS_DIRECTORY, f"Terrain\\{REACH_ID}.hdf")
    request.cls.TERRAIN_VRT = os.path.join(SUBMODELS_DIRECTORY, f"Terrain\\{REACH_ID}.vrt")
    request.cls.RAS_PROJECT_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.prj")
    request.cls.GEOM_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.g01")
    request.cls.PLAN1_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.p01")
    request.cls.FLOW1_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.f01")
    request.cls.RESULT1_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.r01")
    request.cls.PLAN2_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.p02")
    request.cls.FLOW2_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.f02")
    request.cls.RESULT2_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.r02")
    request.cls.PLAN3_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.p03")
    request.cls.FLOW3_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.f03")
    request.cls.RESULT3_FILE = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.r03")
    request.cls.FIM_LIB_DB = os.path.join(SUBMODELS_DIRECTORY, f"fims\\{REACH_ID}.db")
    request.cls.DEPTH_GRIDS_ND = os.path.join(SUBMODELS_DIRECTORY, f"fims\\z_nd")
    integer, decimal = str(np.floor((MIN_ELEVATION + 41) * 2) / 2).split(".")
    request.cls.DEPTH_GRIDS_KWSE = os.path.join(SUBMODELS_DIRECTORY, f"fims\\z_{integer}_{decimal}")
    request.cls.MODEL_STAC_ITEM = os.path.join(SUBMODELS_DIRECTORY, f"{REACH_ID}.model.stac.json")
    request.cls.FIM_LIB_STAC_ITEM = os.path.join(SUBMODELS_DIRECTORY, f"fims\\{REACH_ID}.fim_lib.stac.json")
    request.cls.min_elevation = MIN_ELEVATION
