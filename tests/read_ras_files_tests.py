import os
import unittest
from pathlib import Path

import pytest

from ripple.ras2 import RasFlowText, RasGeomText, RasPlanText, RasProject, RasTextFile

TEST_DIR = os.path.dirname(__file__)
TEST_ITEM_FILE = "ras-data/baxter.json"
TEST_ITEM_PATH = os.path.join(TEST_DIR, TEST_ITEM_FILE)

RAS_PROJECT = os.path.join(TEST_DIR, "ras-data/Baxter/Baxter.prj")
RAS_PLAN = os.path.join(TEST_DIR, "ras-data/Baxter/Baxter.p01")
RAS_GEOM = os.path.join(TEST_DIR, "ras-data/Baxter/Baxter.g02")
RAS_FLOW = os.path.join(TEST_DIR, "ras-data/Baxter/Baxter.f01")


@pytest.fixture(scope="class")
def setup_data(request):
    request.cls.ras_project = RasProject(RAS_PROJECT)
    request.cls.ras_plan = RasPlanText(RAS_PLAN)
    request.cls.ras_geom = RasGeomText(RAS_GEOM)
    request.cls.ras_flow = RasFlowText(RAS_FLOW)


# RasProjectText
@pytest.mark.usefixtures("setup_data")
class TestProject(unittest.TestCase):
    def test_load_data(self):
        self.assertEqual(len(self.ras_project.contents), 36)
        self.assertEqual(self.ras_project.title, "Baxter River GIS Example")
        self.assertEqual(self.ras_project.plans, ["p01", "p02"])
        self.assertEqual(self.ras_project.geoms, ["g02"])
        self.assertEqual(self.ras_project.steady_flows, ["f01"])
        self.assertEqual(self.ras_project.unsteady_flows, ["u01", "u02", "u03"])

    def test_new_project(self):
        pass


# RasPlanText
@pytest.mark.usefixtures("setup_data")
class TestPlan(unittest.TestCase):
    def test_load_data(self):
        self.assertEqual(len(self.ras_plan.contents), 171)
        self.assertEqual(self.ras_plan.title, "Steady Flows")
        self.assertEqual(self.ras_plan.version, "5.00")
        self.assertEqual(self.ras_plan.plan_geom_file, "g02")
        self.assertEqual(self.ras_plan.plan_steady_flow, "f01")

    def test_new_plan(self):
        pass


# RasGeomText
@pytest.mark.usefixtures("setup_data")
class TestGeom(unittest.TestCase):
    def test_load_data(self):
        self.assertEqual(self.ras_geom.title, "Imported GIS Data +Bridges")
        self.assertEqual(self.ras_geom.version, "5.00")

    def test_parser(self):
        self.assertEqual(len(self.ras_geom.river_reaches), 5)
        reach_info = self.ras_geom.river_reach_info("Baxter River    ,Upper Reach")
        self.assertEqual(reach_info.river, "Baxter River")


# RasFlowText
@pytest.mark.usefixtures("setup_data")
class TestFlow(unittest.TestCase):
    def test_load_data(self):
        self.assertEqual(self.ras_flow.title, "Steady Flows")
        self.assertEqual(self.ras_flow.version, "6.30")
        self.assertEqual(self.ras_flow.n_profiles, 3)

    def test_new_flow(self):
        pass