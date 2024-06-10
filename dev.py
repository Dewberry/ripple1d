import logging
import os

from ripple.ripple_logger import configure_logging

configure_logging(logging.DEBUG)

from ripple.ras2 import RasGeomText, RasManager, RasPlanText, RasProject

TEST_DIR = os.path.dirname(__file__)
TEST_ITEM_FILE = "ras-data/baxter.json"
TEST_ITEM_PATH = os.path.join(TEST_DIR, TEST_ITEM_FILE)

RAS_PROJECT = os.path.join(TEST_DIR, "tests/ras-data/Baxter/Baxter.prj")
RAS_PLAN = os.path.join(TEST_DIR, "tests/ras-data/Baxter/Baxter.p01")
RAS_GEOM = os.path.join(TEST_DIR, "tests/ras-data/Baxter/Baxter.g02")
RAS_FLOW = os.path.join(TEST_DIR, "tests/ras-data/Baxter/Baxter.f01")


ras_proj = RasProject(RAS_PROJECT)
new_project = ras_proj.set_current_plan("p11")

ras_plan = RasPlanText(RAS_PLAN)
new_plan = ras_plan.new_plan_from_existing("ripple-demo", "rd", "g10", "f02")
