"""Create HEC-RAS terrain."""

import json
import logging
import os

from ripple1d.ops.ras_terrain import create_ras_terrain
from ripple1d.ripple1d_logger import configure_logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    SOURCE_MODEL_DIR = os.path.dirname(__file__).replace("production", "tests\\ras-data\\Baxter")
    SUBMODEL_DIR = f"{SOURCE_MODEL_DIR}\\submodels\\2823932"
    create_ras_terrain(SUBMODEL_DIR)
