"""Create HEC-RAS terrain."""

import json
import logging
import os

from ripple.ops.create_ras_terrain import create_ras_terrain
from ripple.ripple_logger import configure_logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    SAMPLE_DATA = os.path.dirname(__file__).replace("production", "tests")
    SAMPLE_DATA = f"{SAMPLE_DATA}\\outputs\\submodels\\2823932"
    create_ras_terrain(SAMPLE_DATA)
