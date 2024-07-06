"""Create HEC-RAS flow/plan files for an incremental normal depth run and run the plan."""

import json
import logging
import os

from ripple.ops.run_ras_model import run_incremental_normal_depth
from ripple.ripple_logger import configure_logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    SAMPLE_DATA = os.path.dirname(__file__).replace("production", "tests")
    SAMPLE_DATA = f"{SAMPLE_DATA}\\outputs\\submodels\\2823932"

    run_incremental_normal_depth(SAMPLE_DATA, f"nd", depth_increment=2)
