"""Create HEC-RAS flow/plan files for an known water surface elevation run and run the plan."""

import json
import logging
import os

import numpy as np

from ripple.ops.ras_run import (
    establish_order_of_nwm_ids,
    get_kwse_from_ds_model,
    run_known_wse,
)
from ripple.ripple_logger import configure_logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)

    SAMPLE_DATA = os.path.dirname(__file__).replace("production", "tests")
    SAMPLE_DATA = f"{SAMPLE_DATA}\\outputs\\submodels\\2823932"

    run_known_wse(SAMPLE_DATA, "kwse", min_elevation=60.0, max_elevation=62.0, depth_increment=1.0, ras_version="631")
