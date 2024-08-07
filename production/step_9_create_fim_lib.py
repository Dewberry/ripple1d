"""Create FIM library."""

import json
import logging
import os

from ripple1d.ops.fim_lib import create_fim_lib
from ripple1d.ripple1d_logger import configure_logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    SAMPLE_DATA = os.path.dirname(__file__).replace("production", "tests\\ras-data\\Baxter")
    SAMPLE_DATA = f"{SAMPLE_DATA}\\submodels\\2823932"

    create_fim_lib(
        SAMPLE_DATA,
        plans=["nd", "kwse"],
    )
