"""Create geopackage for cross sections that conflated to NWM hydrofabric."""

import json
import logging
import os

from ripple.ops.subset_gpkg import extract_submodel
from ripple.ripple_logger import configure_logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)

    SOURCE_MODEL_DIR = os.path.dirname(__file__).replace("production", "tests\\ras-data\\Baxter")
    nwm_id = "2823932"
    submodel_dir = f"{SOURCE_MODEL_DIR}\\submodels\\{nwm_id}"

    extract_submodel(
        SOURCE_MODEL_DIR,
        submodel_dir,
        nwm_id,
    )
    # with open(conflation_json_path, "w") as f:
    #     json.dump(ripple_parameters, f, indent=4)
