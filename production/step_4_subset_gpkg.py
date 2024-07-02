"""Create geopackage for cross sections that conflated to NWM hydrofabric."""

import json
import logging
import os

from ripple.ops.subset_gpkg import new_gpkg
from ripple.ripple_logger import configure_logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)

    SAMPLE_DATA = os.path.dirname(__file__).replace("production", "sample_data\\Baxter")
    ras_project_directory = os.path.join(SAMPLE_DATA, "Baxter")
    mip_source_model = ras_project_directory
    ras_gpkg_file_path = os.path.join(SAMPLE_DATA, "Baxter.gpkg")
    conflation_json_path = os.path.join(SAMPLE_DATA, "Baxter-ripple-params.json")

    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    ripple_parameters = {}
    for nwm_id in conflation_parameters.keys():
        ripple_parameters[nwm_id] = new_gpkg(
            mip_source_model,
            os.path.join(ras_project_directory, nwm_id),
            ras_gpkg_file_path,
            nwm_id,
            conflation_parameters[nwm_id],
        )
    with open(conflation_json_path, "w") as f:
        json.dump(ripple_parameters, f, indent=4)
