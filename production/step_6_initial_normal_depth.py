import json
import logging
import os

from ripple.ops.run_ras_model import initial_normal_depth
from ripple.ripple_logger import configure_logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    SAMPLE_DATA = os.path.dirname(__file__).replace("production", "sample_data\\Baxter")
    conflation_json_path = os.path.join(SAMPLE_DATA, "Baxter-ripple-params.json")
    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    for nwm_id in conflation_parameters.keys():
        new_ras_project_text_file = os.path.join(SAMPLE_DATA, f"{nwm_id}\\{nwm_id}.prj")

        subset_gpkg_path = os.path.join(SAMPLE_DATA, f"{nwm_id}\\{nwm_id}.gpkg")

        initial_normal_depth(
            nwm_id,
            f"{nwm_id}_ind",
            conflation_parameters[nwm_id],
            new_ras_project_text_file,
            subset_gpkg_path,
        )
