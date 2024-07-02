"""Create FIM library."""

import json
import logging
import os

from ripple.ops.create_fim_lib import new_fim_lib
from ripple.ripple_logger import configure_logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    SAMPLE_DATA = os.path.dirname(__file__).replace("production", "sample_data\\Baxter")
    conflation_json_path = os.path.join(SAMPLE_DATA, "Baxter-ripple-params.json")

    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    for nwm_id in conflation_parameters.keys():

        logging.info(f"Working on: {nwm_id}")
        ras_project_text_file = os.path.join(SAMPLE_DATA, f"{nwm_id}\\{nwm_id}.prj")
        subset_gpkg_path = os.path.join(SAMPLE_DATA, f"{nwm_id}\\{nwm_id}.gpkg")
        terrain_path = os.path.join(SAMPLE_DATA, f"{nwm_id}\\Terrain.hdf")
        nd_plan_name = f"{nwm_id}_nd"
        kwse_plan_name = f"{nwm_id}_kwse"

        new_fim_lib(
            nwm_id,
            conflation_parameters[nwm_id],
            ras_project_text_file,
            nd_plan_name,
            kwse_plan_name,
            terrain_path,
            subset_gpkg_path,
        )
