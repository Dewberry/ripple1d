import json
import logging
import os

from ripple.ops.create_ras_terrain import new_ras_terrain
from ripple.ripple_logger import configure_logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    SAMPLE_DATA = os.path.dirname(__file__).replace("production", "sample_data\\Baxter")

    conflation_json_path = os.path.join(SAMPLE_DATA, "Baxter-ripple-params.json")

    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    for nwm_id in conflation_parameters.keys():
        print(f"working on {nwm_id}")
        output_terrain_hdf_filepath = os.path.join(SAMPLE_DATA, f"{nwm_id}\\Terrain.hdf")
        gpkg_path = os.path.join(SAMPLE_DATA, f"{nwm_id}\\{nwm_id}.gpkg")

        new_ras_terrain(output_terrain_hdf_filepath, gpkg_path, conflation_parameters[nwm_id], nwm_id)
