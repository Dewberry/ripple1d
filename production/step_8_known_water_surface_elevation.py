import json

import numpy as np

from ripple.ops.run_ras_model import (
    establish_order_of_nwm_ids,
    get_kwse_from_ds_model,
    known_wse,
)

if __name__ == "__main__":

    conflation_json_path = (
        r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\Baxter-ripple-params.json"
    )
    depth_increment = 2
    default_min_elevation = 20  # when no downstream model
    default_max_elevation = 45

    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)
    ordered_ids = establish_order_of_nwm_ids(conflation_parameters)

    for e, nwm_id in enumerate(ordered_ids):
        ds_nwm_id = ordered_ids[e - 1]
        print(f"nwm_id={nwm_id} | ds_nwm_id={ds_nwm_id}")

        ds_nwm_ras_project_file = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\{ds_nwm_id}\{ds_nwm_id}.prj"

        ras_project_text_file = (
            rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\{nwm_id}\{nwm_id}.prj"
        )
        subset_gpkg_path = (
            rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\{nwm_id}\{nwm_id}.gpkg"
        )
        terrain_path = (
            rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\{nwm_id}\Terrain.hdf"
        )

        if e == 0:
            min_elevation = default_min_elevation
            max_elevation = default_max_elevation
        elif "messages" in conflation_parameters[nwm_id]:
            continue
        else:
            min_elevation, max_elevation = get_kwse_from_ds_model(
                ds_nwm_id,
                ds_nwm_ras_project_file,
                [f"{ds_nwm_id}_nd", f"{ds_nwm_id}_kwse"],
            )

        known_wse(
            nwm_id,
            f"{nwm_id}_kwse",
            f"{nwm_id}_nd",
            ras_project_text_file,
            subset_gpkg_path,
            terrain_path,
            min_elevation,
            max_elevation,
            depth_increment,
        )
