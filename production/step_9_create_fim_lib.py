import json
import logging

from ripple.ops.create_fim_lib import new_fim_lib

if __name__ == "__main__":
    conflation_json_path = (
        r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\Baxter-ripple-params.json"
    )

    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    for nwm_id in conflation_parameters.keys():

        print(f"Working on: {nwm_id}")
        ras_project_text_file = (
            rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\{nwm_id}\{nwm_id}.prj"
        )
        subset_gpkg_path = (
            rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\{nwm_id}\{nwm_id}.gpkg"
        )
        terrain_path = (
            rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\{nwm_id}\Terrain.hdf"
        )
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
