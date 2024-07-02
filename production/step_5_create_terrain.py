import json

from ripple.ops.create_ras_terrain import new_ras_terrain

if __name__ == "__main__":
    conflation_json_path = (
        r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\baxter-ripple-params.json"
    )

    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    for nwm_id in conflation_parameters.keys():
        print(f"working on {nwm_id}")
        output_terrain_hdf_filepath = (
            rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\{nwm_id}\Terrain.hdf"
        )
        gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\nwm_models\{nwm_id}\{nwm_id}.gpkg"

        new_ras_terrain(output_terrain_hdf_filepath, gpkg_path, conflation_parameters[nwm_id], nwm_id)
