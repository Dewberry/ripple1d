import json
import os

from ripple.ops.subset_gpkg import new_gpkg

if __name__ == "__main__":

    ras_project_directory = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter"
    mip_source_model = ras_project_directory
    ras_gpkg_file_path = (
        r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\Baxter.gpkg"
    )
    conflation_json_path = (
        r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\Baxter-ripple-params.json"
    )
    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    ripple_parameters = {}
    for nwm_id in conflation_parameters.keys():
        print(f"working on {nwm_id}")
        ripple_parameters[nwm_id] = new_gpkg(
            mip_source_model,
            os.path.join(ras_project_directory, nwm_id),
            ras_gpkg_file_path,
            nwm_id,
            conflation_parameters[nwm_id],
        )
    with open(conflation_json_path, "w") as f:
        json.dump(ripple_parameters, f, indent=4)
