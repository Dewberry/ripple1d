import json
import os

from ripple.process import subset_gpkg


def main(ras_project_directory: str, ras_gpkg_file_path: str, nwm_id: str, conflation_parameters: dict):

    if conflation_parameters["us_xs"]["xs_id"] == "-9999":
        print(f"skipping {nwm_id}; no cross sections conflated.")
    else:
        subset_gpkg_path = subset_gpkg(
            ras_gpkg_file_path,
            ras_project_directory,
            nwm_id,
            conflation_parameters["ds_xs"]["xs_id"],
            conflation_parameters["us_xs"]["xs_id"],
            conflation_parameters["us_xs"]["river"],
            conflation_parameters["us_xs"]["reach"],
            conflation_parameters["ds_xs"]["river"],
            conflation_parameters["ds_xs"]["reach"],
        )


if __name__ == "__main__":

    ras_project_directory = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMAIN\nwm_models"
    ras_gpkg_file_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJMAIN.gpkg"
    conflation_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJ Main.json"

    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    for nwm_id in conflation_parameters.keys():
        print(f"working on {nwm_id}")
        main(os.path.join(ras_project_directory, nwm_id), ras_gpkg_file_path, nwm_id, conflation_parameters[nwm_id])

import os

"1468322" in os.listdir(r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models")
