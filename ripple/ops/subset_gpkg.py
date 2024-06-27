import json
import os

from ripple.process import subset_gpkg


def new_gpkg(
    ras_project_directory: str,
    ras_gpkg_file_path: str,
    nwm_id: str,
    ripple_parameters: dict,
    ripple_version,
):
    """
    Using ripple conflation data, creates a new GPKG from an existing ras geopackage
    """

    if ripple_parameters["us_xs"]["xs_id"] == "-9999":
        ripple_parameters["messages"] = f"skipping {nwm_id}; no cross sections conflated."
        print(ripple_parameters["messages"])
    else:
        subset_gpkg_path, crs = subset_gpkg(
            ras_gpkg_file_path,
            ras_project_directory,
            nwm_id,
            ripple_parameters["ds_xs"]["xs_id"],
            ripple_parameters["us_xs"]["xs_id"],
            ripple_parameters["us_xs"]["river"],
            ripple_parameters["us_xs"]["reach"],
            ripple_parameters["ds_xs"]["river"],
            ripple_parameters["ds_xs"]["reach"],
        )

        ripple_parameters["files"] = {"gpkg": subset_gpkg_path}
        ripple_parameters["crs"] = crs
        ripple_parameters["version"] = ripple_version

        with open(os.path.join(ras_project_directory, f"{nwm_id}.ripple.json"), "w") as f:
            json.dump({nwm_id: ripple_parameters}, f, indent=4)

    return ripple_parameters


# if __name__ == "__main__":

#     ras_project_directory = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models"
#     ras_gpkg_file_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJMain.gpkg"
#     conflation_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJ Main.json"
#     ripple_version = "0.0.1"
#     with open(conflation_json_path) as f:
#         conflation_parameters = json.load(f)

#     ripple_parameters = {}
#     for nwm_id in conflation_parameters.keys():
#         print(f"working on {nwm_id}")
#         ripple_parameters[nwm_id] = new_gpkg(
#             os.path.join(ras_project_directory, nwm_id),
#             ras_gpkg_file_path,
#             nwm_id,
#             conflation_parameters[nwm_id],
#             ripple_version,
#         )
#     with open(conflation_json_path, "w") as f:
#         json.dump(ripple_parameters, f, indent=4)
