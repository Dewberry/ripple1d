import json

import numpy as np

from ripple.ras import RasManager


def main(
    nwm_id: str,
    plan_name: str,
    conflation_parameters: dict,
    new_ras_project_text_file: str,
    subset_gpkg_path: str,
    terrain_path: str = None,
    number_of_discharges_for_initial_normal_depth_runs: int = 10,
    version: str = "631",
):

    if conflation_parameters["us_xs"]["xs_id"] == "-9999":
        print(f"skipping {nwm_id}; no cross sections conflated.")
    else:
        print(f"working on initial normal depth run for nwm_id: {nwm_id}")

        # create new ras manager class
        rm = RasManager.from_gpkg(new_ras_project_text_file, nwm_id, subset_gpkg_path, version, terrain_path)

        # increment flows based on min and max flows specified in conflation parameters
        initial_flows = np.linspace(
            conflation_parameters["low_flow_cfs"],
            conflation_parameters["high_flow_cfs"],
            number_of_discharges_for_initial_normal_depth_runs,
        )

        # # write and compute initial normal depth runs to develop rating curves
        rm.normal_depth_run(
            plan_name,
            nwm_id,
            initial_flows,
            nwm_id,
            nwm_id,
            rm.geoms[nwm_id].rivers[nwm_id][nwm_id].us_xs.river_station,
            write_depth_grids=False,
        )


if __name__ == "__main__":
    conflation_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJ Main.json"
    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    for nwm_id in conflation_parameters.keys():
        print(nwm_id)
        new_ras_project_text_file = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMAIN\nwm_models\{nwm_id}\{nwm_id}.prj"
        subset_gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMAIN\nwm_models\{nwm_id}\{nwm_id}.gpkg"
        main(nwm_id, f"{nwm_id}_ind", conflation_parameters[nwm_id], new_ras_project_text_file, subset_gpkg_path)
