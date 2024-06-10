import json

import numpy as np

from .process import (
    initialize_new_ras_project_from_gpkg,
)


def main(
    nwm_id: str,
    nwm_data: dict,
    ras_project_directory: str,
    subset_gpkg_path: str,
    terrain_name: str,
    number_of_discharges_for_initial_normal_depth_runs: int = 10,
    version: str = "631",
):
    print(f"working on initial normal depth run for nwm_id: {nwm_id}")

    # create new ras manager class
    rm, ras_project_text_file = initialize_new_ras_project_from_gpkg(
        ras_project_directory, nwm_id, subset_gpkg_path, version, terrain_name
    )

    # increment flows based on min and max flows specified in conflation parameters
    initial_flows = np.linspace(
        nwm_data["low_flow_cfs"], nwm_data["high_flow_cfs"], number_of_discharges_for_initial_normal_depth_runs
    )

    # # write and compute initial normal depth runs to develop rating curves
    rm.normal_depth_run(
        nwm_id + "_ind",
        nwm_id,
        initial_flows,
        nwm_id,
        nwm_id,
        rm.geoms[nwm_id].xs_gdf["river_station"].max(),
        write_depth_grids=False,
    )


if __name__ == "__main__":
    nwm_id = "2826228"
    ras_project_directory = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}"
    subset_gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}.gpkg"
    terrain_name = "Terrain"
    json_path = (
        r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\baxter-ripple-params copy.json"
    )

    with open(json_path) as f:
        ripple_parameters = json.load(f)

    main(nwm_id, ripple_parameters[nwm_id], ras_project_directory, subset_gpkg_path, terrain_name)
