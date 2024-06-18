import json

import geopandas as gpd

from ripple.data_model import FlowChangeLocation
from ripple.process import (
    determine_flow_increments,
)
from ripple.ras import RasManager


def main(
    nwm_id: str,
    plan_name: str,
    initial_normal_depth_plan_title: str,
    conflation_parameters: dict,
    ras_project_text_file: str,
    subset_gpkg_path: str,
    terrain_path: str,
    version: str = "631",
):
    print(f"working on normal depth run for nwm_id: {nwm_id}")
    if conflation_parameters["us_xs"]["xs_id"] == "-9999":
        print(f"skipping {nwm_id}; no cross sections conflated.")
    else:
        crs = gpd.read_file(subset_gpkg_path).crs

        rm = RasManager(ras_project_text_file, version=version, terrain_path=terrain_path, crs=crs)

        # determine flow increments
        flows, depths, wses = determine_flow_increments(
            rm,
            initial_normal_depth_plan_title,
            nwm_id,
            nwm_id,
            nwm_id,
        )

        fcl = FlowChangeLocation(nwm_id, nwm_id, rm.geoms[nwm_id].rivers[nwm_id][nwm_id].us_xs.river_station, flows)
        # write and compute flow/plans for normal_depth run
        rm.normal_depth_run(
            plan_name,
            nwm_id,
            [fcl],
            flows.astype(str),
            write_depth_grids=True,
        )


if __name__ == "__main__":
    conflation_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJ Main.json"

    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    for nwm_id in conflation_parameters.keys():
        ras_project_text_file = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\{nwm_id}.prj"
        subset_gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\{nwm_id}.gpkg"
        terrain_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\Terrain.hdf"

        main(
            nwm_id,
            f"{nwm_id}_nd",
            f"{nwm_id}_ind",
            conflation_parameters[nwm_id],
            ras_project_text_file,
            subset_gpkg_path,
            terrain_path,
        )
