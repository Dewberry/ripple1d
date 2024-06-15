import json

import geopandas as gpd
import numpy as np

from ripple.process import (
    create_flow_depth_combinations,
    establish_order_of_nwm_ids,
    get_flow_depth_arrays,
    get_kwse_from_ds_model,
)
from ripple.ras import RasManager


def main(
    nwm_id: str,
    plan_name: str,
    ras_project_text_file: str,
    subset_gpkg_path: str,
    terrain_path: str,
    known_water_surface_elevations: list,
):

    print(f"working on known water surface elevation run for nwm_id: {nwm_id}")

    projection = gpd.read_file(subset_gpkg_path).crs

    # write and compute flow/plans for known water surface elevation runs
    rm = RasManager(ras_project_text_file, version="631", terrain_path=terrain_path, projection=projection)

    # get resulting depths from the second normal depth runs_nd
    rm.plan = rm.plans[nwm_id + "_nd"]
    ds_flows, ds_depths, _ = get_flow_depth_arrays(
        rm,
        nwm_id,
        nwm_id,
        rm.geoms[nwm_id].rivers[nwm_id][nwm_id].ds_xs.river_station,
        rm.geoms[nwm_id].rivers[nwm_id][nwm_id].ds_xs.thalweg,
    )

    known_depths = known_water_surface_elevations - rm.geoms[nwm_id].rivers[nwm_id][nwm_id].ds_xs.thalweg

    # filter known water surface elevations less than depths resulting from the second normal depth run
    depths, flows, wses = create_flow_depth_combinations(
        known_depths,
        known_water_surface_elevations,
        ds_flows,
        ds_depths,
    )

    if not flows:
        print(
            f"No contoling known water surface elevations were identified for {nwm_id}; i.e., the depth of flooding\
 for the normal depth run for a given flow was alway higher than the known water surface elevations of the downstream reach"
        )
    else:
        rm.kwses_run(
            plan_name,
            nwm_id,
            depths,
            wses,
            flows,
            nwm_id,
            nwm_id,
            rm.geoms[nwm_id].rivers[nwm_id][nwm_id].us_xs.river_station,
            write_depth_grids=True,
        )


if __name__ == "__main__":

    conflation_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJ Main.json"

    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)
    ordered_ids = establish_order_of_nwm_ids(conflation_parameters)

    for e, nwm_id in enumerate(ordered_ids):
        ds_nwm_id = ordered_ids[e - 1]
        print(f"nwm_id={nwm_id} | ds_nwm_id={ds_nwm_id}")

        ds_nwm_ras_project_file = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{ds_nwm_id}\{ds_nwm_id}.prj"

        ras_project_text_file = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\{nwm_id}.prj"
        subset_gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\{nwm_id}.gpkg"
        terrain_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\Terrain.hdf"

        if e == 0:
            known_water_surface_elevations = np.arange(42, 55, 0.5)
        elif "messages" in conflation_parameters[nwm_id]:
            continue
        else:
            known_water_surface_elevations = np.concatenate(
                [
                    get_kwse_from_ds_model(ds_nwm_id, ds_nwm_ras_project_file, f"{ds_nwm_id}_nd"),
                    get_kwse_from_ds_model(ds_nwm_id, ds_nwm_ras_project_file, f"{ds_nwm_id}_kwse"),
                ]
            )

        main(
            nwm_id,
            f"{nwm_id}_kwse",
            ras_project_text_file,
            subset_gpkg_path,
            terrain_path,
            known_water_surface_elevations,
        )
