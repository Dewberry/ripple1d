import geopandas as gpd
import numpy as np

from ripple.consts import MIN_FLOW
from ripple.data_model import FlowChangeLocation
from ripple.process import (
    create_flow_depth_combinations,
    determine_flow_increments,
    get_flow_depth_arrays,
)
from ripple.ras import RasManager


def initial_normal_depth(
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
            max([conflation_parameters["low_flow_cfs"], MIN_FLOW]),
            conflation_parameters["high_flow_cfs"],
            number_of_discharges_for_initial_normal_depth_runs,
        ).astype(int)

        # # write and compute initial normal depth runs to develop rating curves
        fcl = FlowChangeLocation(
            nwm_id,
            nwm_id,
            rm.geoms[nwm_id].rivers[nwm_id][nwm_id].us_xs.river_station,
            initial_flows,
        )

        rm.normal_depth_run(
            plan_name,
            nwm_id,
            [fcl],
            initial_flows.astype(str),
            write_depth_grids=False,
        )


def incremental_normal_depth(
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

        fcl = FlowChangeLocation(
            nwm_id,
            nwm_id,
            rm.geoms[nwm_id].rivers[nwm_id][nwm_id].us_xs.river_station,
            flows,
        )
        # write and compute flow/plans for normal_depth run
        rm.normal_depth_run(
            plan_name,
            nwm_id,
            [fcl],
            flows.astype(str),
            write_depth_grids=True,
        )


def known_wse(
    nwm_id: str,
    plan_name: str,
    ras_project_text_file: str,
    subset_gpkg_path: str,
    terrain_path: str,
    known_water_surface_elevations: list,
):

    print(f"working on known water surface elevation run for nwm_id: {nwm_id}")

    crs = gpd.read_file(subset_gpkg_path).crs

    # write and compute flow/plans for known water surface elevation runs
    rm = RasManager(ras_project_text_file, version="631", terrain_path=terrain_path, crs=crs)

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


# if __name__ == "__main__":
#     conflation_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJ Main.json"
#     with open(conflation_json_path) as f:
#         conflation_parameters = json.load(f)

#     for nwm_id in conflation_parameters.keys():
#         print(nwm_id)
#         new_ras_project_text_file = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\{nwm_id}.prj"
#         subset_gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\{nwm_id}.gpkg"
#         initial_normal_depth(
#             nwm_id,
#             f"{nwm_id}_ind",
#             conflation_parameters[nwm_id],
#             new_ras_project_text_file,
#             subset_gpkg_path,
#         )


# if __name__ == "__main__":
#     conflation_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJ Main.json"

#     with open(conflation_json_path) as f:
#         conflation_parameters = json.load(f)

#     for nwm_id in conflation_parameters.keys():
#         ras_project_text_file = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\{nwm_id}.prj"
#         subset_gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\{nwm_id}.gpkg"
#         terrain_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\Terrain.hdf"

#         incremental_normal_depth(
#             nwm_id,
#             f"{nwm_id}_nd",
#             f"{nwm_id}_ind",
#             conflation_parameters[nwm_id],
#             ras_project_text_file,
#             subset_gpkg_path,
#             terrain_path,
#         )


# if __name__ == "__main__":

#     conflation_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJ Main.json"

#     with open(conflation_json_path) as f:
#         conflation_parameters = json.load(f)
#     ordered_ids = establish_order_of_nwm_ids(conflation_parameters)

#     for e, nwm_id in enumerate(ordered_ids):
#         ds_nwm_id = ordered_ids[e - 1]
#         print(f"nwm_id={nwm_id} | ds_nwm_id={ds_nwm_id}")

#         ds_nwm_ras_project_file = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{ds_nwm_id}\{ds_nwm_id}.prj"

#         ras_project_text_file = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\{nwm_id}.prj"
#         subset_gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\{nwm_id}.gpkg"
#         terrain_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\Terrain.hdf"

#         if e == 0:
#             known_water_surface_elevations = np.arange(42, 55, 0.5)
#         elif "messages" in conflation_parameters[nwm_id]:
#             continue
#         else:
#             known_water_surface_elevations = np.concatenate(
#                 [
#                     get_kwse_from_ds_model(ds_nwm_id, ds_nwm_ras_project_file, f"{ds_nwm_id}_nd"),
#                     get_kwse_from_ds_model(ds_nwm_id, ds_nwm_ras_project_file, f"{ds_nwm_id}_kwse"),
#                 ]
#             )

#         known_wse(
#             nwm_id,
#             f"{nwm_id}_kwse",
#             ras_project_text_file,
#             subset_gpkg_path,
#             terrain_path,
#             known_water_surface_elevations,
#         )
