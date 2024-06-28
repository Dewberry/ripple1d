import geopandas as gpd
import numpy as np
import pandas as pd

from ripple.consts import DEFAULT_EPSG, MIN_FLOW
from ripple.data_model import FlowChangeLocation
from ripple.ras import RasManager
from ripple.utils.ripple_utils import (
    create_flow_depth_combinations,
    determine_flow_increments,
    get_flow_depth_arrays,
)


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
    """Write and compute initial normal depth runs to develop initial rating curves."""
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
    """Write and compute incremental normal depth runs to develop rating curves and depth grids."""
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
    """Write and compute known water surface elevation runs to develop rating curves and depth grids."""
    print(f"working on known water surface elevation run for nwm_id: {nwm_id}")

    known_water_surface_elevations = np.array(known_water_surface_elevations, dtype=float)

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


def get_flow_depth_arrays(
    rm: RasManager, river: str, reach: str, river_station: str, thalweg: float
) -> tuple[pd.Series]:
    """Create new flow, depth,wse arrays from rating curve-plans results."""
    # read in flow/wse
    wses, flows = rm.plan.read_rating_curves()

    # get the river_reach_rs for the cross section representing the upstream end of this reach
    river_reach_rs = f"{river} {reach} {str(river_station)}"

    wse = wses.loc[river_reach_rs, :]
    flow = flows.loc[river_reach_rs, :]
    df = pd.DataFrame({"wse": wse.astype(int), "flow": flow.round(1)}).drop_duplicates()

    # convert wse to depth
    depth = df["wse"] - thalweg

    return (df["flow"], depth, df["wse"])


def determine_flow_increments(
    rm: RasManager,
    plan_name: str,
    river: str,
    reach: str,
    nwm_id: str,
    depth_increment: float = 0.5,
) -> tuple[np.array]:
    """Detemine flow increments corresponding to 0.5 ft depth increments using the rating-curve-run results."""
    rm.plan = rm.plans[plan_name]

    river_station = rm.geoms[nwm_id].rivers[nwm_id][nwm_id].us_xs.river_station
    thalweg = rm.geoms[nwm_id].rivers[nwm_id][nwm_id].us_xs.thalweg

    # get new flow/depth for current branch
    flows, depths, _ = get_flow_depth_arrays(rm, river, reach, river_station, thalweg)

    # get new flow/depth incremented every x ft
    new_depths, new_flows = create_flow_depth_array(flows, depths, depth_increment)

    new_wse = new_depths + thalweg  # [i + thalweg for i in new_depths]

    return new_flows.astype(int), new_depths, new_wse


def create_flow_depth_combinations(
    ds_depths: list, ds_wses: list, input_flows: np.array, min_depths: pd.Series
) -> tuple:
    """
    Create flow-depth-wse combinations.

    Args:
        ds_depths (list): downstream depths
        ds_wses (list): downstream water surface elevations
        input_flows (np.array): Flows to create profiles names from. Combine with incremental depths
            of the downstream cross section of the reach
        min_depths (pd.Series): minimum depth to be included. (typically derived from a previous noraml depth run)

    Returns
    -------
        tuple: tuple of depths, flows, and wses
    """
    depths, flows, wses = [], [], []
    for wse, depth in zip(ds_wses, ds_depths):

        for flow in input_flows:
            if depth >= min_depths.loc[str(int(flow))]:

                depths.append(round(depth, 1))
                flows.append(int(max([flow, MIN_FLOW])))
                wses.append(round(wse, 1))
    return (depths, flows, wses)


def get_kwse_from_ds_model(ds_nwm_id: str, ds_nwm_ras_project_file: str, plan_name: str) -> list:
    """Get the kwse values from the downstream model."""
    rm = RasManager(ds_nwm_ras_project_file, crs=DEFAULT_EPSG)

    if plan_name not in rm.plans.keys():
        print(f"{plan_name} is not an existing plan in the specified HEC-RAS model")
        return np.array([])
    rm.plan = rm.plans[plan_name]

    return determine_flow_increments(rm, plan_name, ds_nwm_id, ds_nwm_id, ds_nwm_id)[2]


def establish_order_of_nwm_ids(conflation_parameters: dict) -> list[str]:
    """Establish the order of NWM IDs based on the cross section IDs."""
    order = []
    for id, data in conflation_parameters.items():
        if conflation_parameters[id]["us_xs"]["xs_id"] == "-9999":
            print(f"skipping {id}; no cross sections conflated.")
        else:
            order.append((float(data["us_xs"]["xs_id"]), id))
    order.sort()
    return [i[1] for i in order]


def create_flow_depth_array(flow: list[float], depth: list[float], increment: float = 0.5) -> tuple[np.array]:
    """Interpolate flow values to a new depth array with a specified increment."""
    min_depth = np.min(depth)
    max_depth = np.max(depth)
    start_depth = np.floor(min_depth * 2) / 2  # round down to nearest .0 or .5
    new_depth = np.arange(start_depth, max_depth + increment, increment)
    new_flow = np.interp(new_depth, np.sort(depth), np.sort(flow))

    return new_depth, new_flow


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
