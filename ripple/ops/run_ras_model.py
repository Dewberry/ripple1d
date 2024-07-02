import json

import geopandas as gpd
import numpy as np
import pandas as pd

from ripple.consts import DEFAULT_EPSG, MIN_FLOW
from ripple.data_model import FlowChangeLocation
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
    """Write and compute initial normal depth runs to develop initial rating curves."""
    if conflation_parameters["us_xs"]["xs_id"] == "-9999":
        logging.warning(f"skipping {nwm_id}; no cross sections conflated.")
    else:
        loging.info(f"Working on initial normal depth run for nwm_id: {nwm_id}")

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
    depth_increment=0.5,
):
    """Write and compute incremental normal depth runs to develop rating curves and depth grids."""
    logging.info(f"Working on normal depth run for nwm_id: {nwm_id}")
    if conflation_parameters["us_xs"]["xs_id"] == "-9999":
        logging.warning(f"skipping {nwm_id}; no cross sections conflated.")
    else:
        crs = gpd.read_file(subset_gpkg_path, layer="XS").crs

        rm = RasManager(ras_project_text_file, version=version, terrain_path=terrain_path, crs=crs)

        # determine flow increments
        flows, depths, wses = determine_flow_increments(
            rm, [initial_normal_depth_plan_title], nwm_id, nwm_id, nwm_id, depth_increment=depth_increment
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
    incremental_normal_depth_plan_name: str,
    ras_project_text_file: str,
    subset_gpkg_path: str,
    terrain_path: str,
    min_elevation: float,
    max_elevation: float,
    depth_increment: float,
):
    """Write and compute known water surface elevation runs to develop rating curves and depth grids."""
    logging.info(f"Working on known water surface elevation run for nwm_id: {nwm_id}")

    start_elevation = np.floor(min_elevation * 2) / 2  # round down to nearest .0 or .5
    known_water_surface_elevations = np.arange(start_elevation, max_elevation + depth_increment, depth_increment)

    crs = gpd.read_file(subset_gpkg_path, layer="XS").crs

    # write and compute flow/plans for known water surface elevation runs
    rm = RasManager(ras_project_text_file, version="631", terrain_path=terrain_path, crs=crs)

    # get resulting depths from the second normal depth runs_nd
    rm.plan = rm.plans[incremental_normal_depth_plan_name]
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
        logging.warning(
            f"No controling known water surface elevations were identified for {nwm_id}; i.e., the depth of flooding\
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
    plan_names: str,
    river: str,
    reach: str,
    nwm_id: str,
    depth_increment: float = 0.5,
) -> tuple[np.array]:
    """Detemine flow increments corresponding to 0.5 ft depth increments using the rating-curve-run results."""
    flows, depths = [], []
    for plan_name in plan_names:
        rm.plan = rm.plans[plan_name]

        river_station = rm.geoms[nwm_id].rivers[nwm_id][nwm_id].us_xs.river_station
        thalweg = rm.geoms[nwm_id].rivers[nwm_id][nwm_id].us_xs.thalweg

        # get new flow/depth for current branch
        flow, depth, _ = get_flow_depth_arrays(rm, river, reach, river_station, thalweg)
        flows.append(np.array(flow))
        depths.append(np.array(depth))
    # get new flow/depth incremented every x ft
    new_depths, new_flows = create_flow_depth_array(np.concatenate(flows), np.concatenate(depths), depth_increment)

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


def get_kwse_from_ds_model(ds_nwm_id: str, ds_nwm_ras_project_file: str, plan_names: str) -> tuple[float]:
    """Get the kwse values from the downstream model."""
    rm = RasManager(ds_nwm_ras_project_file, crs=DEFAULT_EPSG)
    wses = []
    for plan_name in plan_names:
        if plan_name not in rm.plans.keys():
            logging.warning(f"{plan_name} is not an existing plan in the specified HEC-RAS model")
            return np.array([])

        rm.plan = rm.plans[plan_name]

        river_reach_rs = rm.plan.geom.rivers[ds_nwm_id][ds_nwm_id].us_xs.river_reach_rs
        thalweg = rm.plan.geom.rivers[ds_nwm_id][ds_nwm_id].us_xs.thalweg

        wse, _ = rm.plan.read_rating_curves()

        wses.append(wse.loc[river_reach_rs, :])

        df = pd.concat(wses)
    return df.min(), df.max()


def establish_order_of_nwm_ids(conflation_parameters: dict) -> list[str]:
    """Establish the order of NWM IDs based on the cross section IDs."""
    order = []
    for id, data in conflation_parameters.items():
        if conflation_parameters[id]["us_xs"]["xs_id"] == "-9999":
            logging.warning(f"skipping {id}; no cross sections conflated.")
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
