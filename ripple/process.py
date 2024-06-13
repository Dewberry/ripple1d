from __future__ import annotations

import logging
import os
import shutil

import boto3
import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.shutil import copy as copy_raster
from shapely.geometry import LineString

from .consts import DEFAULT_EPSG, MIN_FLOW
from .errors import DepthGridNotFoundError
from .ras import RasManager, RasMap
from .utils import create_flow_depth_array


def get_flow_depth_arrays(rm: RasManager, river: str, reach: str, river_station: float, thalweg: float) -> tuple:
    """
    Create new flow, depth,wse arrays from rating curve-plans results.
    """
    # read in flow/wse
    wses, flows = rm.plan.read_rating_curves()

    # get the river_reach_rs for the cross section representing the upstream end of this reach
    river_reach_rs = f"{river} {reach} {str(river_station).rstrip('0')}"

    wse = wses.loc[river_reach_rs, :]
    flow = flows.loc[river_reach_rs, :]

    # convert wse to depth
    depth = wse - thalweg

    return (flow, depth, wse)


def determine_flow_increments(
    rm: RasManager,
    plan_name: str,
    river: str,
    reach: str,
    nwm_id: str,
    river_station: float,
    thalweg: float,
    depth_increment: float = 0.5,
) -> RasManager:
    """
    Detemine flow increments corresponding to 0.5 ft depth increments using the rating-curve-run results
    """
    rm.plan = rm.plans[plan_name]

    # get new flow/depth for current branch
    flows, depths, _ = get_flow_depth_arrays(rm, river, reach, river_station, thalweg)

    # get new flow/depth incremented every x ft
    new_depths, new_flows = create_flow_depth_array(flows, depths, depth_increment)

    new_wse = [i + thalweg for i in new_depths]

    return new_flows, new_depths, new_wse


def post_process_depth_grids(
    rm: RasManager, nwm_id: str, nwm_data: dict, except_missing_grid: bool = False, dest_directory=None
):
    """
    Clip depth grids based on their associated NWM branch and respective cross sections.

    """
    missing_grids_kwse, missing_grids_nd = [], []
    for prefix in ["_kwse", "_nd"]:
        id = nwm_id + prefix

        if id not in rm.plans:
            continue
        for profile_name in rm.plans[id].flow.profile_names:
            # construct the default path to the depth grid for this plan/profile
            src_path = os.path.join(rm.ras_project._ras_dir, str(id), f"Depth ({profile_name}).vrt")

            # if the depth grid path does not exists print a warning then continue to the next profile
            if not os.path.exists(src_path):
                if prefix == "_kwse":
                    missing_grids_kwse.append(profile_name)
                elif prefix == "_nd":
                    missing_grids_nd.append(profile_name)
                if except_missing_grid:
                    logging.warning(f"depth raster does not exists: {src_path}")
                    continue
                else:
                    raise DepthGridNotFoundError(f"depth raster does not exists: {src_path}")

            if "_kwse" in id:
                flow, depth = profile_name.split("-")
            elif "_nd" in id:
                flow = f"f_{profile_name}"
                depth = "z_0_0"

            dest_directory = os.path.join(rm.ras_project._ras_dir, "output", nwm_id, depth)
            os.makedirs(dest_directory, exist_ok=True)
            dest_path = os.path.join(dest_directory, f"{flow}.tif")

            copy_raster(src_path, dest_path)

            # logging.debug(f"Building overviews for: {dest_path}")
            with rasterio.Env(COMPRESS_OVERVIEW="DEFLATE", PREDICTOR_OVERVIEW="3"):
                with rasterio.open(dest_path, "r+") as dst:
                    dst.build_overviews([4, 8, 16], Resampling.nearest)
                    dst.update_tags(ns="rio_overview", resampling="nearest")
    return missing_grids_kwse, missing_grids_nd


def subset_gpkg(
    src_gpkg_path: str,
    ras_project_dir: str,
    nwm_id: str,
    ds_rs: str,
    us_rs: str,
    us_river: str,
    us_reach: str,
    ds_river: str,
    ds_reach: str,
):
    # TODO add logic for junctions/multiple river-reaches

    dest_gpkg_path = os.path.join(ras_project_dir, f"{nwm_id}.gpkg")

    # read data
    xs_gdf = gpd.read_file(src_gpkg_path, layer="XS", driver="GPKG")
    river_gdf = gpd.read_file(src_gpkg_path, layer="River", driver="GPKG")
    # if "Junction" in fiona.listlayers(src_gpkg_path):
    #     junction_gdf = gpd.read_file(src_gpkg_path, layer="Junction", driver="GPKG")

    # subset data
    if us_river == ds_river and us_reach == ds_reach:
        xs_subset_gdf = xs_gdf.loc[
            (xs_gdf["river"] == us_river)
            & (xs_gdf["reach"] == us_reach)
            & (xs_gdf["river_station"] >= float(ds_rs))
            & (xs_gdf["river_station"] <= float(us_rs))
        ]

        river_subset_gdf = river_gdf.loc[(river_gdf["river"] == us_river) & (river_gdf["reach"] == us_reach)]
    else:
        xs_us_reach = xs_gdf.loc[
            (xs_gdf["river"] == us_river) & (xs_gdf["reach"] == us_reach) & (xs_gdf["river_station"] <= float(us_rs))
        ]
        xs_ds_reach = xs_gdf.loc[
            (xs_gdf["river"] == ds_river) & (xs_gdf["reach"] == ds_reach) & (xs_gdf["river_station"] >= float(ds_rs))
        ]

        if xs_us_reach["river_station"].min() <= xs_ds_reach["river_station"].max():
            logging.info(
                f"the lowest river station on the upstream reach ({xs_us_reach['river_station'].min()}) is less"
                f" than the highest river station on the downstream reach ({xs_ds_reach['river_station'].max()}) for nwm_id: {nwm_id}"
            )
            xs_us_reach["river_station"] = xs_us_reach["river_station"] + xs_ds_reach["river_station"].max()
            xs_us_reach["ras_data"] = xs_us_reach["ras_data"].apply(
                lambda ras_data: update_river_station(ras_data, xs_ds_reach["river_station"].max())
            )

        xs_subset_gdf = pd.concat([xs_us_reach, xs_ds_reach])

        us_reach = river_gdf.loc[(river_gdf["river"] == us_river) & (river_gdf["reach"] == us_reach)]
        ds_reach = river_gdf.loc[(river_gdf["river"] == ds_river) & (river_gdf["reach"] == ds_reach)]
        coords = list(us_reach.iloc[0]["geometry"].coords) + list(ds_reach.iloc[0]["geometry"].coords)
        river_subset_gdf = gpd.GeoDataFrame(
            {"geometry": [LineString(coords)], "river": [nwm_id], "reach": [nwm_id]},
            geometry="geometry",
            crs=us_reach.crs,
        )

    pd.options.mode.copy_on_write = True
    # rename river reach
    xs_subset_gdf["river"] = nwm_id
    xs_subset_gdf["reach"] = nwm_id
    river_subset_gdf["river"] = nwm_id
    river_subset_gdf["reach"] = nwm_id

    # check if only 1 cross section for nwm_reach
    if len(xs_subset_gdf) <= 1:
        shutil.rmtree(ras_project_dir)
        logging.warning(f"Only 1 cross section conflated to NWM reach {nwm_id}. Skipping this reach.")
        return None

    # write data
    os.makedirs(ras_project_dir, exist_ok=True)
    xs_subset_gdf.to_file(dest_gpkg_path, layer="XS", driver="GPKG")
    river_subset_gdf.to_file(dest_gpkg_path, layer="River", driver="GPKG")

    return dest_gpkg_path


def update_river_station(ras_data, river_station):
    lines = ras_data.splitlines()
    data = lines[0].split(",")
    data[1] = str(float(lines[0].split(",")[1]) + river_station).rstrip("0").ljust(8)
    lines[0] = ",".join(data)
    return "\n".join(lines) + "\n"


def junction_length_to_reach_lengths():
    # TODO adjust reach lengths using junction lengths
    raise NotImplementedError
    # for row in junction_gdf.iterrows():
    #     if us_river in row["us_rivers"] and us_reach in row["us_reach"]:


def create_flow_depth_combinations(
    ds_depths: list, ds_wses: list, input_flows: np.array, min_depths: pd.Series
) -> tuple:
    """
    Create flow-depth-wse combinations

    Args:
        ds_depths (list): downstream depths
        ds_wses (list): downstream water surface elevations
        input_flows (np.array): Flows to create profiles names from. Combine with incremental depths
            of the downstream cross section of the reach
        min_depths (pd.Series): minimum depth to be included. (typically derived from a previous noraml depth run)

    Returns:
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
    rm = RasManager(ds_nwm_ras_project_file, projection=DEFAULT_EPSG)

    if plan_name not in rm.plans.keys():
        print(f"{plan_name} is not an existing plan in the specified HEC-RAS model")
        return []
    rm.plan = rm.plans[plan_name]

    xs_gdf = rm.geoms[ds_nwm_id].xs_gdf
    river_station = xs_gdf["river_station"].max()
    thalweg = xs_gdf.loc[xs_gdf["river_station"] == river_station, "thalweg"][0]

    return determine_flow_increments(rm, plan_name, ds_nwm_id, ds_nwm_id, ds_nwm_id, river_station, thalweg)[2]
