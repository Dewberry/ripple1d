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
from shapely.geometry import LineString

from .consts import DEFAULT_EPSG, MINDEPTH, NORMAL_DEPTH, TERRAIN_NAME
from .errors import DepthGridNotFoundError
from .ras2 import RasManager, RasMap
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
    rm.plan = rm.plans[str(nwm_id) + "_ind"]

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

    if not dest_directory:
        dest_directory = rm.postprocessed_output_folder

    if os.path.exists(dest_directory):
        raise FileExistsError(dest_directory)

    for prefix in ["_kwse", "_nd"]:
        id = nwm_id + prefix

        for profile_name in rm.plans[id].flow.profile_names:
            # construct the default path to the depth grid for this plan/profile
            src_path = os.path.join(rm.ras_folder, str(id), f"Depth ({profile_name}).vrt")

            # if the depth grid path does not exists print a warning then continue to the next profile
            if not os.path.exists(src_path):
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

            dest_directory = os.path.join(dest_directory, id, depth)
            if not os.path.exists(dest_directory):
                os.makedirs(dest_directory)
            dest_path = os.path.join(dest_directory, f"{flow}.tif")

            # open the src raster the cross section concave hull as a mask
            with rasterio.open(src_path) as src:
                dataset = src.read(1)
                transform = src.transform
                out_meta = src.meta

            # update metadata
            out_meta.update(
                {
                    "driver": "GTiff",
                    "height": dataset.shape[1],
                    "width": dataset.shape[2],
                    "transform": transform,
                    "compress": "LZW",
                    "predictor": 3,
                    "tiled": True,
                }
            )

            # write dest raster
            logging.info(f"Writing: {dest_path}")
            with rasterio.open(dest_path, "w", **out_meta) as dest:
                dest.write(dataset)
            # logging.debug(f"Building overviews for: {dest_path}")
            with rasterio.Env(COMPRESS_OVERVIEW="DEFLATE", PREDICTOR_OVERVIEW="3"):
                with rasterio.open(dest_path, "r+") as dst:
                    dst.build_overviews([4, 8, 16], Resampling.nearest)
                    dst.update_tags(ns="rio_overview", resampling="nearest")


def initialize_new_ras_project_from_gpkg(
    ras_project_dir: str, nwm_id, ras_gpkg_file_path: str, version: str = "631", terrain_name: str = TERRAIN_NAME
):

    ras_project_text_file = os.path.join(ras_project_dir, f"{nwm_id}.prj")

    rm = RasManager(
        ras_project_text_file,
        version,
        terrain_name=terrain_name,
        projection=gpd.read_file(ras_gpkg_file_path).crs,
        new_project=True,
    )

    rm.new_geom_from_gpkg(ras_gpkg_file_path, nwm_id)
    rm.ras_project.write_contents()
    return rm, ras_project_text_file


