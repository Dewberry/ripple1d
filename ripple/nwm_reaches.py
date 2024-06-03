from __future__ import annotations

import logging
import os

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.enums import Resampling


def increment_rc_flows(conflation_params: dict, increments: int = 10) -> dict:
    """
    Determine flows to apply to the model for an initial rating
    curve incremented between flow_2_yr_minus and flow_100_yr_plus

    Args:
        conflation_params (dict): National Water Model conflation data
        increments (int,optional): Number of flow increments
        between flow_2_yr_minus and flow_100_yr_plus

    Returns:
        dict: _description_
    """

    for branch_id, branch_data in conflation_params.items():

        flow = np.linspace(
            branch_data["flows"]["flow_2_yr_minus"], branch_data["flows"]["flow_100_yr_plus"], increments
        )

        flow.sort()

        conflation_params[branch_id]["flows_rc"] = flow

    return conflation_params


def clip_depth_grid(
    src_path: str,
    xs_hull: gpd.GeoDataFrame,
    id: str,
    profile_name: str,
    dest_directory: str,
):
    """
    Clip the depth raster to a concave hull of the cross section associated with NWM branch.
    """
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

        out_image, out_transform = rasterio.mask.mask(
            src, xs_hull.to_crs(src.crs)["geometry"], crop=True, all_touched=True
        )
        out_meta = src.meta

    # update metadata
    out_meta.update(
        {
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
            "compress": "LZW",
            "predictor": 3,
            "tiled": True,
        }
    )
    # write dest raster
    logging.info(f"Writing: {dest_path}")
    with rasterio.open(dest_path, "w", **out_meta) as dest:
        dest.write(out_image)
    # logging.debug(f"Building overviews for: {dest_path}")
    with rasterio.Env(COMPRESS_OVERVIEW="DEFLATE", PREDICTOR_OVERVIEW="3"):
        with rasterio.open(dest_path, "r+") as dst:
            dst.build_overviews([4, 8, 16], Resampling.nearest)
            dst.update_tags(ns="rio_overview", resampling="nearest")
