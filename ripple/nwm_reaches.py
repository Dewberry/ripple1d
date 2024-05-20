from __future__ import annotations
import geopandas as gpd
import pandas as pd
import os
import rasterio
from rasterio.enums import Resampling
import numpy as np

from ras import Ras

# from osgeo import gdal


def get_us_ds_rs(nwm_reach_gdf: gpd.GeoDataFrame, r: Ras):

    xs = r.geom.cross_sections

    xs = xs.sjoin(nwm_reach_gdf.to_crs(xs.crs))

    us, ds, rivers, reaches = [], [], [], []
    for id in nwm_reach_gdf["branch_id"]:

        us.append(xs.loc[xs["branch_id"] == id, "rs"].max())
        ds.append(xs.loc[xs["branch_id"] == id, "rs"].min())

        river = xs.loc[xs["rs"] == us[-1], "river"].iloc[0]
        reach = xs.loc[xs["rs"] == us[-1], "reach"].iloc[0]

        rivers.append(river)
        reaches.append(reach)

    nwm_reach_gdf["us_rs"] = us
    nwm_reach_gdf["ds_rs"] = ds

    nwm_reach_gdf["river"] = rivers
    nwm_reach_gdf["reach"] = reaches

    return nwm_reach_gdf, r, xs


def increment_rc_flows(nwm_dict: dict, increments: int = 10) -> dict:
    """
    Determine flows to apply to the model for an initial rating curve by compiling the 2yr-100yr

    Args:
        nwm_dict (dict): National water model branches
        increments (int,optional): Number of flow increments between 2yr flow * min_ration and 100yr flow * max_ratio

    Returns:
        dict: _description_
    """

    for branch_id, branch_data in nwm_dict.items():

        flow = np.linspace(
            branch_data["flows"]["flow_2_yr_minus"], branch_data["flows"]["flow_100_yr_plus"], increments
        )

        flow.sort()

        nwm_dict[branch_id]["flows_rc"] = flow

    return nwm_dict


def clip_depth_grid(
    src_path: str,
    xs_hull: gpd.GeoDataFrame,
    id: str,
    profile_name: str,
    dest_directory: str,
):

    flow, depth = profile_name.split("-")

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
    print(f"Writing: {dest_path}")
    with rasterio.open(dest_path, "w", **out_meta) as dest:
        dest.write(out_image)
    # print(f"Building overviews for: {dest_path}")
    with rasterio.Env(COMPRESS_OVERVIEW="DEFLATE", PREDICTOR_OVERVIEW="3"):
        with rasterio.open(dest_path, "r+") as dst:
            dst.build_overviews([4, 8, 16], Resampling.nearest)
            dst.update_tags(ns="rio_overview", resampling="nearest")

    return dest_path
