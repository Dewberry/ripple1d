from __future__ import annotations
import geopandas as gpd
import pandas as pd
import os
import rasterio
import numpy as np
from ras import Ras


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


def compile_flows(
    nwm_reach_gdf: gpd.GeoDataFrame, min_ratio: float = 0.8, max_ratio: float = 1.5, increments: int = 10
) -> gpd.GeoDataFrame:
    """
    Determine flows to apply to the model for an initial rating curve by compiling the 2yr-100yr

    Args:
        nwm_reach_gdf (gpd.GeoDataFrame): National water model branches
        min_ratio (float, optional): Ratio to multiply the 2yr event by to get the min flow. Defaults to .8.
        max_ratio (float, optional): Ratio to multiply the 100yr event by to get the max flow. Defaults to 1.5.
        increments (int,optional): Number of flow increments between 2yr flow * min_ration and 100yr flow * max_ratio

    Returns:
        gpd.GeoDataFrame: _description_
    """
    flows = []
    for row in nwm_reach_gdf.itertuples():

        flow = np.linspace(row.min_flow_cfs, row.max_flow_cfs, increments)

        flow.sort()

        flows.append(list(flow.round()))

    nwm_reach_gdf["flows_rc"] = flows

    return nwm_reach_gdf


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
        }
    )

    # write dest raster
    with rasterio.open(dest_path, "w", **out_meta) as dest:
        dest.write(out_image)

    return dest_path
