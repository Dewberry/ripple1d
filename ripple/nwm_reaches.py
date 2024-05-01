import geopandas as gpd
import pandas as pd
import os
import rasterio


def get_us_ds_rs(fcl, r):

    # sort flow change locations based on rs
    fcl.sort_values(by="rs", ascending=False, inplace=True)

    # allocate columns for ds_rs and us_rs
    fcl["ds_rs"] = fcl["rs"]
    fcl["us_rs"] = None

    xs = r.geom.cross_sections

    flows = []
    for i, row in nwm_reach_gdf.iterrows():

        flow = np.linspace(row["flow_2_yr"] * min_ratio, row["flow_100_yr"] * max_ratio, increments)

        flow.sort()

        flows.append(list(flow.round()))

    nwm_reach_gdf["flows_rc"] = flows

    return nwm_reach_gdf


def clip_depth_grid(
    src_path: str,
    xs_hull: gpd.GeoDataFrame,
    river: str,
    reach: str,
    us_rs: str,
    ds_rs: str,
    profile_name: str,
    dest_directory: str,
):

    if not os.path.exists(dest_directory):
        os.makedirs(dest_directory)

    dest_path = os.path.join(dest_directory, f"{river}_{reach}_{ds_rs}_{us_rs}_{profile_name}.tif")

    # open the src raster the cross section concave hull as a mask
    with rasterio.open(src_path) as src:

        out_image, out_transform = rasterio.mask.mask(src, xs_hull.to_crs(src.crs)["geometry"], crop=True)
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
