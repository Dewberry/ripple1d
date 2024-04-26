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

    # itterate through the flow change locations and determine us/ds most cross sections
    for i, row in fcl.iterrows():
        us_rs = None

        # if this is the upsteam most flow change location
        if i == fcl.index[0]:
            us_rs = xs.loc[xs["rs"] >= row["ds_rs"], "rs"].max()
            fcl.loc[i, "us_rs"] = us_rs
            xs.loc[xs["rs"] >= row["ds_rs"], "feature_id"] = row["feature_id"]

        # if this is the downstream most flow change location
        elif i == fcl.index[-1]:
            us_rs = xs.loc[
                (xs["rs"] >= row["rs"]) & (xs["rs"] < previous_rs), "rs"
            ].max()
            fcl.loc[i, "us_rs"] = us_rs
            xs.loc[xs["rs"] < us_rs, "feature_id"] = row["feature_id"]

            ds_rs = xs.loc[xs["rs"] < row["rs"], "rs"].min()
            fcl.loc[i, "ds_rs"] - ds_rs

        # if this is an intermediate flow change location
        else:
            us_rs = xs.loc[
                (xs["rs"] >= row["rs"]) & (xs["rs"] < previous_rs), "rs"
            ].max()
            fcl.loc[i, "us_rs"] = us_rs
            xs.loc[(xs["rs"] >= row["rs"]) & (xs["rs"] < previous_rs), "feature_id"] = (
                row["feature_id"]
            )

        previous_rs = row["rs"]

    r.geom.cross_sections = xs

    return fcl, r


def detemine_necessary_flow_change_locations(nwm_reach_gdf, r):
    points = []
    crs = nwm_reach_gdf.crs

    nwm_reach_gdf.set_geometry("geometry", inplace=True, crs=crs)

    # get end points of reaches
    for i, row in nwm_reach_gdf.iterrows():
        first, last = row.geometry.geoms[0].boundary.geoms
        points.append(last)

    nwm_reach_gdf["last_point"] = points
    nwm_reach_gdf.set_geometry("last_point", inplace=True, crs=crs)

    # determine flow change location
    fcl = nwm_reach_gdf.loc[nwm_reach_gdf["ratio_50"] < 0.8, :]

    # determine last cross section
    last_xs = r.geom.cross_sections.loc[
        r.geom.cross_sections["rs"] == r.geom.cross_sections["rs"].min()
    ]

    # get fcl that exists downstream of last cross section
    nwm_reach_gdf.set_geometry("geometry", inplace=True, crs=crs)
    ds_most_reach = nwm_reach_gdf[
        nwm_reach_gdf.intersects(last_xs.to_crs(nwm_reach_gdf.crs).geometry.iloc[0])
    ]

    fcl = gpd.GeoDataFrame(
        pd.concat([fcl, ds_most_reach]), geometry="last_point", crs=fcl.crs
    )

    fcl = fcl.to_crs(r.projection).sjoin_nearest(r.geom.cross_sections)

    return fcl


def compile_flows(fcl):

    flows = []
    for i, row in fcl.iterrows():

        flow = [row[i] for i in row.index if "rf_" in i]
        flow += [min(flow) * 0.8] + [max(flow) * 1.5]

        flows.append(sorted(flow)[::-1])

    fcl["flows_rc"] = flows

    return fcl


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

    dest_path = os.path.join(
        dest_directory, f"{river}_{reach}_{ds_rs}_{us_rs}_{profile_name}.tif"
    )

    # open the src raster the cross section concave hull as a mask
    with rasterio.open(src_path) as src:

        out_image, out_transform = rasterio.mask.mask(
            src, xs_hull.to_crs(src.crs)["geometry"], crop=True
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
