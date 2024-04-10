import pandas as pd
import geopandas as gpd
import numpy as np


def decode(df: pd.DataFrame):
    for c in df.columns:
        df[c] = df[c].str.decode("utf-8")
    return df


def create_flow_depth_array(flow: list, depth: list):
    min_depth = np.min(depth)
    max_depth = np.max(depth)
    start_depth = np.floor(min_depth * 2) / 2  # round down to nearest .0 or .5
    new_depth = np.arange(start_depth, max_depth + 0.5, 0.5)
    new_flow = np.interp(new_depth, np.sort(depth), np.sort(flow))

    return new_flow, new_depth


def nhd_reach_rc(wse_flow_dict: dict, xs_fp_join: gpd.GeoDataFrame):

    # create joining field
    xs_fp_join["river_reach_rs"] = xs_fp_join[["river", "reach", "rs_str"]].agg(
        " ".join, axis=1
    )

    for id, data in wse_flow_dict.items():

        # unpack sub dictionary
        wse = data["wse"]
        flow = data["flow"]

        # join cross section/flowpath gdf to flow/wse dataframes
        xs_flowpath_wse_join = xs_fp_join.merge(
            wse, left_on="river_reach_rs", right_index=True
        )
        xs_flowpath_flow_join = xs_fp_join.merge(
            flow, left_on="river_reach_rs", right_index=True
        )

        # convert wse to depth
        for c in wse.columns:
            xs_flowpath_wse_join[c] = (
                xs_flowpath_wse_join[c] - xs_flowpath_wse_join["thalweg"]
            )

        # group by nhd feature id and compute mean stage
        mean_wses = xs_flowpath_wse_join.groupby("feature_id").apply(
            lambda s: {i: s[i].mean() for i in wse.columns}
        )
        mean_flows = xs_flowpath_flow_join.groupby("feature_id").apply(
            lambda s: {i: s[i].mean() for i in flow.columns}
        )

        # iterate through nhd flow lines and compute new rating curve for each
        reach_rc = {}
        for id in mean_wses.keys():

            new_flow, new_depth = create_flow_depth_array(
                list(mean_flows[id].values()), list(mean_wses[id].values())
            )

            reach_rc[id] = {"flow": new_flow, "depth": new_depth}

    return reach_rc


def get_terrain_exe_path(ras_ver: str) -> str:
    """Return Windows path to RasProcess.exe exposing CreateTerrain subroutine, compatible with provided RAS version."""
    # 5.0.7 version of RasProcess.exe does not expose CreateTerrain subroutine.
    # Testing shows that RAS 5.0.7 accepts Terrain created by 6.1 version of RasProcess.exe, so use that for 5.0.7.
    d = {
        "507": r"C:\Program Files (x86)\HEC\HEC-RAS\6.1\RasProcess.exe",
        "5.07": r"C:\Program Files (x86)\HEC\HEC-RAS\6.1\RasProcess.exe",
        "600": r"C:\Program Files (x86)\HEC\HEC-RAS\6.0\RasProcess.exe",
        "6.00": r"C:\Program Files (x86)\HEC\HEC-RAS\6.0\RasProcess.exe",
        "610": r"C:\Program Files (x86)\HEC\HEC-RAS\6.1\RasProcess.exe",
        "6.10": r"C:\Program Files (x86)\HEC\HEC-RAS\6.1\RasProcess.exe",
        "6.10": r"C:\Program Files (x86)\HEC\HEC-RAS\6.1\RasProcess.exe",
        "631": r"C:\Program Files (x86)\HEC\HEC-RAS\6.3.1\RasProcess.exe",
        "6.3.1": r"C:\Program Files (x86)\HEC\HEC-RAS\6.3.1\RasProcess.exe",
    }
    try:
        return d[ras_ver]
    except KeyError as e:
        raise ValueError(f"Unsupported ras_ver: {ras_ver}. choices: {sorted(d)}") from e


def get_first_last_points(gdf: gpd.GeoDataFrame) -> tuple:

    first_points, last_points = [], []

    for _, row in gdf.iterrows():

        # Multistring to first and last points
        first, last = row.geometry.geoms[0].boundary.geoms

        # append the points
        first_points.append(first)
        last_points.append(last)

    # set point geometries to columns
    gdf["first_point"] = first_points
    gdf["last_point"] = last_points

    # create individual copies
    first = gdf.copy()
    last = gdf.copy()

    # set geometry and crs
    first.set_geometry("first_point", inplace=True)
    first.set_crs(crs=gdf.crs, inplace=True)

    last.set_geometry("last_point", inplace=True)
    last.set_crs(crs=gdf.crs, inplace=True)

    return first, last


def get_us_ds_ids(first: gpd.GeoDataFrame, last: gpd.GeoDataFrame):

    rows = []
    # iterate through the last points (downstream points)
    for i, row in last.iterrows():

        # determine which reach is downstream
        ds = first.loc[first.intersects(row["last_point"]), "feature_id"]
        if len(ds) > 1:
            stream_order = first.loc[first.intersects(row["last_point"]), "strm_order"]
            rows.append(ds[stream_order == stream_order.min()].iloc[0])

        elif ds.empty:
            rows.append(None)
        else:
            rows.append(ds.iloc[0])

    # set downstream feature id as columns
    last["ds_id"] = rows

    rows = []
    # iterate through the fist points (upstream points)
    for i, row in first.iterrows():

        # determine which reach is downstream
        us = last.loc[last.intersects(row["first_point"]), "feature_id"]
        if len(us) > 1:
            stream_order = last.loc[last.intersects(row["first_point"]), "strm_order"]
            rows.append(us[stream_order == stream_order.max()].iloc[0])

        elif us.empty:
            rows.append(None)
        else:
            rows.append(us.iloc[0])

    # set downstream feature id as columns
    first["us_id"] = rows

    return first, last
