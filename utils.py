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
