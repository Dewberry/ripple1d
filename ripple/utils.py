from __future__ import annotations
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon, LineString
from shapely.validation import make_valid
import pandas as pd
import plotly.graph_objects as go
import os
import rasterio


def decode(df: pd.DataFrame):
    for c in df.columns:
        df[c] = df[c].str.decode("utf-8")
    return df


def create_flow_depth_array(flow: list[float], depth: list[float], increment: float = 0.5):
    min_depth = np.min(depth)
    max_depth = np.max(depth)
    start_depth = np.floor(min_depth * 2) / 2  # round down to nearest .0 or .5
    new_depth = np.arange(start_depth, max_depth + increment, increment)
    new_flow = np.interp(new_depth, np.sort(depth), np.sort(flow))

    return new_flow, new_depth


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


def plot_xs_with_wse_increments(r):
    df = pd.DataFrame(r.geom.cross_sections["station_elevation"].iloc[0])
    fig = go.Figure()

    xs = df.copy()
    xs.loc[len(xs.index)] = [
        xs.loc[len(xs.index) - 1, "station"],
        xs.loc[0, "elevation"],
    ]
    xs.loc[len(xs.index)] = xs.loc[0]

    polygon = Polygon(zip(xs["station"], xs["elevation"]))

    if not polygon.is_valid:
        polygon = make_valid(polygon).geoms[0].geoms[0]

    for wse in r.geom.cross_sections["wses"].iloc[0]:
        line = LineString([[xs["station"].iloc[0], wse], [xs["station"].iloc[-2], wse]])

        new_line = polygon.intersection(line)
        if new_line.length == 0:
            continue
        if new_line.geom_type in ["GeometryCollection", "MultiLineString"]:
            for l in new_line.geoms:
                x, y = l.xy
                fig.add_scatter(x=list(x), y=list(y), marker={"color": "grey", "size": 0.5})
        else:
            x, y = new_line.xy
            fig.add_scatter(x=list(x), y=list(y), marker={"color": "grey", "size": 0.5})

    fig.add_scatter(x=df["station"], y=df["elevation"], line={"color": "red"})
    fig.update_layout({"showlegend": False})

    return fig
