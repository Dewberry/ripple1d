import json
from collections import defaultdict
from copy import copy
from operator import sub
from typing import OrderedDict

import contextily as ctx
import fiona
import geopandas as gpd
import matplotlib.patheffects as path_effects
import matplotlib.pyplot as plt
import numpy as np
import shapely
from matplotlib import axes, figure
from matplotlib.colors import ListedColormap
from pyproj import CRS
from shapely import LineString, Point, distance, get_parts
from shapely.ops import split

from ripple1d.ops.subset_gpkg import RippleGeopackageSubsetter
from tests.conflation_tests.consts import JUNCTION_RAS_DATA, RIVER_RAS_DATA, XS_RAS_DATA

custom_colors = [
    "#01FF70",
    "#FFDC00",
    "#FF851B",
    "#FF4136",
    "#F012BE",
    "#B10DC9",
]  # see color CSS at https://robert-96.github.io/gimp-color-palettes/
custom_cmap = ListedColormap(custom_colors)
LAYER_COLORS = OrderedDict(
    {
        "Junction": {"color": "red", "label": "HEC-RAS Junction", "lw": 2, "zorder": 3},
        "River": {"color": "#03c2fc", "label": "HEC-RAS River", "lw": 2},
        "XS": {"color": "#0ffc03", "label": "HEC-RAS Cross Sections", "lw": 2},
        "nwm": {"lw": 2},
    }
)


def offset_xs(gdf: gpd.GeoDataFrame, pts: float, ax: axes, rch_dict: dict) -> gpd.GeoDataFrame:
    transform = ax.transData.inverted()
    px1, _ = transform.transform((0, 0))  # Data coordinate at origin
    px2, _ = transform.transform((pts, 0))  # Offset in data units
    offset = px2 - px1  # Convert points to data units

    us_xs_id = "_".join(
        [str(rch_dict["us_xs"]["river"]), str(rch_dict["us_xs"]["reach"]), str(rch_dict["us_xs"]["xs_id"])]
    )
    ds_xs_id = "_".join(
        [str(rch_dict["ds_xs"]["river"]), str(rch_dict["ds_xs"]["reach"]), str(rch_dict["ds_xs"]["xs_id"])]
    )

    gdf.loc[gdf["river_reach_rs"] == us_xs_id, "geometry"] = gdf.loc[
        gdf["river_reach_rs"] == us_xs_id, "geometry"
    ].apply(lambda geom: geom.offset_curve(offset))
    gdf.loc[gdf["river_reach_rs"] == ds_xs_id, "geometry"] = gdf.loc[
        gdf["river_reach_rs"] == ds_xs_id, "geometry"
    ].apply(lambda geom: geom.offset_curve(-offset))
    return gdf


def load_data(
    ras_gpkg: str, nwm_path: str, conflation_path: str
) -> tuple[dict[gpd.GeoDataFrame], CRS, gpd.GeoDataFrame, gpd.GeoDataFrame, dict]:
    gdfs = {l: gpd.read_file(ras_gpkg, layer=l) for l in fiona.listlayers(ras_gpkg)}
    crs = gdfs[next(iter(gdfs))].crs
    nwm = gpd.read_parquet(nwm_path).to_crs(crs)
    nwm_endpts = nwm.copy()
    nwm_endpts["geometry"] = shapely.get_point(nwm.geometry, 0)
    with open(conflation_path) as f:
        conflation = json.load(f)
    return gdfs, crs, nwm, nwm_endpts, conflation


def add_default_ras_data(gdfs: dict[gpd.GeoDataFrame], axs: list[axes.Axes]):
    for layer, kwargs in LAYER_COLORS.items():
        if layer in gdfs.keys():
            gdfs[layer].plot(ax=axs[0], **kwargs)
    gdfs["River"].plot(ax=axs[2], color="k", lw=1, label="HEC-RAS River")


def add_default_nwm_data(nwm, nwm_endpts, axs):
    for ind, r in enumerate(nwm["ID"].astype(str).values[::-1]):
        color = custom_colors[ind]
        sub_nwm = nwm[nwm["ID"] == int(r)]
        sub_nwm_endpts = nwm_endpts[nwm_endpts["ID"] == int(r)]
        sub_nwm.plot(ax=axs[1], **LAYER_COLORS["nwm"], label=f"{r}", color=color)
        sub_nwm_endpts.plot(ax=axs[1], **LAYER_COLORS["nwm"], color=color)


def divide_section(line, splits):
    subs = []
    dists = np.linspace(0, line.length, splits + 1, endpoint=True)[1:]
    pt1 = Point(line.coords[0])
    d = 0
    segment = 0
    cur_coords = [pt1]
    ind = 1
    while ind <= len(line.coords):
        pt2 = Point(line.coords[ind])
        inc_dist = distance(pt1, pt2)
        new_d = d + inc_dist
        if new_d - dists[segment] > 1:
            new_pt = Point(line.interpolate(dists[segment]))
            cur_coords.append(new_pt)
            sub = LineString(cur_coords)
            subs.append(sub)
            cur_coords = [new_pt]
            pt1 = new_pt
            d = dists[segment]
            segment += 1
        elif abs(new_d - dists[segment]) < 1:
            cur_coords.append(pt2)
            sub = LineString(cur_coords)
            subs.append(sub)
            cur_coords = [pt2]
            pt1 = pt2
            d = dists[segment]
            segment += 1
            ind += 2
        else:
            cur_coords.append(pt2)
            pt1 = pt2
            d += inc_dist
            ind += 1
    return subs


def clip_xs(subset, overlaps, r):
    for xs_id in subset["river_reach_rs"].values:
        if xs_id in list(overlaps.keys()):
            splits = len(overlaps[xs_id])
            position = overlaps[xs_id].index(r)
            geom = subset.loc[subset["river_reach_rs"] == xs_id, "geometry"].values[0]
            subset.loc[subset["river_reach_rs"] == xs_id, "geometry"] = divide_section(geom, splits)[position]
    return subset


def generate_subset_gdfs(conflation, ras_gpkg, conflation_path) -> dict:
    # Create combo gdf
    subset_gdfs = {}
    overlaps = defaultdict(list)
    for r in conflation["reaches"]:
        subsetter = RippleGeopackageSubsetter(ras_gpkg, conflation_path, None, r)
        subset = subsetter.subset_xs
        subset_gdfs[r] = subset

        # log xs for duplicate detection
        for xs_id in subset["river_reach_rs"]:
            overlaps[xs_id].append(r)

    # split duplicated section geometry
    overlaps = {k: v for k, v in overlaps.items() if len(v) > 1}
    for r in subset_gdfs:
        subset_gdfs[r] = clip_xs(subset_gdfs[r], overlaps, r)

    return subset_gdfs


def add_conflated_data(conflation, ras_gpkg, conflation_path, axs, nwm, nwm_endpts):
    subset_gdfs = generate_subset_gdfs(conflation, ras_gpkg, conflation_path)
    for ind, r in enumerate(nwm["ID"].astype(str).values[::-1]):
        if r not in conflation["reaches"]:
            continue
        color = custom_colors[ind]
        if conflation["reaches"][r]["eclipsed"]:
            ls = "dashed"
            label = f"{r} (eclipsed)"
        else:
            ls = "solid"
            label = f"{r}"

            subset = subset_gdfs[r]
            subset.plot(ax=axs[2], color="k", lw=3, zorder=2)
            subset.plot(ax=axs[2], color=color, lw=2, zorder=2)

        sub_nwm = nwm[nwm["ID"] == int(r)]
        sub_nwm_endpts = nwm_endpts[nwm_endpts["ID"] == int(r)]
        sub_nwm.plot(ax=axs[2], **LAYER_COLORS["nwm"], label=label, ls=ls, color=color, zorder=1)
        sub_nwm_endpts.plot(ax=axs[2], **LAYER_COLORS["nwm"], color=color, zorder=1)


def format(axs: list[axes.Axes], fig: figure, conflation, crs, w):
    # Add labels
    txt = axs[0].text(0.05, 0.95, "HEC-RAS Source Model", transform=axs[0].transAxes, va="top", ha="left", fontsize=8)
    txt.set_path_effects([path_effects.withStroke(linewidth=3, foreground="white")])
    txt = axs[1].text(0.05, 0.95, "NWM Reaches", transform=axs[1].transAxes, va="top", ha="left", fontsize=8)
    txt.set_path_effects([path_effects.withStroke(linewidth=3, foreground="white")])
    txt = axs[2].text(0.05, 0.95, "Conflated Sub Models", transform=axs[2].transAxes, va="top", ha="left", fontsize=8)
    txt.set_path_effects([path_effects.withStroke(linewidth=3, foreground="white")])

    # Format
    y1 = axs[0].get_ylim()[0]
    y2 = axs[0].get_ylim()[1]
    x1 = axs[0].get_ylim()[0]
    x2 = axs[0].get_ylim()[1]
    width = x2 - x1
    axs[0].set_ylim(y1 - (1 * (width / w)), y2)
    for ax in axs:
        # Add an extra 10% for the legend
        ax.legend(fontsize="x-small", loc="lower right", ncols=len(conflation["reaches"]) + 1)
        ax.set_axis_off()
        ctx.add_basemap(ax, crs=crs, source=ctx.providers.USGS.USImagery, attribution=False)

    height = abs(axs[0].get_ylim()[0] - axs[0].get_ylim()[1])
    width = abs(axs[0].get_xlim()[0] - axs[0].get_xlim()[1])
    ar = (height) / width
    fig.set_size_inches(w, 3 * ar * w, forward=True)
    fig.tight_layout()


def plot_conflation(ras_gpkg: str, nwm_path: str, conflation_path: str) -> str:
    """Create summary png for conflation results."""
    # Load data
    gdfs, crs, nwm, nwm_endpts, conflation = load_data(ras_gpkg, nwm_path, conflation_path)

    # Plot
    rows = 3
    w = 6
    fig, axs = plt.subplots(nrows=rows, sharex=True, sharey=True)

    add_default_ras_data(gdfs, axs)
    add_default_nwm_data(nwm, nwm_endpts, axs)
    add_conflated_data(conflation, ras_gpkg, conflation_path, axs, nwm, nwm_endpts)
    format(axs, fig, conflation, crs, w)

    # Save
    out_path = conflation_path.replace(".json", ".png")
    fig.savefig(out_path, dpi=300)
    plt.close(fig)
    return out_path
