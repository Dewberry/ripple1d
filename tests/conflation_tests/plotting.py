import json
from typing import OrderedDict

import contextily as ctx
import fiona
import geopandas as gpd
import matplotlib.patheffects as path_effects
import matplotlib.pyplot as plt
import shapely
from matplotlib import axes
from matplotlib.colors import ListedColormap

from ripple1d.ops.subset_gpkg import RippleGeopackageSubsetter

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


def plot_conflation(ras_gpkg: str, nwm_path: str, conflation_path: str) -> str:
    """Create summary png for conflation results."""
    # Load data
    gdfs = {l: gpd.read_file(ras_gpkg, layer=l) for l in fiona.listlayers(ras_gpkg)}
    crs = gdfs[next(iter(gdfs))].crs
    nwm = gpd.read_parquet(nwm_path).to_crs(crs)
    nwm_endpts = nwm.copy()
    nwm_endpts["geometry"] = shapely.get_point(nwm.geometry, 0)
    with open(conflation_path) as f:
        conflation = json.load(f)

    # Plot
    rows = 3
    w = 6
    fig, axs = plt.subplots(nrows=rows, sharex=True, sharey=True)

    for layer, kwargs in LAYER_COLORS.items():
        if layer in gdfs.keys():
            gdfs[layer].plot(ax=axs[0], **kwargs)
    gdfs["River"].plot(ax=axs[2], color="k", lw=1, label="HEC-RAS River")

    for ind, r in enumerate(nwm["ID"].astype(str).values[::-1]):
        color = custom_colors[ind]
        sub_nwm = nwm[nwm["ID"] == int(r)]
        sub_nwm_endpts = nwm_endpts[nwm_endpts["ID"] == int(r)]
        sub_nwm.plot(ax=axs[1], **LAYER_COLORS["nwm"], label=f"{r}", color=color)
        sub_nwm_endpts.plot(ax=axs[1], **LAYER_COLORS["nwm"], color=color)
        if r not in conflation["reaches"]:
            continue
        if conflation["reaches"][r]["eclipsed"]:
            ls = "dashed"
            label = f"{r} (eclipsed)"
        else:
            ls = "solid"
            label = f"{r}"

            subsetter = RippleGeopackageSubsetter(ras_gpkg, conflation_path, None, r)
            subset = subsetter.subset_xs
            subset = offset_xs(subset, 1.25, axs[2], conflation["reaches"][r])
            subset.plot(ax=axs[2], color="k", lw=3, zorder=2)
            subset.plot(ax=axs[2], color=color, lw=2, zorder=2)
        sub_nwm.plot(ax=axs[2], **LAYER_COLORS["nwm"], label=label, ls=ls, color=color, zorder=1)
        sub_nwm_endpts.plot(ax=axs[2], **LAYER_COLORS["nwm"], color=color, zorder=1)

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

    # Save
    out_path = conflation_path.replace(".json", ".png")
    fig.savefig(out_path, dpi=300)
    plt.close(fig)
    return out_path
