import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import OrderedDict

import contextily as ctx
import fiona
import geopandas as gpd
import matplotlib.patheffects as path_effects
import matplotlib.pyplot as plt
import shapely
from matplotlib import axes
from matplotlib.colors import ListedColormap

import ripple1d
from ripple1d.ops.ras_conflate import conflate_model
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


class BadConflation(Exception):
    """Raised when conflation files do not match."""

    def __init__(self, message: str):
        super().__init__(message)


class PathManager:
    """Generate paths to files for the test directory."""

    ras_model_name = "source_model"
    nwm_file_name = "nwm.parquet"

    def __init__(self, ras_dir: str):
        self.ras_dir = str(Path(__file__).parent / ras_dir)

    @property
    def ras_path(self) -> str:
        """Path to the ras gpkg."""
        return str(Path(self.ras_dir) / f"{self.ras_model_name}.gpkg")

    @property
    def nwm_path(self) -> str:
        """Path to the nwm network."""
        return str(Path(self.ras_dir) / self.nwm_file_name)

    @property
    def nwm_dict(self) -> dict:
        """Format information for conflate_model endpoint."""
        return {"file_name": self.nwm_path, "type": "nwm_hydrofabric", "version": f"{ripple1d.__version__}[testing]"}

    @property
    def conflation_file(self) -> str:
        """Path to the generated conflation file."""
        return str(Path(self.ras_dir) / f"{self.ras_model_name}.conflation.json")

    @property
    def rubric_file(self) -> str:
        """Path to the target conflation file."""
        return str(Path(self.ras_dir) / "validation.conflation.json")

    @property
    def test_id(self) -> str:
        """A short ID for the test."""
        return Path(self.ras_dir).name


class ConflationFile:

    def __init__(self, fpath: str):
        self.fpath = fpath
        with open(fpath) as f:
            self._dict = json.load(f)

    @property
    def reaches(self):
        return {k: ConflationReach(v) for k, v in self._dict["reaches"].items()}

    def __eq__(self, other) -> bool:
        """Check whether this conflation file matches another one."""
        if not isinstance(other, ConflationFile):
            return False
        if not self.reaches.keys() == other.reaches.keys():
            raise BadConflation(
                f"reach mismatch.\n{self.fpath} had reaches {list(self.reaches.keys())}\n{other.fpath} had reaches {list(other.reaches.keys())}"
            )
        for reach in self.reaches:
            if not reach in other.reaches:
                raise BadConflation(f"reach {reach} was present in {self.fpath} but not in {other.fpath}")
            r1 = self.reaches[reach]
            r2 = other.reaches[reach]
            if r1.eclipsed and not r2.eclipsed:
                raise BadConflation(f"{reach} was eclipsed in {self.fpath} but not in {other.fpath}")
            elif not r1.eclipsed and r2.eclipsed:
                raise BadConflation(f"{reach} was not eclipsed in {self.fpath} but was in {other.fpath}")
            elif r1.eclipsed and r2.eclipsed:
                return True
            if not r1.us_xs == r2.us_xs:
                raise BadConflation(
                    f"u/s XS incongruency for {reach}\n{self.fpath}: {r1.us_xs}\n{other.fpath}: {r2.us_xs}"
                )
            if not r1.ds_xs == r2.ds_xs:
                raise BadConflation(
                    f"d/s XS incongruency for {reach}\n{self.fpath}: {r1.ds_xs}\n{other.fpath}: {r2.ds_xs}"
                )
            return True


@dataclass
class ConflationReach:

    _dict: dict

    def val_to_str(self, val: dict) -> str:
        """Format a cross-section entry into text."""
        return f"{val['river']}_{val['reach']}_{val['xs_id']}"

    @property
    def eclipsed(self) -> bool:
        return self._dict["eclipsed"]

    @property
    def us_xs(self) -> str:
        return self.val_to_str(self._dict["us_xs"])

    @property
    def ds_xs(self) -> str:
        return self.val_to_str(self._dict["ds_xs"])


# @pytest.mark.parametrize(["test_a"])
def run_scenario(ras_dir_name: str):
    """Run a specific test case."""
    pm = PathManager(ras_dir_name)
    conflate_model(pm.ras_dir, pm.ras_model_name, pm.nwm_dict)
    plot_conflation(pm.ras_path, pm.nwm_path, pm.conflation_file)
    ConflationFile(pm.conflation_file) == ConflationFile(pm.rubric_file)  # Validation


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
    ].apply(lambda geom: geom.offset_curve(-offset))
    gdf.loc[gdf["river_reach_rs"] == ds_xs_id, "geometry"] = gdf.loc[
        gdf["river_reach_rs"] == ds_xs_id, "geometry"
    ].apply(lambda geom: geom.offset_curve(offset))
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
    for ax in axs:
        # Add an extra 10% for the legend
        y1 = ax.get_ylim()[0]
        y2 = ax.get_ylim()[1]
        dy = y2 - y1
        ax.set_ylim(y1 - (0.1 * dy), y2)
        ax.legend(fontsize="x-small", loc="lower right", ncols=len(conflation["reaches"]))
        ax.set_axis_off()
        ctx.add_basemap(ax, crs=crs, source=ctx.providers.USGS.USImagery, attribution=False)

    fig.tight_layout()
    ar = (axs[0].get_window_extent().height * 4) / axs[0].get_window_extent().width
    fig.set_size_inches(w, ar * w, forward=True)

    # Save
    out_path = conflation_path.replace(".json", ".png")
    fig.savefig(out_path, dpi=300)
    plt.close(fig)
    return out_path


def determine_consistency(rubric: dict, conflation: dict) -> list[bool]:
    """Find mismatch between two conflation files."""
    eclipsed_match = rubric["eclipsed"] == conflation["eclipsed"]
    if rubric["eclipsed"]:
        us_match = True
        ds_match = True
    else:
        us_match = rubric["us_xs"]["xs_id"] == conflation["us_xs"]["xs_id"]
        ds_match = rubric["ds_xs"]["xs_id"] == conflation["ds_xs"]["xs_id"]
    return us_match, ds_match, eclipsed_match


def run_all():
    """Run all conflation tests."""
    # for test in ["test_a", "test_b", "test_c", "test_d"]:
    for test in ["test_d"]:
        run_scenario(test)


if __name__ == "__main__":
    run_all()
