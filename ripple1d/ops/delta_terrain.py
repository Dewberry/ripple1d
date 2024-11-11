"""Script to assess the (dis)agreement between HEC-RAS and DEM terrain profiles."""

import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
import rasterio.transform
from delta_terrain_metrics import pct_incorrect_inundation, series_pct_diff, terrain_bias
from shapely.geometry import LineString

from ripple1d.ras import RasGeomText, RasPlanText


def get_model_terrains(path, raster_path):
    """Get both source and DEM terrain profiles for a model."""
    geom = RasGeomText(path, "EPSG:4326")  # dummy CRS
    sections = geom.cross_sections
    sections = get_dem_elevations(sections, raster_path, interval=30)
    src_series = {}
    dem_series = {}
    for section in sections:
        src_series[section] = np.array(sections[section].station_elevation_points)
        dem_series[section] = sections[section].dem_station_elevation_points
    return src_series, dem_series


def get_dem_elevations(sections, dem_path, interval=None):
    """Get the DEM elevations at the station points of the cross sections."""
    # load raster
    with rasterio.open(dem_path) as dem:
        dem_elevations = dem.read(1)
        dem_transform = dem.transform
        dem_crs = dem.crs
        extent = dem.bounds
        nd_val = dem.nodata
    extent = [extent.left, extent.right, extent.bottom, extent.top]
    dem_elevations[dem_elevations == nd_val] = np.nan
    transformer = rasterio.transform.AffineTransformer(dem_transform)

    for section in sections:
        tmp_planform_xy = np.array(sections[section].coords)
        line = LineString(tmp_planform_xy)
        og_stations = np.array(sections[section].station_elevation_points)[:, 0]
        if interval is None:
            stations = og_stations - og_stations[0]
        else:
            og_stations = np.arange(og_stations[0], og_stations[-1], interval)
            stations = og_stations - og_stations[0]
        interp_xy = line.interpolate(stations)

        # Get the DEM elevations at the interpolated points
        tmp_el = []
        tmp_x, tmp_y = [], []
        for pt in interp_xy:
            pt_x, pt_y = pt.xy
            tmp_x.append(pt_x[0])
            tmp_y.append(pt_y[0])
            row, col = transformer.rowcol(pt_x[0], pt_y[0])
            tmp_el.append(dem_elevations[row, col])

        tmp_el = np.array(tmp_el)
        sections[section].dem_station_elevation_points = np.column_stack((og_stations, tmp_el))

    return sections


def derive_stage_flow_area_curve(station_elevation_series: np.ndarray) -> np.ndarray:
    """Derive a stage-area curve from station-elevation series."""
    x = station_elevation_series[:, 0]
    dx = np.diff(x)
    y = station_elevation_series[:, 1]
    wsels = []
    areas = []
    for el in np.sort(y):
        depths = np.clip(el - y, 0, None)
        area = np.trapezoid(depths, x, dx)
        areas.append(area)
        wsels.append(el)

    return np.column_stack((wsels, areas))


def derive_stage_inundated_area_curve(station_elevation_series: np.ndarray) -> np.ndarray:
    """Derive a stage-area curve from station-elevation series."""
    x = station_elevation_series[:, 0]
    dx = np.diff(x)
    y = station_elevation_series[:, 1]
    wsels = []
    areas = []
    for el in np.sort(y):
        wet = y < el
        area = np.trapezoid(wet, x, dx)
        areas.append(area)
        wsels.append(el)

    return np.column_stack((wsels, areas))


def generate_plot(
    section: str,
    src_el: np.ndarray,
    dem_el: np.ndarray,
    src_fa: np.ndarray,
    dem_fa: np.ndarray,
    src_ie: np.ndarray,
    dem_ie: np.ndarray,
    metrics: dict,
    wsels: dict,
) -> None:
    """Generate a plot comparing the source and DEM stage-area curves."""
    fig, ax = plt.subplots(2, 3, figsize=(12, 6))

    ax[0, 0].plot(src_el[:, 0], src_el[:, 1], label="Source", c="k", alpha=0.7)
    ax[0, 0].plot(dem_el[:, 0], dem_el[:, 1], label="DEM", c="r", alpha=0.7)
    ax[0, 0].set_title(f"Cross Section")
    ax[0, 0].set_xlabel("Station (ft)")
    ax[0, 0].set_ylabel("Elevation (ft)")

    info_text = "\n".join(
        [f"{k.replace('_', ' ').title()}: {v:.2f}" for k, v in metrics.items() if k != "specific_metrics"]
    )
    ax[0, 0].text(0.05, 0.95, info_text, fontsize=8, transform=ax[0, 0].transAxes, va="top")

    ax[0, 1].plot(src_fa[:, 1], src_fa[:, 0], label="Source", c="k", alpha=0.7)
    ax[0, 1].plot(dem_fa[:, 1], dem_fa[:, 0], label="DEM", c="r", alpha=0.7)
    ax[0, 1].set_title(f"Stage-Area Curve")
    ax[0, 1].set_ylabel("Water Surface Elevation (ft)")
    ax[0, 1].set_xlabel("Area (ft^2)")

    ax[0, 2].plot(src_ie[:, 1], src_ie[:, 0], label="Source", c="k", alpha=0.7)
    ax[0, 2].plot(dem_ie[:, 1], dem_ie[:, 0], label="DEM", c="r", alpha=0.7)
    ax[0, 2].set_title(f"Inundated Area Curve")
    ax[0, 2].set_ylabel("Water Surface Elevation (ft)")
    ax[0, 2].set_xlabel("Area (ft^2)")
    ax[0, 2].legend()

    flows = list(metrics["specific_metrics"].keys())
    for i, series_name in enumerate(metrics["specific_metrics"][flows[0]].keys()):
        tmp_series = [metrics["specific_metrics"][flow][series_name] for flow in flows]
        ax[1, i].plot(flows, tmp_series, c="k", marker="o", markersize=3)
        ax[1, i].set_title(series_name.replace("_", " ").title())
        ax[1, i].set_xlabel("Flow (cfs)")
        ax[1, i].set_ylabel("Percent")
        ax[1, i].set_ylim(-5, 105)
        ax[1, i].set_xticks(flows)
        ax[1, i].set_xticklabels(flows, rotation=45, ha="right")

    for f in wsels:
        wse = wsels[f]
        for i in range(3):
            ax[0, i].axhline(wse, c="b", ls="--", lw=0.3)

    for a in ax.flatten():
        a.set_facecolor("whitesmoke")

    fig.suptitle(section)
    fig.tight_layout()
    os.makedirs("plots", exist_ok=True)
    fig.savefig(f"plots/{section}.png")
    plt.close(fig)


def parse_plan(path: str) -> dict:
    """Parse the HEC-RAS plan file."""
    plan = RasPlanText(path)
    wses, flows = plan.read_rating_curves()
    wses = wses.to_dict(orient="index")
    return wses


def process_section_metrics(reach_id: str, rc: pd.DataFrame, src_el: np.ndarray, dem_el: np.ndarray) -> dict:
    """Process error metrics for a single cross section."""
    # Preprocess
    src_fa_curve = derive_stage_flow_area_curve(src_el)
    dem_fa_curve = derive_stage_flow_area_curve(dem_el)
    src_ia_curve = derive_stage_inundated_area_curve(src_el)
    dem_ia_curve = derive_stage_inundated_area_curve(dem_el)

    # Calculate general error metrics
    metrics = {}
    metrics["below_lidar_flow_area"] = np.interp(dem_el[:, 1].min(), src_fa_curve[:, 0], src_fa_curve[:, 1])
    metrics["below_lidar_depth"] = dem_el[:, 1].min() - src_el[:, 1].min()
    metrics["terrain_bias"] = terrain_bias(src_el, dem_el)

    # Calculate discharge-specific error metrics
    specifics_dict = defaultdict(dict)
    for flow in rc:
        wse = rc[flow]
        specifics_dict[flow]["pct_incorrectly_inundated"] = pct_incorrect_inundation(src_el, dem_el, wse)
        specifics_dict[flow]["flow_area_pct_difference"] = series_pct_diff(src_fa_curve, dem_fa_curve, wse)
        specifics_dict[flow]["inundated_area_pct_difference"] = series_pct_diff(src_ia_curve, dem_ia_curve, wse)
    metrics["specific_metrics"] = specifics_dict

    # Generate a plot
    generate_plot(reach_id, src_el, dem_el, src_fa_curve, dem_fa_curve, src_ia_curve, dem_ia_curve, metrics, rc)

    return metrics


def update_database(db_path: str, metrics: dict, reach_id: str) -> None:
    """Log the error metrics to a database."""
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        fields = [
            "reach_id INTEGER",
            "river_reach_rs TEXT",
            "discharge REAL",
            "below_lidar_flow_area REAL",
            "below_lidar_depth REAL",
            "terrain_bias REAL",
            "flow_area_pct_difference REAL",
            "inundated_area_pct_difference REAL",
            "pct_incorrectly_inundated REAL",
        ]
        cur.execute(f"CREATE TABLE IF NOT EXISTS error_metrics ({', '.join(fields)})")

        for section in metrics:
            for flow in metrics[section]["specific_metrics"]:
                cur.execute(
                    """INSERT INTO error_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        reach_id,
                        section,
                        flow,
                        metrics[section]["below_lidar_flow_area"],
                        metrics[section]["below_lidar_depth"],
                        metrics[section]["terrain_bias"],
                        metrics[section]["specific_metrics"][flow]["flow_area_pct_difference"],
                        metrics[section]["specific_metrics"][flow]["inundated_area_pct_difference"],
                        metrics[section]["specific_metrics"][flow]["pct_incorrectly_inundated"],
                    ),
                )
        con.commit()
    con.close()


def compare_src_dem(path_dict: dict, make_plots: bool = False) -> float:
    """Compare the stage-area curve derived from DEM with the one from HEC-RAS geometry file."""
    # Load plan data
    rcs = parse_plan(path_dict["plan"])

    # Get the station-elevation series from the HEC-RAS geometry files
    src, dem = get_model_terrains(path_dict["geometry"], path_dict["terrain"])

    # Derive the stage-area curve from the station-elevation series and calculate the error metric
    section_metrics = {s: process_section_metrics(s, rcs[s], src[s], dem[s]) for s in src}
    update_database(path_dict["rc_db"], section_metrics, path_dict["reach_id"])

    # Generate plots
    # if make_plots:
    #     for section in src:
    #         generate_plot_v2(section, src[section], dem[section], section_metrics[section])

    # out_df = pd.DataFrame(section_errors.items(), columns=["Section", "Error"])
    # out_df.to_csv("error_metrics.csv", index=False)
    # return section_metrics


def get_path_dict(model_dir, plan_suffix):
    """Get a dictionary of all the relevant paths in a model directory."""
    root = Path(model_dir)
    name = root.name
    out_dict = {
        "root": root,
        "reach_id": name,
        "geometry": root / f"{name}.g01",
        "plan": root / f"{name}{plan_suffix}",
        "terrain": root / "Terrain" / f"{name}.USGS_Seamless_DEM_13.tif",
        "rc_db": root / f"{name}_rc_metrics.db",
    }
    return {k: str(v) for k, v in out_dict.items()}


if __name__ == "__main__":
    model_dir = sys.argv[1]
    plan_suffix = sys.argv[2]
    paths = get_path_dict(model_dir, plan_suffix)
    error = compare_src_dem(paths, make_plots=True)
