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
from shapely.geometry import LineString

from ripple1d.ras import RasGeomText, RasManager, RasPlanText


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
) -> None:
    """Generate a plot comparing the source and DEM stage-area curves."""
    fig, ax = plt.subplots(2, 4, figsize=(14, 6))

    # Plot the source and DEM terrain profiles
    ax[0, 0].plot(src_el[:, 0], src_el[:, 1], label="Source", c="k", alpha=0.7)
    ax[0, 0].plot(dem_el[:, 0], dem_el[:, 1], label="DEM", c="r", alpha=0.7)
    ax[0, 0].set_title(f"Cross Section")
    ax[0, 0].set_xlabel("Station (ft)")
    ax[0, 0].set_ylabel("Elevation (ft)")

    # Plot the source and DEM stage-area curves
    ax[0, 1].plot(src_fa[:, 1], src_fa[:, 0], label="Source", c="k", alpha=0.7)
    ax[0, 1].plot(dem_fa[:, 1], dem_fa[:, 0], label="DEM", c="r", alpha=0.7)
    ax[0, 1].set_title(f"Flow Area Curve")
    ax[0, 1].set_ylabel("Water Surface Elevation (ft)")
    ax[0, 1].set_xlabel("Flow Area (ft^2)")

    # Plot the source and DEM inundated area curves
    ax[0, 2].plot(src_ie[:, 1], src_ie[:, 0], label="Source", c="k", alpha=0.7)
    ax[0, 2].plot(dem_ie[:, 1], dem_ie[:, 0], label="DEM", c="r", alpha=0.7)
    ax[0, 2].set_title(f"Inundated Area Curve")
    ax[0, 2].set_ylabel("Water Surface Elevation (ft)")
    ax[0, 2].set_xlabel("Inundated Area (ft^2)")
    ax[0, 2].legend()

    # Plot the percent correct inundation metric
    ax[1, 0].plot(metrics["pct_incorrectly_inundated"], metrics["wse"], c="k")
    ax[1, 0].set_title("Percent Incorrectly Inundated")
    ax[1, 0].set_xlabel("Percent")
    ax[1, 0].set_ylabel("Water Surface Elevation (ft)")
    ax[1, 0].set_xlim(-200, 200)

    # Plot the flow area percent difference metric
    ax[1, 1].plot(metrics["flow_area_pct_difference"], metrics["wse"], c="k")
    ax[1, 1].set_title("Flow Area Percent Difference")
    ax[1, 1].set_xlabel("Percent")
    ax[1, 1].set_ylabel("Water Surface Elevation (ft)")
    ax[1, 1].set_xlim(-200, 200)

    # Plot the inundated area percent difference metric
    ax[1, 2].plot(metrics["inundated_area_pct_difference"], metrics["wse"], c="k")
    ax[1, 2].set_title("Inundated Area Percent Difference")
    ax[1, 2].set_xlabel("Percent")
    ax[1, 2].set_ylabel("Water Surface Elevation (ft)")
    ax[1, 2].set_xlim(-200, 200)

    # merge last column into one plot
    gs = ax[0, 3].get_gridspec()
    for a in ax[:, 3]:
        a.remove()
    axbig = fig.add_subplot(gs[:, 3])
    metric_subset = {k: metrics[k] for k in metrics if isinstance(metrics[k], (int, float))}
    labels_formatted = [k.replace("_", " ").replace("%", r"\%").title() for k in metric_subset.keys()]
    labels_formatted = [r"$\bf{" + k + r"}$" for k in labels_formatted]
    values_formatted = [f"{v:.2f}" for k, v in metric_subset.items()]
    info_text = "\n".join([f"{k}: {v}" for k, v in zip(labels_formatted, values_formatted)])
    axbig.text(-0.1, 0.95, info_text, fontsize=12, transform=axbig.transAxes, va="top", ha="left")
    axbig.axis("off")

    # Add horizontal lines for WSELs
    for wse in metrics["wse"]:
        for a in ax.flatten():
            a.axhline(wse, c="b", ls="--", lw=0.1)

    # Formatting and saving
    for a in ax.flatten():
        a.set_facecolor("whitesmoke")
    fig.suptitle(section)
    fig.tight_layout()
    out_path = Path("plots") / section.split(" ")[0]
    out_path.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(out_path / f"{section}.jpg"), dpi=200)
    plt.close(fig)


def process_section_metrics(
    xs_id: str, wsels: pd.DataFrame, src_el: np.ndarray, dem_el: np.ndarray, plot: bool
) -> dict:
    """Process error metrics for a single cross section."""
    # Preprocess
    src_fa_curve = derive_stage_flow_area_curve(src_el)
    dem_fa_curve = derive_stage_flow_area_curve(dem_el)
    src_ia_curve = derive_stage_inundated_area_curve(src_el)
    dem_ia_curve = derive_stage_inundated_area_curve(dem_el)
    wsels = {k: v for k, v in sorted(wsels.items(), key=lambda item: item[1])}  # sort by WSEL

    # Calculate general error metrics
    metrics = defaultdict(list)
    metrics["wse"] = wsels
    metrics["below_lidar_flow_area"] = np.interp(dem_el[:, 1].min(), src_fa_curve[:, 0], src_fa_curve[:, 1])
    metrics["below_lidar_depth"] = dem_el[:, 1].min() - src_el[:, 1].min()
    # metrics["below_lidar_discharge"] = below_lidar_discharge(rc, dem_el[:, 1].min())
    metrics["terrain_bias"] = terrain_bias(src_el, dem_el)
    for k, v in terrain_diff_summary(src_el, dem_el).items():
        metrics[k] = v

    # Calculate stage-specific error metrics
    out_dict = {}
    for p in wsels:
        wse = wsels[p]
        metrics["pct_incorrectly_inundated"].append(pct_incorrect_inundation(src_el, dem_el, wse))
        metrics["flow_area_pct_difference"].append(series_pct_diff(src_fa_curve, dem_fa_curve, wse))
        ia_pct_diff = series_pct_diff(src_ia_curve, dem_ia_curve, wse)
        metrics["inundated_area_pct_difference"].append(ia_pct_diff)
        out_dict[p] = ia_pct_diff

    # Generate a plot
    if plot:
        generate_plot(xs_id, src_el, dem_el, src_fa_curve, dem_fa_curve, src_ia_curve, dem_ia_curve, metrics)

    return out_dict


def aggregate_errors(metrics: dict) -> dict:
    """Aggregate error metrics across all cross sections."""
    out_dict = {}
    plans = list(metrics[next(iter(metrics))].keys())
    for plan in plans:
        out_dict[plan] = np.nanmean([metrics[xs][plan] for xs in metrics])
    return out_dict


def parse_plans(paths: list) -> dict:
    """Parse the HEC-RAS plan file."""
    all_plans = None
    for p in paths:
        plan = RasPlanText(p)
        wses, flows = plan.read_rating_curves()
        if all_plans is None:
            all_plans = wses
        else:
            all_plans = pd.merge(all_plans, wses, how="outer", left_index=True, right_index=True)
    return all_plans.to_dict(orient="index")


def terrain_quality_metrics(plan_paths: list, geom_path: str, terrain_path: str, make_plots: bool = False) -> float:
    """Quantify the agreement of HEC-RAS cross-sections and DEM used to generate FIM."""
    # Load plan data (rating curves)
    wsels = parse_plans(plan_paths)

    # Get the station-elevation series from the HEC-RAS geometry files
    src, dem = get_model_terrains(geom_path, terrain_path)

    # Derive error metric for each cross-section
    metrics = {s: process_section_metrics(s, wsels[s], src[s], dem[s], make_plots) for s in src}

    # Aggregate error metrics
    return aggregate_errors(metrics)


def terrain_bias(src_el: np.ndarray, dem_el: np.ndarray) -> np.ndarray:
    """Calculate the average terrain difference between two elevation profiles."""
    all_stations = np.sort(np.unique(np.concatenate((src_el[:, 0], dem_el[:, 0]))))
    src_el = np.interp(all_stations, src_el[:, 0], src_el[:, 1])
    dem_el = np.interp(all_stations, dem_el[:, 0], dem_el[:, 1])
    return np.trapezoid(src_el - dem_el, all_stations) / (all_stations[-1] - all_stations[0])


def series_pct_diff(s1: np.ndarray, s2: np.ndarray, wse: float) -> float:
    """Calculate the percent difference between two curves at a water surface elevation."""
    s1 = np.interp(wse, s1[:, 0], s1[:, 1])
    s2 = np.interp(wse, s2[:, 0], s2[:, 1])
    return ((s1 - s2) / s1) * 100


def below_lidar_discharge(wsel_dict: dict, wsel: float) -> float:
    """Calculate the discharge below a water surface elevation from a dictionary of WSELs and discharges."""
    discharges = np.array([float(i) for i in wsel_dict.keys()])
    wsels = np.array(list(wsel_dict.values()))
    return np.interp(wsel, wsels, discharges)


def pct_incorrect_inundation(src_el: np.ndarray, dem_el: np.ndarray, wse: float) -> float:
    """Calculate the percent of the inundated area that is correct."""
    all_stations = np.sort(np.unique(np.concatenate((src_el[:, 0], dem_el[:, 0]))))
    src_el = np.interp(all_stations, src_el[:, 0], src_el[:, 1])
    dem_el = np.interp(all_stations, dem_el[:, 0], dem_el[:, 1])
    src_wet = src_el < wse
    dem_wet = dem_el < wse
    matching = (src_wet != dem_wet) * 1
    return (np.trapezoid(matching, all_stations) / (all_stations[-1] - all_stations[0])) * 100


def terrain_diff_summary(src_el: np.ndarray, dem_el: np.ndarray) -> dict:
    """Calculate a summary of terrain differences between two elevation profiles."""
    dem_el = np.interp(src_el[:, 0], dem_el[:, 0], dem_el[:, 1])
    diff = src_el[:, 1] - dem_el

    out_dict = {}
    for q in [25, 50, 75]:
        out_dict[f"diff_{q}%"] = np.percentile(diff, q)
    out_dict["diff_mean"] = np.mean(diff)
    out_dict["diff_std"] = np.std(diff)
    out_dict["diff_max"] = np.max(diff)
    out_dict["diff_min"] = np.min(diff)

    return out_dict


if __name__ == "__main__":
    model_dir = sys.argv[1]
    plan_suffix = sys.argv[2]
    paths = get_path_dict(model_dir, plan_suffix)
    error = compare_src_dem(paths, make_plots=True)
