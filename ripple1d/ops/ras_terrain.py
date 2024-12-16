"""Create HEC-RAS Terrains."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from pyproj import CRS
from shapely import LineString

from ripple1d.consts import (
    MAP_DEM_BUFFER_DIST_FT,
    MAP_DEM_UNCLIPPED_SRC_URL,
    MAP_DEM_VERT_UNITS,
    METERS_PER_FOOT,
)
from ripple1d.data_model import XS, NwmReachModel
from ripple1d.ras import RasGeomText, create_terrain
from ripple1d.utils.dg_utils import clip_raster, reproject_raster
from ripple1d.utils.ripple_utils import fix_reversed_xs, resample_vertices, xs_concave_hull


def get_geometry_mask(gdf_xs_conc_hull: str, MAP_DEM_UNCLIPPED_SRC_URL: str) -> gpd.GeoDataFrame:
    """Get a geometry mask for the DEM based on the cross sections."""
    # build a DEM mask polygon based on the XS extents

    # Buffer the concave hull by transforming it to Albers, buffering it, then transforming it to the src raster crs
    with rasterio.open(MAP_DEM_UNCLIPPED_SRC_URL) as src:
        gdf_xs_conc_hull_buffered = (
            gdf_xs_conc_hull.to_crs(epsg=5070).buffer(MAP_DEM_BUFFER_DIST_FT * METERS_PER_FOOT).to_crs(src.crs)
        )

    if len(gdf_xs_conc_hull_buffered) != 1:
        raise ValueError(f"Expected 1 record in gdf_xs_conc_hull_buffered, got {len(gdf_xs_conc_hull_buffered)}")
    return gdf_xs_conc_hull_buffered.iloc[0]


def write_projection_file(crs: CRS, terrain_directory: str) -> str:
    """Write a projection file for the terrain."""
    projection_file = os.path.join(terrain_directory, "projection.prj")
    with open(projection_file, "w") as f:
        f.write(CRS(crs).to_wkt("WKT1_ESRI"))
    return projection_file


def create_ras_terrain(
    submodel_directory: str,
    terrain_source_url: str = MAP_DEM_UNCLIPPED_SRC_URL,
    vertical_units: str = MAP_DEM_VERT_UNITS,
    resolution: float = None,
    resolution_units: str = None,
    terrain_agreement_resolution: float = 3,
):
    """Create a RAS terrain file.

    Parameters
    ----------
    submodel_directory : str
        The path to the directory containing a submodel geopackage
    terrain_source_url : str, optional
        URL to a vrt or other raster with topographic data, by default
        MAP_DEM_UNCLIPPED_SRC_URL
    vertical_units : str, optional
        label for the vertical units of the source terrain (ex. Meters), by
        default MAP_DEM_VERT_UNITS
    resolution : float, optional
        horizontal resolution that terrain will be projected to, by default
        None
    resolution_units : str, optional
        unit for resolution parameter, by default None
    task_id : str, optional
        Task ID to use for logging, by default ""

    Returns
    -------
    str
        descriptive string detailing where terrain was saved

    Raises
    ------
    ValueError
        Raised when resolution arg has been provided but resolution_units arg
        has not been provided
    ValueError
        Raised when resolution_units are not Feet or Meters
    FileNotFoundError
        Raised when no geopackage file is found in submodel_directory

    Notes
    -----
    The create_ras_terrain endpoint downloads a digital elevation model (DEM)
    for the modeled area and exports it to a HEC-RAS terrain file.  DEM data
    may be sourced from any virtual raster source, but the default source is
    the `USGS 3DEP 1/3 arcsecond dataset
    <https://www.sciencebase.gov/catalog/item/4f70aa9fe4b058caae3f8de5>`_.  By
    default, terrain data is clipped to a 1,000-foot buffer around a concave
    hull of the submodel cross-sections, however, the buffer distance may be
    adjusted.  If resolution data is passed to the endpoint, the terrain raster
    will be resampled to that resolution.
    """
    logging.info(f"create_ras_terrain starting")
    compute_terrain_agreement_metrics(
        submodel_directory, terrain_agreement_resolution
    )  # JUST HERE FOR DEBUGGING.  MOVE TO EOF WHEN DONE
    return

    if resolution and not resolution_units:
        raise ValueError(
            f"'resolution' arg has been provided but 'resolution_units' arg has not been provided. Please provide both"
        )

    if resolution_units:
        if resolution_units not in ["Feet", "Meters"]:
            raise ValueError(f"invalid resolution_units: {resolution_units}. expected 'Feet' or 'Meters'")

    nwm_rm = NwmReachModel(submodel_directory)

    if not nwm_rm.file_exists(nwm_rm.ras_gpkg_file):
        raise FileNotFoundError(f"NwmReachModel class expecting ras_gpkg_file {nwm_rm.ras_gpkg_file}, file not found")

    if not os.path.exists(nwm_rm.terrain_directory):
        os.makedirs(nwm_rm.terrain_directory, exist_ok=True)

    mask = get_geometry_mask(nwm_rm.xs_concave_hull, terrain_source_url)

    # clip dem
    src_dem_clipped_localfile = os.path.join(nwm_rm.terrain_directory, "temp.tif")
    map_dem_clipped_basename = os.path.basename(terrain_source_url)
    src_dem_reprojected_localfile = os.path.join(
        nwm_rm.terrain_directory, map_dem_clipped_basename.replace(".vrt", ".tif")
    )

    clip_raster(
        terrain_source_url,
        src_dem_clipped_localfile,
        mask,
        vertical_units,
    )

    # reproject/resample dem
    logging.debug(f"Reprojecting/Resampling DEM {src_dem_clipped_localfile} to {src_dem_clipped_localfile}")
    reproject_raster(
        src_dem_clipped_localfile, src_dem_reprojected_localfile, CRS(nwm_rm.crs), resolution, resolution_units
    )
    os.remove(src_dem_clipped_localfile)

    # write projection file
    projection_file = write_projection_file(nwm_rm.crs, nwm_rm.terrain_directory)

    # Make the RAS mapping terrain locally
    result = create_terrain(
        [src_dem_reprojected_localfile],
        projection_file,
        f"{nwm_rm.terrain_directory}\\{nwm_rm.model_name}",
        vertical_units=MAP_DEM_VERT_UNITS,
    )
    os.remove(src_dem_reprojected_localfile)
    nwm_rm.update_write_ripple1d_parameters({"source_terrain": terrain_source_url})
    logging.info(f"create_ras_terrain complete")
    return result


### Terrain Agreement Metrics ###


def compute_terrain_agreement_metrics(submodel_directory: str, max_sample_distance: float = 3):
    """Compute a suite of agreement metrics between source model XS data and mapping DEM."""
    # Load model information
    nwm_rm = NwmReachModel(submodel_directory)
    dem_path = nwm_rm.terrain_file

    # Add DEM data to geom object
    geom = RasGeomText(nwm_rm.derive_path(".g01"), "EPSG:4269")  # Dummy CRS because the real one will be loaded later
    section_data = sample_terrain(geom, dem_path, max_interval=max_sample_distance)

    # Compute agreement metrics
    metrics = geom_agreement_metrics(section_data)

    # Save results and summary
    with open(nwm_rm.terrain_agreement_file, "w") as f:
        json.dump(metrics, f, indent=4)
    nwm_rm.update_write_ripple1d_parameters({"terrain_agreement_summary": metrics["summary"]})


def sample_terrain(geom: RasGeomText, dem_path: str, max_interval: float = 3):
    """Add DEM station_elevations to cross-sections."""
    section_data = {}
    with rasterio.open(dem_path) as dem:
        for section in geom.cross_sections:
            line = LineString(np.array(geom.cross_sections[section].coords))
            station_elevation_points = np.array(geom.cross_sections[section].station_elevation_points)
            stations = station_elevation_points[:, 0]
            stations = resample_vertices(stations, max_interval)
            rel_stations = stations - stations.min()
            interp_xy = [(pt.x, pt.y) for pt in line.interpolate(rel_stations)]
            el = np.array([e for e in dem.sample(interp_xy)])
            dem_xs = np.column_stack((stations, el))
            src_resampled = np.interp(stations, station_elevation_points[:, 0], station_elevation_points[:, 1])
            src_xs = np.column_stack((stations, src_resampled))
            section_data[section] = {"dem_xs": dem_xs, "src_xs": src_xs}
    return section_data


def geom_agreement_metrics(xs_data: dict) -> dict:
    """Compute a suite of agreement metrics between source model XS data and a sampled DEM."""
    metrics = {"xs_specific": {}, "summary": {}}
    for section in xs_data:
        print(section)
        metrics["xs_specific"][section] = xs_agreement_metrics(xs_data[section])

    # aggregate

    return metrics


def xs_agreement_metrics(xs: XS) -> dict:
    """Compute a suite of agreement metrics between source model XS data and mapping DEM."""
    metrics = {}
    src_xs_el = xs["src_xs"][:, 1]
    dem_xs_el = xs["dem_xs"][:, 1]
    metrics["residuals"] = residual_metrics(src_xs_el, dem_xs_el)
    metrics["r_squared"] = r_squared(src_xs_el, dem_xs_el)
    metrics["spectral_angle"] = spectral_angle(src_xs_el, dem_xs_el)
    metrics["spectral_correlation"] = spectral_correlation(src_xs_el, dem_xs_el)
    metrics["correlation"] = correlation(src_xs_el, dem_xs_el)
    metrics["max_cross_correlation"] = cross_correlation(src_xs_el, dem_xs_el)
    metrics["thalweg_elevation_difference"] = thalweg_elevation_difference(src_xs_el, dem_xs_el)
    metrics["variable_metrics"] = variable_metrics(xs["src_xs"], xs["dem_xs"])

    return metrics


def residual_metrics(a1: np.ndarray, a2: np.ndarray) -> dict:
    """Summary statistics on residuals between two arrays."""
    metrics = {}
    residuals = a1 - a2
    metrics["mean"] = residuals.mean()
    metrics["std"] = residuals.std()
    metrics["max"] = residuals.max()
    metrics["min"] = residuals.min()
    metrics["p_25"] = np.percentile(residuals, 25)
    metrics["p_50"] = np.percentile(residuals, 50)
    metrics["p_75"] = np.percentile(residuals, 75)
    metrics["rmse"] = np.sqrt((residuals * residuals).mean())
    metrics["normalized_rmse"] = metrics["rmse"] / (metrics["p_75"] - metrics["p_25"])
    return metrics


def ss(x: np.ndarray, y: np.ndarray) -> float:
    """Calculate the sum of squares."""
    return np.sum((x - np.mean(x)) * (y - np.mean(y)))


def r_squared(a1: np.ndarray, a2: np.ndarray) -> float:
    """Calculate R-squared for two series."""
    return ss(a1, a2) ** 2 / (ss(a1, a1) * ss(a2, a2))


def spectral_angle(a1: np.ndarray, a2: np.ndarray) -> float:
    """Calculate the spectral angle of two vectors.

    Following the approach of https://github.com/BYU-Hydroinformatics/HydroErr/blob/42a84f3e006044f450edc7393ed54d59f27ef35b/HydroErr/HydroErr.py#L3541
    """
    a = np.dot(a1, a2)
    b = np.linalg.norm(a1) * np.linalg.norm(a2)
    return np.arccos(a / b)


def spectral_correlation(a1: np.ndarray, a2: np.ndarray) -> float:
    """Calculate the spectral angle of two vectors.

    Following the approach of https://github.com/BYU-Hydroinformatics/HydroErr/blob/42a84f3e006044f450edc7393ed54d59f27ef35b/HydroErr/HydroErr.py#L3541
    """
    a = np.dot(a1 - np.mean(a1), a2 - np.mean(a2))
    b = np.linalg.norm(a1 - np.mean(a1))
    c = np.linalg.norm(a2 - np.mean(a2))
    e = b * c
    return np.arccos(a / e)


def correlation(a1: np.ndarray, a2: np.ndarray) -> float:
    """Calculate Pearson's correlation of two series."""
    num = np.sum((a1 - a1.mean()) * (a2 - a2.mean()))
    denom = np.sqrt(np.sum(np.square(a1 - a1.mean())) * np.sum(np.square(a1 - a1.mean())))
    return num / denom


def cross_correlation(a1: np.ndarray, a2: np.ndarray) -> float:
    """Calculate the maximum cross-correlation between two time series."""
    # Use FFT approach
    a1 -= a1.mean()  # detrend
    a2 -= a2.mean()
    fft1 = np.fft.fft(a1)
    fft2 = np.fft.fft(a2)
    cross_corr = np.fft.ifft(fft1 * np.conj(fft2)).real
    cross_corr /= np.sqrt(np.sum(a1**2) * np.sum(a2**2))  # Normalize to -1 to 1
    return max(cross_corr)


def thalweg_elevation_difference(a1: np.ndarray, a2: np.ndarray) -> float:
    """Calculate the elevation difference between the thalweg (low point) of two series."""
    return a1.min() - a2.min()


def variable_metrics(src_xs: np.ndarray, dem_xs: np.ndarray) -> dict:
    """Calculate metrics for WSE values every half foot."""
    start_el = src_xs[:, 1].min()
    end_el = min((src_xs[0, 1], src_xs[-1, 1]))
    wses = np.arange(start_el, end_el, 0.5)[1:]
    all_metrics = {}
    for wse in wses:
        tmp_src_xs = add_intersection_pts(src_xs, wse)
        tmp_dem_xs = add_intersection_pts(dem_xs, wse)
        metrics = {}
        metrics["inundation_agreement"] = inundation_agreement(src_xs, dem_xs, wse)
        metrics["top_width_agreement"] = top_width_agreement(tmp_src_xs, tmp_dem_xs, wse)
        metrics["flow_area_agreement"] = flow_area_agreement(tmp_src_xs, tmp_dem_xs, wse)
        metrics["hydraulic_radius_agreement"] = hydraulic_radius_agreement(tmp_src_xs, tmp_dem_xs, wse)
        metrics["median_residual"] = median_residual(src_xs, dem_xs, wse)
        all_metrics[wse] = metrics
    return all_metrics


def add_intersection_pts(section_pts: np.ndarray, wse: float) -> np.ndarray:
    """Add points to XS where WSE hits."""
    intersection = wse_intersection_pts(section_pts, wse)
    if len(intersection) == 0:
        return section_pts
    inds = np.searchsorted(section_pts[:, 0], intersection[:, 0])
    return np.insert(section_pts, inds, intersection, axis=0)


def wse_intersection_pts(section_pts: np.ndarray, wse: float) -> list[tuple[float]]:
    """Find where the cross-section terrain intersects the water-surface elevation."""
    intersection_pts = []

    # Iterate through all pairs of points and find any points where the line would cross the wse
    for i in range(len(section_pts) - 1):
        p1 = section_pts[i]
        p2 = section_pts[i + 1]

        if p1[1] > wse and p2[1] > wse:  # No intesection
            continue
        elif p1[1] < wse and p2[1] < wse:  # Both below wse
            continue
        elif p1[1] == wse or p2[1] == wse:  # already vertex present
            continue

        # Define line
        m = (p2[1] - p1[1]) / (p2[0] - p1[0])
        b = p1[1] - (m * p1[0])

        # Find intersection point with Cramer's rule
        determinant = lambda a, b: (a[0] * b[1]) - (a[1] * b[0])
        div = determinant((1, 1), (-m, 0))
        tmp_y = determinant((b, wse), (-m, 0)) / div
        tmp_x = determinant((1, 1), (b, wse)) / div

        intersection_pts.append((tmp_x, tmp_y))
    return np.array(intersection_pts)


def smape_series(a1: np.ndarray, a2: np.ndarray) -> float:
    """Return the symmetric mean absolute percentage errror of two series."""
    num = np.abs(a1 - a2)
    denom = np.abs(a1) + np.abs(a2)
    return (100 / len(a1))(np.sum(num / denom))


def smape_single(a1: float, a2: float) -> float:
    """Return the symmetric mean absolute percentage errror of two values."""
    return (abs(a1 - a2) / (abs(a1) + abs(a2))) * 100


def inundation_agreement(src_el: np.ndarray, dem_el: np.ndarray, wse: float) -> float:
    """Calculate the percent of the cross-section with agreeing wet/dry."""
    stations = src_el[:, 0]
    src_wet = src_el[:, 1] < wse
    dem_wet = dem_el[:, 1] < wse
    matching = (src_wet != dem_wet) * 1
    return (np.trapezoid(matching, stations) / (stations[-1] - stations[0])) * 100


def top_width_agreement(src_el: np.ndarray, dem_el: np.ndarray, wse: float) -> float:
    """Calculate the agreement of the wetted top widths."""
    src_tw = get_wetted_top_width(src_el, wse)
    dem_tw = get_wetted_top_width(dem_el, wse)
    return smape_single(src_tw, dem_tw)


def flow_area_agreement(src_el: np.ndarray, dem_el: np.ndarray, wse: float) -> float:
    """Calculate the agreement of the wetted top widths."""
    src_area = get_flow_area(src_el, wse)
    dem_area = get_flow_area(dem_el, wse)
    return smape_single(src_area, dem_area)


def hydraulic_radius_agreement(src_el: np.ndarray, dem_el: np.ndarray, wse: float) -> float:
    """Calculate the agreement of the hydraulic radii."""
    src_radius = get_hydraulic_radius(src_el, wse)
    dem_radius = get_hydraulic_radius(dem_el, wse)
    return smape_single(src_radius, dem_radius)


def get_wetted_top_width(station_elevation_series: np.ndarray, wse: float) -> float:
    """Derive wetted-top-width for a given stage."""
    wet = station_elevation_series[:, 1] < wse
    return np.trapezoid(wet, station_elevation_series[:, 0])


def get_flow_area(station_elevation_series: np.ndarray, wse: float) -> float:
    """Derive wetted-top-width for a given stage."""
    depths = np.clip(wse - station_elevation_series[:, 1], 0, None)
    return np.trapezoid(depths, station_elevation_series[:, 0])


def get_wetted_perimeter(station_elevation_series: np.ndarray, wse: float) -> float:
    """Derive wetted-perimeter for a given stage."""
    station_elevation_series = station_elevation_series[station_elevation_series[:, 1] < wse]
    diffs = np.diff(station_elevation_series, axis=0)
    return np.sum(np.sqrt(np.square(diffs[:, 0]) + np.square(diffs[:, 1])))


def get_hydraulic_radius(station_elevation_series: np.ndarray, wse: float) -> float:
    """Derive wetted-perimeter for a given stage."""
    a = get_flow_area(station_elevation_series, wse)
    wp = get_wetted_perimeter(station_elevation_series, wse)
    return a / wp


def median_residual(a1: np.ndarray, a2: np.ndarray, wse: float) -> float:
    """Calculate the median terrain difference for vertices below WSE."""
    valid_mask = (a1 < wse) | (a2 < wse)
    residuals = a1[valid_mask] - a1[valid_mask]
    return np.percentile(residuals, 50)
