"""Create HEC-RAS Terrains."""

from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import geopandas as gpd
import rasterio
from pyproj import CRS

from ripple1d.consts import (
    MAP_DEM_BUFFER_DIST_FT,
    MAP_DEM_UNCLIPPED_SRC_URL,
    MAP_DEM_VERT_UNITS,
    METERS_PER_FOOT,
)
from ripple1d.data_model import NwmReachModel
from ripple1d.ras import create_terrain

# from ripple1d.ripple1d_logger import log_process
from ripple1d.utils.dg_utils import clip_raster, reproject_raster
from ripple1d.utils.ripple_utils import fix_reversed_xs, xs_concave_hull


def get_geometry_mask(gdf_xs_conc_hull: str, MAP_DEM_UNCLIPPED_SRC_URL: str, task_id: str = None) -> gpd.GeoDataFrame:
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
    task_id: str = "",
) -> None:
    """Create a RAS terrain file."""
    logging.info(f"{task_id} | create_ras_terrain starting")

    if resolution and not resolution_units:
        raise ValueError(
            f"{task_id} | 'resolution' arg has been provided but 'resolution_units' arg has not been provided. Please provide both"
        )

    if resolution_units:
        if resolution_units not in ["Feet", "Meters"]:
            raise ValueError(f"{task_id} | invalid resolution_units: {resolution_units}. expected 'Feet' or 'Meters'")

    nwm_rm = NwmReachModel(submodel_directory)

    if not nwm_rm.file_exists(nwm_rm.ras_gpkg_file):
        raise FileNotFoundError(
            f"{task_id} | NwmReachModel class expecting ras_gpkg_file {nwm_rm.ras_gpkg_file}, file not found"
        )

    if not os.path.exists(nwm_rm.terrain_directory):
        os.makedirs(nwm_rm.terrain_directory, exist_ok=True)

    gdf_xs = gpd.read_file(nwm_rm.ras_gpkg_file, layer="XS", driver="GPKG").explode(ignore_index=True)
    crs = gdf_xs.crs

    with ProcessPoolExecutor() as executor:
        future = executor.submit(
            get_geometry_mask, gdf_xs_conc_hull=nwm_rm.xs_concave_hull, MAP_DEM_UNCLIPPED_SRC_URL=terrain_source_url
        )
        mask = future.result()

    # clip dem
    src_dem_clipped_localfile = os.path.join(nwm_rm.terrain_directory, "temp.tif")
    map_dem_clipped_basename = os.path.basename(terrain_source_url)
    src_dem_reprojected_localfile = os.path.join(
        nwm_rm.terrain_directory, map_dem_clipped_basename.replace(".vrt", ".tif")
    )

    with ProcessPoolExecutor() as executor:
        future = executor.submit(
            clip_raster,
            src_path=terrain_source_url,
            dst_path=src_dem_clipped_localfile,
            mask_polygon=mask,
            vertical_units=vertical_units,
        )
        future.result()

    # reproject/resample dem
    logging.debug(f"Reprojecting/Resampling DEM {src_dem_clipped_localfile} to {src_dem_clipped_localfile}")
    reproject_raster(src_dem_clipped_localfile, src_dem_reprojected_localfile, nwm_rm.crs, resolution, resolution_units)
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
    logging.info(f"{task_id} | create_ras_terrain complete")
    return result
