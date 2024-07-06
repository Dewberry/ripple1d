"""Create HEC-RAS Terrains."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import geopandas as gpd
import rasterio
from pyproj import CRS

from ripple.consts import (
    MAP_DEM_BUFFER_DIST_FT,
    MAP_DEM_CLIPPED_BASENAME,
    MAP_DEM_UNCLIPPED_SRC_URL,
    MAP_DEM_VERT_UNITS,
    METERS_PER_FOOT,
)
from ripple.ras import create_terrain
from ripple.utils.dg_utils import clip_raster, reproject_raster
from ripple.utils.ripple_utils import xs_concave_hull


def get_geometry_mask(gdf_xs: str, MAP_DEM_UNCLIPPED_SRC_URL: str) -> gpd.GeoDataFrame:
    """Get a geometry mask for the DEM based on the cross sections."""
    # build a DEM mask polygon based on the XS extents
    gdf_xs_conc_hull = xs_concave_hull(gdf_xs)

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


def create_ras_terrain(submodel_directory: str, resolution: float = None) -> None:
    """Create a RAS terrain file."""
    logging.info(f"Processing: {submodel_directory}")
    gpkg_file = Path(submodel_directory) / f"{Path(submodel_directory).name}.gpkg"

    if not os.path.exists(gpkg_file):
        raise FileNotFoundError(f"Expecting {gpkg_file}, file not found")

    # terrain directory
    terrain_directory = f"{submodel_directory}/Terrain"
    os.makedirs(terrain_directory, exist_ok=True)

    # get geometry mask
    gdf_xs = gpd.read_file(gpkg_file, layer="XS", driver="GPKG").explode(ignore_index=True)
    crs = gdf_xs.crs
    mask = get_geometry_mask(gdf_xs, MAP_DEM_UNCLIPPED_SRC_URL)

    # clip dem
    src_dem_clipped_localfile = os.path.join(terrain_directory, "temp.tif")
    src_dem_reprojected_localfile = os.path.join(terrain_directory, MAP_DEM_CLIPPED_BASENAME)

    logging.debug(f"Clipping DEM {MAP_DEM_UNCLIPPED_SRC_URL} to {src_dem_clipped_localfile}")
    clip_raster(
        src_path=MAP_DEM_UNCLIPPED_SRC_URL,
        dst_path=src_dem_clipped_localfile,
        mask_polygon=mask,
    )
    # reproject/resample dem
    logging.debug(f"Reprojecting/Resampling DEM {src_dem_clipped_localfile} to {src_dem_clipped_localfile}")
    reproject_raster(src_dem_clipped_localfile, src_dem_reprojected_localfile, crs, resolution)
    os.remove(src_dem_clipped_localfile)

    # write projection file
    projection_file = write_projection_file(gdf_xs.crs, terrain_directory)

    # Make the RAS mapping terrain locally
    result = create_terrain(
        [src_dem_reprojected_localfile],
        projection_file,
        dst_terrain_filepath=submodel_directory,
        vertical_units=MAP_DEM_VERT_UNITS,
    )
    os.remove(src_dem_reprojected_localfile)
    return result
