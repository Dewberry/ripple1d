from __future__ import annotations

import logging
import os

import geopandas as gpd
import rasterio
from pyproj import CRS

from ripple.consts import (
    MAP_DEM_BUFFER_DIST_FT,
    MAP_DEM_CLIPPED_BASENAME,
    MAP_DEM_HDF_NAME,
    MAP_DEM_UNCLIPPED_SRC_URL,
    MAP_DEM_VERT_UNITS,
    METERS_PER_FOOT,
)

from ..ras2 import create_terrain
from ..ripple_logger import configure_logging
from ..utils import clip_raster, xs_concave_hull


def get_geometry_mask(gdf_xs: str, MAP_DEM_UNCLIPPED_SRC_URL: str):

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


def write_projection_file(crs: CRS, terrain_directory: str):
    projection_file = os.path.join(terrain_directory, "projection.prj")
    with open(projection_file, "w") as f:
        f.write(CRS(crs).to_wkt("WKT1_ESRI"))
    return projection_file


def main(terrain_hdf_filename: str, gpkg_path: str):
    """Requires Windows with geospatial libs, so typically run using OSGeo4W shell."""

    # terrain directory
    terrain_directory = os.path.dirname(terrain_hdf_filename)
    os.makedirs(terrain_directory, exist_ok=True)

    # get geometry mask
    gdf_xs = gpd.read_file(gpkg_path, layer="XS", driver="GPKG").explode(ignore_index=True)
    mask = get_geometry_mask(gdf_xs, MAP_DEM_UNCLIPPED_SRC_URL)

    # clip dem
    src_dem_clipped_localfile = os.path.join(terrain_directory, MAP_DEM_CLIPPED_BASENAME)
    logging.info(f"Clipping DEM {MAP_DEM_UNCLIPPED_SRC_URL} to {src_dem_clipped_localfile}")
    clip_raster(src_path=MAP_DEM_UNCLIPPED_SRC_URL, dst_path=src_dem_clipped_localfile, mask_polygon=mask)

    # write projection file
    projection_file = write_projection_file(gdf_xs.crs, terrain_directory)

    # Make the RAS mapping terrain locally
    create_terrain(
        [src_dem_clipped_localfile],
        projection_file,
        terrain_hdf_filename=terrain_hdf_filename,
        vertical_units=MAP_DEM_VERT_UNITS,
    )


if __name__ == "__main__":

    nwm_id = "2823932"

    terrain_hdf_filename = (
        rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}\Terrain.hdf"
    )
    gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}.gpkg"

    main(terrain_hdf_filename, gpkg_path)
