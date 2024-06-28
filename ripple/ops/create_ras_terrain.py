from __future__ import annotations

import logging
import os

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
from ripple.utils.dg_utils import clip_raster
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


def new_ras_terrain(output_terrain_hdf_filepath: str, gpkg_path: str, conflation_parameters: dict, nwm_id: str) -> None:
    """Require Windows with geospatial libs, so typically run using OSGeo4W shell."""
    if conflation_parameters["us_xs"]["xs_id"] == "-9999":
        logging.info(f"skipping {nwm_id}; no cross sections conflated.")
    else:
        logging.info(f"Processing: {nwm_id}")
        # terrain directory
        terrain_directory = os.path.dirname(output_terrain_hdf_filepath)
        os.makedirs(terrain_directory, exist_ok=True)

        # get geometry mask
        gdf_xs = gpd.read_file(gpkg_path, layer="XS", driver="GPKG").explode(ignore_index=True)
        mask = get_geometry_mask(gdf_xs, MAP_DEM_UNCLIPPED_SRC_URL)

        # clip dem
        src_dem_clipped_localfile = os.path.join(terrain_directory, MAP_DEM_CLIPPED_BASENAME)
        logging.info(f"Clipping DEM {MAP_DEM_UNCLIPPED_SRC_URL} to {src_dem_clipped_localfile}")
        clip_raster(
            src_path=MAP_DEM_UNCLIPPED_SRC_URL,
            dst_path=src_dem_clipped_localfile,
            mask_polygon=mask,
        )

        # write projection file
        projection_file = write_projection_file(gdf_xs.crs, terrain_directory)

        # Make the RAS mapping terrain locally
        create_terrain(
            [src_dem_clipped_localfile],
            projection_file,
            terrain_hdf_filepath=output_terrain_hdf_filepath,
            vertical_units=MAP_DEM_VERT_UNITS,
        )


# if __name__ == "__main__":
#     conflation_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJ Main.json"

#     with open(conflation_json_path) as f:
#         conflation_parameters = json.load(f)

#     for nwm_id in conflation_parameters.keys():

#         output_terrain_hdf_filepath = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\Terrain.hdf"
#         gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models\{nwm_id}\{nwm_id}.gpkg"

#         main(output_terrain_hdf_filepath, gpkg_path, conflation_parameters[nwm_id],nwm_id)
