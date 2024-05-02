from __future__ import annotations

import argparse
import logging
import os
import posixpath
import re
import tempfile
import urllib.parse

import geopandas as gpd
import shapely
from shapely.geometry import Polygon, box

from ripple.ras import Ras
from ripple.utils import init_log, get_sessioned_s3_client, clip_raster, xs_concave_hull
from ripple.consts import (
    MAP_DEM_UNCLIPPED_SRC_URL,
    MAP_DEM_CLIPPED_BASENAME,
    MAP_DEM_BUFFER_DIST_FT,
    MAP_DEM_DIRNAME,
    MAP_DEM_HDF_NAME,
    MAP_DEM_VERT_UNITS,
    METERS_PER_FOOT,
)


def regex_extract_group_assert_one_group(pattern: str, s: str) -> str:
    groups = re.findall(pattern, s)
    if not groups:
        raise ValueError(f"String {repr(s)} had no groups found with pattern {repr(pattern)} (expected 1)")
    if len(groups) != 1:
        raise ValueError(f"String {repr(s)} had {len(groups)} found with pattern {repr(pattern)} (expected 1)")
    return groups[0]


def main():
    """Requires Windows with geospatial libs, so typically run using OSGeo4W shell.
    Example usage: python -um ripple.exe.make_ras_terrain --ras-model-stac-href "https://stac.dewberryanalytics.com/collections/huc-12040101/items/WFSJ_Main-cd42"
    """
    init_log()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ras-model-stac-href",
        required=True,
        help="STAC item URL for the model, e.g. 'https://stac.dewberryanalytics.com/collections/huc-12040101/items/WFSJ_Main-cd42'",
    )
    args = parser.parse_args()
    if not args.ras_model_stac_href:
        raise ValueError("CLI arg --ras-model-stac-href is empty or missing")

    s3_client = get_sessioned_s3_client()
    bucket = os.environ["AWS_BUCKET"]
    if not bucket:
        raise ValueError(f"Empty bucket var: {repr(bucket)}")

    with tempfile.TemporaryDirectory(suffix="make-ras-terrain") as tmp_dir:
        ras = Ras(
            path=tmp_dir,
            stac_href=args.ras_model_stac_href,
            s3_client=s3_client,
            s3_bucket=bucket,
            default_epsg=2277,  # TODO get from the model itself, or at least assert that this is valid after assigning it
        )
        ras.write_projection()
        if not ras.projection_file:
            raise ValueError(f"Projection file is None or empty: {repr(ras.projection_file)}")

        basename2asset = ras.stac_item.assets
        grouper_pattern_s3key_from_href = f"^https://{bucket}.s3.amazonaws.com/(.+)$"

        # Get the project file to infer the model directory path
        prj_hrefs = [asset.href for basename, asset in basename2asset.items() if basename.endswith(".prj")]
        if len(prj_hrefs) != 1:
            raise ValueError(f"{len(prj_hrefs)} hrefs end with .prj (expected 1): {prj_hrefs}")
        prj_href = prj_hrefs[0]
        prj_key = regex_extract_group_assert_one_group(grouper_pattern_s3key_from_href, prj_href)
        model_dir_key = posixpath.dirname(prj_key)

        # Get the gpkg file to build a DEM mask polygon based on the XS extents
        gpkg_hrefs = [asset.href for basename, asset in basename2asset.items() if basename.endswith(".gpkg")]
        if len(gpkg_hrefs) != 1:
            raise ValueError(f"{len(gpkg_hrefs)} hrefs end with .gpkg (expected 1): {gpkg_hrefs}")
        gpkg_href = gpkg_hrefs[0]
        gpkg_key = regex_extract_group_assert_one_group(grouper_pattern_s3key_from_href, gpkg_href)
        gpkg_vsis3 = f"/vsis3/{bucket}/{gpkg_key}"
        logging.info(f"Reading: {gpkg_vsis3}")
        gdf_xs = gpd.read_file(gpkg_vsis3, layer="XS").explode(ignore_index=True)
        gdf_xs_conc_hull = xs_concave_hull(gdf_xs)
        # Buffer the concave hull by transforming it to Albers, buffering it, then transforming it back to the original CRS
        gdf_xs_conc_hull_buffered = (
            gdf_xs_conc_hull.to_crs(epsg=5070)
            .buffer(MAP_DEM_BUFFER_DIST_FT * METERS_PER_FOOT)
            .to_crs(gdf_xs_conc_hull.crs)
        )
        if len(gdf_xs_conc_hull_buffered) != 1:
            raise ValueError(f"Expected 1 record in gdf_xs_conc_hull_buffered, got {len(gdf_xs_conc_hull_buffered)}")
        mask = gdf_xs_conc_hull_buffered.iloc[0]
        # mask = shapely.geometry.box(
        #     *ras.stac_item.bbox
        # )  # GCS coordinates to match NED 1/3-arcsecond DEM which also uses GCS

        src_dem_clipped_localfile = os.path.join(tmp_dir, MAP_DEM_CLIPPED_BASENAME)
        src_dem_clipped_s3key = posixpath.join(model_dir_key, MAP_DEM_CLIPPED_BASENAME)

        logging.info(f"Clipping DEM {MAP_DEM_UNCLIPPED_SRC_URL} to {src_dem_clipped_localfile}")

        # The bbox should be in form (minx, miny, maxx, maxy)
        if not (-180 <= min(ras.stac_item.bbox) <= 180) and (-180 <= max(ras.stac_item.bbox) <= 180):
            raise ValueError(f"Unexpected value outside of range -180 to 180 found in bbox: {ras.stac_item.bbox}")

        clip_raster(src_path=MAP_DEM_UNCLIPPED_SRC_URL, dst_path=src_dem_clipped_localfile, mask=mask)

        # Make the RAS mapping terrain locally
        map_dem_dir = os.path.join(tmp_dir, MAP_DEM_DIRNAME)
        os.makedirs(map_dem_dir)
        terrain_dir_local = ras.create_terrain(
            [src_dem_clipped_localfile],
            terrain_dirname=map_dem_dir,
            hdf_filename=MAP_DEM_HDF_NAME,
            vertical_units=MAP_DEM_VERT_UNITS,
        )
        terrain_dir_s3key = posixpath.join(model_dir_key, os.path.basename(terrain_dir_local))

        # Upload the RAS mapping terrain
        if not os.path.isdir(terrain_dir_local):
            raise NotADirectoryError(terrain_dir_local)
        for root, dirs, files in os.walk(terrain_dir_local):
            if dirs:
                raise ValueError(f"Unexpected subdirs within {terrain_dir_local}: {dirs}")
            for fn in sorted(files):
                fp = os.path.join(root, fn)
                s3_key = posixpath.join(terrain_dir_s3key, fn)
                logging.info(f"Uploading: {fp} -> s3://{bucket}/{s3_key}")
                s3_client.upload_file(fp, bucket, s3_key)

        # Upload the tif that was clipped from raw source DEM
        logging.info(f"Uploading: {src_dem_clipped_localfile} -> s3://{bucket}/{src_dem_clipped_s3key}")
        s3_client.upload_file(src_dem_clipped_localfile, bucket, src_dem_clipped_s3key)


if __name__ == "__main__":
    main()
