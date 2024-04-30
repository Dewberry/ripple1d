from __future__ import annotations

import argparse
import logging
import os
import posixpath
import re
import tempfile

import shapely
from shapely.geometry import Polygon, box

from ripple.ras import Ras
from ripple.utils import init_log, get_sessioned_s3_client, clip_raster
from ripple.consts import (
    MAP_DEM_UNCLIPPED_SRC_URL,
    MAP_DEM_CLIPPED_BASENAME,
    # MAP_DEM_BUFFER_DIST,
    MAP_DEM_DIRNAME,
    MAP_DEM_HDF_NAME,
    MAP_DEM_VERT_UNITS,
)


def regex_extract_group_assert_one_group(pattern: str, s: str) -> str:
    groups = re.findall(pattern, s)
    if not groups:
        raise ValueError(f"String {repr(s)} had no groups found with pattern {repr(pattern)} (expected 1)")
    if len(groups) != 1:
        raise ValueError(f"String {repr(s)} had {len(groups)} found with pattern {repr(pattern)} (expected 1)")
    return groups[0]


def main():
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
        # ras.plan.geom.scan_for_xs()  # crashing
        if not ras.projection_file:
            raise ValueError(f"Projection file is None or empty: {repr(ras.projection_file)}")

        basename2asset = ras.stac_item.assets
        prj_hrefs = [asset.href for basename, asset in basename2asset.items() if basename.endswith(".prj")]
        if len(prj_hrefs) != 1:
            raise ValueError(f"{len(prj_hrefs)} hrefs end with .prj (expected 1): {prj_hrefs}")
        prj_href = prj_hrefs[0]

        grouper_pattern_s3key_from_href = f"^https://{bucket}.s3.amazonaws.com/(.+)$"
        prj_key = regex_extract_group_assert_one_group(grouper_pattern_s3key_from_href, prj_href)
        model_dir_key = posixpath.dirname(prj_key)

        src_dem_clipped_localfile = os.path.join(tmp_dir, MAP_DEM_CLIPPED_BASENAME)
        src_dem_clipped_s3key = posixpath.join(model_dir_key, MAP_DEM_CLIPPED_BASENAME)

        logging.info(f"Clipping DEM {MAP_DEM_UNCLIPPED_SRC_URL} to {src_dem_clipped_localfile}")

        # The bbox should be in form (minx, miny, maxx, maxy)
        if not (-180 <= min(ras.stac_item.bbox) <= 180) and (-180 <= max(ras.stac_item.bbox) <= 180):
            raise ValueError(f"Unexpected value outside of range -180 to 180 found in bbox: {ras.stac_item.bbox}")

        mask = shapely.geometry.box(
            *ras.stac_item.bbox
        )  # GCS coordinates to match NED 1/3-arcsecond DEM which also uses GCS
        clip_raster(MAP_DEM_UNCLIPPED_SRC_URL, src_dem_clipped_localfile, mask)

        # Make the RAS mapping terrain locally
        os.makedirs(MAP_DEM_DIRNAME)
        terrain_dir_local = ras.create_terrain(
            [src_dem_clipped_localfile],
            terrain_dirname=MAP_DEM_DIRNAME,
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
