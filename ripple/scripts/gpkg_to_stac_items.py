import logging
import os
import tempfile
from pathlib import PurePosixPath

import pandas as pd
import pystac

from ripple.stacio.gpkg_plot import (
    create_geom_item,
    create_thumbnail_from_gpkg,
    get_asset_info,
    gpkg_to_geodataframe,
)
from ripple.stacio.s3_utils import list_keys
from ripple.stacio.utils.dg_utils import bbox_to_polygon

# from utils.dg_utils import *
from ripple.stacio.utils.s3_utils import (
    copy_item_to_s3,
    get_basic_object_metadata,
    init_s3_resources,
    s3_key_public_url_converter,
    split_s3_key,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="""{"time": "%(asctime)s" , "level": "%(levelname)s", "message": "%(message)s"}""",
    handlers=[logging.StreamHandler()],
)


def new_gpkg_item(
    gpkg_s3_path: str,
    new_gpkg_item_s3_path: str,
    thumbnail_png_s3_path: str,
    dev_mode: bool = False,
):
    logging.info("Creating item from gpkg")
    # Prep parameters
    bucket_name, gpkg_key = split_s3_key(gpkg_s3_path)

    # Instantitate S3 resources

    session, s3_client, s3_resource = init_s3_resources(dev_mode)
    bucket = s3_resource.Bucket(bucket_name)

    asset_list = list_keys(s3_client, bucket_name, os.path.dirname(gpkg_key))
    asset_list = [f"s3://{bucket_name}/{asset}" for asset in asset_list]

    # Download gpkg as temp file
    with tempfile.NamedTemporaryFile() as tmp:
        gpkg_local_path = f"{tmp.name}.gpkg"
        s3_client.download_file(Bucket=bucket_name, Key=gpkg_key, Filename=gpkg_local_path)
        # Convert gpkg to geodataframe for easy handling
        gdfs = gpkg_to_geodataframe(gpkg_local_path)

        logging.info("Creating png thumbnail")
        create_thumbnail_from_gpkg(gdfs, thumbnail_png_s3_path, s3_client)

        # Create item
        bbox = pd.concat(gdfs).total_bounds
        footprint = bbox_to_polygon(bbox)
        item = create_geom_item(gpkg_key, bbox, footprint)

        asset_list = asset_list + [thumbnail_png_s3_path, gpkg_s3_path]
        for asset_file in asset_list:
            _, asset_key = split_s3_key(asset_file)
            obj = bucket.Object(asset_key)
            metadata = get_basic_object_metadata(obj)
            asset_info = get_asset_info(asset_file)
            asset = pystac.Asset(
                s3_key_public_url_converter(asset_file, dev_mode),
                extra_fields=metadata,
                roles=asset_info["roles"],
                description=asset_info["description"],
            )
            item.add_asset(asset_info["title"], asset)

        copy_item_to_s3(item, new_gpkg_item_s3_path, s3_client)

        logging.info("Program completed successfully")


def main(gpkg_s3_path: str, new_gpkg_item_s3_path: str, thumbnail_png_s3_path: str):
    new_gpkg_item(gpkg_s3_path, new_gpkg_item_s3_path, thumbnail_png_s3_path)


if __name__ == "__main__":
    ras_project_key = "s3://fim/mip/dev2/Caney Creek-Lake Creek/BUMS CREEK/BUMS CREEK.prj"
    root = os.path.splitext(ras_project_key)[0]
    gpkg_s3_path = f"{root}.gpkg"
    thumbnail_png_s3_path = f"{root}.png"
    new_gpkg_item_s3_path = f"{root}.json"
    main(gpkg_s3_path, new_gpkg_item_s3_path, thumbnail_png_s3_path)
# s3://fim/mip/dev/
