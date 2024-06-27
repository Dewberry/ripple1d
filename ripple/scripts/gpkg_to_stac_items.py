import json
import logging
import os
import sqlite3
import tempfile
import traceback
from pathlib import Path

import pandas as pd
import pystac

from ripple.consts import RIPPLE_VERSION
from ripple.stacio.gpkg_utils import (
    create_geom_item,
    create_thumbnail_from_gpkg,
    get_asset_info,
    get_river_miles,
    gpkg_to_geodataframe,
    reproject,
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


def create_table(db_path: str, table_name: str):
    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        res = cursor.execute(f"select name from sqlite_master where name='{table_name}'")
        if res.fetchone():
            cursor.execute(f"drop table {table_name}")
        cursor.execute(f"""Create table {table_name} (key TEXT, gpkg TEXT, crs TEXT, stac TEXT, exc TEXT, tb TEXT)""")
        connection.commit()


def new_gpkg_item(
    gpkg_s3_key: str,
    new_stac_item_s3_key: str,
    thumbnail_png_s3_key: str,
    bucket: str,
    ripple_version: str,
    mip_case_no: str,
    dev_mode: bool = False,
):
    logging.info("Creating item from gpkg")
    # Instantitate S3 resources

    session, s3_client, s3_resource = init_s3_resources(dev_mode)
    print(gpkg_s3_key.replace(Path(gpkg_s3_key).name, ""))
    asset_list = list_keys(s3_client, bucket, gpkg_s3_key.replace(Path(gpkg_s3_key).name, ""))

    gdfs = gpkg_to_geodataframe(f"s3://{bucket}/{gpkg_s3_key}")
    river_miles = get_river_miles(gdfs["River"])
    crs = gdfs["River"].crs
    gdfs = reproject(gdfs)

    logging.info("Creating png thumbnail")
    create_thumbnail_from_gpkg(gdfs, thumbnail_png_s3_key, bucket, s3_client)

    # Create item
    bbox = pd.concat(gdfs).total_bounds
    footprint = bbox_to_polygon(bbox)
    item = create_geom_item(
        gpkg_s3_key,
        bbox,
        footprint,
        ripple_version,
        gdfs["XS"].iloc[0],
        river_miles,
        crs,
        mip_case_no,
    )

    asset_list = asset_list + [thumbnail_png_s3_key, gpkg_s3_key]
    for asset_key in asset_list:
        obj = s3_resource.Bucket(bucket).Object(asset_key)
        metadata = get_basic_object_metadata(obj)
        asset_info = get_asset_info(asset_key, bucket)
        if asset_key == thumbnail_png_s3_key:
            asset = pystac.Asset(
                s3_key_public_url_converter(f"s3://{bucket}/{asset_key}"),
                extra_fields=metadata,
                roles=asset_info["roles"],
                description=asset_info["description"],
            )
        else:
            asset = pystac.Asset(
                f"s3://{bucket}/{asset_key}",
                extra_fields=metadata,
                roles=asset_info["roles"],
                description=asset_info["description"],
            )
        item.add_asset(asset_info["title"], asset)

    s3_client.put_object(
        Body=json.dumps(item.to_dict()).encode(),
        Bucket=bucket,
        Key=new_stac_item_s3_key,
    )

    logging.info("Program completed successfully")


def main(
    db_path: str,
    table_name: str,
    bucket: str,
    ripple_version: str,
    mip_case_no: str,
):
    out_table = f"gpkg_stac_{table_name.split("_")[-1]}"
    create_table(db_path, out_table)

    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        cursor.execute(f"select key,crs,gpkg from {table_name}")
        keys = cursor.fetchall()
        for i, (key, crs, gpkg_key) in enumerate(keys):
            gpkg_key = gpkg_key.replace(f"s3://{bucket}/", "")

            logging.info(f"working on ({i+1}/{len(keys)} | {round(100*(i+1)/len(keys),1)}% | key: {key}")
            exc, tb = ["null"] * 2
            thumbnail_png_s3_key = key.replace("mip", "stac").replace(".prj", ".png")
            new_stac_item_s3_key = key.replace("mip", "stac").replace(".prj", ".json")
            try:
                new_gpkg_item(
                    gpkg_key,
                    new_stac_item_s3_key,
                    thumbnail_png_s3_key,
                    bucket,
                    ripple_version,
                    mip_case_no,
                )
            except Exception as e:
                exc = str(e)
                tb = traceback.format_exc()
                logging.error(exc)
            cursor.execute(
                f"""insert or replace into {out_table} (key, gpkg, crs, stac, exc, tb) values (?,?,?,?,?,?)""",
                (key, gpkg_key, crs, new_stac_item_s3_key, exc, tb),
            )
            connection.commit()


if __name__ == "__main__":

    db_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\mip_models\tx_ble.db"
    table_name = "geom_gpkg_A"
    mip_case_no = "ble_test"

    bucket = "fim"
    ripple_version = RIPPLE_VERSION
    main(db_path, table_name, bucket, ripple_version, mip_case_no)
