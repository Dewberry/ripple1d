
import logging
import sqlite3
import traceback

from ripple.consts import RIPPLE_VERSION
from ripple.ras_to_gpkg import new_gpkg_item_s3

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
                new_gpkg_item_s3(
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
