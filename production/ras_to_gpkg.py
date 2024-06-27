import logging
import traceback

from dotenv import find_dotenv, load_dotenv

from ripple.ripple_logger import configure_logging
from ripple.scripts.geom_to_gpkg import process_one_geom

from .db_utils import PGFim

load_dotenv()


def main(cases_db_path: str, table_name: str, bucket: str = None):
    """
    Reads from database a list of ras files to convert to geopackage
    """
    db = PGFim()
    mip_group = "a"

    keys = db.read_case_db(table_name)
    for i, (mip_case, key, crs) in enumerate(keys):
        key = key.replace("s3://fim/", "")
        gpkg_path, exc, tb = ["null"] * 3

        logging.info(f"working on ({i+1}/{len(keys)} | {round(100*(i+1)/len(keys),1)}% | key: {key}")
        try:
            gpkg_path = process_one_geom(key, crs, bucket)
            db.update_case_status(mip_group, mip_case, key, True, None, None)
            db.update_table(
                "mip_gpkg",
                ("mip_group", "mip_case", "key", "gpkg"),
                (mip_group, mip_case, key, gpkg_path),
            )
        except Exception as e:
            exc = e
            tb = traceback.format_exc()
            logging.error(exc)
            db.update_case_status(mip_group, mip_case, key, False, exc, tb)
            db.update_table(
                "mip_gpkg",
                ("mip_group", "mip_case", "key", "gpkg"),
                (mip_group, mip_case, key, gpkg_path),
            )


if __name__ == "__main__":

    configure_logging(level=logging.INFO, logfile="geom_to_gpkg.log")
    load_dotenv(find_dotenv())

    db_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\mip_models\tx_ble.db"
    table_name = "tx_ble_crs_A"
    bucket = "fim"  # "fim"

    s3_prefix = "mip/dev2"
    prescribed_crs = 2277
    create_ble_db = True

    if create_ble_db:
        db = PGFim()
        db.create_table("inferred_crs_tx_ble", [("mip_case", "TEXT"), ("key", "TEXT"), (crs)])

    main(db_path, table_name, bucket)
