import logging
import traceback
from time import sleep

from production.db_utils import PGFim
from ripple.consts import RIPPLE_VERSION
from ripple.ras_to_gpkg import new_stac_item_s3
from ripple.ripple_logger import configure_logging


def main(mip_group: str, table_name: str, bucket: str, ripple_version: str):
    """Read from database a list of geopackages to create stac items."""
    db = PGFim()
    optional_condition = "AND gpkg_complete=true AND stac_complete IS NULL"
    data = db.read_cases(table_name, ["case_id", "s3_key"], mip_group, optional_condition)
    while data:

        for i, (mip_case, s3_ras_project_key) in enumerate(data):

            gpkg_key = s3_ras_project_key.replace(".prj", ".gpkg")
            thumbnail_png_s3_key = s3_ras_project_key.replace("mip", "stac").replace(".prj", ".png")
            new_stac_item_s3_key = s3_ras_project_key.replace("mip", "stac").replace(".prj", ".json")

            logging.info(
                f"progress: ({i+1}/{len(data)} | {round(100*(i+1)/len(data),1)}% | working on key: {s3_ras_project_key}"
            )

            try:
                new_stac_item_s3(
                    gpkg_key,
                    new_stac_item_s3_key,
                    thumbnail_png_s3_key,
                    bucket,
                    ripple_version,
                    mip_case,
                )

                db.update_case_status(mip_group, mip_case, s3_ras_project_key, True, None, None, "stac")
                logging.info(f"Successfully finished stac item for {s3_ras_project_key}")
            except Exception as e:
                exc = str(e)
                tb = str(traceback.format_exc())
                logging.error(exc)
                db.update_case_status(mip_group, mip_case, s3_ras_project_key, False, exc, tb, "stac")
        sleep(1)
        data = db.read_cases(table_name, ["case_id", "s3_key"], mip_group, optional_condition)


if __name__ == "__main__":
    configure_logging(level=logging.INFO, logfile="create_stac.log")

    mip_group = "tx_ble"
    table_name = "processing "
    bucket = "fim"
    ripple_version = RIPPLE_VERSION

    main(mip_group, table_name, bucket, ripple_version)
