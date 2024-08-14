"""Create STAC Item for HEC-RAS model."""

import logging
import traceback
from time import sleep

from production.db_utils import PGFim
import ripple1d
from ripple1d.ras_to_gpkg import new_stac_item_s3
from ripple1d.ripple1d_logger import configure_logging


def main(mip_group: str, processing_table_name: str, bucket: str, ripple1d_version: str):
    """Read from database a list of geopackages to create stac items."""
    db = PGFim()
    optional_condition = "AND gpkg_complete=true AND stac_complete IS NULL"
    data = db.read_cases(processing_table_name, ["case_id", "s3_key"], mip_group, optional_condition)
    while data:
        for i, (mip_case, s3_ras_project_key) in enumerate(data):
            gpkg_key = s3_ras_project_key.replace(".prj", ".gpkg").replace("dev2", "gpkg_tx_ble_1")
            thumbnail_png_s3_key = (
                s3_ras_project_key.replace("mip", "stac").replace("dev2", "tx_ble_1").replace(".prj", ".png")
            )
            new_stac_item_s3_key = (
                s3_ras_project_key.replace("mip", "stac").replace("dev2", "tx_ble_1").replace(".prj", ".json")
            )

            logging.info(
                f"Progress: ({i+1}/{len(data)} | {round(100*(i+1)/len(data),1)}% | working on key: {s3_ras_project_key}"
            )

            try:
                new_stac_item_s3(
                    gpkg_key,
                    new_stac_item_s3_key,
                    thumbnail_png_s3_key,
                    s3_ras_project_key,
                    bucket,
                    mip_case,
                )

                db.update_case_status(
                    processing_table_name, mip_group, mip_case, s3_ras_project_key, True, None, None, "stac"
                )
                logging.debug(f"Successfully finished stac item for {s3_ras_project_key}")
            except Exception as e:
                exc = str(e)
                tb = str(traceback.format_exc())
                logging.error(exc)
                logging.error(tb)
                db.update_case_status(
                    processing_table_name, mip_group, mip_case, s3_ras_project_key, False, exc, tb, "stac"
                )
        sleep(1)
        data = db.read_cases(processing_table_name, ["case_id", "s3_key"], mip_group, optional_condition)


if __name__ == "__main__":
    configure_logging(level=logging.INFO, logfile="create_stac.log")

    mip_group = "tx_ble_1"
    processing_table_name = "processing_tx_ble_1"
    bucket = "fim"
    ripple1d_version = ripple1d.__version__

    main(mip_group, processing_table_name, bucket, ripple1d_version)
