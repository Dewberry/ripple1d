"""Extract geopackage from HEC-RAS geometry file."""

import logging
import traceback

from dotenv import find_dotenv, load_dotenv

from production.db_utils import PGFim
from ripple.errors import (
    NotAPrjFile,
)
from ripple.ras_to_gpkg import geom_to_gpkg_s3
from ripple.ripple_logger import configure_logging

load_dotenv()


def process_one_geom(
    key: str,
    crs: str,
    bucket: str = None,
):
    """Process one geometry file and convert it to geopackage."""
    # create path name for gpkg
    if key.endswith(".prj"):
        gpkg_path = key.replace("prj", "gpkg")
    else:
        raise NotAPrjFile(f"{key} does not have a '.prj' extension")

    # read the geometry and write the geopackage
    if bucket:
        geom_to_gpkg_s3(key, crs, gpkg_path, bucket)
    return f"s3://{bucket}/{gpkg_path}"


def main(table_name: str, mip_group: str, bucket: str = None):
    """Read from database a list of ras files to convert to geopackage."""
    db = PGFim()

    data = db.read_cases(table_name, ["mip_case", "s3_key", "crs"], mip_group)

    for i, (mip_case, key, crs) in enumerate(data):
        key = key.replace(f"s3://{bucket}/", "")

        logging.info(f"Working on ({i+1}/{len(data)} | {round(100*(i+1)/len(data),1)}% | key: {key}")
        try:
            _ = process_one_geom(key, crs, bucket)
            db.update_case_status(mip_group, mip_case, key, True, None, None, "gpkg")

        except Exception as e:
            exc = str(e)
            tb = str(traceback.format_exc())
            logging.error(exc)
            logging.error(tb)
            db.update_case_status(mip_group, mip_case, key, False, exc, tb, "gpkg")


if __name__ == "__main__":
    configure_logging(level=logging.INFO, logfile="extract_geometry.log")
    load_dotenv(find_dotenv())

    table_name = "inferred_crs"
    bucket = "fim"
    mip_group = "tx_ble"

    main(table_name, mip_group, bucket)
