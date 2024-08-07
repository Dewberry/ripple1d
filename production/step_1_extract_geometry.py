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


def main(crs_table_name: str, processing_table: str, mip_group: str, bucket: str = None):
    """Read from database a list of ras files to convert to geopackage."""
    db = PGFim()
    processing_data = db.read_cases(
        processing_table,
        ["s3_key"],
        mip_group,
        "AND gpkg_complete=false AND gpkg_exc='expected 1 result, no results found'",
    )
    processing_data = [i[0] for i in processing_data]

    crs_data = db.read_cases(crs_table_name, ["s3_key", "crs"], mip_group)

    for i, (key, crs) in enumerate(crs_data):
        if key.replace(f"s3://{bucket}/", "").replace("cases", "cases_s1") in processing_data:
            key = key.replace(f"s3://{bucket}/", "").replace("cases", "cases_s1")
            mip_case = key.split("cases_s1/")[1].split("/")[0]

            logging.info(
                f"Working on ({i+1}/{len(crs_data)} | {round(100*(i+1)/len(crs_data),1)}% | key: {key} | mip case: {mip_case}"
            )
            try:
                _ = process_one_geom(key, crs, bucket)
                db.update_case_status(processing_table, mip_group, mip_case, key, True, None, None, "gpkg")

            except Exception as e:
                exc = str(e)
                tb = str(traceback.format_exc())
                logging.error(exc)
                logging.error(tb)
                db.update_case_status(processing_table, mip_group, mip_case, key, False, exc, tb, "gpkg")


if __name__ == "__main__":
    configure_logging(level=logging.INFO, logfile="extract_geometry.log")
    load_dotenv(find_dotenv())

    crs_table_name = "inferred_crs_v2"
    bucket = "fim"
    mip_group = "b"
    processing_table = "processing_s1"

    main(crs_table_name, processing_table, mip_group, bucket)
