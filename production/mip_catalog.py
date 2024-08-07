"""Build STAC Catalog for MIP HEC-RAS models."""

# mip_catalog.py
import logging
from typing import List

import pystac
import pystac_client
from dotenv import load_dotenv

from production.db_utils import PGFim
from production.headers import get_auth_header
from ripple1d.ripple1d_logger import configure_logging
from ripple1d.utils.stac_utils import (
    collection_exists,
    create_collection,
    delete_collection,
    key_to_uri,
    upsert_collection,
    upsert_item,
)

load_dotenv()

STAC_ENDPOINT = "https://stac2.dewberryanalytics.com"


GROUP_1a = {
    "pg_group": "a",
    "sql_condition": "AND stac_complete=true AND conflation_complete=true",
    "collection_id": "owp_30_pct_mip_cv1_g1",
    "collection_title": "OWP 30% Group 1a Conflated FEMA MIP Models",
    "description": "HEC-RAS models collected from FEMA's Mapping Information Platform. Group 1a collection contains \
                                        models within the OWP 30% coverage area that have \
                                        been auto-georeferenced with modest confidence and \
                                        version 1.alpha conflation process was successful.",
}

GROUP_1b = {
    "pg_group": "a",
    "sql_condition": "AND stac_complete=true AND (conflation_complete=false or conflation_complete is null)",
    "collection_id": "owp_30_pct_mip_noc_g1",
    "collection_title": "OWP 30% Group 1b FEMA MIP Models (pending conflation)",
    "description": "HEC-RAS models collected from FEMA's Mapping Information Platform. Group 1b collection contains \
                                        models within the OWP 30% coverage area that have \
                                        been auto-georeferenced with modest confidence but \
                                        version 1.alpha conflation process failed.",
}


GROUP_2a = {
    "pg_group": "b",
    "sql_condition": "AND stac_complete=true AND conflation_complete=true",
    "collection_id": "owp_30_pct_mip_cv1_g2",
    "collection_title": "OWP 30% Group 2a Conflated FEMA MIP Models",
    "description": "HEC-RAS models collected from FEMA's Mapping Information Platform. Group 2a collection contains \
                                        models within the OWP 30% coverage area that have \
                                        been auto-georeferenced with low confidence and \
                                        version 1.alpha conflation process was successful.",
}

GROUP_2b = {
    "pg_group": "b",
    "sql_condition": "AND stac_complete=true AND (conflation_complete=false or conflation_complete is null)",
    "collection_id": "owp_30_pct_mip_noc_g2",
    "collection_title": "OWP 30% Group 2b FEMA MIP Models (pending conflation)",
    "description": "HEC-RAS models collected from FEMA's Mapping Information Platform. Group 2b collection contains \
                                        models within the OWP 30% coverage area that have \
                                        been auto-georeferenced with low confidence but \
                                        version 1.alpha conflation process failed.",
}


GROUP_3 = {
    "pg_group": "tx_ble",
    "sql_condition": "AND stac_complete=true AND conflation_complete=true",
    "collection_id": "ripple1d_test_data",
    "collection_title": "Test collection for Ripple using Texas BLE data.",
    "description": "Test collection for Ripple using Texas BLE data accessed via https://webapps.usgs.gov/infrm/estbfe/",
}


def list_items_on_s3(table_name: str, mip_group: str, bucket: str = "fim", condition: str = None) -> List[pystac.Item]:
    """Read from database a list of geopackages to create stac items."""
    db = PGFim()

    data = db.read_cases(
        table_name,
        [
            "case_id",
            "s3_key",
        ],
        mip_group,
        condition,
    )

    stac_items = []
    logging.info(f"Found {len(data)} items for group {mip_group}")
    for i, (mip_case, s3_ras_project_key) in enumerate(data):
        stac_item_s3_key = s3_ras_project_key.replace("mip", "stac").replace(".prj", ".json")

        try:
            logging.info(f"loading from case: {mip_case} | key found:  {stac_item_s3_key}")
            item = pystac.Item.from_file(key_to_uri(stac_item_s3_key, bucket=bucket))
            stac_items.append(item)
        except Exception:
            logging.error(f"Error reading item: {stac_item_s3_key}")
            continue

    return stac_items


if __name__ == "__main__":
    configure_logging(level=logging.INFO, logfile="create_stac.log")
    table_name = "processing"
    bucket = "fim"

    for group in [GROUP_1a, GROUP_1b, GROUP_2a, GROUP_2b, GROUP_3]:
        mip_group = group["pg_group"]
        collection_id = group["collection_id"]
        collection_title = group["collection_title"]

        header = get_auth_header()

        # TODO: Be careful temporarily deteleting collections
        r = collection_exists(STAC_ENDPOINT, collection_id)
        if r.ok:
            delete_collection(STAC_ENDPOINT, collection_id, header)

        stac_items = list_items_on_s3(table_name, mip_group, bucket, condition=group["sql_condition"])

        if len(stac_items) == 0:
            continue

        else:
            first_items = stac_items[0:2]
            collection = create_collection(
                first_items,
                collection_id,
                group["description"],
                collection_title,
            )
            logging.info(f"Created collection {collection.id}")
            upsert_collection(STAC_ENDPOINT, collection, header)
            logging.info("Added collection to catalog")

            for item in stac_items:
                try:
                    item.collection_id = collection_id
                    upsert_item(STAC_ENDPOINT, collection_id, item, header)
                    logging.info(f"upserting item: {item.id}")

                except Exception as e:
                    logging.error(f"Error upserting item: {e}")

            client = pystac_client.Client.open(STAC_ENDPOINT)
            collection = client.get_collection(collection_id)
            collection.update_extent_from_items()
            header = get_auth_header()
            upsert_collection(STAC_ENDPOINT, collection, header)
            logging.info("Updated collection extent from items")
