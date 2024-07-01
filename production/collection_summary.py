import logging

import pystac_client

from production.headers import get_auth_header
from ripple.utils.stac_utils import (
    upsert_collection,
)

STAC_ENDPOINT = "https://stac2.dewberryanalytics.com"

client = pystac_client.Client.open(STAC_ENDPOINT)
collections = client.get_collections()

for collection in client.get_collections():
    logging.info(f"Adding summary to {collection.id}")
    version_summary = {}
    coverage_summary = {
        "1D_HEC-RAS_models": 0,
        "1D_HEC-RAS_river_miles": 0,
        "2D_HEC-RAS_models": 0,
    }

    for item in collection.get_all_items():
        river_miles = float(item.properties["river miles"])

        coverage_summary["1D_HEC-RAS_river_miles"] += river_miles
        coverage_summary["1D_HEC-RAS_models"] += 1

        case_no = item.properties["MIP:case_ID"]
        ras_version = item.properties["ras version"]

        if ras_version not in version_summary:
            version_summary[ras_version] = {case_no: {"river_miles": 0, "ras_models": 0}}

        if case_no not in version_summary[ras_version]:
            version_summary[ras_version][case_no] = {"river_miles": 0, "ras_models": 0}

        version_summary[ras_version][case_no]["river_miles"] += river_miles
        version_summary[ras_version][case_no]["ras_models"] += 1

    coverage_summary["1D_HEC-RAS_river_miles"] = int(coverage_summary["1D_HEC-RAS_river_miles"])

    # collection.summaries.remove("ras_version_summary_by_MIP_case_ID")
    # collection.summaries.remove("ras_version_summary")
    collection.summaries.add("coverage", coverage_summary)
    collection.summaries.add("ras_version_summary_with_MIP_case_IDs", version_summary)
    header = get_auth_header()
    upsert_collection(STAC_ENDPOINT, collection, header)
