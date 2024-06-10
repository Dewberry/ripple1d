import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import pystac
import pystac_client
import requests

from ripple.conflate.plotter import plot_conflation_results
from ripple.conflate.rasfim import RasFimConflater
from ripple.conflate.run_rasfim import main as conflate_main

STAC_API_URL = "https://stac2.dewberryanalytics.com"
# from ripple.conflate.run_rasfim import conflate_branches, point_method_conflation

logging.getLogger("fiona").setLevel(logging.ERROR)
logging.getLogger("botocore").setLevel(logging.ERROR)


def upsert_item(endpoint: str, collection_id: str, item: pystac.Item) -> str:
    items_url = f"{endpoint}/collections/{collection_id}/items"
    response = requests.post(items_url, json=item.to_dict())
    if response.status_code == 409:
        item_update_url = f"{items_url}/{item.id}"
        response = requests.put(item_update_url, json=item.to_dict())
    if not response.ok:
        return f"Response from STAC API: {response.status_code}"


def href_to_vsis(href: str, bucket: str) -> str:
    return href.replace(f"https://{bucket}.s3.amazonaws.com", f"/vsis3/{bucket}")


def main(item, client, bucket, s3_prefix, collection_id, rfc, low_flows, river_reach_name):
    conflation_results = conflate_main(rfc, low_flows)

    if conflation_results["metrics"]["conflation_score"] == 0:
        fim_stream = rfc.local_nwm_reaches
        limit_plot = False
    else:
        # us_most_branch_id, ds_most_branch_id, summary = conflate_branches(rfc)

        # item.properties["NWM_FIM:Upstream_Branch_ID"] = us_most_branch_id
        # item.properties["NWM_FIM:Downstream_Branch_ID"] = ds_most_branch_id
        ids = [r for r in conflation_results.keys() if r not in ["metrics", "ras_river_to_nwm_reaches_ratio"]]

        fim_stream = rfc.local_nwm_reaches[rfc.local_nwm_reaches["ID"].isin(ids)]

        ripple_parameters_key = f"{s3_prefix}/ripple_parameters.json"
        ripple_parameters_href = f"https://{bucket}.s3.amazonaws.com/{ripple_parameters_key}"

        client.put_object(
            Body=json.dumps(conflation_results).encode(),
            Bucket=bucket,
            Key=ripple_parameters_key,
        )

        # Add ripple parameters asset to item
        item.add_asset(
            ripple_parameters_key.replace(" ", ""),
            pystac.Asset(
                ripple_parameters_href,
                title="ConflationParameters",
                roles=[pystac.MediaType.JSON, "ripple-params"],
                extra_fields={
                    "software": "ripple v0.1.0-alpha.1",
                    "date_created": datetime.now().isoformat(),
                },
            ),
        )

        for asset in item.get_assets():
            item.assets[asset].href = item.assets[asset].href.replace("https:/fim", "https://fim")
        limit_plot = True

    conflation_thumbnail_key = f"stac/{collection_id}/thumbnails/{item.id}-conflation.png".replace(" ", "")
    conflation_thumbnail_href = f"https://{bucket}.s3.amazonaws.com/{conflation_thumbnail_key}"

    item.properties["NWM_FIM:Conflation_Results"] = conflation_results

    plot_conflation_results(
        rfc,
        fim_stream,
        conflation_thumbnail_key,
        bucket=bucket,
        s3_client=client,
        limit_plot_to_nearby_reaches=limit_plot,
    )

    # Add thumbnal asset to item
    item.add_asset(
        conflation_thumbnail_key,
        pystac.Asset(
            conflation_thumbnail_href.replace(" ", ""),
            title="ThumbnailConflationResults",
            roles=[pystac.MediaType.PNG, "thumbnail"],
            extra_fields={
                "software": "ripple v0.1.0-alpha.1",
                "date_created": datetime.now().isoformat(),
            },
        ),
    )

    r = upsert_item(STAC_API_URL, collection_id, item)
    logging.info(f"collection_id {collection_id}, item_id, {item.id}, response {r}")


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, filename="conflate_with_stac-v1.log")

    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--collection_id", type=str, required=True, help="Collection ID")

    args = parser.parse_args()
    collection_id = args.collection_id

    bucket = "fim"

    # Connect to STAC API
    client = pystac_client.Client.open(STAC_API_URL)
    collection = client.get_collection(collection_id)

    # Fetch the branch geopackage containint control points
    # role="ras-conflation"
    # for asset in collection.get_assets():

    # branches_s3_key = collection.assets["conflation-ref-v1"].extra_fields["s3_key"]
    # nwm_gpkg = f"/vsis3/{branches_s3_key}"
    # logging.debug("nwm_gpkg", branches_s3_key)

    nwm_pq = "nwm_flows.parquet"

    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    low_flows = pd.read_parquet("nwm_high_water_threshold.parquet")

    client = session.client("s3")

    # Get all items in the collection
    items = collection.get_all_items()
    for item in items:
        logging.info(item.id)
        for asset in item.get_assets(role="ras-geometry-gpkg"):
            gpkg_name = Path(item.assets[asset].href).name
            s3_prefix = (
                item.assets[asset].href.replace(f"https://{bucket}.s3.amazonaws.com/", "").replace(f"/{gpkg_name}", "")
            )
            ras_gpkg = href_to_vsis(item.assets[asset].href, bucket="fim")

        rfc = RasFimConflater(nwm_pq, ras_gpkg)

        for river_reach_name in rfc.ras_river_reach_names:
            logging.info(f"item_id {item.id}, river_reach {river_reach_name}")

        try:
            main(
                item,
                client,
                bucket,
                s3_prefix,
                collection_id,
                rfc,
                low_flows,
                river_reach_name,
            )
            logging.info(f"{item.id}: Successfully processed")
        except Exception as e:
            logging.error(f"{item.id}: Error processing | {e}")
