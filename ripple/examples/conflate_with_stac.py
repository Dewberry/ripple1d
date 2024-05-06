import pystac_client
import pystac
import os
import boto3
import json
import requests
from urljoin import url_path_join
import logging

from pathlib import Path    

from ripple.conflate.ras1d import RasFimConflater, nwm_conflated_reaches
from ripple.conflate import run
from ripple.conflate.plotter import plot_conflation_results

from ripple.conflate.ras1d import STAC_API_URL


def create_update_item(endpoint: str, collection_id: str, item: pystac.Item):
    items_url = url_path_join(endpoint, f"collections/{collection_id}/items")
    response = requests.post(items_url, json=item.to_dict())
    if response.status_code == 409:
        item_update_url = url_path_join(items_url, f"{item.id}")
        response = requests.put(item_update_url, json=item.to_dict())

def href_to_vsis(href:str, bucket:str) -> str:
    return href.replace(f"https://{bucket}.s3.amazonaws.com",f"/vsis3/{bucket}")


def main(item, nwm_gpkg, client, bucket, collection_id):
    for asset in item.get_assets(role="ras-geometry-gpkg"):
        gpkg_name = Path(item.assets[asset].href).name 
        s3_prefix =item.assets[asset].href.replace(f"https://{bucket}.s3.amazonaws.com/","").replace(f"/{gpkg_name}","")
        ras_gpkg =href_to_vsis(item.assets[asset].href, bucket="fim")

    rfc = RasFimConflater(nwm_gpkg, ras_gpkg)
    summary = run.main(rfc)

    fim_stream = nwm_conflated_reaches(rfc, summary)

    ripple_parameters_key = f"{s3_prefix}/ripple_parameters.json"
    ripple_parameters_href = f"https://{bucket}.s3.amazonaws.com/{ripple_parameters_key}"

    client.put_object(
        Body=json.dumps(summary).encode(),
        Bucket=bucket,
        Key=ripple_parameters_key,
    )

    # Add ripple parameters asset to item
    item.add_asset(
        ripple_parameters_key.replace(" ",""),
        pystac.Asset(
            ripple_parameters_href,
            title="ConflationParameters",
            roles=[pystac.MediaType.JSON, "ripple-params"],
           extra_fields={"software": "ripple v0.1.0-alpha.1", "date_created": "5-1-2024"},
        ),
    )


    conflation_thumbnail_key = f"stac/{collection_id}/thumbnails/{item.id}-conflation.png"
    conflation_thumbnail_href = f"https://{bucket}.s3.amazonaws.com/{conflation_thumbnail_key}"

    plot_conflation_results(
        rfc,
        fim_stream,
        conflation_thumbnail_key,
        bucket=bucket,
        s3_client=client,
    )

    # Add thumbnal asset to item
    item.add_asset(
        conflation_thumbnail_key,
        pystac.Asset(
            conflation_thumbnail_href.replace(" ",""),
            title="ConflationThumbnail",
            roles=[pystac.MediaType.PNG, "Thumbnail"],
           extra_fields={"software": "ripple v0.1.0-alpha.1", "date_created": "5-1-2024"},
        ),
    )

    create_update_item(STAC_API_URL, collection_id, item)


if __name__=="__main__":

    logging.basicConfig(level=logging.ERROR,
                        filename="conflate_with_stac-v2.log")

    STAC_API_URL = "https://stac.dewberryanalytics.com"
    collection_id = "huc-12040101"

    # Connect to STAC API
    client = pystac_client.Client.open(STAC_API_URL)
    collection = client.get_collection(collection_id)
    branch_item = collection.get_item("nwm-huc-12040101-branches-v2024-5-4")

    # Fetch the branch geopackage containint control points
    for asset in branch_item.get_assets(role="ras-conflation"):
        nwm_gpkg = href_to_vsis(branch_item.assets[asset].href, bucket="fim")
        
    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    client = session.client("s3")
    bucket = "fim"

    # Get all items in the collection
    items = collection.get_all_items()
    i=0
    for item in items:
        if item.id == branch_item.id:
            continue

        try:
            main(item, nwm_gpkg, client, bucket, collection_id)
            logging.critical(f"{item.id}: Successfully processed")
        except Exception as e:
            logging.error(f"{item.id}: Error processing | {e}")
            continue