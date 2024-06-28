import json
import os
from pathlib import Path
from typing import List
from urllib.parse import requote_uri

import boto3
import pystac
import requests

from ripple.utils.s3_utils import s3_get_output_s3path


def key_to_uri(key: str, bucket: str) -> str:
    """Convert a key to a uri."""
    return f"https://{bucket}.s3.amazonaws.com/{key}"


def uri_to_key(href: str, bucket: str) -> str:
    """Convert a uri to a key."""
    return href.replace(f"https://{bucket}.s3.amazonaws.com/", "")


def create_collection(
    models: List[pystac.Item], id: str, description: str = None, title: str = None
) -> pystac.Collection:
    """Create a STAC collection from a list of STAC items."""
    extent = pystac.Extent.from_items(models)
    return pystac.Collection(
        id=id,
        description=description,
        title=title,
        extent=extent,
    )


def upsert_collection(endpoint: str, collection: pystac.Collection):
    """Upsert a collection to a STAC API."""
    collections_url = f"{endpoint}/collections"
    response = requests.post(collections_url, json=collection.to_dict())
    if response.status_code == 409:
        response = requests.put(collections_url, json=collection.to_dict())
        if response.status_code != 200:
            raise RuntimeError(f"Error upserting collection: {response.text}")
    elif response.status_code != 200:
        raise RuntimeError(f"Error upserting collection: {response.text}")


def upsert_item(endpoint: str, collection_id: str, item: pystac.Item):
    """Upsert an item to a STAC API."""
    items_url = f"{endpoint}/collections/{collection_id}/items"
    response = requests.post(items_url, json=item.to_dict())
    if response.status_code == 409:
        item_update_url = f"{items_url}/{item.id}"
        response = requests.put(item_update_url, json=item.to_dict())
    if not response.ok:
        return f"Response from STAC API: {response.status_code}"


def derive_input_from_stac_item(
    ras_model_stac_href: str,
    ras_directory: str,
    client: boto3.session.Session.client,
    bucket: str,
) -> tuple:
    """TODO: leverage the contents of this function but split it into multiple functions."""
    # read stac item
    stac_item = pystac.Item.from_file(requote_uri(ras_model_stac_href))

    # download RAS model from stac item. derive terrain_name during download.
    # terrain_name is the basename of the terrain hdf without extension.
    terrain_name = download_model_from_stac_item(stac_item, ras_directory, client, bucket)

    # get nwm conflation parameters
    # ripple_parameters = create_ripple_parameters_from_stac_item(stac_item, client, bucket)

    # directory for post processed depth grids/sqlite db. The default is None which will not upload to s3
    postprocessed_output_s3_path = s3_get_output_s3path(bucket, ras_model_stac_href)

    # return terrain_name, ripple_parameters, postprocessed_output_s3_path


def get_conflation_parameters_from_stac_item(
    stac_item: pystac.Item, client: boto3.session.Session.client, bucket: str
) -> dict:
    """Get a dictionary of conflation parameters from a stac item."""
    # create nwm dictionary
    for _, asset in stac_item.get_assets(role="nwm_conflation").items():

        response = client.get_object(
            Bucket=bucket,
            Key=asset.href.replace(f"https://{bucket}.s3.amazonaws.com/", ""),
        )
        json_data = response["Body"].read()

    return json.loads(json_data)


def download_model_from_stac_item(
    stac_item: pystac.Item,
    ras_directory: str,
    client: boto3.session.Session.client,
    bucket: str,
) -> str:
    """Download HEC-RAS model from stac href. Return the terrain_name."""
    # make RAS directory if it does not exists
    if not os.path.exists(ras_directory):
        os.makedirs(ras_directory)

    # download HEC-RAS model files
    for _, asset in stac_item.get_assets(role="hec-ras").items():

        s3_key = asset.extra_fields["s3_key"]

        file = os.path.join(ras_directory, Path(s3_key).name)
        client.download_file(bucket, s3_key, file)

    # download HEC-RAS topo files
    for _, asset in stac_item.get_assets(role="ras-topo").items():

        s3_key = asset.extra_fields["s3_key"]

        file = os.path.join(ras_directory, Path(s3_key).name)
        client.download_file(bucket, s3_key, file)

        if ".hdf" in Path(s3_key).name:
            terrain_name = Path(s3_key).name.rstrip(".hdf")

    return terrain_name
