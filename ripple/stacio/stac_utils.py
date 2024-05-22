from typing import List

import pystac
import requests


def key_to_uri(key: str, bucket: str) -> str:
    return f"https://{bucket}.s3.amazonaws.com/{key}"


def uri_to_key(href: str, bucket: str) -> str:
    return href.replace(f"https://{bucket}.s3.amazonaws.com/", "")


def create_collection(
    models: List[pystac.Item], id: str, description: str = None, title: str = None
) -> pystac.Collection:
    extent = pystac.Extent.from_items(models)
    return pystac.Collection(
        id=id,
        description=description,
        title=title,
        extent=extent,
    )


def upsert_collection(endpoint: str, collection: pystac.Collection):
    collections_url = f"{endpoint}/collections"
    response = requests.post(collections_url, json=collection.to_dict())
    if response.status_code == 409:
        response = requests.put(collections_url, json=collection.to_dict())
        if response.status_code != 200:
            raise RuntimeError(f"Error upserting collection: {response.text}")
    elif response.status_code != 200:
        raise RuntimeError(f"Error upserting collection: {response.text}")


def upsert_item(endpoint: str, collection_id: str, item: pystac.Item):
    items_url = f"{endpoint}/collections/{collection_id}/items"
    response = requests.post(items_url, json=item.to_dict())
    if response.status_code == 409:
        item_update_url = f"{items_url}/{item.id}"
        response = requests.put(item_update_url, json=item.to_dict())
    if not response.ok:
        return f"Response from STAC API: {response.status_code}"
