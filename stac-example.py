import pystac_client
import pystac
from dotenv import load_dotenv
from pathlib import Path
import requests
from urljoin import url_path_join

load_dotenv()


def create_update_item(endpoint: str, collection_id: str, item: pystac.Item):
    items_url = url_path_join(endpoint, f"collections/{collection_id}/items")
    response = requests.post(items_url, json=item.to_dict())
    if response.status_code == 409:
        print(f"Item {item.id} already exists")
        item_update_url = url_path_join(items_url, f"{item.id}")
        print(items_url, "-->", item_update_url)
        print("Retrying with PUT...")
        response = requests.put(item_update_url, json=item.to_dict())
    if not response.ok:
        print(f"Response from STAC API: {response.status_code}")
        print(response.text)


API_URL = "https://stac.dewberryanalytics.com"

collection_id = "huc-12040101"

dev_item_ids = ["CANEY_CREEK_NORTH-629f", "WFSJ_Main-cd42", "STEWARTS_CREEK-958e"]


# Connect to STAC API
client = pystac_client.Client.open(API_URL)
collection = client.get_collection(collection_id)

# Iterate over items in collection
for item in collection.get_all_items():
    if item.id in dev_item_ids:
        print(f"Found first item: {item.id}")
        break

# Grab the gpkg location (href)
for asset_name in item.get_assets(role=pystac.MediaType.GEOPACKAGE):
    asset = item.assets[asset_name]
    print(f"Found data asset: {asset.href}")

# Use the gpkg href to create path for storing terrain
topo_href = Path(asset.href).parent / "Terrain/Terrain.tif"
ras_topo_href = Path(asset.href).parent / "Terrain/Terrain.hdf"

print(f"Copy topo to : {topo_href}")
print(f"Copy ras_topo_href to : {ras_topo_href}")

# copy terrain to s3 here...

# Add topo asset to item
item.add_asset(
    "Terrain.tif",
    pystac.Asset(
        topo_href,
        title="Terrain.tif",
        roles=[pystac.MediaType.COG],
        extra_fields={
            "topo_source_url": "usgs...",
            "date_aquired": "5-1-2024",
            "foo": "bar",
        },
    ),
)

# Add ras topo asset to item
item.add_asset(
    "Terrain.hdf",
    pystac.Asset(
        topo_href,
        title="Terrain.hdf",
        roles=[pystac.MediaType.HDF],
        extra_fields={"software": "ripple", "date_created": "5-1-2024", "foo": "bar"},
    ),
)

# Turn this on to update the stac item
# create_update_item(API_URL, collection.id, item)
