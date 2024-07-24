"""
This script downloads GeoPackage and Conflation files for models in an STAC collection, to a local folder.
"""

import os
import urllib.parse
import urllib.request

import boto3
import pystac_client

stac_endpoint = "https://stac2.dewberryanalytics.com"
stac_collection = "ripple_test_data"
source_models_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\source_models"
aws_profile = "DewFIM"

client = pystac_client.Client.open(stac_endpoint)
collection = client.get_collection(stac_collection)

session = boto3.Session(profile_name=aws_profile)
s3_client = session.client("s3")

models_data = {}
for item in collection.get_items():
    models_data[item.id] = {}
    if not item.properties["ras version"][0] == 3:
        models_data[item.id]["gpkg"] = item.assets["GeoPackage_file"].href
        models_data[item.id]["conflation"] = item.assets["NWM_Conflation"].href

for id, files in models_data.items():
    try:
        model_dir = os.path.join(source_models_dir, id)
        os.makedirs(model_dir, exist_ok=True)

        local_gpkg_path = os.path.join(model_dir, f"{id}.gpkg")
        gpkg_url = files["gpkg"]
        bucket_name, key = gpkg_url.replace("s3://", "").split("/", 1)
        s3_client.download_file(bucket_name, key, local_gpkg_path)

        local_conflation_path = os.path.join(model_dir, f"{id}.conflation.json")
        encoded_conflation_url = urllib.parse.quote(files["conflation"], safe=":/")
        urllib.request.urlretrieve(encoded_conflation_url, local_conflation_path)

        print(f"Successfully downloaded files for {id}")
    except Exception as e:
        print(f"Failed to download files for {id}: {e}")
