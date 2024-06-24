import json
import logging
import re
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict

import boto3
import contextily as ctx
import fiona
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import pystac
from shapely import Polygon, to_geojson

from ripple.consts import LAYER_COLORS
from ripple.utils import get_sessioned_s3_client, str_from_s3

from .utils.s3_utils import split_s3_key


def gpkg_to_geodataframe(gpkg_local_path: str) -> dict:
    """
    Converts a local geopackage file to a GeoDataFrame.

    Parameters:
        gpkg_local_path (str): Path of locally saved geopackage.

    Returns:
        gpkg_gdf (dict): dictionary of GeoDataFrame.

    """
    layers = fiona.listlayers(gpkg_local_path)
    gdfs = {}

    for layer in layers:
        gdfs[layer] = gpd.read_file(gpkg_local_path, layer=layer).to_crs(4326)  # reproject to WSG 84

    return gdfs


def create_thumbnail_from_gpkg(gdfs: dict, png_s3_path: str, s3_client: boto3.Session.client):
    """
    Generates a PNG thumbnail for a geopandas dataframe and uploads it to AWS S3.

    Parameters:
    - gdf (dict): A dictionary of geopandas dataframes containing the geometries to plot.
    - png_s3_path (str): The S3 path where the generated PNG thumbnail is to be stored.
    - s3_client: The AWS S3 client instance used for uploading the PNG.
    """

    # Define colors for each layer type

    # Plotting
    fig, ax = plt.subplots(1, 1, figsize=(10, 10))

    for layer, color in LAYER_COLORS.items():
        if layer in gdfs.keys():
            gdfs[layer].plot(ax=ax, color=LAYER_COLORS[layer], linewidth=1, label=layer)
            crs = gdfs[layer].crs
    # Add openstreetmap basemap
    ctx.add_basemap(ax, crs=crs)

    ax.legend()
    # Hide all axis text ticks or tick labels
    ax.set_xticks([])
    ax.set_yticks([])

    # Save plot to a bytes buffer
    buf = BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)

    bucket, key = split_s3_key(png_s3_path)
    # Download the PNG to s3
    s3_client.put_object(Bucket=bucket, Key=key, Body=buf, ContentType="image/png")


def create_geom_item(gpkg_key: str, bbox: Polygon, footprint: list[float]):
    """
    This function creates a PySTAC Item for a gpkg file stored in an AWS S3 bucket.

    Parameters:
        gpkg_key (str): gpkg file key for item_id naming.
        bbox: Item bounding box.
        footprint: Item Footprint.

    Returns:
        pystac.Item: The PySTAC Item representing the gpkg file.
    """

    gpkg_name = gpkg_key.split("/")[-1].replace(".gpkg", "")

    item_id = gpkg_name + " Geometry"

    # TODO: Adjust start_time selection for item
    start_time = datetime.utcnow()
    item = pystac.Item(
        id=item_id,
        geometry=json.loads(to_geojson(footprint)),
        bbox=bbox.tolist(),
        datetime=start_time,
        properties={"ripple: version": "0.0.1"},
    )

    return item


def parse_featuresproperties(json_data, metadata_to_remove):
    """
    Parses and cleans FeaturesProperties data from json. Removes unwanted fields.

    Parameters:
        json_data (dict): Input JSON data containing the metadata to be parsed.
        metadata_to_remove (list): List of metadata fields to be removed from the output.

    Returns:
        dict: Organized FeaturesProperties data.
    """

    # Navigate to FeaturesProperties within the JSON data
    geometry_files = json_data["Files"]["InputFiles"]["GeometryFiles"]["FeaturesProperties"]

    # Initialize a single dictionary to hold all reformatted data
    geom_files_meta = {}
    i = 1

    # Loop through each features property entry
    for file_key, data_properties in geometry_files.items():

        file_ext = data_properties["File Extension"].replace(".", "")
        prefix = f"geometry:{file_ext}"

        # Remove unwanted data
        updated_data_properties = {
            key: data_properties[key] for key in data_properties if key not in metadata_to_remove
        }
        # Remove empty values
        cleaned_data_properties = {key: value for key, value in updated_data_properties.items() if value}

        geom_files_meta[prefix] = cleaned_data_properties
        i += 1

    return geom_files_meta


def parse_control_files(json_data, metadata_to_remove):
    """
    Parses and cleans ControlFiles data from json. Removes unwanted fields.

    Parameters:
        json_data (dict): Input JSON data containing the metadata to be parsed.
        metadata_to_remove (list): List of metadata fields to be removed from the output.

    Returns:
        dict: Organized ControlFiles data.
    """
    # Navigate to ControlFiles within the JSON data
    control_files = json_data["Files"]["InputFiles"]["ControlFiles"]["Data"]

    # Initialize a single dictionary to hold all reformatted data entries
    control_files_meta = {}
    i = 1
    # Loop through each data entry in ControlFiles
    for file_key, data_properties in control_files.items():
        file_ext = data_properties["FileExt"].replace(".", "")
        prefix = f"control:{file_ext}"
        # Remove unwanted data
        updated_data_properties = {
            key: data_properties[key] for key in data_properties if key not in metadata_to_remove
        }
        # Remove empty values
        cleaned_data_properties = {key: value for key, value in updated_data_properties.items() if value}

        control_files_meta[prefix] = cleaned_data_properties
        i += 1

    return control_files_meta


def parse_forcingfiles(json_data, metadata_to_remove):
    """
    Parses and cleans ForcingFiles data from json. Removes unwanted fields.

    Parameters:
        json_data (dict): Input JSON data containing the metadata to be parsed.
        metadata_to_remove (list): List of metadata fields to be removed from the output.

    Returns:
        dict: Organized ForcingFiles data.
    """
    # Navigate to ForcingFiles within the JSON data
    data_files = json_data["Files"]["InputFiles"]["ForcingFiles"]["Data"]

    # Initialize a single dictionary to hold all reformatted data entries
    forcing_files_meta = {}
    i = 1
    # Loop through each data entry in "Data"
    for data_key, data_properties in data_files.items():

        file_ext = data_properties["FileExt"].replace(".", "")
        prefix = f"forcing:{file_ext}"

        # Remove unwanted data
        updated_data_properties = {
            key: data_properties[key] for key in data_properties if key not in metadata_to_remove
        }

        cleaned_data_properties = {key: value for key, value in updated_data_properties.items() if value}

        forcing_files_meta[prefix] = cleaned_data_properties
        i += 1

    return forcing_files_meta


def parse_metadata(json_data, metadata_to_remove):
    """
    Parses and cleans metadata from a JSON object. Combines metadata from GeometryFiles, ControlFiles, and ForcingFiles.

    Parameters:
        json_data (dict): Input JSON data containing the metadata to be parsed.
        metadata_to_remove (list): List of metadata fields to be removed from the output.

    Returns:
        dict: Processed metadata with specified fields removed.
    """

    # Initialize a single dictionary to hold all reformatted data entries
    comprehensive_data = {}

    # Check and parse FeaturesProperties if present
    if "GeometryFiles" in json_data.get("Files", {}).get("InputFiles", {}):
        try:
            geometry_files = json_data["Files"]["InputFiles"]["GeometryFiles"]
            if "FeaturesProperties" in geometry_files:
                features_properties_data = parse_featuresproperties(json_data, metadata_to_remove)
                comprehensive_data.update(features_properties_data)
        except Exception as e:
            logging.error(f"Error processing FeaturesProperties: {e}")

    # Check and parse ControlFiles if present
    if "ControlFiles" in json_data.get("Files", {}).get("InputFiles", {}):
        try:
            control_data = parse_control_files(json_data, metadata_to_remove)
            comprehensive_data.update(control_data)
        except Exception as e:
            logging.error(f"Error processing ControlFiles: {e}")

    # Check and parse ForcingFiles if present
    if "ForcingFiles" in json_data.get("Files", {}).get("InputFiles", {}):
        try:
            forcing_files_data = parse_forcingfiles(json_data, metadata_to_remove)
            comprehensive_data.update(forcing_files_data)
        except Exception as e:
            logging.error(f"Error processing ForcingFiles: {e}")

    return comprehensive_data


def get_asset_info(asset_file: str, bucket: str):
    """This function generates information for an asset based on its file extension.

    Parameters:
        asset_file (str): The S3 path of the asset.

    Returns:
        dict: A dictionary with the roles, the description, and the title of the asset.
    """

    file_extension = Path(asset_file).suffix.lstrip(".")
    title = Path(asset_file).name.replace(" ", "_")
    description = ""
    roles = []
    if re.match("f[0-9]{2}", file_extension):
        roles.extend(["forcing-file", "ras-file", pystac.MediaType.TEXT])
        description = """Forcing file for ras."""

    elif re.match("g[0-9]{2}", file_extension):
        roles.extend(["geometry-file", "ras-file", pystac.MediaType.TEXT])
        description = """Geometry file for ras."""

    elif re.match("p[0-9]{2}", file_extension):
        roles.extend(["plan-file", "ras-file", pystac.MediaType.TEXT])
        description = """Plan file for ras."""

    elif re.match("O[0-9]{2}", file_extension):
        roles.extend(["output-file", "ras-file", pystac.MediaType.TEXT])
        description = """Output file for ras."""

    elif re.match("r[0-9]{2}", file_extension):
        roles.extend(["run-file", "ras-file", pystac.MediaType.TEXT])
        description = """Run file for ras."""

    elif re.match("png", file_extension):
        roles.extend(["thumbnail", pystac.MediaType.PNG])
        description = """PNG of geometry with OpenStreetMap basemap."""
        title = "Thumbnail"

    elif re.match("gpkg", file_extension):
        roles.extend(["ras-geometry-gpkg", pystac.MediaType.GEOPACKAGE])
        description = """GeoPackage file with geometry data extracted from .gxx file."""
        title = "GeoPackage_file"

    elif file_extension == "hdf":
        roles.extend(["hdf-file", pystac.MediaType.HDF])

    elif file_extension == "prj":
        client = get_sessioned_s3_client()
        string = str_from_s3(asset_file.replace(f"s3://{bucket}/", ""), client, bucket)
        if "Proj Title=" in string:
            roles.extend(["project-file", "ras-file", pystac.MediaType.TEXT])
            description = """Project file for ras."""

    return {"roles": roles, "description": description, "title": title}


def find_hash(item_metadata: Dict, asset_file: str):
    """
    Extracts the hash value for a given asset file from a metadata dictionary.

    This function searches through a metadata dictionary for an asset file based on the file's extension.
    It then extracts and returns the hash value associated with that file extension from the metadata.

    Parameters:
        item_metadata (dict): A dictionary containing metadata items with hash info.
        asset_file (str): The path to the asset file.

    Returns:
        hash_dict (Dict): A dictionary with a single key-value pair where the value is the hash for the
        asset file. Returns an empty dictionary if no hash is found for the file extension.
    """

    hash_dict = {}

    file_extension = Path(asset_file).suffix.lstrip(".")
    for key in item_metadata.keys():
        key_file_ext = key.split(":")[-1]
        if file_extension == key_file_ext:
            hash_dict = {"hash": item_metadata[key]["Hash"]}

    return hash_dict


def remove_hash_from_metadata(item_metadata: Dict):
    """
    Removes "Hash" from each key (if it exists) in metedata dictionary.

    Parameters:
        item_metadata (Dict): Dictionary of item metadata with hashs.

    Returns:
        no_hash_metadata (Dict): New metadata dictionary without hash info.
    """

    # Copy the dictionary to avoid modifying the original
    no_hash_metadata = item_metadata.copy()
    for key in no_hash_metadata:
        if "Hash" in no_hash_metadata[key]:
            del no_hash_metadata[key]["Hash"]
    return no_hash_metadata
