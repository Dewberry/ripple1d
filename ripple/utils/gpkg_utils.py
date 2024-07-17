"""Utils for working with geopackages."""

import json
import logging
import os
import re
import subprocess
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
from pyproj import CRS
from shapely import Polygon, to_geojson

from ripple.consts import LAYER_COLORS
from ripple.data_model import NwmReachModel
from ripple.errors import UnkownCRSUnitsError
from ripple.ras import RasManager
from ripple.utils.s3_utils import init_s3_resources, str_from_s3


def get_river_miles(river_gdf: gpd.GeoDataFrame):
    """Compute the total length of the river centerlines in miles."""
    if "units" not in river_gdf.crs.to_dict().keys():
        raise UnkownCRSUnitsError("No units specified. The coordinate system may be Geographic.")
    units = river_gdf.crs.to_dict()["units"]
    if units in ["ft-us", "ft", "us-ft"]:
        conversion_factor = 1 / 5280
    elif units in ["m", "meters"]:
        conversion_factor = 1 / 1609
    else:
        raise UnkownCRSUnitsError(f"Expected feet or meters; got: {units}")
    return round(river_gdf.length.sum() * conversion_factor, 2)


def gpkg_to_geodataframe(gpkg_s3_uri: str) -> dict:
    """
    Convert a local geopackage file to a GeoDataFrame.

    Parameters
    ----------
        gpkg_key (str): Path of locally saved geopackage.

    Returns
    -------
        gpkg_gdf (dict): dictionary of GeoDataFrame.

    """
    layers = fiona.listlayers(gpkg_s3_uri)
    gdfs = {}

    for layer in layers:
        gdfs[layer] = gpd.read_file(gpkg_s3_uri, layer=layer)

    return gdfs


def reproject(gdfs: dict, crs=4326) -> dict:
    """Reproject a gdf to a new CRS."""
    for layer, gdf in gdfs.items():
        gdfs[layer] = gdf.to_crs(crs)  # reproject to WSG 84
    return gdfs


def create_thumbnail_from_gpkg(gdfs: dict) -> plt.Figure:
    """
    Create a figure displaying the geopandas dataframe provided in the gdfs dictionary.

    Parameters
    ----------
    - gdf (dict): A dictionary of geopandas dataframes containing the geometries to plot.
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
    return fig


def write_thumbnail_to_s3(fig: plt.Figure, png_s3_key: str, bucket: str, s3_client: boto3.Session.client):
    """
    Write a PNG thumbnail to AWS S3.

    Parameters
    ----------
    - fig (plt.Figure): The figure to export to png on s3.
        - png_s3_key (str): The S3 path where the generated PNG thumbnail is to be stored.
    - bucket (str): The S3 bucket
    - s3_client: The AWS S3 client instance used for uploading the PNG.
    """
    # Save plot to a bytes buffer
    buf = BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close()

    # Download the PNG to s3
    s3_client.put_object(Bucket=bucket, Key=png_s3_key, Body=buf, ContentType="image/png")


def create_geom_item(
    item_id: str,
    bbox: Polygon,
    footprint: list[float],
    properties: dict,
) -> pystac.Item:
    """
    Create a PySTAC Item for a gpkg file stored in an AWS S3 bucket.

    Parameters
    ----------
        item_id (str): item_id.
        bbox (shapely.Polygon): Item bounding box.
        footprint (list[float]): Item Footprint.

    Returns
    -------
        pystac.Item: The PySTAC Item representing the gpkg file.
    """
    # TODO: Adjust start_time selection for item
    start_time = datetime.utcnow()
    item = pystac.Item(
        id=item_id,
        geometry=json.loads(to_geojson(footprint)),
        bbox=bbox.tolist(),
        datetime=start_time,
        properties=properties,
    )

    return item


def parse_featuresproperties(json_data, metadata_to_remove) -> dict:
    """
    Parse and clean FeaturesProperties data from json. Removes unwanted fields.

    Parameters
    ----------
        json_data (dict): Input JSON data containing the metadata to be parsed.
        metadata_to_remove (list): List of metadata fields to be removed from the output.

    Returns
    -------
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


def parse_control_files(json_data, metadata_to_remove) -> dict:
    """
    Parse and clean ControlFiles data from json. Removes unwanted fields.

    Parameters
    ----------
        json_data (dict): Input JSON data containing the metadata to be parsed.
        metadata_to_remove (list): List of metadata fields to be removed from the output.

    Returns
    -------
        dict: Organized ControlFiles data.
    """
    # Navigate to ControlFiles within the JSON data
    control_files = json_data["Files"]["InputFiles"]["ControlFiles"]["Data"]

    # Initialize a single dictionary to hold all reformatted data entries
    control_files_meta = {}
    i = 1
    # Loop through each data entry in ControlFiles
    for _, data_properties in control_files.items():
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


def parse_forcingfiles(json_data, metadata_to_remove) -> dict:
    """
    Pars and clean ForcingFiles data from json. Removes unwanted fields.

    Parameters
    ----------
        json_data (dict): Input JSON data containing the metadata to be parsed.
        metadata_to_remove (list): List of metadata fields to be removed from the output.

    Returns
    -------
        dict: Organized ForcingFiles data.
    """
    # Navigate to ForcingFiles within the JSON data
    data_files = json_data["Files"]["InputFiles"]["ForcingFiles"]["Data"]

    # Initialize a single dictionary to hold all reformatted data entries
    forcing_files_meta = {}
    i = 1
    # Loop through each data entry in "Data"
    for _, data_properties in data_files.items():
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


def parse_metadata(json_data, metadata_to_remove) -> dict:
    """
    Pars and clean metadata from a JSON object. Combines metadata from GeometryFiles, ControlFiles, and ForcingFiles.

    Parameters
    ----------
        json_data (dict): Input JSON data containing the metadata to be parsed.
        metadata_to_remove (list): List of metadata fields to be removed from the output.

    Returns
    -------
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


def get_asset_info(asset_key: str, ras_model_directory: str, bucket: str = None) -> dict:
    """
    Generate information for an asset based on its file extension.

    Parameters
    ----------
        asset_key (str): The S3 key of the asset.
        ras_model_directory (str): The directory where the ras model is stored.
        bucket (str): The S3 bucket where the asset is stored.

    Returns
    -------
        dict: A dictionary with the roles, the description, and the title of the asset.
    """
    nwm_rm = NwmReachModel(ras_model_directory)
    rm = RasManager(nwm_rm.ras_project_file, crs=nwm_rm.crs)
    file_extension = Path(asset_key).suffix
    title = Path(asset_key).name.replace(" ", "_")
    description = ""
    roles = []
    extra_fields = {"file:size": os.path.getsize(asset_key), "last_modified": os.path.getmtime(asset_key)}
    if re.match(".f[0-9]{2}", file_extension):
        roles.extend(["forcing", "hec-ras", pystac.MediaType.TEXT])
        description = """Forcing file for ras."""
        flow = [i for i in rm.flows.values() if i.file_extension == file_extension][0]
        extra_fields["Title"] = flow.title
        extra_fields["Number of Profiles"] = flow.n_profiles
        extra_fields["Profile Names"] = flow.profile_names

    elif re.match(".g[0-9]{2}", file_extension):
        roles.extend(["geometry", "hec-ras", pystac.MediaType.TEXT])
        description = """Geometry file for ras."""
        geom = [i for i in rm.geoms.values() if i.file_extension == file_extension][0]
        extra_fields["Title"] = geom.title
        extra_fields["Number of rivers"] = geom.n_rivers
        extra_fields["Number of reaches"] = geom.n_reaches
        extra_fields["Number of cross sections"] = geom.n_cross_sections
        extra_fields["Number of junctions"] = geom.n_junctions

    elif re.match(".p[0-9]{2}", file_extension):
        roles.extend(["plan", "hec-ras", pystac.MediaType.TEXT])
        description = """Plan file for ras."""
        plan = [i for i in rm.plans.values() if i.file_extension == file_extension][0]
        extra_fields["Title"] = plan.title
        extra_fields["Geometry Title"] = plan.geom.title
        extra_fields["Geometry Extension"] = plan.geom.file_extension
        extra_fields["Flow Title"] = plan.flow.title
        extra_fields["Flow Extension"] = plan.flow.file_extension

    elif re.match(".O[0-9]{2}", file_extension):
        roles.extend(["output", "hec-ras", pystac.MediaType.TEXT])
        description = """Output file for ras."""

    elif re.match(".r[0-9]{2}", file_extension):
        roles.extend(["run", "hec-ras", pystac.MediaType.TEXT])
        description = """Run file for ras."""

    elif re.match(".rasmap", file_extension):
        roles.extend(["rasmap", "hec-ras", pystac.MediaType.XML])
        description = """Rasmapper file for ras."""

    elif ".rasmap.backup" in title:
        roles.extend(["rasmap-backup", "hec-ras", pystac.MediaType.XML])
        description = """Rasmapper backup file for ras."""

    elif ".computeMsgs.txt" in title:
        roles.extend(["compute-messages", "hec-ras", pystac.MediaType.TEXT])
        description = """Compute messages file for ras."""

    elif file_extension == ".hdf":
        if asset_key in nwm_rm.terrain_assets:
            roles.extend(["Terrain", pystac.MediaType.HDF])
        else:
            roles.extend(["hec-ras", pystac.MediaType.HDF])

    elif file_extension == ".vrt":
        if asset_key in nwm_rm.terrain_assets:
            roles.extend(["Terrain", pystac.MediaType.XML])

    elif file_extension == ".tif":
        if asset_key in nwm_rm.terrain_assets:
            roles.extend(["Terrain", pystac.MediaType.GEOTIFF])

    elif re.match(".png", file_extension):
        roles.extend(["thumbnail", pystac.MediaType.PNG])
        description = """PNG of geometry with OpenStreetMap basemap."""
        title = "Thumbnail"

    elif re.match(".gpkg", file_extension):
        roles.extend(["ras-geometry-gpkg", pystac.MediaType.GEOPACKAGE])
        description = """GeoPackage file with geometry data extracted from .gxx file."""
        title = title

    elif ".ripple.json" in title:
        roles.extend(["ripple-parameters", pystac.MediaType.JSON])
        description = """Json file containing Ripple parameters."""
        title = "Ripple parameters"

    elif file_extension == ".prj":
        if bucket:
            _, client, _ = init_s3_resources()
            string = str_from_s3(asset_key, client, bucket)
        else:
            with open(asset_key, "r") as file:
                string = file.read()
        if "Proj Title=" in string:
            roles.extend(["project-file", "hec-ras", pystac.MediaType.TEXT])
            description = """Project file for ras."""
        else:
            if asset_key in nwm_rm.terrain_assets:
                roles.extend(["Terrain", "projection-file", pystac.MediaType.TEXT])
            else:
                roles.extend(["projection-file", pystac.MediaType.TEXT])
            description = """Projection file for ras."""

    return {"roles": roles, "description": description, "title": title, "extra_fields": extra_fields}


def find_hash(item_metadata: Dict, asset_file: str) -> dict:
    """
    Extract the hash value for a given asset file from a metadata dictionary.

    This function searches through a metadata dictionary for an asset file based on the file's extension.
    It then extracts and returns the hash value associated with that file extension from the metadata.

    Parameters
    ----------
        item_metadata (dict): A dictionary containing metadata items with hash info.
        asset_file (str): The path to the asset file.

    Returns
    -------
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


def remove_hash_from_metadata(item_metadata: Dict) -> dict:
    """
    Remove "Hash" from each key (if it exists) in metedata dictionary.

    Parameters
    ----------
        item_metadata (Dict): Dictionary of item metadata with hashs.

    Returns
    -------
        no_hash_metadata (Dict): New metadata dictionary without hash info.
    """
    # Copy the dictionary to avoid modifying the original
    no_hash_metadata = item_metadata.copy()
    for key in no_hash_metadata:
        if "Hash" in no_hash_metadata[key]:
            del no_hash_metadata[key]["Hash"]
    return no_hash_metadata
