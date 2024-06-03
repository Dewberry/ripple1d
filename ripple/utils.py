from __future__ import annotations

import json
import logging
import os
import pathlib
import posixpath
import traceback
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3
import botocore
import geopandas as gpd
import numpy as np
import pandas as pd
import pystac
import rasterio
from dotenv import find_dotenv, load_dotenv
from requests.utils import requote_uri
from shapely.geometry import Point, Polygon

from .errors import (
    RASComputeError,
    RASComputeMeshError,
    RASGeometryError,
    RASStoreAllMapsError,
)

load_dotenv(find_dotenv())


def get_sessioned_s3_client():
    """Use env variables to establish a boto3 (AWS) session and return that session's S3 client handle."""
    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
    )
    s3_client = session.client("s3")
    return s3_client


def clip_raster(src_path: str, dst_path: str, mask: Polygon):
    if os.path.exists(dst_path):
        raise FileExistsError(dst_path)
    if not isinstance(mask, Polygon):
        raise TypeError(mask)
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    logging.info(f"Reading: {src_path}")
    with rasterio.open(src_path) as src:
        out_meta = src.meta
        out_image, out_transform = rasterio.mask.mask(src, [mask], all_touched=True, crop=True)

    out_meta.update(
        {
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
            "compress": "LZW",
            "predictor": 3,
            "tiled": True,
        }
    )

    logging.info(f"Writing as masked: {dst_path}")
    with rasterio.open(dst_path, "w", **out_meta) as dest:
        dest.write(out_image)


def decode(df: pd.DataFrame):
    for c in df.columns:
        df[c] = df[c].str.decode("utf-8")
    return df


def create_flow_depth_array(flow: list[float], depth: list[float], increment: float = 0.5):
    min_depth = np.min(depth)
    max_depth = np.max(depth)
    start_depth = np.floor(min_depth * 2) / 2  # round down to nearest .0 or .5
    new_depth = np.arange(start_depth, max_depth + increment, increment)
    new_flow = np.interp(new_depth, np.sort(depth), np.sort(flow))

    return new_flow, new_depth


def get_terrain_exe_path(ras_ver: str) -> str:
    """Return Windows path to RasProcess.exe exposing CreateTerrain subroutine, compatible with provided RAS version."""
    # 5.0.7 version of RasProcess.exe does not expose CreateTerrain subroutine.
    # Testing shows that RAS 5.0.7 accepts Terrain created by 6.1 version of RasProcess.exe, so use that for 5.0.7.
    d = {
        "507": r"C:\Program Files (x86)\HEC\HEC-RAS\6.1\RasProcess.exe",
        "5.07": r"C:\Program Files (x86)\HEC\HEC-RAS\6.1\RasProcess.exe",
        "600": r"C:\Program Files (x86)\HEC\HEC-RAS\6.0\RasProcess.exe",
        "6.00": r"C:\Program Files (x86)\HEC\HEC-RAS\6.0\RasProcess.exe",
        "610": r"C:\Program Files (x86)\HEC\HEC-RAS\6.1\RasProcess.exe",
        "6.10": r"C:\Program Files (x86)\HEC\HEC-RAS\6.1\RasProcess.exe",
        "631": r"C:\Program Files (x86)\HEC\HEC-RAS\6.3.1\RasProcess.exe",
        "6.3.1": r"C:\Program Files (x86)\HEC\HEC-RAS\6.3.1\RasProcess.exe",
    }
    try:
        return d[ras_ver]
    except KeyError as e:
        raise ValueError(f"Unsupported ras_ver: {ras_ver}. choices: {sorted(d)}") from e


def s3_upload_dir_recursively(local_src_dir: str, tgt_dir: str, s3_client: botocore.client.BaseClient):
    """Copies all files from a local directory. tgt_dir can be local or a s3:// prefix"""
    assert tgt_dir.startswith("s3://")
    pathmod = posixpath
    if not os.path.isdir(local_src_dir):
        raise NotADirectoryError(local_src_dir)
    for root, _, files in os.walk(local_src_dir):
        rel_root = os.path.relpath(root, start=local_src_dir)
        if os.path is not posixpath:
            # copying to s3 (posix system), but running in Windows
            rel_root = pathlib.PurePath(rel_root).as_posix()
        if rel_root == ".":
            rel_root = ""
        for fn in files:
            src_file = os.path.join(root, fn)
            tgt_file = pathmod.join(tgt_dir, rel_root, fn)
            logging.debug(f"Uploading: {src_file} -> {tgt_file}")
            bucket_name, key = extract_bucketname_and_keyname(s3path=tgt_file)
            s3_client.upload_file(
                Filename=src_file,
                Bucket=bucket_name,
                Key=key,
            )


def s3_delete_dir_recursively(s3_dir: str, s3_resource: boto3.resources.factory.ServiceResource) -> None:
    """Delete a s3:// directory and its contents recursively. OK if dir does not exist."""
    logging.debug(f"Deleting directory if exists: {s3_dir}")
    if not s3_dir.startswith("s3://"):
        raise ValueError(f"Expected s3_dir to start with s3://, but got: {s3_dir}")
    bucket, key = extract_bucketname_and_keyname(s3path=s3_dir)
    if not key.strip():
        raise ValueError(f"s3 path too short: {s3_dir}")
    if len(key.split("/")) < 3:
        raise ValueError(f"s3 path too short: {s3_dir}")
    if not key.endswith("/"):
        key += "/"
    bucket_handle = s3_resource.Bucket(bucket)
    bucket_handle.objects.filter(Prefix=key).delete()


def extract_bucketname_and_keyname(s3path: str) -> tuple[str, str]:
    """Parse the provided s3:// object path and return its bucket name and key."""
    if not s3path.startswith("s3://"):
        raise ValueError(f"s3path does not start with s3://: {s3path}")
    bucket, _, key = s3path[5:].partition("/")
    return bucket, key


def s3_upload_status_file(stac_href: str, s3_bucket: str, s3_client: botocore.client.BaseClient, e: Exception | None):
    """If e is a Python exception, then upload a 'fail' json file to the href's standard
    output location on s3.  If e is None, then upload a 'succeed' json file.  Either file
    will have key "time" indicating the time that the file was uploaded.  A 'fail' file will
    also have keys "err" and "traceback" containing the exception as a string and the Python
    traceback of the exception, respectively."""

    s3_output_key_succeed, s3_output_key_fail = s3_get_ripple_status_file_key_names(stac_href, s3_bucket, s3_client)

    time_now_str = datetime.now(tz=timezone.utc).isoformat()
    if e is None:
        s3_output_key = s3_output_key_succeed
        body = {"time": time_now_str}
    elif isinstance(e, Exception):
        s3_output_key = s3_output_key_fail
        body = {"time": time_now_str, "err": str(e), "traceback": "".join(traceback.format_tb(e.__traceback__))}
    else:
        raise TypeError(f"For e, expected None or type Exception, but got type: {type(e)}")

    logging.debug(f"Deleting if exists: {s3_output_key_succeed}")
    s3_client.delete_object(Bucket=s3_bucket, Key=s3_output_key_succeed)
    logging.debug(f"Deleting if exists: {s3_output_key_fail}")
    s3_client.delete_object(Bucket=s3_bucket, Key=s3_output_key_fail)

    body_str = json.dumps(body, indent=2)
    logging.debug(f"Writing: {s3_output_key} with body: {body_str}")
    s3_client.put_object(Body=body_str, Bucket=s3_bucket, Key=s3_output_key, ContentType="application/json")


def s3_ripple_status_succeed_file_exists(stac_href: str, s3_bucket: str, s3_client: botocore.client.BaseClient) -> bool:
    """Check if the standard ripple succeed sentinel file exists.  If it does, return True, otherwise return False."""
    s3_output_key_succeed, _ = s3_get_ripple_status_file_key_names(stac_href, s3_bucket, s3_client)
    logging.debug(f"Checking if s3 file exists: s3://{s3_bucket}/{s3_output_key_succeed}")
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=s3_output_key_succeed)
    except botocore.exceptions.ClientError as e:
        if "Not Found" in str(e):
            return False  # typical ClientError when the object does not exist
        else:
            raise  # unexpected ClientError
    return True


def s3_get_ripple_status_file_key_names(
    stac_href: str, s3_bucket: str, s3_client: botocore.client.BaseClient
) -> tuple[str, str]:
    """Return two S3 key paths, the first to a succeed sentinel file, the second t oa failure sentinel file.
    This function does not check if the keys exist."""
    _, s3_output_dir_key = extract_bucketname_and_keyname(s3_get_output_s3path(s3_bucket, stac_href))
    s3_output_key_succeed = posixpath.join(s3_output_dir_key, "ripple-succeed.json")
    s3_output_key_fail = posixpath.join(s3_output_dir_key, "ripple-fail.json")
    return s3_output_key_succeed, s3_output_key_fail


def s3_get_output_s3path(s3_bucket: str, stac_href: str) -> str:
    return f"s3://{s3_bucket}/mip/dev/ripple/output{urlparse(stac_href).path}/"


def xs_concave_hull(xs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Compute and return the concave hull (polygon) for a set of cross sections (lines all facing the same direction)."""

    points = xs.boundary.explode().unstack()
    points_last_xs = [Point(coord) for coord in xs["geometry"].iloc[-1].coords]
    points_first_xs = [Point(coord) for coord in xs["geometry"].iloc[0].coords[::-1]]

    polygon = Polygon(points_first_xs + list(points[0]) + points_last_xs + list(points[1])[::-1])

    return gpd.GeoDataFrame({"geometry": [polygon]}, geometry="geometry", crs=xs.crs)


def derive_input_from_stac_item(
    ras_model_stac_href: str, ras_directory: str, client: boto3.session.Session.client, bucket: str
) -> tuple:

    # read stac item
    stac_item = pystac.Item.from_file(requote_uri(ras_model_stac_href))

    # download RAS model from stac item. derive terrain_name during download.
    # terrain_name is the basename of the terrain hdf without extension.
    terrain_name = download_model(stac_item, ras_directory, client, bucket)

    # get nwm conflation parameters
    nwm_dict = create_nwm_dict_from_stac_item(stac_item, client, bucket)

    # directory for post processed depth grids/sqlite db. The default is None which will not upload to s3
    postprocessed_output_s3_path = s3_get_output_s3path(bucket, ras_model_stac_href)

    return terrain_name, nwm_dict, postprocessed_output_s3_path


def create_nwm_dict_from_stac_item(stac_item: pystac.Item, client: boto3.session.Session.client, bucket: str) -> dict:

    # create nwm dictionary
    for _, asset in stac_item.get_assets(role="ripple-params").items():

        response = client.get_object(Bucket=bucket, Key=asset.href.replace(f"https://{bucket}.s3.amazonaws.com/", ""))
        json_data = response["Body"].read()

    return json.loads(json_data)


def download_model(
    stac_item: pystac.Item, ras_directory: str, client: boto3.session.Session.client, bucket: str
) -> str:
    """
    Download HEC-RAS model from stac href
    """

    # make RAS directory if it does not exists
    if not os.path.exists(ras_directory):
        os.makedirs(ras_directory)

    # download HEC-RAS model files
    for _, asset in stac_item.get_assets(role="ras-file").items():

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


def search_contents(lines: list, search_string: str, token: str = "=", expect_one: bool = True):
    """
    Splits a line by a token and returns the second half of the line
        if the search_string is found in the first half
    """
    results = []
    for line in lines:
        if f"{search_string}{token}" in line:
            results.append(line.split(token)[1])

    if expect_one and len(results) > 1:
        raise ValueError(f"expected 1 result, got {len(results)}")
    elif expect_one and len(results) == 0:
        raise ValueError("expected 1 result, no results found")
    elif expect_one and len(results) == 1:
        return results[0]
    else:
        return results


def replace_line_in_contents(lines: list, search_string: str, replacement: str, token: str = "="):
    """
    Splits a line by a token and replaces the second half of the line
    (for the first occurence only!)
    """
    updated = 0
    for i, line in enumerate(lines):
        if f"{search_string}{token}" in line:
            updated += 1
            lines[i] = line.split(token)[0] + token + replacement
    if updated == 0:
        raise ValueError(f"search_string: {search_string} not found in lines")
    elif updated > 1:
        raise ValueError(f"expected 1 result, got {updated} occurences of {replacement}")
    else:
        return lines


def text_block_from_start_end_str(start_str: str, end_str: str, lines: list):
    """
    Search for an exact match to the start_str and return
    all lines from there to a line that contains the end_str.
    """

    results = []
    in_block = False
    for line in lines:

        if line == start_str:
            in_block = True
            results.append(line)
            continue

        if in_block:
            if end_str in line:
                return results
            else:
                results.append(line)
    return results


def text_block_from_start_str_to_empty_line(start_str: str, lines: list):
    """
    Search for an exact match to the start_str and return
    all lines from there to the next empty line.
    """

    results = []
    in_block = False
    for line in lines:

        if line == start_str:
            in_block = True
            results.append(line)
            continue

        if in_block:
            if line == "":
                results.append(line)
                return results
            else:
                results.append(line)
    return results


def text_block_from_start_str_length(start_str: str, number_of_lines: int, lines: list):
    """
    Search for an exact match to the start token and return
    a number of lines equal to number_of_lines

    start_token:
    """

    results = []
    in_block = False
    for line in lines:
        if line == start_str:
            in_block = True
            continue

        if in_block:
            if len(results) >= number_of_lines:
                return results
            else:
                results.append(line)


def data_pairs_from_text_block(lines: list[str], width: int):
    """
    Split lines at given width to get paired data string.
    Split the string in half and convert to tuple of floats.
    """
    pairs = []
    for line in lines:
        for i in range(0, len(line), width):

            x = line[i : int(i + width / 2)]
            y = line[int(i + width / 2) : int(i + width)]
            pairs.append((float(x), float(y)))

    return pairs


def assert_no_mesh_error(compute_message_file: str, require_exists: bool):
    try:
        with open(compute_message_file) as f:
            content = f.read()
    except FileNotFoundError:
        if require_exists:
            raise
    else:
        for line in content.splitlines():
            if "error generating mesh" in line.lower():
                raise RASComputeMeshError(
                    f"'error generating mesh' found in {compute_message_file}. Full file content:\n{content}\n^^^ERROR^^^"
                )


def assert_no_ras_geometry_error(compute_message_file: str):
    """Scan *.computeMsgs.txt for errors encountered"""
    with open(compute_message_file) as f:
        content = f.read()
    for line in content.splitlines():
        if "geometry writer failed" in line.lower() or "error processing geometry" in line.lower():
            raise RASGeometryError(
                f"geometry error found in {compute_message_file}. Full file content:\n{content}\n^^^ERROR^^^"
            )


def assert_no_ras_compute_error_message(compute_message_file: str):
    """Scan *.computeMsgs.txt for errors encountered"""
    with open(compute_message_file) as f:
        content = f.read()
    for line in content.splitlines():
        if "ERROR:" in line:
            raise RASComputeError(
                f"'ERROR:' found in {compute_message_file}. Full file content:\n{content}\n^^^ERROR^^^"
            )


def assert_no_store_all_maps_error_message(compute_message_file: str):
    """Scan *.computeMsgs.txt for errors encountered"""
    with open(compute_message_file) as f:
        content = f.read()
    for line in content.splitlines():
        if "error executing: storeallmaps" in line.lower():
            raise RASStoreAllMapsError(
                f"{repr(line)} found in {compute_message_file}. Full file content:\n{content}\n^^^ERROR^^^"
            )
