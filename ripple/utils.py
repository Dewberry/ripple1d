from __future__ import annotations

import boto3
import botocore
from datetime import datetime, timezone
import json
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon, LineString
from shapely.validation import make_valid
import pandas as pd
import plotly.graph_objects as go
import os
import pathlib
import posixpath
import rasterio
import shutil
from urllib.parse import urlparse
import traceback


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
        "6.10": r"C:\Program Files (x86)\HEC\HEC-RAS\6.1\RasProcess.exe",
        "631": r"C:\Program Files (x86)\HEC\HEC-RAS\6.3.1\RasProcess.exe",
        "6.3.1": r"C:\Program Files (x86)\HEC\HEC-RAS\6.3.1\RasProcess.exe",
    }
    try:
        return d[ras_ver]
    except KeyError as e:
        raise ValueError(f"Unsupported ras_ver: {ras_ver}. choices: {sorted(d)}") from e


def plot_xs_with_wse_increments(r):
    df = pd.DataFrame(r.geom.cross_sections["station_elevation"].iloc[0])
    fig = go.Figure()

    xs = df.copy()
    xs.loc[len(xs.index)] = [
        xs.loc[len(xs.index) - 1, "station"],
        xs.loc[0, "elevation"],
    ]
    xs.loc[len(xs.index)] = xs.loc[0]

    polygon = Polygon(zip(xs["station"], xs["elevation"]))

    if not polygon.is_valid:
        polygon = make_valid(polygon).geoms[0].geoms[0]

    for wse in r.geom.cross_sections["wses"].iloc[0]:
        line = LineString([[xs["station"].iloc[0], wse], [xs["station"].iloc[-2], wse]])

        new_line = polygon.intersection(line)
        if new_line.length == 0:
            continue
        if new_line.geom_type in ["GeometryCollection", "MultiLineString"]:
            for l in new_line.geoms:
                x, y = l.xy
                fig.add_scatter(x=list(x), y=list(y), marker={"color": "grey", "size": 0.5})
        else:
            x, y = new_line.xy
            fig.add_scatter(x=list(x), y=list(y), marker={"color": "grey", "size": 0.5})

    fig.add_scatter(x=df["station"], y=df["elevation"], line={"color": "red"})
    fig.update_layout({"showlegend": False})

    return fig


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
            print(f"Uploading: {src_file} -> {tgt_file}")
            bucket_name, key = extract_bucketname_and_keyname(s3path=tgt_file)
            s3_client.upload_file(
                Filename=src_file,
                Bucket=bucket_name,
                Key=key,
            )


def s3_delete_dir_recursively(s3_dir: str, s3_resource: boto3.resources.factory.ServiceResource) -> None:
    """Delete a s3:// directory and its contents recursively. OK if dir does not exist."""
    print(f"Deleting directory if exists: {s3_dir}")
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

    print(f"Deleting if exists: {s3_output_key_succeed}")
    s3_client.delete_object(Bucket=s3_bucket, Key=s3_output_key_succeed)
    print(f"Deleting if exists: {s3_output_key_fail}")
    s3_client.delete_object(Bucket=s3_bucket, Key=s3_output_key_fail)

    body_str = json.dumps(body, indent=2)
    print(f"Writing: {s3_output_key} with body: {body_str}")
    s3_client.put_object(Body=body_str, Bucket=s3_bucket, Key=s3_output_key, ContentType="application/json")


def s3_ripple_status_succeed_file_exists(stac_href: str, s3_bucket: str, s3_client: botocore.client.BaseClient) -> bool:
    """Check if the standard ripple succeed sentinel file exists.  If it does, return True, otherwise return False."""
    s3_output_key_succeed, _ = s3_get_ripple_status_file_key_names(stac_href, s3_bucket, s3_client)
    print(f"Checking if s3 file exists: s3://{s3_bucket}/{s3_output_key_succeed}")
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
