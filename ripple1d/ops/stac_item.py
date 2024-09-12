"""Tools for creating, manipulating and exporting STAC assets."""

import glob
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import pandas as pd
import pystac
import pystac.item
from shapely import to_geojson

import ripple1d
from ripple1d.data_model import RasModelStructure, RippleSourceModel
from ripple1d.ras import RasManager
from ripple1d.ras_to_gpkg import gpkg_from_ras
from ripple1d.stacio.stac_ras_roles import get_asset_info
from ripple1d.utils.dg_utils import bbox_to_polygon
from ripple1d.utils.gpkg_utils import create_thumbnail_from_gpkg, get_river_miles, gpkg_to_geodataframe, reproject
from ripple1d.utils.ripple_utils import get_last_model_update, prj_is_ras, xs_concave_hull
from ripple1d.utils.s3_utils import get_basic_object_metadata, init_s3_resources, list_keys


def rasmodel_to_stac(rasmodel: RippleSourceModel, save_json: bool = False):
    """Create a stac item from a local RAS model."""
    logging.debug("Creating STAC item from RasModelStructure")

    # Instantiate RasManager
    rasmanager = RasManager(rasmodel.ras_project_file, crs=rasmodel.crs)

    # Load geopackage
    gdfs = gpkg_to_geodataframe(rasmodel.ras_gpkg_file)
    meta_dict = gdfs["metadata"]
    meta_dict = dict(zip(meta_dict["key"], meta_dict["value"]))

    # ID
    item_id = rasmodel.model_name.replace(" ", "_")

    # Geometry, bbox, and misc geospatial
    og_crs = gdfs["River"].crs
    river_miles = get_river_miles(gdfs["River"])
    gdfs = reproject(gdfs)
    bbox = pd.concat(gdfs).total_bounds
    footprint = xs_concave_hull(gdfs["XS"])

    # datetime
    ras_data = gdfs["River"]["ras_data"].iloc[0].split("\n")
    dt = get_last_model_update(ras_data)
    if dt is None:
        dt = datetime.now()
        dt_valid = False
    else:
        dt_valid = True

    # properties
    properties = {
        "ripple: version": ripple1d.__version__,
        "ras version": meta_dict.get("ras_version", ""),
        "ras_units": meta_dict.get("units", ""),
        "project title": meta_dict.get("ras_project_title", ""),
        "plans": {key: val.file_extension for key, val in rasmanager.plans.items()},
        "geometries": {key: val.file_extension for key, val in rasmanager.geoms.items()},
        "flows": {key: val.file_extension for key, val in rasmanager.flows.items()},
        "river miles": str(river_miles),
        "dt_valid": dt_valid,
        "proj:wkt2": og_crs.to_wkt(),
        "proj:epsg": og_crs.to_epsg(),
    }

    # collection
    collection = None

    # Make a thumbnail
    create_thumbnail_from_gpkg(gdfs, rasmodel.thumbnail_png)

    # Assets
    assets = make_stac_assets(rasmodel.assets)

    # Make pystac item
    stac = pystac.item.Item(
        id=item_id,
        geometry=json.loads(footprint.to_json()),
        bbox=bbox.tolist(),
        datetime=dt,
        properties=properties,
        collection=collection,
        assets=assets,
        stac_extensions=["Projection", "Storage"],
    )

    if save_json:
        # Export STAC item
        with open(rasmodel.model_stac_json_file, "w") as dst:
            dst.write(json.dumps(stac.to_dict()))

    logging.debug("Program completed successfully")
    return stac


def make_stac_assets(asset_list: list, bucket: str = None):
    """Convert a list of paths to stac assets with associated metadata."""
    assets = dict()
    for key in asset_list:
        asset_info = get_asset_info(key, bucket)
        title = asset_info["title"].replace(" ", "_")
        if bucket is not None:
            href = f"s3://{bucket}/{key}"
        else:
            href = os.path.relpath(key)
        asset = pystac.Asset(
            href=href,
            title=title,
            extra_fields=asset_info["extra_fields"],
            roles=asset_info["roles"],
            description=asset_info["description"],
        )
        assets[title] = asset
    return assets

def s3_ras_to_stac(bucket: str, key: str, crs: str) -> dict:
    """Process s3 key, and on error, return null dict."""
    try:
        return process_key(bucket, key, crs)
    except Exception:
        return {
            "stac_item": None,
            "thumbnail": None,
            "gpkg": None,
        }

def process_key(bucket: str, key: str, crs: str) -> dict:
    """Convert RAS model associated with a .prj S3 key to stac."""
    logging.info(f"Processing key: {key}")

    _, s3_client, s3_resource = init_s3_resources()

    # Get assets and Download model
    logging.info(f"Finding assets associated with prefix {key}")
    prefix = "/".join(key.split("/")[:-1]) + "/"
    keys = list_keys(s3_client, bucket, prefix)
    assets = get_assets(s3_resource, bucket, keys)

    with tempfile.TemporaryDirectory() as tmp_dir:
        logging.debug(f"Temp folder created at {tmp_dir}")

        download_model(s3_client, bucket, assets, tmp_dir)

        # Make a geopackage
        logging.info(f"Making geopackage for {key}")
        gpkg_from_ras(tmp_dir, crs, {})

        # Find ras .prj file, make instance of RippleSourceModel, and convert to stac
        prjs = glob.glob(f"{tmp_dir}/*.prj")
        prjs = [prj for prj in prjs if prj_is_ras(prj)]
        if len(prjs) != 1:
            raise KeyError(f"Expected 1 RAS file, found {len(prjs)}: {prjs}")
        else:
            ras_prj_path = prjs[0]
        rm = RippleSourceModel(ras_prj_path, crs)
        logging.info(f"Making stac item for {key}")
        stac = rasmodel_to_stac(rm, save_json=False)

        # Upload png and gpkg to s3
        assets["Thumbnail"] = upload_file(
            bucket,
            s3_client,
            s3_resource,
            os.path.basename(rm.thumbnail_png),
            rm.thumbnail_png,
            "stac",
            prefix,
            public=True,
        )
        assets["GeoPackage_file"] = upload_file(
            bucket,
            s3_client,
            s3_resource,
            os.path.basename(rm.ras_gpkg_file),
            rm.ras_gpkg_file,
            "gpkgs",
            prefix,
            public=False,
        )

        # Overwrite some asset data with S3 metadata
        for s3_asset in assets:
            title = s3_asset.split("/")[-1].replace(" ", "_")
            meta = assets[s3_asset]
            if not title in stac.assets:
                # Make a new asset
                stac.assets[title] = make_stac_assets([s3_asset], bucket=bucket)[title]
            else:
                # replace basic object metadata
                stac.assets[title].href = assets[s3_asset]['href']
                for k, v in meta.items():
                    stac.assets[title].extra_fields[k] = v

        # Export
        assets["stac_item"] = upload_file(
            bucket,
            s3_client,
            s3_resource,
            os.path.basename(rm.model_stac_json_file),
            stac.to_dict(),
            "stac",
            prefix,
            public=True,
        )

    return {
        "stac_item": {"href": assets["stac_item"]["href"]},
        "thumbnail": {"href": assets["Thumbnail"]["href"]},
        "gpkg": {"href": assets["GeoPackage_file"]["href"]},
    }


def download_model(s3_client, bucket: str, keys: list, tmp_dir: str) -> None:
    """Download all ras files in the directory of .prj file."""
    logging.debug(f'Downloading files: {keys}')
    RELEVANT_FILE_FINDER = re.compile(r'\.[Pp][Rr][Jj]|\.[Gg]\d{2}|\.[Pp]\d{2}|\.[FfUuQq]\d{2}')
    for key in keys:
        if RELEVANT_FILE_FINDER.fullmatch(Path(key).suffix):
            s3_client.download_file(bucket, key, os.path.join(tmp_dir, Path(key).name))


def upload_file(bucket, s3_client, s3_resource, basename: str, file: str | dict, type: str, prefix: str, public: bool) -> dict:
    """Upload file to correct spot on S3."""
    out_key = f'ebfedata-derived/{type}/v{ripple1d.__version__}-rc/{prefix.replace('ebfedata/', '')}{basename}'
    if isinstance(file, str):
        s3_client.upload_file(Bucket=bucket, Key=out_key, Filename=file)
    elif isinstance(file, dict):
        s3_client.put_object(Bucket=bucket, Key=out_key, Body=json.dumps(file).encode())

    obj = s3_resource.Bucket(bucket).Object(out_key)
    meta = get_basic_object_metadata(obj)

    if public:
        return {'href': f'https://{bucket}.amazonaws.com/{out_key}', 'meta': meta}
    else:
        return{'href': f's3://{bucket}/{out_key}', 'meta': meta}


def get_assets(s3_resource, bucket, keys)-> dict:
    """Get metadata for a list of S3 keys."""
    asset_dict = {}
    for key in keys:
        obj = s3_resource.Bucket(bucket).Object(key)
        asset_dict[key] = {
            'href': f's3://{bucket}/{key}',
            'meta': get_basic_object_metadata(obj)
            }
    return asset_dict

