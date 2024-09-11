"""Tools for creating, manipulating and exporting STAC assets."""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import pandas as pd
import pystac
import pystac.item
from shapely import to_geojson

import ripple1d
from ripple1d.data_model import RasModelStructure, RippleSourceModel
from ripple1d.ras import RasManager
from ripple1d.ras_utils import get_asset_info
from ripple1d.utils.dg_utils import bbox_to_polygon
from ripple1d.utils.gpkg_utils import create_thumbnail_from_gpkg, get_river_miles, gpkg_to_geodataframe, reproject
from ripple1d.utils.ripple_utils import get_last_model_update, xs_concave_hull


def rasmodel_to_stac(rasmodel: RippleSourceModel):
    """Create a stac item."""
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
    fig = create_thumbnail_from_gpkg(gdfs)
    fig.savefig(rasmodel.thumbnail_png)

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
