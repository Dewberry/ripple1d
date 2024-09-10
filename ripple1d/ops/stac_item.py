"""Tools for creating, manipulating and exporting STAC assets"""

import json
import logging
import os
from pathlib import Path, PurePosixPath
import hashlib
from datetime import datetime, timezone

import pandas as pd
import pystac
import pystac.item
from shapely import to_geojson

import ripple1d
from ripple1d.data_model import RasModelStructure, RippleSourceModel
from ripple1d.utils.dg_utils import bbox_to_polygon
from ripple1d.utils.gpkg_utils import (
    create_thumbnail_from_gpkg,
    get_asset_info,
    get_river_miles,
    gpkg_to_geodataframe,
    reproject
)


def rasmodel_to_stac(rasmodel: RippleSourceModel, ras_s3_prefix: str):
    logging.debug("Creating STAC item from RasModelStructure")
    # Load geopackage
    gdfs = gpkg_to_geodataframe(rasmodel.ras_gpkg_file)
    meta_dict = gdfs['metadata']
    meta_dict = dict(zip(meta_dict['key'], meta_dict['value']))

    # ID
    item_id = rasmodel.model_name.replace(' ', '_')

    # Geometry, bbox, and misc geospatial
    bbox = pd.concat(gdfs).total_bounds
    footprint = bbox_to_polygon(bbox)
    crs = gdfs["River"].crs
    rasmodel.crs = crs
    river_miles = get_river_miles(gdfs["River"])
    gdfs = reproject(gdfs)

    # datetime
    dt = datetime.now(timezone.utc)

    # properties
    properties = {
        "ripple: version": ripple1d.__version__,
        "ras version": meta_dict.get('ras_version', ''),
        "ras_units": meta_dict.get('units', ''),
        "project title": meta_dict.get('ras_project_title', ''),
        "plan titles": meta_dict.get('plans_titles', ''),
        "geom titles": meta_dict.get('geom_titles', ''),
        "flow titles": meta_dict.get('flow_titles', ''),
        "river miles": str(river_miles),
        "proj:wkt2": crs.to_wkt(),
        "proj:epsg": crs.to_epsg(),
    }

    # collection
    collection = None

    # Assets
    assets = dict()
    for key in rasmodel.assets:
        asset_info = get_asset_info(key, rasmodel)

        # If local model is mirror of s3, revert paths
        if ras_s3_prefix:
            key = str(PurePosixPath(Path(key.replace(rasmodel.model_directory, ras_s3_prefix))))
        
        asset = pystac.Asset(
            os.path.relpath(key),
            extra_fields=asset_info["extra_fields"],
            roles=asset_info["roles"],
            description=asset_info["description"],
        )

        assets[key] = asset
        
    # Make pystac item
    stac = pystac.item.Item(
        id=item_id,
        geometry=json.loads(to_geojson(footprint)),
        bbox=bbox.tolist(),
        datetime=dt,
        properties=properties,
        collection=collection,
        assets=assets
    )

    # Make a thumbnail
    fig = create_thumbnail_from_gpkg(gdfs)
    fig.savefig(rasmodel.thumbnail_png)
    # plt.close(fig)

    # Export STAC item
    with open(rasmodel.model_stac_json_file, "w") as dst:
        dst.write(json.dumps(stac.to_dict()))

    logging.debug("Program completed successfully")
