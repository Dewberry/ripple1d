"""Utils for working with raster data."""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pystac
import rasterio
import rasterio.warp
import shapely
import shapely.ops
from mypy_boto3_s3.service_resource import Object
from pyproj import CRS
from rasterio import mask
from rasterio.session import AWSSession
from rasterio.warp import Resampling, calculate_default_transform, reproject
from shapely import Polygon

from .s3_utils import *

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


def get_raster_bounds(
    s3_key: str, aws_session: AWSSession, dev_mode: bool = False
) -> Tuple[float, float, float, float]:
    """
    Retrieve the geographic bounds of a raster file stored in an AWS S3 bucket and returns them in the WGS 84 (EPSG:4326) coordinate reference system.

    Parameters
    ----------
        s3_key (str): The key of the raster file in the S3 bucket.
        aws_session (AWSSession): The AWS session to use to access the S3 bucket.

    Returns
    -------
        Tuple[float, float, float, float]: The geographic bounds of the raster file in the WGS 84 (EPSG:4326) coordinate reference system. The bounds are returned as a tuple of four floats: (west, south, east, north).
    """
    if dev_mode:
        with rasterio.open(s3_key.replace("s3://", f"/vsicurl/{os.environ.get('MINIO_S3_ENDPOINT')}/")) as src:
            bounds = src.bounds
            crs = src.crs
            bounds_4326 = rasterio.warp.transform_bounds(crs, "EPSG:4326", *bounds)
            return bounds_4326

    else:
        with rasterio.Env(aws_session):
            with rasterio.open(s3_key) as src:
                bounds = src.bounds
                crs = src.crs
                bounds_4326 = rasterio.warp.transform_bounds(crs, "EPSG:4326", *bounds)
                return bounds_4326


def get_raster_metadata(s3_key: str, aws_session: AWSSession, dev_mode: bool = False) -> dict:
    """
    Retrieve the metadata of a raster file stored in an AWS S3 bucket.

    Parameters
    ----------
        s3_key (str): The key of the raster file in the S3 bucket.
        aws_session (AWSSession): The AWS session to use to access the S3 bucket.

    Returns
    -------
        dict: The metadata of the raster file. The metadata is returned as a dictionary
        where the keys are the names of the metadata items and the values are the values of the metadata items.
    """
    if dev_mode:
        with rasterio.open(s3_key.replace("s3://", f"/vsicurl/{os.environ.get('MINIO_S3_ENDPOINT')}/")) as src:
            return src.tags(1)
    else:
        with rasterio.Env(aws_session):
            with rasterio.open(s3_key) as src:
                return src.tags(1)


def bbox_to_polygon(bbox) -> shapely.Polygon:
    """
    Convert a bounding box to a Shapely Polygon.

    Parameters
    ----------
        bbox: The bounding box to convert. It should be a sequence of four numbers: (min_x, min_y, max_x, max_y).

    Returns
    -------
        shapely.Polygon: The Shapely Polygon representing the bounding box. The Polygon is a rectangle with the lower
          left corner at (min_x, min_y) and the upper right corner at (max_x, max_y).
    """
    min_x, min_y, max_x, max_y = bbox
    return shapely.Polygon(
        [
            [min_x, min_y],
            [min_x, max_y],
            [max_x, max_y],
            [max_x, min_y],
        ]
    )


def create_depth_grid_item(
    s3_obj: Object, item_id: str, aws_session: AWSSession, dev_mode: bool = False
) -> pystac.Item:
    """
    Create a PySTAC Item for a depth grid raster file stored in an AWS S3 bucket.

    Parameters
    ----------
        s3_obj (Object): The s3 object of the raster file in the S3 bucket.
        item_id (str): The ID to assign to the PySTAC Item.
        aws_session (AWSSession): The AWS session to use to access the S3 bucket.

    Returns
    -------
        pystac.Item: The PySTAC Item representing the raster file. The Item has an Asset with the href set to the S3
        key of the raster file, the title set to the name of the raster file, the media type set to COG, and the role
        set to "ras-depth-grid". The Asset's extra fields are updated with the basic object metadata of the raster file
        and the metadata of the raster file. The Item's bbox is set to the geographic bounds of the raster file in the
        WGS 84 (EPSG:4326) coordinate reference system, the datetime is set to the current datetime, and the geometry
        is set to the GeoJSON representation of the bbox.
    """
    s3_full_key = f"s3://{s3_obj.bucket_name}/{s3_obj.key}"
    title = Path(s3_obj.key).name
    bbox = get_raster_bounds(s3_full_key, aws_session, dev_mode=dev_mode)
    geometry = bbox_to_polygon(bbox)
    item = pystac.Item(
        id=item_id,
        properties={},
        bbox=bbox,
        datetime=datetime.now(),
        geometry=json.loads(shapely.to_geojson(geometry)),
    )
    # non_null = not raster_is_all_null(depth_grid.key)
    asset = pystac.Asset(
        href=s3_key_public_url_converter(s3_full_key, dev_mode=dev_mode),
        title=title,
        media_type=pystac.MediaType.COG,
        roles=["ras-depth-grid"],
    )
    asset.extra_fields.update(get_basic_object_metadata(s3_obj))
    asset.extra_fields = dict(sorted(asset.extra_fields.items()))
    metadata = get_raster_metadata(s3_full_key, aws_session, dev_mode=dev_mode)
    if metadata:
        asset.extra_fields.update(metadata)
    item.add_asset(key=asset.title, asset=asset)
    return item


def reproject_raster(src_path: str, dest_path: str, dst_crs: CRS, resolution: float = None):
    """Reproject/resample raster."""
    with rasterio.open(src_path) as src:
        if not resolution:
            resolution = src.res[0]
            transform, width, height = calculate_default_transform(src.crs, dst_crs, src.width, src.height, *src.bounds)
        else:
            transform, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds, resolution=resolution
            )
        kwargs = src.meta.copy()
        kwargs.update({"crs": dst_crs, "transform": transform, "width": width, "height": height})

        with rasterio.open(dest_path, "w", **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    dst_resolution=resolution,
                    resampling=Resampling.nearest,
                )


def clip_raster(src_path: str, dst_path: str, mask_polygon: Polygon):
    """Clip a raster file to a polygon and save the result to a new file."""
    if os.path.exists(dst_path):
        raise FileExistsError(dst_path)
    if not isinstance(mask_polygon, Polygon):
        raise TypeError(mask_polygon)
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    logging.info(f"Reading: {src_path}")
    with rasterio.open(src_path) as src:
        out_meta = src.meta
        out_image, out_transform = mask.mask(src, [mask_polygon], all_touched=True, crop=True)

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
