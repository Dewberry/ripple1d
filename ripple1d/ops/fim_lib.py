"""Create FIM library."""

import json
import logging
import os
from datetime import datetime
from pathlib import Path, PurePosixPath

import geopandas as gpd
import pystac
import rasterio
import shapely
from pyproj import CRS

# from osgeo import gdal
from rasterio.enums import Resampling
from rasterio.shutil import copy as copy_raster

import ripple1d
from ripple1d.conflate.rasfim import RasFimConflater
from ripple1d.data_model import NwmReachModel
from ripple1d.errors import DepthGridNotFoundError
from ripple1d.ras import RasManager
from ripple1d.ras_to_gpkg import geom_flow_to_gdfs, new_stac_item
from ripple1d.utils.dg_utils import bbox_to_polygon, get_raster_bounds, get_raster_metadata, reproject_raster
from ripple1d.utils.s3_utils import get_basic_object_metadata, init_s3_resources
from ripple1d.utils.sqlite_utils import create_db_and_table, rating_curves_to_sqlite, zero_depth_to_sqlite


def post_process_depth_grids(
    rm: RasManager,
    plan_names: str,
    dest_directory: str,
    except_missing_grid: bool = False,
    tiled=False,
    overviews=False,
    dest_crs: CRS = 5070,
    resolution: float = 3,
    resolution_units: str = "Meters",
) -> tuple[list[str]]:
    """Clip depth grids based on their associated NWM branch and respective cross sections."""
    if resolution and not resolution_units:
        raise ValueError(
            f"The 'resolution' arg has been provided but 'resolution_units' arg has not been provided. Please provide both"
        )

    if resolution_units:
        if resolution_units not in ["Feet", "Meters"]:
            raise ValueError(f"Invalid resolution_units: {resolution_units}. expected 'Feet' or 'Meters'")

    missing_grids_kwse, missing_grids_nd = [], []
    for plan_name in plan_names:
        if plan_name not in rm.plans:
            logging.info(f"Plan {plan_name} not found in the model, skipping...")
            continue
        for profile_name in rm.plans[plan_name].flow.profile_names:
            # construct the default path to the depth grid for this plan/profile
            src_path = os.path.join(
                rm.ras_project._ras_dir,
                str(plan_name),
                f"Depth ({profile_name}).{rm.ras_project.title}.USGS_Seamless_DEM_13.tif",
            )

            # if the depth grid path does not exists print a warning then continue to the next profile
            if not os.path.exists(src_path):
                if "_kwse" in plan_name:
                    missing_grids_kwse.append(profile_name)
                elif "_nd" in plan_name:
                    missing_grids_nd.append(profile_name)
                if except_missing_grid:
                    logging.warning(f"depth raster does not exists: {src_path}")
                    continue
                else:
                    raise DepthGridNotFoundError(f"depth raster does not exists: {src_path}")

            if "_kwse" in plan_name:
                flow, depth = profile_name.split("-", 1)
            elif "_nd" in plan_name:
                flow = f"f_{profile_name}"
                depth = "z_nd"

            flow_sub_directory = os.path.join(dest_directory, depth)
            os.makedirs(flow_sub_directory, exist_ok=True)
            dest_path = os.path.join(flow_sub_directory, f"{flow}.tif")

            if tiled:
                tiled = "yes"
            else:
                tiled = "no"
            # copy_raster(
            #     src_path,
            #     dest_path,
            #     COMPRESS="DEFLATE",
            #     PREDICTOR="3",
            #     num_threads=4,
            #     tiled=tiled,
            #     blockxsize=512,
            #     blockysize=512,
            # )
            reproject_raster(src_path, dest_path, CRS(dest_crs), resolution, resolution_units)

            # TODO currently the compression for the overviews does not seem to be working.
            # need to figure out why this is not working.
            logging.debug(f"Building overviews for: {dest_path}")
            # with rasterio.Env(COMPRESS_OVERVIEW="DEFLATE", PREDICTOR_OVERVIEW="3"):
            if overviews:
                with rasterio.open(dest_path, "r+") as dst:
                    dst.build_overviews([4, 8, 16], Resampling.nearest)
                    dst.update_tags(ns="rio_overview", resampling="nearest")

            # gdal.UseExceptions()
            # # open the file
            # ds = gdal.Open(dest_path, gdal.GA_Update)  # GA_Update == 1, aka Write mode
            # if ds is None:
            #     raise RuntimeError(f"Could not open: {file_path}")
            # ds.BuildOverviews(
            #     "NEAREST", [4, 8, 16], options={"COMPRESS_OVERVIEW": "DEFLATE", "PREDICTOR_OVERVIEW": "3"}
            # )
            # # close the file
            # del ds

    return missing_grids_kwse, missing_grids_nd


def create_fim_lib(
    submodel_directory: str,
    plans: list,
    ras_version: str = "631",
    table_name: str = "rating_curves",
    tiled=False,
    overviews=False,
    resolution: float = 3,
    resolution_units: str = "Meters",
):
    """Create a new FIM library for a NWM id."""
    nwm_rm = NwmReachModel(submodel_directory)
    if not nwm_rm.file_exists(nwm_rm.ras_gpkg_file):
        raise FileNotFoundError(f"cannot find ras_gpkg_file file {nwm_rm.ras_gpkg_file}, please ensure file exists")

    crs = gpd.read_file(nwm_rm.ras_gpkg_file, layer="XS").crs

    rm = RasManager(nwm_rm.ras_project_file, version=ras_version, terrain_path=nwm_rm.ras_terrain_hdf, crs=crs)
    ras_plans = [f"{nwm_rm.model_name}_{plan}" for plan in plans]

    missing_grids_kwse, missing_grids_nd = post_process_depth_grids(
        rm,
        ras_plans,
        nwm_rm.fim_results_directory,
        except_missing_grid=True,
        tiled=tiled,
        overviews=overviews,
        resolution=resolution,
        resolution_units=resolution_units,
    )

    # create dabase and table
    create_db_and_table(nwm_rm.fim_results_database, table_name)

    for plan in plans:
        if f"kwse" in plan:
            rating_curves_to_sqlite(
                rm,
                f"{nwm_rm.model_name}_kwse",
                nwm_rm.model_name,
                missing_grids_kwse,
                nwm_rm.fim_results_database,
                table_name,
            )
        if f"nd" in plan:
            zero_depth_to_sqlite(
                rm,
                f"{nwm_rm.model_name}_nd",
                nwm_rm.model_name,
                missing_grids_nd,
                nwm_rm.fim_results_database,
                table_name,
            )

    return {"fim_results_directory": nwm_rm.fim_results_directory, "fim_results_database": nwm_rm.fim_results_database}


def nwm_reach_model_stac(
    ras_project_directory: str,
    ras_model_s3_prefix: str = None,
    bucket: str = None,
):
    """Convert a FIM RAS model to a STAC item."""
    nwm_rm = NwmReachModel(ras_project_directory)

    # create new stac item
    new_stac_item(
        ras_project_directory,
        ras_model_s3_prefix,
    )

    # upload to s3
    if bucket and ras_model_s3_prefix:
        nwm_rm.upload_files_to_s3(ras_model_s3_prefix, bucket)

        stac_item = pystac.read_file(nwm_rm.model_stac_json_file)
        # update asset hrefs
        for id, asset in stac_item.assets.items():
            if "thumbnail" in asset.roles:
                asset.href = f"https://{bucket}.s3.amazonaws.com/{ras_model_s3_prefix}/{Path(asset.href).name}"
            else:
                asset.href = f"s3://{bucket}/{ras_model_s3_prefix}/{Path(asset.href).name}"

        stac_item.set_self_href(
            f"https://{bucket}.s3.amazonaws.com/{ras_model_s3_prefix}/{Path(stac_item.self_href).name}"
        )
        # write updated stac item to s3
        _, s3_client, _ = init_s3_resources()
        key = f"{ras_model_s3_prefix}/{Path(nwm_rm.model_stac_json_file).name}"
        s3_client.put_object(
            Body=json.dumps(stac_item.to_dict()).encode(),
            Bucket=bucket,
            Key=key,
        )
        nwm_rm.update_write_ripple1d_parameters({"model_stac_item": f"https://{bucket}.s3.amazonaws.com/{key}"})
    else:
        nwm_rm.update_write_ripple1d_parameters({"model_stac_item": nwm_rm.model_stac_json_file})


def update_stac_s3_location(stac_item_file: pystac.Item, bucket: str, s3_prefix: str):
    """Update the href locations for a stac item and its assets."""
    stac_item = pystac.read_file(stac_item_file)
    _, s3_client, s3_resource = init_s3_resources()
    s3_bucket = s3_resource.Bucket(bucket)

    # update asset hrefs
    for asset in stac_item.assets.values():
        parts = list(Path(asset.href).parts[-2:])
        file_name = "-".join(parts)
        s3_key = f"{s3_prefix}/{file_name}"

        s3_uri = f"s3://{bucket}/{s3_key}"
        if "thumbnail" in asset.roles:
            asset.href = f"https://{bucket}.s3.amazonaws.com/{s3_key}"
        else:
            asset.href = s3_uri

        s3_obj = s3_bucket.Object(s3_key)
        metadata = get_basic_object_metadata(s3_obj)
        asset.extra_fields.update(metadata)

    stac_item.set_self_href(f"https://{bucket}.s3.amazonaws.com/{s3_prefix}/{Path(stac_item.self_href).name}")
    # write updated stac item to s3
    s3_client.put_object(
        Body=json.dumps(stac_item.to_dict()).encode(),
        Bucket=bucket,
        Key=f"{s3_prefix}/{Path(stac_item_file).name}",
    )


def fim_lib_item(item_id: str, assets: list, stac_json: str, metadata: dict) -> pystac.Item:
    """
    Create a PySTAC Item for a fim_lib_item.

    Parameters
    ----------
        item_id (str): The ID to assign to the PySTAC Item.
        assets (str): assets (files) to add to the stac item
        stac_json (str): json file to write the stac item to

    Returns
    -------
        pystac.Item: The PySTAC Item representing the raster file. The Item has an Asset with the raster file,
        the title set to the name of the raster file, the media type set to COG, and the role
        set to "ras-depth-grid". The Asset's extra fields are updated with the basic object metadata of the raster file
        and the metadata of the raster file. The Item's bbox is set to the geographic bounds of the raster file in the
        WGS 84 (EPSG:4326) coordinate reference system, the datetime is set to the current datetime, and the geometry
        is set to the GeoJSON representation of the bbox.
    """
    bbox = get_raster_bounds(assets[0])
    geometry = bbox_to_polygon(bbox)
    item = pystac.Item(
        id=item_id,
        properties=metadata["properties"],
        bbox=bbox,
        datetime=datetime.now(),
        geometry=json.loads(shapely.to_geojson(geometry)),
    )
    # add assets
    for file in assets:
        parts = list(Path(file).parts[-2:])
        title = "-".join(parts).rstrip(".tif")
        asset = pystac.Asset(
            href=file,
            title=title,
            media_type=pystac.MediaType.COG,
            roles=["fim"],
        )

        asset_metadata = get_raster_metadata(file)
        if asset_metadata:
            asset.extra_fields.update(asset_metadata)
        item.add_asset(key=asset.title, asset=asset)
    item.add_derived_from(metadata["derived_from"]["model_stac_item"])

    with open(stac_json, "w") as f:
        f.write(json.dumps(item.to_dict()))
    return item


def fim_lib_stac(ras_project_directory: str, nwm_reach_id: str, s3_prefix: str = None, bucket: str = None):
    """Create a stac item for a fim library."""
    nwm_rm = NwmReachModel(ras_project_directory)

    metadata = {
        "properties": {
            "NOAA_NWM:FIM Reach ID": nwm_reach_id,
            "NOAA_NWM:FIM to Reach ID": nwm_rm.ripple1d_parameters["nwm_to_id"],
            "NOAA_NWM:FIM Depth units": "ft",
            "NOAA_NWM:FIM Flow units": "cfs",
            "NOAA_NWM:FIM Rating Curve (Flow, Depth)": nwm_rm.fim_rating_curve,
            "proj:wkt2": CRS(nwm_rm.crs).to_wkt(),
            "proj:epsg": CRS(nwm_rm.crs).to_epsg(),
            "Ripple Version": ripple1d.__version__,
        },
        "derived_from": {"model_stac_item": nwm_rm.ripple1d_parameters["model_stac_item"]},
    }
    assets = nwm_rm.fim_lib_assets
    fim_lib_item(nwm_reach_id, nwm_rm.fim_lib_assets, nwm_rm.fim_lib_stac_json_file, metadata)

    if s3_prefix and bucket:
        nwm_rm.upload_fim_lib_assets(s3_prefix, bucket)
        update_stac_s3_location(nwm_rm.fim_lib_stac_json_file, bucket, s3_prefix)
