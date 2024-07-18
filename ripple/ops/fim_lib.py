"""Create FIM library."""

import json
import logging
import os
from pathlib import Path

import geopandas as gpd
import pystac
import rasterio
from rasterio.enums import Resampling
from rasterio.shutil import copy as copy_raster

from ripple.conflate.rasfim import RasFimConflater
from ripple.consts import RIPPLE_VERSION
from ripple.data_model import NwmReachModel
from ripple.errors import DepthGridNotFoundError
from ripple.ras import RasManager
from ripple.ras_to_gpkg import geom_flow_to_gdfs, new_stac_item
from ripple.utils.s3_utils import init_s3_resources
from ripple.utils.sqlite_utils import create_db_and_table, rating_curves_to_sqlite, zero_depth_to_sqlite


def post_process_depth_grids(
    rm: RasManager, plan_names: str, dest_directory: str, except_missing_grid: bool = False
) -> tuple[list[str]]:
    """Clip depth grids based on their associated NWM branch and respective cross sections."""
    missing_grids_kwse, missing_grids_nd = [], []
    for plan_name in plan_names:
        if plan_name not in rm.plans:
            logging.info(f"Plan {plan_name} not found in the model, skipping...")
            continue
        for profile_name in rm.plans[plan_name].flow.profile_names:
            # construct the default path to the depth grid for this plan/profile
            src_path = os.path.join(rm.ras_project._ras_dir, str(plan_name), f"Depth ({profile_name}).vrt")

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
                flow, depth = profile_name.split("-")
            elif "_nd" in plan_name:
                flow = f"f_{profile_name}"
                depth = "z_0_0"

            flow_sub_directory = os.path.join(dest_directory, depth)
            os.makedirs(flow_sub_directory, exist_ok=True)
            dest_path = os.path.join(flow_sub_directory, f"{flow}.tif")

            copy_raster(src_path, dest_path)

            logging.debug(f"Building overviews for: {dest_path}")
            with rasterio.Env(COMPRESS_OVERVIEW="DEFLATE", PREDICTOR_OVERVIEW="3"):
                with rasterio.open(dest_path, "r+") as dst:
                    dst.build_overviews([4, 8, 16], Resampling.nearest)
                    dst.update_tags(ns="rio_overview", resampling="nearest")

    return missing_grids_kwse, missing_grids_nd


def create_fim_lib(
    submodel_directory: str,
    plans: list,
    ras_version: str = "631",
    table_name: str = "rating_curves",
):
    """Create a new FIM library for a NWM id."""
    nwm_rm = NwmReachModel(submodel_directory)
    if not nwm_rm.file_exists(nwm_rm.ras_gpkg_file):
        raise FileNotFoundError(f"cannot find ras_gpkg_file file {nwm_rm.ras_gpkg_file}, please ensure file exists")

    crs = gpd.read_file(nwm_rm.ras_gpkg_file, layer="XS").crs

    rm = RasManager(nwm_rm.ras_project_file, version=ras_version, terrain_path=nwm_rm.ras_terrain_hdf, crs=crs)
    ras_plans = [f"{nwm_rm.model_name}_{plan}" for plan in plans]

    missing_grids_kwse, missing_grids_nd = post_process_depth_grids(
        rm, ras_plans, nwm_rm.fim_results_directory, except_missing_grid=True
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
    ripple_version: str = RIPPLE_VERSION,
):
    """Convert a FIM RAS model to a STAC item."""
    nwm_rm = NwmReachModel(ras_project_directory)

    # create new stac item
    new_stac_item(
        ras_project_directory,
        ripple_version,
        ras_model_s3_prefix,
    )

    # upload to s3
    if bucket and ras_model_s3_prefix:
        nwm_rm.upload_files_to_s3(ras_model_s3_prefix, bucket)

        stac_item = pystac.read_file(nwm_rm.stac_json_file)
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
        s3_client.put_object(
            Body=json.dumps(stac_item.to_dict()).encode(),
            Bucket=bucket,
            Key=f"{ras_model_s3_prefix}/{Path(nwm_rm.stac_json_file).name}",
        )
