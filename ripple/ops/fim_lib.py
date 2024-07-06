"""Create FIM library."""

import json
import logging
import os
from pathlib import Path

import geopandas as gpd
import rasterio
from rasterio.enums import Resampling
from rasterio.shutil import copy as copy_raster

from ripple.errors import DepthGridNotFoundError
from ripple.ras import RasManager
from ripple.utils.sqlite_utils import rating_curves_to_sqlite, zero_depth_to_sqlite


def post_process_depth_grids(
    rm: RasManager,
    plan_names: str,
    nwm_id: str,
    except_missing_grid: bool = False,
    dest_directory=None,
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

            dest_directory = os.path.join(rm.ras_project._ras_dir, "output", nwm_id, depth)
            os.makedirs(dest_directory, exist_ok=True)
            dest_path = os.path.join(dest_directory, f"{flow}.tif")

            copy_raster(src_path, dest_path)

            logging.debug(f"Building overviews for: {dest_path}")
            with rasterio.Env(COMPRESS_OVERVIEW="DEFLATE", PREDICTOR_OVERVIEW="3"):
                with rasterio.open(dest_path, "r+") as dst:
                    dst.build_overviews([4, 8, 16], Resampling.nearest)
                    dst.update_tags(ns="rio_overview", resampling="nearest")

    return missing_grids_kwse, missing_grids_nd


def create_fim_lib(submodel_directory: str, plans: list, fim_output_dir: str = None, ras_version: str = "631"):
    """Create a new FIM library for a NWM id."""
    model_name = Path(submodel_directory).name
    source_model = f"{submodel_directory}/{model_name}.prj"
    terrain_path = f"{submodel_directory}\\Terrain\\{model_name}.hdf"
    conflation_parameters_path = source_model.replace(".prj", ".ripple.json")
    ras_gpkg_file_path = source_model.replace(".prj", ".gpkg")
    if not os.path.exists(ras_gpkg_file_path):
        raise FileNotFoundError(f"cannot find file ras-geometry file {ras_gpkg_file_path}, please ensure file exists")

    with open(conflation_parameters_path, "r") as f:
        conflation_parameters = json.loads(f.read())
    crs = gpd.read_file(ras_gpkg_file_path, layer="XS").crs

    rm = RasManager(source_model, version=ras_version, terrain_path=terrain_path, crs=crs)
    ras_plans = [f"{model_name}_{plan}" for plan in plans]
    missing_grids_kwse, missing_grids_nd = post_process_depth_grids(rm, ras_plans, model_name, except_missing_grid=True)

    if f"kwse" in plans:
        rating_curves_to_sqlite(rm, f"{model_name}_kwse", model_name, missing_grids_kwse)
    if f"nd" in plans:
        zero_depth_to_sqlite(rm, f"{model_name}_nd", model_name, missing_grids_nd)
