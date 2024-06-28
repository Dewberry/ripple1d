import json
import logging
import os

import geopandas as gpd
import rasterio
from rasterio.enums import Resampling
from rasterio.shutil import copy as copy_raster

from ripple.errors import DepthGridNotFoundError
from ripple.ras import RasManager
from ripple.utils.dg_utils import post_process_depth_grids
from ripple.utils.sqlite_utils import rating_curves_to_sqlite, zero_depth_to_sqlite


def new_fim_lib(
    nwm_id: str,
    nwm_data: dict,
    ras_project_text_file: str,
    terrain_path: str,
    subset_gpkg_path: str,
):
    """Create a new FIM library for a NWM id."""
    crs = gpd.read_file(subset_gpkg_path).crs

    rm = RasManager(ras_project_text_file, version="631", terrain_path=terrain_path, crs=crs)
    missing_grids_kwse, missing_grids_nd = post_process_depth_grids(rm, nwm_id, nwm_data, except_missing_grid=True)

    rating_curves_to_sqlite(rm, nwm_id, missing_grids_kwse)
    zero_depth_to_sqlite(rm, nwm_id, missing_grids_nd)


def post_process_depth_grids(
    rm: RasManager,
    nwm_id: str,
    nwm_data: dict,
    except_missing_grid: bool = False,
    dest_directory=None,
) -> tuple[list[str]]:
    """Clip depth grids based on their associated NWM branch and respective cross sections."""
    missing_grids_kwse, missing_grids_nd = [], []
    for prefix in ["_kwse", "_nd"]:
        id = nwm_id + prefix

        if id not in rm.plans:
            continue
        for profile_name in rm.plans[id].flow.profile_names:
            # construct the default path to the depth grid for this plan/profile
            src_path = os.path.join(rm.ras_project._ras_dir, str(id), f"Depth ({profile_name}).vrt")

            # if the depth grid path does not exists print a warning then continue to the next profile
            if not os.path.exists(src_path):
                if prefix == "_kwse":
                    missing_grids_kwse.append(profile_name)
                elif prefix == "_nd":
                    missing_grids_nd.append(profile_name)
                if except_missing_grid:
                    logging.warning(f"depth raster does not exists: {src_path}")
                    continue
                else:
                    raise DepthGridNotFoundError(f"depth raster does not exists: {src_path}")

            if "_kwse" in id:
                flow, depth = profile_name.split("-")
            elif "_nd" in id:
                flow = f"f_{profile_name}"
                depth = "z_0_0"

            dest_directory = os.path.join(rm.ras_project._ras_dir, "output", nwm_id, depth)
            os.makedirs(dest_directory, exist_ok=True)
            dest_path = os.path.join(dest_directory, f"{flow}.tif")

            copy_raster(src_path, dest_path)

            # logging.debug(f"Building overviews for: {dest_path}")
            with rasterio.Env(COMPRESS_OVERVIEW="DEFLATE", PREDICTOR_OVERVIEW="3"):
                with rasterio.open(dest_path, "r+") as dst:
                    dst.build_overviews([4, 8, 16], Resampling.nearest)
                    dst.update_tags(ns="rio_overview", resampling="nearest")

    return missing_grids_kwse, missing_grids_nd


# if __name__ == "__main__":
#     nwm_id = "2826228"
#     ras_project_text_file = (
#         rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}\{nwm_id}.prj"
#     )
#     subset_gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}.gpkg"
#     terrain_path = (
#         rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}Terrain.hdf"
#     )
#     json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\baxter-ripple-params.json"

#     with open(json_path) as f:
#         ripple_parameters = json.load(f)

#     main(
#         nwm_id,
#         ripple_parameters[nwm_id],
#         ras_project_text_file,
#         terrain_path,
#         subset_gpkg_path,
#     )
