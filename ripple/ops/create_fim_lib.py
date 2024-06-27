import json

import geopandas as gpd

from ripple.process import post_process_depth_grids
from ripple.ras import RasManager
from ripple.sqlite_utils import rating_curves_to_sqlite, zero_depth_to_sqlite


def new_fim_lib(
    nwm_id: str,
    nwm_data: dict,
    ras_project_text_file: str,
    terrain_path: str,
    subset_gpkg_path: str,
):

    crs = gpd.read_file(subset_gpkg_path).crs

    rm = RasManager(ras_project_text_file, version="631", terrain_path=terrain_path, crs=crs)
    missing_grids_kwse, missing_grids_nd = post_process_depth_grids(rm, nwm_id, nwm_data, except_missing_grid=True)

    rating_curves_to_sqlite(rm, nwm_id, nwm_data, missing_grids_kwse)
    zero_depth_to_sqlite(rm, nwm_id, missing_grids_nd)


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
