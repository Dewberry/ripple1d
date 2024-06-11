import json

import geopandas as gpd

from .process import (
    determine_flow_increments,
)
from .ras2 import RasManager


def main(
    nwm_id: str,
    nwm_data: dict,
    ras_project_text_file: str,
    subset_gpkg_path: str,
    terrain_path: str,
    version: str = "631",
):
    print(f"working on normal depth run for nwm_id: {nwm_id}")

    projection = gpd.read_file(subset_gpkg_path).crs

    rm = RasManager(ras_project_text_file, version=version, terrain_path=terrain_path, projection=projection)

    # determine flow increments
    flows, depths, wses = determine_flow_increments(
        rm,
        nwm_id,
        nwm_id,
        nwm_id,
        rm.geoms[nwm_id].xs_gdf["river_station"].max(),
        nwm_data["us_xs"]["min_elevation"],
    )

    # write and compute flow/plans for normal_depth runs
    rm.normal_depth_run(
        f"{nwm_id}_nd",
        nwm_id,
        flows,
        nwm_id,
        nwm_id,
        rm.geoms[nwm_id].xs_gdf["river_station"].max(),
        write_depth_grids=True,
    )


if __name__ == "__main__":
    nwm_id = "2823932"
    ras_project_text_file = (
        rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}\{nwm_id}.prj"
    )
    subset_gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}.gpkg"
    terrain_path = (
        rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}\Terrain.hdf"
    )
    json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\baxter-ripple-params.json"

    with open(json_path) as f:
        ripple_parameters = json.load(f)

    main(nwm_id, ripple_parameters[nwm_id], ras_project_text_file, subset_gpkg_path, terrain_path)
