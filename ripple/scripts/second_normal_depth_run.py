import json

import geopandas as gpd

from ripple.process import (
    determine_flow_increments,
)
from ripple.ras import RasManager


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
    conflation_json_path = (
        r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\baxter-ripple-params.json"
    )

    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    for nwm_id in conflation_parameters.keys():
        ras_project_text_file = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\nwm_models\{nwm_id}\{nwm_id}.prj"
        subset_gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\nwm_models\{nwm_id}\{nwm_id}.gpkg"
        terrain_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\nwm_models\{nwm_id}\Terrain.hdf"

        main(nwm_id, conflation_parameters[nwm_id], ras_project_text_file, subset_gpkg_path, terrain_path)
