import json

import geopandas as gpd

from .process import (
    create_flow_depth_combinations,
    get_flow_depth_arrays,
    get_kwse_from_ds_model,
)
from .ras2 import RasManager


def main(
    nwm_id: str,
    nwm_data: dict,
    ras_project_text_file: str,
    subset_gpkg_path: str,
    terrain_name: str,
    known_water_surface_elevations: list,
):

    print(f"working on known water surface elevation run for nwm_id: {nwm_id}")

    projection = gpd.read_file(subset_gpkg_path).crs

    # write and compute flow/plans for known water surface elevation runs
    rm = RasManager(ras_project_text_file, version="631", terrain_name=terrain_name, projection=projection)

    # get resulting depths from the second normal depth runs_nd
    rm.plan = rm.plans[nwm_id + "_nd"]
    ds_flows, ds_depths, _ = get_flow_depth_arrays(
        rm, nwm_id, nwm_id, rm.geoms[nwm_id].xs_gdf["river_station"].min(), nwm_data["ds_xs"]["min_elevation"]
    )

    known_depths = known_water_surface_elevations - float(nwm_data["ds_xs"]["min_elevation"])

    # filter known water surface elevations less than depths resulting from the second normal depth run
    depths, flows, wses = create_flow_depth_combinations(
        known_depths,
        known_water_surface_elevations,
        ds_flows,
        ds_depths,
    )

    rm.kwses_run(
        f"{nwm_id}_kwse1",
        nwm_id,
        depths,
        wses,
        flows,
        nwm_id,
        nwm_id,
        rm.geoms[nwm_id].xs_gdf["river_station"].max(),
        rm.geoms[nwm_id].xs_gdf["river_station"].min(),
        write_depth_grids=True,
    )


if __name__ == "__main__":
    nwm_id = "2826228"
    ras_project_text_file = (
        rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}\{nwm_id}.prj"
    )
    subset_gpkg_path = rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\{nwm_id}.gpkg"
    terrain_name = "Terrain"
    json_path = (
        r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\baxter-ripple-params copy.json"
    )

    ds_nwm_id = "2823932"
    ds_nwm_ras_project_file = (
        r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test\2823932\2823932.prj"
    )

    known_water_surface_elevations = get_kwse_from_ds_model(
        ds_nwm_id,
        ds_nwm_ras_project_file,
    )

    with open(json_path) as f:
        ripple_parameters = json.load(f)

    main(
        nwm_id,
        ripple_parameters[nwm_id],
        ras_project_text_file,
        subset_gpkg_path,
        terrain_name,
        known_water_surface_elevations,
    )
