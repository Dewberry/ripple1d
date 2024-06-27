import json
import logging
import os
import shutil

import boto3
import botocore
import numpy as np
from dotenv import find_dotenv, load_dotenv

from .consts import DEFAULT_EPSG
from .process import (
    create_flow_depth_combinations,
    determine_flow_increments,
    get_flow_depth_arrays,
    initialize_new_ras_project_from_gpkg,
    post_process_depth_grids,
    subset_gpkg,
)
from .ras import RasManager
from .sqlite_utils import rating_curves_to_sqlite, zero_depth_to_sqlite
from .utils import (
    derive_input_from_stac_item,
)


def main(
    ras_projects_dir: str,
    ras_gpkg_file_path: str,
    ripple_parameters: dict,
    depth_increment: float,
    number_of_discharges_for_initial_normal_depth_runs: int,
    terrain_files: list,
    default_depths: list,
    version: str = "631",
    postprocessed_output_s3_path: str = None,
):
    """
    Processes 1 RAS-STAC item at a time. Processes all NWM branches identified in the
    conflation parameters asset of the STAC item.
    """

    for nwm_id, nwm_data in ripple_parameters.items():
        print(f"working on initial normal depth runs for nwm_id: {nwm_id}")

        # create sub directory for nwm_id
        ras_project_dir = os.path.join(ras_projects_dir, f"{nwm_id}")
        if os.path.exists(ras_project_dir):
            shutil.rmtree(ras_project_dir)
        if not os.path.exists(ras_project_dir):
            os.makedirs(ras_project_dir)

        # copy terrain to sub directory
        for file in terrain_files:
            file_basename = os.path.basename(file)
            shutil.copy(file, os.path.join(ras_project_dir, file_basename))

            if ".hdf" in file_basename:
                terrain_name = file_basename.rstrip(".hdf")

        # TODO each nwm reach have its own geopackage
        # subset_gpkg by nwm_id
        subset_gpkg_path = subset_gpkg(
            ras_gpkg_file_path,
            ras_project_dir,
            nwm_id,
            nwm_data["ds_xs"]["xs_id"],
            nwm_data["us_xs"]["xs_id"],
            nwm_data["us_xs"]["ras_river"],
            nwm_data["us_xs"]["ras_reach"],
            nwm_data["ds_xs"]["ras_river"],
            nwm_data["ds_xs"]["ras_reach"],
        )

        if not subset_gpkg_path:
            ripple_parameters[nwm_id].update({"skipped": "skipped because not enough cross sections conflated"})
            continue

        if nwm_data["low_flow_cfs"] == -9999 or nwm_data["high_flow_cfs"] == -9999:
            ripple_parameters[nwm_id].update({"skipped": "skipped because flow specified==-9999"})
            continue

        # initialize new ras manager class
        rm, ras_project_text_file = initialize_new_ras_project_from_gpkg(
            ras_project_dir, nwm_id, subset_gpkg_path, version, terrain_name
        )
        ripple_parameters[nwm_id]["ras_project_text_file"] = ras_project_text_file

        # increment flows based on min and max flows specified in conflation parameters
        initial_flows = np.linspace(
            nwm_data["low_flow_cfs"],
            nwm_data["high_flow_cfs"],
            number_of_discharges_for_initial_normal_depth_runs,
        )

        logging.info(f"working on initial normal depth run for nwm_id: {nwm_id}")

        # # write and compute initial normal depth runs to develop rating curves
        rm.normal_depth_run(
            nwm_id + "_ind",
            nwm_id,
            initial_flows,
            nwm_id,
            nwm_id,
            rm.geoms[nwm_id].xs_gdf["river_station"].max(),
            write_depth_grids=False,
        )

        # TODO write results from initial normal depth run to central location
        # process results
        ripple_parameters[nwm_id]["ind_results"] = determine_flow_increments(
            rm,
            nwm_id,
            nwm_id,
            nwm_id,
            rm.geoms[nwm_id].xs_gdf["river_station"].max(),
            nwm_data["us_xs"]["min_elevation"],
        )

        print(f"working on normal depth run for nwm_id: {nwm_id}")

        # write and compute flow/plans for normal_depth runs
        rm.normal_depth_run(
            f"{nwm_id}_nd",
            nwm_id,
            nwm_data["ind_results"]["us_flows"],
            nwm_id,
            nwm_id,
            rm.geoms[nwm_id].xs_gdf["river_station"].max(),
            write_depth_grids=False,
        )

        # get resulting depths from normal depth runs
        flows, depths = get_flow_depth_arrays(
            rm,
            nwm_id,
            nwm_id,
            rm.geoms[nwm_id].xs_gdf["river_station"].min(),
            nwm_data["ds_xs"]["min_elevation"],
        )
        ripple_parameters[nwm_id]["nd_results"] = {
            "ds_flows": flows,
            "ds_depths": depths,
        }

    for nwm_id, nwm_data in ripple_parameters.items():

        if "skipped" in nwm_data.keys():
            continue

        print(f"working on known water surface elevation run for nwm_id: {nwm_id}")

        # determine downstream nwm_id. if not in ripple_parameters then only normal depth run will exist for this reach
        if nwm_data["ds_xs"]["nwm_id"] not in ripple_parameters.keys():
            continue
        ds_data = ripple_parameters[nwm_data["ds_xs"]["nwm_id"]]

        # filter depths less than depths resulting from the normal depth run
        depths, flows, wses = create_flow_depth_combinations(
            ds_data["ind_results"]["us_depths"],
            ds_data["ind_results"]["us_wses"],
            nwm_data["nd_results"]["ds_flows"],
            nwm_data["nd_results"]["ds_depths"],
        )

        # write and compute flow/plans for known water surface elevation runs
        rm = RasManager(nwm_data["ras_project_text_file"], version="631", projection=rm.projection)
        rm.kwses_run(
            f"{nwm_id}_kwse",
            nwm_id,
            depths,
            wses,
            flows,
            nwm_id,
            nwm_id,
            rm.geoms[nwm_id].xs_gdf["river_station"].max(),
            nwm_data["ds_xs"]["xs_id"],
            write_depth_grids=False,
        )

    # # post process the depth grids
    # post_process_depth_grids(rm)

    # # post process the rating curves
    # rating_curves_to_sqlite(rm)
    # zero_depth_to_sqlite(rm)

    # # upload to s3 if an s3 path was provided
    # if postprocessed_output_s3_path:
    #     utils.s3_delete_dir_recursively(s3_dir=r.postprocessed_output_s3_path, s3_resource=s3_resource)
    #     utils.s3_upload_dir_recursively(
    #         local_src_dir=r.postprocessed_output_folder,
    #         tgt_dir=r.postprocessed_output_s3_path,
    #         s3_client=s3_client,
    #     )


if __name__ == "__main__":

    # ras_model_stac_href = "https://stac2.dewberryanalytics.com/collections/huc-12040101/items/STEWARTS%20CREEK"
    ras_directory = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test"
    ras_gpkg_file_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter.gpkg"
    # bucket = "fim"
    depth_increment = 0.5
    number_of_discharges_for_rating_curve = 10
    default_depths = list(np.arange(2, 10, 0.5))
    json_path = (
        r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\baxter-ripple-params copy.json"
    )
    with open(json_path) as f:
        ripple_parameters = json.load(f)

    terrain_folder = r"C:\Users\mdeshotel\Downloads\Example_Projects_6_5\Example_Projects\1D Steady Flow Hydraulics\Baxter RAS Mapper\RAS Model\Terrain"
    terrain_files = [os.path.join(terrain_folder, i) for i in os.listdir(terrain_folder)]

    # # load s3 credentials
    # load_dotenv(find_dotenv())

    # session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
    # client = session.client("s3")
    # resource = session.resource("s3")

    # # derive input from stac item
    # terrain_name, ripple_parameters, postprocessed_output_s3_path = derive_input_from_stac_item(
    #     ras_model_stac_href, ras_directory, client, bucket
    # )

    main(
        ras_directory,
        ras_gpkg_file_path,
        ripple_parameters,
        depth_increment,
        number_of_discharges_for_rating_curve,
        terrain_files,
        default_depths,
        postprocessed_output_s3_path=None,
    )
