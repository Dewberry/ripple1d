import os

import boto3
import botocore
import numpy as np
import utils
from dotenv import find_dotenv, load_dotenv
from nwm_reaches import increment_rc_flows
from process import (
    determine_flow_increments,
    filter_ds_depths,
    post_process_depth_grids,
    read_ras,
    run_normal_depth_runs,
    run_production_runs,
    run_rating_curves,
)
from sqlite_utils import rating_curves_to_sqlite, zero_depth_to_sqlite
from utils import (
    derive_input_from_stac_item,
)


def main(
    ras_directory: str,
    nwm_dict: dict,
    bucket: str,
    s3_resource: boto3.resources.factory.ServiceResource,
    s3_client: botocore.client.BaseClient,
    depth_increment: float,
    number_of_discharges_for_rating_curve: int,
    terrain_name: str,
    default_depths: list,
    postprocessed_output_s3_path: str = None,
):
    """
    Processes 1 RAS-STAC item at a time. Processes all NWM branches identified in the
    conflation parameters asset of the STAC item.
    """

    # read stac item, download ras model, load nwm conflation parameters
    r = read_ras(ras_directory, nwm_dict, terrain_name, bucket, s3_client, postprocessed_output_s3_path)

    # increment flows based on min and max flows specified in conflation parameters
    r.nwm_dict = increment_rc_flows(r.nwm_dict, number_of_discharges_for_rating_curve)

    # write and compute initial flows/plans to develop rating curves
    run_rating_curves(r)

    # determine flow increments from rating curves derived from initial runs
    r = determine_flow_increments(r, default_depths, depth_increment)

    # write and compute flow/plans for normal_depth runs
    r = run_normal_depth_runs(r)

    # get resulting depths from normal depth runs
    r = filter_ds_depths(r)

    # write and compute flow/plans for production runs
    r = run_production_runs(r)

    # post process the depth grids
    post_process_depth_grids(r)

    # post process the rating curves
    rating_curves_to_sqlite(r)
    zero_depth_to_sqlite(r)

    # upload to s3 if an s3 path was provided
    if postprocessed_output_s3_path:
        utils.s3_delete_dir_recursively(s3_dir=r.postprocessed_output_s3_path, s3_resource=s3_resource)
        utils.s3_upload_dir_recursively(
            local_src_dir=r.postprocessed_output_folder,
            tgt_dir=r.postprocessed_output_s3_path,
            s3_client=s3_client,
        )


if __name__ == "__main__":

    ras_model_stac_href = "https://stac2.dewberryanalytics.com/collections/huc-12040101/items/STEWARTS%20CREEK"
    ras_directory = r"C:\Users\mdeshotel\Downloads\STEWARTS_CREEK"
    bucket = "fim"
    depth_increment = 0.5
    number_of_discharges_for_rating_curve = 10
    default_depths = list(np.arange(2, 10, 0.5))

    # load s3 credentials
    load_dotenv(find_dotenv())

    session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
    client = session.client("s3")
    resource = session.resource("s3")

    # derive input from stac item
    terrain_name, nwm_dict, postprocessed_output_s3_path = derive_input_from_stac_item(
        ras_model_stac_href, ras_directory, client, bucket
    )

    main(
        ras_directory,
        nwm_dict,
        bucket,
        resource,
        client,
        depth_increment,
        number_of_discharges_for_rating_curve,
        terrain_name,
        default_depths,
        postprocessed_output_s3_path=None,
    )
