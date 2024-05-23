import os

import boto3
import botocore
import numpy as np
import utils
from dotenv import find_dotenv, load_dotenv
from nwm_reaches import increment_rc_flows
from process import (
    determine_flow_increments,
    post_process_depth_grids,
    read_ras,
    run_production_runs,
    run_rating_curves,
)
from sqlite_utils import rating_curves_to_sqlite
from utils import (
    derive_input_from_stac_item,
)


def main(
    stac_href: str,
    ras_directory: str,
    bucket: str,
    s3_resource: boto3.resources.factory.ServiceResource,
    s3_client: botocore.client.BaseClient,
    depth_increment: float,
    number_of_discharges_for_rating_curve: int = 10,
):
    """
    Processes 1 RAS-STAC item at a time. Processes all NWM branches identified in the
    conflation parameters asset of the STAC item.
    """

    # read stac item, download ras model, load nwm conflation parameters
    r = read_ras_nwm(stac_href, ras_directory, bucket, s3_client)

    # increment flows based on min and max flows specified in conflation parameters
    r.nwm_dict = increment_rc_flows(r.nwm_dict, number_of_discharges_for_rating_curve)

    # write and compute initial flows/plans to develop rating curves
    run_rating_curves(r)

    # determine flow increments from rating curves derived from initial runs
    r = determine_flow_increments(r, depth_increment)

    # write and compute flow/plans for production runs
    r = run_production_runs(r)

    # post process the depth grids
    post_process_depth_grids(r)

    # post process the rating curves
    rating_curves_to_sqlite(r)

    utils.s3_delete_dir_recursively(s3_dir=r.postprocessed_output_s3_path, s3_resource=s3_resource)
    utils.s3_upload_dir_recursively(
        local_src_dir=r.postprocessed_output_folder,
        tgt_dir=r.postprocessed_output_s3_path,
        s3_client=s3_client,
    )


if __name__ == "__main__":
    # skip_stac_hrefs = ["https://stac.dewberryanalytics.com/collections/huc-12040101/items/WFSJ_Main-cd42"]
    skip_stac_hrefs = []

    collection_id = "huc-12040101"
    bucket = "fim"
    depth_increment = 0.5
    number_of_discharges_for_rating_curve = 10

    load_dotenv(find_dotenv())

    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
    )
    s3_resource = session.resource("s3")
    s3_client = session.client("s3")
    stac_client = pystac_client.Client.open(STAC_API_URL)

    collection = stac_client.get_collection(collection_id)
    items = sorted(collection.get_all_items(), key=lambda x: x.id)

    hrefs_skipped = []
    hrefs_failed = []
    hrefs_succeeded = []

    for i, item in enumerate(items):
        pct_s = "{:.0%}".format((i + 1) / len(items))
        print(f"{pct_s} ({i+1} / {len(items)}) {item.id}")

        hrefs = [link.target for link in item.links if link.rel == "self"]
        if len(hrefs) != 1:
            raise ValueError(f"Expected 1 STAC href, but got {len(hrefs)} for item ID {item.id}: {hrefs}")
        ras_model_stac_href = hrefs[0]

        if ras_model_stac_href in skip_stac_hrefs:
            print(f"SKIPPING HREF SINCE IN skip_stac_hrefs: {ras_model_stac_href}")
            hrefs_skipped.append(f"{ras_model_stac_href}: REASON: in skip_stac_hrefs")
            continue
        if utils.s3_ripple_status_succeed_file_exists(ras_model_stac_href, bucket, s3_client):
            print(f"SKIPPING HREF SINCE SUCCEED FILE EXISTS: {ras_model_stac_href}")
            hrefs_skipped.append(f"{ras_model_stac_href}: REASON: ripple succeed file exists")
            continue

        url_parsed = urlparse(ras_model_stac_href)
        tmp_dir_suffix = f"ras-{url_parsed.path.replace('/', '-').replace(':', '-')}"
        try:
            # ras_directory = os.path.join(os.getcwd(), tmp_dir_suffix)
            # if True:
            with tempfile.TemporaryDirectory(suffix=tmp_dir_suffix) as ras_directory:

                ras_directory = os.path.realpath(ras_directory)

                print(f"Processing {repr(ras_model_stac_href)}, writing to folder {repr(ras_directory)}")
                main(
                    ras_model_stac_href,
                    ras_directory,
                    bucket,
                    s3_resource,
                    s3_client,
                    depth_increment,
                    number_of_discharges_for_rating_curve,
                )

        except Exception as e:
            utils.s3_upload_status_file(ras_model_stac_href, bucket, s3_client, e)
            print(f"HREF FAILED {ras_model_stac_href}")
            hrefs_failed.append(f"{ras_model_stac_href}: ERROR: {e}")
        else:
            utils.s3_upload_status_file(ras_model_stac_href, bucket, s3_client, None)
            print(f"HREF SUCCEEDED {ras_model_stac_href}")
            hrefs_succeeded.append(ras_model_stac_href)

    print(
        f"\n\nvvv {len(hrefs_skipped)} TOTAL HREFS SKIPPED: vvv\n{'\n'.join(hrefs_skipped)}\n^^^ {len(hrefs_skipped)} TOTAL HREFS SKIPPED ^^^"
    )
    print(
        f"\n\nvvv {len(hrefs_succeeded)} TOTAL HREFS SUCCEEDED: vvv\n{'\n'.join(hrefs_succeeded)}\n^^^ {len(hrefs_succeeded)} TOTAL HREFS SUCCEEDED ^^^"
    )
    print(
        f"\n\nvvv {len(hrefs_failed)} TOTAL HREFS FAILED: vvv\n{'\n'.join(hrefs_failed)}\n^^^ {len(hrefs_failed)} TOTAL HREFS FAILED ^^^"
    )
