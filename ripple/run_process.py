from ras import Ras
from utils import create_flow_depth_array
import boto3
import botocore
import os
import pprint
import sys
from urllib.parse import urlparse
from dotenv import load_dotenv, find_dotenv
from nwm_reaches import increment_rc_flows
import pystac_client
from ras import RasMap, Ras
from consts import TERRAIN_NAME, STAC_API_URL, MINDEPTH
import tempfile
import utils
from nwm_reaches import clip_depth_grid
import warnings
from sqlite_utils import rating_curves_to_sqlite
from errors import DepthGridNotFoundError

# load s3 credentials
load_dotenv(find_dotenv())

dewberrywrc_acct_session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
client = dewberrywrc_acct_session.client("s3")


def read_ras_nwm(stac_href: str, ras_directory: str, bucket: str, client):

    # read ras model and create cross section gdf
    r = Ras(ras_directory, stac_href, client, bucket, default_epsg=2277)
    # r.model_downloaded = True
    r.download_model()
    r.read_ras()
    r.plan.geom.scan_for_xs()
    r.create_nwm_dict()

    return r


def run_rating_curves(r: Ras):

    for branch_id, branch_data in r.nwm_dict.items():

        id = branch_id + "_rc"

        # write the new flow file
        r.write_new_flow_rating_curves(id, branch_data, normal_depth=0.001)

        # write the new plan file
        r.write_new_plan(r.geom, r.flows[id], id, id)

        # update the content of the RAS project file
        r.update_content()
        r.set_current_plan(r.plans[str(id)])

        # write the update RAS project file content back to disk
        r.write()

        # run the RAS plan
        r.RunSIM(close_ras=True, show_ras=True, ignore_store_all_maps_error=True)
    return r


def get_new_flow_depth_arrays(r: Ras, branch_data: dict, upstream_downstream: str) -> tuple:

    # read in flow/wse
    rc = r.plan.read_rating_curves()
    wses, flows = rc.values()

    # get the river_reach_rs for the cross section representing the upstream end of this reach
    river = branch_data[f"{upstream_downstream}_data"]["river"]
    reach = branch_data[f"{upstream_downstream}_data"]["reach"]
    rs = branch_data[f"{upstream_downstream}_data"]["xs_id"]
    river_reach_rs = f"{river} {reach} {rs}"

    wse = wses.loc[river_reach_rs, :]
    flow = flows.loc[river_reach_rs, :]

    # convert wse to depth
    thalweg = branch_data[f"{upstream_downstream}_data"]["min_elevation"]
    depth = [e - thalweg for e in wse]

    # get new flow/depth incremented every x ft
    new_flow, new_depth = create_flow_depth_array(flow, depth, depth_increment)

    # enforce min depth
    new_depth[new_depth < MINDEPTH] = MINDEPTH

    return new_depth, new_flow


# TODO
def determine_flow_increments(r: Ras, depth_increment: float):

    for branch_id, branch_data in r.nwm_dict.items():

        r.plan = r.plans[str(branch_id) + "_rc"]

        # get new flow/depth for current branch
        new_depth_us, new_flow_us = get_new_flow_depth_arrays(r, branch_data, "upstream")
        new_depth_ds, new_flow_ds = get_new_flow_depth_arrays(r, branch_data, "downstream")

        # get new depth for downstream branch
        ds_node = branch_data["downstream_data"]["node_id"]

        if ds_node in r.nwm_dict.keys():
            r.plan = r.plans[str(ds_node) + "_rc"]
            new_depth_from_ds_branch, _ = get_new_flow_depth_arrays(r, r.nwm_dict[ds_node], "upstream")

            # combine depths for downstream cross section and for the same cross section but for the
            # downstream branch (for which this cross section is the upstream cross section)
            new_depth_ds = np.unique(np.concatenate([new_depth_ds, new_depth_from_ds_branch]))

        # get new flows for upstream branch
        us_node = branch_data["upstream_data"]["node_id"]

        if us_node in r.nwm_dict.keys():
            r.plan = r.plans[str(us_node) + "_rc"]
            _, new_flow_from_us_branch = get_new_flow_depth_arrays(r, r.nwm_dict[us_node], "downstream")

            # combine flows for upstream cross section and for the same cross section but for the
            # upstream branch (for which this cross section is the downstream cross section)
            new_flow_us = np.unique(np.concatenate([new_flow_us, new_flow_from_us_branch]))

        # get thalweg for the downstream cross section
        thalweg = branch_data[f"downstream_data"]["min_elevation"]

        r.nwm_dict[branch_id]["us_flows"] = new_flow_us
        r.nwm_dict[branch_id]["ds_depths"] = new_depth_ds
        r.nwm_dict[branch_id]["ds_wses"] = [i + thalweg for i in new_depth_ds]

    return r


def create_ras_terrain(r: Ras, src_dem: str):

    # create terrain
    tif = r.clip_dem(src_dem)
    r.create_terrain([tif], TERRAIN_NAME, f"{TERRAIN_NAME}.hdf")

    return r


def run_production_runs(r: Ras):

    for branch_id, branch_data in r.nwm_dict.items():
        print(f"Handling branch_id={branch_id}")

        # write the new flow file
        r.write_new_flow_production_runs(branch_id, branch_data, 0.001)

        # write the new plan file
        r.write_new_plan(r.geom, r.flows[branch_id], branch_id, branch_id)

        # update the content of the RAS project file
        r.update_content()
        r.set_current_plan(r.plans[branch_id])

        # write the update RAS project content to file
        r.write()

        # manage rasmapper
        map_file = os.path.join(r.ras_folder, f"{r.ras_project_basename}.rasmap")
        profiles = r.plan.flow.profile_names
        plan_name = r.plan.title
        plan_hdf = os.path.basename(r.plan.text_file) + ".hdf"

        if os.path.exists(map_file):
            os.remove(map_file)

        if os.path.exists(map_file + ".backup"):
            os.remove(map_file + ".backup")

        rm = RasMap(map_file, r.version)

        rm.update_projection(r.projection_file)

        rm.add_terrain(r.terrain_name)
        rm.add_plan_layer(plan_name, plan_hdf, profiles)
        rm.add_result_layers(plan_name, profiles, "Depth")
        rm.write()

        # run the RAS plan
        r.RunSIM(close_ras=True, show_ras=True, ignore_store_all_maps_error=False)

    return r


def post_process_depth_grids(r: Ras, except_missing_grid: bool = False, dest_directory=None):

    xs = r.geom.cross_sections

    # contruct the dest directory for the clipped depth grid

    if not dest_directory:
        dest_directory = r.postprocessed_output_folder

    if os.path.exists(dest_directory):
        raise FileExistsError(dest_directory)

    # iterate thorugh the flow change locations
    for branch_id, branch_data in r.nwm_dict.items():

        id = branch_id

        # get cross section asociated with this nwm reach
        truncated_xs = xs[
            (xs["river"] == branch_data["upstream_data"]["river"])
            & (xs["reach"] == branch_data["upstream_data"]["reach"])
            & (xs["rs"] <= float(branch_data["upstream_data"]["xs_id"]))
            & (xs["rs"] >= float(branch_data["downstream_data"]["xs_id"]))
        ]

        # create concave hull for this nwm reach/cross sections
        xs_hull = r.geom.xs_concave_hull(truncated_xs)

        # iterate through the profile names for this plan
        for profile_name in r.plans[id].flow.profile_names:

            # construct the default path to the depth grid for this plan/profile
            depth_file = os.path.join(r.ras_folder, str(id), f"Depth ({profile_name}).vrt")

            # if the depth grid path does not exists print a warning then continue to the next profile
            if not os.path.exists(depth_file):
                if except_missing_grid:
                    warnings.warn(f"depth raster does not exists: {depth_file}")
                    continue
                else:
                    raise DepthGridNotFoundError(f"depth raster does not exists: {depth_file}")

            # clip the depth grid naming it with with branch_id, downstream depth, and flow
            out_file = clip_depth_grid(
                depth_file,
                xs_hull,
                id,
                profile_name,
                dest_directory,
            )


def main(
    stac_href: str,
    ras_directory: str,
    bucket: str,
    s3_resource: boto3.resources.factory.ServiceResource,
    s3_client: botocore.client.BaseClient,
    depth_increment: float,
):

    r = read_ras_nwm(stac_href, ras_directory, bucket, s3_client)

    r.nwm_dict = increment_rc_flows(r.nwm_dict, 10)

    run_rating_curves(r)

    r = determine_flow_increments(r, depth_increment)

    r = run_production_runs(r)

    # post_process_depth_grids(r,dest_directory)
    post_process_depth_grids(r, dest_directory=r"C:\Users\mdeshotel\Downloads\STEWARTS_CREEK\output")

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
    ras_directory = r"C:\Users\mdeshotel\Downloads\STEWARTS_CREEK"
    stac_href = "https://stac.dewberryanalytics.com/collections/huc-12040101/items/STEWARTS_CREEK-958e"
    depth_increment = 0.5

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
                main(ras_model_stac_href, ras_directory, bucket, s3_resource, s3_client, depth_increment)

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
