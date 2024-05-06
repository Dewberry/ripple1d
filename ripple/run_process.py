from ras import Ras
from utils import create_flow_depth_array
import boto3
import os
from dotenv import load_dotenv, find_dotenv
from nwm_reaches import increment_rc_flows
from ras import RasMap, Ras
from consts import TERRAIN_NAME
from nwm_reaches import clip_depth_grid
import warnings


# load s3 credentials
load_dotenv(find_dotenv())

dewberrywrc_acct_session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
client = dewberrywrc_acct_session.client("s3")


def read_ras_nwm(stac_href: str, ras_directory: str, bucket: str, client):

    # read ras model and create cross section gdf
    r = Ras(ras_directory, stac_href, client, bucket, default_epsg=2277)
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
        r.RunSIM(close_ras=True, show_ras=True)
    return r


def determine_flow_increments(r: Ras, depth_increment: float):

    for branch_id, branch_data in r.nwm_dict.items():

        r.plan = r.plans[str(branch_id) + "_rc"]

        # read in flow/wse
        rc = r.plan.read_rating_curves()
        wses, flows = rc.values()

        # get the river_reach_rs for the cross section representing the upstream end of this reach
        river = branch_data["upstream_data"]["river"]
        reach = branch_data["upstream_data"]["reach"]
        rs = branch_data["upstream_data"]["xs_id"]
        river_reach_rs = f"{river} {reach} {rs}"

        wse = wses.loc[river_reach_rs, :]
        flow = flows.loc[river_reach_rs, :]
        # convert wse to depth
        thalweg = branch_data["upstream_data"]["min_elevation"]
        depth = [e - thalweg for e in wse]

        new_flow, new_depth = create_flow_depth_array(flow, depth, depth_increment)

        r.nwm_dict[branch_id]["us_flows"] = new_flow
        r.nwm_dict[branch_id]["us_depths"] = new_depth
        r.nwm_dict[branch_id]["us_wses"] = [i + thalweg for i in new_depth]

        # get the river_reach_rs for the cross section representing the downstream end of this reach
        river = branch_data["downstream_data"]["river"]
        reach = branch_data["downstream_data"]["reach"]
        rs = branch_data["downstream_data"]["xs_id"]
        river_reach_rs = f"{river} {reach} {rs}"

        wse = wses.loc[river_reach_rs, :]
        flow = flows.loc[river_reach_rs, :]

        # convert wse to depth
        thalweg = branch_data["downstream_data"]["min_elevation"]
        depth = [e - thalweg for e in wse]

        new_flow, new_depth = create_flow_depth_array(flow, depth, depth_increment)

        r.nwm_dict[branch_id]["ds_flows"] = new_flow
        r.nwm_dict[branch_id]["ds_depths"] = new_depth
        r.nwm_dict[branch_id]["ds_wses"] = [i + thalweg for i in new_depth]

    return r


def create_ras_terrain(r: Ras, src_dem: str):

    # create terrain
    tif = r.clip_dem(src_dem)
    r.create_terrain([tif], TERRAIN_NAME, f"{TERRAIN_NAME}.hdf")

    return r


def run_production_runs(r: Ras):

    for branch_id, branch_data in r.nwm_dict.items():

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
        r.RunSIM(close_ras=True, show_ras=True)
    return r


def post_process_depth_grids(r: Ras):

    xs = r.geom.cross_sections

    # contruct the dest directory for the clipped depth grid
    dest_directory = os.path.join(r.ras_folder, "output")

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

                warnings.warn(f"depth raster does not exists: {depth_file}")
                continue

            # clip the depth grid naming it with with river_reach_ds_rs_us_rs_flow_depth
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
    client,
    depth_increment: float,
):

    r = read_ras_nwm(stac_href, ras_directory, bucket, client)

    r.nwm_dict = increment_rc_flows(r.nwm_dict, 10)

    run_rating_curves(r)

    r = determine_flow_increments(r, depth_increment)

    r = run_production_runs(r)

    post_process_depth_grids(r)


if __name__ == "__main__":
    pass

bucket = "fim"

ras_directory = r"C:\Users\mdeshotel\Downloads\WFSJR_097"

stac_href = "https://stac.dewberryanalytics.com/collections/huc-12040101/items/WFSJR_097-448d"

depth_increment = 0.5

main(stac_href, ras_directory, bucket, client, depth_increment)
