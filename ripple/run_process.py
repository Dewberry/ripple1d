from ras import Ras
from utils import create_flow_depth_array
import boto3
import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import geopandas as gpd
from nwm_reaches import compile_flows, get_us_ds_rs
from ras import RasMap
from consts import TERRAIN_NAME
from nwm_reaches import clip_depth_grid
import warnings

# load s3 credentials
load_dotenv(find_dotenv())

dewberrywrc_acct_session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
client = dewberrywrc_acct_session.client("s3")


def read_ras_nwm(stac_href, ras_directory, bucket):

    # read ras model and create cross section gdf
    r = Ras(ras_directory, stac_href, client, bucket, default_epsg=2277)
    r.plan.geom.scan_for_xs()

    return r


def run_rating_curves(r):

    for i, row in r.nwm_df.iterrows():

        id = str(row["branch_id"]) + "_rc"

        # write the new flow file
        r.write_new_flow_rating_curves(id, row, normal_depth=0.001)

        # write the new plan file
        r.write_new_plan(r.geom, r.flows[id], id, id)

        # update the content of the RAS project file
        r.update_content()
        r.set_current_plan(r.plans[str(id)])

        # write the update RAS project file content back to disk
        r.write()

        # run the RAS plan
        r.RunSIM(close_ras=True, show_ras=True)


def determine_flow_increments(r: Ras, depth_increment: float):

    xs = r.geom.cross_sections

    new_us_flows, new_us_depths, new_us_wses = [], [], []
    new_ds_flows, new_ds_depths, new_ds_wses = [], [], []

    rivers, reaches = [], []

    for i, row in r.nwm_df.iterrows():

        r.plan = r.plans[str(row["branch_id"]) + "_rc"]

        # read in flow/wse
        rc = r.plan.read_rating_curves()
        wses, flows = rc.values()

        # get the river_reach_rs for the cross section representing the upstream end of this reach
        river_reach_rs = xs.loc[xs["rs"] == float(row["nearest_xs_us"]), "river_reach_rs"]
        rivers.append(xs.loc[xs["rs"] == float(row["nearest_xs_us"]), "river"].iloc[0])
        reaches.append(xs.loc[xs["rs"] == float(row["nearest_xs_us"]), "reach"].iloc[0])

        wse = wses.loc[river_reach_rs, :].iloc[0]
        flow = flows.loc[river_reach_rs, :].iloc[0]

        # convert wse to depth
        thalweg = xs.loc[xs["rs"] == float(row["nearest_xs_us"]), "thalweg"].iloc[0]
        depth = [e - thalweg for e in wse]

        new_flow, new_depth = create_flow_depth_array(flow, depth, depth_increment)
        new_us_flows.append(new_flow)

        new_us_depths.append(new_depth)
        new_us_wses.append([i + thalweg for i in new_depth])

        # get the river_reach_rs for the cross section representing the downstream end of this reach
        river_reach_rs = xs.loc[xs["rs"] == float(row["nearest_xs_ds"]), "river_reach_rs"]

        wse = wses.loc[river_reach_rs, :].iloc[0]
        flow = flows.loc[river_reach_rs, :].iloc[0]

        # convert wse to depth
        thalweg = xs.loc[xs["rs"] == float(row["nearest_xs_ds"]), "thalweg"].iloc[0]
        depth = [e - thalweg for e in wse]

        new_flow, new_depth = create_flow_depth_array(flow, depth, depth_increment)

        new_ds_depths.append(new_depth)
        new_ds_wses.append([i + thalweg for i in new_depth])

        new_ds_flows.append(new_flow)

    r.nwm_df["river"] = rivers
    r.nwm_df["reach"] = reaches

    r.nwm_df["us_flows"] = new_us_flows
    r.nwm_df["us_depths"] = new_us_depths
    r.nwm_df["us_wses"] = new_us_wses

    r.nwm_df["ds_flows"] = new_ds_flows
    r.nwm_df["ds_depths"] = new_ds_depths
    r.nwm_df["ds_wses"] = new_ds_wses


def create_ras_terrain(r, src_dem):

    # create terrain
    tif = r.clip_dem(src_dem)
    r.create_terrain([tif], TERRAIN_NAME, f"{TERRAIN_NAME}.hdf")


def run_production_runs(r):

    for i, row in r.nwm_df.iterrows():

        id = row["branch_id"]

        # write the new flow file
        r.write_new_flow_production_runs(id, row, 0.001)

        # write the new plan file
        r.write_new_plan(r.geom, r.flows[id], id, id)

        # update the content of the RAS project file
        r.update_content()
        r.set_current_plan(r.plans[id])

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

        rm.add_terrain()
        rm.add_plan_layer(plan_name, plan_hdf, profiles)
        rm.add_result_layers(plan_name, profiles, "Depth")
        rm.write()

        # run the RAS plan
        r.RunSIM(close_ras=True, show_ras=True)


def post_process_depth_grids(r):

    xs = r.geom.cross_sections

    # contruct the dest directory for the clipped depth grid
    dest_directory = os.path.join(r.ras_folder, "output")

    # iterate thorugh the flow change locations
    for i, nwm_reach_data in r.nwm_df.iterrows():

        id = nwm_reach_data["branch_id"]

        # get cross section asociated with this nwm reach
        truncated_xs = xs[
            (xs["river"] == nwm_reach_data["river"])
            & (xs["reach"] == nwm_reach_data["reach"])
            & (xs["rs"] <= float(nwm_reach_data["nearest_xs_us"]))
            & (xs["rs"] >= float(nwm_reach_data["nearest_xs_ds"]))
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


def main(stac_href, ras_directory, bucket, src_dem):

    r = read_ras_nwm(stac_href, ras_directory, bucket)

    compile_flows(r.nwm_df)

    run_rating_curves(r)

    determine_flow_increments(r, depth_increment)

    create_ras_terrain(r, src_dem)

    run_production_runs(r)

    post_process_depth_grids(r)


if __name__ == "__main__":
    pass

bucket = "fim"

ras_directory = r"C:\Users\mdeshotel\Downloads\test_stac9"

stac_href = "https://fim.s3.amazonaws.com/stac/ripple/WFSJ_Main-cd42.json"

src_dem = "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt"

depth_increment = 0.5

main(stac_href, ras_directory, bucket, client, src_dem, depth_increment)
