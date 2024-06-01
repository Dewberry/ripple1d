from __future__ import annotations

import logging
import os

import boto3
import numpy as np
from consts import DEFAULT_EPSG, MINDEPTH, NORMAL_DEPTH
from errors import DepthGridNotFoundError
from nwm_reaches import clip_depth_grid
from ras import Ras, RasMap
from utils import create_flow_depth_array


def read_ras(
    ras_directory: str,
    nwm_dict: dict,
    terrain_name: str,
    bucket: str,
    client: boto3.session.Session.client,
    postprocessed_output_s3_path: str = None,
    default_epsg: int = DEFAULT_EPSG,
) -> Ras:
    """ """

    # initialize ras class
    r = Ras(
        ras_directory, nwm_dict, terrain_name, client, bucket, postprocessed_output_s3_path, default_epsg=default_epsg
    )

    # read ras files
    r.read_ras()

    # create cross section gdf
    r.plan.geom.scan_for_xs()

    return r


def run_rating_curves(r: Ras, normal_depth: float = NORMAL_DEPTH) -> Ras:
    """
    Write the flow/plan files needed to produce initial rating curves. 10 (default)
    discharges are applied incremented evenly between flow_2_yr_minus and  flow_100_yr_plus
    specified in the NWM conflation stac item.
    """
    for branch_id, branch_data in r.nwm_dict.items():
        logging.info(f"Handling initial (rating curve) run for branch_id={branch_id}")

        id = branch_id + "_rc"

        # write the new flow file
        r.write_new_flow_rating_curves(id, branch_data, normal_depth=normal_depth)

        # write the new plan file
        r.write_new_plan(r.geom, r.flows[id], id, id)

        # update the content of the RAS project file
        r.update_content()
        r.set_current_plan(r.plans[str(id)])

        # write the update RAS project file content
        r.write()

        # run the RAS plan
        r.RunSIM(close_ras=True, show_ras=True, ignore_store_all_maps_error=True)

    return r


def get_flow_depth_arrays(r: Ras, branch_data: dict, upstream_downstream: str) -> tuple:
    """
    Create new flow and depth arrays from rating curve-plans results.
    """
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
    depth = wse - thalweg

    return (depth, flow)


def determine_flow_increments(r: Ras, default_depths: list[float], depth_increment: float = 0.5) -> Ras:
    """
    Detemine flow increments corresponding to 0.5 ft depth increments using the rating-curve-run results
    """
    for branch_id, branch_data in r.nwm_dict.items():

        r.plan = r.plans[str(branch_id) + "_rc"]

        # get new flow/depth for current branch
        depth_us, flow_us = get_flow_depth_arrays(r, branch_data, "upstream")

        # get new flow/depth incremented every x ft
        new_flow_us, _ = create_flow_depth_array(flow_us, depth_us, depth_increment)

        # get new depth for downstream branch
        ds_node = str(branch_data["downstream_data"]["node_id"])

        if ds_node in r.nwm_dict.keys():
            r.plan = r.plans[str(ds_node) + "_rc"]
            depth_from_ds_branch, flow_from_ds_branch = get_flow_depth_arrays(r, r.nwm_dict[ds_node], "upstream")

            # get new flow/depth incremented every x ft
            _, new_depth_from_ds_branch = create_flow_depth_array(flow_us, depth_us, depth_increment)

            # enforce min depth
            new_depth_from_ds_branch[new_depth_from_ds_branch < MINDEPTH] = MINDEPTH
        else:
            # logging.debug("using default depths")
            # logging.debug(ds_node, r.nwm_dict.keys())
            new_depth_from_ds_branch = default_depths
        # get thalweg for the downstream cross section
        thalweg = branch_data["downstream_data"]["min_elevation"]

        r.nwm_dict[branch_id]["us_flows"] = new_flow_us
        r.nwm_dict[branch_id]["ds_depths"] = new_depth_from_ds_branch
        r.nwm_dict[branch_id]["ds_wses"] = [i + thalweg for i in new_depth_from_ds_branch]

    return r


def run_normal_depth_runs(r: Ras, normal_depth: float = NORMAL_DEPTH) -> Ras:
    """
    Write and compute the normal depth run plans using the flow increments determined from the
    initial rating-curve-runs.
    """
    for branch_id, branch_data in r.nwm_dict.items():
        logging.info(f"Handling normal depth run for branch_id={branch_id}")

        branch_id = branch_id + "_nd"

        # write the new flow file
        r.write_new_flow_production_runs(
            branch_id, branch_data, normal_depth=normal_depth, intermediate_known_wse=False
        )

        # write the new plan file
        r.write_new_plan(r.geom, r.flows[branch_id], branch_id, branch_id)

        # update the content of the RAS project file
        r.update_content()
        r.set_current_plan(r.plans[branch_id])

        # write the update RAS project content to file
        r.write()

        # update rasmapper file
        r = update_rasmapper_for_mapping(r)

        # run the RAS plan
        r.RunSIM(close_ras=True, show_ras=True, ignore_store_all_maps_error=False)

    return r


def update_rasmapper_for_mapping(r: Ras):
    """
    Write a rasmapper file to output depth grids for the current plan
    """

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

    return r


def filter_ds_depths(r: Ras):
    for branch_id, branch_data in r.nwm_dict.items():

        r.plan = r.plans[str(branch_id) + "_nd"]

        # get ds wses resulting from normal depth runs
        nd_depth, nd_flow = get_flow_depth_arrays(r, branch_data, "downstream")
        r.nwm_dict[branch_id]["nd_depth"] = nd_depth

    return r


def run_kwse_runs(r: Ras, normal_depth: float = NORMAL_DEPTH) -> Ras:
    """
    Write and compute the production run plans using the flow/wse increments determined from the
    initial rating-curve-runs.
    """
    for branch_id, branch_data in r.nwm_dict.items():
        logging.info(f"Handling production run for branch_id={branch_id}")

        branch_id = branch_id + "_kwse"

        # write the new flow file
        r.write_new_flow_production_runs(branch_id, branch_data, normal_depth=normal_depth, intermediate_known_wse=True)

        # write the new plan file
        r.write_new_plan(r.geom, r.flows[branch_id], branch_id, branch_id)

        # update the content of the RAS project file
        r.update_content()
        r.set_current_plan(r.plans[branch_id])

        # write the update RAS project content to file
        r.write()

        # update rasmapper file
        r = update_rasmapper_for_mapping(r)

        # run the RAS plan
        r.RunSIM(close_ras=True, show_ras=True, ignore_store_all_maps_error=False)

    return r


def post_process_depth_grids(r: Ras, except_missing_grid: bool = False, dest_directory=None):
    """
    Clip depth grids based on their associated NWM branch and respective cross sections.

    """
    xs = r.geom.cross_sections

    # contruct the dest directory for the clipped depth grid

    if not dest_directory:
        dest_directory = r.postprocessed_output_folder

    if os.path.exists(dest_directory):
        raise FileExistsError(dest_directory)

    # iterate thorugh the flow change locations
    for branch_id, branch_data in r.nwm_dict.items():

        for prefix in ["_kwse", "_nd"]:
            id = branch_id + prefix

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
                        logging.warning(f"depth raster does not exists: {depth_file}")
                        continue
                    else:
                        raise DepthGridNotFoundError(f"depth raster does not exists: {depth_file}")

                # clip the depth grid naming it with with branch_id, downstream depth, and flow
                clip_depth_grid(
                    depth_file,
                    xs_hull,
                    id,
                    profile_name,
                    dest_directory,
                )
