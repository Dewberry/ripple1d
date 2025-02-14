"""Conflate HEC-RAS Model."""

import copy
import json
import logging
import os
import traceback
from datetime import datetime
from itertools import chain, permutations
from urllib.parse import quote

import boto3
import pandas as pd
import pystac

import ripple1d
from ripple1d.conflate.rasfim import (
    RasFimConflater,
    nearest_line_to_point,
    ras_reaches_metadata,
    ras_xs_geometry_data,
    walk_network,
)
from ripple1d.ops.metrics import compute_conflation_metrics
from ripple1d.utils.ripple_utils import NWMWalker, clip_ras_centerline

logging.getLogger("fiona").setLevel(logging.ERROR)
logging.getLogger("botocore").setLevel(logging.ERROR)


def href_to_vsis(href: str, bucket: str) -> str:
    """Convert public aws href to a virtual ref for gdal read."""
    return href.replace(f"https://{bucket}.s3.amazonaws.com", f"/vsis3/{bucket}")


def s3_public_href(bucket: str, key: str) -> str:
    """Convert bucket and key to public href."""
    return f"https://{bucket}.s3.amazonaws.com/{quote(key)}"


def conflate_single_nwm_reach(rfc: RasFimConflater, nwm_reach_id: int):
    """Conflate a HEC-RAS model with a specific NWM reach."""
    nwm_reach_id_identified = False
    model_local_nwm_reaches = rfc.local_nwm_reaches()
    for river_reach_name in rfc.ras_river_reach_names:
        local_nwm_reaches = model_local_nwm_reaches.intersects(
            rfc.ras_xs_concave_hull(river_reach_name)["geometry"].iloc[0]
        )

        ras_start_point, ras_stop_point = rfc.ras_start_end_points(river_reach_name=river_reach_name)
        us_most_reach_id = nearest_line_to_point(local_nwm_reaches, ras_start_point)
        ds_most_reach_id = nearest_line_to_point(local_nwm_reaches, ras_stop_point)

        potential_reach_path = walk_network(local_nwm_reaches, us_most_reach_id, ds_most_reach_id)
        candidate_reaches = local_nwm_reaches.query(f"ID in {potential_reach_path}")
        if len(candidate_reaches.query(f"ID == {nwm_reach_id}")) == 1:
            nwm_reach_id_identified = True
            return ras_reaches_metadata(rfc, candidate_reaches[candidate_reaches["ID"] == nwm_reach_id])
    if not nwm_reach_id_identified:
        raise ValueError(f"nwm_reach_id {nwm_reach_id} not conflating to the ras model geometry.")


def conflate_model(source_model_directory: str, model_name: str, source_network: dict):
    """Conflate a HEC-RAS model with NWM reaches.

    Parameters
    ----------
    source_model_directory : str
        The path to the directory containing HEC-RAS project, plan, geometry,
        and flow files.
    model_name : str
        The name of the HEC-RAS model.
    source_network : dict
        Information on the network to conflate

        - **file_name** (str):
            path/to/nwm_network.parquet (required)
        - **type** (str):
            must be 'nwm_hydrofabric' (required)
        - **version** (str):
            optional version number to log
    task_id : str, optional
        Task ID to use for logging, by default ""

    Returns
    -------
    str
        Path to the .conflation.json file generated

    Raises
    ------
    KeyError
        Raises when source_network dict does not contain a file_name value
    ValueError
        Raises when source_network type is not nwm_hydrofabric

    Notes
    -----
    The spatial extents of HEC-RAS river reaches and National Water model (NWM)
    reaches are not aligned.  The conflate_model endpoint resolves these
    differences by associating HEC-RAS models and model components (e.g.
    cross-sections) with the NWM reaches they overlap.

    #. Generate a concave hull (bounding geometry) around the HEC-RAS source
       model cross-sections
    #. Extract NWM reaches intersecting the hull
    #. For each HEC-RAS river reach within the source model,
      #. Locate the NWM reaches nearest to the most upstream and most
         downstream cross-sections
      #. Extract all intermediate NWM reaches by walking   the network from
         upstream to downstream
    #. For each NWM reach extracted,
      #. Locate the HEC-RAS cross-sections that intersect the reach
        #. Discard cross-sections that are not drawn right to left looking
           downstream
        #. If no cross-sections intersect the reach, mark the reach as
           “eclipsed”
      #. Mark the HEC-RAS cross-section closest to the upstream end of the
         reach as the “us_xs ”
      #. Identify the HEC-RAS cross-section that is closest to the downstream
         end of the reach, and then mark the next HEC-RAS cross-section
         downstream of it as the “ds_xs”
        #. If the “ds_xs” would be a HEC-RAS junction, mark the first
           cross-section downstream of the junction as “ds_xs”
      #. If “ds_xs” and “us_xs” are the same, mark the reach as eclipsed
    #. Generate a map of the conflated reaches and calculate conflation metrics

    Additionally, high and low flows are generated for each reach to bound the
    SRC generated in later steps. The low flow is 1.2 times the high flow
    threshold listed for the reach in the NWM network.  The high flow is the
    100 year flow from the NWM network.
    """
    logging.info(f"conflate_model starting")
    if not "file_name" in source_network:
        raise KeyError(f"source_network must contain 'file_name', invalid parameters: {source_network}")

    if not source_network["type"] == "nwm_hydrofabric":
        raise ValueError(f"source_network type must be 'nwm_hydrofabric', invalid parameters: {source_network}")

    conflation = _conflate_model(source_model_directory, model_name, source_network)
    logging.debug(f"Conflation results: {conflation}")

    # if not conflated(metadata):
    #     return f"no reaches conflated"

    conflation_file = os.path.join(source_model_directory, f"{model_name}.conflation.json")
    with open(conflation_file, "w") as f:
        f.write(json.dumps(conflation, indent=4))

    try:
        compute_conflation_metrics(source_model_directory, model_name, source_network)
    except Exception as e:
        logging.error(f"| Error: {e}")
        logging.error(f"| Traceback: {traceback.format_exc()}")

    logging.info(f"conflate_model complete")
    return {"conflation_file": conflation_file}


def _conflate_model(source_model_directory: str, model_name: str, source_network: dict) -> dict:
    """Create dictionary mapping NWM reach to RAS u/s and d/s XS limits."""
    rfc = RasFimConflater(source_network["file_name"], source_model_directory, model_name)
    local_nwm_reaches = list(set(chain.from_iterable([get_nwm_reaches(rr, rfc) for rr in rfc.ras_river_reach_names])))
    conflation = {
        "reaches": ras_reaches_metadata(rfc, local_nwm_reaches),
        "metadata": generate_metadata(source_network, rfc),
    }
    conflation = find_eclipsed_reaches(rfc, conflation)
    conflation = fix_junctions(rfc, conflation)
    return conflation


def find_eclipsed_reaches(rfc: RasFimConflater, conflation: dict) -> dict:
    """Update the conflation dictionary to add NWM reaches between HEC-RAS sections."""
    linked_reaches = get_linked_reaches(conflation["reaches"])
    for us_xs, ds_xs in linked_reaches:
        eclipsed = rfc.nwm_walker.walk(us_xs, ds_xs)
        for r in eclipsed:
            if not r in [us_xs, ds_xs]:
                conflation["reaches"][r] = {"eclipsed": True} | rfc.get_nwm_reach_metadata(r)
    return conflation


def fix_junctions(rfc: RasFimConflater, conflation: dict) -> dict:
    """Update conflation such that confluences match between NWM and HEC-RAS."""
    for reach in conflation["reaches"]:
        if conflation["reaches"][reach]["eclipsed"]:
            continue

        children = rfc.nwm_walker.tree_dict_us[reach]
        # Check if both tribs are in conflation file.
        if all([child in conflation["reaches"] for child in children]) and len(children) == 2:
            # walk tribs for eclipsed reaches
            _children = []
            for trib in children:
                while conflation["reaches"][trib]["eclipsed"]:
                    children = [i for i in rfc.nwm_walker.tree_dict_us[trib] if i in conflation["reaches"]]
                    assert len(children) == 1, f"Failed finding a non-eclipsed child for {trib}.  Got {children}"
                    trib = children[0]
                _children.append(trib)
            children = _children

            # Find confluence
            rr_serializer = lambda xs: f"{xs["river"]}_{xs["reach"]}"
            us_limits = [rr_serializer(conflation["reaches"][r]["us_xs"]) for r in children]
            confluence = rfc.ras_walker.get_confluence(us_limits[0], us_limits[1])
            if confluence is None:
                continue  # hydrologically disconnected

            # Correct Parent
            new_us_limit = rfc.ras_xs[rfc.ras_xs["river_reach"] == confluence]["river_station"].max()
            new_us_limit = f"{confluence}_{new_us_limit}"
            common_section = ras_xs_geometry_data(rfc, new_us_limit)
            conflation["reaches"][reach]["us_xs"] = common_section

            # Correct Children
            for trib in children:
                conflation["reaches"][trib]["ds_xs"] = common_section
    return conflation


def get_linked_reaches(reaches: dict) -> list:
    """Return list of NWM IDs that share cross-sections (u/s, d/s)."""
    # serialize ids to reduce errors
    serialized = {}
    for r in reaches:
        serialized[r] = {}
        for xs in ["us_xs", "ds_xs"]:
            serialized[r][xs] = f"{reaches[r][xs]['river']}_{reaches[r][xs]['reach']}_{reaches[r][xs]['xs_id']}"
    # Find matches
    return [p for p in permutations(reaches.keys(), 2) if serialized[p[0]]["ds_xs"] == serialized[p[1]]["us_xs"]]


def get_nwm_reaches(river_reach_name: str, rfc: RasFimConflater) -> list[str]:
    """Find a valid path of nwm reaches across a HEC-RAS river_reach_name (if one exists)."""
    try:
        local_nwm_reaches = rfc.local_nwm_reaches(river_reach_name, buffer=1000)

        # get the start and end points of the river reach
        ras_start_point, ras_stop_point = rfc.ras_start_end_points(river_reach_name=river_reach_name, clip_to_xs=True)

        # if this is the only river reach, raise error
        river = rfc.ras_centerline_by_river_reach_name(river_reach_name)
        river = clip_ras_centerline(river, rfc.xs_by_river_reach_name(river_reach_name))
        truncate_distance = 0
        us_most_reach_id, ds_most_reach_id = None, None
        while True:
            try:
                us_most_reach_id = nearest_line_to_point(local_nwm_reaches, ras_start_point, start_reach_distance=100)
                break
            except ValueError as e:
                if truncate_distance > 10000:
                    logging.info(f"Could not identifiy a network reach near the upstream end of {river_reach_name}")
                    break
                truncate_distance += 100
                ras_start_point = river.interpolate(truncate_distance)

                logging.debug(
                    f"truncate_distance: {truncate_distance} | length {ras_start_point} |  river reach: {river_reach_name}"
                )
        if us_most_reach_id is None:
            raise RuntimeError(f"Could not identifiy a network reach near the upstream end of {river_reach_name}")

        try:
            ds_most_reach_id = nearest_line_to_point(local_nwm_reaches, ras_stop_point)
        except:
            raise RuntimeError(f"Could not identifiy a network reach near the downstream end of {river_reach_name}")

        logging.info(
            f"{river_reach_name} | us_most_reach_id ={us_most_reach_id} and ds_most_reach_id = {ds_most_reach_id}"
        )

        # walk network to get the potential reach ids
        potential_reach_path = rfc.nwm_walker.walk(us_most_reach_id, ds_most_reach_id)
    except Exception as e:
        logging.error(f"river-reach: {river_reach_name} | Error: {e}")
        logging.error(f"river-reach: {river_reach_name} | Traceback: {traceback.format_exc()}")
        return []
    else:
        return potential_reach_path


def generate_metadata(source_network: dict, rfc: RasFimConflater) -> dict:
    """Log metadata about how conflation was generated."""
    metadata = {}
    metadata["source_network"] = source_network.copy()
    metadata["source_network"]["file_name"] = os.path.basename(rfc.nwm_pq)
    metadata["conflation_ripple1d_version"] = ripple1d.__version__
    metadata["metrics_ripple1d_version"] = ripple1d.__version__
    metadata["source_ras_model"] = {
        "stac_api": rfc.stac_api,
        "stac_collection_id": rfc.stac_collection_id,
        "stac_item_id": rfc.stac_item_id,
        "units": rfc.units,
    }
    metadata["source_ras_model"]["source_ras_files"] = {
        "geometry": rfc.primary_geom_file,
        "forcing": rfc.primary_flow_file,
        "project-file": rfc.ras_project_file,
        "plan": rfc.primary_plan_file,
    }
    return metadata


def conflated(metadata: dict) -> bool:
    """Determine if any reaches conflated."""
    count = 0
    for reach_data in metadata["reaches"].values():
        if not reach_data["eclipsed"]:
            count += 1
    if count == 0:
        return False
    else:
        return True
