"""Conflate HEC-RAS Model."""

import json
import logging
import os
import traceback
from datetime import datetime
from urllib.parse import quote

import boto3
import pandas as pd
import pystac

import ripple1d
from ripple1d.conflate.plotter import plot_conflation_results
from ripple1d.conflate.rasfim import (
    RasFimConflater,
    nearest_line_to_point,
    ras_reaches_metadata,
    walk_network,
)
from ripple1d.ops.metrics import compute_conflation_metrics
from ripple1d.utils.ripple_utils import clip_ras_centerline

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


def conflate_model(source_model_directory: str, source_network: dict, task_id: str = ""):
    """Conflate a HEC-RAS model with NWM reaches.

    source_network example:
    {
        "file_name": "nwm_flows_v3.parquet", // required
        "version": "2.1" // could be empty if not provided by the caller
        "type": "nwm_hydrofabric" // required
    }
    """
    logging.info(f"{task_id} | conflate_model starting")
    try:
        nwm_pq_path = source_network["file_name"]
    except KeyError:
        raise KeyError(f"source_network must contain 'file_name', invalid parameters: {source_network}")

    if not source_network["type"] == "nwm_hydrofabric":
        raise ValueError(f"source_network type must be 'nwm_hydrofabric', invalid parameters: {source_network}")

    version = source_network.get("version", "")

    rfc = RasFimConflater(nwm_pq_path, source_model_directory)
    metadata = {"reaches": {}}
    buffer = 1000
    for river_reach_name in rfc.ras_river_reach_names:
        try:
            local_nwm_reaches = rfc.local_nwm_reaches(river_reach_name, buffer=buffer)

            # get the start and end points of the river reach
            ras_start_point, ras_stop_point = rfc.ras_start_end_points(
                river_reach_name=river_reach_name, clip_to_xs=True
            )

            # if this is the only river reach, raise error
            river = rfc.ras_centerline_by_river_reach_name(river_reach_name)
            river = clip_ras_centerline(river, rfc.xs_by_river_reach_name(river_reach_name))
            truncate_distance = 0
            us_most_reach_id, ds_most_reach_id = None, None
            while True:
                try:
                    us_most_reach_id = nearest_line_to_point(
                        local_nwm_reaches, ras_start_point, start_reach_distance=100
                    )
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
                continue

            try:
                ds_most_reach_id = nearest_line_to_point(local_nwm_reaches, ras_stop_point)
            except:
                logging.info(f"Could not identifiy a network reach near the downstream end of {river_reach_name}")
                continue

            logging.info(
                f"{task_id} | {river_reach_name} | us_most_reach_id ={us_most_reach_id} and ds_most_reach_id = {ds_most_reach_id}"
            )

            # walk network to get the potential reach ids
            potential_reach_path = walk_network(local_nwm_reaches, us_most_reach_id, ds_most_reach_id, river_reach_name)
            potential_reach_path = list(set(potential_reach_path) - set(metadata.keys()))

            # get gdf of the candidate reaches
            candidate_reaches = local_nwm_reaches.query(f"ID in {potential_reach_path}")

            metadata["reaches"].update(ras_reaches_metadata(rfc, candidate_reaches, river_reach_name, task_id))
        except Exception as e:
            logging.error(f"{task_id} | river-reach: {river_reach_name} | Error: {e}")
            logging.error(f"{task_id} | river-reach: {river_reach_name} | Traceback: {traceback.format_exc()}")

    # if not conflated(metadata):
    #     return f"{task_id} | no reaches conflated"

    ids = list(metadata["reaches"].keys())
    fim_stream = rfc.local_nwm_reaches()[rfc.local_nwm_reaches()["ID"].isin(ids)]
    # conflation_png = f"{rfc.ras_gpkg.replace('.gpkg','.conflation.png')}"

    # plot_conflation_results(
    #     rfc,
    #     fim_stream,
    #     conflation_png,
    #     limit_plot_to_nearby_reaches=True,
    # )

    logging.debug(f"{task_id} | Conflation results: {metadata}")
    conflation_file = f"{rfc.ras_gpkg.replace('.gpkg','.conflation.json')}"

    metadata["metadata"] = {}
    metadata["metadata"]["source_network"] = source_network.copy()
    metadata["metadata"]["source_network"]["file_name"] = os.path.basename(nwm_pq_path)
    # metadata["metadata"]["conflation_png"] = os.path.basename(conflation_png)
    metadata["metadata"]["conflation_ripple1d_version"] = ripple1d.__version__
    metadata["metadata"]["metrics_ripple1d_version"] = ripple1d.__version__
    metadata["metadata"]["source_ras_model"] = {
        "stac_api": rfc.stac_api,
        "stac_collection_id": rfc.stac_collection_id,
        "stac_item_id": rfc.stac_item_id,
    }
    metadata["metadata"]["source_ras_model"]["source_ras_files"] = {
        "geometry": rfc.primary_geom_file,
        "forcing": rfc.primary_flow_file,
        "project-file": rfc.ras_project_file,
        "plan": rfc.primary_plan_file,
    }

    with open(conflation_file, "w") as f:
        f.write(json.dumps(metadata, indent=4))

    try:
        compute_conflation_metrics(source_model_directory, source_network, task_id)
    except Exception as e:
        logging.error(f"{task_id} | Error: {e}")
        logging.error(f"{task_id} | Traceback: {traceback.format_exc()}")

    logging.info(f"{task_id} | conflate_model complete")
    return conflation_file


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


# def conflate_s3_model(
#     item: pystac.Item, client: boto3.client, bucket: str, stac_item_s3_key: str, rfc: RasFimConflater
# ):
#     """Conflate a model from s3."""
#     # build conflation key and href
#     nwm_conflation_key = stac_item_s3_key.replace(".json", ".conflation.json")
#     nwm_conflation_href = s3_public_href(bucket, nwm_conflation_key)

#     # conflate the mip ras model to nwm reaches
#     conflation_results = conflate(rfc)

#     # write conflation results to s3
#     client.put_object(
#         Body=json.dumps(conflation_results).encode(),
#         Bucket=bucket,
#         Key=nwm_conflation_key,
#     )

#     # Add ripple parameters asset to item
#     item.add_asset(
#         "NWM_Conflation",
#         pystac.Asset(
#             nwm_conflation_href,
#             title="NWM_Conflation",
#             roles=[pystac.MediaType.JSON, "nwm-conflation"],
#             extra_fields={
#                 "software": f"ripple1d {ripple1d.__version__}",
#                 "date_created": datetime.now().isoformat(),
#             },
#         ),
#     )

#     limit_plot = True

#     conflation_thumbnail_key = nwm_conflation_key.replace("json", "png")
#     conflation_thumbnail_href = s3_public_href(bucket, conflation_thumbnail_key)

#     ids = list(conflation_results["reaches"].keys())

#     fim_stream = rfc.local_nwm_reaches()[rfc.local_nwm_reaches()["ID"].isin(ids)]

#     plot_conflation_results(
#         rfc,
#         fim_stream,
#         conflation_thumbnail_key,
#         bucket=bucket,
#         s3_client=client,
#         limit_plot_to_nearby_reaches=limit_plot,
#     )
#     # Add thumbnail asset to item
#     item.add_asset(
#         "ThumbnailConflationResults",
#         pystac.Asset(
#             conflation_thumbnail_href,
#             title="ThumbnailConflationResults",
#             roles=[pystac.MediaType.PNG, "thumbnail"],
#             extra_fields={
#                 "software": f"ripple1d {ripple1d.__version__}",
#                 "date_created": datetime.now().isoformat(),
#             },
#             description="""PNG of NWM conflation results with OpenStreetMap basemap.""",
#         ),
#     )

#     client.put_object(Body=json.dumps(item.to_dict()).encode(), Bucket=bucket, Key=stac_item_s3_key)
