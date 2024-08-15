"""Conflate HEC-RAS Model."""

import json
import logging
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


def conflate(rfc: RasFimConflater):
    """Conflate a HEC-RAS model with NWM reaches."""
    metadata = {}
    # get nwm reaches that intersect the convex hull of the ras model
    model_local_nwm_reaches = rfc.local_nwm_reaches()
    for river_reach_name in rfc.ras_river_reach_names:

        # get the concave hull of the this river reach
        concave_hull = rfc.ras_xs_concave_hull(river_reach_name)

        # get the nwm reaches that intersect the concave hull of the cross section of this river reach using the convex hull for this model
        local_nwm_reaches = model_local_nwm_reaches[
            model_local_nwm_reaches.intersects(concave_hull["geometry"].iloc[0])
        ]

        # get the start and end points of the river reach
        ras_start_point, ras_stop_point = rfc.ras_start_end_points(river_reach_name=river_reach_name)

        # get the nearest upstream and downstream nwm reaches to the start and end points of the river reach
        if len(rfc.ras_river_reach_names) == 1:
            # if there are multiple river reaches, don't raise error; quitely continue to the next river reach
            us_most_reach_id = nearest_line_to_point(local_nwm_reaches, ras_start_point)
            ds_most_reach_id = nearest_line_to_point(local_nwm_reaches, ras_stop_point)
        else:
            # if this is the only river reach, raise error
            try:
                us_most_reach_id = nearest_line_to_point(local_nwm_reaches, ras_start_point)
                ds_most_reach_id = nearest_line_to_point(local_nwm_reaches, ras_stop_point)
            except ValueError as e:
                logging.error(f"Error: {e}")
                continue

        logging.info(
            f"{river_reach_name} | us_most_reach_id ={us_most_reach_id} and ds_most_reach_id = {ds_most_reach_id}"
        )

        # walk network to get the potential reach ids
        potential_reach_path = walk_network(local_nwm_reaches, us_most_reach_id, ds_most_reach_id)
        potential_reach_path = list(set(potential_reach_path) - set(metadata.keys()))

        # get gdf of the candidate reaches
        candidate_reaches = local_nwm_reaches.query(f"ID in {potential_reach_path}")

        reach_metadata = ras_reaches_metadata(rfc, candidate_reaches)
        metadata.update(reach_metadata)

    rfc.write_hulls()
    metadata["nwm_reach_source"] = rfc.nwm_pq

    return metadata


def conflate_s3_model(
    item: pystac.Item, client: boto3.client, bucket: str, stac_item_s3_key: str, rfc: RasFimConflater
):
    """Conflate a model from s3."""
    # build conflation key and href
    nwm_conflation_key = stac_item_s3_key.replace(".json", ".conflation.json")
    nwm_conflation_href = s3_public_href(bucket, nwm_conflation_key)

    # conflate the mip ras model to nwm reaches
    conflation_results = conflate(rfc)

    # write conflation results to s3
    client.put_object(
        Body=json.dumps(conflation_results).encode(),
        Bucket=bucket,
        Key=nwm_conflation_key,
    )

    # Add ripple parameters asset to item
    item.add_asset(
        "NWM_Conflation",
        pystac.Asset(
            nwm_conflation_href,
            title="NWM_Conflation",
            roles=[pystac.MediaType.JSON, "nwm-conflation"],
            extra_fields={
                "software": f"ripple1d {ripple1d.__version__}",
                "date_created": datetime.now().isoformat(),
            },
        ),
    )

    limit_plot = True

    conflation_thumbnail_key = nwm_conflation_key.replace("json", "png")
    conflation_thumbnail_href = s3_public_href(bucket, conflation_thumbnail_key)

    ids = [r for r in conflation_results.keys() if r not in ["metrics", "ras_river_to_nwm_reaches_ratio"]]

    fim_stream = rfc.local_nwm_reaches()[rfc.local_nwm_reaches()["ID"].isin(ids)]

    plot_conflation_results(
        rfc,
        fim_stream,
        conflation_thumbnail_key,
        bucket=bucket,
        s3_client=client,
        limit_plot_to_nearby_reaches=limit_plot,
    )
    # Add thumbnail asset to item
    item.add_asset(
        "ThumbnailConflationResults",
        pystac.Asset(
            conflation_thumbnail_href,
            title="ThumbnailConflationResults",
            roles=[pystac.MediaType.PNG, "thumbnail"],
            extra_fields={
                "software": f"ripple1d {ripple1d.__version__}",
                "date_created": datetime.now().isoformat(),
            },
            description="""PNG of NWM conflation results with OpenStreetMap basemap.""",
        ),
    )

    client.put_object(Body=json.dumps(item.to_dict()).encode(), Bucket=bucket, Key=stac_item_s3_key)
