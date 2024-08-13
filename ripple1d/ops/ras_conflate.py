"""Conflate HEC-RAS Model."""

import json
import logging
from datetime import datetime
from urllib.parse import quote

import boto3
import pystac

from ripple1d.conflate.plotter import plot_conflation_results
from ripple1d.conflate.rasfim import (
    RasFimConflater,
    nearest_line_to_point,
    ras_reaches_metadata,
    walk_network,
)
from ripple1d.consts import RIPPLE_VERSION

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
    for river_reach_name in rfc.ras_river_reach_names:
        ras_start_point, ras_stop_point = rfc.ras_start_end_points(river_reach_name=river_reach_name)
        us_most_reach_id = nearest_line_to_point(rfc.local_nwm_reaches, ras_start_point)
        ds_most_reach_id = nearest_line_to_point(rfc.local_nwm_reaches, ras_stop_point)

        potential_reach_path = walk_network(rfc.local_nwm_reaches, us_most_reach_id, ds_most_reach_id)
        candidate_reaches = rfc.local_nwm_reaches.query(f"ID in {potential_reach_path}")
        if len(candidate_reaches.query(f"ID == {nwm_reach_id}")) == 1:
            nwm_reach_id_identified = True
            return ras_reaches_metadata(rfc, candidate_reaches[candidate_reaches["ID"] == nwm_reach_id])
    if not nwm_reach_id_identified:
        raise ValueError(f"nwm_reach_id {nwm_reach_id} not conflating to the ras model geometry.")


def conflate(rfc: RasFimConflater):
    """Conflate a HEC-RAS model with NWM reaches."""
    metadata = {}
    for river_reach_name in rfc.ras_river_reach_names:
        # logging.info(f"Processing {river_reach_name}")
        ras_start_point, ras_stop_point = rfc.ras_start_end_points(river_reach_name=river_reach_name)
        # TODO: Add check / alt method for when ras_start_point is associated  with the wrong reach
        us_most_reach_id = nearest_line_to_point(rfc.local_nwm_reaches, ras_start_point)
        ds_most_reach_id = nearest_line_to_point(rfc.local_nwm_reaches, ras_stop_point)
        logging.debug(
            f"{river_reach_name} | us_most_reach_id ={us_most_reach_id} and ds_most_reach_id = {ds_most_reach_id}"
        )

        potential_reach_path = walk_network(rfc.local_nwm_reaches, us_most_reach_id, ds_most_reach_id)
        candidate_reaches = rfc.local_nwm_reaches.query(f"ID in {potential_reach_path}")
        reach_metadata = ras_reaches_metadata(rfc, candidate_reaches)
        metadata.update(reach_metadata)

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

    # Add ripple1d parameters asset to item
    item.add_asset(
        "NWM_Conflation",
        pystac.Asset(
            nwm_conflation_href,
            title="NWM_Conflation",
            roles=[pystac.MediaType.JSON, "nwm-conflation"],
            extra_fields={
                "software": f"ripple1d {RIPPLE_VERSION}",
                "date_created": datetime.now().isoformat(),
            },
        ),
    )

    limit_plot = True

    conflation_thumbnail_key = nwm_conflation_key.replace("json", "png")
    conflation_thumbnail_href = s3_public_href(bucket, conflation_thumbnail_key)

    ids = [r for r in conflation_results.keys() if r not in ["metrics", "ras_river_to_nwm_reaches_ratio"]]

    fim_stream = rfc.local_nwm_reaches[rfc.local_nwm_reaches["ID"].isin(ids)]

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
                "software": f"ripple1d {RIPPLE_VERSION}",
                "date_created": datetime.now().isoformat(),
            },
            description="""PNG of NWM conflation results with OpenStreetMap basemap.""",
        ),
    )

    client.put_object(Body=json.dumps(item.to_dict()).encode(), Bucket=bucket, Key=stac_item_s3_key)
