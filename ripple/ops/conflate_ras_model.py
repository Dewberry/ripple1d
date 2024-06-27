import json
import logging
from datetime import datetime
from pathlib import Path

import pystac

from ripple.conflate.plotter import plot_conflation_results
from ripple.conflate.rasfim import (
    RasFimConflater,
    nearest_line_to_point,
    ras_reaches_metadata,
    walk_network,
)
from ripple.consts import RIPPLE_VERSION
from ripple.stacio.utils.s3_utils import init_s3_resources

logging.getLogger("fiona").setLevel(logging.ERROR)
logging.getLogger("botocore").setLevel(logging.ERROR)


def href_to_vsis(href: str, bucket: str) -> str:
    return href.replace(f"https://{bucket}.s3.amazonaws.com", f"/vsis3/{bucket}")


def conflate(rfc: RasFimConflater):

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


def conflate_s3_model(item, client, bucket, stac_item_s3_key, rfc, river_reach_name):

    # build conflation key and href
    nwm_conflation_key = stac_item_s3_key.replace(".json", "-nwm_conflation.json")
    nwm_conflation_href = f"https://{bucket}.s3.amazonaws.com/{nwm_conflation_key}"

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
                "software": f"ripple {RIPPLE_VERSION}",
                "date_created": datetime.now().isoformat(),
            },
        ),
    )
    # for asset in item.get_assets():
    #     item.assets[asset].href = item.assets[asset].href.replace("https:/fim", "https://fim")
    limit_plot = True

    conflation_thumbnail_key = nwm_conflation_key.replace("json", "png")
    conflation_thumbnail_href = f"https://{bucket}.s3.amazonaws.com/{conflation_thumbnail_key}"

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
                "software": f"ripple {RIPPLE_VERSION}",
                "date_created": datetime.now().isoformat(),
            },
            description="""PNG of NWM conflation results with OpenStreetMap basemap.""",
        ),
    )

    client.put_object(Body=json.dumps(item.to_dict()).encode(), Bucket=bucket, Key=stac_item_s3_key)


# if __name__ == "__main__":

#     logging.basicConfig(level=logging.INFO, filename="conflate_with_stac-v1.log")

#     # parser = argparse.ArgumentParser(description="")
#     # parser.add_argument("--collection_id", type=str, required=True, help="Collection ID")

#     # args = parser.parse_args()
#     # collection_id = args.collection_id
#     bucket = "fim"

#     stac_item_href = "https://fim.s3.amazonaws.com/stac/dev2/Caney%20Creek-Lake%20Creek/BUMS%20CREEK/BUMS%20CREEK.json"
#     stac_item_s3_key = stac_item_href.replace(f"https://{bucket}.s3.amazonaws.com/", "").replace("%20", " ")
#     nwm_pq_path = r"C:\Users\mdeshotel\Downloads\nwm_flows_v3.parquet"

#     session, client, s3_resource = init_s3_resources()

#     item = pystac.Item.from_file(stac_item_href)

#     logging.info(item.id)

#     for asset in item.get_assets(role="ras-geometry-gpkg"):
#         gpkg_name = Path(item.assets[asset].href).name
#         ras_gpkg = href_to_vsis(item.assets[asset].href, bucket="fim")

#     rfc = RasFimConflater(nwm_pq_path, ras_gpkg)

#     for river_reach_name in rfc.ras_river_reach_names:
#         logging.info(f"item_id {item.id}, river_reach {river_reach_name}")

#     try:
#         conflate_s3_model(
#             item,
#             client,
#             bucket,
#             stac_item_s3_key,
#             rfc,
#             river_reach_name,
#         )
#         logging.info(f"{item.id}: Successfully processed")
#     except Exception as e:
#         logging.error(f"{item.id}: Error processing | {e}")
