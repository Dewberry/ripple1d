import json
import logging

import pandas as pd

from ripple.conflate.rasfim import (
    RasFimConflater,
    calculate_conflation_metrics,
    convert_linestring_to_points,
    nearest_line_to_point,
    ras_reaches_metadata,
    walk_network,
)
from ripple.ripple_logger import configure_logging


def main(
    rfc: RasFimConflater,
    low_flows: pd.DataFrame,
):

    configure_logging(logging.CRITICAL)

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

        # ras_points = convert_linestring_to_points(
        #     rfc.ras_centerlines.loc[0].geometry, crs=rfc.common_crs, point_spacing=10
        # )

        # rfc.ras_xs.fields.loc[0]
        # xs_group = rfc.xs_by_river_reach_name(river_reach_name)
        # metrics = calculate_conflation_metrics(
        #     rfc,
        #     candidate_reaches,
        #     xs_group,
        #     ras_points,
        # )

        # ras_to_fim_length_ratio = rfc.ras_centerlines.loc[0].geometry.length / candidate_reaches.geometry.length.sum()

        reach_metadata = ras_reaches_metadata(rfc, low_flows, candidate_reaches)
        # reach_metadata["ras_river_to_nwm_reaches_ratio"] = ras_to_fim_length_ratio
        # reach_metadata["metrics"] = metrics

        metadata.update(reach_metadata)

    return metadata


if __name__ == "__main__":
    # wkdir = "/Users/slawler/repos/ripple"
    ras_gpkg_path = r"s3://fim/mip/dev2/Caney Creek-Lake Creek/BUMS CREEK/BUMS CREEK.gpkg"

    nwm_pq_path = r"C:\Users\mdeshotel\Downloads\nwm_flows_v3.parquet"
    low_flows = pd.read_parquet(nwm_pq_path)

    conflation_output = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\mip_models/bums_creek-ripple-params.json"

    rfc = RasFimConflater(
        nwm_pq_path,
        ras_gpkg_path,
    )

    results = main(rfc, low_flows)

    with open(conflation_output, "w") as f:
        f.write(json.dumps(results, indent=4))
