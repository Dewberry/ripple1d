import pandas as pd

from ripple.conflate.rasfim import (
    RasFimConflater,
    calculate_conflation_metrics,
    convert_linestring_to_points,
    nearest_line_to_point,
    ras_reaches_metadata,
    walk_network,
)

# from ripple.conflate.rasfim2 import (
#     RasFimConflater,
#     calculate_conflation_metrics,
#     convert_linestring_to_points,
#     nearest_line_to_point,
#     ras_reaches_metadata,
#     walk_network,
# )


def main(
    rfc: RasFimConflater,
    low_flows: pd.DataFrame,
):

    ras_start_point, ras_stop_point = rfc.ras_start_end_points()
    us_most_reach_id = nearest_line_to_point(rfc.local_nwm_reaches, ras_start_point)
    ds_most_reach_id = nearest_line_to_point(rfc.local_nwm_reaches, ras_stop_point)

    potential_reach_path = walk_network(
        rfc.local_nwm_reaches, us_most_reach_id, ds_most_reach_id
    )

    candidate_reaches = rfc.local_nwm_reaches.query(f"ID in {potential_reach_path}")

    ras_points = convert_linestring_to_points(
        rfc.ras_centerlines.loc[0].geometry, crs=rfc.common_crs, point_spacing=10
    )

    for river_reach_name in rfc.ras_river_reach_names:
        print(river_reach_name)

    # xs_group = rfc.xs_by_river_reach_name(river_reach_name)
    xs_group = rfc.ras_xs

    metrics = calculate_conflation_metrics(
        rfc,
        candidate_reaches,
        xs_group,
        ras_points,
    )

    # rfc.local_nwm_reaches.index
    ras_to_fim_length_ratio = (
        rfc.ras_centerlines.loc[0].geometry.length
        / candidate_reaches.geometry.length.sum()
    )

    metadata = ras_reaches_metadata(rfc, low_flows, candidate_reaches)
    metadata["ras_river_to_nwm_reaches_ratio"] = ras_to_fim_length_ratio
    metadata["metrics"] = metrics
    return metadata
