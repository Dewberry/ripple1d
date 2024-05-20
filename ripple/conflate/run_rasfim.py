from .rasfim import RasFimConflater
from .rasfim import (
    convert_linestring_to_points,
    calculate_conflation_metrics,
    nearest_line_to_point,
    conflation_summary,
    find_ds_most_branch,
    walk_branches,
    map_control_nodes_to_xs,
    find_flow_change_locations,
)


def point_method_conflation(rfc: RasFimConflater, river_reach_name: str) -> dict:
    """
    Conflate a single river reach with nwm branches
    Method 1: Nearest Points
    """

    xs_group = rfc.xs_by_river_reach_name(river_reach_name)

    # Buffer the ras centerline points to improve intersection with nwm branches
    ras_centerline_to_densified_points = rfc.ras_centerline_densified_points(
        river_reach_name
    )

    # Subset the branches to only those that are within 10 meters of the ras centerline points
    # This could use nearest instead of the buffer/interesct approach,
    candidate_branches = rfc.candidate_nwm_branches_via_point_buffer(
        ras_centerline_to_densified_points, 10
    )

    # Convert the ras centerline to points
    ras_points = convert_linestring_to_points(
        rfc.ras_centerlines.loc[0].geometry, crs=rfc.common_crs, point_spacing=10
    )

    # fim_stream = rfc.nwm_branches[rfc.nwm_branches["branch_id"].isin(candidate_branches)]
    # plot_conflation_results(rfc, candidate_branches, "test.png")

    # Find the cross sections that intersect the ras centerline and calculate conflation metrics
    return calculate_conflation_metrics(
        rfc,
        candidate_branches,
        xs_group,
        ras_points,
    )


def conflate_branches(rfc: RasFimConflater) -> dict:
    ras_start_point, ras_stop_point = rfc.ras_start_end_points()

    candidate_reaches = rfc.intersected_ras_river_nwm_branches()

    us_most_branch_id = nearest_line_to_point(candidate_reaches, ras_start_point)

    ds_most_branch_id = find_ds_most_branch(rfc, rfc.nwm_nodes, ras_stop_point)

    branch_info = walk_branches(rfc, us_most_branch_id, ds_most_branch_id)

    branch_info_with_ras_xs = map_control_nodes_to_xs(rfc, branch_info)

    flow_changes = find_flow_change_locations(branch_info_with_ras_xs)

    summary = conflation_summary(rfc, branch_info_with_ras_xs, flow_changes)

    return us_most_branch_id, ds_most_branch_id, summary
