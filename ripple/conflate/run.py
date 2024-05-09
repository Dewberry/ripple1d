import json
import boto3
import os
from pathlib import Path

from .ras1d import RasFimConflater
from .ras1d import (
    nearest_line_to_point,
    conflation_summary,
    find_ds_most_branch,
    walk_branches,
    map_control_nodes_to_xs,
    find_flow_change_locations,
    nwm_conflated_reaches,
)

from .plotter import plot_conflation_results


def main(rfc: RasFimConflater) -> dict:

    ras_start_point, ras_stop_point = rfc.ras_start_end_points

    candidate_reaches = rfc.intersected_ras_river_nwm_branches()

    us_most_branch_id = nearest_line_to_point(candidate_reaches, ras_start_point)

    ds_most_branch_id = find_ds_most_branch(rfc, rfc.nwm_nodes, ras_stop_point)

    branch_info = walk_branches(rfc, us_most_branch_id, ds_most_branch_id)

    branch_info_with_ras_xs = map_control_nodes_to_xs(rfc, branch_info)

    flow_changes = find_flow_change_locations(branch_info_with_ras_xs)

    summary = conflation_summary(rfc, branch_info_with_ras_xs, flow_changes)

    return us_most_branch_id, ds_most_branch_id, summary


if __name__ == "__main__":

    import dotenv

    dotenv.load_dotenv()

    bucket = "fim"
    nwm_gpkg = f"/vsis3/fim/mip/dev/branches.gpkg"
    ras_gpkg = "/vsis3/fim/mip/dev/Crystal Creek-West Fork San Jacinto River/STEWARTS CREEK/STEWARTS CREEK.gpkg"

    conflation_thumbnail = Path(ras_gpkg).parent / "conflation.png"
    ripple_parameters = Path(ras_gpkg).parent / "ripple_parameters.json"

    rfc = RasFimConflater(nwm_gpkg, ras_gpkg)
    us_most_branch_id, ds_most_branch_id, summary = main(rfc)

    fim_stream = nwm_conflated_reaches(rfc, summary)

    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    client = session.client("s3")
    plot_conflation_results(
        rfc,
        fim_stream,
        str(conflation_thumbnail).replace("/vsis3/", "s3://"),
        bucket="fim",
        s3_client=client,
    )
    client.put_object(
        Body=json.dumps(summary).encode(),
        Bucket=bucket,
        Key=str(ripple_parameters).replace("/vsis3/", "s3://"),
    )
