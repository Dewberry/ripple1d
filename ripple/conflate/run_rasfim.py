import json
import boto3
import geopandas as gpd
import os
from pathlib import Path

# from .rasfim import RasFimConflater
from rasfim import RasFimConflater
from rasfim import (
    convert_linestring_to_points,
    compare_nearest_points,
    count_intersecting_lines,
)


import dotenv

dotenv.load_dotenv()

bucket = "fim"
nwm_gpkg = f"/vsis3/fim/mip/dev/branches.gpkg"
ras_gpkg = "/vsis3/fim/mip/dev/Crystal Creek-West Fork San Jacinto River/STEWARTS CREEK/STEWARTS CREEK.gpkg"

rfc = RasFimConflater(nwm_gpkg, ras_gpkg)

rfc.load_data()

# rfc.ras_river_reach_names

for river_reach_name in rfc.ras_river_reach_names:
    xs_group = rfc.xs_by_river_reach_name(river_reach_name)


for river_reach_name in rfc.ras_river_reach_names:
    buffered_ras_centerline_points = rfc.ras_centerline_densified_points(
        river_reach_name
    )

candidate_branches = rfc.candidate_nwm_branches(buffered_ras_centerline_points, 10)

ras_points = convert_linestring_to_points(rfc.ras_centerlines.loc[0].geometry)


next_round_candidates = []
total_hits = 0
for i in candidate_branches.index:
    candidate_branch_points = convert_linestring_to_points(
        candidate_branches.loc[i].geometry
    )
    if compare_nearest_points(candidate_branch_points, ras_points) < 2000:
        next_round_candidates.append(candidate_branches.loc[i]["branch_id"])
        gdftmp = gpd.GeoDataFrame(
            geometry=[candidate_branches.loc[i].geometry], crs=rfc.nwm_branches.crs
        )
        xs_hits = count_intersecting_lines(xs_group, gdftmp)
        total_hits += xs_hits

        print(total_hits, xs_group.shape[0])

conflation_score = total_hits / xs_group.shape[0]

if conflation_score == 1:
    print("Conflation complete")
elif conflation_score >= 0.25:
    print(f"Check: disconnected branches score: {conflation_score}")
elif conflation_score < 0.25:
    print(f"Fail: disconnected branches score: {conflation_score}")
elif conflation_score > 1:
    print(f"Fail: diverging branches score: {conflation_score}")
