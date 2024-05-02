import geopandas as gpd
from shapely.ops import nearest_points
from shapely.geometry import Point, MultiLineString
import boto3
import json
import pystac

MIN_FLOW_FACTOR = 0.85
MAX_FLOW_FACTOR = 1.5

root_dir = "/Users/slawler/repos/ripple/"

# NWM Data
nwm_gpkg = f"{root_dir}/nwm_branches.gpkg"  # exported from QGIS to retain branch_id
branches = gpd.read_file(nwm_gpkg, layer="branches")

nwm_gpkg_no_ids = f"{root_dir}/branches.gpkg"  # original
nodes = gpd.read_file(nwm_gpkg_no_ids, layer="nodes")

# Buffer nwm nodes to identify the main stem reaches
buffered_nodes = nodes.copy()
buffered_nodes.geometry = nodes.geometry.buffer(250)

# RAS Data
ras_gpkg = f"{root_dir}/WFSJ Main.gpkg"

ras_centerline = gpd.read_file(ras_gpkg, layer="Rivers")
ras_centerline.to_crs(nodes.crs, inplace=True)

ras_xs = gpd.read_file(ras_gpkg, layer="XS")
ras_xs.to_crs(nodes.crs, inplace=True)

# ras_banks = gpd.read_file(ras_gpkg, layer="Banks")
# ras_banks.to_crs(nodes.crs, inplace=True)


# Intersect the buffered nodes with the ras centerline
intersected_nodes_ras_river = gpd.sjoin(buffered_nodes, ras_centerline, how="inner", op="intersects")

# Get intersecting reaches. branch_id in the nodes later = the branch_id in the nwm reach layer corresponding
# to the reach downstream of the node with the same branch_id.
intersecting_reaches = [int(v) for v in intersected_nodes_ras_river.branch_id.values]

# Filter NWM branches to only those that intersect the ras river
candidate_reaches = branches[branches["branch_id"].isin(intersecting_reaches)]

# candidate_reaches.to_file(f"{root_dir}/intersected_branches.gpkg", layer="nwm_reaches", driver="GPKG")

# Search line segments (reaches) for connected segments and drop any dangling endpoints
start_points = set(row.geometry.coords[0] for row in candidate_reaches.itertuples())
end_points = set(row.geometry.coords[-1] for row in candidate_reaches.itertuples())

connected_reaches = []
for _, row in candidate_reaches.iterrows():
    start_point = row.geometry.coords[0]
    end_point = row.geometry.coords[-1]

    if end_point in start_points:
        connected_reaches.append(row)

column_names = candidate_reaches.columns
connected_reaches_gdf = gpd.GeoDataFrame(connected_reaches, columns=column_names)
connected_reaches_gdf.set_geometry("geometry", inplace=True, crs=nodes.crs)
# connected_reaches_gdf.plot()
# connected_reaches_gdf.to_file(f"{root_dir}/intersected_branches.gpkg", layer="flow_changes", driver="GPKG")

nearest_xs = {}
meta_data = []
xs_multipoint = ras_xs.geometry.unary_union

for reach in connected_reaches_gdf.itertuples():

    # get start/end points of the reach
    end_point = reach.geometry.coords[-1]
    start_point = reach.geometry.coords[0]

    # compute the distance from each cross section to the reach start/end point
    end_distances = ras_xs.distance(Point(end_point))
    start_distances = ras_xs.distance(Point(start_point))

    nearest_ds_xs_index = end_distances.idxmin()
    nearest_us_xs_index = start_distances.idxmin()
    if nearest_ds_xs_index not in nearest_xs.values():

        ds_xs_id = ras_xs.loc[nearest_ds_xs_index].id
        us_xs_id = ras_xs.loc[nearest_us_xs_index].id

        meta_data.append(
            {
                "branch_id": str(reach.branch_id),
                "min_flow_cfs": round(reach.flow_2_yr * MIN_FLOW_FACTOR),
                "max_flow_cfs": round(reach.flow_100_yr * MAX_FLOW_FACTOR),
                "control_by_node": str(int(reach.control_by_node)),
                "nearest_xs_ds": ds_xs_id,
                "nearest_xs_us": us_xs_id,
            }
        )


# get the min elevation and max elevation for each cross section that is nearest the reach end points
for idx, item in enumerate(meta_data):
    xs_id = item["nearest_xs_ds"]
    xs = ras_xs[ras_xs["id"] == xs_id]

    multiline = xs.geometry.values[0]
    if isinstance(multiline, MultiLineString):
        for linestring in multiline.geoms:
            meta_data[idx]["min_elevation"] = min([p[2] for p in linestring.coords])
            meta_data[idx]["max_elevation"] = max([p[2] for p in linestring.coords])


# Add metadata to STAC Item

#  Fetch STAC Item
ras_item = "https://stac.dewberryanalytics.com/collections/huc-12040101/items/WFSJ_Main-cd42"
item = pystac.Item.from_file(ras_item)

# Update STAC ITEM
item.properties["Ripple:NWM_Conflation"] = meta_data

# Write to S3
s3 = boto3.client("s3")
bucket = "fim"
dev_key = f"stac/ripple/WFSJ_Main-cd42.json"

item.set_self_href(f"https://fim.s3.amazonaws.com/{dev_key}")

s3.put_object(Body=json.dumps(item.to_dict()), Bucket=bucket, Key=dev_key)

# https://radiantearth.github.io/stac-browser/#/https://fim.s3.amazonaws.com/stac/ripple/WFSJ_Main-cd42.json?.language=en
