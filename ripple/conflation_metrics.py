import json

import geopandas as gpd
import pandas as pd
from geopandas.tools import sjoin
from shapely.geometry import LineString, Point
from shapely.ops import unary_union


class CoverageCalculator:
    """Read and load in files"""

    def __init__(self, json_data_path, parquet_data_path, gpkg_data_path, river_id):
        self.parq = pd.read_parquet(parquet_data_path)
        with open(json_data_path, "r") as f:
            self.json_data = json.load(f)
        self.xs = gpd.read_file(gpkg_data_path, layer="XS")
        self.test = gpd.read_file(gpkg_data_path, layer="XS")  # Specify the correct layer
        self.river = gpd.read_file(gpkg_data_path, layer="River")
        self.river_id = river_id

    def cut(self, line, distance):
        """Cut reaches in thirds."""
        if distance <= 0.0 or distance >= line.length:
            return [LineString(line)]
        coords = list(line.coords)
        for i, p in enumerate(coords):
            pd = line.project(Point(p))
            if pd == distance:
                return [LineString(coords[: i + 1]), LineString(coords[i:])]
            if pd > distance:
                cp = line.interpolate(distance)
                return [LineString(coords[:i] + [(cp.x, cp.y)]), LineString([(cp.x, cp.y)] + coords[i:])]

    def split_river_into_thirds(self, baxter_reach):
        """Cut river into thirds."""
        linestring = baxter_reach.geometry.iloc[0]
        total_length = linestring.length

        third1, remaining = self.cut(linestring, total_length / 3)
        third2, third3 = self.cut(remaining, total_length / 3)

        bu_thirds = gpd.GeoDataFrame(geometry=[third1, third2, third3], crs=baxter_reach.crs)
        bu_thirds["section"] = ["upstream", "middle", "downstream"]

        bu_thirds_buffered = bu_thirds.copy()
        bu_thirds_buffered.geometry = bu_thirds.geometry.buffer(0.0001)

        return bu_thirds_buffered

    def calculate_coverage(self):
        """Calculate how much of the river is covered by crosssections."""
        parq_id = self.river_id
        new_test = self.parq[self.parq["ID"] == parq_id]

        json_us_xsid = self.json_data[str(parq_id)]["us_xs"]["xs_id"]
        json_ds_xsid = self.json_data[str(parq_id)]["ds_xs"]["xs_id"]
        us_filtered = self.xs[self.xs["river_station"] == float(json_us_xsid)]
        ds_filtered = self.xs[self.xs["river_station"] == float(json_ds_xsid)]

        if us_filtered["reach"].values[0] != ds_filtered["reach"].values[0]:
            reach_us = us_filtered["reach"].values[0]
            reach_ds = ds_filtered["reach"].values[0]
            riv_filtered = self.test[
                (self.test["reach"].isin([reach_us, reach_ds]))
                & (self.test["river_station"].isin([float(json_us_xsid), float(json_ds_xsid)]))
            ]
        else:
            riv_filtered = self.test[self.test["river_station"] == float(json_us_xsid)]

        reaches_in_riv_filtered = riv_filtered["reach"].unique()
        baxter_reach = self.river[self.river["reach"].isin(reaches_in_riv_filtered)]

        combined_baxter_reach = baxter_reach.geometry.unary_union

        us_geom = us_filtered.geometry.unary_union
        ds_geom = ds_filtered.geometry.unary_union

        # Validate geometries
        if not us_geom.is_valid:
            us_geom = us_geom.buffer(0)
        if not ds_geom.is_valid:
            ds_geom = ds_geom.buffer(0)
        if not combined_baxter_reach.is_valid:
            combined_baxter_reach = combined_baxter_reach.buffer(0)

        us_point = combined_baxter_reach.intersection(us_geom)
        ds_point = combined_baxter_reach.intersection(ds_geom)

        us_distance = combined_baxter_reach.project(us_point)
        ds_distance = combined_baxter_reach.project(ds_point)
        distance = abs(us_distance - ds_distance)

        us_point_gdf = gpd.GeoDataFrame(geometry=[us_point], crs=baxter_reach.crs)
        ds_point_gdf = gpd.GeoDataFrame(geometry=[ds_point], crs=baxter_reach.crs)

        total_length_baxter_reach = combined_baxter_reach.length
        pct_coverage = (distance / total_length_baxter_reach) * 100

        bu_thirds_buffered = self.split_river_into_thirds(baxter_reach)

        # Ensure CRS consistency
        target_crs = "EPSG:2227"
        if us_point_gdf.crs != target_crs:
            us_point_gdf = us_point_gdf.to_crs(target_crs)
        if ds_point_gdf.crs != target_crs:
            ds_point_gdf = ds_point_gdf.to_crs(target_crs)
        if bu_thirds_buffered.crs != target_crs:
            bu_thirds_buffered = bu_thirds_buffered.to_crs(target_crs)

        """Figure out which part of the river is covered by xs"""
        us_point_gdf = sjoin(us_point_gdf, bu_thirds_buffered, how="left", predicate="intersects")
        ds_point_gdf = sjoin(ds_point_gdf, bu_thirds_buffered, how="left", predicate="intersects")

        return {
            "pct_coverage": float(round(pct_coverage, 2)),
            "upstream_xs": us_point_gdf["section"].iloc[0],
            "ds_point_gdf": ds_point_gdf["section"].iloc[0],
        }
