import json
import os
import unittest
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import LineString, MultiLineString, Point, Polygon

from ripple.conflate.rasfim import (
    RasFimConflater,
    cacl_avg_nearest_points,
    count_intersecting_lines,
    endpoints_from_multiline,
    filter_gdf,
    nearest_line_to_point,
)
from ripple.conflate.run_rasfim import main

TEST_DIR = os.path.dirname(__file__)
TEST_ITEM_FILE = "ras-data/baxter.json"
TEST_ITEM_PATH = os.path.join(TEST_DIR, TEST_ITEM_FILE)

# Expected counts
NWM_REACHES = 13
RAS_CENTERLINES = 3
RAS_XS = 173
GAGES = 1

# Other expected data
RIVER_REACHES = ["Baxter River, Upper Reach", "Tule Creek, Tributary", "Baxter River, Lower Reach"]
LOW_FLOW_DATA = "high_water_threshold.parquet"
NWM_REACHES_DATA = "flow_paths.parquet"
RAS_DIR = "Baxter"
RAS_GEOMETRY_GPKG = "Baxter.gpkg"


@pytest.fixture(scope="class")
def setup_data(request):
    nwm_pq_path = os.path.join(TEST_DIR, "nwm-data", NWM_REACHES_DATA)
    ras_gpkg_path = os.path.join(TEST_DIR, "ras-data", RAS_DIR, RAS_GEOMETRY_GPKG)
    conflater = RasFimConflater(nwm_pq_path, ras_gpkg_path)
    request.cls.conflater = conflater


@pytest.mark.usefixtures("setup_data")
class TestRasFimConflater(unittest.TestCase):

    def test_load_data(self):
        self.conflater.load_data()
        self.assertEqual(self.conflater._nwm_reaches.shape[0], NWM_REACHES)
        self.assertEqual(self.conflater._ras_centerlines.shape[0], RAS_CENTERLINES)
        self.assertEqual(self.conflater._ras_xs.shape[0], 173)

    def test_ras_centerlines_exist(self):
        centerlines = self.conflater.ras_centerlines
        self.assertEqual(centerlines.shape[0], RAS_CENTERLINES)

    def test_ras_river_reach_names_exist(self):
        reach_names = self.conflater.ras_river_reach_names
        self.assertEqual(reach_names, RIVER_REACHES)
        self.assertEqual(len(reach_names), RAS_CENTERLINES)

    def test_ras_xs_exist(self):
        ras_xs = self.conflater.ras_xs
        self.assertEqual(ras_xs.shape[0], RAS_XS)

    def test_ras_xs_bbox_is_polygon(self):
        bbox = self.conflater.ras_xs_bbox
        self.assertIsInstance(bbox, Polygon)

    def test_nwm_reaches_exist(self):
        nwm_reaches = self.conflater.nwm_reaches
        self.assertEqual(nwm_reaches.shape[0], NWM_REACHES)

    def test_local_nwm_reaches_exist(self):
        # TODO: update test data to include an extra few reaches
        local_reaches = self.conflater.local_nwm_reaches
        self.assertEqual(local_reaches.shape[0], NWM_REACHES)

    def test_local_gages_exist(self):
        gages = self.conflater.local_gages
        self.assertEqual(len(gages), GAGES)

    # geospatial operations
    def test_endpoints_from_multiline(self):
        mline = MultiLineString([LineString([(0, 0), (1, 1)]), LineString([(1, 1), (2, 2)])])
        start, end = endpoints_from_multiline(mline)
        self.assertEqual(start, Point(0, 0))
        self.assertEqual(end, Point(2, 2))

    def test_nearest_line_to_point(self):
        lines = gpd.GeoDataFrame({"geometry": [LineString([(0, 0), (1, 1)])], "ID": [1]})
        point = Point(0.5, 0.5)
        line_id = nearest_line_to_point(lines, point)
        self.assertEqual(line_id, 1)

    def test_cacl_avg_nearest_points(self):
        reference_gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0), Point(1, 1)]})
        compare_points_gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0), Point(1, 1), Point(2, 2)]})
        avg_distance = cacl_avg_nearest_points(reference_gdf, compare_points_gdf)
        self.assertAlmostEqual(avg_distance, 0.0)

    def test_count_intersecting_lines(self):
        ras_xs = gpd.GeoDataFrame({"geometry": [LineString([(0, 0), (1, 1)])]}, crs="EPSG:4326")
        nwm_reaches = gpd.GeoDataFrame({"geometry": [LineString([(0, 0), (1, 1)])]}, crs="EPSG:4326")
        count = count_intersecting_lines(ras_xs, nwm_reaches)
        self.assertEqual(count.shape[0], 1)

    def test_filter_gdf(self):
        gdf = gpd.GeoDataFrame({"id": [1, 2, 3], "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)]})
        filtered_gdf = filter_gdf(gdf, [1, 2])
        self.assertEqual(filtered_gdf.shape[0], 1)
        self.assertEqual(filtered_gdf.iloc[0]["id"], 3)


@pytest.mark.usefixtures("setup_data")
class TestConflationExample(unittest.TestCase):

    def setUp(self):
        self.conflater.load_data()
        self.low_flows = pd.read_parquet(os.path.join(TEST_DIR, "nwm-data", LOW_FLOW_DATA))

    def test_main_function(self):
        metadata = main(self.conflater, self.low_flows)
        for reach in RIVER_REACHES:
            self.assertIn(reach, metadata)

        for reach in RIVER_REACHES:
            test_data_results = os.path.join(
                TEST_DIR, "ras-data", RAS_DIR, f"{reach.replace(',','').replace(' ','')}-ripple-params.json"
            )
            with open(test_data_results, "r") as f:
                expected_metadata = f.read()
                self.assertEqual(json.dumps(metadata[reach], indent=4), expected_metadata)