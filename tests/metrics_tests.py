import os
import unittest

import geopandas as gpd
import pytest

from ripple1d.ops.metrics import ConflationMetrics, combine_reaches, compute_conflation_metrics
from ripple1d.ops.subset_gpkg import RippleGeopackageSubsetter

# Expected counts
NWM_REACHES = 1
LOCAL_NWM_REACHES = 18
SRC_RAS_CENTERLINES = 3
SRC_RAS_XS = 173
SRC_RAS_STRUCTURES = 7
RIPPLE_RAS_CENTERLINES = 1
RIPPLE_RAS_XS = 51
RIPPLE_RAS_STRUCTURES = 2
NWM_REACH_COORDS_LEN = 96

# Other expected data
US_RIVER = "Baxter River"
US_REACH = "Upper Reach"
US_XS_ID = 78658.0
DS_RIVER = "Baxter River"
DS_REACH = "Lower Reach"
DS_XS_ID = 47694.0
INTERSECTION_DELTA_XY_MEAN = 23.0
THALWEG_INTERSECTION_DELTA_XY_MEAN = 458.0
RAS_REACH_LENGTH_MEAN = 188.0
NWM_REACH_LENGTH_MEAN = 186.0
NWM_TO_RAS_RATIO_MEAN = 0.99

TEST_DIR = os.path.dirname(__file__)

NWM_REACHES_DATA = "flows.parquet"
RAS_DIR = "Baxter"
RAS_GEOMETRY_GPKG = "Baxter.gpkg"
CONFLATION_JSON = "Baxter.conflation.json"
NWM_REACH_ID = 2823960
NWM_TO_ID = 2823972
NWM_CRS = 5070


@pytest.fixture(scope="class")
def setup_data(request):
    nwm_pq_path = os.path.join(TEST_DIR, "nwm-data", NWM_REACHES_DATA)
    src_gpkg_path = os.path.join(TEST_DIR, "ras-data", RAS_DIR, RAS_GEOMETRY_GPKG)
    conflation_json = os.path.join(TEST_DIR, "ras-data", RAS_DIR, CONFLATION_JSON)
    rgs = RippleGeopackageSubsetter(src_gpkg_path, conflation_json, "", str(NWM_REACH_ID))

    layers = {}
    for layer, gdf in rgs.subset_gdfs.items():
        layers[layer] = gdf.to_crs(NWM_CRS)

    nwm_reaches = gpd.read_parquet(nwm_pq_path, bbox=layers["XS"].total_bounds)
    nwm_reach = combine_reaches(nwm_reaches, NWM_REACH_ID)

    cm = ConflationMetrics(layers["XS"], layers["River"], nwm_reach)
    request.cls.rgs = rgs
    request.cls.cm = cm
    request.cls.nwm_reaches = nwm_reaches
    request.cls.nwm_reach = nwm_reach
    request.cls.nwm_to_id = NWM_TO_ID
    request.cls.nwm_id = NWM_REACH_ID


@pytest.mark.usefixtures("setup_data")
class TestRippleGeopackageSubsetter(unittest.TestCase):
    def test_load_data(self):
        self.rgs.ripple1d_parameters
        self.rgs.source_xs
        self.rgs.source_river
        self.rgs.source_structure

    def test_src_ras_centerlines_exist(self):
        self.assertEqual(self.rgs.source_river.shape[0], SRC_RAS_CENTERLINES)

    def test_src_ras_xs_exist(self):
        self.assertEqual(self.rgs.source_xs.shape[0], SRC_RAS_XS)

    def test_src_ras_structures_exist(self):
        self.assertEqual(self.rgs.source_structure.shape[0], SRC_RAS_STRUCTURES)

    def test_ripple_xs_exist(self):
        self.assertEqual(self.rgs.ripple_xs.shape[0], RIPPLE_RAS_XS)

    def test_ripple_river_exist(self):
        self.assertEqual(self.rgs.ripple_river.shape[0], RIPPLE_RAS_CENTERLINES)

    def test_ripple_structures_exist(self):
        self.assertEqual(self.rgs.ripple_structure.shape[0], RIPPLE_RAS_STRUCTURES)

    def test_us_river(self):
        self.assertEqual(self.rgs.us_river, US_RIVER)

    def test_us_reach(self):
        self.assertEqual(self.rgs.us_reach, US_REACH)

    def test_us_rs(self):
        self.assertEqual(self.rgs.us_rs, US_XS_ID)

    def test_ds_river(self):
        self.assertEqual(self.rgs.ds_river, DS_RIVER)

    def test_ds_reach(self):
        self.assertEqual(self.rgs.ds_reach, DS_REACH)

    def test_ds_rs(self):
        self.assertEqual(self.rgs.ds_rs, DS_XS_ID)


@pytest.mark.usefixtures("setup_data")
class TestConflationMetrics(unittest.TestCase):
    def test_nwm_reaches_exist(self):
        nwm_reaches = self.nwm_reaches.loc[self.nwm_reaches["ID"] == int(self.nwm_id), :]
        self.assertEqual(nwm_reaches.shape[0], NWM_REACHES)
        self.assertEqual(len(self.nwm_reach.coords), NWM_REACH_COORDS_LEN)

    def test_nwm_to_id(self):
        to_id = self.nwm_reaches.loc[self.nwm_reaches["ID"] == int(self.nwm_id), "to_id"].iloc[0]
        self.assertEqual(to_id, self.nwm_to_id)

    def test_xs(self):
        self.assertEqual(self.cm.xs_gdf.shape[0], RIPPLE_RAS_XS)

    def test_thalweg_metrics(self):
        self.assertEqual(
            self.cm.thalweg_metrics(self.rgs.ripple_xs.to_crs(NWM_CRS))["intersection_delta_xy"]["mean"],
            INTERSECTION_DELTA_XY_MEAN,
        )
        self.assertEqual(
            self.cm.thalweg_metrics(self.rgs.ripple_xs.to_crs(NWM_CRS))["thalweg_delta_xy"]["mean"],
            THALWEG_INTERSECTION_DELTA_XY_MEAN,
        )

    def test_reach_length_metrics(self):

        self.assertEqual(
            self.cm.reach_length_metrics(self.rgs.ripple_xs.to_crs(NWM_CRS))["ras"]["mean"], RAS_REACH_LENGTH_MEAN
        )
        self.assertEqual(
            self.cm.reach_length_metrics(self.rgs.ripple_xs.to_crs(NWM_CRS))["nwm"]["mean"], NWM_REACH_LENGTH_MEAN
        )
        self.assertEqual(
            self.cm.reach_length_metrics(self.rgs.ripple_xs.to_crs(NWM_CRS))["nwm_to_ras_ratio"]["mean"],
            NWM_TO_RAS_RATIO_MEAN,
        )

    # def test_coverage_metrics(self):
    #     cm.coverage_metrics(layers["XS"])
