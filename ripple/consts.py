from __future__ import annotations

MAP_DEM_UNCLIPPED_SRC_URL = (
    "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt"
)
MAP_DEM_CLIPPED_BASENAME = "ned13.tif"
MAP_DEM_BUFFER_DIST_FT = 1000.0
MAP_DEM_DIRNAME = "MapTerrain"
MAP_DEM_HDF_NAME = "MapTerrain.hdf"
MAP_DEM_VERT_UNITS = "Feet"

METERS_PER_FOOT = 1200.0 / 3937.0

STAC_API_URL = "https://stac.dewberryanalytics.com"

MINDEPTH = 0.1

DEFAULT_EPSG = 4269
NORMAL_DEPTH = 0.001

MIN_FLOW_FACTOR = 0.85
MAX_FLOW_FACTOR = 1.5

STAC_API_URL = "https://stac2.dewberryanalytics.com"

TERRAIN_NAME = "MapTerrain"
SUPPORTED_LAYERS = ["River", "XS"]

WSE_HDF_PATH = "/Results/Steady/Output/Output Blocks/Base Output/Steady Profiles/Cross Sections/Water Surface"
FLOW_HDF_PATH = "/Results/Steady/Output/Output Blocks/Base Output/Steady Profiles/Cross Sections/Flow"
XS_NAMES_HDF_PATH = "/Results/Steady/Output/Geometry Info/Cross Section Only"
PROFILE_NAMES_HDF_PATH = "/Results/Steady/Output/Output Blocks/Base Output/Steady Profiles/Profile Names"
