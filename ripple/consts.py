"""Constants used throughout."""

from collections import OrderedDict

MAP_DEM_UNCLIPPED_SRC_URL = (
    "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt"
)

MAP_DEM_BUFFER_DIST_FT = 1000.0
MAP_DEM_DIRNAME = "MapTerrain"
MAP_DEM_HDF_NAME = "MapTerrain.hdf"
MAP_DEM_VERT_UNITS = "Meters"

METERS_PER_FOOT = 1200.0 / 3937.0

MINDEPTH = 0.1  # ft
MIN_FLOW = 1  # cfs

DEFAULT_EPSG = 4269
NORMAL_DEPTH = 0.001

STAC_API_URL = "https://stac2.dewberryanalytics.com"

TERRAIN_NAME = "Terrain_Name"
TERRAIN_PATH = "Terrain_Path"
SUPPORTED_LAYERS = ["River", "XS"]

WSE_HDF_PATH = "/Results/Steady/Output/Output Blocks/Base Output/Steady Profiles/Cross Sections/Water Surface"
FLOW_HDF_PATH = "/Results/Steady/Output/Output Blocks/Base Output/Steady Profiles/Cross Sections/Flow"
XS_NAMES_HDF_PATH = "/Results/Steady/Output/Geometry Info/Cross Section Only"
PROFILE_NAMES_HDF_PATH = "/Results/Steady/Output/Output Blocks/Base Output/Steady Profiles/Profile Names"

LAYER_COLORS = OrderedDict(
    {
        "Banks": "red",
        "Junction": "red",
        "BCLines": "brown",
        "BreakLines": "black",
        "Connections": "cyan",
        "HydraulicStructures": "magenta",
        "Mesh": "yellow",
        "River": "blue",
        "StorageAreas": "orange",
        "TwoDAreas": "purple",
        "XS": "green",
    }
)

RIPPLE_VERSION = "0.0.1"
SHOW_RAS = False
