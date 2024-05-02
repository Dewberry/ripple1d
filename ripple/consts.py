HDFGEOMETRIES = {
    "Cross Sections": {"shape": "Line", "poly": "Polyline", "RGB": [0, 255, 0, 100]},
    "River Centerlines": {"shape": "Line", "poly": "Polyline", "RGB": [0, 0, 255, 100]},
    "River Bank Lines": {"shape": "Line", "poly": "Polyline", "RGB": [255, 0, 0, 100]},
    "River Flow Paths": {
        "shape": "Line",
        "poly": "Flow Path Lines",
        "RGB": [0, 255, 255, 100],
    },
    "2D Flow Areas": {"shape": "Polygon", "poly": "Polygon", "RGB": [0, 0, 255, 50]},
    "Boundary Condition Lines": {
        "shape": "Line",
        "poly": "Polyline",
        "RGB": [0, 255, 255, 100],
    },
    "2D Flow Area Break Lines": {
        "shape": "Line",
        "poly": "Polyline",
        "RGB": [128, 0, 0, 100],
    },
    "Reference Lines": {"shape": "Line", "poly": "Polyline", "RGB": [255, 191, 0, 100]},
    "Structures": {"shape": "Line", "poly": "Centerline", "RGB": [128, 128, 128, 100]},
    "Reference Points": {
        "shape": "Point",
        "poly": "Polyline",
        "RGB": [255, 191, 0, 100],
    },
    "IC Points": {"shape": "Point", "poly": "Polyline", "RGB": [0, 255, 255, 100]},
    "Junctions": {"shape": "Point", "poly": "Polyline", "RGB": [255, 0, 0, 100]},
    "2D Flow Area Refinement Regions": {
        "shape": "Polygon",
        "poly": "Polygon",
        "RGB": [128, 0, 32, 100],
    },
}

PLOTTINGSTRUCTURES = {
    "2D Bridges": "Structure Variables",
    "Bridges": "Bridge Variables",
    "Culverts": "Culvert Variables",
    "Inline Structures": "Structure Variables",
    "Lateral Structures": "Structure Variables",
    "Multiple Openings": "Multiple Opening Variables",
    "SA 2D Area Conn": "Structure Variables",
}
PLOTTINGREFERENCE = {
    "Cross Sections": {
        "name": "Cross Section Only",
        "variables": ["Flow", "Water Surface"],
    },
    "Reference Lines": {"name": "Name", "variables": ["Flow", "Water Surface"]},
    "Reference Points": {"name": "Name", "variables": ["Water Surface"]},
}

STRUCTURGROUPS = {
    "Culvert": "Culverts",
    "Bridge": "Bridges",
    "Lateral": "Lateral Structures",
    "Inline": "Inline Structures",
    "Multiple Opening": "Multiple Opeings",
    "Connection": "SA 2D Area Conn",
}

MAP_DEM_UNCLIPPED_SRC_URL = (
    "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt"
)
MAP_DEM_CLIPPED_BASENAME = "ned13.tif"
MAP_DEM_BUFFER_DIST_FT = 1000.0
MAP_DEM_DIRNAME = "MapTerrain"
MAP_DEM_HDF_NAME = "MapTerrain.hdf"
MAP_DEM_VERT_UNITS = "Feet"

METERS_PER_FOOT = 1200.0 / 3937.0
