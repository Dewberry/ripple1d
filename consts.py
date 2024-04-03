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


FLOWS = (
    [1, 5]
    + [i for i in range(10, 100, 10)]
    + [i for i in range(100, 1000, 100)]
    + [i for i in range(1000, 10000, 1000)]
    + [i for i in range(10000, 100000, 10000)]
    + [i for i in range(100000, 1000000, 100000)]
)
