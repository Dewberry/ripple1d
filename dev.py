import json
import logging
import os

import geopandas as gpd
import pandas as pd

from ripple.conflate.rasfim import RasFimConflater
from ripple.conflate.run_rasfim import main as conflate
from ripple.ripple_logger import configure_logging

configure_logging(logging.INFO)
from ripple.geom_to_gpkg import main as export_geom

# https://radiantearth.github.io/stac-browser/#/external/stac2.dewberryanalytics.com/collections/huc-12040101/items/WFSJR%20055
# https://radiantearth.github.io/stac-browser/#/external/stac2.dewberryanalytics.com/collections/huc-12040101/items/STEWARTS%20CREEK
# https://radiantearth.github.io/stac-browser/#/external/stac2.dewberryanalytics.com/collections/huc-12040101/items/WFSJ%20Main

if __name__ == "__main__":

    tesdir = "/Users/slawler/repos/ripple/devdata"

    # projname = "WFSJR 055"
    # projname = "STEWARTS CREEK"
    projname = "WFSJ Main"
    geom_ext = "g01"

    ras_gpkg_path = os.path.join(tesdir, f"{projname}.gpkg")
    ras_text_file_path = os.path.join(tesdir, f"{projname}.{geom_ext}")
    conflation_output = os.path.join(tesdir, f"{projname}.json")
    projection = os.path.join(tesdir, "projection.prj")
    nwm_pq_path = os.path.join(tesdir, "nwm_flows_v3.parquet")

    low_flows = gpd.read_parquet(nwm_pq_path)
    low_flows = low_flows[["ID", "high_flow_threshold", "f100year"]]

    _ = export_geom(ras_text_file_path, projection, ras_gpkg_path)

    rfc = RasFimConflater(
        nwm_pq_path,
        ras_gpkg_path,
    )

    rfc.nwm_reaches.head()

    results = conflate(rfc, low_flows)

    with open(conflation_output, "w") as f:
        f.write(json.dumps(results, indent=4))
