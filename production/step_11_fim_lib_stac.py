import os
import logging
from ripple1d.ops.fim_lib import fim_lib_stac
from ripple1d.ripple1d_logger import configure_logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    nwm_reach_id = "2823932"
    ras_project_directory = os.path.dirname(__file__).replace(
        "production", f"tests\\ras-data\\Baxter\\submodels\\{nwm_reach_id}"
    )
    s3_prefix = "stac/test-data/fim_libs"
    bucket = "fim"

    fim_lib_stac(ras_project_directory, nwm_reach_id, s3_prefix, bucket)
