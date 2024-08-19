import os

from ripple1d.ops.fim_lib import nwm_reach_model_stac
from ripple1d.ripple1d_logger import configure_logging
import logging

if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    reach_id = "2823932"

    ras_project_directory = os.path.dirname(__file__).replace(
        "production", f"tests\\ras-data\\Baxter\\submodels\\{reach_id}"
    )
    ras_model_s3_prefix = f"stac/test-data/nwm_reach_models/{reach_id}"
    bucket = "fim"
    nwm_reach_model_stac(ras_project_directory, ras_model_s3_prefix, bucket)
