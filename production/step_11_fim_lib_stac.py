import os

from ripple1d.ops.fim_lib import fim_lib_stac

if __name__ == "__main__":

    nwm_reach_id = "2823932"
    ras_project_directory = os.path.dirname(__file__).replace(
        "production", f"tests\\ras-data\\Baxter\\submodels\\{nwm_reach_id}"
    )
    s3_prefix = "stac/test-data/fim_libs"
    bucket = "fim"

    fim_lib_stac(ras_project_directory, nwm_reach_id, s3_prefix, bucket)
