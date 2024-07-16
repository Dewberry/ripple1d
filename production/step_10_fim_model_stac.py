import json
import os
from pathlib import Path

import pystac

from ripple.consts import RIPPLE_VERSION
from ripple.data_model import NwmReachModel
from ripple.ras_to_gpkg import geom_flow_to_gdfs, new_stac_item
from ripple.utils.s3_utils import init_s3_resources


def fim_model_to_stac(
    ras_project_directory: str,
    ras_model_s3_prefix: str = None,
    bucket: str = None,
    ripple_version: str = RIPPLE_VERSION,
):
    """Convert a FIM RAS model to a STAC item."""
    nwm_rm = NwmReachModel(ras_project_directory)

    # create a new gpkg
    geom_flow_to_gdfs(nwm_rm.ras_project_file, nwm_rm.crs)

    # create new stac item
    new_stac_item(
        ras_project_directory,
        ripple_version,
    )

    # upload to s3
    if bucket and ras_model_s3_prefix:
        nwm_rm.upload_files_to_s3(ras_model_s3_prefix, bucket)

        stac_item = pystac.read_file(nwm_rm.stac_json_file)
        # update asset hrefs
        for id, asset in stac_item.assets.items():
            if "thumbnail" in asset.roles:
                asset.href = f"https://{bucket}.s3.amazonaws.com/{ras_model_s3_prefix}/{Path(asset.href).name}"
            else:
                asset.href = f"s3://{bucket}/{ras_model_s3_prefix}/{Path(asset.href).name}"

        stac_item.set_self_href(
            f"https://{bucket}.s3.amazonaws.com/{ras_model_s3_prefix}/{Path(stac_item.self_href).name}"
        )
        # write updated stac item to s3
        _, s3_client, _ = init_s3_resources()
        s3_client.put_object(
            Body=json.dumps(stac_item.to_dict()).encode(),
            Bucket=bucket,
            Key=f"{ras_model_s3_prefix}/{Path(nwm_rm.stac_json_file).name}",
        )


if __name__ == "__main__":

    reach_id = "2823932"

    ras_project_directory = os.path.dirname(__file__).replace(
        "production", f"tests\\ras-data\\Baxter\\submodels\\{reach_id}"
    )
    ras_model_s3_prefix = f"stac/test-data/fim_models/{reach_id}"
    bucket = "fim"
    fim_model_to_stac(ras_project_directory, ras_model_s3_prefix, bucket)
