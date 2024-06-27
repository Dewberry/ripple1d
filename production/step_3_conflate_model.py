import logging
import traceback
from pathlib import Path
from time import sleep

import pystac

from production.db_utils import PGFim
from ripple.conflate.rasfim import RasFimConflater
from ripple.ops.conflate_ras_model import conflate_s3_model, href_to_vsis
from ripple.ripple_logger import configure_logging
from ripple.stacio.utils.s3_utils import init_s3_resources, s3_key_public_url_converter


def main(table_name: str, mip_group: str, bucket: str, nwm_pq_path: str):

    db = PGFim()

    session, client, s3_resource = init_s3_resources()
    rfc = None
    optional_condition = "AND stac_complete=true AND conflation_complete IS NULL"
    data = db.read_cases(
        table_name,
        [
            "case_id",
            "s3_key",
        ],
        mip_group,
        optional_condition,
    )
    while data:

        for i, (mip_case, s3_ras_project_key) in enumerate(data):
            try:
                logging.info(
                    f"progress: ({i+1}/{len(data)} | {round(100*(i+1)/len(data),1)}% | working on key: {s3_ras_project_key}"
                )

                stac_item_s3_key = s3_ras_project_key.replace("mip", "stac").replace(".prj", ".json")
                stac_item_href = f"https://{bucket}.s3.amazonaws.com/{stac_item_s3_key}".replace(" ", "%20")
                print(stac_item_s3_key)
                print(stac_item_href)
                item = pystac.Item.from_file(stac_item_href)

                for asset in item.get_assets(role="ras-geometry-gpkg"):
                    ras_gpkg = href_to_vsis(item.assets[asset].href, bucket="fim")

                if not rfc:
                    rfc = RasFimConflater(nwm_pq_path, ras_gpkg)
                else:
                    rfc.set_ras_gpkg(ras_gpkg)

                for river_reach_name in rfc.ras_river_reach_names:
                    logging.info(f"item_id {item.id}, river_reach {river_reach_name}")

                conflate_s3_model(
                    item,
                    client,
                    bucket,
                    stac_item_s3_key,
                    rfc,
                    river_reach_name,
                )
                logging.info(f"{item.id}: Successfully processed")
                db.update_case_status(mip_group, mip_case, s3_ras_project_key, True, None, None, "conflation")

            except Exception as e:
                exc = str(e)
                tb = str(traceback.format_exc())
                logging.error(exc)
                db.update_case_status(mip_group, mip_case, s3_ras_project_key, False, exc, tb, "conflation")
        sleep(1)

        data = db.read_cases(
            table_name,
            [
                "case_id",
                "s3_key",
            ],
            mip_group,
            optional_condition,
        )


if __name__ == "__main__":

    configure_logging(level=logging.INFO, logfile="create_stac.log")
    mip_group = "tx_ble"
    table_name = "processing"
    bucket = "fim"
    nwm_pq_path = r"C:\Users\mdeshotel\Downloads\nwm_flows_v3.parquet"

    main(table_name, mip_group, bucket, nwm_pq_path)
