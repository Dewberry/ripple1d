from __future__ import annotations

import logging

import pystac
import pystac_client
from dotenv import find_dotenv, load_dotenv

from ripple import consts
from ripple.exe import make_ras_terrain
from ripple.utils import init_log

load_dotenv(find_dotenv())

COLLECTION_ID = "huc-12040101"


def main():
    client = pystac_client.Client.open(consts.STAC_API_URL)
    collection = client.get_collection(COLLECTION_ID)
    items = sorted(collection.get_all_items(), key=lambda x: x.id)

    for i, item in enumerate(items):
        pct_s = "{:.0%}".format((i + 1) / len(items))
        logging.info(f"{pct_s} ({i+1} / {len(items)}) {item.id}")
        hrefs = [link.target for link in item.links if link.rel == "self"]
        if len(hrefs) != 1:
            raise ValueError(f"Expected 1 STAC href, but got {len(hrefs)} for item ID {item.id}: {hrefs}")
        ras_model_stac_href = hrefs[0]
        logging.info(f"Building RAS Terrain for: {ras_model_stac_href}")
        make_ras_terrain.main(ras_model_stac_href)


if __name__ == "__main__":
    init_log()
    main()
