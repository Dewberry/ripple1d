"""
Update existing collection.

TODO: Refactor, this script was written in one setting and needs to be revised significantly.
"""

# import argparse
# import logging

# from ripple1d.utils.s3_utils import read_json_from_s3

# from .fim_collection import FIMCollection, FIMCollectionRasItem

# if __name__ == "__main__":
#     # STAC API URL
#     API_URL = "https://stac2.dewberryanalytics.com"

#     parser = argparse.ArgumentParser(description="Process some integers.")
#     parser.add_argument("--collection_id", type=str, required=True, help="Collection ID")

#     args = parser.parse_args()
#     collection_id = args.collection_id

#     fc = FIMCollection(API_URL, collection_id)
#     for item in fc.collection.get_all_items():
#         logging.info(f"item_id: {item.id}")

#         fci = FIMCollectionRasItem(API_URL, collection_id, item.id)

#         fci.sanitize_ras_stac_props()

#         fci.map_topo_assets()

#         conflated = fci.add_ripple1d_params()

#         fci.add_s3_key_to_assets()

#         fci.ensure_asset_roles_unique()

#         if conflated:
#             params_json = fci.item.assets["ripple1d_parameters.json"].extra_fields["s3_key"]
#             data = read_json_from_s3("fim", params_json)

#             fci.item.properties["FIM:Branch Metadata"] = data
#         else:
#             logging.error(f"Failed to add ripple1d params for {fci.item.id}")

#         fci.post_item_updates()
