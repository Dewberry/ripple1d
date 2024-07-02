"""
Script is a prototype for creating a STAC collection for the FIM depth grids. 

TODO: Refactor, this script was written in one setting and needs to be revised significantly.     
"""

# from typing import List

# import pystac
# from fim_collection import FIMCollection, FIMCollectionRasItem
# from ras_stac.utils.dg_utils import *
# from ras_stac.utils.dg_utils import create_depth_grid_item
# from ras_stac.utils.ras_stac import *
# from ras_stac.utils.s3_utils import *
# from s3_utils import check_s3_key_exists, init_s3_resources, list_keys
# from stac_utils import create_collection, upsert_collection, upsert_item, uri_to_key


# def fim_dg_item(
#     dg_id: str,
#     dgs: list,
#     asset_list: list = None,
#     minio_mode: bool = False,
#     bucket_name: str = "fim",
# ):

#     # Instantitate S3 resources
#     session, _, s3_resource = init_s3_resources()
#     bucket = s3_resource.Bucket(bucket_name)
#     AWS_SESSION = AWSSession(session)

#     dg_obj = bucket.Object(dgs[0])
#     dg_item = create_depth_grid_item(dg_obj, dg_id, AWS_SESSION, minio_mode=minio_mode)

#     assets = []
#     for asset in dg_item.assets:
#         assets.append(asset)
#     for asset in assets:
#         del dg_item.assets[asset]

#     for asset_key in asset_list:
#         obj = bucket.Object(asset_key)
#         metadata = get_basic_object_metadata(obj)
#         asset = pystac.Asset(
#             f"https://fim.s3.amazonaws.com/{asset_key}",
#             extra_fields=metadata,
#             roles=["application/x-sqlite3"],
#             description="Additional data for the FIM DG item",
#         )
#         dg_item.add_asset(Path(asset_key).name.replace(" ", ""), asset)

#     for asset_key in dgs:
#         obj = bucket.Object(asset_key)
#         metadata = get_basic_object_metadata(obj)
#         asset = pystac.Asset(
#             f"https://fim.s3.amazonaws.com/{asset_key}",
#             extra_fields=metadata,
#             roles=["fim-dg", pystac.MediaType.COG],
#             description="(Prototype) RAS1D Fim Depth Grids",
#         )
#         dg_item.add_asset(f"{Path(asset_key).parent.name}-{Path(asset_key).name}", asset)

#     return dg_item


# def split_parts(key, prefix):
#     key_without_prefix = key[len(prefix) :].lstrip("/")
#     parts = key_without_prefix.split("/")
#     key_part = parts[0]
#     z_parts = parts[1:]
#     return {key_part: z_parts}


# def map_output_fims(bucket, output_prefix, client):
#     dg_stages = {}
#     tifs = list_keys(client, bucket, output_prefix, ".tif")

#     for t in tifs:
#         split_dict = split_parts(t, output_prefix)
#         for key_part, z_parts in split_dict.items():
#             if key_part not in dg_stages:
#                 dg_stages[key_part] = {}
#             z_key = z_parts[0]
#             if z_key not in dg_stages[key_part]:
#                 dg_stages[key_part][z_key] = []
#             dg_stages[key_part][z_key].append(z_parts[1])
#     return dg_stages


# def main(
#     new_collection_items: List[pystac.Item],
#     collection_id: str,
#     description: str,
#     title: str,
# ):
#     """
#     Given a list of items created using ras-stac library prior to publishing. This function
#     creates a new collection in the STAC_API and adds the items to the collection.
#     """
#     collection = create_collection(new_collection_items, collection_id, description, title)
#     r = upsert_collection(API_URL, collection)
#     logging.info(f"response: {r}")

#     for item in new_collection_items:
#         r = upsert_item(API_URL, collection_id, item)
#         logging.info(f"item_id: {item.id},response: {r}")


# if __name__ == "__main__":

#     _, s3_client, _ = init_s3_resources()

#     huc_id = "12040101"
#     ras_collection_id = f"huc-{huc_id}"
#     fim_collection_id = f"huc-{huc_id}-fims"

#     bucket_name = "fim"
#     bucket_prefix = "stac/12040101"

#     description = f"""Prototype catalog for FIMS from the huc-{huc_id} RAS model collection"""
#     title = f"FIMS for HUC-{huc_id}"

#     # STAC API URL
#     API_URL = "https://stac2.dewberryanalytics.com"

#     fc = FIMCollection(API_URL, ras_collection_id)
#     new_dg_items = []
#     i = 0
#     for item in fc.collection.get_all_items():
#         i += 1
#         logging.info(f"{i} : {item.id}")
#         try:
#             model_branches = item.properties["FIM:Branch Metadata"].keys()
#             # logging.debug(f"{len(model_branches)} branches in FIM:Branch Metadata for {item.id}")
#         except KeyError:
#             logging.error(f"FIM:Branch Metadata does not exist for {item.id}")

#         prefix = f"mip/dev/ripple/output/collections/{ras_collection_id}/items/{item.id}"
#         model_db = f"{prefix}/{item.id}.db"
#         ripple_succeed = f"{prefix}/ripple-succeed.json"

#         if check_s3_key_exists("fim", ripple_succeed):
#             # logging.debug(f"Model database exists for {item.id}")
#             fim_dict = map_output_fims(bucket_name, f"{prefix}/", s3_client)
#             fci = FIMCollectionRasItem(API_URL, ras_collection_id, item.id)

#             for branch_id, stage_values in fim_dict.items():
#                 dg_id = branch_id
#                 assets = [model_db]
#                 dgs = []
#                 for z_key in stage_values.keys():
#                     dgs.extend([f"{prefix}/{branch_id}/{z_key}/{v}" for v in fim_dict[branch_id][z_key]])
#                 dg_item = fim_dg_item(dg_id, dgs, assets)
#                 dg_item.add_derived_from(item)

#                 for asset_name in dg_item.assets:
#                     asset = dg_item.assets[asset_name]
#                     asset.extra_fields["s3_key"] = uri_to_key(asset.href, bucket_name)
#                 dg_item.properties["FIM:Branch Metadata"] = item.properties["FIM:Branch Metadata"][branch_id]

#                 dg_item.properties["software"] = "ripple v0.1.0-alpha.1"

#                 new_dg_items.append(dg_item)

#                 # dg_item_link = pystac.Link(
#                 #         rel="related",
#                 #         target=dg_item,
#                 #         title=f"FIMs-{branch_id}",
#                 #     )

#     main(new_dg_items, fim_collection_id, description, title)
