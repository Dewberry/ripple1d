from fim_collection import FIMCollectionRasItem, FIMCollection
from s3_utils import read_json_from_s3


if __name__ == "__main__":
    # STAC API URL
    API_URL = "https://stac2.dewberryanalytics.com"
    collection_id = "huc-12040101"

    fc = FIMCollection(API_URL, collection_id)
    for item in fc.collection.get_all_items():
        print(item.id)

        fci = FIMCollectionRasItem(API_URL, collection_id, item.id)

        fci.sanitize_ras_stac_props()

        fci.map_topo_assets()

        conflated = fci.add_ripple_params()

        fci.add_s3_key_to_assets()

        fci.ensure_asset_roles_unique()

        if conflated:
            # print(f"Added ripple params for {fci.item.id}")
            params_json = fci.item.assets["ripple_parameters.json"].extra_fields[
                "s3_key"
            ]
            data = read_json_from_s3("fim", params_json)

            fci.item.properties["FIM:Branch Metadata"] = data
        else:
            print(f"Failed to add ripple params for {fci.item.id}")

        fci.post_item_updates()
