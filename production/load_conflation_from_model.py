"""
Expects conflation_data table with id and to_id coloumns already populated for each reach
"""

import json
import sqlite3

model_keys = ["Baxter"]
source_models_directory = r"D:\Users\abdul.siddiqui\workbench\projects\test_production\source_models"
db_path = r"D:\Users\abdul.siddiqui\workbench\projects\test_production\library.sqlite"


def load_json(file_path):
    """"""
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def insert_data_to_db(db_path, data, model_key):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for key, value in data.items():
        us_xs_river = value["us_xs"].get("river", None)
        us_xs_reach = value["us_xs"].get("reach", None)
        us_xs_id = value["us_xs"].get("xs_id", None)
        ds_xs_river = value.get("ds_xs", {}).get("river", None)
        ds_xs_reach = value.get("ds_xs", {}).get("reach", None)
        ds_xs_id = value.get("ds_xs", {}).get("xs_id", None)

        if (us_xs_id, us_xs_reach, us_xs_river) == (str(ds_xs_id), ds_xs_reach, ds_xs_river):
            print("single XS", model_key, us_xs_id, us_xs_reach, us_xs_river)
            cursor.execute(
                """
                UPDATE conflation
                SET
                    model_key = ?,
                    us_xs_id = -9999.0
                WHERE reach_id = ?
                AND us_xs_id IS NULL;
            """,
                (model_key, key),
            )
        else:
            cursor.execute(
                """
                UPDATE conflation
                SET
                    model_key = ?,
                    us_xs_river = ?,
                    us_xs_reach = ?,
                    us_xs_id = ?,
                    ds_xs_river = ?,
                    ds_xs_reach = ?,
                    ds_xs_id = ?
                WHERE reach_id = ?
                AND (us_xs_id IS NULL OR us_xs_id = -9999.0);
            """,
                (model_key, us_xs_river, us_xs_reach, us_xs_id, ds_xs_river, ds_xs_reach, ds_xs_id, key),
            )

    conn.commit()
    conn.close()


for model_key in model_keys:
    json_data = load_json(f"{source_models_directory}\\{model_key}\\{model_key}.conflation.json")
    insert_data_to_db(db_path, json_data, model_key)
