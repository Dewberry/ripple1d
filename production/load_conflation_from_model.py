"""
Expects conflation_data table with id and to_id coloumns already populated for each reach
"""

import json
import sqlite3


def load_json(file_path):
    """"""
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def column_exists(cursor, table_name, column_name):
    """"""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [column[1] for column in cursor.fetchall()]
    return column_name in columns


def insert_data_to_db(db_path, data, model_key):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    columns_to_add = {
        "model_key": "TEXT",
        "us_xs_river": "TEXT",
        "us_xs_reach": "TEXT",
        "us_xs_id": "REAL",
        "ds_xs_river": "TEXT",
        "ds_xs_reach": "TEXT",
        "ds_xs_id": "REAL",
    }  # Add new columns to the conflation_data table if they don't exist

    for column, col_type in columns_to_add.items():
        if not column_exists(cursor, "conflation", column):
            cursor.execute(f"ALTER TABLE conflation ADD COLUMN {column} {col_type}")

    for key, value in data.items():
        us_xs_river = value["us_xs"].get("river", None)
        us_xs_reach = value["us_xs"].get("reach", None)
        us_xs_id = value["us_xs"].get("xs_id", None)
        ds_xs_river = value.get("ds_xs", {}).get("river", None)
        ds_xs_reach = value.get("ds_xs", {}).get("reach", None)
        ds_xs_id = value.get("ds_xs", {}).get("xs_id", None)

        cursor.execute(
            """
            UPDATE conflation
            SET model_key = ?, us_xs_river = ?, us_xs_reach = ?, us_xs_id = ?, ds_xs_river = ?, ds_xs_reach = ?, ds_xs_id = ?
            WHERE reach_id = ?
        """,
            (model_key, us_xs_river, us_xs_reach, us_xs_id, ds_xs_river, ds_xs_reach, ds_xs_id, key),
        )

    conn.commit()
    conn.close()


model_key = "Baxter"
source_models_directory = r"D:\Users\abdul.siddiqui\workbench\projects\production\source_models"

json_data = load_json(f"{source_models_directory}\\{model_key}\\{model_key}.conflation.json")

insert_data_to_db(r"D:\Users\abdul.siddiqui\workbench\projects\production\conflation.sqlite", json_data, model_key)
