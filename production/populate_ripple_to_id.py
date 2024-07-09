"""
Expects conflation table to exist with following columns
id, us_xs_river, us_xs_reach, us_xs_id, ds_xs_river, ds_xs_reach, ds_xs_id, model_key
"""

import sqlite3


def column_exists(cursor, table_name, column_name):
    """Function to check if a column exists"""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [column[1] for column in cursor.fetchall()]
    return column_name in columns


def update_conflation_to_id(db_path):
    """Function to update conflation_to_id in the SQLite database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Add new column to the conflation table if it doesn't exist
    if not column_exists(cursor, "conflation", "conflation_to_id"):
        cursor.execute("ALTER TABLE conflation ADD COLUMN conflation_to_id INTEGER")

    cursor.execute(
        "SELECT reach_id, us_xs_river, us_xs_reach, us_xs_id, ds_xs_river, ds_xs_reach, ds_xs_id, model_key FROM conflation WHERE model_key IS NOT NULL"
    )
    records = cursor.fetchall()

    record_map = {}
    for record in records:
        id, us_xs_river, us_xs_reach, us_xs_id, ds_xs_river, ds_xs_reach, ds_xs_id, model_key = record
        key = (us_xs_river, us_xs_reach, us_xs_id, model_key)
        record_map[key] = id

    # Update conflation_to_id where ds_xs_* matches us_xs_*
    for record in records:
        id, us_xs_river, us_xs_reach, us_xs_id, ds_xs_river, ds_xs_reach, ds_xs_id, model_key = record
        key = (ds_xs_river, ds_xs_reach, ds_xs_id, model_key)
        if key in record_map:
            conflation_to_id = record_map[key]
            cursor.execute("UPDATE conflation SET conflation_to_id = ? WHERE reach_id = ?", (conflation_to_id, id))

    conn.commit()
    conn.close()


db_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\conflation.sqlite"

update_conflation_to_id(db_path)
