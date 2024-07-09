"""
Expects conflation_data table to exist with following coloumns
id, us_xs_river, us_xs_reach, us_xs_id, ds_xs_river, ds_xs_reach, ds_xs_id, model
"""

import sqlite3


def column_exists(cursor, table_name, column_name):
    """Function to check if a column exists"""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [column[1] for column in cursor.fetchall()]
    return column_name in columns


def update_ripple_to_id(db_path):
    """Function to update ripple_to_id in the SQLite database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Add new column to the conflation_data table if it doesn't exist
    if not column_exists(cursor, "conflation_data", "ripple_to_id"):
        cursor.execute("ALTER TABLE conflation_data ADD COLUMN ripple_to_id INTEGER")

    cursor.execute(
        "SELECT id, us_xs_river, us_xs_reach, us_xs_id, ds_xs_river, ds_xs_reach, ds_xs_id, model FROM conflation_data"
    )
    records = cursor.fetchall()

    record_map = {}
    for record in records:
        id, us_xs_river, us_xs_reach, us_xs_id, ds_xs_river, ds_xs_reach, ds_xs_id, model = record
        key = (us_xs_river, us_xs_reach, us_xs_id, model)
        record_map[key] = id

    # Update ripple_to_id where ds_xs_* matches us_xs_*
    for record in records:
        id, us_xs_river, us_xs_reach, us_xs_id, ds_xs_river, ds_xs_reach, ds_xs_id, model = record
        key = (ds_xs_river, ds_xs_reach, ds_xs_id, model)
        if key in record_map:
            ripple_to_id = record_map[key]
            cursor.execute("UPDATE conflation_data SET ripple_to_id = ? WHERE id = ?", (ripple_to_id, id))

    conn.commit()
    conn.close()


db_path = r"D:\Users\abdul.siddiqui\workbench\projects\ripple_1d_outputs\production.db.sqlite"

update_ripple_to_id(db_path)
