import os
import sqlite3

# Paths
library_db_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\library.sqlite"
library_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\library"


def process_reach_db(submodel, reach_db_path, library_conn):
    """Process a reach_id.db file and insert rating curves into the library database."""
    reach_conn = sqlite3.connect(reach_db_path)
    reach_cursor = reach_conn.cursor()
    reach_cursor.execute(
        "SELECT reach_id, us_flow, us_depth, us_wse, ds_depth, ds_wse, boundary_condition, ripple_version FROM rating_curves"
    )
    rows = reach_cursor.fetchall()

    cursor = library_conn.cursor()
    cursor.executemany(
        """
        INSERT INTO rating_curves (
            reach_id, us_flow, us_depth, us_wse, ds_depth, ds_wse, boundary_condition, ripple_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    library_conn.commit()

    reach_conn.close()


library_conn = sqlite3.connect(library_db_path)

for submodel in os.listdir(library_dir):
    sub_db_path = os.path.join(
        library_dir,
        submodel,
        f"{submodel}.db",
    )
    process_reach_db(submodel, sub_db_path, library_conn)

library_conn.close()
