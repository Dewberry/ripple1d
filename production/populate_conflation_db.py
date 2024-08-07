"""
This script reads data from a NWM Flowlines Parquet file and inserts it into an Conflation SQLite database.
DuckDB is used to read the Parquet file due to its efficiency in handling columnar data formats and its ability to work well with large datasets.
The data is inserted into the SQLite database in batches using the `executemany` method to improve insertion performance and reduce the number of database transactions.
"""

import sqlite3

import duckdb


def read_parquet_with_duckdb(parquet_file):

    con = duckdb.connect()
    query = f"SELECT id AS reach_id, to_id AS nwm_to_id FROM read_parquet('{parquet_file}')"
    table = con.execute(query).fetchall()
    con.close()

    return table


def insert_table_to_sqlite(table, sqlite_db):

    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()

    # Prepare rows for insertion
    cursor.executemany(
        """
        INSERT INTO conflation (reach_id, nwm_to_id)
        VALUES (?, ?)
        """,
        table,
    )

    conn.commit()
    conn.close()


parquet_file = "D:/Users/abdul.siddiqui/workbench/projects/ripple_1d_outputs/nwm_flowlines.parquet"
sqlite_db = "D:/Users/abdul.siddiqui/workbench/projects/test_production/library.sqlite"

table = read_parquet_with_duckdb(parquet_file)
insert_table_to_sqlite(table, sqlite_db)
