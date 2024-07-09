"""
This script reads data from a NWM Flowlines Parquet file and inserts it into an Conflation SQLite database.

PyArrow is used to read the Parquet file due to its efficiency in handling columnar data formats and its ability to work well with large datasets.
The script avoids using Pandas to reduce memory overhead and improve performance. Instead, it directly handles the Arrow Table for data extraction and insertion.
The data is inserted into the SQLite database in batches using the `executemany` method to improve insertion performance and reduce the number of database transactions.

"""

import sqlite3

import pyarrow.parquet as pq


def read_parquet_to_table(parquet_file):
    table = pq.read_table(parquet_file)
    return table


def insert_table_to_sqlite(table, sqlite_db):
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()

    rows = [(table.column("id")[i].as_py(), table.column("to_id")[i].as_py()) for i in range(table.num_rows)]

    cursor.executemany(
        """
    INSERT INTO conflation (reach_id, nwm_to_id)
    VALUES (?, ?)
    """,
        rows,
    )

    conn.commit()
    conn.close()


parquet_file = "D:/Users/abdul.siddiqui/workbench/projects/ripple_1d_outputs/nwm_flowlines.parquet"
sqlite_db = "D:/Users/abdul.siddiqui/workbench/projects/production/conflation.sqlite"

table = read_parquet_to_table(parquet_file)
insert_table_to_sqlite(table, sqlite_db)
