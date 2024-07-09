import sqlite3

# Creates the file if it doesn't exist
connection = sqlite3.connect(r"D:\Users\abdul.siddiqui\workbench\projects\production\conflation.sqlite")
cursor = connection.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS conflation (
        reach_id INTEGER PRIMARY KEY,
        nwm_to_id INTEGER,
        conflation_to_id INTEGER
    )
    """
)

# Indexes will speed up where downstream = queries
cursor.execute(
    """
    CREATE INDEX IF NOT EXISTS conflation_nwm_to_id_idx ON conflation (nwm_to_id)
    """
)
cursor.execute(
    """
    CREATE INDEX IF NOT EXISTS conflation_conflation_to_id_idx ON conflation (conflation_to_id)
    """
)

connection.commit()
connection.close()
