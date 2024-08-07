import sqlite3

# Creates the file if it doesn't exist
connection = sqlite3.connect(r"D:\Users\abdul.siddiqui\workbench\projects\test_production\library.sqlite")
cursor = connection.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS conflation (
        reach_id INTEGER PRIMARY KEY,
        nwm_to_id INTEGER,
        conflation_to_id INTEGER
    );
    """
)

# Indexes will speed up where downstream = queries
cursor.execute(
    """
    CREATE INDEX IF NOT EXISTS conflation_nwm_to_id_idx ON conflation (nwm_to_id);
    """
)
cursor.execute(
    """
    CREATE INDEX IF NOT EXISTS conflation_conflation_to_id_idx ON conflation (conflation_to_id);
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS rating_curves (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reach_id INTEGER,
        us_flow INTEGER,
        us_depth REAL,
        us_wse Real,
        ds_depth REAL,
        ds_wse REAL,
        boundary_condition TEXT CHECK(boundary_condition IN ('nd','kwse')) NOT NULL,
        ripple_version TEXT,
        UNIQUE(reach_id, us_flow, ds_wse, boundary_condition)
    );
    """
)

cursor.execute(
    """
    CREATE INDEX IF NOT EXISTS rating_curves_reach_id ON rating_curves (reach_id);
    """
)

cursor.execute(
    """
    CREATE TABLE "processing" (
        reach_id INTEGER PRIMARY KEY,
        "model_key" TEXT,
        "extract_submodel_job_id" TEXT,
        "extract_submodel_status" TEXT ,
        `create_ras_terrain_job_id` TEXT,
        `create_ras_terrain_status` TEXT,
        `create_model_run_normal_depth_job_id` TEXT,
        `create_model_run_normal_depth_status` TEXT,
        `run_incremental_normal_depth_job_id` TEXT,
        `run_incremental_normal_depth_status` TEXT,
        `run_known_wse_job_id` TEXT,
        `run_known_wse_status` TEXT,
        "create_fim_lib_job_id" TEXT,
        `create_fim_lib_status` TEXT
    );
    """
)


connection.commit()
connection.close()
