"""
Needs to be reworked to use the ripple_to_id coloumn from database rather than implementing the same logic here
"""

import sqlite3
import subprocess

ras_projects = r"D:\Users\abdul.siddiqui\workbench\projects\trial_run_ripple"
db_path = r"D:\Users\abdul.siddiqui\workbench\projects\ripple_1d_outputs\production.db.sqlite"
start_id = 1465570

conn = sqlite3.connect(db_path)
cursor = conn.cursor()


def get_model_info(id):
    cursor.execute("SELECT id, us_xs_river, us_xs_reach, us_xs_id, model FROM conflation_data WHERE id = ?", (id,))
    return cursor.fetchone()


def get_related_ids(model, us_xs_river, us_xs_reach, us_xs_id):
    cursor.execute(
        """
        SELECT id FROM conflation_data
        WHERE model = ? AND ds_xs_river = ? AND ds_xs_reach = ? AND ds_xs_id = ?
    """,
        (model, us_xs_river, us_xs_reach, us_xs_id),
    )
    return [row[0] for row in cursor.fetchall()]


def process_id(id, to_id):
    model_info = get_model_info(id)
    if model_info is None:
        print(f"No information found for ID: {id}")
        return

    id, us_xs_river, us_xs_reach, us_xs_id, model_name = model_info

    ras_project_text_file = rf"{ras_projects}\{model_name}\{id}\{id}.prj"
    subset_gpkg_path = rf"{ras_projects}\{model_name}\{id}\{id}.gpkg"
    json_path = rf"{ras_projects}\{model_name}\{model_name}.json"
    terrain_path = rf"{ras_projects}\{model_name}\{id}\Terrain.hdf"
    if to_id:
        ds_nwm_ras_project_file = rf"{ras_projects}\WFSJ Main\{to_id}\{to_id}.prj"

        command = [
            "python",
            "-m",
            "ripple.scripts.known_water_surface_elevation_run",
            str(id),
            ras_project_text_file,
            subset_gpkg_path,
            terrain_path,
            json_path,
            str(to_id),
            ds_nwm_ras_project_file,
        ]

        print("\n>>>>>>>>>>>>>>>>>>>>>\n")
        print(f"Executing command: {' '.join(command)}")
        subprocess.run(command)

    # process connected reaches
    related_ids = get_related_ids(model_name, us_xs_river, us_xs_reach, us_xs_id)
    for related_id in related_ids:
        process_id(related_id, id)


process_id(start_id, 1465590)  # start traversing

conn.close()
