import os
import shutil
import sqlite3

reach_ids = []
library_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\library"
submodels_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\submodels"
library_db_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\library.sqlite"
delete_submodels = False
delete_library = False
delete_rc_records = False
reset_conflation_records = False
reset_processing_records = False


def delete_reach_data(
    reach_ids,
    library_dir,
    submodels_dir,
    db_location,
    delete_submodels=False,
    delete_library=False,
    delete_db_records=False,
    reset_conflation_records=False,
    reset_processing_records=False,
):
    # Connect to the database
    conn = sqlite3.connect(db_location)
    cursor = conn.cursor()

    # Delete records from the database if option is enabled
    if delete_db_records:
        placeholders = ", ".join("?" for _ in reach_ids)
        cursor.execute(f"DELETE FROM rating_curves WHERE reach_id IN ({placeholders});", reach_ids)
        conn.commit()

    # Reset conflation_records
    if reset_conflation_records:
        placeholders = ", ".join("?" for _ in reach_ids)
        cursor.execute(
            f"""
                        UPDATE conflation
                        SET conflation_to_id = NULL,
                            us_xs_river = NULL,
                            us_xs_reach = NULL,
                            us_xs_id = NULL,
                            ds_xs_river = NULL,
                            ds_xs_reach = NULL,
                            ds_xs_id = NULL,
                            "model_key" = NULL
                        WHERE reach_id IN ({placeholders});
                        """,
            reach_ids,
        )
        conn.commit()

    if reset_processing_records:
        placeholders = ", ".join("?" for _ in reach_ids)
        cursor.execute(
            f"""
                        UPDATE processing
                        SET extract_submodel_job_id = NULL,
                            extract_submodel_status = NULL,
                            create_ras_terrain_job_id = NULL,
                            create_ras_terrain_status = NULL,
                            create_model_run_normal_depth_job_id = NULL,
                            create_model_run_normal_depth_status = NULL,
                            run_incremental_normal_depth_job_id = NULL,
                            run_incremental_normal_depth_status = NULL,
                            run_known_wse_job_id = NULL,
                            run_known_wse_status = NULL,
                            create_fim_lib_job_id = NULL,
                            create_fim_lib_status = NULL
                        WHERE reach_id IN ({placeholders});
                        """,
            reach_ids,
        )
        conn.commit()

    conn.close()

    # Delete folders in submodels_dir if option is enabled
    if delete_submodels:
        for reach_id in reach_ids:
            folder_path = os.path.join(submodels_dir, str(reach_id))
            if os.path.exists(folder_path):
                shutil.rmtree(folder_path)

    # Delete folders in library_dir if option is enabled
    if delete_library:
        for reach_id in reach_ids:
            folder_path = os.path.join(library_dir, str(reach_id))
            if os.path.exists(folder_path):
                shutil.rmtree(folder_path)


delete_reach_data(
    reach_ids,
    library_dir,
    submodels_dir,
    library_db_path,
    delete_submodels,
    delete_library,
    delete_rc_records,
    reset_conflation_records,
    reset_processing_records,
)
