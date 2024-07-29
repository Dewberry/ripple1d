import json
import os
import sqlite3
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Lock
from typing import List, Optional, Tuple

import requests

submodels_directory = r"D:\Users\abdul.siddiqui\workbench\projects\production\submodels"
conflation_db_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\library.sqlite"
initial_reaches = [(6414614, None)]

use_central_db = False

wait_time = 3
db_lock = Lock()


def get_db_connection():
    return sqlite3.connect(conflation_db_path)


def get_upstream_reaches(conflation_to_id: int) -> List[int]:
    """Fetch upstream reach IDs from the 'conflation' table."""
    with db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT reach_id FROM conflation
            WHERE conflation_to_id = ?
        """,
            (conflation_to_id,),
        )
        result = [row[0] for row in cursor.fetchall()]
        conn.close()
        return result


def check_fim_lib_created(reach_id: int) -> bool:
    """Fetch upstream reach IDs from the 'conflation' table."""
    with db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT create_fim_lib_job_id
            FROM processing
            WHERE reach_id = ?;
        """,
            (reach_id,),
        )

        result = cursor.fetchone()
        conn.close()

        if result is None:
            raise ValueError(f"No record found for reach_id {reach_id}")

        create_fim_lib_job_id = result[0]
        return create_fim_lib_job_id is not None


def get_min_max_elevation(downstream_id: int) -> Tuple[Optional[float], Optional[float]]:
    """Fetch min and max elevation from the submodel database."""

    if use_central_db:
        if not os.path.exists(conflation_db_path):
            print(f"central database not found for")
            return None, None
        with db_lock:
            with sqlite3.connect(conflation_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT MIN(us_wse), MAX(us_wse) FROM rating_curves WHERE reach_ID = ?", (downstream_id,)
                )
                min_elevation, max_elevation = cursor.fetchone()
            return min_elevation, max_elevation
    else:
        ds_submodel_db_path = os.path.join(submodels_directory, str(downstream_id), "fims", f"{downstream_id}.db")
        if not os.path.exists(ds_submodel_db_path):
            print(f"Submodel database not found for reach_id: {downstream_id}")
            return None, None

        with sqlite3.connect(ds_submodel_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MIN(us_wse), MAX(us_wse) FROM rating_curves")
            min_elevation, max_elevation = cursor.fetchone()
        return min_elevation, max_elevation


def check_job_status(job_id: str) -> bool:
    """Poll job status until it completes or fails."""
    url = f"http://localhost/jobs/{job_id}"
    while True:
        response = requests.get(url)
        job_status = response.json().get("status")
        if job_status == "successful":
            return True
        elif job_status == "failed":
            print(f"Job {job_id} failed.")
            return False
        print(f"Waiting for job {job_id} to complete...")
        time.sleep(wait_time)  # Wait for a few seconds before checking again


def update_processing_table(reach_id, process_name, job_id, job_status):
    """Update the processing table with job_id and job_status."""
    with db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            UPDATE processing
            SET {process_name}_job_id = ?, {process_name}_status = ?
            WHERE reach_id = ?;
            """,
            (job_id, job_status, reach_id),
        )
        conn.commit()
        conn.close()


def process_reach(reach_id: int, downstream_id: Optional[int], task_queue: Queue) -> None:
    """Process a single reach."""
    try:

        # if check_fim_lib_created(reach_id):
        #     upstream_reaches = get_upstream_reaches(reach_id)
        #     for upstream_reach in upstream_reaches:
        #         task_queue.put((upstream_reach, reach_id))
        #     return

        print(f"<<<<<< processing reach {reach_id}")

        submodel_directory_path = os.path.join(submodels_directory, str(reach_id))
        headers = {"Content-Type": "application/json"}

        if downstream_id:
            min_elevation, max_elevation = get_min_max_elevation(downstream_id)
            if min_elevation is None or max_elevation is None:
                print(f"Could not retrieve min/max elevation for reach_id: {downstream_id}")
                return

            url = "http://localhost/processes/run_known_wse/execution"
            payload = json.dumps(
                {
                    "submodel_directory": submodel_directory_path,
                    "plan_suffix": "kwse",
                    "min_elevation": min_elevation,
                    "max_elevation": max_elevation,
                    "depth_increment": 1,
                    "ras_version": "631",
                }
            )
            print(f"<<<<<< payload for reach {reach_id}\n{payload}")

            response = requests.post(url, headers=headers, data=payload)
            response_json = response.json()
            job_id = response_json.get("jobID")
            if not job_id or not check_job_status(job_id):
                print(f"KWSE run failed for {reach_id} api job ID: {job_id}")
                update_processing_table(reach_id, "run_known_wse", job_id, "failed")
                upstream_reaches = get_upstream_reaches(reach_id)
                for upstream_reach in upstream_reaches:
                    task_queue.put((upstream_reach, None))
                return

            update_processing_table(reach_id, "run_known_wse", job_id, "successful")

        fim_url = "http://localhost/processes/create_fim_lib/execution"
        fim_payload = json.dumps({"submodel_directory": submodel_directory_path, "plans": ["nd", "kwse"]})
        response = requests.post(fim_url, headers=headers, data=fim_payload)
        fim_response_json = response.json()
        fim_job_id = fim_response_json.get("jobID")
        if not fim_job_id or not check_job_status(fim_job_id):
            print(f"Create Fim Lib failed for {reach_id} api job ID: {fim_job_id}")
            update_processing_table(reach_id, "create_fim_lib", fim_job_id, "failed")
            upstream_reaches = get_upstream_reaches(reach_id)
            for upstream_reach in upstream_reaches:
                task_queue.put((upstream_reach, None))
            return
        update_processing_table(reach_id, "create_fim_lib", fim_job_id, "successful")

        upstream_reaches = get_upstream_reaches(reach_id)
        for upstream_reach in upstream_reaches:
            task_queue.put((upstream_reach, reach_id))

    except Exception as e:
        print(f"Error processing reach {reach_id}: {str(e)}")
        traceback.print_exc()


def process_network(initial_reaches: List[Tuple[int, Optional[int]]]) -> None:
    """Start processing the network from the given list of initial reaches."""
    task_queue = Queue()
    for reach_pair in initial_reaches:
        task_queue.put(reach_pair)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        while not task_queue.empty() or futures:
            while not task_queue.empty():
                reach_id, downstream_id = task_queue.get()
                print(f"Submitting task for reach {reach_id} with downstream {downstream_id}")
                future = executor.submit(process_reach, reach_id, downstream_id, task_queue)
                futures.append(future)

            # Remove completed futures from the list
            for future in futures.copy():
                if future.done():
                    futures.remove(future)

            time.sleep(1)  # Add a small delay to avoid busy-waiting


if __name__ == "__main__":
    process_network(initial_reaches)
