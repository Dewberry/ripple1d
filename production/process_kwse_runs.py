"""
This script processes KWSE runs of reaches by traversing the network. It gets the river network by querying conflation SQLite database 'conflation_to_id' column. It also creates fim libraries as those are needed for the processing upstream reach.
"""

import json
import os
import sqlite3
import time
from typing import List, Optional, Tuple

import requests

submodels_directory = r"D:\Users\abdul.siddiqui\workbench\projects\production\submodels"
conflation_db_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\conflation.sqlite"
start_reach = 2821866

wait_time = 3

conn = sqlite3.connect(conflation_db_path)
cursor = conn.cursor()


def get_upstream_reaches(conflation_to_id: int) -> List[int]:
    """Fetch upstream reach IDs from the 'conflation' table."""
    cursor.execute(
        """
        SELECT reach_id FROM conflation
        WHERE conflation_to_id = ?
    """,
        (conflation_to_id,),
    )
    return [row[0] for row in cursor.fetchall()]


def get_min_max_elevation(downstream_id: int) -> Tuple[Optional[float], Optional[float]]:
    """Fetch min and max elevation from the submodel database."""
    ds_submodel_db_path = os.path.join(submodels_directory, str(downstream_id), "fims", f"{downstream_id}.db")
    if not os.path.exists(ds_submodel_db_path):
        print(f"Submodel database not found for reach_id: {downstream_id}")
        return None, None

    ds_reach_conn = sqlite3.connect(ds_submodel_db_path)
    ds_reach_cursor = ds_reach_conn.cursor()
    ds_reach_cursor.execute(f"SELECT MIN(wse), MAX(wse) FROM `'{downstream_id}'`")
    min_elevation, max_elevation = ds_reach_cursor.fetchone()
    ds_reach_conn.close()
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
        time.sleep(wait_time)  # Wait for 10 seconds before checking again


def process_network(start_reach: int) -> None:
    """Process the given reach ID and iteratively process related upstream reach IDs."""

    stack = [(start_reach, None)]  # Use a stack to avoid deep recursion

    while stack:
        reach_id, downstream_id = stack.pop()

        print(f"<<<<<< processing reach {reach_id}")

        submodel_directory_path = os.path.join(submodels_directory, str(reach_id))
        headers = {"Content-Type": "application/json"}

        if downstream_id:
            min_elevation, max_elevation = get_min_max_elevation(downstream_id)
            if min_elevation is None or max_elevation is None:
                print(f"Could not retrieve min/max elevation for reach_id: {downstream_id}")
                continue

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
                continue

        fim_url = "http://localhost/processes/create_fim_lib/execution"
        fim_payload = json.dumps({"submodel_directory": submodel_directory_path, "plans": ["nd", "kwse"]})
        fim_response = requests.post(fim_url, headers=headers, data=fim_payload)
        fim_response_json = fim_response.json()
        fim_job_id = fim_response_json.get("jobID")
        if not fim_job_id or not check_job_status(fim_job_id):
            print(f"Create Fim Lib failed for {reach_id} api job ID: {fim_job_id}")
            continue

        upstream_reaches = get_upstream_reaches(reach_id)
        for upstream_reach in upstream_reaches:
            stack.append((upstream_reach, reach_id))


process_network(start_reach)  # Start traversing

conn.close()
