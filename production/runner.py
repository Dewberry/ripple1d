import concurrent.futures
import csv
import json
import random
import sqlite3
from time import sleep

import requests

runner_file = r"D:\Users\abdul.siddiqui\workbench\projects\lwoc_huc12\runner.csv"
db_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\library.sqlite"
process_name = "extract_submodel"

payload_templates = {
    "extract_submodel": {
        "source_model_directory": "D:\\Users\\abdul.siddiqui\\workbench\\projects\\production\\source_models\\{model_key}",
        "submodel_directory": "D:\\Users\\abdul.siddiqui\\workbench\\projects\\production\\submodels\\{nwm_reach_id}",
        "nwm_id": "{nwm_reach_id}",
        "ripple_version": "0.0.1",
    },
    "create_ras_terrain": {
        "submodel_directory": "D:\\Users\\abdul.siddiqui\\workbench\\projects\\production\\submodels\\{nwm_reach_id}"
    },
    "create_model_run_normal_depth": {
        "submodel_directory": "D:\\Users\\abdul.siddiqui\\workbench\\projects\\production\\submodels\\{nwm_reach_id}",
        "plan_suffix": "ind",
        "num_of_discharges_for_initial_normal_depth_runs": 10,
        "ras_version": "631",
    },
    "run_incremental_normal_depth": {
        "submodel_directory": "D:\\Users\\abdul.siddiqui\\workbench\\projects\\production\\submodels\\{nwm_reach_id}",
        "plan_suffix": "nd",
        "depth_increment": 0.5,
        "ras_version": "631",
    },
}


def format_payload(template, nwm_reach_id, model_key):
    payload = {}
    for key, value in template.items():
        if type(value) != str:
            continue
        payload[key] = value.format(nwm_reach_id=nwm_reach_id, model_key=model_key)
    return payload


def execute_request(nwm_reach_id, model_key, process_name):
    sleep(random.uniform(0, 3))  # Sleep to avoid database locked error on Ripple API side
    url = f"http://localhost/processes/{process_name}/execution"
    payload = json.dumps(format_payload(payload_templates[process_name], nwm_reach_id, model_key))
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 201:
        job_id = response.json().get("jobID")
        return (job_id, "accepted", nwm_reach_id)
    else:
        print(f"Failed to process nwm_reach_id {nwm_reach_id}, status code: {response.status_code}")
        return None


def execute_process(csv_file, process_name):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Read CSV file
    with open(csv_file, mode="r") as file:
        csv_reader = csv.DictReader(file)
        rows = [(row["nwm_reach_id"], row["model_key"]) for row in csv_reader]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(execute_request, nwm_reach_id, model_key, process_name) for nwm_reach_id, model_key in rows
        ]
        data_to_update = [
            future.result() for future in concurrent.futures.as_completed(futures) if future.result() is not None
        ]

    cursor.executemany(
        f"""
    UPDATE processing
    SET {process_name}_job_id = ?, {process_name}_status = ?
    WHERE reach_id = ?;
    """,
        data_to_update,
    )

    conn.commit()
    conn.close()


execute_process(runner_file, process_name)
