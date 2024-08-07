import csv
import sqlite3
from typing import List, Optional, Tuple

# Paths
conflation_db_path = r"D:\Users\abdul.siddiqui\workbench\projects\test_production\library.sqlite"
runner_file = r"D:\Users\abdul.siddiqui\workbench\projects\test_production\runner.csv"
model_keys = ["Baxter"]


def get_reaches(model_keys) -> List[Tuple[int, str]]:
    """"""
    conn = sqlite3.connect(conflation_db_path)
    cursor = conn.cursor()

    params = ",".join(["?" for _ in model_keys])
    params
    cursor.execute(
        f"""
    SELECT reach_id, model_key FROM conflation
        WHERE us_xs_id IS NOT NULL and us_xs_id != -9999.0
        AND model_key IN ({params});
    """,
        model_keys,
    )
    data = cursor.fetchall()

    conn.close()
    return data


def build_runner_file(data, runner_file):
    """"""
    with open(runner_file, "w", newline="") as f:
        f.write("nwm_reach_id,model_key\n")
        writer = csv.writer(f)
        writer.writerows(data)


data = get_reaches(model_keys)
build_runner_file(data, runner_file)
