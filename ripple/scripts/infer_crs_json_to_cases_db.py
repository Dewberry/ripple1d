import json
import os
import posixpath
import sqlite3


def read_inferred_crs_json(inferred_crs_json_path: str) -> dict:
    with open(inferred_crs_json_path, "r") as f:
        return json.loads(f.read())


def update_key_base_path(data: list, new_key_base: str, old_key_base: str) -> list:
    new_data = {}
    for key, val in data.items():
        if key != "null":
            key = posixpath.join(new_key_base, key.lstrip(old_key_base).replace("\\", "/"))
            new_data[key] = val
    return new_data


def create_table(cases_db_path: str, table_name: str):
    with sqlite3.connect(cases_db_path) as connection:
        cursor = connection.cursor()

        # create inferred crs A table if it doesn't exists
        res = cursor.execute(f"SELECT name FROM sqlite_master WHERE name='{table_name}'")
        if res.fetchone():
            cursor.execute(f"DROP TABLE {table_name}")
        connection.commit()

        sql_query = f"""
        CREATE TABLE {table_name}(
        mip_case TEXT,
        key TEXT,
        crs TEXT,
        ratio_of_best_crs TEXT)
        """
        cursor.execute(sql_query)
        connection.commit()


def main(inferred_crs_json_path: str, new_key_base: str, old_key_base: str, cases_db_path: str):
    # read inferred crs json
    crs_data = read_inferred_crs_json(inferred_crs_json_path)

    # update keys
    if new_key_base and old_key_base:
        crs_data = update_key_base_path(crs_data, new_key_base, old_key_base)

    for table_name in ["inferred_crs_A", "inferred_crs_B"]:
        create_table(cases_db_path, table_name)

    with sqlite3.connect(cases_db_path) as connection:
        cursor = connection.cursor()

        for key, data in crs_data.items():
            epsgs = []
            for county in data["county_results"]:
                if county["crscode2intersectratio"]:
                    epsgs += list(county["crscode2intersectratio"].keys())
                    
            if data["best_crs"] and len(set(epsgs)) == 1:
                cursor.execute(
                    "INSERT INTO inferred_crs_A (mip_case, key, crs, ratio_of_best_crs) VALUES (?, ?, ?, ?)",
                    (data["county_results"][0]["mip_case"], key, data["best_crs"], data["ratio_of_best_crs"]),
                )
            elif data["best_crs"] and len(set(epsgs)) > 1:
                cursor.execute(
                    "INSERT INTO inferred_crs_B (mip_case, key, crs, ratio_of_best_crs) VALUES (?, ?, ?, ?)",
                    (data["county_results"][0]["mip_case"], key, data["best_crs"], data["ratio_of_best_crs"]),
                )

        connection.commit()


if __name__ == "__main__":

    inferred_crs_json_path = r"C:\Users\mdeshotel\Downloads\ras_projects_crs_inference_v2esque_with_best_crs\ras_projects_crs_inference_v2esque.json"
    new_key_base = r"s3://fim/mip/cases"
    old_key_base = r"T:\CCSI\TECH\owp\mip"
    cases_db_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\mip_models\cases.db"
    bucket = "fim"

    main(inferred_crs_json_path, new_key_base, old_key_base, cases_db_path)
