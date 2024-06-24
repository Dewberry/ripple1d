import json
import os
import posixpath
import sqlite3


def read_inferred_crs_json(inferred_crs_json_path: str) -> dict:
    with open(inferred_crs_json_path, "r") as f:
        return json.loads(f.read())


def update_key_base_path(data: list, new_key_base: str, old_key_base: str) -> list:
    new_data = []
    for item in data:
        if item["key"]:
            item["key"] = posixpath.join(new_key_base, item["key"].lstrip(old_key_base).replace("\\", "/"))
            new_data.append(item)
    return new_data


def main(inferred_crs_json_path: str, new_key_base: str, old_key_base: str, cases_db_path: str):
    # read inferred crs json
    crs_data = read_inferred_crs_json(inferred_crs_json_path)

    # update keys
    if new_key_base and old_key_base:
        crs_data = update_key_base_path(crs_data, new_key_base, old_key_base)

    with sqlite3.connect(cases_db_path) as connection:
        cursor = connection.cursor()

        # create inferred crs A table if it doesn't exists
        res = cursor.execute("SELECT name FROM sqlite_master WHERE name='inferred_crs_A'")
        if res.fetchone():
            cursor.execute("DROP TABLE inferred_crs_A")
        connection.commit()

        sql_query = """
        CREATE TABLE inferred_crs_A(
        mip_case TEXT,
        key TEXT,
        crs TEXT)
        """
        cursor.execute(sql_query)
        connection.commit()
        for data in crs_data:
            if data["crscode2intersectratio"] and len(data["crscode2intersectratio"]) == 1:
                crs = list(data["crscode2intersectratio"].keys())[0]
                cursor.execute(
                    "INSERT INTO inferred_crs_A (mip_case, key, crs) VALUES (?, ?, ?)",
                    (data["mip_case"], data["key"], crs),
                )
        connection.commit()


if __name__ == "__main__":

    inferred_crs_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\mip_models\ras_projects_crs_inference\ras_projects_crs_inference.json"
    new_key_base = r"s3://fim/mip/cases"
    old_key_base = r"T:\CCSI\TECH\owp\mip"
    cases_db_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\mip_models\cases.db"
    bucket = "fim"

    main(inferred_crs_json_path, new_key_base, old_key_base, cases_db_path)
